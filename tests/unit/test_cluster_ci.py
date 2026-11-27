"""Check that multi-node validation uses the candidate and preserves evidence."""

from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import HTTPError

import pytest
import yaml


def test_cluster_ci_checks_candidate_and_keeps_failure_logs():
    workflow = yaml.safe_load(
        (Path(__file__).parents[2] / ".github/workflows/ci.yml").read_text()
    )
    job = workflow["jobs"]["ray-cluster-it"]
    defaults = workflow["env"]
    clickhouse = "docker.io/clickhouse/clickhouse-server"
    assert defaults["RAY_CLICKHOUSE_IT_IMAGE"] == (
        f"{clickhouse}@sha256:"
        "9f8fa0d51f86611fda226c4085cdafdddce35312a60c934de2e4450d61fcb99a"
    )
    assert defaults["RAY_CLICKHOUSE_RAY_BASE_SOURCE"] == (
        "docker.io/rayproject/ray@sha256:"
        "0ffca209fb4c984d648db550b8dbe7879bdba3bbb5d96ee5894843cf6ac12eb5"
    )
    single = workflow["jobs"]["clickhouse-it"]
    single_step = next(
        step
        for step in single["steps"]
        if step.get("name") == "Run ClickHouse integration"
    )
    single_env = defaults | single.get("env", {}) | single_step.get("env", {})
    assert single_env["RAY_CLICKHOUSE_IT_IMAGE"] == defaults["RAY_CLICKHOUSE_IT_IMAGE"]
    assert job["needs"] == "candidate"
    assert job["timeout-minutes"] == 45
    assert job["strategy"]["fail-fast"] is False
    matrix = job["strategy"]["matrix"]["include"]
    assert {row["clickhouse"]: row["image"] for row in matrix} == {
        "23.3.22.3": f"{clickhouse}:23.3.22.3",
        "24.8.14.39": f"{clickhouse}:24.8.14.39",
        "26.8.1.2041": defaults["RAY_CLICKHOUSE_IT_IMAGE"],
    }
    checkout, execute, upload = job["steps"]
    assert checkout["with"]["ref"] == "${{ needs.candidate.outputs.sha }}"
    assert checkout["with"]["persist-credentials"] is False
    assert execute["run"] == (
        "./scripts/run_distributed_it.sh tests/distributed tests/integration"
    )
    assert execute["env"]["RAY_CLICKHOUSE_CLUSTER_PYTEST_MARKER"] == "cluster"
    assert execute["env"]["RAY_CLICKHOUSE_IT_IMAGE"] == "${{ matrix.image }}"
    cluster_env = defaults | job.get("env", {}) | execute["env"]
    assert (
        cluster_env["RAY_CLICKHOUSE_RAY_BASE_SOURCE"]
        == defaults["RAY_CLICKHOUSE_RAY_BASE_SOURCE"]
    )
    assert upload["with"]["name"] == "${{ matrix.artifact }}"
    assert upload["if"] == "always()"
    assert upload["with"]["if-no-files-found"] == "error"
    assert "ray-cluster-it" in workflow["jobs"]["candidate-record"]["needs"]


def test_distributed_grant_mode_override_preserves_topology_and_users():
    compose = yaml.safe_load(
        (Path(__file__).parents[2] / "docker/distributed/compose.yaml").read_text()
    )
    for name in ("clickhouse", "ch2", "ch3", "ch4"):
        mounts = compose["services"][name]["volumes"]
        assert any("/config.xml:" in mount for mount in mounts)
        assert any("/users.xml:" in mount for mount in mounts)
    assert any(
        "legacy-system-grants.xml" in mount
        for mount in compose["services"]["clickhouse"]["volumes"]
    )


def test_predicate_compatibility_uses_a_separate_pinned_ray258_runtime():
    workflow = yaml.safe_load(
        (Path(__file__).parents[2] / ".github/workflows/ci.yml").read_text()
    )
    job = workflow["jobs"]["predicate-ray258-it"]
    assert job["needs"] == "candidate"
    checkout, execute, upload = job["steps"]
    assert checkout["with"]["ref"] == "${{ needs.candidate.outputs.sha }}"
    assert "tests/integration/test_predicate_pushdown.py" in execute["run"]
    assert "tests/distributed/test_predicate_pushdown.py" in execute["run"]
    predicate_env = workflow["env"] | job.get("env", {}) | execute["env"]
    assert (
        predicate_env["RAY_CLICKHOUSE_IT_IMAGE"]
        == workflow["env"]["RAY_CLICKHOUSE_IT_IMAGE"]
    )
    assert predicate_env["RAY_CLICKHOUSE_RAY_BASE_SOURCE"] == (
        "docker.io/rayproject/ray@sha256:"
        "c3c9573c5c6bfe4127885f79622d6a32064d34cafc7d156ec728aab8657be250"
    )
    assert "2.58.0" in execute["env"]["RAY_CLICKHOUSE_RAY_BASE_LOCAL_IMAGE"]
    assert execute["env"]["RAY_CLICKHOUSE_CLUSTER_PYTEST_MARKER"] == "integration"
    assert upload["if"] == "always()"
    assert "predicate-ray258-it" in workflow["jobs"]["candidate-record"]["needs"]


@pytest.mark.parametrize(
    "error",
    [
        None,
        ConnectionRefusedError("Jobs API is starting"),
        HTTPError("http://127.0.0.1:8265/api/version", 503, "not ready", {}, None),
    ],
)
def test_ray_head_healthcheck_requires_available_jobs_api(error):
    root = Path(__file__).parents[2]
    compose = yaml.safe_load((root / "docker/ray/compose.yaml").read_text())
    healthcheck = compose["services"]["ray-head"]["healthcheck"]
    command = healthcheck["test"]
    assert command[:3] == ["CMD", "python", "-c"]
    assert healthcheck["interval"] == "2s"
    assert healthcheck["timeout"] == "5s"
    assert healthcheck["retries"] == 60
    assert (
        "up --detach --wait --no-build"
        in (root / "scripts/run_ray_cluster_it.sh").read_text()
    )
    response = Mock()
    with patch(
        "urllib.request.urlopen", return_value=response, side_effect=error
    ) as request:
        if error is None:
            exec(command[3], {})
            response.close.assert_called_once_with()
        else:
            with pytest.raises(type(error)):
                exec(command[3], {})
            response.close.assert_not_called()
    request.assert_called_once_with("http://127.0.0.1:8265/api/version", timeout=2)
