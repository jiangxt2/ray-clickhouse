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
    assert job["needs"] == "candidate"
    assert job["timeout-minutes"] == 45
    assert job["strategy"]["fail-fast"] is False
    assert {row["clickhouse"] for row in job["strategy"]["matrix"]["include"]} == {
        "23.3.22.3",
        "24.8.14.39",
        "26.8.1.2041",
    }
    checkout, execute, upload = job["steps"]
    assert checkout["with"]["ref"] == "${{ needs.candidate.outputs.sha }}"
    assert checkout["with"]["persist-credentials"] is False
    assert execute["run"] == (
        "./scripts/run_distributed_it.sh tests/distributed tests/integration"
    )
    assert execute["env"]["RAY_CLICKHOUSE_CLUSTER_PYTEST_MARKER"] == "cluster"
    assert execute["env"]["RAY_CLICKHOUSE_IT_IMAGE"] == "${{ matrix.image }}"
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
