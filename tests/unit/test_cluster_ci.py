"""Check that multi-node validation uses the candidate and preserves evidence."""

from pathlib import Path

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
    assert "@sha256:" in execute["env"]["RAY_CLICKHOUSE_RAY_BASE_SOURCE"]
    assert "2.58.0" in execute["env"]["RAY_CLICKHOUSE_RAY_BASE_LOCAL_IMAGE"]
    assert execute["env"]["RAY_CLICKHOUSE_CLUSTER_PYTEST_MARKER"] == "integration"
    assert upload["if"] == "always()"
    assert "predicate-ray258-it" in workflow["jobs"]["candidate-record"]["needs"]
