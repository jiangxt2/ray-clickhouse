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
    checkout, execute, upload = job["steps"]
    assert checkout["with"]["ref"] == "${{ needs.candidate.outputs.sha }}"
    assert checkout["with"]["persist-credentials"] is False
    assert execute["run"] == "./scripts/run_ray_cluster_it.sh"
    assert upload["if"] == "always()"
    assert upload["with"]["if-no-files-found"] == "error"
    assert "ray-cluster-it" in workflow["jobs"]["candidate-record"]["needs"]
