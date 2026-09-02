from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def test_ci_keeps_required_workflow_and_fail_closed_noop_classifier() -> None:
    workflow = ROOT.joinpath(".github/workflows/ci.yml").read_text(encoding="utf-8")

    assert "paths-ignore:" not in workflow
    assert "skip-checks:" not in workflow
    assert "runtime_relevant" in workflow
    assert "ray_clickhouse_comparison/config.py" in workflow
    assert "--github-output" in workflow
    assert "Checkout validated candidate" in workflow
    assert 'test "$(git rev-parse HEAD)" = "${CANDIDATE_SHA}"' in workflow
    assert 'python-version: "3.12"' in workflow
    assert "uv run --project comparison/official python" in workflow
    assert "python3 comparison/official/src/ray_clickhouse_comparison/config.py" not in workflow
    assert "Record evidence-only no-op" in workflow
    assert "if: needs.candidate.outputs.runtime_relevant == 'true'" in workflow
    assert "if: always() && needs.candidate.outputs.runtime_relevant == 'true'" in workflow


def test_reference_workflow_is_manual_pinned_and_read_only() -> None:
    workflow = ROOT.joinpath(".github/workflows/official-comparison.yml").read_text(
        encoding="utf-8"
    )

    assert "workflow_dispatch:" in workflow
    assert "  push:" not in workflow
    assert "contents: read" in workflow
    assert "cancel-in-progress: false" in workflow
    assert "runs-on: ubuntu-24.04" in workflow
    assert "persist-credentials: false" in workflow
    assert "retention-days:" in workflow
    assert "workflow-identity.json" in workflow
    assert "workflow_sha" in workflow
    assert "artifact-provenance.json" in workflow
    assert "ray-clickhouse-comparison-provenance" in workflow
    assert "artifact-provenance.json\\n" in workflow
    assert "SHA256SUMS" in workflow
    assert "artifact-digest" in workflow
    assert "Upload artifact provenance" in workflow
    assert "ray-clickhouse-comparison-preflight" in workflow
    assert "Stage workflow preflight evidence" in workflow
    assert "Sanitize failure diagnostics" in workflow
    assert "Upload failure diagnostics" in workflow
    assert "--require-complete" in workflow
    assert "workflow_run_attempt" in workflow
    assert "sanitize-tree" in workflow
    assert "orchestration.log" in workflow
    assert "if: success() && steps.sanitize.outcome == 'success'" in workflow
    assert "if: always() && steps.diagnostics.outcome == 'success'" in workflow
    for action in ("actions/checkout@", "astral-sh/setup-uv@", "actions/upload-artifact@"):
        occurrences = [line for line in workflow.splitlines() if action in line]
        assert occurrences
        assert all(
            "# v" in line and len(line.split("@", 1)[1].split()[0]) == 40 for line in occurrences
        )


def test_comparison_docker_context_is_deny_all_allowlist() -> None:
    ignore = ROOT.joinpath("docker/comparison/Dockerfile.dockerignore").read_text(encoding="utf-8")
    lines = [line for line in ignore.splitlines() if line and not line.startswith("#")]

    assert lines[0] == "**"
    assert "!.git" not in lines
    assert "!.venv" not in lines
    assert "!comparison/official/evidence/**" not in lines
    assert "comparison/official/evidence/**" not in lines
    assert "!src/**" not in lines
    assert "!comparison/official/src/**" not in lines
    assert "!comparison/official/env/official-requirements.txt" in lines
    assert "!comparison/official/env/external-requirements.txt" in lines
    assert "!comparison/official/config/**" in lines
    assert "!comparison/official/schema/**" in lines

    dockerfile = ROOT.joinpath("docker/comparison/Dockerfile").read_text(encoding="utf-8")
    root_copy_sources = {
        line.split()[1]
        for line in dockerfile.splitlines()
        if line.startswith("COPY ") and "--from=" not in line
    }
    assert root_copy_sources == {
        "comparison/official/env/official-requirements.txt",
        "comparison/official/env/external-requirements.txt",
        "comparison/official/config",
        "comparison/official/schema",
    }


def test_comparison_image_is_built_once_and_compose_never_builds() -> None:
    runner = ROOT.joinpath("scripts/run_official_comparison.sh").read_text(encoding="utf-8")
    compose = ROOT.joinpath("docker/comparison/compose.yaml").read_text(encoding="utf-8")
    dockerfile = ROOT.joinpath("docker/comparison/Dockerfile").read_text(encoding="utf-8")

    assert runner.count("docker_cmd buildx build") == 1
    assert "uv build comparison/official --wheel --force-pep517 --no-cache" in runner
    assert "ray-clickhouse-comparison verify-wheel" in runner
    assert "ray-clickhouse-comparison context-manifest" in runner
    assert "compose_cmd up --detach --wait --no-build" in runner
    assert "build:" not in compose
    assert '$(basename "${external_wheels[0]}")' in runner
    assert "/tmp/external-wheel/*.whl" in dockerfile
    assert "/tmp/harness-wheel/*.whl" in dockerfile
    assert "/tmp/ray_clickhouse.whl" not in dockerfile
    assert dockerfile.count("--no-deps /tmp/external-wheel/*.whl") == 1
    assert dockerfile.count("--no-deps /tmp/harness-wheel/*.whl") == 3
    assert dockerfile.count("-r /tmp/official-requirements.txt") == 1
    assert dockerfile.count("-r /tmp/external-requirements.txt") == 1
    assert "target=/var/cache/pip" in dockerfile
    assert "PIP_DEFAULT_TIMEOUT=60" in dockerfile
    assert "PIP_RETRIES=8" in dockerfile
    assert "target=/root/.cache/pip" not in dockerfile
    assert dockerfile.startswith("# syntax=docker.m.daocloud.io/docker/dockerfile:1.7@sha256:")
    assert "docker.io/docker/dockerfile" not in dockerfile
    assert "--include-dashboard=true" in compose
    assert "pull_policy: never" in compose
    assert "--object-spilling-directory=/tmp/ray-spill" in compose
    assert "job submit" not in runner
    assert "condition: service_healthy" in compose
    assert (
        "urllib.request.urlopen('http://127.0.0.1:18123/ping', timeout=1).status == 200" in compose
    )
    assert (
        "urllib.request.urlopen('http://127.0.0.1:18123/ping', timeout=1).status == 200" in compose
    )
    assert "if (( repetition % 2 == 1 ))" in runner
    assert "RAY_COMPARISON_PAIR_POSITION=$pair_position" in runner
    assert "ray_clickhouse_comparison prepare" in runner
    assert "ray_clickhouse_comparison warmup" in runner
    assert "RAY_COMPARISON_TASK_EVIDENCE" in runner
    assert "RAY_COMPARISON_MEASUREMENT_STARTED" in runner
    assert "RAY_COMPARISON_MEASUREMENT_COMPLETE" in runner
    assert "ray-clickhouse-comparison validate-result" in runner
    assert "if ! compose_cmd down --volumes --remove-orphans; then" in runner
    assert 'if [[ "$status" -eq 0 && "$cleanup_status" -ne 0 ]]' in runner
    assert "label=com.docker.compose.project=$active_project" in runner
    assert "docker-images-dangling-before.txt" in runner
    assert "docker-images-dangling-after.txt" in runner
    assert "baseline_recorded=true" in runner
    assert "{{.CreatedAt}}" in runner
    assert 'docker_cmd image inspect "$base_image"' in runner
    assert 'docker_cmd tag "$base_image" "$base_local_image"' in runner
    assert '--build-arg "RAY_BASE_IMAGE=$base_local_image"' in runner
    assert "RAY_COMPARISON_DRIVER_PID_FILE" in runner
    assert "artifact directory must be absent or empty" in runner
    assert "cleanup_temporary_root" in runner
    assert "sparse per-service Ray Object Store" in runner
    assert "run_case official read.default.single 0 0 none true false" in runner
    assert "run_case external read.default.single 0 1 none true false" in runner
    assert 'active_project=""' in runner
    assert "dockerfile-frontend-pull.log" in runner
    assert "docker.m.daocloud.io/docker/dockerfile:1.7@sha256:" in runner
    assert "run_with_timeout 10" in runner
    assert "compose_cmd exec -T ray-head" in runner
    assert "RAY_COMPARISON_RAY_ADDRESS: auto" in compose
    assert "RAY_COMPARISON_RUNTIME: ${RAY_COMPARISON_RUNTIME" in compose
    assert "ray.init(address='auto')" in runner


def test_package_initializer_has_no_runtime_import_side_effects() -> None:
    initializer = ROOT.joinpath(
        "comparison/official/src/ray_clickhouse_comparison/__init__.py"
    ).read_text(encoding="utf-8")

    assert "from ray_clickhouse_comparison" not in initializer
    assert "import pyarrow" not in initializer
    assert "import ray" not in initializer


def test_runner_uses_only_public_connector_entry_points() -> None:
    runner = ROOT.joinpath("comparison/official/src/ray_clickhouse_comparison/runner.py").read_text(
        encoding="utf-8"
    )

    assert "ray.data._internal" not in runner
    assert "ray_clickhouse.datasource" not in runner
    assert "ray_clickhouse.datasink" not in runner
    assert 'import_module("ray.data")' in runner
    assert 'import_module("ray_clickhouse")' in runner
    assert ".read_clickhouse(" in runner
    assert ".write_clickhouse(" in runner
