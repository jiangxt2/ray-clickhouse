from __future__ import annotations

import re
from pathlib import Path

import pytest

from tools.check_compatibility import run_checks, validate_compatibility_texts

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def _repository_texts() -> tuple[str, str, str, str]:
    return (
        (REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml").read_text(
            encoding="utf-8"
        ),
        (REPOSITORY_ROOT / "README.md").read_text(encoding="utf-8"),
        (REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8"),
        (REPOSITORY_ROOT / "src" / "ray_clickhouse" / "_compat.py").read_text(
            encoding="utf-8"
        ),
    )


def _replace_clickhouse_job_fragment(workflow: str, old: str, new: str) -> str:
    start = workflow.index("  clickhouse-it:\n")
    end = workflow.index("  package-build:\n", start)
    job = workflow[start:end]
    assert old in job
    return workflow[:start] + job.replace(old, new, 1) + workflow[end:]


def _replace_job_fragment(workflow: str, job_name: str, old: str, new: str) -> str:
    start = workflow.index(f"  {job_name}:\n")
    match = re.search(r"(?m)^  [A-Za-z0-9_-]+:\n", workflow[start + 1 :])
    end = start + 1 + match.start() if match is not None else len(workflow)
    body = workflow[start:end]
    assert old in body
    return workflow[:start] + body.replace(old, new, 1) + workflow[end:]


def test_repository_compatibility_contracts_match() -> None:
    assert run_checks() == []


@pytest.mark.parametrize(
    ("source_index", "old", "new", "expected"),
    (
        (0, 'ray: "2.56.1"', 'ray: "2.56.0"', "CI unit matrix"),
        (
            0,
            "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
            "actions/checkout@v7",
            "full SHA",
        ),
        (1, "| 3.11 | 2.58.0 |", "| 3.14 | 2.58.0 |", "README matrix"),
        (2, '"ray[data]>=2.55,<2.59"', '"ray[data]>=2.55,<2.60"', "Ray dependency"),
        (
            2,
            '"Programming Language :: Python :: 3.13"',
            '"Programming Language :: Python :: 3.14"',
            "Python classifiers",
        ),
        (3, "_MAX_RAY = (2, 59, 0)", "_MAX_RAY = (2, 60, 0)", "maximum Ray version"),
    ),
)
def test_compatibility_checker_rejects_drift(
    source_index: int, old: str, new: str, expected: str
) -> None:
    sources = list(_repository_texts())
    assert old in sources[source_index]
    sources[source_index] = sources[source_index].replace(old, new, 1)

    errors = validate_compatibility_texts(*sources)

    assert any(expected in error for error in errors)


def test_compatibility_checker_rejects_package_python_drift() -> None:
    workflow, readme, pyproject, compatibility_module = _repository_texts()
    prefix, package_job = workflow.split("  package-smoke:\n", 1)
    assert '- python: "3.11"' in package_job
    package_job = package_job.replace('- python: "3.11"', '- python: "3.14"', 1)

    errors = validate_compatibility_texts(
        prefix + "  package-smoke:\n" + package_job,
        readme,
        pyproject,
        compatibility_module,
    )

    assert any("package Python matrix" in error for error in errors)


def test_compatibility_checker_rejects_extra_python_classifier() -> None:
    workflow, readme, pyproject, compatibility_module = _repository_texts()
    pyproject = pyproject.replace(
        '    "Programming Language :: Python :: 3.13",\n',
        '    "Programming Language :: Python :: 3.13",\n'
        '    "Programming Language :: Python :: 3.14",\n',
    )

    errors = validate_compatibility_texts(
        workflow, readme, pyproject, compatibility_module
    )

    assert any("Python classifiers" in error for error in errors)


@pytest.mark.parametrize(
    ("old", "new", "expected"),
    (
        ("^[0-9a-f]{40}$", "^[0-9a-f]{7,40}$", "candidate job"),
        (
            "ref: ${{ needs.candidate.outputs.sha }}",
            "ref: ${{ inputs.candidate_sha || github.sha }}",
            "unit job",
        ),
        ("needs: candidate", "needs: []", "unit job"),
    ),
)
def test_compatibility_checker_rejects_mutable_candidate_policy(
    old: str, new: str, expected: str
) -> None:
    workflow, readme, pyproject, compatibility_module = _repository_texts()
    assert old in workflow
    workflow = workflow.replace(old, new, 1)

    errors = validate_compatibility_texts(
        workflow, readme, pyproject, compatibility_module
    )

    assert any(expected in error for error in errors)


@pytest.mark.parametrize(
    ("fragment", "source_anchor", "target_anchor", "expected"),
    (
        (
            "      - run: .venv/bin/mypy\n",
            "      - run: .venv/bin/mypy\n",
            "      - run: .venv/bin/python -m build\n",
            "quality job",
        ),
        (
            "      - run: .venv/bin/python -m build\n",
            "      - run: .venv/bin/python -m build\n",
            "      - run: .venv/bin/mypy\n",
            "package-build job",
        ),
        (
            ".venv/bin/python -m pytest tests/unit tests/contract",
            ".venv/bin/python -m pytest tests/unit tests/contract",
            "      - run: .venv/bin/python -m build\n",
            "unit job",
        ),
    ),
)
def test_compatibility_checker_rejects_command_moved_to_wrong_job(
    fragment: str, source_anchor: str, target_anchor: str, expected: str
) -> None:
    workflow, readme, pyproject, compatibility_module = _repository_texts()
    assert source_anchor in workflow
    assert target_anchor in workflow
    workflow = workflow.replace(source_anchor, "", 1)
    workflow = workflow.replace(target_anchor, target_anchor + fragment, 1)

    errors = validate_compatibility_texts(
        workflow, readme, pyproject, compatibility_module
    )

    assert any(expected in error for error in errors)


def test_compatibility_checker_requires_clickhouse_integration_job() -> None:
    workflow, readme, pyproject, compatibility_module = _repository_texts()
    workflow = workflow.replace("  clickhouse-it:\n", "  optional-clickhouse-it:\n", 1)

    errors = validate_compatibility_texts(
        workflow, readme, pyproject, compatibility_module
    )

    assert "CI workflow is missing required job: 'clickhouse-it'" in errors


@pytest.mark.parametrize(
    ("old", "new", "expected"),
    (
        ("needs: candidate", "needs: []", "clickhouse-it job"),
        (
            "ref: ${{ needs.candidate.outputs.sha }}",
            "ref: ${{ github.sha }}",
            "clickhouse-it job",
        ),
        (
            "uv sync --extra dev --frozen",
            "uv sync --extra dev",
            "clickhouse-it job",
        ),
        (
            "run: ./scripts/run_clickhouse_it.sh",
            "run: echo integration-disabled",
            "Run ClickHouse integration",
        ),
        (
            "RAY_CLICKHOUSE_IT_ARTIFACT_DIR: artifacts/it",
            "RAY_CLICKHOUSE_IT_ARTIFACT_DIR: .artifacts/it",
            "Run ClickHouse integration",
        ),
        ("if: always()", "if: success()", "Upload ClickHouse integration evidence"),
        (
            "uses: actions/upload-artifact@",
            "uses: actions/download-artifact@",
            "Upload ClickHouse integration evidence",
        ),
        (
            "id: upload-evidence",
            "id: upload-results",
            "Upload ClickHouse integration evidence",
        ),
        (
            "name: clickhouse-26.8-integration",
            "name: clickhouse-integration",
            "Upload ClickHouse integration evidence",
        ),
        (
            "path: artifacts/it",
            "path: .artifacts/it",
            "Upload ClickHouse integration evidence",
        ),
        (
            "if-no-files-found: error",
            "if-no-files-found: warn",
            "Upload ClickHouse integration evidence",
        ),
        (
            "retention-days: 90",
            "retention-days: 1",
            "Upload ClickHouse integration evidence",
        ),
    ),
)
def test_compatibility_checker_rejects_clickhouse_job_drift(
    old: str, new: str, expected: str
) -> None:
    workflow, readme, pyproject, compatibility_module = _repository_texts()
    workflow = _replace_clickhouse_job_fragment(workflow, old, new)

    errors = validate_compatibility_texts(
        workflow, readme, pyproject, compatibility_module
    )

    assert any(expected in error for error in errors)


def test_compatibility_checker_rejects_reusable_candidate_attestation() -> None:
    workflow, readme, pyproject, compatibility_module = _repository_texts()
    workflow = _replace_job_fragment(
        workflow,
        "candidate-record",
        "jiangxt2/ray-clickhouse/.github/workflows/ci.yml@refs/heads/master",
        "jiangxt2/ray-clickhouse/.github/workflows/caller.yml@refs/heads/master",
    )

    errors = validate_compatibility_texts(
        workflow, readme, pyproject, compatibility_module
    )

    assert any("candidate-record job" in error for error in errors)


def test_compatibility_checker_rejects_package_build_release_privilege() -> None:
    workflow, readme, pyproject, compatibility_module = _repository_texts()
    workflow = _replace_job_fragment(
        workflow,
        "package-build",
        "      contents: read\n",
        "      contents: read\n      id-token: write\n      attestations: write\n",
    )

    errors = validate_compatibility_texts(
        workflow, readme, pyproject, compatibility_module
    )

    assert any("must not receive release privilege" in error for error in errors)


def test_compatibility_checker_rejects_bare_artifact_digest_output() -> None:
    workflow, readme, pyproject, compatibility_module = _repository_texts()
    workflow = workflow.replace(
        "artifact-digest: sha256:${{ steps.upload-evidence.outputs.artifact-digest }}",
        "artifact-digest: ${{ steps.upload-evidence.outputs.artifact-digest }}",
        1,
    )

    errors = validate_compatibility_texts(
        workflow, readme, pyproject, compatibility_module
    )

    assert any("sha256 prefixes" in error for error in errors)


def test_compatibility_checker_rejects_path_filters() -> None:
    workflow, readme, pyproject, compatibility_module = _repository_texts()
    workflow = workflow.replace(
        "  pull_request:\n", "  pull_request:\n    paths:\n      - src/**\n"
    )

    errors = validate_compatibility_texts(
        workflow, readme, pyproject, compatibility_module
    )

    assert "CI workflow must not use path filters" in errors


@pytest.mark.parametrize(
    ("job", "replacement"),
    (
        ("docs", "docs-disabled"),
        ("docs-linkcheck", "docs-linkcheck-disabled"),
        ("candidate-record", "candidate-record-disabled"),
    ),
)
def test_compatibility_checker_requires_release_readiness_jobs(
    job: str, replacement: str
) -> None:
    workflow, readme, pyproject, compatibility_module = _repository_texts()
    workflow = workflow.replace(f"  {job}:\n", f"  {replacement}:\n", 1)

    errors = validate_compatibility_texts(
        workflow, readme, pyproject, compatibility_module
    )

    assert f"CI workflow is missing required job: '{job}'" in errors


@pytest.mark.parametrize(
    ("job", "old", "new", "expected"),
    (
        (
            "quality",
            ".venv/bin/python tools/check_release.py check",
            "echo release-check-disabled",
            "quality job",
        ),
        (
            "candidate-record",
            "artifact-ids: ${{ needs.package-build.outputs.artifact-id }}",
            "name: distributions",
            "candidate-record job",
        ),
        (
            "candidate-record",
            "digest-mismatch: error",
            "digest-mismatch: warn",
            "candidate-record job",
        ),
        (
            "candidate-record",
            "needs: [candidate, unit, quality, docs, docs-linkcheck, "
            "clickhouse-it, package-build, package-smoke]",
            "needs: [candidate, unit, quality, package-build, package-smoke]",
            "candidate-record job",
        ),
        (
            "package-smoke",
            "d.locate_file('ray_clickhouse/py.typed').is_file()",
            "True",
            "package-smoke job",
        ),
    ),
)
def test_compatibility_checker_rejects_release_candidate_drift(
    job: str, old: str, new: str, expected: str
) -> None:
    workflow, readme, pyproject, compatibility_module = _repository_texts()
    workflow = _replace_job_fragment(workflow, job, old, new)

    errors = validate_compatibility_texts(
        workflow, readme, pyproject, compatibility_module
    )

    assert any(expected in error for error in errors)
