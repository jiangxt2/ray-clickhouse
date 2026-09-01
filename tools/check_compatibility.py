"""Validate compatibility declarations and CI policy without third-party parsers."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import TypeVar

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent

EXPECTED_MATRIX = (
    ("3.12", "2.55.0"),
    ("3.12", "2.56.1"),
    ("3.12", "2.57.0"),
    ("3.12", "2.58.0"),
    ("3.10", "2.58.0"),
    ("3.11", "2.58.0"),
    ("3.13", "2.58.0"),
)
EXPECTED_PACKAGE_PYTHONS = ("3.10", "3.11", "3.12", "3.13")
EXPECTED_PYTHON_RANGE = ">=3.10,<3.14"
EXPECTED_RAY_RANGE = ">=2.55,<2.59"

_JOB_PATTERN_TEMPLATE = r"(?ms)^  {job}:\n(?P<body>.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)"
_MATRIX_ENTRY_PATTERN = re.compile(
    r'^          - python: "(?P<python>[0-9]+\.[0-9]+)"\s*\n'
    r'^            ray: "(?P<ray>[0-9]+\.[0-9]+\.[0-9]+)"\s*$',
    re.MULTILINE,
)
_PACKAGE_ENTRY_PATTERN = re.compile(
    r'^          - python: "(?P<python>[0-9]+\.[0-9]+)"\s*$', re.MULTILINE
)
_README_MATRIX_PATTERN = re.compile(
    r"^\|\s*(?P<python>[0-9]+\.[0-9]+)\s*"
    r"\|\s*(?P<ray>[0-9]+\.[0-9]+\.[0-9]+)\s*\|",
    re.MULTILINE,
)
_PYTHON_CLASSIFIER_PATTERN = re.compile(
    r'^\s+"Programming Language :: Python :: (?P<python>[0-9]+\.[0-9]+)",\s*$',
    re.MULTILINE,
)
_USES_PATTERN = re.compile(r"^\s*(?:-\s*)?uses:\s*(?P<target>\S+)", re.MULTILINE)
_FULL_SHA = re.compile(r"[0-9a-f]{40}")
T = TypeVar("T")


def _job_body(workflow: str, job: str) -> str:
    match = re.search(_JOB_PATTERN_TEMPLATE.format(job=re.escape(job)), workflow)
    return match.group("body") if match is not None else ""


def _unit_matrix(workflow: str) -> tuple[tuple[str, str], ...]:
    return tuple(
        (entry.group("python"), entry.group("ray"))
        for entry in _MATRIX_ENTRY_PATTERN.finditer(_job_body(workflow, "unit"))
    )


def _package_pythons(workflow: str) -> tuple[str, ...]:
    return tuple(
        entry.group("python")
        for entry in _PACKAGE_ENTRY_PATTERN.finditer(
            _job_body(workflow, "package-smoke")
        )
    )


def _readme_matrix(readme: str) -> tuple[tuple[str, str], ...]:
    return tuple(
        (entry.group("python"), entry.group("ray"))
        for entry in _README_MATRIX_PATTERN.finditer(readme)
    )


def _duplicates(values: tuple[T, ...]) -> tuple[T, ...]:
    seen: set[T] = set()
    duplicates: list[T] = []
    for value in values:
        if value in seen and value not in duplicates:
            duplicates.append(value)
        seen.add(value)
    return tuple(duplicates)


def validate_compatibility_texts(
    workflow: str,
    readme: str,
    pyproject: str,
    compatibility_module: str,
) -> list[str]:
    """Return every compatibility or workflow policy violation."""
    errors: list[str] = []

    unit_matrix = _unit_matrix(workflow)
    readme_matrix = _readme_matrix(readme)
    package_pythons = _package_pythons(workflow)
    python_classifiers = tuple(
        entry.group("python")
        for entry in _PYTHON_CLASSIFIER_PATTERN.finditer(pyproject)
    )
    if unit_matrix != EXPECTED_MATRIX:
        errors.append(
            f"CI unit matrix is {unit_matrix!r}; expected {EXPECTED_MATRIX!r}"
        )
    if readme_matrix != EXPECTED_MATRIX:
        errors.append(
            f"README matrix is {readme_matrix!r}; expected {EXPECTED_MATRIX!r}"
        )
    if package_pythons != EXPECTED_PACKAGE_PYTHONS:
        errors.append(
            "CI package Python matrix is "
            f"{package_pythons!r}; expected {EXPECTED_PACKAGE_PYTHONS!r}"
        )
    for label, matrix in (("CI unit", unit_matrix), ("README", readme_matrix)):
        duplicates = _duplicates(matrix)
        if duplicates:
            errors.append(f"{label} matrix has duplicate entries: {duplicates!r}")

    if f'requires-python = "{EXPECTED_PYTHON_RANGE}"' not in pyproject:
        errors.append(f"pyproject requires-python must be {EXPECTED_PYTHON_RANGE}")
    if f'"ray[data]{EXPECTED_RAY_RANGE}"' not in pyproject:
        errors.append(f"pyproject Ray dependency must be {EXPECTED_RAY_RANGE}")
    if python_classifiers != EXPECTED_PACKAGE_PYTHONS:
        errors.append(
            "pyproject Python classifiers are "
            f"{python_classifiers!r}; expected {EXPECTED_PACKAGE_PYTHONS!r}"
        )
    if f"Ray releases `{EXPECTED_RAY_RANGE}`" not in readme:
        errors.append(f"README Ray range must be {EXPECTED_RAY_RANGE}")
    if "Python 3.10–3.13" not in readme:
        errors.append(f"README Python range must be {EXPECTED_PYTHON_RANGE}")
    if "_MIN_RAY = (2, 55, 0)" not in compatibility_module:
        errors.append("compatibility module minimum Ray version must be 2.55.0")
    if "_MAX_RAY = (2, 59, 0)" not in compatibility_module:
        errors.append("compatibility module maximum Ray version must be 2.59.0")

    required_workflow_fragments = (
        "workflow_call:",
        "pull_request:",
        "branches: [master]",
        "permissions:",
        "contents: read",
        "cancel-in-progress: true",
    )
    for fragment in required_workflow_fragments:
        if fragment not in workflow:
            errors.append(f"CI workflow is missing required fragment: {fragment!r}")

    shared_candidate_ref = "ref: ${{ needs.candidate.outputs.sha }}"
    required_job_fragments = {
        "candidate": (
            "outputs:",
            "sha: ${{ steps.resolve.outputs.sha }}",
            "INPUT_CANDIDATE_SHA: ${{ inputs.candidate_sha }}",
            'candidate_sha="${INPUT_CANDIDATE_SHA:-${GITHUB_SHA}}"',
            '[[ ! "${candidate_sha}" =~ ^[0-9a-f]{40}$ ]]',
            'echo "sha=${candidate_sha}" >> "${GITHUB_OUTPUT}"',
        ),
        "unit": (
            "needs: candidate",
            shared_candidate_ref,
            'uv pip install -e ".[dev]" "ray[data]==${{ matrix.ray }}"',
            ".venv/bin/python -m pytest tests/unit tests/contract",
            "--cov=ray_clickhouse --cov-report=term-missing",
            'RAY_ENABLE_UV_RUN_RUNTIME_ENV: "0"',
            'RAY_USAGE_STATS_ENABLED: "0"',
        ),
        "quality": (
            "needs: candidate",
            shared_candidate_ref,
            "uv sync --extra dev --frozen",
            "uv lock --check",
            ".venv/bin/ruff format --check .",
            ".venv/bin/ruff check .",
            ".venv/bin/mypy",
            ".venv/bin/python tools/check_compatibility.py",
        ),
        "package-build": (
            "needs: candidate",
            shared_candidate_ref,
            ".venv/bin/python -m build",
            ".venv/bin/twine check dist/*",
            "actions/upload-artifact@",
        ),
        "package-smoke": (
            "needs: [candidate, package-build]",
            shared_candidate_ref,
            "actions/download-artifact@",
            'test "${#packages[@]}" -eq 1',
            "uv pip check --python .venv-wheel/bin/python",
            "uv pip check --python .venv-sdist/bin/python",
            "import clickhouse_connect, pyarrow, ray, ray_clickhouse",
        ),
    }
    for job, fragments in required_job_fragments.items():
        body = _job_body(workflow, job)
        if not body:
            errors.append(f"CI workflow is missing required job: {job!r}")
            continue
        for fragment in fragments:
            if fragment not in body:
                errors.append(
                    f"CI {job} job is missing required fragment: {fragment!r}"
                )

    if re.search(r"(?m)^\s+paths(?:-ignore)?:", workflow):
        errors.append("CI workflow must not use path filters")
    for target in _USES_PATTERN.findall(workflow):
        if target.startswith("./"):
            continue
        if "@" not in target:
            errors.append(f"workflow action is missing a ref: {target!r}")
            continue
        _, ref = target.rsplit("@", 1)
        if _FULL_SHA.fullmatch(ref) is None:
            errors.append(f"workflow action is not pinned to a full SHA: {target!r}")
    return errors


def run_checks() -> list[str]:
    """Read repository facts and return every validation error."""
    return validate_compatibility_texts(
        (REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml").read_text(
            encoding="utf-8"
        ),
        (REPOSITORY_ROOT / "README.md").read_text(encoding="utf-8"),
        (REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8"),
        (REPOSITORY_ROOT / "src" / "ray_clickhouse" / "_compat.py").read_text(
            encoding="utf-8"
        ),
    )


def main() -> int:
    errors = run_checks()
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("Compatibility and CI checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
