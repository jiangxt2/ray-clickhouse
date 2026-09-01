"""Validate release policy and machine-readable promotion identities."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import urllib.request
import xml.etree.ElementTree as ET
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
REPOSITORY = "jiangxt2/ray-clickhouse"
PROJECT = "ray-clickhouse"
VERSION = "0.1.0"
TAG = "v0.1.0"
CI_WORKFLOW = ".github/workflows/ci.yml"
RELEASE_WORKFLOW = ".github/workflows/release.yml"
OPERATIONS = ("dry-run", "testpypi", "release-tag", "pypi", "github-release")
PREDECESSOR = {
    "testpypi": "dry-run",
    "release-tag": "testpypi",
    "pypi": "release-tag",
    "github-release": "pypi",
}

_FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_USES = re.compile(r"^\s*(?:-\s*)?uses:\s*(?P<target>\S+)", re.MULTILINE)
_WHEEL = re.compile(r"^ray_clickhouse-0\.1\.0-[A-Za-z0-9_.-]+\.whl$")
_ARTIFACT_URL = re.compile(
    r"^https://github\.com/jiangxt2/ray-clickhouse/actions/runs/[1-9][0-9]*/artifacts/"
    r"(?P<artifact_id>[1-9][0-9]*)$"
)
_INDEX_URLS = {
    "https://pypi.org/pypi/ray-clickhouse/json",
    "https://test.pypi.org/pypi/ray-clickhouse/json",
}


def _positive_int(value: Any, *, name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a positive integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be a positive integer") from None
    if parsed < 1:
        raise ValueError(f"{name} must be a positive integer")
    return parsed


def _full_sha(value: Any, *, name: str) -> str:
    if not isinstance(value, str) or _FULL_SHA.fullmatch(value) is None:
        raise ValueError(f"{name} must be a full lowercase commit SHA")
    return value


def _digest(value: Any, *, name: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ValueError(f"{name} must be a sha256 digest")
    return value


def _artifact_url(value: Any, *, artifact_id: int, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} artifact_url is invalid")
    match = _ARTIFACT_URL.fullmatch(value)
    if match is None or int(match.group("artifact_id")) != artifact_id:
        raise ValueError(f"{name} artifact_url does not match its artifact ID")
    return value


def parse_sha256sums(text: str) -> dict[str, str]:
    """Parse a strict SHA256SUMS file."""
    result: dict[str, str] = {}
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line:
            continue
        match = re.fullmatch(r"([0-9a-f]{64}) [ *](\S+)", line)
        if match is None:
            raise ValueError(f"invalid SHA256SUMS line {line_number}")
        digest, name = match.groups()
        if Path(name).name != name or name in result:
            raise ValueError(f"invalid or duplicate release filename: {name!r}")
        result[name] = digest
    if len(result) != 2:
        raise ValueError(
            "SHA256SUMS must contain exactly one wheel and one source archive"
        )
    wheels = [name for name in result if name.endswith(".whl")]
    sources = [name for name in result if name.endswith(".tar.gz")]
    if len(wheels) != 1 or len(sources) != 1:
        raise ValueError(
            "SHA256SUMS must contain one wheel and one .tar.gz source archive"
        )
    if _WHEEL.fullmatch(wheels[0]) is None:
        raise ValueError("SHA256SUMS wheel does not match ray-clickhouse 0.1.0")
    if sources[0] != "ray_clickhouse-0.1.0.tar.gz":
        raise ValueError(
            "SHA256SUMS source archive does not match ray-clickhouse 0.1.0"
        )
    return result


def sha256sums_from_file(path: Path) -> dict[str, str]:
    return parse_sha256sums(path.read_text(encoding="utf-8"))


def release_file_sizes(directory: Path, files: Mapping[str, str]) -> dict[str, int]:
    """Return validated byte sizes for the candidate distribution files."""
    sizes: dict[str, int] = {}
    for name in files:
        path = directory / name
        if not path.is_file():
            raise ValueError(f"missing release file for size record: {name}")
        sizes[name] = _positive_int(path.stat().st_size, name=f"size for {name}")
    return sizes


def verify_files(
    directory: Path,
    expected: Mapping[str, str],
    expected_sizes: Mapping[str, int] | None = None,
) -> list[str]:
    """Return identity errors for release files in a directory."""
    errors: list[str] = []
    for name, digest in expected.items():
        path = directory / name
        if not path.is_file():
            errors.append(f"missing release file: {name}")
            continue
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != digest:
            errors.append(f"release file digest mismatch: {name}")
        if expected_sizes is not None and path.stat().st_size != expected_sizes.get(
            name
        ):
            errors.append(f"release file size mismatch: {name}")
    actual_names = {
        path.name
        for path in directory.iterdir()
        if path.is_file() and path.name != "SHA256SUMS"
    }
    unexpected = actual_names.difference(expected)
    if unexpected:
        errors.append(f"unexpected release files: {sorted(unexpected)!r}")
    return errors


def verify_clickhouse_evidence(directory: Path) -> list[str]:
    """Validate the minimum persisted evidence from the real ClickHouse suite."""
    errors: list[str] = []
    required = (
        "pytest.xml",
        "pytest.log",
        "clickhouse.log",
        "compose-images.txt",
        "compose-image-references.txt",
        "docker-images-dangling-before.txt",
        "docker-images-dangling-after.txt",
        "docker-system-df-before.txt",
        "docker-system-df-after.txt",
    )
    for name in required:
        path = directory / name
        if not path.is_file() or path.stat().st_size == 0:
            errors.append(f"ClickHouse evidence is missing or empty: {name}")
    report = directory / "pytest.xml"
    if report.is_file():
        try:
            root = ET.parse(report).getroot()
        except ET.ParseError as exc:
            errors.append(f"ClickHouse pytest.xml is invalid: {exc}")
        else:
            suites = (
                [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
            )
            tests = sum(int(suite.get("tests", "0")) for suite in suites)
            failures = sum(int(suite.get("failures", "0")) for suite in suites)
            errors_count = sum(int(suite.get("errors", "0")) for suite in suites)
            if tests < 1 or failures or errors_count:
                errors.append(
                    "ClickHouse JUnit evidence must contain passing collected tests"
                )
    return errors


def validate_index_data(
    data: Mapping[str, Any], expected: Mapping[str, str]
) -> list[str]:
    """Validate PyPI/TestPyPI JSON data against candidate files."""
    errors: list[str] = []
    info = data.get("info")
    if not isinstance(info, Mapping):
        errors.append("index response is missing project metadata")
    else:
        name = info.get("name")
        normalized_name = (
            re.sub(r"[-_.]+", "-", name).lower() if isinstance(name, str) else None
        )
        if normalized_name != PROJECT:
            errors.append(f"index project name is {name!r}; expected {PROJECT!r}")
        if info.get("version") != VERSION:
            errors.append(
                f"index current version is {info.get('version')!r}; "
                f"expected {VERSION!r}"
            )
    releases = data.get("releases")
    if not isinstance(releases, Mapping):
        return ["index response is missing releases"]
    files = releases.get(VERSION)
    if not isinstance(files, Sequence) or isinstance(files, (str, bytes)):
        return [f"index response is missing release {VERSION}"]
    observed: dict[str, str] = {}
    for item in files:
        if not isinstance(item, Mapping):
            continue
        filename = item.get("filename")
        digests = item.get("digests")
        if isinstance(filename, str) and isinstance(digests, Mapping):
            sha256 = digests.get("sha256")
            if isinstance(sha256, str):
                if filename in observed:
                    errors.append(f"index contains duplicate release file: {filename}")
                observed[filename] = sha256
    if observed != dict(expected):
        errors.append(
            f"index file identity is {observed!r}; expected {dict(expected)!r}"
        )
    return errors


def create_candidate_record(
    *,
    candidate_sha: str,
    source_run_id: int,
    distribution_id: int,
    distribution_digest: str,
    distribution_url: str,
    evidence_id: int,
    evidence_digest: str,
    evidence_url: str,
    files: Mapping[str, str],
    file_sizes: Mapping[str, int],
) -> dict[str, Any]:
    """Create a validated release candidate descriptor."""
    record: dict[str, Any] = {
        "schema_version": 1,
        "repository": REPOSITORY,
        "candidate_sha": candidate_sha,
        "source_run_id": source_run_id,
        "distribution": {
            "artifact_id": distribution_id,
            "artifact_digest": distribution_digest,
            "artifact_url": distribution_url,
            "files": dict(sorted(files.items())),
            "file_sizes": dict(sorted(file_sizes.items())),
        },
        "clickhouse_evidence": {
            "artifact_id": evidence_id,
            "artifact_digest": evidence_digest,
            "artifact_url": evidence_url,
        },
    }
    validate_candidate_record(record)
    return record


def validate_candidate_record(record: Mapping[str, Any]) -> None:
    """Raise ValueError if a candidate descriptor is malformed."""
    if record.get("schema_version") != 1 or record.get("repository") != REPOSITORY:
        raise ValueError("candidate record schema or repository is invalid")
    _full_sha(record.get("candidate_sha"), name="candidate_sha")
    _positive_int(record.get("source_run_id"), name="source_run_id")
    distribution = record.get("distribution")
    evidence = record.get("clickhouse_evidence")
    if not isinstance(distribution, Mapping) or not isinstance(evidence, Mapping):
        raise ValueError("candidate record is missing artifact identities")
    distribution_id = _positive_int(
        distribution.get("artifact_id"), name="distribution artifact_id"
    )
    _digest(distribution.get("artifact_digest"), name="distribution artifact_digest")
    _artifact_url(
        distribution.get("artifact_url"),
        artifact_id=distribution_id,
        name="distribution",
    )
    files = distribution.get("files")
    if not isinstance(files, Mapping):
        raise ValueError("candidate record is missing distribution files")
    parse_sha256sums("\n".join(f"{digest}  {name}" for name, digest in files.items()))
    file_sizes = distribution.get("file_sizes")
    if not isinstance(file_sizes, Mapping) or set(file_sizes) != set(files):
        raise ValueError("candidate record distribution file sizes are incomplete")
    for name, size in file_sizes.items():
        _positive_int(size, name=f"size for {name}")
    evidence_id = _positive_int(
        evidence.get("artifact_id"), name="evidence artifact_id"
    )
    _digest(evidence.get("artifact_digest"), name="evidence artifact_digest")
    _artifact_url(
        evidence.get("artifact_url"), artifact_id=evidence_id, name="evidence"
    )


def _validate_external_state(
    operation: str,
    state: Mapping[str, Any],
    *,
    candidate_sha: str,
    files: Mapping[str, str],
) -> None:
    if state.get("operation") != operation:
        raise ValueError("external state operation does not match receipt operation")
    if operation == "dry-run":
        if state != {
            "operation": "dry-run",
            "pypi": "absent",
            "testpypi": "absent",
            "tag": "absent",
        }:
            raise ValueError("dry-run external state is incomplete")
        return
    if operation in {"testpypi", "pypi"}:
        expected_url = (
            "https://test.pypi.org/pypi/ray-clickhouse/json"
            if operation == "testpypi"
            else "https://pypi.org/pypi/ray-clickhouse/json"
        )
        if (
            state.get("index_url") != expected_url
            or state.get("project") != PROJECT
            or state.get("version") != VERSION
            or state.get("files") != dict(files)
        ):
            raise ValueError(f"{operation} external file identity is incomplete")
        return
    if operation == "release-tag":
        if state.get("tag") != TAG or state.get("tag_type") != "annotated":
            raise ValueError("release-tag external state is invalid")
        if state.get("target") != candidate_sha:
            raise ValueError("release-tag target does not match candidate SHA")
        return
    assets = state.get("assets")
    if (
        state.get("tag") != TAG
        or state.get("target") != candidate_sha
        or not isinstance(assets, Mapping)
        or any(assets.get(name) != digest for name, digest in files.items())
        or set(assets) != {*files, "SHA256SUMS"}
        or re.fullmatch(r"[0-9a-f]{64}", str(assets.get("SHA256SUMS"))) is None
    ):
        raise ValueError("GitHub Release external asset identity is incomplete")


def create_promotion_receipt(
    *,
    operation: str,
    run_id: int,
    candidate: Mapping[str, Any],
    candidate_record_id: int,
    candidate_record_digest: str,
    predecessor: Mapping[str, Any] | None,
    external_state: Mapping[str, Any],
    result: str,
) -> dict[str, Any]:
    """Create one validated promotion-state receipt."""
    validate_candidate_record(candidate)
    receipt: dict[str, Any] = {
        "schema_version": 1,
        "repository": REPOSITORY,
        "operation": operation,
        "run_id": run_id,
        "candidate_sha": candidate["candidate_sha"],
        "source_run_id": candidate["source_run_id"],
        "candidate_record": {
            "artifact_id": candidate_record_id,
            "artifact_digest": candidate_record_digest,
        },
        "predecessor": predecessor,
        "distribution": candidate["distribution"],
        "clickhouse_evidence": candidate["clickhouse_evidence"],
        "external_state": dict(external_state),
        "result": result,
    }
    validate_promotion_receipt(receipt)
    return receipt


def validate_promotion_receipt(receipt: Mapping[str, Any]) -> None:
    """Raise ValueError if a promotion receipt is malformed or out of order."""
    if receipt.get("schema_version") != 1 or receipt.get("repository") != REPOSITORY:
        raise ValueError("promotion receipt schema or repository is invalid")
    operation = receipt.get("operation")
    if operation not in OPERATIONS:
        raise ValueError("promotion receipt operation is invalid")
    _positive_int(receipt.get("run_id"), name="promotion run_id")
    _positive_int(receipt.get("source_run_id"), name="promotion source_run_id")
    candidate_sha = _full_sha(
        receipt.get("candidate_sha"), name="promotion candidate_sha"
    )
    candidate_record = receipt.get("candidate_record")
    if not isinstance(candidate_record, Mapping):
        raise ValueError("promotion receipt candidate_record is invalid")
    _positive_int(candidate_record.get("artifact_id"), name="candidate record id")
    _digest(candidate_record.get("artifact_digest"), name="candidate record digest")
    predecessor = receipt.get("predecessor")
    expected = PREDECESSOR.get(str(operation))
    if expected is None:
        if predecessor is not None:
            raise ValueError("dry-run receipt must not have a predecessor")
    else:
        if not isinstance(predecessor, Mapping):
            raise ValueError(f"{operation} receipt requires predecessor {expected}")
        if predecessor.get("operation") != expected:
            raise ValueError(f"{operation} predecessor must be {expected}")
        _positive_int(predecessor.get("run_id"), name="predecessor run_id")
        _positive_int(predecessor.get("artifact_id"), name="predecessor artifact_id")
        _digest(predecessor.get("artifact_digest"), name="predecessor artifact_digest")
    if receipt.get("result") not in {"success", "recovered"}:
        raise ValueError("promotion receipt result must be success or recovered")
    distribution = receipt.get("distribution")
    evidence = receipt.get("clickhouse_evidence")
    candidate = {
        "schema_version": 1,
        "repository": REPOSITORY,
        "candidate_sha": candidate_sha,
        "source_run_id": receipt["source_run_id"],
        "distribution": distribution,
        "clickhouse_evidence": evidence,
    }
    validate_candidate_record(candidate)
    external_state = receipt.get("external_state")
    if not isinstance(external_state, Mapping):
        raise ValueError("promotion receipt external_state is invalid")
    if not isinstance(distribution, Mapping) or not isinstance(
        distribution.get("files"), Mapping
    ):
        raise ValueError("promotion receipt distribution identity is invalid")
    _validate_external_state(
        str(operation),
        external_state,
        candidate_sha=candidate_sha,
        files=distribution["files"],
    )


def validate_receipt_against_candidate(
    receipt: Mapping[str, Any],
    candidate: Mapping[str, Any],
    *,
    candidate_record_id: int,
    candidate_record_digest: str,
    expected_run_id: int | None = None,
) -> None:
    """Validate that a receipt belongs to one exact candidate descriptor."""
    validate_promotion_receipt(receipt)
    validate_candidate_record(candidate)
    if expected_run_id is not None and receipt.get("run_id") != expected_run_id:
        raise ValueError("promotion receipt run ID does not match workflow run")
    expected = {
        "candidate_sha": candidate["candidate_sha"],
        "source_run_id": candidate["source_run_id"],
        "distribution": candidate["distribution"],
        "clickhouse_evidence": candidate["clickhouse_evidence"],
    }
    for field, value in expected.items():
        if receipt.get(field) != value:
            raise ValueError(f"promotion receipt {field} does not match candidate")
    candidate_record = receipt.get("candidate_record")
    if not isinstance(candidate_record, Mapping):
        raise ValueError("promotion receipt candidate record is invalid")
    if (
        candidate_record.get("artifact_id") != candidate_record_id
        or candidate_record.get("artifact_digest") != candidate_record_digest
    ):
        raise ValueError("promotion receipt candidate record identity does not match")


def validate_release_texts(
    ci_workflow: str,
    release_workflow: str,
    pyproject: str,
    release_notes: str,
) -> list[str]:
    """Return release metadata and workflow policy errors."""
    errors: list[str] = []
    pyproject_fragments = (
        'version = "0.1.0"',
        'license = "Apache-2.0"',
        'license-files = ["LICENSE"]',
        '"Typing :: Typed"',
        "[project.urls]",
        'Homepage = "https://github.com/jiangxt2/ray-clickhouse"',
        'Documentation = "https://github.com/jiangxt2/ray-clickhouse/tree/master/doc"',
        'Changelog = "https://github.com/jiangxt2/ray-clickhouse/tree/master/release-notes"',
        "[dependency-groups]",
        "docs = [",
        'files = ["src/ray_clickhouse", "tools"]',
    )
    for fragment in pyproject_fragments:
        if fragment not in pyproject:
            errors.append(f"pyproject is missing release metadata: {fragment!r}")
    if "Maturity: Alpha." not in release_notes:
        errors.append("release notes must state the permanent Alpha maturity")
    for phrase in ("not yet published", "release candidate"):
        if phrase in release_notes.lower():
            errors.append(
                f"release notes contain temporary publication state: {phrase!r}"
            )

    options_match = re.search(
        r"(?m)^      operation:\n(?:^        .*\n)*?^        options:\n"
        r"(?P<options>(?:^          - [a-z-]+\n)+)",
        release_workflow,
    )
    options = (
        tuple(
            line.removeprefix("          - ")
            for line in options_match.group("options").splitlines()
        )
        if options_match is not None
        else ()
    )
    if options != OPERATIONS:
        errors.append(
            f"release workflow operation choices are {options!r}; "
            f"expected {OPERATIONS!r}"
        )

    operation_jobs = re.findall(
        r"(?m)^  (dry-run|testpypi|release-tag|pypi|github-release):$",
        release_workflow,
    )
    if tuple(operation_jobs) != OPERATIONS:
        errors.append("release workflow must define each operation job exactly once")

    for operation in OPERATIONS:
        if f"- {operation}" not in release_workflow:
            errors.append(f"release workflow is missing operation choice: {operation}")
        if f"inputs.operation == '{operation}'" not in release_workflow:
            errors.append(
                f"release workflow is missing exclusive job condition: {operation}"
            )
    release_fragments = (
        "type: choice",
        "source_run_id:",
        "candidate_record_artifact_id:",
        "candidate_record_artifact_digest:",
        "predecessor_run_id:",
        "predecessor_receipt_artifact_id:",
        "predecessor_receipt_artifact_digest:",
        "--predecessor-run-id",
        "--expected-run-id",
        "environment: release-tag",
        "environment: testpypi",
        "environment: pypi",
        "environment: github-release",
        "digest-mismatch: error",
        "artifact-ids:",
        "run-id:",
        "github-token:",
        "gh attestation verify",
        "--signer-workflow jiangxt2/ray-clickhouse/.github/workflows/ci.yml",
        '--source-digest "${CANDIDATE_SHA}"',
        "--source-ref refs/heads/master",
        "--no-deps",
        "refs/tags/v0.1.0^{}",
        "promotion-receipt.json",
        "Record promotion receipt identity",
        "workflow_dispatch",
        ".github/workflows/release.yml",
        "candidate-source/tools/check_release.py",
        "verify-clickhouse-evidence",
        ".head_repository.full_name",
        'test "$(jq -r \'.head_branch\' predecessor-run.json)" = "master"',
        "source-jobs.json",
        "predecessor-jobs.json",
        'validate_job="Validate immutable candidate and predecessor"',
        'expected_job="Dry-run release validation"',
        'expected_job="Publish and smoke TestPyPI"',
        'expected_job="Create annotated release tag"',
        'expected_job="Publish PyPI"',
        "operation_jobs=(",
        '.conclusion == "success")] | length == 0',
        'name="promotion-receipt-${expected}"',
    )
    for fragment in release_fragments:
        if fragment not in release_workflow:
            errors.append(
                f"release workflow is missing required fragment: {fragment!r}"
            )
    download_count = release_workflow.count("uses: actions/download-artifact@")
    if release_workflow.count("digest-mismatch: error") != download_count:
        errors.append("every release artifact download must use digest-mismatch: error")
    if release_workflow.count("artifact-ids:") != download_count:
        errors.append(
            "every release artifact download must use an immutable artifact ID"
        )
    for forbidden in (
        "artifact-metadata: write",
        "push-to-registry",
        "create-storage-record",
    ):
        for label, workflow in (("CI", ci_workflow), ("release", release_workflow)):
            if forbidden in workflow:
                errors.append(
                    f"{label} workflow contains forbidden fragment: {forbidden!r}"
                )
    for forbidden in ("python -m build", "uv build"):
        if forbidden in release_workflow:
            errors.append(
                f"release workflow contains forbidden fragment: {forbidden!r}"
            )

    ci_fragments = (
        "candidate-record:",
        "needs: [candidate, unit, quality, docs, docs-linkcheck, "
        "clickhouse-it, package-build, package-smoke]",
        "SHA256SUMS",
        "release-candidate.json",
        "artifact-id",
        "artifact-digest",
        "actions/attest@",
        "subject-checksums: candidate-distributions/SHA256SUMS",
        "id-token: write",
        "attestations: write",
        "if-no-files-found: error",
    )
    for fragment in ci_fragments:
        if fragment not in ci_workflow:
            errors.append(
                f"CI workflow is missing candidate producer fragment: {fragment!r}"
            )

    for label, workflow in (("CI", ci_workflow), ("release", release_workflow)):
        for target in _USES.findall(workflow):
            if target.startswith("./"):
                continue
            if (
                "@" not in target
                or re.fullmatch(r"[0-9a-f]{40}", target.rsplit("@", 1)[1]) is None
            ):
                errors.append(
                    f"{label} workflow action is not pinned to a full SHA: {target!r}"
                )

    expected_environments = {
        "dry-run": None,
        "testpypi": "testpypi",
        "release-tag": "release-tag",
        "pypi": "pypi",
        "github-release": "github-release",
    }
    for index, operation in enumerate(OPERATIONS):
        start = release_workflow.find(f"  {operation}:\n")
        next_starts = [
            release_workflow.find(f"  {later}:\n", start + 1)
            for later in OPERATIONS[index + 1 :]
        ]
        valid_next_starts = [position for position in next_starts if position >= 0]
        end = min(valid_next_starts) if valid_next_starts else len(release_workflow)
        body = release_workflow[start:end] if start >= 0 else ""
        required = (
            "needs: validate",
            "ref: ${{ needs.validate.outputs.candidate-sha }}",
            "create-promotion-receipt",
            f"name: promotion-receipt-{operation}",
            "id: upload-receipt",
            "Record promotion receipt identity",
        )
        for fragment in required:
            if fragment not in body:
                errors.append(
                    f"release {operation} job is missing required fragment: "
                    f"{fragment!r}"
                )
        if (
            re.search(
                rf"(?m)^    if: inputs\.operation == '{re.escape(operation)}'$", body
            )
            is None
        ):
            errors.append(
                f"release {operation} job must have one exact operation condition"
            )
        environment = expected_environments[operation]
        if environment is None:
            if re.search(r"(?m)^    environment:", body):
                errors.append("release dry-run job must not use an environment")
        elif f"environment: {environment}" not in body:
            errors.append(
                f"release {operation} job must use environment {environment!r}"
            )
    return errors


def run_checks(root: Path = REPOSITORY_ROOT) -> list[str]:
    """Read repository release inputs and return policy errors."""
    return validate_release_texts(
        (root / ".github/workflows/ci.yml").read_text(encoding="utf-8"),
        (root / ".github/workflows/release.yml").read_text(encoding="utf-8"),
        (root / "pyproject.toml").read_text(encoding="utf-8"),
        (root / "release-notes/v0.1.0.md").read_text(encoding="utf-8"),
    )


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _read_json(path: Path) -> Mapping[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("check")

    candidate = subparsers.add_parser("create-candidate-record")
    candidate.add_argument("--output", type=Path, required=True)
    candidate.add_argument("--candidate-sha", required=True)
    candidate.add_argument("--source-run-id", type=int, required=True)
    candidate.add_argument("--distribution-id", type=int, required=True)
    candidate.add_argument("--distribution-digest", required=True)
    candidate.add_argument("--distribution-url", required=True)
    candidate.add_argument("--evidence-id", type=int, required=True)
    candidate.add_argument("--evidence-digest", required=True)
    candidate.add_argument("--evidence-url", required=True)
    candidate.add_argument("--sha256sums", type=Path, required=True)

    verify_candidate = subparsers.add_parser("verify-candidate-record")
    verify_candidate.add_argument("--record", type=Path, required=True)
    verify_candidate.add_argument("--candidate-sha")
    verify_candidate.add_argument("--source-run-id", type=int)

    receipt = subparsers.add_parser("create-promotion-receipt")
    receipt.add_argument("--output", type=Path, required=True)
    receipt.add_argument("--operation", choices=OPERATIONS, required=True)
    receipt.add_argument("--run-id", type=int, required=True)
    receipt.add_argument("--candidate", type=Path, required=True)
    receipt.add_argument("--candidate-record-id", type=int, required=True)
    receipt.add_argument("--candidate-record-digest", required=True)
    receipt.add_argument("--predecessor", type=Path)
    receipt.add_argument("--predecessor-run-id", type=int)
    receipt.add_argument("--predecessor-artifact-id", type=int)
    receipt.add_argument("--predecessor-artifact-digest")
    receipt.add_argument("--external-state", type=Path, required=True)
    receipt.add_argument("--result", choices=("success", "recovered"), required=True)

    verify_receipt = subparsers.add_parser("verify-promotion-receipt")
    verify_receipt.add_argument("--receipt", type=Path, required=True)
    verify_receipt.add_argument(
        "--expected-operation", choices=OPERATIONS, required=True
    )
    verify_receipt.add_argument("--candidate", type=Path, required=True)
    verify_receipt.add_argument("--candidate-record-id", type=int, required=True)
    verify_receipt.add_argument("--candidate-record-digest", required=True)
    verify_receipt.add_argument("--expected-run-id", type=int, required=True)

    verify_files_parser = subparsers.add_parser("verify-files")
    verify_files_parser.add_argument("--directory", type=Path, required=True)
    verify_files_parser.add_argument("--sha256sums", type=Path, required=True)
    verify_files_parser.add_argument("--candidate", type=Path)

    verify_evidence = subparsers.add_parser("verify-clickhouse-evidence")
    verify_evidence.add_argument("--directory", type=Path, required=True)

    verify_index = subparsers.add_parser("verify-index")
    verify_index.add_argument("--json-url", required=True)
    verify_index.add_argument("--sha256sums", type=Path, required=True)
    verify_index.add_argument("--output", type=Path)

    github_release = subparsers.add_parser("record-github-release-state")
    github_release.add_argument("--directory", type=Path, required=True)
    github_release.add_argument("--sha256sums", type=Path, required=True)
    github_release.add_argument("--candidate-sha", required=True)
    github_release.add_argument("--output", type=Path, required=True)
    return parser


def _run_command(args: argparse.Namespace) -> list[str]:
    if args.command == "check":
        return run_checks()
    if args.command == "create-candidate-record":
        created_candidate = create_candidate_record(
            candidate_sha=args.candidate_sha,
            source_run_id=args.source_run_id,
            distribution_id=args.distribution_id,
            distribution_digest=args.distribution_digest,
            distribution_url=args.distribution_url,
            evidence_id=args.evidence_id,
            evidence_digest=args.evidence_digest,
            evidence_url=args.evidence_url,
            files=(files := sha256sums_from_file(args.sha256sums)),
            file_sizes=release_file_sizes(args.sha256sums.parent, files),
        )
        _write_json(args.output, created_candidate)
        return []
    if args.command == "verify-candidate-record":
        candidate_record = _read_json(args.record)
        validate_candidate_record(candidate_record)
        if (
            args.candidate_sha is not None
            and candidate_record["candidate_sha"] != args.candidate_sha
        ):
            return ["candidate record SHA does not match expected SHA"]
        if (
            args.source_run_id is not None
            and candidate_record["source_run_id"] != args.source_run_id
        ):
            return ["candidate record run ID does not match expected run ID"]
        return []
    if args.command == "create-promotion-receipt":
        candidate = _read_json(args.candidate)
        predecessor_value: Mapping[str, Any] | None = None
        if args.predecessor is not None:
            predecessor = _read_json(args.predecessor)
            validate_promotion_receipt(predecessor)
            if (
                args.predecessor_run_id is None
                or args.predecessor_artifact_id is None
                or args.predecessor_artifact_digest is None
            ):
                return ["predecessor artifact identity is required"]
            validate_receipt_against_candidate(
                predecessor,
                candidate,
                candidate_record_id=args.candidate_record_id,
                candidate_record_digest=args.candidate_record_digest,
                expected_run_id=args.predecessor_run_id,
            )
            predecessor_value = {
                "operation": predecessor["operation"],
                "run_id": predecessor["run_id"],
                "artifact_id": args.predecessor_artifact_id,
                "artifact_digest": args.predecessor_artifact_digest,
            }
        created_receipt = create_promotion_receipt(
            operation=args.operation,
            run_id=args.run_id,
            candidate=candidate,
            candidate_record_id=args.candidate_record_id,
            candidate_record_digest=args.candidate_record_digest,
            predecessor=predecessor_value,
            external_state=_read_json(args.external_state),
            result=args.result,
        )
        _write_json(args.output, created_receipt)
        return []
    if args.command == "verify-promotion-receipt":
        verified_receipt = _read_json(args.receipt)
        candidate = _read_json(args.candidate)
        validate_receipt_against_candidate(
            verified_receipt,
            candidate,
            candidate_record_id=args.candidate_record_id,
            candidate_record_digest=args.candidate_record_digest,
            expected_run_id=args.expected_run_id,
        )
        errors: list[str] = []
        if verified_receipt["operation"] != args.expected_operation:
            errors.append("promotion receipt operation does not match")
        return errors
    if args.command == "verify-files":
        expected = sha256sums_from_file(args.sha256sums)
        expected_sizes: Mapping[str, int] | None = None
        if args.candidate is not None:
            candidate = _read_json(args.candidate)
            validate_candidate_record(candidate)
            distribution = candidate["distribution"]
            if distribution["files"] != expected:
                return ["candidate record files do not match SHA256SUMS"]
            expected_sizes = distribution["file_sizes"]
        return verify_files(args.directory, expected, expected_sizes)
    if args.command == "verify-clickhouse-evidence":
        return verify_clickhouse_evidence(args.directory)
    if args.command == "verify-index":
        if args.json_url not in _INDEX_URLS:
            return ["index URL is not an approved PyPI endpoint"]
        with urllib.request.urlopen(args.json_url, timeout=30) as response:
            data = json.load(response)
        if not isinstance(data, Mapping):
            return ["index response must be a JSON object"]
        errors = validate_index_data(data, sha256sums_from_file(args.sha256sums))
        if not errors and args.output is not None:
            _write_json(
                args.output,
                {
                    "operation": (
                        "testpypi" if "test.pypi.org" in args.json_url else "pypi"
                    ),
                    "index_url": args.json_url,
                    "project": PROJECT,
                    "version": VERSION,
                    "files": sha256sums_from_file(args.sha256sums),
                },
            )
        return errors
    if args.command == "record-github-release-state":
        candidate_sha = _full_sha(args.candidate_sha, name="candidate_sha")
        expected = sha256sums_from_file(args.sha256sums)
        errors = verify_files(args.directory, expected)
        downloaded_sums = args.directory / "SHA256SUMS"
        if not downloaded_sums.is_file():
            errors.append("GitHub Release is missing SHA256SUMS")
        elif downloaded_sums.read_bytes() != args.sha256sums.read_bytes():
            errors.append("GitHub Release SHA256SUMS does not match candidate")
        if errors:
            return errors
        assets = dict(expected)
        assets["SHA256SUMS"] = hashlib.sha256(downloaded_sums.read_bytes()).hexdigest()
        _write_json(
            args.output,
            {
                "operation": "github-release",
                "tag": TAG,
                "target": candidate_sha,
                "assets": dict(sorted(assets.items())),
            },
        )
        return []
    return [f"unknown command: {args.command}"]


def main() -> int:
    try:
        errors = _run_command(_parser().parse_args())
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("Release checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
