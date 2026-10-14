from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from tools import check_release
from tools.check_release import (
    OPERATIONS,
    PREDECESSOR,
    create_candidate_record,
    create_promotion_receipt,
    parse_sha256sums,
    project_version,
    run_checks,
    validate_candidate_record,
    validate_index_data,
    validate_promotion_receipt,
    validate_receipt_against_candidate,
    validate_release_texts,
    verify_clickhouse_evidence,
    verify_files,
    workflow_release_version,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SHA = "a" * 40
DIGEST = "sha256:" + "b" * 64
RELEASE_VERSION = "1.1"
WHEEL = f"ray_clickhouse-{RELEASE_VERSION}-py3-none-any.whl"
SDIST = f"ray_clickhouse-{RELEASE_VERSION}.tar.gz"
FILES = {WHEEL: "c" * 64, SDIST: "d" * 64}
FILE_SIZES = {WHEEL: 100, SDIST: 200}


@pytest.fixture(autouse=True)
def formal_release_context(monkeypatch) -> None:
    """Exercise the existing promotion contracts against a selected formal snapshot."""
    monkeypatch.setattr(check_release, "VERSION", RELEASE_VERSION)
    monkeypatch.setattr(check_release, "TAG", f"v{RELEASE_VERSION}")
    monkeypatch.setattr(
        check_release,
        "_WHEEL",
        re.compile(
            rf"^ray_clickhouse-{re.escape(RELEASE_VERSION)}-[A-Za-z0-9_.-]+\.whl$"
        ),
    )
    monkeypatch.setattr(
        check_release,
        "_INDEX_URLS",
        {
            f"https://pypi.org/pypi/ray-clickhouse/{RELEASE_VERSION}/json",
            f"https://test.pypi.org/pypi/ray-clickhouse/{RELEASE_VERSION}/json",
        },
    )


def _repository_texts() -> tuple[str, str, str, str]:
    return (
        (REPOSITORY_ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"),
        (REPOSITORY_ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8"),
        (REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8"),
        (REPOSITORY_ROOT / f"release-notes/v{RELEASE_VERSION}.md").read_text(
            encoding="utf-8"
        ),
    )


def _candidate() -> dict[str, object]:
    return create_candidate_record(
        candidate_sha=SHA,
        source_run_id=11,
        distribution_id=12,
        distribution_digest=DIGEST,
        distribution_url="https://github.com/jiangxt2/ray-clickhouse/actions/runs/11/artifacts/12",
        evidence_id=13,
        evidence_digest=DIGEST,
        evidence_url="https://github.com/jiangxt2/ray-clickhouse/actions/runs/11/artifacts/13",
        files=FILES,
        file_sizes=FILE_SIZES,
    )


def _external_state(operation: str) -> dict[str, object]:
    if operation == "dry-run":
        return {
            "operation": operation,
            "pypi": "absent",
            "testpypi": "absent",
            "tag": "absent",
        }
    if operation in {"testpypi", "pypi"}:
        host = "test.pypi.org" if operation == "testpypi" else "pypi.org"
        return {
            "operation": operation,
            "index_url": f"https://{host}/pypi/ray-clickhouse/{RELEASE_VERSION}/json",
            "project": "ray-clickhouse",
            "version": RELEASE_VERSION,
            "files": FILES,
        }
    if operation == "release-tag":
        return {
            "operation": operation,
            "tag": f"v{RELEASE_VERSION}",
            "tag_type": "annotated",
            "target": SHA,
        }
    return {
        "operation": operation,
        "tag": f"v{RELEASE_VERSION}",
        "target": SHA,
        "assets": {**FILES, "SHA256SUMS": "e" * 64},
    }


def _receipt(
    operation: str, predecessor: dict[str, object] | None
) -> dict[str, object]:
    return create_promotion_receipt(
        operation=operation,
        run_id=100 + OPERATIONS.index(operation),
        candidate=_candidate(),
        candidate_record_id=14,
        candidate_record_digest=DIGEST,
        predecessor=predecessor,
        external_state=_external_state(operation),
        result="success",
    )


def _receipt_chain(operation: str) -> dict[str, object]:
    predecessor_operation = PREDECESSOR.get(operation)
    predecessor = None
    if predecessor_operation is not None:
        predecessor_receipt = _receipt_chain(predecessor_operation)
        predecessor = {
            "operation": predecessor_receipt["operation"],
            "run_id": predecessor_receipt["run_id"],
            "artifact_id": 200 + OPERATIONS.index(operation),
            "artifact_digest": DIGEST,
        }
    return _receipt(operation, predecessor)


def _write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value) + "\n", encoding="utf-8")


def _release_cli(
    *arguments: str, version: str = RELEASE_VERSION
) -> subprocess.CompletedProcess[str]:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        (root / "tools").mkdir()
        script = root / "tools/check_release.py"
        script.write_bytes((REPOSITORY_ROOT / "tools/check_release.py").read_bytes())
        (root / "pyproject.toml").write_text(f'version = "{version}"\n')
        return subprocess.run(
            [sys.executable, str(script), *arguments],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )


def test_repository_release_contracts_match() -> None:
    assert run_checks() == []


@pytest.mark.parametrize(
    ("expected_version", "returncode"),
    (("1.1", 0), ("0.1.0", 1), ("1.0", 1)),
)
def test_candidate_cli_requires_requested_release_version(
    expected_version: str, returncode: int, tmp_path: Path
) -> None:
    record = tmp_path / "candidate.json"
    _write_json(record, _candidate())

    result = _release_cli(
        "verify-candidate-record",
        "--record",
        str(record),
        "--candidate-sha",
        SHA,
        "--source-run-id",
        "11",
        "--expected-version",
        expected_version,
    )

    assert result.returncode == returncode
    if returncode:
        assert f"expected {expected_version!r}" in result.stderr
    else:
        assert "Release checks passed." in result.stdout


@pytest.mark.parametrize("version", ("1.0", "1.1", "1.10", "2.0", "2.17"))
def test_formal_version_route_has_no_minor_ceiling(
    version: str, tmp_path: Path
) -> None:
    metadata = tmp_path / "pyproject.toml"
    metadata.write_text(f'version = "{version}"\n', encoding="utf-8")

    assert project_version(metadata) == version


@pytest.mark.parametrize(
    "version", ("1", "1.0.1", "1.1.1", "v1.0", "1.0rc1", "01.0", "1.01", "0.1")
)
def test_formal_version_route_rejects_noncanonical_versions(
    version: str, tmp_path: Path
) -> None:
    metadata = tmp_path / "pyproject.toml"
    metadata.write_text(f'version = "{version}"\n', encoding="utf-8")

    with pytest.raises(ValueError, match="major.minor"):
        project_version(metadata)


def test_formal_version_requires_static_metadata(tmp_path: Path) -> None:
    metadata = tmp_path / "pyproject.toml"
    metadata.write_text('dynamic = ["version"]\n', encoding="utf-8")

    with pytest.raises(ValueError, match="static project version"):
        project_version(metadata)


def test_development_metadata_requires_explicit_permission(tmp_path: Path) -> None:
    metadata = tmp_path / "pyproject.toml"
    metadata.write_text('version = "1.1.dev0"\n')
    with pytest.raises(ValueError, match="major.minor"):
        project_version(metadata)
    assert project_version(metadata, allow_development=True) == "1.1.dev0"


def test_formal_candidate_passes_static_policy() -> None:
    assert run_checks() == []


def test_release_version_is_derived_from_candidate_metadata() -> None:
    _, workflow, _, _ = _repository_texts()

    assert workflow_release_version(workflow, "1.1") == "1.1"
    assert workflow_release_version(workflow, "1.2.dev0") == "1.2"
    assert workflow_release_version(workflow, "1.0") == "1.0"
    assert "v1.0" not in workflow
    assert (REPOSITORY_ROOT / "release-notes/v1.0.md").is_file()


def test_release_workflow_rejects_a_static_version() -> None:
    _, workflow, _, _ = _repository_texts()
    static_workflow = workflow.replace(
        "release-version: ${{ steps.candidate-version.outputs.release-version }}",
        "release-version: 1.1",
        1,
    )

    with pytest.raises(ValueError, match="candidate-derived version"):
        workflow_release_version(static_workflow, "1.1")


def test_release_workflow_rejects_a_static_candidate_metadata_assignment() -> None:
    _, workflow, _, _ = _repository_texts()
    assignment = next(
        line
        for line in workflow.splitlines()
        if line.strip().startswith("RELEASE_VERSION=$(python3 -c")
    )
    static_workflow = workflow.replace(assignment, "          RELEASE_VERSION=1.1", 1)

    with pytest.raises(
        ValueError, match="derive its version from candidate package metadata"
    ):
        workflow_release_version(static_workflow, "1.1")


def test_release_workflow_rejects_a_hardcoded_testpypi_promotion_version() -> None:
    _, workflow, _, _ = _repository_texts()
    static_workflow = workflow.replace(
        "ray-clickhouse==${{ needs.validate.outputs.release-version }}",
        "ray-clickhouse==1.1",
        1,
    )

    with pytest.raises(ValueError, match="testpypi operation must use"):
        workflow_release_version(static_workflow, "1.1")


def test_development_source_cannot_create_candidate(monkeypatch) -> None:
    monkeypatch.setattr(check_release, "VERSION", "1.1.dev0")
    with pytest.raises(ValueError, match="development versions cannot"):
        _candidate()


def test_development_cli_emits_full_ci_version(tmp_path: Path) -> None:
    output = tmp_path / "github-output"
    result = _release_cli(
        "ci-version", "--github-output", str(output), version="1.1.dev0"
    )
    assert result.returncode == 0, result.stderr
    assert output.read_text() == "version=1.1.dev0\nformal=false\n"


def test_formal_cli_emits_release_candidate_permission(tmp_path: Path) -> None:
    output = tmp_path / "github-output"
    result = _release_cli("ci-version", "--github-output", str(output))
    assert result.returncode == 0, result.stderr
    assert output.read_text() == "version=1.1\nformal=true\n"


@pytest.mark.parametrize("version", ["1.1rc1", "1.1.dev1", "1.1.0.dev0", "01.1.dev0"])
def test_development_permission_rejects_other_version_forms(
    tmp_path: Path, version: str
) -> None:
    metadata = tmp_path / "pyproject.toml"
    metadata.write_text(f'version = "{version}"\n')
    with pytest.raises(ValueError, match="major.minor"):
        project_version(metadata, allow_development=True)


def test_development_ci_cannot_enable_formal_candidate_recording() -> None:
    sources = list(_repository_texts())
    sources[0] = sources[0].replace("needs.candidate.outputs.formal == 'true'", "true")
    assert any("formal" in error for error in validate_release_texts(*sources))


@pytest.mark.parametrize(
    ("version", "message"),
    [
        ("1.2", "release notes must identify the current project version"),
        ("1.1.dev1", "canonical major.minor"),
    ],
)
def test_static_policy_preserves_formal_identity_and_development_format(
    version: str, message: str
) -> None:
    sources = list(_repository_texts())
    sources[2], replacements = re.subn(
        r'(?m)^version = "[^"]+"$',
        f'version = "{version}"',
        sources[2],
        count=1,
    )
    assert replacements == 1
    assert any(message in error for error in validate_release_texts(*sources))


@pytest.mark.parametrize(
    "command",
    [
        "create-candidate-record",
        "verify-candidate-record",
        "create-promotion-receipt",
        "verify-promotion-receipt",
        "verify-index",
        "record-github-release-state",
    ],
)
def test_development_commands_stop_before_side_effects(
    monkeypatch, command: str
) -> None:
    import argparse

    monkeypatch.setattr(check_release, "VERSION", "1.1.dev0")
    with pytest.raises(ValueError, match="development versions cannot"):
        check_release._run_command(argparse.Namespace(command=command))


def test_sha256sums_requires_one_wheel_and_one_source_archive() -> None:
    text = f"{FILES[WHEEL]}  {WHEEL}\n{FILES[SDIST]}  {SDIST}\n"

    assert parse_sha256sums(text) == FILES

    with pytest.raises(ValueError, match="one wheel"):
        parse_sha256sums(f"{FILES[WHEEL]}  {WHEEL}\n")

    with pytest.raises(ValueError, match="ray-clickhouse 1.1"):
        parse_sha256sums(
            f"{FILES[WHEEL]}  other-1.1-py3-none-any.whl\n"
            f"{FILES[SDIST]}  other-1.1.tar.gz\n"
        )


def test_candidate_record_round_trip_contract() -> None:
    candidate = _candidate()

    validate_candidate_record(candidate)
    assert candidate["candidate_sha"] == SHA
    assert candidate["distribution"]["files"] == FILES


@pytest.mark.parametrize(
    ("field", "value", "expected"),
    (
        ("candidate_sha", "short", "full lowercase"),
        ("source_run_id", 0, "positive integer"),
        ("repository", "other/repository", "schema or repository"),
    ),
)
def test_candidate_record_rejects_invalid_identity(
    field: str, value: object, expected: str
) -> None:
    candidate = _candidate()
    candidate[field] = value

    with pytest.raises(ValueError, match=expected):
        validate_candidate_record(candidate)


def test_candidate_record_requires_exact_positive_file_sizes() -> None:
    candidate = _candidate()
    candidate["distribution"]["file_sizes"][WHEEL] = 0

    with pytest.raises(ValueError, match="size for"):
        validate_candidate_record(candidate)


def test_promotion_receipts_enforce_complete_order() -> None:
    predecessor: dict[str, object] | None = None
    for operation in OPERATIONS:
        receipt_predecessor = None
        if predecessor is not None:
            receipt_predecessor = {
                "operation": predecessor["operation"],
                "run_id": predecessor["run_id"],
                "artifact_id": 200 + OPERATIONS.index(operation),
                "artifact_digest": DIGEST,
            }
        predecessor = _receipt(operation, receipt_predecessor)
        validate_promotion_receipt(predecessor)

    assert predecessor is not None
    assert predecessor["operation"] == "github-release"


def test_promotion_receipt_rejects_skipped_predecessor() -> None:
    wrong_predecessor = {
        "operation": "dry-run",
        "run_id": 101,
        "artifact_id": 201,
        "artifact_digest": DIGEST,
    }

    with pytest.raises(ValueError, match="predecessor must be release-tag"):
        _receipt("pypi", wrong_predecessor)


def test_dry_run_receipt_rejects_predecessor() -> None:
    predecessor = {
        "operation": "dry-run",
        "run_id": 101,
        "artifact_id": 201,
        "artifact_digest": DIGEST,
    }

    with pytest.raises(ValueError, match="must not have a predecessor"):
        _receipt("dry-run", predecessor)


def test_promotion_receipt_requires_exact_external_state() -> None:
    receipt = _receipt(
        "testpypi",
        {
            "operation": "dry-run",
            "run_id": 100,
            "artifact_id": 200,
            "artifact_digest": DIGEST,
        },
    )
    receipt["external_state"] = {"operation": "testpypi"}

    with pytest.raises(ValueError, match="external file identity"):
        validate_promotion_receipt(receipt)


def test_receipt_must_match_exact_candidate_identity() -> None:
    receipt = _receipt("dry-run", None)
    candidate = _candidate()
    candidate["source_run_id"] = 99

    with pytest.raises(ValueError, match="source_run_id"):
        validate_receipt_against_candidate(
            receipt,
            candidate,
            candidate_record_id=14,
            candidate_record_digest=DIGEST,
        )


@pytest.mark.parametrize("operation", OPERATIONS)
def test_create_promotion_receipt_cli_paths(operation: str, tmp_path: Path) -> None:
    candidate_path = tmp_path / "candidate.json"
    external_state_path = tmp_path / "external-state.json"
    output_path = tmp_path / "promotion-receipt.json"
    _write_json(candidate_path, _candidate())
    _write_json(external_state_path, _external_state(operation))
    run_id = 500 + OPERATIONS.index(operation)
    arguments = [
        "create-promotion-receipt",
        "--output",
        str(output_path),
        "--operation",
        operation,
        "--run-id",
        str(run_id),
        "--candidate",
        str(candidate_path),
        "--candidate-record-id",
        "14",
        "--candidate-record-digest",
        DIGEST,
        "--external-state",
        str(external_state_path),
        "--result",
        "success",
    ]
    predecessor_operation = PREDECESSOR.get(operation)
    if predecessor_operation is not None:
        predecessor = _receipt_chain(predecessor_operation)
        predecessor_path = tmp_path / "predecessor.json"
        _write_json(predecessor_path, predecessor)
        arguments.extend(
            [
                "--predecessor",
                str(predecessor_path),
                "--predecessor-run-id",
                str(predecessor["run_id"]),
                "--predecessor-artifact-id",
                "299",
                "--predecessor-artifact-digest",
                DIGEST,
            ]
        )

    completed = _release_cli(*arguments)

    assert completed.returncode == 0, completed.stderr
    receipt = json.loads(output_path.read_text(encoding="utf-8"))
    assert receipt["operation"] == operation
    assert receipt["run_id"] == run_id


def test_verify_promotion_receipt_cli_enforces_run_id(tmp_path: Path) -> None:
    candidate_path = tmp_path / "candidate.json"
    receipt_path = tmp_path / "receipt.json"
    receipt = _receipt("dry-run", None)
    _write_json(candidate_path, _candidate())
    _write_json(receipt_path, receipt)
    arguments = [
        "verify-promotion-receipt",
        "--receipt",
        str(receipt_path),
        "--expected-operation",
        "dry-run",
        "--candidate",
        str(candidate_path),
        "--candidate-record-id",
        "14",
        "--candidate-record-digest",
        DIGEST,
    ]

    valid = _release_cli(*arguments, "--expected-run-id", "100")
    invalid = _release_cli(*arguments, "--expected-run-id", "999")

    assert valid.returncode == 0, valid.stderr
    assert invalid.returncode == 1
    assert "run ID does not match" in invalid.stderr

    with pytest.raises(ValueError, match="run ID"):
        validate_receipt_against_candidate(
            receipt,
            _candidate(),
            candidate_record_id=14,
            candidate_record_digest=DIGEST,
            expected_run_id=999,
        )


def test_verify_files_detects_missing_unexpected_and_mismatch(tmp_path: Path) -> None:
    wheel = tmp_path / WHEEL
    source = tmp_path / SDIST
    wheel.write_bytes(b"wheel")
    source.write_bytes(b"source")
    expected = {
        WHEEL: hashlib.sha256(b"wheel").hexdigest(),
        SDIST: hashlib.sha256(b"source").hexdigest(),
    }

    sizes = {WHEEL: len(b"wheel"), SDIST: len(b"source")}
    assert verify_files(tmp_path, expected, sizes) == []
    assert any(
        "size mismatch" in error
        for error in verify_files(tmp_path, expected, {**sizes, WHEEL: 99})
    )

    wheel.write_bytes(b"changed")
    (tmp_path / "unexpected.txt").write_text("unexpected", encoding="utf-8")
    errors = verify_files(tmp_path, expected)
    assert any("digest mismatch" in error for error in errors)
    assert any("unexpected release files" in error for error in errors)


def test_index_identity_requires_exact_candidate_files() -> None:
    data = {
        "info": {"name": "ray-clickhouse", "version": "1.1"},
        "urls": [
            {"filename": name, "digests": {"sha256": digest}}
            for name, digest in FILES.items()
        ],
    }

    assert validate_index_data(data, FILES) == []

    data["urls"][0]["digests"]["sha256"] = "0" * 64
    assert validate_index_data(data, FILES)


@pytest.mark.parametrize("version", ("0.1.0", "1.0"))
def test_index_identity_rejects_another_release_version(version: str) -> None:
    data = {
        "info": {"name": "ray-clickhouse", "version": version},
        "urls": [
            {"filename": name, "digests": {"sha256": digest}}
            for name, digest in FILES.items()
        ],
    }

    assert any("release version" in error for error in validate_index_data(data, FILES))


def test_index_identity_rejects_missing_version_files() -> None:
    data = {"info": {"name": "ray-clickhouse", "version": "1.1"}, "urls": []}

    assert any("file identity" in error for error in validate_index_data(data, FILES))


def test_release_index_cli_rejects_project_wide_endpoint() -> None:
    result = _release_cli(
        "verify-index",
        "--json-url",
        "https://pypi.org/pypi/ray-clickhouse/json",
        "--sha256sums",
        "unused-checksums",
    )

    assert result.returncode == 1
    assert "approved PyPI endpoint" in result.stderr


def test_clickhouse_evidence_requires_passing_junit_and_logs(tmp_path: Path) -> None:
    for name in (
        "pytest.log",
        "clickhouse.log",
        "compose-images.txt",
        "compose-image-references.txt",
        "docker-images-dangling-before.txt",
        "docker-images-dangling-after.txt",
        "docker-system-df-before.txt",
        "docker-system-df-after.txt",
    ):
        (tmp_path / name).write_text("evidence\n", encoding="utf-8")
    report = tmp_path / "pytest.xml"
    report.write_text(
        '<testsuites><testsuite tests="13" failures="0" errors="0"/></testsuites>',
        encoding="utf-8",
    )

    assert verify_clickhouse_evidence(tmp_path) == []

    report.write_text(
        '<testsuites><testsuite tests="13" failures="1" errors="0"/></testsuites>',
        encoding="utf-8",
    )
    assert any(
        "passing collected tests" in error
        for error in verify_clickhouse_evidence(tmp_path)
    )


@pytest.mark.parametrize(
    ("source_index", "old", "new", "expected"),
    (
        (0, "candidate-record:", "candidate-record-disabled:", "candidate-record"),
        (0, "subject-checksums:", "subject-path:", "subject-checksums"),
        (
            0,
            "RECORD_DIGEST: sha256:${{ steps.upload-record.outputs.artifact-digest }}",
            "RECORD_DIGEST: ${{ steps.upload-record.outputs.artifact-digest }}",
            "RECORD_DIGEST",
        ),
        (1, "- github-release", "- publish-all", "github-release"),
        (
            1,
            '--expected-version "${RELEASE_VERSION}"',
            "",
            "candidate-derived version",
        ),
        (
            1,
            '--expected-version "${RELEASE_VERSION}"',
            "--expected-version 0.1.0",
            "candidate-derived version",
        ),
        (
            1,
            "inputs.operation == 'pypi'",
            "inputs.operation != 'dry-run'",
            "exclusive job condition",
        ),
        (1, "digest-mismatch: error", "digest-mismatch: warn", "digest-mismatch"),
        (
            1,
            "RECEIPT_DIGEST: sha256:"
            "${{ steps.upload-receipt.outputs.artifact-digest }}",
            "RECEIPT_DIGEST: ${{ steps.upload-receipt.outputs.artifact-digest }}",
            "sha256 prefix",
        ),
        (
            1,
            "--source-ref refs/heads/master",
            "--source-ref refs/heads/release",
            "source-ref",
        ),
        (
            1,
            "name: promotion-receipt-pypi",
            "name: promotion-receipt-release-tag",
            "pypi job",
        ),
        (
            1,
            'test "$(jq -r \'.head_branch\' predecessor-run.json)" = "master"',
            'test "$(jq -r \'.head_branch\' predecessor-run.json)" = "release"',
            "head_branch",
        ),
        (
            1,
            "artifacts?name=promotion-receipt-${expected}",
            "artifacts?name=promotion-receipt-${other}",
            "missing required fragment",
        ),
        (
            1,
            'validate_job="Validate immutable candidate and predecessor"',
            'validate_job="Unverified validation"',
            "Validate immutable candidate",
        ),
        (
            1,
            '.conclusion == "success")] | length == 0',
            '.conclusion == "success")] | length >= 0',
            "length == 0",
        ),
        (2, 'license = "Apache-2.0"', 'license = {text = "Apache-2.0"}', "license"),
        (
            2,
            'files = ["src/ray_clickhouse", "tools"]',
            'files = ["src/ray_clickhouse"]',
            "files",
        ),
        (
            3,
            "# ray-clickhouse 1.1",
            "Status: release candidate; not yet published",
            "temporary publication state",
        ),
    ),
)
def test_release_checker_rejects_policy_drift(
    source_index: int, old: str, new: str, expected: str
) -> None:
    sources = list(_repository_texts())
    assert old in sources[source_index]
    sources[source_index] = sources[source_index].replace(old, new, 1)

    errors = validate_release_texts(*sources)

    assert any(expected in error for error in errors)


def test_predecessor_map_is_complete_and_linear() -> None:
    assert PREDECESSOR == {
        "testpypi": "dry-run",
        "release-tag": "testpypi",
        "pypi": "release-tag",
        "github-release": "pypi",
    }


@pytest.mark.parametrize("source_index", (2, 3))
def test_release_checker_rejects_current_alpha_marker(source_index: int) -> None:
    sources = list(_repository_texts())
    sources[source_index] += "\n# Alpha\n"

    assert any("Alpha marker" in error for error in validate_release_texts(*sources))


@pytest.mark.parametrize(
    "forbidden",
    ("artifact-metadata: write", "push-to-registry", "create-storage-record"),
)
def test_release_checker_rejects_forbidden_ci_capability(forbidden: str) -> None:
    ci, release, pyproject, release_notes = _repository_texts()

    errors = validate_release_texts(
        ci + f"\n# {forbidden}\n", release, pyproject, release_notes
    )

    assert any(forbidden in error for error in errors)


def test_release_checker_rejects_rebuild_in_promotion() -> None:
    ci, release, pyproject, release_notes = _repository_texts()

    errors = validate_release_texts(
        ci, release + "\n# uv build\n", pyproject, release_notes
    )

    assert any("uv build" in error for error in errors)


def test_release_checker_rejects_multiple_operation_activation() -> None:
    ci, release, pyproject, release_notes = _repository_texts()
    release = release.replace(
        "if: inputs.operation == 'github-release'",
        "if: inputs.operation == 'pypi'",
        1,
    )

    errors = validate_release_texts(ci, release, pyproject, release_notes)

    assert any("github-release job" in error for error in errors)
