from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

from tools.check_release import (
    OPERATIONS,
    PREDECESSOR,
    create_candidate_record,
    create_promotion_receipt,
    parse_sha256sums,
    run_checks,
    validate_candidate_record,
    validate_index_data,
    validate_promotion_receipt,
    validate_receipt_against_candidate,
    validate_release_texts,
    verify_clickhouse_evidence,
    verify_files,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SHA = "a" * 40
DIGEST = "sha256:" + "b" * 64
WHEEL = "ray_clickhouse-0.1.0-py3-none-any.whl"
SDIST = "ray_clickhouse-0.1.0.tar.gz"
FILES = {WHEEL: "c" * 64, SDIST: "d" * 64}
FILE_SIZES = {WHEEL: 100, SDIST: 200}


def _repository_texts() -> tuple[str, str, str, str]:
    return (
        (REPOSITORY_ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"),
        (REPOSITORY_ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8"),
        (REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8"),
        (REPOSITORY_ROOT / "release-notes/v0.1.0.md").read_text(encoding="utf-8"),
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
            "index_url": f"https://{host}/pypi/ray-clickhouse/json",
            "project": "ray-clickhouse",
            "version": "0.1.0",
            "files": FILES,
        }
    if operation == "release-tag":
        return {
            "operation": operation,
            "tag": "v0.1.0",
            "tag_type": "annotated",
            "target": SHA,
        }
    return {
        "operation": operation,
        "tag": "v0.1.0",
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


def _release_cli(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(REPOSITORY_ROOT / "tools/check_release.py"), *arguments],
        cwd=REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def test_repository_release_contracts_match() -> None:
    assert run_checks() == []


def test_sha256sums_requires_one_wheel_and_one_source_archive() -> None:
    text = f"{FILES[WHEEL]}  {WHEEL}\n{FILES[SDIST]}  {SDIST}\n"

    assert parse_sha256sums(text) == FILES

    with pytest.raises(ValueError, match="one wheel"):
        parse_sha256sums(f"{FILES[WHEEL]}  {WHEEL}\n")

    with pytest.raises(ValueError, match="ray-clickhouse 0.1.0"):
        parse_sha256sums(
            f"{FILES[WHEEL]}  other-0.1.0-py3-none-any.whl\n"
            f"{FILES[SDIST]}  other-0.1.0.tar.gz\n"
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
        "info": {"name": "ray-clickhouse", "version": "0.1.0"},
        "releases": {
            "0.1.0": [
                {"filename": name, "digests": {"sha256": digest}}
                for name, digest in FILES.items()
            ]
        },
    }

    assert validate_index_data(data, FILES) == []

    data["releases"]["0.1.0"][0]["digests"]["sha256"] = "0" * 64
    assert validate_index_data(data, FILES)


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
        (1, "- github-release", "- publish-all", "github-release"),
        (
            1,
            "inputs.operation == 'pypi'",
            "inputs.operation != 'dry-run'",
            "exclusive job condition",
        ),
        (1, "digest-mismatch: error", "digest-mismatch: warn", "digest-mismatch"),
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
            "Maturity: Alpha.",
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
