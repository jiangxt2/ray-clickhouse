from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pytest

from ray_clickhouse_comparison.evidence import (
    artifact_record,
    atomic_write_json,
    build_context_manifest,
    collect_case_evidence,
    load_schema,
    pip_report_identity,
    read_jsonl,
    sanitize_tree,
    sanitize_value,
    sensitive_findings,
    validate_complete_tree,
    validate_document,
    verify_wheel_sources,
    wheel_identity,
    write_checksums,
    write_jsonl,
)

ROOT = Path(__file__).resolve().parents[1]
ZERO = "0" * 64


def _result() -> dict[str, object]:
    return {
        "schema_version": 1,
        "run_id": "run-20260901",
        "scenario_id": "read.default.single",
        "side": "official",
        "repetition": 0,
        "status": "valid",
        "reason": None,
        "correctness": {"row_count": 1, "schema_sha256": ZERO, "multiset_sha256": ZERO},
        "metrics": {"duration_seconds": 1.0, "query_count": 0},
        "exception": None,
    }


def test_result_schema_accepts_valid_row_and_rejects_extra_field() -> None:
    schema = load_schema(ROOT / "schema/result.schema.json")
    validate_document(_result(), schema)
    invalid = {**_result(), "unexpected": True}
    with pytest.raises(ValueError, match="Additional properties"):
        validate_document(invalid, schema)


def test_sanitizer_redacts_sensitive_keys_dsn_and_host_paths() -> None:
    sanitized, classes = sanitize_value(
        {
            "password": "value",
            "message": "clickhouse+http://user:pass@host/db /Users/alice/project",
        }
    )
    payload = json.dumps(sanitized)
    assert sanitized["password"] == "<redacted>"
    assert "user:pass" not in payload
    assert "/Users/alice" not in payload
    assert set(classes) == {"dsn_credentials", "endpoints", "host_paths", "sensitive_keys"}
    assert sensitive_findings(payload) == ()


def test_jsonl_checksums_and_artifact_record_are_deterministic(tmp_path: Path) -> None:
    results = tmp_path / "results.jsonl"
    write_jsonl(results, [_result(), _result()])
    assert read_jsonl(results) == (_result(), _result())
    record = artifact_record(results, root=tmp_path)
    assert record["name"] == "results.jsonl"
    assert record["size"] == results.stat().st_size
    checksums = tmp_path / "SHA256SUMS"
    write_checksums(checksums, [results], root=tmp_path)
    assert str(record["sha256"]) in checksums.read_text(encoding="utf-8")


def test_atomic_json_has_stable_format(tmp_path: Path) -> None:
    path = tmp_path / "value.json"
    atomic_write_json(path, {"b": 2, "a": 1})
    assert path.read_text(encoding="utf-8") == '{\n  "a": 1,\n  "b": 2\n}\n'


def test_sanitize_tree_publishes_text_and_skips_binary(tmp_path: Path) -> None:
    source = tmp_path / "raw"
    destination = tmp_path / "sanitized"
    case = source / "case-a"
    case.mkdir(parents=True)
    case.joinpath("ray-job.log").write_text(
        "dsn=clickhouse+http://user:pass@host/db /home/runner/work/project/file\n",
        encoding="utf-8",
    )
    case.joinpath("ray-worker-1-metrics.prom").write_bytes(b"\xff\xfe")

    report = sanitize_tree(source, destination)

    assert report["skipped_binary_files"] == ["case-a/ray-worker-1-metrics.prom"]
    assert "user:pass" not in destination.joinpath("case-a/ray-job.log").read_text(encoding="utf-8")
    assert destination.joinpath("redaction-report.json").is_file()
    assert destination.joinpath("SHA256SUMS").is_file()
    with pytest.raises(ValueError, match="non-UTF-8"):
        sanitize_tree(source, tmp_path / "sanitized-complete", require_complete=True)


def test_complete_validation_and_sanitizer_reject_symlink_directories(tmp_path: Path) -> None:
    source = tmp_path / "raw"
    source.mkdir()
    target = tmp_path / "target"
    target.mkdir()
    source.joinpath("case-link").symlink_to(target, target_is_directory=True)

    with pytest.raises(ValueError, match="symbolic link"):
        validate_complete_tree(
            source,
            mode="smoke",
            scenarios_path=ROOT / "config/scenarios.toml",
        )
    with pytest.raises(ValueError, match="symbolic link"):
        sanitize_tree(source, tmp_path / "sanitized")


def test_sanitize_tree_rejects_unknown_files_and_json_fields(tmp_path: Path) -> None:
    source = tmp_path / "raw"
    source.joinpath("case-a").mkdir(parents=True)
    source.joinpath("case-a/unknown.log").write_text("safe\n", encoding="utf-8")
    with pytest.raises(ValueError, match="publication allowlist"):
        sanitize_tree(source, tmp_path / "sanitized-unknown")

    source.joinpath("case-a/unknown.log").unlink()
    source.joinpath("case-a/result.json").write_text(
        json.dumps({**_result(), "unexpected": True}), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="JSON fields"):
        sanitize_tree(source, tmp_path / "sanitized-fields")

    source.joinpath("case-a/result.json").write_text(
        json.dumps({**_result(), "metrics": {"duration_seconds": 1, "unknown_metric": 2}}),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="metrics"):
        sanitize_tree(source, tmp_path / "sanitized-nested-fields")

    source.joinpath("results.jsonl").write_text(
        json.dumps({**_result(), "metrics": {"duration_seconds": 1, "unknown_metric": 2}}) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="metrics"):
        sanitize_tree(source, tmp_path / "sanitized-jsonl-fields")


def test_complete_tree_requires_root_evidence(tmp_path: Path) -> None:
    source = tmp_path / "raw"
    source.mkdir()
    with pytest.raises(ValueError, match="missing root files"):
        validate_complete_tree(
            source,
            mode="smoke",
            scenarios_path=ROOT / "config/scenarios.toml",
        )
    with pytest.raises(ValueError, match="workflow-identity.json"):
        validate_complete_tree(
            source,
            mode="dry-run",
            scenarios_path=ROOT / "config/scenarios.toml",
        )


def test_complete_tree_requires_operational_logs_and_telemetry(tmp_path: Path) -> None:
    source = tmp_path / "raw"
    source.mkdir()
    root_files = (
        "clickhouse-pull.log",
        "docker-context-manifest.json",
        "docker-images-dangling-after.txt",
        "docker-images-dangling-before.txt",
        "docker-images-dangling-completed.txt",
        "docker-system-df-after.txt",
        "docker-system-df-before.txt",
        "docker-system-df-completed.txt",
        "external-install-report.json",
        "manifest.json",
        "official-install-report.json",
        "queries.csv",
        "ray-base-image.txt",
        "ray-base-pull.log",
        "results.jsonl",
        "runtime-image-build.log",
        "runtime-image-cleanup.log",
        "runtime-image.txt",
        "summary.json",
        "dockerfile-frontend-pull.log",
        "wheel-sha256.txt",
    )
    for name in root_files:
        source.joinpath(name).write_text(
            "{}\n" if name.endswith(".json") else "ok\n", encoding="utf-8"
        )
    source.joinpath("manifest.json").write_text(
        json.dumps({"environment": {"mode": "smoke"}}), encoding="utf-8"
    )
    cases = (
        ("official", "read.default.single", "none", True),
        ("external", "read.default.single", "none", True),
        ("external", "write.transport.post_commit", "drop_response", False),
        ("official", "write.worker.post_commit", "hold_response", False),
    )
    case_files = (
        "compose-config.yaml",
        "compose-images.txt",
        "compose.log",
        "docker-stats.jsonl",
        "driver.pid",
        "expected-identity.json",
        "measurement-complete.json",
        "measurement-started.json",
        "prepare.log",
        "process-samples.jsonl",
        "queries.jsonl",
        "ray-head-metrics-error.log",
        "ray-head-metrics.prom",
        "ray-job.log",
        "ray-metrics-samples.prom",
        "ray-ready.log",
        "ray-worker-1-metrics-error.log",
        "ray-worker-1-metrics.prom",
        "ray-worker-2-metrics-error.log",
        "ray-worker-2-metrics.prom",
        "resources.json",
        "result.json",
        "resource-baseline-ready",
        "tasks.jsonl",
    )
    result_rows = []
    for side, scenario_id, fault, warmup in cases:
        case_name = f"{side}-{scenario_id.replace('.', '-')}-{fault}-0"
        case = source / case_name
        case.mkdir()
        for name in case_files:
            case.joinpath(name).write_text("{}\n", encoding="utf-8")
        if warmup:
            case.joinpath("warmup.log").write_text("ok\n", encoding="utf-8")
        if fault != "none":
            control = case / "control"
            control.mkdir()
            control.joinpath("fault-control.json").write_text("{}\n", encoding="utf-8")
            control.joinpath("fault-event.json").write_text("{}\n", encoding="utf-8")
        if fault == "hold_response":
            case.joinpath("killed-worker.txt").write_text("worker\n", encoding="utf-8")
        result_rows.append(
            {
                "side": side,
                "scenario_id": scenario_id,
                "repetition": 0,
                "status": "valid",
                "metrics": {"fault_mode": fault},
            }
        )
    write_jsonl(source / "results.jsonl", result_rows)

    validate_complete_tree(source, mode="smoke", scenarios_path=ROOT / "config/scenarios.toml")
    (source / "official-read-default-single-none-0/ray-job.log").unlink()
    with pytest.raises(ValueError, match="ray-job.log"):
        validate_complete_tree(source, mode="smoke", scenarios_path=ROOT / "config/scenarios.toml")


def test_pip_report_identity_binds_selected_wheel_hash(tmp_path: Path) -> None:
    report = tmp_path / "pip-report.json"
    report.write_text(
        json.dumps(
            {
                "install": [
                    {
                        "metadata": {"name": "ray", "version": "2.58.0"},
                        "download_info": {
                            "url": "https://files.pythonhosted.org/ray-2.58.0-cp312.whl",
                            "archive_info": {"hashes": {"sha256": "a" * 64}},
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    assert pip_report_identity(report, "ray") == {
        "name": "ray",
        "version": "2.58.0",
        "filename": "ray-2.58.0-cp312.whl",
        "sha256": "a" * 64,
    }


def test_collect_case_evidence_validates_and_combines_results(tmp_path: Path) -> None:
    case = tmp_path / "case-a"
    case.mkdir()
    result = _result()
    result["metrics"] = {"duration_seconds": 1.0, "query_count": 1}
    atomic_write_json(case / "result.json", result)
    atomic_write_json(case / "resources.json", {"telemetry_complete": True})
    write_jsonl(
        case / "queries.jsonl",
        [
            {
                "query_id": "q1",
                "event_type": "QueryFinish",
                "role": "data",
                "read_rows": 1,
                "read_bytes": 8,
                "duration_ms": 2,
                "memory_usage": 3,
                "log_comment": "run=x",
                "query": "SELECT 1",
            }
        ],
    )
    results = tmp_path / "results.jsonl"
    queries = tmp_path / "queries.csv"

    counts = collect_case_evidence(
        tmp_path,
        results,
        queries,
        load_schema(ROOT / "schema/result.schema.json"),
    )

    assert counts == (1, 1)
    assert len(read_jsonl(results)) == 1
    assert "q1,QueryFinish,data,1,8,2,3,run=x,SELECT 1" in queries.read_text(encoding="utf-8")


def test_wheel_source_verification_rejects_stale_member(tmp_path: Path) -> None:
    source = tmp_path / "package"
    source.mkdir()
    source.joinpath("__init__.py").write_text("VALUE = 1\n", encoding="utf-8")
    wheel = tmp_path / "package-1-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("package/__init__.py", "VALUE = 1\n")
        archive.writestr(
            "package-1.dist-info/METADATA", "Metadata-Version: 2.4\nName: package\nVersion: 1\n"
        )
    verify_wheel_sources(wheel, source, "package")
    assert wheel_identity(wheel, "package") == {
        "name": "package",
        "version": "1",
        "filename": wheel.name,
        "sha256": artifact_record(wheel, root=tmp_path)["sha256"],
    }
    source.joinpath("__init__.py").write_text("VALUE = 2\n", encoding="utf-8")
    with pytest.raises(ValueError, match="differs"):
        verify_wheel_sources(wheel, source, "package")


def test_sanitize_tree_rejects_nested_or_stale_destination(tmp_path: Path) -> None:
    source = tmp_path / "raw"
    source.mkdir()
    source.joinpath("value.txt").write_text("safe\n", encoding="utf-8")
    with pytest.raises(ValueError, match="nested"):
        sanitize_tree(source, source / "sanitized")

    destination = tmp_path / "sanitized"
    destination.mkdir()
    destination.joinpath("stale.txt").write_text("stale\n", encoding="utf-8")
    with pytest.raises(ValueError, match="absent or empty"):
        sanitize_tree(source, destination)


def test_context_manifest_contains_only_actual_copy_inputs(tmp_path: Path) -> None:
    output = tmp_path / "context.json"
    manifest = build_context_manifest(ROOT.parents[1], output)
    names = {record["name"] for record in manifest["files"]}

    assert "docker/comparison/Dockerfile" in names
    assert "comparison/official/config/reference.toml" in names
    assert "comparison/official/src/ray_clickhouse_comparison/runner.py" not in names
    assert output.is_file()
