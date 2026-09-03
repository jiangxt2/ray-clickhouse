"""Schema validation, sanitization, and content-addressed evidence helpers."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import tempfile
import zipfile
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from jsonschema import Draft202012Validator

_SENSITIVE_KEY = re.compile(
    r"(?:authorization|cookie|credential|password|secret|token)", re.IGNORECASE
)
_DSN_CREDENTIAL = re.compile(
    r"(?P<scheme>[a-z][a-z0-9+.-]*://)(?!<redacted>@)[^/@\s]+@", re.IGNORECASE
)
_HOST_PATH = re.compile(r"(?:/Users/[^/\s]+|/home/runner/work/[^\s]+)")
_URL_ENDPOINT = re.compile(
    r"(?P<scheme>[a-z][a-z0-9+.-]*://)(?!<redacted-endpoint>)[^\s,;]+", re.IGNORECASE
)
_IP_ENDPOINT = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}(?::\d+)?\b")
_SENSITIVE_TEXT = (
    re.compile(r"(?i)(?:password|token|secret|authorization)\s*[:=]\s*[^\s,;}]+"),
    re.compile(r"(?i)bearer\s+[a-z0-9._~+/-]+"),
)

_ALLOWED_ROOT_FILES = frozenset(
    {
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
        "orchestration.log",
        "queries.csv",
        "ray-base-image.txt",
        "ray-base-pull.log",
        "results.jsonl",
        "runtime-image-build.log",
        "runtime-image-cleanup.log",
        "runtime-image.txt",
        "runner-identity.txt",
        "summary.json",
        "dockerfile-frontend-pull.log",
        "wheel-sha256.txt",
        "workflow-identity.json",
    }
)
_ALLOWED_CASE_FILES = frozenset(
    {
        "compose-config.yaml",
        "compose-images.txt",
        "compose.log",
        "docker-stats.jsonl",
        "driver.pid",
        "expected-identity.json",
        "killed-worker.txt",
        "measurement-complete.json",
        "measurement-started.json",
        "permission-cleanup.log",
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
        "warmup.log",
    }
)
_ALLOWED_CONTROL_FILES = frozenset({"fault-control.json", "fault-event.json", "killed-worker.txt"})
_REMOTE_ROOT_FILES = frozenset(
    {"orchestration.log", "runner-identity.txt", "workflow-identity.json"}
)
_COMMON_ROOT_FILES = _ALLOWED_ROOT_FILES - _REMOTE_ROOT_FILES
_COMMON_CASE_FILES = _ALLOWED_CASE_FILES - {
    "killed-worker.txt",
    "permission-cleanup.log",
    "docker-stats.jsonl",
    "driver.pid",
    "measurement-complete.json",
    "measurement-started.json",
    "process-samples.jsonl",
    "ray-head-metrics-error.log",
    "ray-head-metrics.prom",
    "ray-metrics-samples.prom",
    "ray-worker-1-metrics-error.log",
    "ray-worker-1-metrics.prom",
    "ray-worker-2-metrics-error.log",
    "ray-worker-2-metrics.prom",
    "resources.json",
    "resource-baseline-ready",
    "warmup.log",
}
_RESOURCE_CASE_FILES = frozenset(
    {
        "docker-stats.jsonl",
        "driver.pid",
        "measurement-complete.json",
        "measurement-started.json",
        "process-samples.jsonl",
        "ray-head-metrics-error.log",
        "ray-head-metrics.prom",
        "ray-metrics-samples.prom",
        "ray-worker-1-metrics-error.log",
        "ray-worker-1-metrics.prom",
        "ray-worker-2-metrics-error.log",
        "ray-worker-2-metrics.prom",
        "resources.json",
        "resource-baseline-ready",
    }
)
_CASE_DIR = re.compile(r"^[a-z0-9][a-z0-9_-]{2,160}$")
_JSON_FIELDS = {
    "docker-context-manifest.json": frozenset(
        {"schema_version", "files", "file_count", "total_bytes"}
    ),
    "expected-identity.json": frozenset({"row_count", "schema_sha256", "multiset_sha256"}),
    "fault-control.json": frozenset({"armed", "mode", "schema_version", "token"}),
    "fault-event.json": frozenset({"client_ip", "mode", "observed_at", "schema_version", "token"}),
    "manifest.json": frozenset(
        {
            "artifacts",
            "environment",
            "external",
            "harness",
            "official",
            "provenance",
            "run_id",
            "scenario_digest",
            "schema_version",
        }
    ),
    "measurement-complete.json": frozenset({"completed_at"}),
    "measurement-started.json": frozenset({"started_at"}),
    "official-install-report.json": frozenset({"environment", "install", "pip_version", "version"}),
    "external-install-report.json": frozenset({"environment", "install", "pip_version", "version"}),
    "result.json": frozenset(
        {
            "correctness",
            "exception",
            "metrics",
            "reason",
            "repetition",
            "run_id",
            "scenario_id",
            "schema_version",
            "side",
            "status",
        }
    ),
    "summary.json": frozenset({"groups", "schema_version", "status_counts"}),
    "workflow-identity.json": frozenset(
        {
            "candidate_sha",
            "mode",
            "retention_days",
            "schema_version",
            "workflow_run_attempt",
            "workflow_run_id",
            "workflow_sha",
        }
    ),
}
_IDENTITY_FIELDS = frozenset({"name", "version", "filename", "sha256", "commit"})
_RESULT_CORRECTNESS_FIELDS = frozenset({"row_count", "schema_sha256", "multiset_sha256"})
_RESULT_EXCEPTION_FIELDS = frozenset({"type", "module", "message", "traceback"})
_RESULT_METRIC_FIELDS = frozenset(
    {
        "block_count",
        "clickhouse_peak_memory_bytes",
        "duration_seconds",
        "exception_chain",
        "exception_observed",
        "expected_error",
        "expected_inserted_rows",
        "expected_multiset_sha256",
        "expected_server_insert_attempt_count",
        "expected_write_failed_task_attempt_count",
        "expected_write_max_task_attempt_number",
        "expected_write_task_attempt_count",
        "expected_write_task_states",
        "fault_event_count",
        "fault_mode",
        "fault_observed",
        "inserted_rows",
        "max_block_bytes",
        "max_block_rows",
        "non_replicated_deduplication_window",
        "outcome",
        "pair_position",
        "query_count",
        "query_duration_ms",
        "query_ids",
        "query_finish_count",
        "ray_failed_task_attempt_count",
        "ray_max_task_attempt_number",
        "ray_task_attempt_count",
        "ray_task_ids",
        "ray_task_states",
        "ray_unique_task_count",
        "read_bytes",
        "read_rows",
        "server_insert_attempt_count",
        "total_bytes",
        "total_rows",
        "unexpected_query_roles",
        "unclassified_query_count",
        "worker_kill_observed",
        "write_retry_mode",
    }
)
_RESOURCE_METRIC_FIELDS = frozenset(
    {
        "comparison_container_memory_baseline_bytes",
        "comparison_container_memory_peak_bytes",
        "clickhouse_container_memory_baseline_bytes",
        "clickhouse_container_memory_peak_bytes",
        "clickhouse_container_memory_peak_delta_bytes",
        "clickhouse_peak_memory_bytes",
        "driver_private_rss_baseline_bytes",
        "driver_private_rss_peak_bytes",
        "driver_private_rss_peak_delta_bytes",
        "driver_process_sample_count",
        "head_container_memory_baseline_bytes",
        "head_container_memory_peak_bytes",
        "head_container_memory_peak_delta_bytes",
        "proxy_container_memory_baseline_bytes",
        "proxy_container_memory_peak_bytes",
        "proxy_container_memory_peak_delta_bytes",
        "ray_container_memory_baseline_bytes",
        "ray_container_memory_peak_bytes",
        "ray_container_memory_peak_delta_bytes",
        "ray_metric_sample_count",
        "ray_object_store_baseline_missing_location_count",
        "ray_object_store_baseline_missing_locations",
        "ray_object_store_baseline_missing_dynamic_location_count",
        "ray_object_store_baseline_missing_dynamic_locations",
        "ray_object_store_measured_missing_location_count",
        "ray_object_store_measured_missing_locations",
        "ray_object_store_measured_missing_dynamic_location_count",
        "ray_object_store_measured_missing_dynamic_locations",
        "resource_sample_count",
        "telemetry_complete",
        "worker_container_memory_baseline_bytes",
        "worker_container_memory_peak_bytes",
        "worker_container_memory_peak_delta_bytes",
        "worker_private_rss_baseline_bytes",
        "worker_private_rss_peak_bytes",
        "worker_private_rss_peak_delta_bytes",
        "worker_process_sample_count",
    }
)
_QUERY_FIELDS = frozenset(
    {
        "query_id",
        "event_type",
        "role",
        "read_rows",
        "read_bytes",
        "duration_ms",
        "memory_usage",
        "log_comment",
        "query",
    }
)
_TASK_FIELDS = frozenset(
    {"task_id", "attempt_number", "name", "state", "type", "node_id", "worker_id"}
)
_PROCESS_SAMPLE_FIELDS = frozenset(
    {
        "timestamp",
        "aggregate_rss_bytes",
        "aggregate_shared_bytes",
        "aggregate_private_rss_bytes",
        "service",
        "sample_index",
    }
)
_DOCKER_STAT_FIELDS = frozenset(
    {"BlockIO", "CPUPerc", "Container", "ID", "MemPerc", "MemUsage", "Name", "NetIO", "PIDs"}
)
_SUMMARY_GROUP_FIELDS = frozenset(
    {
        "numeric_metrics",
        "paired_correctness_runs",
        "paired_numeric_metrics",
        "runs",
        "scenario_id",
        "side",
        "stable_correctness",
        "valid_runs",
    }
)
_SUMMARY_METRIC_FIELDS = frozenset({"min", "median", "max"})
_INSTALL_METADATA_FIELDS = frozenset(
    {
        "author",
        "author_email",
        "classifier",
        "description",
        "description_content_type",
        "download_url",
        "dynamic",
        "home_page",
        "keywords",
        "license",
        "license_expression",
        "license_file",
        "maintainer",
        "maintainer_email",
        "metadata_version",
        "name",
        "platform",
        "project_url",
        "provides_extra",
        "requires_dist",
        "requires_python",
        "summary",
        "version",
    }
)
_INSTALL_ITEM_FIELDS = frozenset(
    {"download_info", "is_direct", "is_yanked", "metadata", "requested"}
)
_INSTALL_DOWNLOAD_FIELDS = frozenset({"archive_info", "url"})
_INSTALL_ARCHIVE_FIELDS = frozenset({"hash", "hashes"})


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_schema(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("JSON schema must be an object")
    Draft202012Validator.check_schema(value)
    return value


def validate_document(document: Mapping[str, Any], schema: Mapping[str, Any]) -> None:
    errors = sorted(Draft202012Validator(schema).iter_errors(document), key=lambda item: item.path)
    if errors:
        first = errors[0]
        location = ".".join(str(part) for part in first.absolute_path) or "<root>"
        raise ValueError(f"schema validation failed at {location}: {first.message}")


def sanitize_text(value: str) -> tuple[str, tuple[str, ...]]:
    classes: set[str] = set()
    sanitized = _DSN_CREDENTIAL.sub(lambda match: f"{match.group('scheme')}<redacted>@", value)
    if sanitized != value:
        classes.add("dsn_credentials")
    rewritten = _HOST_PATH.sub("<host-path>", sanitized)
    if rewritten != sanitized:
        classes.add("host_paths")
    sanitized = rewritten
    rewritten = _URL_ENDPOINT.sub(
        lambda match: f"{match.group('scheme')}<redacted-endpoint>", sanitized
    )
    if rewritten != sanitized:
        classes.add("endpoints")
    sanitized = rewritten
    rewritten = _IP_ENDPOINT.sub("<redacted-endpoint>", sanitized)
    if rewritten != sanitized:
        classes.add("endpoints")
    sanitized = rewritten
    for pattern in _SENSITIVE_TEXT:
        rewritten = pattern.sub("<redacted-sensitive-value>", sanitized)
        if rewritten != sanitized:
            classes.add("sensitive_text")
        sanitized = rewritten
    return sanitized, tuple(sorted(classes))


def sanitize_value(value: Any) -> tuple[Any, tuple[str, ...]]:
    classes: set[str] = set()
    if isinstance(value, Mapping):
        sanitized: dict[str, Any] = {}
        for raw_key, raw_item in value.items():
            key = str(raw_key)
            if _SENSITIVE_KEY.search(key):
                sanitized[key] = "<redacted>"
                classes.add("sensitive_keys")
                continue
            item, item_classes = sanitize_value(raw_item)
            sanitized[key] = item
            classes.update(item_classes)
        return sanitized, tuple(sorted(classes))
    if isinstance(value, (list, tuple)):
        items: list[Any] = []
        for raw_item in value:
            item, item_classes = sanitize_value(raw_item)
            items.append(item)
            classes.update(item_classes)
        return items, tuple(sorted(classes))
    if isinstance(value, str):
        return sanitize_text(value)
    if value is None or isinstance(value, (bool, int, float)):
        return value, ()
    raise TypeError(f"unsupported evidence value: {type(value).__name__}")


def _is_result_metric_field(name: str) -> bool:
    if name in _RESULT_METRIC_FIELDS or name in _RESOURCE_METRIC_FIELDS:
        return True
    if re.fullmatch(
        r"(?:query_count|query_finish_count|read_rows|read_bytes)\.(?:planning|estimate|sample|data)",
        name,
    ):
        return True
    if re.fullmatch(
        r"ray_object_store_memory\.(?:MMAP_SHM|MMAP_DISK|SPILLED|WORKER_HEAP)_(?:baseline|peak|peak_delta)_bytes",
        name,
    ):
        return True
    if re.fullmatch(r"(?:container_peak_bytes|worker_private_rss_peak_bytes)\.[a-z0-9_.-]+", name):
        return True
    return False


def _check_fields(value: Mapping[str, Any], allowed: set[str] | frozenset[str], path: str) -> None:
    unknown = set(value) - allowed
    if unknown:
        raise ValueError(
            f"JSON fields are not on the publication allowlist: {path}: {sorted(unknown)}"
        )


def _validate_json_document(name: str, value: Any) -> None:
    if name in {"official-install-report.json", "external-install-report.json"}:
        if not isinstance(value, Mapping):
            raise ValueError(f"allowlisted JSON evidence must be an object: {name}")
        _check_fields(value, _JSON_FIELDS[name], name)
        environment = value.get("environment")
        if not isinstance(environment, Mapping):
            raise ValueError(f"JSON field {name}.environment must be an object")
        _check_fields(
            environment,
            {
                "implementation_name",
                "implementation_version",
                "os_name",
                "platform_machine",
                "platform_python_implementation",
                "platform_release",
                "platform_system",
                "platform_version",
                "python_full_version",
                "python_version",
                "sys_platform",
            },
            f"{name}.environment",
        )
        installations = value.get("install")
        if not isinstance(installations, list):
            raise ValueError(f"JSON field {name}.install must be a list")
        for index, installation in enumerate(installations):
            if not isinstance(installation, Mapping):
                raise ValueError(f"JSON install record must be an object: {name}.install[{index}]")
            _check_fields(installation, _INSTALL_ITEM_FIELDS, f"{name}.install[{index}]")
            metadata = installation.get("metadata")
            if not isinstance(metadata, Mapping):
                raise ValueError(
                    f"JSON install metadata must be an object: {name}.install[{index}]"
                )
            _check_fields(metadata, _INSTALL_METADATA_FIELDS, f"{name}.install[{index}].metadata")
            download_info = installation.get("download_info")
            if not isinstance(download_info, Mapping):
                raise ValueError(f"JSON download info must be an object: {name}.install[{index}]")
            _check_fields(
                download_info,
                _INSTALL_DOWNLOAD_FIELDS,
                f"{name}.install[{index}].download_info",
            )
            archive_info = download_info.get("archive_info")
            if not isinstance(archive_info, Mapping):
                raise ValueError(f"JSON archive info must be an object: {name}.install[{index}]")
            _check_fields(
                archive_info,
                _INSTALL_ARCHIVE_FIELDS,
                f"{name}.install[{index}].download_info.archive_info",
            )
            hashes = archive_info.get("hashes")
            if hashes is not None:
                if not isinstance(hashes, Mapping):
                    raise ValueError(
                        f"JSON archive hashes must be an object: {name}.install[{index}]"
                    )
                _check_fields(
                    hashes,
                    {"sha256"},
                    f"{name}.install[{index}].download_info.archive_info.hashes",
                )
        return
    if name == "docker-context-manifest.json":
        if not isinstance(value, Mapping):
            raise ValueError(f"allowlisted JSON evidence must be an object: {name}")
        _check_fields(value, _JSON_FIELDS[name], name)
        records = value.get("files")
        if not isinstance(records, list):
            raise ValueError(f"JSON field files must be a list: {name}")
        for index, record in enumerate(records):
            if not isinstance(record, Mapping):
                raise ValueError(f"JSON file record must be an object: {name}.files[{index}]")
            _check_fields(record, {"name", "sha256", "size"}, f"{name}.files[{index}]")
        return
    if name in {"manifest.json", "workflow-identity.json"}:
        if not isinstance(value, Mapping):
            raise ValueError(f"allowlisted JSON evidence must be an object: {name}")
        _check_fields(value, _JSON_FIELDS[name], name)
        if name == "manifest.json":
            for child in ("harness", "official", "external"):
                identity = value.get(child)
                if not isinstance(identity, Mapping):
                    raise ValueError(f"JSON field {name}.{child} must be an object")
                _check_fields(identity, _IDENTITY_FIELDS, f"{name}.{child}")
            provenance = value.get("provenance")
            if not isinstance(provenance, Mapping):
                raise ValueError(f"JSON field {name}.provenance must be an object")
            _check_fields(
                provenance,
                {"candidate_sha", "harness_commit", "workflow_sha"},
                f"{name}.provenance",
            )
            environment = value.get("environment")
            if not isinstance(environment, Mapping):
                raise ValueError(f"JSON field {name}.environment must be an object")
            _check_fields(
                environment,
                {
                    "python_version",
                    "ray_version",
                    "clickhouse_version",
                    "clickhouse_image",
                    "ray_base_image",
                    "runtime_image_id",
                    "mode",
                    "harness_git_state",
                    "controller_lock_sha256",
                    "official_requirements_sha256",
                    "external_requirements_sha256",
                    "result_schema_version",
                    "result_schema_sha256",
                },
                f"{name}.environment",
            )
            artifacts = value.get("artifacts")
            if not isinstance(artifacts, list):
                raise ValueError(f"JSON field {name}.artifacts must be a list")
            for index, record in enumerate(artifacts):
                if not isinstance(record, Mapping):
                    raise ValueError(
                        f"JSON artifact record must be an object: {name}.artifacts[{index}]"
                    )
                _check_fields(record, {"name", "sha256", "size"}, f"{name}.artifacts[{index}]")
        return
    if name == "result.json":
        if not isinstance(value, Mapping):
            raise ValueError(f"allowlisted JSON evidence must be an object: {name}")
        _check_fields(value, _JSON_FIELDS[name], name)
        correctness = value.get("correctness")
        if not isinstance(correctness, Mapping):
            raise ValueError(f"JSON field {name}.correctness must be an object")
        _check_fields(correctness, _RESULT_CORRECTNESS_FIELDS, f"{name}.correctness")
        metrics = value.get("metrics")
        if not isinstance(metrics, Mapping):
            raise ValueError(f"JSON field {name}.metrics must be an object")
        unknown_metrics = {str(key) for key in metrics if not _is_result_metric_field(str(key))}
        if unknown_metrics:
            raise ValueError(
                f"JSON fields are not on the publication allowlist: {name}.metrics: "
                f"{sorted(unknown_metrics)}"
            )
        exception = value.get("exception")
        if exception is not None:
            if not isinstance(exception, Mapping):
                raise ValueError(f"JSON field {name}.exception must be an object or null")
            _check_fields(exception, _RESULT_EXCEPTION_FIELDS, f"{name}.exception")
        return
    if name == "resources.json":
        if not isinstance(value, Mapping):
            raise ValueError(f"allowlisted JSON evidence must be an object: {name}")
        unknown = {str(key) for key in value if not _is_result_metric_field(str(key))}
        if unknown:
            raise ValueError(
                f"JSON fields are not on the publication allowlist: {name}: {sorted(unknown)}"
            )
        return
    if name == "summary.json":
        if not isinstance(value, Mapping):
            raise ValueError(f"allowlisted JSON evidence must be an object: {name}")
        _check_fields(value, _JSON_FIELDS[name], name)
        groups = value.get("groups")
        if not isinstance(groups, list):
            raise ValueError(f"JSON field {name}.groups must be a list")
        for index, group in enumerate(groups):
            if not isinstance(group, Mapping):
                raise ValueError(f"JSON group must be an object: {name}.groups[{index}]")
            _check_fields(group, _SUMMARY_GROUP_FIELDS, f"{name}.groups[{index}]")
            for field in ("numeric_metrics", "paired_numeric_metrics"):
                metrics = group.get(field)
                if not isinstance(metrics, Mapping):
                    raise ValueError(f"JSON field {name}.groups[{index}].{field} must be an object")
                for metric_name, metric in metrics.items():
                    if not _is_result_metric_field(str(metric_name)):
                        raise ValueError(
                            f"JSON field is not on the publication allowlist: "
                            f"{name}.groups[{index}].{field}.{metric_name}"
                        )
                    if not isinstance(metric, Mapping):
                        raise ValueError(
                            "JSON metric must be an object: "
                            f"{name}.groups[{index}].{field}.{metric_name}"
                        )
                    _check_fields(
                        metric,
                        _SUMMARY_METRIC_FIELDS,
                        f"{name}.groups[{index}].{field}.{metric_name}",
                    )
        status_counts = value.get("status_counts")
        if not isinstance(status_counts, Mapping) or not set(status_counts) <= {
            "valid",
            "invalid",
            "failed",
        }:
            raise ValueError(f"JSON field {name}.status_counts has unknown statuses")
        return
    allowed = _JSON_FIELDS.get(name)
    if allowed is not None:
        if not isinstance(value, Mapping):
            raise ValueError(f"allowlisted JSON evidence must be an object: {name}")
        _check_fields(value, allowed, name)


def sensitive_findings(value: str) -> tuple[str, ...]:
    findings: set[str] = set()
    if _DSN_CREDENTIAL.search(value):
        findings.add("dsn_credentials")
    if _HOST_PATH.search(value):
        findings.add("host_paths")
    if _URL_ENDPOINT.search(value) or _IP_ENDPOINT.search(value):
        findings.add("endpoints")
    if any(pattern.search(value) for pattern in _SENSITIVE_TEXT):
        findings.add("sensitive_text")
    return tuple(sorted(findings))


def atomic_write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n"
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False, prefix=f".{path.name}."
    ) as stream:
        stream.write(payload)
        temporary = Path(stream.name)
    os.replace(temporary, path)


def write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, sort_keys=True, separators=(",", ":")))
            stream.write("\n")


def read_jsonl(path: Path) -> tuple[dict[str, Any], ...]:
    rows: list[dict[str, Any]] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        value = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError(f"JSONL line {number} must contain an object")
        rows.append(value)
    return tuple(rows)


def artifact_record(path: Path, *, root: Path) -> dict[str, int | str]:
    return {
        "name": path.relative_to(root).as_posix(),
        "sha256": sha256_file(path),
        "size": path.stat().st_size,
    }


def pip_report_identity(path: Path, package: str) -> dict[str, str]:
    report = json.loads(path.read_text(encoding="utf-8"))
    expected = package.lower().replace("_", "-")
    for item in report.get("install", []):
        metadata = item.get("metadata", {})
        name = str(metadata.get("name", ""))
        if name.lower().replace("_", "-") != expected:
            continue
        download_info = item.get("download_info", {})
        archive = download_info.get("archive_info", {})
        hashes = archive.get("hashes", {})
        sha256 = hashes.get("sha256")
        if sha256 is None:
            raw_hash = str(archive.get("hash", ""))
            if raw_hash.startswith("sha256="):
                sha256 = raw_hash.removeprefix("sha256=")
        if not isinstance(sha256, str) or re.fullmatch(r"[0-9a-f]{64}", sha256) is None:
            raise ValueError(f"pip report lacks a valid SHA-256 for {package}")
        filename = Path(unquote(urlparse(str(download_info.get("url", ""))).path)).name
        if not filename.endswith(".whl"):
            raise ValueError(f"pip report lacks an immutable wheel filename for {package}")
        return {
            "name": name,
            "version": str(metadata["version"]),
            "filename": filename,
            "sha256": sha256,
        }
    raise ValueError(f"pip report does not contain package {package}")


def wheel_identity(wheel: Path, package: str) -> dict[str, str]:
    expected = package.lower().replace("_", "-")
    with zipfile.ZipFile(wheel) as archive:
        metadata_names = [
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        ]
        if len(metadata_names) != 1:
            raise ValueError("wheel must contain exactly one distribution metadata file")
        fields: dict[str, str] = {}
        for line in archive.read(metadata_names[0]).decode("utf-8").splitlines():
            name, separator, value = line.partition(":")
            if separator and name in {"Name", "Version"}:
                fields[name] = value.strip()
        if fields.get("Name", "").lower().replace("_", "-") != expected:
            raise ValueError(f"wheel metadata does not identify package {package}")
        if not fields.get("Version"):
            raise ValueError("wheel metadata has no version")
    return {
        "name": fields["Name"],
        "version": fields["Version"],
        "filename": wheel.name,
        "sha256": sha256_file(wheel),
    }


def verify_wheel_sources(wheel: Path, source: Path, package: str) -> None:
    expected = {
        f"{package}/{path.relative_to(source).as_posix()}": path.read_bytes()
        for path in sorted(source.rglob("*"))
        if path.is_file() and "__pycache__" not in path.parts
    }
    if not expected:
        raise ValueError("source package contains no files")
    with zipfile.ZipFile(wheel) as archive:
        actual_names = {
            name
            for name in archive.namelist()
            if name.startswith(f"{package}/") and not name.endswith("/")
        }
        if actual_names != set(expected):
            raise ValueError("wheel package members do not match current source members")
        for name, content in expected.items():
            if archive.read(name) != content:
                raise ValueError(f"wheel member differs from current source: {name}")


def build_context_manifest(root: Path, output: Path) -> dict[str, Any]:
    relative_files = [
        Path("docker/comparison/Dockerfile"),
        Path("docker/comparison/Dockerfile.dockerignore"),
        Path("comparison/official/env/official-requirements.txt"),
        Path("comparison/official/env/external-requirements.txt"),
    ]
    for directory in (
        root / "comparison/official/config",
        root / "comparison/official/schema",
    ):
        relative_files.extend(
            path.relative_to(root) for path in sorted(directory.rglob("*")) if path.is_file()
        )
    records: list[dict[str, int | str]] = []
    for relative in sorted(set(relative_files)):
        path = root / relative
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"Docker context member is missing or unsafe: {relative}")
        records.append(artifact_record(path, root=root))
    document = {
        "schema_version": 1,
        "files": records,
        "file_count": len(records),
        "total_bytes": sum(int(record["size"]) for record in records),
    }
    atomic_write_json(output, document)
    return document


def write_checksums(path: Path, files: Iterable[Path], *, root: Path) -> None:
    lines = [f"{sha256_file(file)}  {file.relative_to(root).as_posix()}" for file in files]
    path.write_text("\n".join(sorted(lines)) + "\n", encoding="utf-8")


def collect_case_evidence(
    root: Path,
    results_path: Path,
    queries_path: Path,
    result_schema: Mapping[str, Any],
) -> tuple[int, int]:
    result_rows: list[dict[str, Any]] = []
    query_rows: list[dict[str, Any]] = []
    for path in sorted(root.glob("*/result.json")):
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError(f"case result must be an object: {path}")
        resources_path = path.with_name("resources.json")
        if resources_path.is_file():
            resources = json.loads(resources_path.read_text(encoding="utf-8"))
            if not isinstance(resources, dict) or not isinstance(value.get("metrics"), dict):
                raise ValueError(f"case resource summary must be an object: {resources_path}")
            value["metrics"].update(resources)
        case_queries_path = path.with_name("queries.jsonl")
        case_queries = read_jsonl(case_queries_path) if case_queries_path.is_file() else ()
        if int(value["metrics"].get("query_count", -1)) != len(case_queries):
            raise ValueError(f"case query evidence count does not match result: {path}")
        validate_document(value, result_schema)
        result_rows.append(value)
        query_rows.extend(case_queries)
    if not result_rows:
        raise ValueError("no case results were collected")
    write_jsonl(results_path, result_rows)
    queries_path.parent.mkdir(parents=True, exist_ok=True)
    fields = (
        "query_id",
        "event_type",
        "role",
        "read_rows",
        "read_bytes",
        "duration_ms",
        "memory_usage",
        "log_comment",
        "query",
    )
    expected_fields = set(fields)
    for row in query_rows:
        if set(row) != expected_fields:
            raise ValueError("query evidence row does not match the closed CSV schema")
    with queries_path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="raise")
        writer.writeheader()
        writer.writerows(query_rows)
    return len(result_rows), len(query_rows)


def _expected_case_names(mode: str, scenarios_path: Path) -> set[str]:
    return set(_expected_cases(mode, scenarios_path))


def _expected_cases(mode: str, scenarios_path: Path) -> dict[str, Any]:
    from ray_clickhouse_comparison.config import load_scenarios

    scenarios = load_scenarios(scenarios_path)
    cases: tuple[tuple[str, str, str, int], ...]
    if mode == "smoke":
        cases = (
            ("official", "read.default.single", "none", 0),
            ("external", "read.default.single", "none", 0),
            ("external", "write.transport.post_commit", "drop_response", 0),
            ("official", "write.worker.post_commit", "hold_response", 0),
        )
    elif mode == "dry-run":
        cases = (
            ("official", "read.controlled.ordered", "none", 0),
            ("external", "read.controlled.ordered", "none", 0),
        )
    elif mode == "formal":
        cases = tuple(
            (side, scenario.id, scenario.fault, repetition)
            for scenario in scenarios
            for repetition in range(scenario.repetitions)
            for side in scenario.sides
        )
    else:
        raise ValueError(f"unsupported comparison mode: {mode}")
    scenarios_by_id = {scenario.id: scenario for scenario in scenarios}
    expected: dict[str, Any] = {}
    for side, scenario_id, fault, repetition in cases:
        try:
            scenario = scenarios_by_id[scenario_id]
        except KeyError:
            raise ValueError(
                f"comparison mode references undeclared scenario: {scenario_id}"
            ) from None
        case_name = f"{side}-{scenario_id.replace('.', '-').replace(':', '-')}-{fault}-{repetition}"
        expected[case_name] = scenario
    return expected


def _required_case_files(scenario: Any) -> set[str]:
    required = set(_COMMON_CASE_FILES)
    if scenario.resource_metrics_required:
        required.update(_RESOURCE_CASE_FILES)
    if scenario.warmup:
        required.add("warmup.log")
    if scenario.id == "read.error.permission":
        required.add("permission-cleanup.log")
    return required


def _required_control_files(scenario: Any) -> set[str]:
    if scenario.fault == "none":
        return set()
    required = {"control/fault-control.json", "control/fault-event.json"}
    if scenario.fault == "hold_response":
        required.add("killed-worker.txt")
    return required


def validate_complete_tree(source: Path, *, mode: str, scenarios_path: Path) -> None:
    if source.is_symlink():
        raise ValueError("complete comparison evidence tree cannot be a symbolic link")
    for path in source.rglob("*"):
        if path.is_symlink():
            raise ValueError(
                f"complete comparison evidence tree contains a symbolic link: "
                f"{path.relative_to(source)}"
            )
    required_root = set(_COMMON_ROOT_FILES)
    if mode in {"dry-run", "formal"}:
        required_root.update(_REMOTE_ROOT_FILES)
    missing_root = sorted(name for name in required_root if not (source / name).is_file())
    if missing_root:
        raise ValueError(f"complete comparison evidence is missing root files: {missing_root}")
    manifest = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
    environment = manifest.get("environment") if isinstance(manifest, Mapping) else None
    if not isinstance(environment, Mapping) or environment.get("mode") != mode:
        raise ValueError("comparison manifest mode does not match the requested artifact mode")
    expected_cases = _expected_cases(mode, scenarios_path)
    actual_cases = {
        path.name for path in source.iterdir() if path.is_dir() and path.name not in {"control"}
    }
    if actual_cases != set(expected_cases):
        raise ValueError(
            f"comparison case set mismatch: expected {sorted(expected_cases)}, "
            f"found {sorted(actual_cases)}"
        )
    for case, scenario in sorted(expected_cases.items()):
        required_case_files = _required_case_files(scenario)
        missing = sorted(
            name for name in required_case_files if not (source / case / name).is_file()
        )
        missing.extend(
            name
            for name in sorted(_required_control_files(scenario))
            if not (source / case / name).is_file()
        )
        if missing:
            raise ValueError(f"complete comparison case is missing files: {case}: {missing}")
    rows = read_jsonl(source / "results.jsonl")
    if len(rows) != len(expected_cases) or any(row.get("status") != "valid" for row in rows):
        raise ValueError("complete comparison evidence must contain one valid result per case")
    result_cases = {
        f"{row.get('side')}-{str(row.get('scenario_id', '')).replace('.', '-').replace(':', '-')}-"
        f"{row.get('metrics', {}).get('fault_mode', 'none')}-{row.get('repetition')}"
        for row in rows
    }
    if result_cases != set(expected_cases):
        raise ValueError("result rows do not cover the declared comparison case set")


def sanitize_tree(
    source: Path, destination: Path, *, require_complete: bool = False
) -> dict[str, Any]:
    """Create a sanitized text-only artifact tree and a machine-readable report."""
    if source.resolve() == destination.resolve():
        raise ValueError("source and destination evidence trees must differ")
    if source.is_symlink():
        raise ValueError("source evidence tree cannot be a symbolic link")
    if not source.is_dir():
        raise ValueError("source evidence tree must be an existing directory")
    if source.resolve() in destination.resolve().parents:
        raise ValueError("destination evidence tree cannot be nested under source")
    if destination.exists() and any(destination.iterdir()):
        raise ValueError("destination evidence tree must be absent or empty")
    destination.mkdir(parents=True, exist_ok=True)
    removed_classes: set[str] = set()
    published: list[Path] = []
    skipped: list[str] = []
    for path in sorted(source.rglob("*")):
        if path.is_symlink():
            raise ValueError(f"evidence tree contains a symbolic link: {path.relative_to(source)}")
        if not path.is_file():
            continue
        relative = path.relative_to(source)
        parts = relative.parts
        if len(parts) == 1:
            allowed = relative.name in _ALLOWED_ROOT_FILES
        elif len(parts) == 2:
            allowed = bool(_CASE_DIR.fullmatch(parts[0])) and parts[1] in _ALLOWED_CASE_FILES
        elif len(parts) == 3:
            allowed = (
                bool(_CASE_DIR.fullmatch(parts[0]))
                and parts[1] == "control"
                and parts[2] in _ALLOWED_CONTROL_FILES
            )
        else:
            allowed = False
        if not allowed:
            raise ValueError(f"evidence file is not on the publication allowlist: {relative}")
        target = destination / relative
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            if require_complete:
                raise ValueError(
                    f"complete evidence contains a non-UTF-8 required file: {relative}"
                ) from None
            skipped.append(relative.as_posix())
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        if path.suffix == ".json":
            try:
                value = json.loads(text)
            except json.JSONDecodeError:
                raise ValueError(f"allowlisted JSON evidence is malformed: {relative}") from None
            else:
                sanitized_value, classes = sanitize_value(value)
                _validate_json_document(relative.name, sanitized_value)
                target.write_text(
                    json.dumps(sanitized_value, sort_keys=True, indent=2, ensure_ascii=True) + "\n",
                    encoding="utf-8",
                )
        elif path.suffix == ".jsonl":
            lines: list[str] = []
            line_classes: set[str] = set()
            for number, line in enumerate(text.splitlines(), start=1):
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    raise ValueError(
                        f"allowlisted JSONL evidence is malformed: {relative}:{number}"
                    ) from None
                sanitized_value, item_classes = sanitize_value(value)
                if relative.name == "results.jsonl":
                    _validate_json_document("result.json", sanitized_value)
                elif relative.name == "docker-stats.jsonl":
                    if not isinstance(sanitized_value, Mapping):
                        raise ValueError(
                            f"allowlisted JSONL evidence must contain objects: {relative}:{number}"
                        )
                    docker_allowed = (
                        {"sample_index"}
                        if "sample_index" in sanitized_value
                        else _DOCKER_STAT_FIELDS
                    )
                    _check_fields(sanitized_value, docker_allowed, f"{relative}:{number}")
                else:
                    line_allowed = {
                        "queries.jsonl": _QUERY_FIELDS,
                        "tasks.jsonl": _TASK_FIELDS,
                        "process-samples.jsonl": _PROCESS_SAMPLE_FIELDS,
                    }.get(relative.name)
                    if line_allowed is None or not isinstance(sanitized_value, Mapping):
                        raise ValueError(f"unsupported allowlisted JSONL evidence: {relative}")
                    _check_fields(sanitized_value, line_allowed, f"{relative}:{number}")
                line_classes.update(item_classes)
                lines.append(json.dumps(sanitized_value, sort_keys=True, separators=(",", ":")))
            classes = tuple(sorted(line_classes))
            target.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
        else:
            sanitized_text, classes = sanitize_text(text)
            target.write_text(sanitized_text, encoding="utf-8")
        removed_classes.update(classes)
        findings = sensitive_findings(target.read_text(encoding="utf-8"))
        if findings:
            raise ValueError(f"sanitized artifact retains sensitive data: {relative}: {findings}")
        published.append(target)
    report = {
        "schema_version": 1,
        "removed_classes": sorted(removed_classes),
        "skipped_binary_files": skipped,
        "published_files": [path.relative_to(destination).as_posix() for path in published],
    }
    report_path = destination / "redaction-report.json"
    atomic_write_json(report_path, report)
    published.append(report_path)
    checksums = destination / "SHA256SUMS"
    write_checksums(checksums, published, root=destination)
    return report
