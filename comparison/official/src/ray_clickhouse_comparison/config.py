"""Typed configuration and fail-closed CI change classification."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import tomllib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Literal

Side = Literal["official", "external"]
Profile = Literal["default", "controlled"]
TerminalAction = Literal["stream", "materialize", "write"]
FaultMode = Literal["none", "drop_response", "hold_response"]
SplitMode = Literal["single", "partition", "range"]
WriteRetryMode = Literal["default", "zero"]

_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_ALLOWED_SIDES = frozenset({"official", "external"})
_ALLOWED_PROFILES = frozenset({"default", "controlled"})
_ALLOWED_ACTIONS = frozenset({"stream", "materialize", "write"})
_ALLOWED_FAULTS = frozenset({"none", "drop_response", "hold_response"})
_ALLOWED_SPLITS = frozenset({"single", "partition", "range"})
_ALLOWED_RETRY_MODES = frozenset({"default", "zero"})
_ALLOWED_QUERY_ROLES = frozenset({"planning", "estimate", "sample", "data"})
_SCENARIO_ID = re.compile(r"^[a-z][a-z0-9_.-]{2,79}$")
_TOKEN = re.compile(r"^[a-z][a-z0-9_]{1,79}$")
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RUN_ID = re.compile(r"^[a-z0-9][a-z0-9._-]{7,127}$")
_EVIDENCE_FILES = frozenset(
    {
        "manifest.json",
        "artifact-index.json",
        "results.jsonl",
        "queries.csv",
        "summary.json",
        "redaction-report.json",
        "SHA256SUMS",
    }
)


@dataclass(frozen=True)
class ResourceConfig:
    ray_workers: int
    cpus_per_worker: int
    worker_memory_bytes: int
    task_memory_bytes: int
    head_memory_bytes: int
    runner_memory_bytes: int
    head_object_store_memory_bytes: int
    worker_object_store_memory_bytes: int
    clickhouse_max_threads: int
    external_batch_rows: int
    external_batch_bytes: int


@dataclass(frozen=True)
class EvidenceConfig:
    schema_version: int
    complete_log_retention_days: int
    result_retention_days: int
    require_sanitization: bool


@dataclass(frozen=True)
class DockerConfig:
    context: str
    dockerfile: str
    dockerignore: str
    compose_file: str


@dataclass(frozen=True)
class ReferenceConfig:
    schema_version: int
    runtime_base_commit: str
    python_version: str
    ray_version: str
    clickhouse_version: str
    clickhouse_image: str
    ray_base_image: str
    reference_runner: str
    warmup_repetitions: int
    measured_repetitions: int
    sampling_interval_seconds: float
    scenario_timeout_seconds: int
    resources: ResourceConfig
    evidence: EvidenceConfig
    docker: DockerConfig


@dataclass(frozen=True)
class Scenario:
    id: str
    group: str
    sides: tuple[Side, ...]
    profile: Profile
    terminal_action: TerminalAction
    fault: FaultMode
    split: SplitMode
    write_retry: WriteRetryMode
    correctness_gate: str
    query_roles: tuple[str, ...]
    invalid_if: tuple[str, ...]
    fixture_rows: int
    fixture_payload_bytes: int
    repetitions: int
    warmup: bool
    resource_metrics_required: bool
    unique_order_key: str | None = None
    columns: tuple[str, ...] | None = None


@dataclass(frozen=True)
class ChangeRecord:
    status: str
    path: str


@dataclass(frozen=True)
class ChangeClassification:
    runtime_relevant: bool
    paths: tuple[str, ...]
    reason: str


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require_positive(name: str, value: int | float) -> None:
    if isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} must be positive")


def load_reference(path: Path) -> ReferenceConfig:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    resources = ResourceConfig(**data.pop("resources"))
    evidence = EvidenceConfig(**data.pop("evidence"))
    docker = DockerConfig(**data.pop("docker"))
    config = ReferenceConfig(
        **data,
        resources=resources,
        evidence=evidence,
        docker=docker,
    )
    if config.schema_version != 1:
        raise ValueError("reference schema_version must be 1")
    if _COMMIT.fullmatch(config.runtime_base_commit) is None:
        raise ValueError("runtime_base_commit must be a full lowercase commit SHA")
    for name, value in (
        ("warmup_repetitions", config.warmup_repetitions),
        ("measured_repetitions", config.measured_repetitions),
        ("sampling_interval_seconds", config.sampling_interval_seconds),
        ("scenario_timeout_seconds", config.scenario_timeout_seconds),
        ("ray_workers", resources.ray_workers),
        ("cpus_per_worker", resources.cpus_per_worker),
        ("worker_memory_bytes", resources.worker_memory_bytes),
        ("task_memory_bytes", resources.task_memory_bytes),
        ("head_memory_bytes", resources.head_memory_bytes),
        ("runner_memory_bytes", resources.runner_memory_bytes),
        ("head_object_store_memory_bytes", resources.head_object_store_memory_bytes),
        ("worker_object_store_memory_bytes", resources.worker_object_store_memory_bytes),
        ("clickhouse_max_threads", resources.clickhouse_max_threads),
        ("external_batch_rows", resources.external_batch_rows),
        ("external_batch_bytes", resources.external_batch_bytes),
    ):
        _require_positive(name, value)
    if docker.context != ".":
        raise ValueError("comparison Docker context must be the repository root")
    for name, image in (
        ("clickhouse_image", config.clickhouse_image),
        ("ray_base_image", config.ray_base_image),
    ):
        if (
            "docker.m.daocloud.io/" not in image
            or re.search(r"@sha256:[0-9a-f]{64}$", image) is None
        ):
            raise ValueError(f"{name} must use the approved mirror and an immutable digest")
    return config


def load_scenarios(path: Path) -> tuple[Scenario, ...]:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    if data.get("schema_version") != 1:
        raise ValueError("scenario schema_version must be 1")
    scenarios: list[Scenario] = []
    seen: set[str] = set()
    for item in data.get("scenario", []):
        raw_sides = tuple(item["sides"])
        if len(raw_sides) != 2 or set(raw_sides) != _ALLOWED_SIDES:
            raise ValueError(
                f"scenario {item.get('id')!r} must compare official and external exactly once"
            )
        profile = item["profile"]
        action = item["terminal_action"]
        fault = item["fault"]
        split = item.get("split", "single")
        write_retry = item.get("write_retry", "default")
        if profile not in _ALLOWED_PROFILES:
            raise ValueError(f"invalid profile {profile!r}")
        if action not in _ALLOWED_ACTIONS:
            raise ValueError(f"invalid terminal action {action!r}")
        if fault not in _ALLOWED_FAULTS:
            raise ValueError(f"invalid fault mode {fault!r}")
        if split not in _ALLOWED_SPLITS:
            raise ValueError(f"invalid split mode {split!r}")
        if write_retry not in _ALLOWED_RETRY_MODES:
            raise ValueError(f"invalid write retry mode {write_retry!r}")
        scenario = Scenario(
            id=item["id"],
            group=item["group"],
            sides=raw_sides,
            profile=profile,
            terminal_action=action,
            fault=fault,
            split=split,
            write_retry=write_retry,
            correctness_gate=item["correctness_gate"],
            query_roles=tuple(item["query_roles"]),
            invalid_if=tuple(item["invalid_if"]),
            fixture_rows=item["fixture_rows"],
            fixture_payload_bytes=item["fixture_payload_bytes"],
            repetitions=item["repetitions"],
            warmup=item.get("warmup", False),
            resource_metrics_required=item.get("resource_metrics_required", False),
            unique_order_key=item.get("unique_order_key"),
            columns=tuple(item["columns"]) if item.get("columns") is not None else None,
        )
        if _SCENARIO_ID.fullmatch(scenario.id) is None:
            raise ValueError(f"invalid scenario id: {scenario.id!r}")
        if _TOKEN.fullmatch(scenario.group) is None:
            raise ValueError(f"invalid scenario group: {scenario.group!r}")
        if scenario.id in seen:
            raise ValueError(f"duplicate scenario id: {scenario.id}")
        if (
            scenario.unique_order_key is not None
            and _IDENTIFIER.fullmatch(scenario.unique_order_key) is None
        ):
            raise ValueError(f"invalid unique order key: {scenario.unique_order_key!r}")
        if scenario.columns is not None:
            if scenario.terminal_action == "write":
                raise ValueError(f"write scenario {scenario.id!r} cannot project columns")
            if not scenario.columns or len(set(scenario.columns)) != len(scenario.columns):
                raise ValueError(f"scenario {scenario.id!r} has an invalid column projection")
            if any(_IDENTIFIER.fullmatch(column) is None for column in scenario.columns):
                raise ValueError(f"scenario {scenario.id!r} has an invalid column projection")
        if scenario.split == "range" and scenario.unique_order_key is None:
            raise ValueError(f"range scenario {scenario.id!r} requires a unique order key")
        if not scenario.correctness_gate or not scenario.invalid_if:
            raise ValueError(f"scenario {scenario.id!r} has an incomplete correctness contract")
        if not scenario.query_roles or not set(scenario.query_roles) <= _ALLOWED_QUERY_ROLES:
            raise ValueError(f"scenario {scenario.id!r} has invalid query roles")
        if len(set(scenario.query_roles)) != len(scenario.query_roles):
            raise ValueError(f"scenario {scenario.id!r} has duplicate query roles")
        for name, value in (
            ("fixture_rows", scenario.fixture_rows),
            ("fixture_payload_bytes", scenario.fixture_payload_bytes),
            ("repetitions", scenario.repetitions),
        ):
            _require_positive(f"{scenario.id}.{name}", value)
        if scenario.terminal_action != "write" and scenario.fault != "none":
            raise ValueError(f"read scenario {scenario.id!r} cannot inject a write fault")
        if scenario.terminal_action != "write" and scenario.write_retry != "default":
            raise ValueError(f"read scenario {scenario.id!r} cannot set write retry policy")
        if scenario.terminal_action == "write" and scenario.fault == "none":
            raise ValueError(f"write scenario {scenario.id!r} must declare a fault")
        if scenario.terminal_action == "write" and scenario.split != "single":
            raise ValueError(f"write scenario {scenario.id!r} cannot declare a read split")
        if scenario.resource_metrics_required and scenario.terminal_action == "write":
            raise ValueError(f"write scenario {scenario.id!r} cannot require resource comparison")
        seen.add(scenario.id)
        scenarios.append(scenario)
    if not scenarios:
        raise ValueError("at least one scenario is required")
    return tuple(scenarios)


def scenario_digest(scenarios: tuple[Scenario, ...]) -> str:
    payload = [
        {
            "id": scenario.id,
            "group": scenario.group,
            "sides": list(scenario.sides),
            "profile": scenario.profile,
            "terminal_action": scenario.terminal_action,
            "fault": scenario.fault,
            "split": scenario.split,
            "write_retry": scenario.write_retry,
            "correctness_gate": scenario.correctness_gate,
            "query_roles": list(scenario.query_roles),
            "invalid_if": list(scenario.invalid_if),
            "fixture_rows": scenario.fixture_rows,
            "fixture_payload_bytes": scenario.fixture_payload_bytes,
            "repetitions": scenario.repetitions,
            "warmup": scenario.warmup,
            "resource_metrics_required": scenario.resource_metrics_required,
            "unique_order_key": scenario.unique_order_key,
            "columns": list(scenario.columns) if scenario.columns is not None else None,
        }
        for scenario in scenarios
    ]
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _is_evidence_only(path: str) -> bool:
    normalized = PurePosixPath(path).as_posix()
    parts = PurePosixPath(normalized).parts
    if len(parts) == 5 and parts[:3] == ("comparison", "official", "evidence"):
        return bool(_RUN_ID.fullmatch(parts[3])) and parts[4] in _EVIDENCE_FILES
    return normalized in {"docs/official-comparison.md", "tests/it-ledger.md"}


def classify_changed_paths(records: tuple[ChangeRecord, ...]) -> ChangeClassification:
    if not records:
        return ChangeClassification(True, (), "empty or unresolved change set")
    paths: list[str] = []
    evidence_runs: set[str] = set()
    for record in records:
        if record.status not in {"A", "M"}:
            return ChangeClassification(True, tuple(paths), f"unsupported status {record.status}")
        path = PurePosixPath(record.path).as_posix()
        paths.append(path)
        if any(character in path for character in ("\n", "\r", "\t")):
            return ChangeClassification(True, tuple(paths), "non-portable changed path")
        if path.startswith("/") or ".." in PurePosixPath(path).parts:
            return ChangeClassification(True, tuple(paths), "unsafe changed path")
        if not _is_evidence_only(path):
            return ChangeClassification(True, tuple(paths), f"runtime-relevant path: {path}")
        parts = PurePosixPath(path).parts
        if len(parts) == 5:
            evidence_runs.add(parts[3])
            if len(evidence_runs) > 1:
                return ChangeClassification(True, tuple(paths), "multiple evidence run IDs")
    return ChangeClassification(False, tuple(sorted(paths)), "closed evidence-only allowlist")


def parse_name_status_z(payload: bytes) -> tuple[ChangeRecord, ...]:
    parts = payload.split(b"\0")
    if parts and parts[-1] == b"":
        parts.pop()
    records: list[ChangeRecord] = []
    index = 0
    while index < len(parts):
        status = parts[index].decode("ascii", errors="strict")
        index += 1
        path_count = 2 if status.startswith(("R", "C")) else 1
        if index + path_count > len(parts):
            raise ValueError("malformed git name-status output")
        paths = [
            parts[index + offset].decode("utf-8", errors="surrogateescape")
            for offset in range(path_count)
        ]
        index += path_count
        path = paths[-1] if path_count == 1 else f"{paths[0]} -> {paths[1]}"
        records.append(ChangeRecord(status, path))
    return tuple(records)


def classify_git_range(event: str, base: str, head: str, repo: Path) -> ChangeClassification:
    if event != "push" or _COMMIT.fullmatch(base) is None or _COMMIT.fullmatch(head) is None:
        return ChangeClassification(True, (), "non-push or unresolved comparison")
    if set(base) == {"0"}:
        return ChangeClassification(True, (), "zero push base")
    try:
        subprocess.run(
            ["git", "cat-file", "-e", f"{base}^{{commit}}"],
            cwd=repo,
            check=True,
            capture_output=True,
        )
        diff = subprocess.run(
            ["git", "diff", "--name-status", "-z", base, head],
            cwd=repo,
            check=True,
            capture_output=True,
        ).stdout
        return classify_changed_paths(parse_name_status_z(diff))
    except (OSError, subprocess.CalledProcessError, UnicodeError, ValueError) as exc:
        return ChangeClassification(True, (), f"git comparison failed: {type(exc).__name__}")


def write_github_classification(path: Path, classification: ChangeClassification) -> None:
    with path.open("a", encoding="utf-8") as stream:
        stream.write(f"runtime_relevant={str(classification.runtime_relevant).lower()}\n")
        stream.write(f"reason={classification.reason}\n")
        stream.write(f"changed_paths={json.dumps(classification.paths, separators=(',', ':'))}\n")


def _main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event", required=True)
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", required=True)
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument("--github-output", type=Path, required=True)
    args = parser.parse_args(argv)
    write_github_classification(
        args.github_output,
        classify_git_range(args.event, args.base, args.head, args.repo),
    )


if __name__ == "__main__":
    _main()
