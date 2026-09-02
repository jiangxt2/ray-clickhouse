"""Execute one public-API scenario inside a comparison Ray cluster."""

from __future__ import annotations

import argparse
import importlib
import json
import os
import re
import signal
import time
import traceback
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import quote

import pyarrow as pa

from ray_clickhouse_comparison.config import (
    ReferenceConfig,
    Scenario,
    load_reference,
    load_scenarios,
)
from ray_clickhouse_comparison.evidence import (
    atomic_write_json,
    load_schema,
    validate_document,
    write_jsonl,
)
from ray_clickhouse_comparison.faults import FaultController, FaultMode
from ray_clickhouse_comparison.fixtures import (
    CorrectnessAccumulator,
    correctness_identity,
    make_fixture,
)

Side = Literal["official", "external"]
RunMode = Literal["smoke", "dry-run", "formal"]
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PERMISSION_USER = "ray_clickhouse_comparison_no_select"


class ScenarioExecutionTimeoutError(TimeoutError):
    """Raised when a measured terminal action exceeds the fixed harness timeout."""


@contextmanager
def _scenario_timeout(seconds: int) -> Iterator[None]:
    def expire(signum: int, frame: object) -> None:
        del signum, frame
        raise ScenarioExecutionTimeoutError("measured scenario exceeded its configured timeout")

    previous = signal.signal(signal.SIGALRM, expire)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def _load_scenario(path: Path, scenario_id: str) -> Scenario:
    matches = [scenario for scenario in load_scenarios(path) if scenario.id == scenario_id]
    if len(matches) != 1:
        raise ValueError(f"scenario is not declared exactly once: {scenario_id!r}")
    return matches[0]


def _fixture_shape(scenario: Scenario, mode: RunMode) -> tuple[int, int]:
    if mode == "formal":
        return scenario.fixture_rows, scenario.fixture_payload_bytes
    return min(scenario.fixture_rows, 256), min(scenario.fixture_payload_bytes, 64)


def _required_env(name: str) -> str:
    try:
        return os.environ[name]
    except KeyError:
        raise RuntimeError(f"required environment variable is unavailable: {name}") from None


def _direct_client(database: str = "default") -> Any:
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=_required_env("RAY_COMPARISON_CLICKHOUSE_DIRECT_HOST"),
        port=int(_required_env("RAY_COMPARISON_CLICKHOUSE_DIRECT_PORT")),
        database=database,
        username="default",
        password="",
        query_retries=0,
        show_clickhouse_errors="scrub",
    )


def _proxy_host() -> str:
    return _required_env("RAY_COMPARISON_CLICKHOUSE_PROXY_HOST")


def _proxy_port() -> int:
    return int(_required_env("RAY_COMPARISON_CLICKHOUSE_PROXY_PORT"))


def _dsn(
    database: str,
    *,
    username: str = "default",
    password: str = "",
    port: int | None = None,
) -> str:
    user_info = quote(username, safe="")
    if password:
        user_info += f":{quote(password, safe='')}"
    return f"clickhouse+http://{user_info}@{_proxy_host()}:{port or _proxy_port()}/{database}"


def _setup_read_fixture(
    database: str,
    table_name: str,
    rows: int,
    *,
    payload_bytes: int,
) -> pa.Table:
    fixture = make_fixture(rows, payload_bytes=payload_bytes)
    client = _direct_client()
    try:
        client.command(f"CREATE DATABASE IF NOT EXISTS `{database}`")
        client.command(f"DROP TABLE IF EXISTS `{database}`.`{table_name}`")
        client.command(
            f"""
            CREATE TABLE `{database}`.`{table_name}` (
                `id` Int64,
                `partition_key` String,
                `nullable_value` Nullable(Int64),
                `amount` Decimal(18, 2),
                `event_time` DateTime64(6, 'UTC'),
                `event_date` Date,
                `payload` String
            ) ENGINE = MergeTree
            PARTITION BY partition_key
            ORDER BY id
            """
        )
        client.insert_arrow(table_name, fixture, database=database)
    finally:
        client.close()
    return fixture


def _setup_write_table(database: str, table_name: str) -> None:
    client = _direct_client()
    try:
        client.command(f"CREATE DATABASE IF NOT EXISTS `{database}`")
        client.command(f"DROP TABLE IF EXISTS `{database}`.`{table_name}`")
        client.command(
            f"""
            CREATE TABLE `{database}`.`{table_name}` (
                `id` Int64,
                `payload` String
            ) ENGINE = MergeTree
            ORDER BY id
            SETTINGS non_replicated_deduplication_window = 0
            """
        )
    finally:
        client.close()


def _setup_unknown_type_table(database: str, table_name: str) -> None:
    client = _direct_client()
    try:
        client.command(f"CREATE DATABASE IF NOT EXISTS `{database}`")
        client.command(f"DROP TABLE IF EXISTS `{database}`.`{table_name}`")
        client.command(
            f"CREATE TABLE `{database}`.`{table_name}` "
            "(`id` UInt64, `state` AggregateFunction(sum, UInt64)) "
            "ENGINE = AggregatingMergeTree ORDER BY id"
        )
    finally:
        client.close()


def _write_table_name(side: Side, scenario: Scenario) -> str:
    return f"write_{side}_{scenario.fault}"


def _permission_user() -> str:
    if _IDENTIFIER.fullmatch(_PERMISSION_USER) is None:
        raise RuntimeError("isolated permission fixture has an unsafe user name")
    return _PERMISSION_USER


def _setup_permission_fixture() -> None:
    client = _direct_client()
    try:
        username = _permission_user()
        client.command(f"DROP USER IF EXISTS `{username}`")
        client.command(f"CREATE USER `{username}` IDENTIFIED WITH no_password")
    finally:
        client.close()


def cleanup_permission_fixture() -> None:
    client = _direct_client()
    try:
        client.command(f"DROP USER IF EXISTS `{_permission_user()}`")
    finally:
        client.close()


def prepare_scenario(
    *,
    side: Side,
    scenario: Scenario,
    mode: RunMode,
    expected_output: Path,
) -> dict[str, int | str]:
    """Create immutable fixtures outside every measured terminal action."""
    database = "ray_clickhouse_comparison"
    zero = "0" * 64
    identity: dict[str, int | str] = {
        "row_count": 0,
        "schema_sha256": zero,
        "multiset_sha256": zero,
    }
    if scenario.terminal_action == "write":
        _setup_write_table(database, _write_table_name(side, scenario))
    elif scenario.id == "contract.unknown_type":
        _setup_unknown_type_table(database, "unknown_type_table")
    elif scenario.id == "read.error.object_not_found":
        client = _direct_client(database)
        try:
            client.command("DROP TABLE IF EXISTS `missing_table`")
        finally:
            client.close()
    elif scenario.id == "read.error.transport":
        pass
    else:
        rows, payload_bytes = _fixture_shape(scenario, mode)
        fixture = _setup_read_fixture(
            database,
            "read_fixture",
            rows,
            payload_bytes=payload_bytes,
        )
        identity = correctness_identity(fixture)
    if scenario.id == "read.error.permission":
        _setup_permission_fixture()
    atomic_write_json(expected_output, identity)
    return identity


def _consume(dataset: Any, action: str) -> tuple[dict[str, int | str], dict[str, int]]:
    source = dataset.materialize() if action == "materialize" else dataset
    accumulator = CorrectnessAccumulator()
    block_count = 0
    total_rows = 0
    total_bytes = 0
    max_rows = 0
    max_bytes = 0
    for block in source.iter_batches(
        batch_size=None,
        prefetch_batches=0,
        batch_format="pyarrow",
    ):
        if isinstance(block, pa.RecordBatch):
            table = pa.Table.from_batches([block])
        elif isinstance(block, pa.Table):
            table = block
        else:
            raise TypeError(f"Ray yielded unsupported block type: {type(block).__name__}")
        accumulator.update(table)
        block_count += 1
        total_rows += table.num_rows
        total_bytes += table.nbytes
        max_rows = max(max_rows, table.num_rows)
        max_bytes = max(max_bytes, table.nbytes)
    if block_count == 0:
        raise RuntimeError("comparison fixture unexpectedly produced no blocks")
    return accumulator.finish(), {
        "block_count": block_count,
        "total_rows": total_rows,
        "total_bytes": total_bytes,
        "max_block_rows": max_rows,
        "max_block_bytes": max_bytes,
    }


def _consume_error_path(dataset: Any) -> int:
    block_count = 0
    for block in dataset.iter_batches(
        batch_size=None,
        prefetch_batches=0,
        batch_format="pyarrow",
    ):
        if not isinstance(block, (pa.Table, pa.RecordBatch)):
            raise TypeError(f"Ray yielded unsupported block type: {type(block).__name__}")
        block_count += 1
    return block_count


def _official_read(
    database: str,
    table_name: str,
    scenario: Scenario,
    reference: ReferenceConfig,
    log_comment: str,
    *,
    username: str = "default",
    password: str = "",
    port: int | None = None,
    filter_sql: str | None = None,
    timeout_seconds: float | None = None,
) -> Any:
    ray_data: Any = importlib.import_module("ray.data")

    kwargs: dict[str, Any] = {
        "table": f"{database}.{table_name}",
        "dsn": _dsn(database, username=username, password=password, port=port),
        "client_settings": {"log_comment": log_comment},
    }
    if scenario.profile == "controlled":
        effective_timeout = timeout_seconds or reference.scenario_timeout_seconds
        kwargs["client_settings"].update(
            max_threads=reference.resources.clickhouse_max_threads,
            max_execution_time=max(1, round(effective_timeout)),
        )
        kwargs.update(
            client_kwargs={
                "connect_timeout": effective_timeout,
                "send_receive_timeout": effective_timeout,
                "query_retries": 0,
            },
            concurrency=reference.resources.ray_workers,
            override_num_blocks=reference.resources.ray_workers,
            num_cpus=reference.resources.cpus_per_worker,
            memory=reference.resources.task_memory_bytes,
        )
        if scenario.unique_order_key is not None:
            kwargs["order_by"] = ([scenario.unique_order_key], False)
    if filter_sql is not None:
        kwargs["filter"] = filter_sql
    if scenario.columns is not None:
        kwargs["columns"] = list(scenario.columns)
    return ray_data.read_clickhouse(**kwargs)


def _external_read(
    database: str,
    table_name: str,
    scenario: Scenario,
    reference: ReferenceConfig,
    log_comment: str,
    *,
    username: str = "default",
    password: str = "",
    port: int | None = None,
    filter_sql: str | None = None,
    timeout_seconds: float | None = None,
) -> Any:
    ray_clickhouse: Any = importlib.import_module("ray_clickhouse")

    kwargs: dict[str, Any] = {
        "host": _proxy_host(),
        "port": port or _proxy_port(),
        "database": database,
        "table": table_name,
        "username": username,
        "password": password,
        "settings": {"log_comment": log_comment},
    }
    if scenario.profile == "controlled":
        effective_timeout = timeout_seconds or reference.scenario_timeout_seconds
        kwargs["settings"].update(max_threads=reference.resources.clickhouse_max_threads)
        kwargs.update(
            concurrency=reference.resources.ray_workers,
            override_num_blocks=reference.resources.ray_workers,
            batch_rows=reference.resources.external_batch_rows,
            batch_bytes=reference.resources.external_batch_bytes,
            query_timeout_seconds=effective_timeout,
            connect_timeout_seconds=effective_timeout,
            num_cpus=reference.resources.cpus_per_worker,
            memory=reference.resources.task_memory_bytes,
        )
        if scenario.split == "partition":
            kwargs.update(
                split="partition",
                target_tasks=reference.resources.ray_workers,
                max_tasks=reference.resources.ray_workers,
            )
        elif scenario.split == "range":
            kwargs.update(
                split="range",
                range_column=scenario.unique_order_key,
                target_tasks=reference.resources.ray_workers,
                max_tasks=reference.resources.ray_workers,
            )
        if scenario.unique_order_key is not None and scenario.split == "single":
            kwargs["order_by"] = ([scenario.unique_order_key], False)
    if filter_sql is not None:
        kwargs["filter"] = filter_sql
    if scenario.columns is not None:
        kwargs["columns"] = list(scenario.columns)
    return ray_clickhouse.read_clickhouse(**kwargs)


def classify_query_role(query: str) -> str:
    normalized = " ".join(query.lower().split())
    if normalized.startswith(("describe ", "desc ", "show ", "exists ", "explain ")):
        return "planning"
    if " from system." in normalized:
        return "planning"
    if " limit 0" in normalized:
        return "planning"
    if "count(" in normalized or "sum(bytesize(" in normalized:
        return "estimate"
    if " limit 100" in normalized or "fetch first 100" in normalized:
        return "sample"
    if normalized.startswith("insert "):
        return "data"
    if normalized.startswith("select "):
        return "data"
    return "unclassified"


def _query_metrics(log_comment: str, scenario: Scenario) -> dict[str, int | str]:
    client = _direct_client("system")
    try:
        client.command("SYSTEM FLUSH LOGS")
        result = client.query(
            """
            SELECT query_id, toString(type), read_rows, read_bytes,
                   query_duration_ms, memory_usage, query
            FROM system.query_log
            WHERE type IN ('QueryFinish', 'ExceptionBeforeStart', 'ExceptionWhileProcessing')
              AND log_comment = {tag:String}
            ORDER BY event_time_microseconds, query_id
            """,
            parameters={"tag": log_comment},
        )
    finally:
        client.close()
    rows = result.result_rows
    evidence_rows = [
        {
            "query_id": str(row[0]),
            "event_type": str(row[1]),
            "log_comment": log_comment,
            "role": classify_query_role(str(row[6])),
            "read_rows": int(row[2]),
            "read_bytes": int(row[3]),
            "duration_ms": int(row[4]),
            "memory_usage": int(row[5]),
            "query": str(row[6]),
        }
        for row in rows
    ]
    evidence_path = os.environ.get("RAY_COMPARISON_QUERY_EVIDENCE")
    if evidence_path:
        write_jsonl(Path(evidence_path), evidence_rows)
    metrics: dict[str, int | str] = {
        "query_count": len(rows),
        "read_rows": sum(int(row[2]) for row in rows),
        "read_bytes": sum(int(row[3]) for row in rows),
        "query_duration_ms": sum(int(row[4]) for row in rows),
        "clickhouse_peak_memory_bytes": max((int(row[5]) for row in rows), default=0),
        "query_ids": ",".join(str(row[0]) for row in rows),
        "unclassified_query_count": sum(row["role"] == "unclassified" for row in evidence_rows),
    }
    observed_roles = {str(row["role"]) for row in evidence_rows}
    unexpected = sorted(observed_roles - set(scenario.query_roles))
    metrics["unexpected_query_roles"] = ",".join(unexpected)
    for role in scenario.query_roles:
        role_rows = [row for row in evidence_rows if row["role"] == role]
        metrics[f"query_count.{role}"] = len(role_rows)
        metrics[f"query_finish_count.{role}"] = sum(
            row["event_type"] == "QueryFinish" for row in role_rows
        )
        metrics[f"read_rows.{role}"] = sum(int(str(row["read_rows"])) for row in role_rows)
        metrics[f"read_bytes.{role}"] = sum(int(str(row["read_bytes"])) for row in role_rows)
    return metrics


def _task_metrics(
    evidence_path: Path | None,
    *,
    require_task_attempt: bool = True,
    task_name: str | None = None,
) -> dict[str, int | str]:
    ray: Any = importlib.import_module("ray")
    state: Any = importlib.import_module("ray.util.state")
    job_id = ray.get_runtime_context().get_job_id()
    tasks: list[Any] = []
    attempts = 40 if require_task_attempt else 1
    for _ in range(attempts):
        tasks = state.list_tasks(
            filters=[("job_id", "=", job_id)],
            limit=10_000,
            timeout=10,
            detail=True,
            raise_on_missing_output=True,
        )
        normal_tasks = [task for task in tasks if str(getattr(task, "type", "")) == "NORMAL_TASK"]
        selected_tasks = [
            task
            for task in normal_tasks
            if task_name is None or str(getattr(task, "name", "")) == task_name
        ]
        if selected_tasks and (
            not require_task_attempt
            or all(
                str(getattr(task, "state", "")) in {"FINISHED", "FAILED"} for task in selected_tasks
            )
        ):
            break
        time.sleep(0.25)
    rows = [
        {
            "task_id": str(getattr(task, "task_id", "")),
            "attempt_number": int(getattr(task, "attempt_number", 0)),
            "name": str(getattr(task, "name", "")),
            "state": str(getattr(task, "state", "")),
            "type": str(getattr(task, "type", "")),
            "node_id": str(getattr(task, "node_id", "") or ""),
            "worker_id": str(getattr(task, "worker_id", "") or ""),
        }
        for task in tasks
        if str(getattr(task, "type", "")) == "NORMAL_TASK"
        and (task_name is None or str(getattr(task, "name", "")) == task_name)
    ]
    if evidence_path is not None:
        write_jsonl(evidence_path, rows)
    task_ids = {str(row["task_id"]) for row in rows if row["task_id"]}
    return {
        "ray_task_attempt_count": len(rows),
        "ray_unique_task_count": len(task_ids),
        "ray_max_task_attempt_number": max(
            (int(str(row["attempt_number"])) for row in rows), default=0
        ),
        "ray_failed_task_attempt_count": sum(row["state"] == "FAILED" for row in rows),
        "ray_task_states": ",".join(
            str(row["state"])
            for row in sorted(
                rows,
                key=lambda row: (str(row["task_id"]), int(str(row["attempt_number"]))),
            )
        ),
        "ray_task_ids": ",".join(sorted(task_ids)),
    }


def _exception_record(exc: BaseException) -> dict[str, str]:
    return {
        "type": type(exc).__name__,
        "module": type(exc).__module__,
        "message": str(exc),
        "traceback": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
    }


def validate_smoke_result(document: dict[str, Any], scenario: Scenario, mode: RunMode) -> None:
    metrics = document.get("metrics")
    if not isinstance(metrics, dict):
        raise ValueError("result metrics are unavailable")
    if ".error." in scenario.id or scenario.id == "contract.unknown_type":
        if metrics.get("outcome") not in {"exception", "empty_dataset"}:
            raise ValueError("expected-error scenario did not expose an error or empty outcome")
        return
    if scenario.fault == "none":
        if document.get("status") not in {"valid", "invalid", "failed"}:
            raise ValueError("normal-path result did not complete")
        if int(metrics.get("query_count", 0)) < 1:
            raise ValueError("normal-path result has no attributed ClickHouse query")
        if int(metrics.get("unclassified_query_count", 0)) != 0:
            raise ValueError("normal-path result has unclassified ClickHouse queries")
        if metrics.get("unexpected_query_roles"):
            raise ValueError("normal-path result has undeclared ClickHouse query roles")
        if mode == "smoke" and document.get("status") != "valid":
            raise ValueError("local smoke normal path did not pass correctness")
        return
    if document.get("status") != "valid" or metrics.get("fault_observed") is not True:
        raise ValueError("one-shot fault boundary was not proven")


def _mark_measurement_complete() -> None:
    path = os.environ.get("RAY_COMPARISON_MEASUREMENT_COMPLETE")
    if path is not None:
        atomic_write_json(Path(path), {"completed_at": time.time()})


def _mark_measurement_started() -> None:
    baseline_path = os.environ.get("RAY_COMPARISON_RESOURCE_BASELINE_READY")
    if baseline_path is not None:
        deadline = time.monotonic() + 30
        while not Path(baseline_path).is_file() and time.monotonic() < deadline:
            time.sleep(0.05)
        if not Path(baseline_path).is_file():
            raise RuntimeError("resource sampler did not complete its baseline handshake")
    path = os.environ.get("RAY_COMPARISON_MEASUREMENT_STARTED")
    if path is not None:
        atomic_write_json(Path(path), {"started_at": time.time()})


def _record_driver_pid() -> None:
    path = os.environ.get("RAY_COMPARISON_DRIVER_PID_FILE")
    if path is not None:
        pid_path = Path(path)
        pid_path.parent.mkdir(parents=True, exist_ok=True)
        pid_path.write_text(f"{os.getpid()}\n", encoding="utf-8")


def _pair_position() -> int:
    raw = os.environ.get("RAY_COMPARISON_PAIR_POSITION", "0")
    position = int(raw)
    if position < 0:
        raise ValueError("pair position must be non-negative")
    return position


def _exception_type_names(exc: BaseException) -> tuple[str, ...]:
    names: list[str] = []
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        names.append(type(current).__name__)
        ray_cause = getattr(current, "cause", None)
        if isinstance(ray_cause, BaseException) and id(ray_cause) not in seen:
            current = ray_cause
        else:
            current = current.__cause__ or current.__context__
    return tuple(names)


def _read_result(
    side: Side,
    scenario: Scenario,
    reference: ReferenceConfig,
    run_id: str,
    repetition: int,
    expected_identity: dict[str, Any],
) -> dict[str, Any]:
    database = "ray_clickhouse_comparison"
    table_name = "read_fixture"
    log_comment = (
        f"ray-clickhouse-comparison run={run_id} side={side} "
        f"scenario={scenario.id} repetition={repetition} role=data"
    )
    caught: Exception | None = None
    actual: dict[str, int | str] | None = None
    blocks: dict[str, int] = {}
    _mark_measurement_started()
    started = time.monotonic()
    try:
        with _scenario_timeout(reference.scenario_timeout_seconds):
            dataset = (
                _official_read(database, table_name, scenario, reference, log_comment)
                if side == "official"
                else _external_read(database, table_name, scenario, reference, log_comment)
            )
            actual, blocks = _consume(dataset, scenario.terminal_action)
    except ScenarioExecutionTimeoutError:
        raise
    except Exception as exc:
        caught = exc
    terminal_duration = time.monotonic() - started
    _mark_measurement_complete()
    query_metrics = _query_metrics(log_comment, scenario)
    task_metrics = _task_metrics(
        Path(os.environ["RAY_COMPARISON_TASK_EVIDENCE"])
        if "RAY_COMPARISON_TASK_EVIDENCE" in os.environ
        else None
    )
    metrics: dict[str, int | float | str | bool | None] = {
        **blocks,
        **query_metrics,
        **task_metrics,
        "duration_seconds": terminal_duration,
        "expected_multiset_sha256": expected_identity["multiset_sha256"],
        "pair_position": _pair_position(),
    }
    zero = "0" * 64
    actual = actual or {"row_count": 0, "schema_sha256": zero, "multiset_sha256": zero}
    attribution_valid = (
        int(query_metrics["query_count"]) > 0
        and int(query_metrics["unclassified_query_count"]) == 0
        and not query_metrics["unexpected_query_roles"]
        and int(task_metrics["ray_task_attempt_count"]) > 0
    )
    if caught is not None:
        status = "failed"
        reason = "public read terminal action failed"
    elif actual != expected_identity or not attribution_valid:
        status = "invalid"
        reason = "fixture correctness or query attribution mismatch"
    else:
        status = "valid"
        reason = None
    return {
        "schema_version": 1,
        "run_id": run_id,
        "scenario_id": scenario.id,
        "side": side,
        "repetition": repetition,
        "status": status,
        "reason": reason,
        "correctness": actual,
        "metrics": metrics,
        "exception": _exception_record(caught) if caught is not None else None,
    }


def _read_error_result(
    side: Side,
    scenario: Scenario,
    reference: ReferenceConfig,
    run_id: str,
    repetition: int,
) -> dict[str, Any]:
    database = "ray_clickhouse_comparison"
    error_kind = scenario.id.rsplit(".", 1)[-1]
    table_name = "read_fixture"
    if error_kind == "unknown_type":
        table_name = "unknown_type_table"
    if error_kind == "object_not_found":
        table_name = "missing_table"
    username = "nonexistent_comparison_user" if error_kind == "authentication" else "default"
    if error_kind == "permission":
        username = _permission_user()
    port = 1 if error_kind == "transport" else None
    filter_sql = "sleepEachRow(0.01) = 0" if error_kind == "timeout" else None
    timeout_seconds = 1.0 if error_kind in {"timeout", "transport"} else 10.0
    log_comment = (
        f"ray-clickhouse-comparison run={run_id} side={side} "
        f"scenario={scenario.id} repetition={repetition} role=error"
    )
    caught: Exception | None = None
    block_count = 0
    _mark_measurement_started()
    started = time.monotonic()
    try:
        with _scenario_timeout(reference.scenario_timeout_seconds):
            dataset = (
                _official_read(
                    database,
                    table_name,
                    scenario,
                    reference,
                    log_comment,
                    username=username,
                    port=port,
                    filter_sql=filter_sql,
                    timeout_seconds=timeout_seconds,
                )
                if side == "official"
                else _external_read(
                    database,
                    table_name,
                    scenario,
                    reference,
                    log_comment,
                    username=username,
                    port=port,
                    filter_sql=filter_sql,
                    timeout_seconds=timeout_seconds,
                )
            )
            block_count = _consume_error_path(dataset)
    except ScenarioExecutionTimeoutError:
        raise
    except Exception as exc:
        caught = exc
    terminal_duration = time.monotonic() - started
    _mark_measurement_complete()
    query_metrics = _query_metrics(log_comment, scenario)
    task_metrics = _task_metrics(
        Path(os.environ["RAY_COMPARISON_TASK_EVIDENCE"])
        if "RAY_COMPARISON_TASK_EVIDENCE" in os.environ
        else None,
        require_task_attempt=False,
    )
    expected_external = {
        "authentication": "AuthenticationError",
        "permission": "PermissionError",
        "timeout": "TransportError",
        "object_not_found": "ObjectNotFoundError",
        "transport": "TransportError",
        "unknown_type": "SchemaError",
    }
    chain = _exception_type_names(caught) if caught is not None else ()
    outcome = (
        "exception" if caught is not None else ("empty_dataset" if block_count == 0 else "data")
    )
    classification_matches = not query_metrics["unexpected_query_roles"] and (
        expected_external[error_kind] in chain
        if side == "external"
        else outcome in {"exception", "empty_dataset"}
    )
    zero = "0" * 64
    return {
        "schema_version": 1,
        "run_id": run_id,
        "scenario_id": scenario.id,
        "side": side,
        "repetition": repetition,
        "status": "valid" if classification_matches else "invalid",
        "reason": None if classification_matches else "public exception classification mismatch",
        "correctness": {"row_count": 0, "schema_sha256": zero, "multiset_sha256": zero},
        "metrics": {
            **query_metrics,
            **task_metrics,
            "duration_seconds": terminal_duration,
            "expected_error": expected_external[error_kind] if side == "external" else "exception",
            "exception_chain": ",".join(chain),
            "outcome": outcome,
            "block_count": block_count,
            "pair_position": _pair_position(),
        },
        "exception": _exception_record(caught) if caught is not None else None,
    }


def _write_dataset(
    side: Side,
    database: str,
    table_name: str,
    log_comment: str,
    scenario: Scenario,
    reference: ReferenceConfig,
) -> None:
    ray_data: Any = importlib.import_module("ray.data")

    table = pa.table({"id": pa.array([1001, 1002], type=pa.int64()), "payload": ["a", "b"]})
    dataset = ray_data.from_arrow(table).repartition(1)
    if side == "official":
        kwargs: dict[str, Any] = {
            "mode": ray_data.SinkMode.APPEND,
            "client_settings": {"log_comment": log_comment},
        }
        if scenario.profile == "controlled":
            kwargs["client_settings"].update(max_threads=reference.resources.clickhouse_max_threads)
            kwargs["client_kwargs"] = {
                "connect_timeout": reference.scenario_timeout_seconds,
                "send_receive_timeout": reference.scenario_timeout_seconds,
                "query_retries": 0,
            }
        if scenario.write_retry == "zero":
            kwargs["ray_remote_args"] = {"max_retries": 0}
        if scenario.fault == "hold_response":
            kwargs.setdefault("ray_remote_args", {})["num_cpus"] = 1
        dataset.write_clickhouse(f"{database}.{table_name}", _dsn(database), **kwargs)
        return
    ray_clickhouse: Any = importlib.import_module("ray_clickhouse")

    settings: dict[str, Any] = {"log_comment": log_comment}
    kwargs = {
        "host": _proxy_host(),
        "port": _proxy_port(),
        "database": database,
        "table": table_name,
        "settings": settings,
    }
    if scenario.profile == "controlled":
        settings["max_threads"] = reference.resources.clickhouse_max_threads
        kwargs.update(
            connect_timeout_seconds=reference.scenario_timeout_seconds,
            query_timeout_seconds=reference.scenario_timeout_seconds,
        )
    if scenario.fault == "hold_response":
        kwargs["ray_remote_args"] = {"num_cpus": 1}
    ray_clickhouse.write_clickhouse(dataset, **kwargs)


def _expected_worker_loss(
    side: Side, scenario: Scenario
) -> tuple[int, int, int, str, int, int] | None:
    """Return the exact one-shot worker-loss contract for the public paths."""
    if scenario.fault != "hold_response":
        return None
    if side == "official" and scenario.write_retry == "default":
        return (2, 1, 1, "FAILED,FINISHED", 4, 4)
    if side == "official":
        return (1, 1, 0, "FAILED", 2, 2)
    return (1, 1, 0, "FAILED", 1, 2)


def _write_result(
    side: Side,
    scenario: Scenario,
    reference: ReferenceConfig,
    run_id: str,
    repetition: int,
    control_dir: Path,
) -> dict[str, Any]:
    database = "ray_clickhouse_comparison"
    table_name = _write_table_name(side, scenario)
    log_comment = (
        f"ray-clickhouse-comparison run={run_id} side={side} "
        f"scenario={scenario.id} repetition={repetition} role=insert"
    )
    token = f"{run_id}-{side}-{scenario.fault}-{repetition}"
    FaultController(control_dir).arm(cast(FaultMode, scenario.fault), token)
    caught: Exception | None = None
    _mark_measurement_started()
    started = time.monotonic()
    try:
        with _scenario_timeout(reference.scenario_timeout_seconds):
            _write_dataset(side, database, table_name, log_comment, scenario, reference)
    except ScenarioExecutionTimeoutError:
        raise
    except Exception as exc:
        caught = exc
    terminal_duration = time.monotonic() - started
    _mark_measurement_complete()
    task_metrics = _task_metrics(
        Path(os.environ["RAY_COMPARISON_TASK_EVIDENCE"])
        if "RAY_COMPARISON_TASK_EVIDENCE" in os.environ
        else None,
        task_name="Write",
    )
    client = _direct_client(database)
    try:
        inserted_rows = int(client.query(f"SELECT count() FROM `{table_name}`").result_rows[0][0])
    finally:
        client.close()
    event_path = control_dir / "fault-event.json"
    event = json.loads(event_path.read_text(encoding="utf-8")) if event_path.exists() else None
    boundary_valid = (
        isinstance(event, dict)
        and event.get("token") == token
        and event.get("mode") == scenario.fault
        and event.get("client_ip")
        in (
            {"10.251.0.11", "10.251.0.12"}
            if scenario.fault == "hold_response"
            else {"10.251.0.10", "10.251.0.11", "10.251.0.12"}
        )
    )
    query_metrics = _query_metrics(log_comment, scenario)
    chain = _exception_type_names(caught) if caught is not None else ()
    worker_kill_observed = (
        scenario.fault != "hold_response" or (control_dir.parent / "killed-worker.txt").is_file()
    )
    metrics: dict[str, int | float | str | bool | None] = {
        **query_metrics,
        **task_metrics,
        "duration_seconds": terminal_duration,
        "inserted_rows": inserted_rows,
        "fault_observed": boundary_valid,
        "fault_mode": scenario.fault,
        "fault_event_count": 1 if isinstance(event, dict) else 0,
        "exception_observed": caught is not None,
        "exception_chain": ",".join(chain),
        "worker_kill_observed": worker_kill_observed,
        "server_insert_attempt_count": int(query_metrics.get("query_finish_count.data", 0)),
        "write_retry_mode": scenario.write_retry,
        "non_replicated_deduplication_window": 0,
        "pair_position": _pair_position(),
    }
    exception_contract = True
    if side == "external" and scenario.fault == "drop_response":
        exception_contract = "AmbiguousWriteError" in chain
    if scenario.write_retry == "zero":
        exception_contract = exception_contract and caught is not None
    attribution_valid = (
        int(query_metrics["query_count"]) > 0
        and int(query_metrics["unclassified_query_count"]) == 0
        and not query_metrics["unexpected_query_roles"]
        and int(query_metrics.get("query_finish_count.data", 0)) > 0
    )
    expected_worker_loss = _expected_worker_loss(side, scenario)
    task_attempts_valid = int(task_metrics["ray_task_attempt_count"]) > 0
    if expected_worker_loss is not None:
        (
            expected_attempts,
            expected_failed_attempts,
            expected_max_attempt,
            expected_states,
            expected_insert_attempts,
            expected_rows,
        ) = expected_worker_loss
        task_attempts_valid = (
            int(task_metrics["ray_task_attempt_count"]) == expected_attempts
            and int(task_metrics["ray_failed_task_attempt_count"]) == expected_failed_attempts
            and int(task_metrics["ray_max_task_attempt_number"]) == expected_max_attempt
            and str(task_metrics["ray_task_states"]) == expected_states
            and int(query_metrics.get("query_finish_count.data", 0)) == expected_insert_attempts
            and inserted_rows == expected_rows
        )
        metrics.update(
            {
                "expected_write_task_attempt_count": expected_attempts,
                "expected_write_failed_task_attempt_count": expected_failed_attempts,
                "expected_write_max_task_attempt_number": expected_max_attempt,
                "expected_write_task_states": expected_states,
                "expected_server_insert_attempt_count": expected_insert_attempts,
                "expected_inserted_rows": expected_rows,
            }
        )
    status = (
        "valid"
        if boundary_valid
        and inserted_rows >= 2
        and exception_contract
        and attribution_valid
        and task_attempts_valid
        and worker_kill_observed
        else "invalid"
    )
    reason = (
        None
        if status == "valid"
        else "one-shot fault, exception, or query attribution boundary was not proven"
    )
    zero = "0" * 64
    return {
        "schema_version": 1,
        "run_id": run_id,
        "scenario_id": scenario.id,
        "side": side,
        "repetition": repetition,
        "status": status,
        "reason": reason,
        "correctness": {
            "row_count": inserted_rows,
            "schema_sha256": zero,
            "multiset_sha256": zero,
        },
        "metrics": metrics,
        "exception": _exception_record(caught) if caught is not None else None,
    }


def warmup_scenario(
    *,
    side: Side,
    scenario: Scenario,
    reference: ReferenceConfig,
    run_id: str,
) -> None:
    if not scenario.warmup:
        return
    ray: Any = importlib.import_module("ray")
    ray.init(address=_required_env("RAY_COMPARISON_RAY_ADDRESS"), ignore_reinit_error=False)
    try:
        for repetition in range(reference.warmup_repetitions):
            log_comment = (
                f"ray-clickhouse-comparison run={run_id} side={side} "
                f"scenario={scenario.id} repetition={repetition} role=warmup"
            )
            with _scenario_timeout(reference.scenario_timeout_seconds):
                dataset = (
                    _official_read(
                        "ray_clickhouse_comparison",
                        "read_fixture",
                        scenario,
                        reference,
                        log_comment,
                    )
                    if side == "official"
                    else _external_read(
                        "ray_clickhouse_comparison",
                        "read_fixture",
                        scenario,
                        reference,
                        log_comment,
                    )
                )
                _consume(dataset, scenario.terminal_action)
    finally:
        ray.shutdown()


def execute(
    *,
    side: Side,
    scenario: Scenario,
    reference: ReferenceConfig,
    run_id: str,
    repetition: int,
    output: Path,
    result_schema: Path,
    control_dir: Path,
    expected_identity_path: Path,
) -> dict[str, Any]:
    _record_driver_pid()
    ray: Any = importlib.import_module("ray")
    expected_identity = json.loads(expected_identity_path.read_text(encoding="utf-8"))
    if not isinstance(expected_identity, dict):
        raise ValueError("prepared correctness identity must be an object")
    if side not in scenario.sides:
        raise ValueError(f"scenario {scenario.id!r} does not declare side {side!r}")
    ray.init(address=_required_env("RAY_COMPARISON_RAY_ADDRESS"), ignore_reinit_error=False)
    try:
        if ".error." in scenario.id or scenario.id == "contract.unknown_type":
            result = _read_error_result(side, scenario, reference, run_id, repetition)
        elif scenario.terminal_action == "write":
            result = _write_result(side, scenario, reference, run_id, repetition, control_dir)
        else:
            result = _read_result(
                side,
                scenario,
                reference,
                run_id,
                repetition,
                expected_identity,
            )
        validate_document(result, load_schema(result_schema))
        atomic_write_json(output, result)
        return result
    finally:
        ray.shutdown()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--side", choices=("official", "external"), required=True)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--scenarios", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--repetition", type=int, default=0)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--result-schema", type=Path, required=True)
    parser.add_argument("--control-dir", type=Path, required=True)
    parser.add_argument("--expected-identity", type=Path, required=True)
    args = parser.parse_args(argv)
    execute(
        side=args.side,
        scenario=_load_scenario(args.scenarios, args.scenario),
        reference=load_reference(args.reference),
        run_id=args.run_id,
        repetition=args.repetition,
        output=args.output,
        result_schema=args.result_schema,
        control_dir=args.control_dir,
        expected_identity_path=args.expected_identity,
    )


if __name__ == "__main__":
    main()
