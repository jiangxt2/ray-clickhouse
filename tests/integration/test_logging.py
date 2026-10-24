"""Real client and worker evidence for events, confirmation and diagnostic lookup."""

from __future__ import annotations

import json
import uuid
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pytest
import ray
from log_capture import capture_events, execute_read_task

from ray_clickhouse._errors import ReadError, TransportError, WriteError
from ray_clickhouse._logging import LogContext
from ray_clickhouse._models import (
    ClickHouseConnection,
    QualifiedTable,
    QuerySpec,
    ResourceLimits,
)
from ray_clickhouse._query import ClickHouseQueryDatasource, QueryReadConfig
from ray_clickhouse._transport import stream_query
from ray_clickhouse.datasink import ClickHouseDataSink
from ray_clickhouse.datasource import ClickHouseDatasource, ClickHouseReadConfig

pytestmark = [pytest.mark.integration, pytest.mark.ray]


@pytest.fixture
def logging_ray():
    owned = not ray.is_initialized()
    if owned:
        ray.init(address="local", include_dashboard=False, num_cpus=2)
    try:
        yield
    finally:
        if owned:
            ray.shutdown()


@pytest.mark.parametrize("read_mode", ["table", "query"])
def test_read_events_reach_real_worker(
    logging_ray,
    clickhouse_client,
    connection_options,
    table_name,
    monkeypatch,
    read_mode,
    record_property,
):
    database = connection_options["database"]
    clickhouse_client.command(
        f"CREATE TABLE {database}.{table_name} (id UInt64) ENGINE=MergeTree ORDER BY id"
    )
    clickhouse_client.command(f"INSERT INTO {database}.{table_name} VALUES (1),(2),(3)")
    monkeypatch.setenv("RAY_CLICKHOUSE_LOG_BATCHES", "1")
    connection = ClickHouseConnection(**connection_options)
    with capture_events() as driver:
        if read_mode == "table":
            source = ClickHouseDatasource(
                ClickHouseReadConfig(
                    connection,
                    QualifiedTable(database, table_name),
                    limits=ResourceLimits(batch_rows=1),
                )
            )
        else:
            source = ClickHouseQueryDatasource(
                QueryReadConfig(
                    connection,
                    f"SELECT id FROM {database}.{table_name}",
                    limits=ResourceLimits(batch_rows=1),
                )
            )
        tasks = source.get_read_tasks(1)
    rows, worker = ray.get(
        ray.remote(max_retries=0)(execute_read_task).remote(tasks[0])
    )
    assert sorted(row["id"] for row in rows) == [1, 2, 3]
    plans = [r for r in driver if r["event"] == "clickhouse.plan.finished"]
    final = [r for r in worker if r["event"] == "clickhouse.read.finished"]
    assert len(final) == 1 and final[0]["status"] == "succeeded"
    assert final[0]["emitted_rows"] == 3 and final[0]["arrow_bytes"] == 24
    assert final[0]["plan_id"] == plans[-1]["plan_id"]
    assert final[0]["operation_id"] == source._log_context.operation_id
    assert final[0]["ray_task_id"] and final[0]["ray_worker_id"]
    assert len([r for r in worker if r["event"] == "clickhouse.read.progress"]) == 1
    if read_mode == "query":
        assert all("engine" not in r and "split_mode" not in r for r in driver)
    assert table_name not in repr(driver + worker)
    record_property("connector_events", json.dumps(driver + worker))


def _write_on_worker(sink, table):
    with capture_events() as records:
        result = sink.write([table], None)
    return result, records


@pytest.mark.parametrize("insert_mode", ["sync", "async"])
def test_write_confirmation_and_local_insert_identity(
    logging_ray,
    clickhouse_client,
    connection_options,
    table_name,
    monkeypatch,
    record_property,
    insert_mode,
):
    database = connection_options["database"]
    clickhouse_client.command(
        f"CREATE TABLE {database}.{table_name} (id UInt64) ENGINE=MergeTree ORDER BY id"
    )
    monkeypatch.setenv("RAY_CLICKHOUSE_LOG_BATCHES", "1")
    table = pa.table({"id": pa.array([1, 2, 3], type=pa.uint64())})
    sink = ClickHouseDataSink(
        connection=ClickHouseConnection(**connection_options),
        table=QualifiedTable(database, table_name),
        insert_mode=insert_mode,
        limits=ResourceLimits(batch_rows=1),
    )
    with capture_events() as driver:
        sink.on_write_start(table.schema)
    summary, worker = ray.get(
        ray.remote(max_retries=0)(_write_on_worker).remote(sink, table)
    )
    with capture_events() as completion:
        sink.on_write_complete(SimpleNamespace(num_rows=3, write_returns=[summary]))
    assert sink.receipt.rows_written == 3
    assert (
        clickhouse_client.query(
            f"SELECT count() FROM {database}.{table_name}"
        ).first_row[0]
        == 3
    )
    first = [r for r in worker if r["event"] == "clickhouse.insert.finished"]
    assert len(first) == 1 and first[0]["status"] == "confirmed"
    assert "query_id" not in first[0] and first[0]["client_insert_id"]
    assert len([r for r in worker if r["event"] == "clickhouse.write.progress"]) == 1
    assert (
        completion[-1]["status"] == "confirmed"
        and completion[-1]["confirmed_rows"] == 3
    )
    assert {r["operation_id"] for r in driver + worker + completion} == {
        sink._log_context.operation_id
    }
    clickhouse_client.command("SYSTEM FLUSH LOGS")
    ids = {
        r[0]
        for r in clickhouse_client.query(
            "SELECT query_id FROM system.query_log "
            "WHERE type='QueryFinish' AND query_kind='Insert' "
            "AND position(query, %(table)s)>0",
            parameters={"table": table_name},
        ).result_rows
    }
    assert ids, "real INSERTs must have server log evidence"
    record_property(
        "client_insert_id_found_in_server_log", str(first[0]["client_insert_id"] in ids)
    )
    record_property("connector_events", json.dumps(driver + worker + completion))


def _failed_read(connection, flush):
    query = QuerySpec(
        "SELECT throwIf(1, 'logging failure test') AS marker",
        (),
        pa.schema([("marker", pa.uint8())]),
    )
    with capture_events() as records:
        try:
            list(
                stream_query(
                    connection,
                    query,
                    ResourceLimits(),
                    diagnostic_flush_logs=flush,
                    log_context=LogContext.create(),
                )
            )
        except (ReadError, TransportError) as error:
            return error.query_id, error.diagnostic, records
    raise AssertionError("expected a server-side query failure")


@pytest.mark.parametrize("flush", [False, True])
def test_restricted_query_log_retains_client_failure(
    logging_ray, clickhouse_client, connection_options, flush, record_property
):
    username = "logging_" + uuid.uuid4().hex[:12]
    clickhouse_client.command(f"CREATE USER {username} IDENTIFIED WITH no_password")
    try:
        connection = ClickHouseConnection(
            **(connection_options | {"username": username})
        )
        query_id, diagnostic, records = ray.get(
            ray.remote(max_retries=0)(_failed_read).remote(connection, flush)
        )
        assert query_id and diagnostic is None
        final = [r for r in records if r["event"] == "clickhouse.read.finished"]
        assert len(final) == 1 and final[0]["status"] == "failed"
        assert final[0]["query_id"] == query_id
        assert not [r for r in records if r["event"] == "clickhouse.read.first_block"]
        lookup = [r for r in records if r["event"] == "clickhouse.diagnostic.finished"]
        assert len(lookup) == 1 and lookup[0]["available"] is False
        assert "logging failure test" not in repr(records) and username not in repr(
            records
        )
        record_property("connector_events", json.dumps(records))
    finally:
        clickhouse_client.command(f"DROP USER IF EXISTS {username}")


def _write_bad_second_block(sink, first):
    with capture_events() as records:
        try:
            sink.write([first, pa.table({"id": ["private type mismatch"]})], None)
        except WriteError:
            return records
    raise AssertionError("expected second block schema validation to fail")


def test_review_fix_later_block_classification(
    logging_ray, clickhouse_client, connection_options, table_name, record_property
):
    database = connection_options["database"]
    clickhouse_client.command(
        f"CREATE TABLE {database}.{table_name} (id UInt64) ENGINE=MergeTree ORDER BY id"
    )
    table = pa.table({"id": pa.array([1, 2, 3], type=pa.uint64())})
    sink = ClickHouseDataSink(
        connection=ClickHouseConnection(**connection_options),
        table=QualifiedTable(database, table_name),
    )
    sink.on_write_start(table.schema)
    records = ray.get(
        ray.remote(max_retries=0)(_write_bad_second_block).remote(sink, table)
    )
    final = [r for r in records if r["event"] == "clickhouse.write.finished"]
    assert len(final) == 1 and final[0]["status"] == "failed"
    assert (
        final[0]["phase"] == "block_prepare"
        and final[0]["mismatch_category"] == "schema_validation"
    )
    assert final[0]["confirmed_rows"] == 3 and final[0]["batches"] == 1
    assert final[0]["expected_fields"] == final[0]["actual_fields"] == 1
    assert (
        clickhouse_client.query(
            f"SELECT count() FROM {database}.{table_name}"
        ).first_row[0]
        == 3
    )
    assert "private type mismatch" not in repr(records)
    record_property("connector_events", json.dumps(records))


def _real_stream_exit_failure(connection, table_name):
    import clickhouse_connect

    original = clickhouse_connect.get_client
    counts = {"open": 0, "exit": 0, "close": 0}
    instrumented = False

    class ExitFailure:
        def __init__(self, inner):
            self.inner = inner

        def __enter__(self):
            return self.inner.__enter__()

        def __exit__(self, *args):
            counts["exit"] += 1
            self.inner.__exit__(*args)
            raise RuntimeError("private injected exit failure")

    def get_client(*args, **kwargs):
        nonlocal instrumented
        client = original(*args, **kwargs)
        if not instrumented:
            instrumented = True
            open_stream, close = client.query_arrow_stream, client.close

            def wrapped_stream(*args, **kwargs):
                counts["open"] += 1
                return ExitFailure(open_stream(*args, **kwargs))

            def wrapped_close():
                counts["close"] += 1
                return close()

            client.query_arrow_stream, client.close = wrapped_stream, wrapped_close
        return client

    query = QuerySpec(
        f"SELECT id FROM {connection.database}.{table_name}",
        (),
        pa.schema([("id", pa.uint64())]),
    )
    with (
        capture_events() as records,
        patch("clickhouse_connect.get_client", side_effect=get_client),
    ):
        stream = stream_query(connection, query, ResourceLimits(batch_rows=1))
        next(stream)
        try:
            stream.close()
        except ReadError:
            return records, counts
    raise AssertionError("expected stream exit failure")


def test_review_fix_real_stream_exit_logging(
    logging_ray, clickhouse_client, connection_options, table_name, record_property
):
    database = connection_options["database"]
    clickhouse_client.command(
        f"CREATE TABLE {database}.{table_name} (id UInt64) ENGINE=MergeTree ORDER BY id"
    )
    clickhouse_client.command(f"INSERT INTO {database}.{table_name} VALUES (1),(2),(3)")
    records, counts = ray.get(
        ray.remote(max_retries=0)(_real_stream_exit_failure).remote(
            ClickHouseConnection(**connection_options), table_name
        )
    )
    final = [r for r in records if r["event"] == "clickhouse.read.finished"]
    cleanup = [r for r in records if r["event"] == "clickhouse.cleanup.failed"]
    assert len(final) == len(cleanup) == 1
    assert final[0]["phase"] == "cleanup" and final[0]["status"] == "failed"
    assert "mismatch_category" not in final[0]
    assert cleanup[0]["resource_kind"] == "read_stream"
    assert counts == {"open": 1, "exit": 1, "close": 1}
    assert final[0]["emitted_rows"] == 1
    assert "private injected exit failure" not in repr(records)
    record_property("connector_events", json.dumps(records))
