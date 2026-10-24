"""Events must remain useful without changing execution or exposing payloads."""

import itertools
import logging
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from ray import cloudpickle

from ray_clickhouse._ddl import create_or_replace_table
from ray_clickhouse._discovery import DiscoverySnapshot, PartitionInfo, RangeFacts
from ray_clickhouse._distributed import NotApplicable
from ray_clickhouse._errors import (
    AmbiguousTableManagementError,
    AmbiguousWriteError,
    ConfigurationError,
    PermissionError,
    ReadError,
    TransportError,
    WriteError,
)
from ray_clickhouse._logging import (
    LogContext,
    ProgressBudget,
    current_context,
    emit,
    runtime_fields,
    stage,
    use_context,
)
from ray_clickhouse._models import (
    ClickHouseConnection,
    QualifiedTable,
    QuerySpec,
    ResourceLimits,
)
from ray_clickhouse._query import ClickHouseQueryDatasource, QueryReadConfig
from ray_clickhouse._schema import SchemaPlan, TargetColumn, TargetTable
from ray_clickhouse._transport import (
    ClickHouseInsertSession,
    _observe_stream_cleanup,
    stream_query,
)
from ray_clickhouse.datasink import ClickHouseDataSink
from ray_clickhouse.datasource import ClickHouseDatasource, ClickHouseReadConfig

CONNECTION = ClickHouseConnection(host="example", database="analytics")
SCHEMA = pa.schema([pa.field("id", pa.uint64(), nullable=False)])
TABLE = pa.table({"id": pa.array([1, 2, 3], type=pa.uint64())}, schema=SCHEMA)
QUERY = QuerySpec("SELECT id FROM private_source", (), SCHEMA)
TARGET = TargetTable(
    "analytics", "events", "MergeTree", (TargetColumn("id", "UInt64", "", "", 1),)
)


class Stream:
    def __init__(self, tables, error=None, exit_error=None):
        self.tables = tables
        self.error = error
        self.exit_error = exit_error

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self.exit_error:
            raise self.exit_error
        return False

    def __iter__(self):
        yield from self.tables
        if self.error:
            raise self.error


@pytest.fixture(autouse=True)
def event_logging(caplog):
    caplog.set_level(logging.DEBUG, logger="ray_clickhouse")


def events(caplog, name=None):
    return [
        r
        for r in caplog.records
        if hasattr(r, "event") and (name is None or r.event == name)
    ]


@pytest.mark.parametrize("value,expected", [(None, False), ("0", False), ("1", True)])
def test_configuration_is_frozen_and_serializable(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv("RAY_CLICKHOUSE_LOG_BATCHES", raising=False)
    else:
        monkeypatch.setenv("RAY_CLICKHOUSE_LOG_BATCHES", value)
    source = ClickHouseDatasource(
        ClickHouseReadConfig(CONNECTION, QualifiedTable("analytics", "events"))
    )
    query = ClickHouseQueryDatasource(QueryReadConfig(CONNECTION, "SELECT 1 AS id"))
    sink = ClickHouseDataSink(
        connection=CONNECTION, table=QualifiedTable("analytics", "events")
    )
    monkeypatch.setenv("RAY_CLICKHOUSE_LOG_BATCHES", "invalid-secret")
    for obj in (source, query, sink):
        restored = cloudpickle.loads(cloudpickle.dumps(obj))
        assert restored._log_context == obj._log_context
        assert restored._log_context.batches is expected
        assert b"invalid-secret" not in cloudpickle.dumps(restored._log_context)
    with patch("clickhouse_connect.get_client") as client:
        with pytest.raises(ConfigurationError) as raised:
            LogContext.create(from_environment=True)
        assert "invalid-secret" not in str(raised.value)
        client.assert_not_called()
    assert not LogContext.create().batches


def test_rate_budget_uses_time_stride_and_hard_cap(monkeypatch):
    now = [0.0]
    monkeypatch.setattr("ray_clickhouse._logging.time.monotonic", lambda: now[0])
    budget = ProgressBudget(
        replace(LogContext.create(), batches=True),
        logging.getLogger("ray_clickhouse._transport"),
    )
    selected = []
    for index in range(1, 20_001):
        if index == 200:
            now[0] = 1.0
        elif index > 200 and index % 100 == 0:
            now[0] += 1.0
        if budget.admit():
            selected.append(index)
    assert selected[:3] == [1, 200, 300]
    assert len(selected) == 100
    assert budget.suppressed == 19_900


@pytest.mark.parametrize(
    "enabled,level", [(False, logging.DEBUG), (True, logging.INFO)]
)
def test_disabled_progress_never_reads_clock(monkeypatch, enabled, level):
    logger = logging.getLogger("ray_clickhouse.progress-test")
    monkeypatch.setattr(logger, "level", level)
    budget = ProgressBudget(replace(LogContext.create(), batches=enabled), logger)
    with patch(
        "ray_clickhouse._logging.time.monotonic", side_effect=AssertionError("clock")
    ):
        assert not budget.admit()
    assert budget.seen == 0


def test_allowlist_rejects_objects_raw_fields_and_multiline_identifiers(caplog):
    class Hostile:
        def __str__(self):
            raise AssertionError("must not stringify")

    logger = logging.getLogger("ray_clickhouse._transport")
    root_handlers = list(logging.getLogger().handlers)
    context = LogContext.create()
    emit(
        logger,
        "clickhouse.read.finished",
        context,
        phase="stream",
        status="failed",
        query="private SQL",
        password="credential",
        diagnostic={"exception": "payload"},
        query_id="injected\ncredential",
        error_type=Hostile(),
        emitted_rows=3,
        duration_ms=float("nan"),
    )
    record = events(caplog)[0]
    assert record.emitted_rows == 3
    for key in (
        "query",
        "password",
        "diagnostic",
        "query_id",
        "error_type",
        "duration_ms",
    ):
        assert key not in vars(record)
    assert record.fields_truncated
    assert logging.getLogger().handlers == root_handlers
    assert "credential" not in caplog.text and "private SQL" not in caplog.text


def test_handler_failure_does_not_change_read_or_cleanup():
    class Broken(logging.Handler):
        def emit(self, record):
            raise RuntimeError("broken collector")

    logger = logging.getLogger("ray_clickhouse._transport")
    handler = Broken()
    client = MagicMock()
    client.query_arrow_stream.return_value = Stream([TABLE])
    logger.addHandler(handler)
    try:
        with patch("clickhouse_connect.get_client", return_value=client):
            assert list(stream_query(CONNECTION, QUERY, ResourceLimits())) == [TABLE]
        client.close.assert_called_once()
        with patch(
            "clickhouse_connect.get_client",
            side_effect=RuntimeError("connection failed"),
        ):
            with pytest.raises(TransportError, match="initialization"):
                list(stream_query(CONNECTION, QUERY, ResourceLimits()))
        diagnostic = MagicMock()
        diagnostic.command.side_effect = RuntimeError("flush denied")
        client = MagicMock()
        client.query_arrow_stream.return_value = Stream(
            [], ReadError("original read failure")
        )
        with patch("clickhouse_connect.get_client", side_effect=[client, diagnostic]):
            with pytest.raises(ReadError, match="original read failure"):
                list(
                    stream_query(
                        CONNECTION, QUERY, ResourceLimits(), diagnostic_flush_logs=True
                    )
                )
        client.close.assert_called_once()
        diagnostic.close.assert_called_once()
    finally:
        logger.removeHandler(handler)


def test_context_scope_and_no_implicit_ray_start():
    context = LogContext.create()
    with use_context(context):
        assert current_context() is context
    assert current_context().operation_id != context.operation_id
    with (
        patch("ray.is_initialized", return_value=False),
        patch("ray.init") as initialize,
    ):
        assert runtime_fields(worker=True) == {}
        initialize.assert_not_called()


@pytest.mark.parametrize(
    "outcome",
    [
        "eof",
        "empty",
        "consumer_closed",
        "failure",
        "close_failure",
        "both_fail",
        "interrupt",
    ],
)
def test_read_terminal_counts_first_block_and_error_precedence(caplog, outcome):
    client = MagicMock()
    tables = [] if outcome == "empty" else [TABLE]
    error = (
        ReadError("source-private-data")
        if outcome in {"failure", "both_fail"}
        else (KeyboardInterrupt() if outcome == "interrupt" else None)
    )
    client.query_arrow_stream.return_value = Stream(tables, error)
    if outcome in {"close_failure", "both_fail"}:
        client.close.side_effect = RuntimeError("secret close error")
    with (
        patch("clickhouse_connect.get_client", return_value=client),
        patch(
            "ray_clickhouse._transport._lookup_query_diagnostic_impl", return_value=None
        ),
    ):
        stream = stream_query(CONNECTION, QUERY, ResourceLimits(batch_rows=2))
        if outcome == "consumer_closed":
            next(stream)
            stream.close()
        elif outcome in {"failure", "both_fail"}:
            with pytest.raises(ReadError, match="source-private-data"):
                list(stream)
        elif outcome == "close_failure":
            with pytest.raises(TransportError, match="close"):
                list(stream)
        elif outcome == "interrupt":
            with pytest.raises(KeyboardInterrupt):
                list(stream)
        else:
            list(stream)
    finished = events(caplog, "clickhouse.read.finished")
    assert len(finished) == 1
    final = finished[0]
    expected = {
        "eof": "succeeded",
        "empty": "succeeded",
        "consumer_closed": "consumer_closed",
        "interrupt": "cancelled",
    }.get(outcome, "failed")
    assert final.status == expected
    if outcome in {"consumer_closed", "interrupt"}:
        assert "mismatch_category" not in vars(final)
    if outcome not in {"close_failure", "both_fail"}:
        assert not events(caplog, "clickhouse.cleanup.failed")
    assert final.emitted_rows == (
        0 if outcome == "empty" else 2 if outcome == "consumer_closed" else 3
    )
    assert final.arrow_bytes == final.emitted_rows * 8
    assert len(events(caplog, "clickhouse.read.first_block")) == (outcome != "empty")
    assert not events(caplog, "clickhouse.read.progress")
    assert final.query_id == events(caplog, "clickhouse.read.started")[0].query_id
    assert "private" not in caplog.text and "secret" not in caplog.text
    client.close.assert_called_once()


def test_schema_only_output_has_no_first_data_latency(caplog):
    client = MagicMock()
    client.query_arrow_stream.return_value = Stream([TABLE.slice(0, 0)])
    with patch("clickhouse_connect.get_client", return_value=client):
        list(stream_query(CONNECTION, QUERY, ResourceLimits()))
    assert not events(caplog, "clickhouse.read.first_block")
    assert events(caplog, "clickhouse.read.finished")[0].emitted_rows == 0


@pytest.mark.parametrize("level", [logging.INFO, logging.DEBUG])
@pytest.mark.parametrize("flush", [False, True])
def test_diagnostic_io_and_explicit_flush_independent_of_level(caplog, level, flush):
    caplog.set_level(level, logger="ray_clickhouse")
    client = MagicMock()
    client.query_arrow_stream.return_value = Stream([], ReadError("failure"))
    with (
        patch("clickhouse_connect.get_client", return_value=client),
        patch(
            "ray_clickhouse._transport._lookup_query_diagnostic_impl", return_value=None
        ) as lookup,
    ):
        with pytest.raises(ReadError):
            list(
                stream_query(
                    CONNECTION, QUERY, ResourceLimits(), diagnostic_flush_logs=flush
                )
            )
    assert client.query_arrow_stream.call_count == 1
    assert lookup.call_count == 1
    assert lookup.call_args.kwargs["flush_logs"] is flush
    settings = client.query_arrow_stream.call_args.kwargs["settings"]
    assert settings.get("log_queries") == (1 if flush else None)
    if level == logging.DEBUG:
        assert events(caplog, "clickhouse.diagnostic.finished")[0].available is False


def test_discovery_started_precedes_existing_io_and_replanning_only_reuses_metadata(
    caplog,
):
    source = ClickHouseDatasource(
        ClickHouseReadConfig(CONNECTION, QualifiedTable("analytics", "events"))
    )
    client = MagicMock()

    def engine_query(*args, **kwargs):
        assert events(caplog, "clickhouse.discovery.started")[-1].phase == "engine"
        assert not [
            r
            for r in events(caplog, "clickhouse.discovery.finished")
            if r.phase == "engine"
        ]
        return SimpleNamespace(result_rows=[("MergeTree",)])

    client.query.side_effect = engine_query
    plan = SchemaPlan(SCHEMA, TARGET.columns)
    with (
        patch("ray_clickhouse.datasource.discover_schema", return_value=plan) as schema,
        patch("ray_clickhouse._discovery._open", return_value=client),
    ):
        first = source.get_read_tasks(1)[0]
        second = source.get_read_tasks(1)[0]
    assert schema.call_count == client.query.call_count == 1
    plans = events(caplog, "clickhouse.plan.finished")
    assert [r.cache_hit for r in plans] == [False, True]
    assert plans[0].plan_id != plans[1].plan_id
    with patch(
        "ray_clickhouse.datasource.stream_query", return_value=iter(())
    ) as stream:
        list(first.read_fn())
        list(second.read_fn())
    snapshots = [call.kwargs["log_context"] for call in stream.call_args_list]
    assert snapshots[0].split_id != snapshots[1].split_id
    assert all(s.operation_id == source._log_context.operation_id for s in snapshots)


def test_query_path_never_claims_table_engine_or_split(caplog):
    source = ClickHouseQueryDatasource(QueryReadConfig(CONNECTION, "SELECT 1 AS id"))
    with patch(
        "ray_clickhouse._query.discover_query_schema",
        return_value=SchemaPlan(SCHEMA, TARGET.columns),
    ) as discovery:
        source.get_read_tasks(8)
        source.get_read_tasks(4)
    assert discovery.call_count == 1
    plans = events(caplog, "clickhouse.plan.finished")
    assert all(r.read_mode == "query" and r.task_count == 1 for r in plans)
    assert all("engine" not in vars(r) and "split_mode" not in vars(r) for r in plans)


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("failure", [False, True])
def test_first_insert_is_visible_before_call_and_never_replayed(caplog, mode, failure):
    client = MagicMock()

    def insert(*args, **kwargs):
        assert events(caplog, "clickhouse.insert.started")
        assert not events(caplog, "clickhouse.insert.finished")
        if failure:
            raise RuntimeError("connection reset credential")

    client.insert_arrow.side_effect = insert
    session = ClickHouseInsertSession(
        CONNECTION,
        ResourceLimits(),
        database="analytics",
        table="events",
        insert_mode=mode,
    )
    with patch("clickhouse_connect.get_client", return_value=client):
        session.start()
        if failure:
            with pytest.raises(AmbiguousWriteError):
                session.insert(TABLE)
        else:
            session.insert(TABLE)
        session.close()
    assert client.insert_arrow.call_count == 1
    terminal = events(
        caplog, "clickhouse.insert.failed" if failure else "clickhouse.insert.finished"
    )
    assert len(terminal) == 1
    assert terminal[0].status == ("ambiguous" if failure else "confirmed")
    assert (
        terminal[0].client_insert_id
        == client.insert_arrow.call_args.kwargs["transport_settings"]["query_id"]
    )
    assert not events(caplog, "clickhouse.write.progress")
    assert "credential" not in caplog.text


def test_partial_write_keeps_confirmed_totals_and_preserves_primary_error(
    caplog, monkeypatch
):
    monkeypatch.setenv("RAY_CLICKHOUSE_LOG_BATCHES", "1")
    sink = ClickHouseDataSink(
        connection=CONNECTION,
        table=QualifiedTable("analytics", "events"),
        limits=ResourceLimits(batch_rows=1),
    )
    client = MagicMock()
    client.insert_arrow.side_effect = [None, RuntimeError("connection reset")]
    client.close.side_effect = RuntimeError("secret close")
    with (
        patch("ray_clickhouse.datasink.discover_target", return_value=TARGET),
        patch("clickhouse_connect.get_client", return_value=client),
    ):
        sink.on_write_start(SCHEMA)
        with pytest.raises(AmbiguousWriteError):
            sink.write([TABLE], None)
    final = [
        r for r in events(caplog, "clickhouse.write.finished") if r.role == "worker"
    ][0]
    assert final.status == "ambiguous" and final.confirmed_rows == final.batches == 1
    assert events(caplog, "clickhouse.write.progress")[0].confirmed_rows == 1
    assert client.insert_arrow.call_count == 2
    assert len(events(caplog, "clickhouse.cleanup.failed")) == 1
    sink.on_write_failed(AmbiguousWriteError("private"))
    driver = events(caplog, "clickhouse.write.finished")[-1]
    assert driver.role == "driver" and driver.status == "failed"
    assert "confirmed_rows" not in vars(driver)


def test_empty_completion_is_noop_without_preparation(caplog):
    sink = ClickHouseDataSink(
        connection=CONNECTION, table=QualifiedTable("analytics", "events")
    )
    with patch("ray_clickhouse.datasink.discover_target") as target:
        sink.on_write_complete(SimpleNamespace(num_rows=0, write_returns=[]))
        target.assert_not_called()
    assert events(caplog, "clickhouse.write.finished")[-1].status == "noop"
    assert not events(caplog, "clickhouse.write.prepare.started")


def test_schema_prepare_failure_and_ddl_ambiguity_keep_original_contract(caplog):
    sink = ClickHouseDataSink(
        connection=CONNECTION, table=QualifiedTable("analytics", "events")
    )
    with patch("ray_clickhouse.datasink.discover_target", return_value=TARGET):
        with pytest.raises(WriteError):
            sink.on_write_start(pa.schema([("id", pa.string())]))
    failures = [
        r
        for r in events(caplog, "clickhouse.write.prepare.finished")
        if r.status == "failed"
    ]
    assert (
        failures[-1].phase == "schema"
        and failures[-1].mismatch_category == "schema_validation"
    )
    client = MagicMock()
    client.query.return_value.result_rows = [(1,)]

    def command(sql, **kwargs):
        phase = "drop" if sql.startswith("DROP") else "create"
        assert events(caplog, "clickhouse.table_management.started")[-1].phase == phase
        if phase == "create":
            raise RuntimeError("private server text")

    client.command.side_effect = command
    with patch("ray_clickhouse._ddl._open", return_value=client):
        with pytest.raises(AmbiguousTableManagementError):
            create_or_replace_table(
                CONNECTION,
                QualifiedTable("analytics", "events"),
                ResourceLimits(),
                SCHEMA,
                mode="overwrite",
                engine="MergeTree",
                order_by=None,
            )
    assert client.command.call_count == 2
    final = events(caplog, "clickhouse.table_management.finished")[-1]
    assert final.status == "ambiguous" and final.drop_attempted
    assert "private" not in caplog.text


def test_discovery_permission_error_does_not_become_fallback_or_success(caplog):
    source = ClickHouseDatasource(
        ClickHouseReadConfig(CONNECTION, QualifiedTable("analytics", "events"))
    )
    with patch(
        "ray_clickhouse.datasource.discover_schema",
        side_effect=PermissionError("private metadata"),
    ):
        with pytest.raises(PermissionError):
            source.get_read_tasks(1)
    assert events(caplog, "clickhouse.plan.finished")[-1].status == "failed"
    assert not events(caplog, "clickhouse.read.started")


def test_progress_exhaustion_does_not_hide_failure_or_change_rows(caplog, monkeypatch):
    ticks = itertools.count()
    monkeypatch.setattr(
        "ray_clickhouse._logging.time.monotonic", lambda: float(next(ticks))
    )
    table = pa.table({"id": pa.array(range(10_100), type=pa.uint64())}, schema=SCHEMA)
    client = MagicMock()
    client.query_arrow_stream.return_value = Stream([table], ReadError("private"))
    context = replace(LogContext.create(), batches=True)
    with (
        patch("clickhouse_connect.get_client", return_value=client),
        patch(
            "ray_clickhouse._transport._lookup_query_diagnostic_impl", return_value=None
        ),
    ):
        with pytest.raises(ReadError):
            list(
                stream_query(
                    CONNECTION, QUERY, ResourceLimits(batch_rows=1), log_context=context
                )
            )
    assert len(events(caplog, "clickhouse.read.progress")) == 100
    final = events(caplog, "clickhouse.read.finished")[-1]
    assert final.status == "failed" and final.emitted_rows == 10_100
    assert final.suppressed_progress == 10_000
    assert client.query_arrow_stream.call_count == 1


def test_query_schema_drift_logs_counts_without_field_names(caplog):
    table = pa.table({"private_field": ["sensitive value"]})
    query = replace(QUERY, strict_schema=True)
    client = MagicMock()
    client.query_arrow_stream.return_value = Stream([table])
    with (
        patch("clickhouse_connect.get_client", return_value=client),
        patch(
            "ray_clickhouse._transport._lookup_query_diagnostic_impl", return_value=None
        ),
    ):
        with pytest.raises(ReadError):
            list(stream_query(CONNECTION, query, ResourceLimits()))
    final = events(caplog, "clickhouse.read.finished")[-1]
    assert final.phase == "conversion" and final.mismatch_category == "arrow_validation"
    assert final.expected_fields == final.actual_fields == 1
    assert final.emitted_rows == 0 and not events(caplog, "clickhouse.read.first_block")
    assert "private_field" not in caplog.text and "sensitive value" not in caplog.text


@pytest.mark.parametrize("failure", [False, True])
def test_stage_is_visible_before_call_and_measures_only_its_local_interval(
    caplog, monkeypatch, failure
):
    now = [10.0]
    monkeypatch.setattr("ray_clickhouse._logging.time.monotonic", lambda: now[0])
    logger = logging.getLogger("ray_clickhouse.datasink")
    context = LogContext.create()

    def operation():
        started = events(caplog, "clickhouse.write.prepare.started")
        assert len(started) == 1 and not hasattr(started[0], "duration_ms")
        assert not events(caplog, "clickhouse.write.prepare.finished")
        now[0] += 0.125
        if failure:
            raise PermissionError("private credential")

    if failure:
        with pytest.raises(PermissionError, match="private credential"):
            with stage(logger, context, "clickhouse.write.prepare", "target"):
                operation()
    else:
        with stage(logger, context, "clickhouse.write.prepare", "target"):
            operation()
    finished = events(caplog, "clickhouse.write.prepare.finished")
    assert len(finished) == 1 and finished[0].duration_ms == 125
    assert finished[0].status == ("failed" if failure else "succeeded")
    assert "private credential" not in caplog.text


@pytest.mark.parametrize("split", ["partition", "range", "auto"])
def test_local_parent_omits_physical_split_but_child_keeps_it(caplog, split):
    config = ClickHouseReadConfig(
        CONNECTION,
        QualifiedTable("analytics", "distributed"),
        split=split,
        range_column="id" if split == "range" else None,
        convert_distributed_to_local=True,
        target_tasks=4,
    )
    source = ClickHouseDatasource(config)
    schema = SchemaPlan(SCHEMA, TARGET.columns)
    source._planning_snapshot = DiscoverySnapshot(schema, "Distributed")
    child = ClickHouseDatasource(
        replace(
            config,
            table=QualifiedTable("analytics", "local"),
            convert_distributed_to_local=False,
        ),
        log_context=source._log_context,
    )
    child._planning_snapshot = DiscoverySnapshot(
        schema,
        "MergeTree",
        (PartitionInfo("p1", 10, 1, 80), PartitionInfo("p2", 10, 1, 80)),
        RangeFacts("id", "UInt64", 20, 0, 0, 19),
    )
    source._local_sources = (child,)
    tasks = source.get_read_tasks(4)
    plans = [
        r for r in events(caplog, "clickhouse.plan.finished") if r.phase == "planning"
    ]
    parent = [r for r in plans if r.read_mode == "local_shard"]
    assert len(parent) == 1 and parent[0].task_count == len(tasks)
    assert "split_mode" not in vars(parent[0])
    assert [r.split_mode for r in plans if r.read_mode == "table"] == [split]


@pytest.mark.parametrize(
    "option,reason",
    [
        ("headers", "client_options_not_reusable"),
        ("settings", "query_settings_not_reusable"),
    ],
)
def test_expected_local_fallback_is_not_error_and_keeps_safe_reason(
    caplog, option, reason
):
    connection = (
        replace(
            CONNECTION,
            client_options=(("headers", {"X-Private": "credential-private"}),),
        )
        if option == "headers"
        else replace(CONNECTION, settings=(("max_result_rows", 1),))
    )
    source = ClickHouseDatasource(
        ClickHouseReadConfig(
            connection,
            QualifiedTable("analytics", "events"),
            filter_sql="id >= 0",
            convert_distributed_to_local=True,
        )
    )
    with (
        patch(
            "ray_clickhouse.datasource.discover_schema",
            return_value=SchemaPlan(SCHEMA, TARGET.columns),
        ),
        patch("ray_clickhouse.datasource.discover_engine", return_value="Distributed"),
        patch("ray_clickhouse._distributed.metadata_reader") as reader,
    ):
        assert len(source.get_read_tasks(1)) == 1
        reader.assert_not_called()
    assert not [r for r in events(caplog) if r.levelno >= logging.ERROR]
    fallback = [
        r
        for r in events(caplog, "clickhouse.read.path_selected")
        if hasattr(r, "reason_code")
    ]
    assert fallback[-1].reason_code == reason
    assert events(caplog, "clickhouse.discovery.finished")[-1].status == "fallback"
    assert "credential-private" not in caplog.text


def test_unknown_fallback_text_is_not_a_reason_code():
    assert (
        NotApplicable("SELECT credential-private").reason_code
        == "local_conversion_not_applicable"
    )


@pytest.mark.parametrize("consumer_closed", [False, True])
def test_stream_exit_failure_is_cleanup_not_arrow_validation(caplog, consumer_closed):
    client = MagicMock()
    client.query_arrow_stream.return_value = Stream(
        [TABLE], exit_error=RuntimeError("private stream close")
    )
    with (
        patch("clickhouse_connect.get_client", return_value=client),
        patch(
            "ray_clickhouse._transport._lookup_query_diagnostic_impl", return_value=None
        ),
    ):
        stream = stream_query(CONNECTION, QUERY, ResourceLimits())
        if consumer_closed:
            next(stream)
            with pytest.raises(ReadError):
                stream.close()
        else:
            with pytest.raises(ReadError):
                list(stream)
    final = events(caplog, "clickhouse.read.finished")
    assert len(final) == 1 and final[0].status == "failed"
    assert final[0].phase == "cleanup" and "mismatch_category" not in vars(final[0])
    cleanup = events(caplog, "clickhouse.cleanup.failed")
    assert len(cleanup) == 1 and cleanup[0].resource_kind == "read_stream"
    assert cleanup[0].execution_id == final[0].execution_id
    assert "private stream close" not in caplog.text
    client.close.assert_called_once()
    assert client.query_arrow_stream.call_count == 1


@pytest.mark.parametrize("mismatch", ["type", "fields", "null"])
def test_later_write_block_validation_retains_safe_classification(caplog, mismatch):
    first = TABLE.cast(pa.schema([("id", pa.uint64())]))
    bad = {
        "type": pa.table({"id": ["private payload"]}),
        "fields": pa.table({"private_column": [1], "other_private": [2]}),
        "null": pa.table({"id": pa.array([None], type=pa.uint64())}),
    }[mismatch]
    sink = ClickHouseDataSink(
        connection=CONNECTION, table=QualifiedTable("analytics", "events")
    )
    client = MagicMock()
    with (
        patch("ray_clickhouse.datasink.discover_target", return_value=TARGET),
        patch("clickhouse_connect.get_client", return_value=client),
    ):
        sink.on_write_start(first.schema)
        with pytest.raises(WriteError):
            sink.write([first, bad], None)
    final = events(caplog, "clickhouse.write.finished")[-1]
    assert final.status == "failed" and final.phase == "block_prepare"
    assert final.mismatch_category == "schema_validation"
    assert final.expected_fields == 1 and final.actual_fields == len(bad.schema)
    assert final.confirmed_rows == 3 and final.batches == 1
    assert client.insert_arrow.call_count == 1 and client.close.call_count == 1
    assert "private" not in caplog.text
    assert len(events(caplog, "clickhouse.write.prepare.started")) == 4


@pytest.mark.parametrize(
    "mode",
    [
        "normal",
        "enter_failure",
        "body_failure",
        "exit_failure",
        "body_and_exit",
        "suppressed",
    ],
)
def test_stream_observer_preserves_native_context_protocol(mode):
    primary, closing = ReadError("primary"), RuntimeError("closing")

    class Context:
        def __init__(self):
            self.entered = self.exited = 0

        def __enter__(self):
            self.entered += 1
            if mode == "enter_failure":
                raise primary
            return self

        def __exit__(self, *args):
            self.exited += 1
            if mode in {"exit_failure", "body_and_exit"}:
                raise closing
            return mode == "suppressed"

    def run(observe):
        context, notifications = Context(), []
        manager = (
            _observe_stream_cleanup(context, notifications.append)
            if observe
            else context
        )
        try:
            with manager:
                if mode in {"body_failure", "body_and_exit", "suppressed"}:
                    raise primary
        except BaseException as error:
            outcome = error
        else:
            outcome = None
        return outcome, context.entered, context.exited, notifications

    native, observed = run(False), run(True)
    assert native[:3] == observed[:3]
    assert observed[3] == (
        [closing] if mode in {"exit_failure", "body_and_exit"} else []
    )
