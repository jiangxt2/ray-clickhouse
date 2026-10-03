from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from ray_clickhouse import read_clickhouse
from ray_clickhouse._discovery import discover_query_schema
from ray_clickhouse._errors import (
    ConfigurationError,
    ReadError,
    SchemaError,
    TransportError,
)
from ray_clickhouse._models import ClickHouseConnection, QuerySpec, ResourceLimits
from ray_clickhouse._query import (
    ClickHouseQueryDatasource,
    QueryReadConfig,
    validate_read_query,
)
from ray_clickhouse._schema import SchemaPlan, TargetColumn
from ray_clickhouse._transport import stream_query


def _connection():
    return ClickHouseConnection(host="localhost", database="analytics")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id FROM events",
        "WITH source AS (SELECT id FROM events) SELECT id FROM source",
        "-- leading comment\nSELECT 'DROP; FORMAT JSON' AS text;",
        "SELECT 'one''two;three' AS text",
        "SELECT 1 AS id -- trailing comment",
        "SELECT 1 AS id /* comment */;",
        "SELECT id FROM system.query_log",
    ],
)
def test_trusted_read_subset_handles_quotes_and_comments(sql):
    result = validate_read_query(sql)
    assert result


@pytest.mark.parametrize(
    "sql",
    [
        "",
        " ",
        "SELECT 1 AS id\0",
        "INSERT INTO events VALUES (1)",
        "WITH source AS (SELECT 1) INSERT INTO events SELECT * FROM source",
        "SELECT 1 AS id; SELECT 2 AS id",
        "SELECT 1 AS id FORMAT JSON",
        "SELECT 1 AS id INTO OUTFILE '/tmp/result'",
        "SELECT 1 AS id SETTINGS readonly=0",
        "SELECT 1 AS id /* unclosed",
        "SELECT 'unclosed AS id",
        "SELECT {id:UInt32} AS id",
    ],
)
def test_unsafe_or_unsupported_query_forms_are_rejected(sql):
    with pytest.raises(ConfigurationError):
        validate_read_query(sql)


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"table": "events", "query": "SELECT 1 AS id"},
        {"query": "SELECT 1 AS id", "columns": ["id"]},
        {"query": "SELECT 1 AS id", "filter": "id=1"},
        {"query": "SELECT 1 AS id", "order_by": (["id"], False)},
        {"query": "SELECT 1 AS id", "split": "auto"},
        {"query": "SELECT 1 AS id", "range_column": "id"},
        {"query": "SELECT 1 AS id", "settings": {"readonly": 0}},
    ],
)
def test_source_exclusivity_and_query_options_fail_before_read(kwargs):
    with (
        patch("ray_clickhouse._api.ray.data.read_datasource") as read,
        pytest.raises(ConfigurationError),
    ):
        read_clickhouse(host="localhost", database="analytics", **kwargs)
    read.assert_not_called()


def test_query_api_snapshots_values_and_forwards_resources():
    parameters = {"minimum": 2}
    with patch(
        "ray_clickhouse._api.ray.data.read_datasource", return_value="dataset"
    ) as read:
        assert (
            read_clickhouse(
                host="localhost",
                database="analytics",
                query="SELECT id FROM events WHERE id >= %(minimum)s",
                query_parameters=parameters,
                concurrency=2,
                override_num_blocks=8,
                num_cpus=0.5,
                memory=1024,
                ray_remote_args={"max_retries": 0},
            )
            == "dataset"
        )
    source = read.call_args.args[0]
    assert isinstance(source, ClickHouseQueryDatasource)
    parameters["minimum"] = 9
    assert source.config.query_parameters == (("minimum", 2),)
    assert dict(source.config.connection.settings)["readonly"] == 1
    assert "SELECT" not in repr(source.config)
    assert read.call_args.kwargs == {
        "concurrency": 2,
        "override_num_blocks": 8,
        "num_cpus": 0.5,
        "memory": 1024,
        "ray_remote_args": {"max_retries": 0},
    }


def test_query_planning_is_cached_and_always_builds_one_task():
    schema = pa.schema([pa.field("id", pa.uint64(), nullable=False)])
    plan = SchemaPlan(schema, (TargetColumn("id", "UInt64", "", "", 1),))
    source = ClickHouseQueryDatasource(
        QueryReadConfig(_connection(), "SELECT id FROM events")
    )
    with (
        patch(
            "ray_clickhouse._query.discover_query_schema", return_value=plan
        ) as discover,
        patch("ray_clickhouse._query.make_read_task", side_effect=lambda fn, *args: fn),
        patch("ray_clickhouse._query.stream_query", return_value=iter(())) as stream,
    ):
        assert source.estimate_inmemory_data_size() is None
        for parallelism in (1, 8):
            tasks = source.get_read_tasks(parallelism)
            assert len(tasks) == 1
            list(tasks[0]())
    discover.assert_called_once()
    assert stream.call_count == 2
    assert stream.call_args.args[1].strict_schema is True
    assert source.arrow_schema == schema


@pytest.mark.parametrize(
    ("rows", "arrow"),
    [
        (
            [["wrong alias", "UInt64"]],
            pa.table({"wrong alias": pa.array([], pa.uint64())}),
        ),
        (
            [["id", "UInt64"], ["id", "UInt64"]],
            pa.table({"id": pa.array([], pa.uint64())}),
        ),
        ([["id", "UInt64"]], pa.table({"other": pa.array([], pa.uint64())})),
        ([["id", "UInt64"]], pa.table({"id": pa.array([], pa.float64())})),
    ],
)
def test_query_metadata_rejects_names_and_type_mismatch_and_closes(rows, arrow):
    client = MagicMock()
    client.query.return_value.result_rows = rows
    client.query_arrow.return_value = arrow
    with (
        patch("ray_clickhouse._discovery._open", return_value=client),
        pytest.raises(SchemaError),
    ):
        discover_query_schema(
            _connection(), "SELECT id FROM events", ResourceLimits(), parameters={}
        )
    client.close.assert_called_once()


def test_query_schema_parameters_and_readonly_are_applied_to_both_requests():
    client = MagicMock()
    client.query.return_value.result_rows = [["id", "UInt64"]]
    schema = pa.schema([pa.field("id", pa.uint64(), nullable=False)])
    client.query_arrow.return_value = pa.Table.from_batches([], schema=schema)
    with patch("ray_clickhouse._discovery._open", return_value=client):
        result = discover_query_schema(
            _connection(),
            "SELECT id FROM events WHERE id >= %(minimum)s",
            ResourceLimits(),
            parameters={"minimum": 1},
        )
    assert result.arrow_schema == schema
    for method in (client.query, client.query_arrow):
        assert method.call_args.kwargs["parameters"] == {"minimum": 1}
        assert method.call_args.kwargs["settings"]["readonly"] == 1
    client.close.assert_called_once()


def test_successful_discovery_reports_close_failure():
    client = MagicMock()
    client.query.return_value.result_rows = [["id", "UInt64"]]
    client.query_arrow.return_value = pa.Table.from_batches(
        [], schema=pa.schema([pa.field("id", pa.uint64(), nullable=False)])
    )
    client.close.side_effect = RuntimeError("close failed")
    with (
        patch("ray_clickhouse._discovery._open", return_value=client),
        pytest.raises(TransportError, match="close"),
    ):
        discover_query_schema(
            _connection(), "SELECT id FROM events", ResourceLimits(), parameters={}
        )


def test_strict_query_schema_rejects_a_castable_type_change():
    client = MagicMock()
    client.query_arrow_stream.return_value.__enter__.return_value = iter(
        [pa.table({"id": [1.0]})]
    )
    query = QuerySpec(
        "SELECT id FROM events",
        (),
        pa.schema([("id", pa.int64())]),
        strict_schema=True,
    )
    with (
        patch("clickhouse_connect.get_client", return_value=client),
        patch("ray_clickhouse._transport._lookup_query_diagnostic", return_value=None),
        pytest.raises(ReadError, match="changed since planning"),
    ):
        list(stream_query(_connection(), query, ResourceLimits()))
    client.close.assert_called_once()


def test_strict_query_schema_rejects_nonnullable_null_values():
    schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    table = pa.Table.from_arrays([pa.array([None], pa.int64())], schema=schema)
    client = MagicMock()
    client.query_arrow_stream.return_value.__enter__.return_value = iter([table])
    with (
        patch("clickhouse_connect.get_client", return_value=client),
        patch("ray_clickhouse._transport._lookup_query_diagnostic", return_value=None),
        pytest.raises(ReadError, match="NULL"),
    ):
        list(
            stream_query(
                _connection(),
                QuerySpec("SELECT id FROM events", (), schema, strict_schema=True),
                ResourceLimits(),
            )
        )
    client.close.assert_called_once()


@pytest.mark.parametrize(
    "predicate",
    [
        "SELECT id FROM events WHERE id = %(missing)s",
        "SELECT '%(id)s' AS text FROM events WHERE id = %(id)s",
        "SELECT '{id:UInt32}' AS text FROM events WHERE id = %(id)s",
    ],
)
def test_query_bindings_reject_missing_and_ambiguous_forms(predicate):
    with pytest.raises(ConfigurationError):
        QueryReadConfig(_connection(), predicate, (("id", 1),))
