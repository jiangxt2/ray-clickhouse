"""Projection values, native row counts and real ClickHouse data-query pruning."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pyarrow as pa
import pytest
import ray
from ray.data.expressions import col

from ray_clickhouse import ConfigurationError, SchemaError, read_clickhouse
from ray_clickhouse._compat import supports_v1_projection_pushdown

from .conftest import DATABASE

pytestmark = [pytest.mark.integration, pytest.mark.ray]


@pytest.fixture(scope="module", autouse=True)
def projection_runtime():
    owns_runtime = not ray.is_initialized()
    if owns_runtime:
        ray.init(address="local", include_dashboard=False, num_cpus=2)
    try:
        yield
    finally:
        if owns_runtime:
            ray.shutdown()


def _create(client, table, count=12):
    client.command(
        f"CREATE TABLE `{DATABASE}`.`{table}` "
        "(id Int64, value Nullable(String), day Date, amount Decimal(12,2), "
        "payload String) ENGINE = MergeTree PARTITION BY id % 3 ORDER BY id"
    )
    rows = [
        [
            i,
            None if i % 3 == 0 else f"v-{i}",
            dt.date(2026, 10, i + 1),
            Decimal(i).scaleb(-2),
            "x" * 16384,
        ]
        for i in range(count)
    ]
    if rows:
        client.insert(
            table,
            rows,
            column_names=["id", "value", "day", "amount", "payload"],
            database=DATABASE,
        )
    return rows


def _source(options, table, split, **kwargs):
    return read_clickhouse(
        **options,
        table=table,
        split=split,
        **({"range_column": "id"} if split == "range" else {}),
        filter="id >= %(lo)s AND id < %(hi)s",
        query_parameters={"lo": 2, "hi": 10},
        target_tasks=4,
        override_num_blocks=4,
        concurrency=2,
        batch_rows=3,
        diagnostic_flush_logs=True,
        **kwargs,
    )


def _data_queries(client, table):
    client.command("SYSTEM FLUSH LOGS")
    queries = client.query(
        "SELECT query FROM system.query_log WHERE type = 'QueryFinish' "
        "AND is_initial_query = 1 "
        "AND startsWith(log_comment, 'ray-clickhouse operation=read ') "
        "AND position(query, %(table)s) > 0",
        parameters={"table": table},
    ).result_rows
    assert queries, "No completed connector data query was observed"
    return [row[0] for row in queries]


@pytest.mark.parametrize("split", ["single", "partition", "range", "auto"])
def test_dataset_projection_prunes_data_sql_and_preserves_values(
    clickhouse_client, connection_options, table_name, split
):
    rows = _create(clickhouse_client, table_name)
    dataset = _source(
        connection_options,
        table_name,
        split,
        columns=("id", "payload", "amount", "day", "value"),
    )
    assert dataset.schema().names == ["id", "payload", "amount", "day", "value"]
    projected = dataset.select_columns(["amount", "value", "day", "id"]).select_columns(
        ["value", "day", "amount"]
    )
    expected = [
        {"value": r[1], "day": r[2], "amount": r[3]} for r in rows if 2 <= r[0] < 10
    ]
    result = projected.materialize()
    assert sorted(result.take_all(), key=lambda r: r["day"]) == expected
    assert result.count() == 8
    schema = result.schema().base_schema
    assert schema.names == ["value", "day", "amount"]
    assert schema.field("value").nullable
    if supports_v1_projection_pushdown():
        assert not schema.field("day").nullable
    assert schema.field("day").type == pa.date32()
    assert schema.field("amount").type == pa.decimal128(12, 2)
    for sql in _data_queries(clickhouse_client, table_name):
        projection = sql.split(" FROM ", 1)[0]
        if supports_v1_projection_pushdown():
            assert "`payload`" not in projection and "`id`" not in projection
        assert "WHERE" in sql and "id >=" in sql
    # Evaluating a sibling afterwards must not reuse the narrowed scan schema.
    assert sorted(dataset.select_columns(["id"]).take_all(), key=lambda r: r["id"]) == [
        {"id": i} for i in range(2, 10)
    ]


@pytest.mark.parametrize("split", ["single", "partition", "range", "auto"])
@pytest.mark.parametrize("count", [0, 12])
def test_empty_projection_matches_native_materialization_and_count(
    clickhouse_client, connection_options, table_name, split, count
):
    rows = _create(clickhouse_client, table_name, count)
    dataset = _source(connection_options, table_name, split)
    schema = dataset.schema().base_schema
    native_rows = [
        dict(zip(schema.names, row, strict=True)) for row in rows if 2 <= row[0] < 10
    ]
    projected = dataset.select_columns([]).materialize()
    native = (
        ray.data.from_arrow(pa.Table.from_pylist(native_rows, schema=schema))
        .select_columns([])
        .materialize()
    )
    assert projected.count() == native.count()
    # Ray 2.55 retains the source columns for an empty native selection. Match
    # their values without assuming ordering across independent physical splits.
    assert sorted(projected.take_all(), key=repr) == sorted(native.take_all(), key=repr)
    assert (
        projected.map_batches(lambda block: block, batch_format="pyarrow").count()
        == native.count()
    )
    for sql in _data_queries(clickhouse_client, table_name):
        if supports_v1_projection_pushdown():
            assert sql.startswith("SELECT `id` FROM")
        assert "count(" not in sql.lower()


def test_projection_keeps_order_key_and_ray_filter_dependency(
    clickhouse_client, connection_options, table_name
):
    rows = _create(clickhouse_client, table_name)
    dataset = _source(
        connection_options, table_name, "single", order_by=(("id",), True)
    )
    result = (
        dataset.filter(lambda row: row["id"] % 2 == 0)
        .select_columns(["value"])
        .take_all()
    )
    # Ray's default executor may deliver filtered blocks in completion order.
    # Check exact rows independently of the server-side ORDER BY clause.
    assert sorted(result, key=repr) == sorted(
        [{"value": rows[i][1]} for i in (8, 6, 4, 2)], key=repr
    )
    queries = _data_queries(clickhouse_client, table_name)
    # The HTTP Arrow driver appends its FORMAT clause to the logged statement.
    assert all(
        sql.rsplit("\n FORMAT ", 1)[0].rstrip().endswith("ORDER BY `id` DESC")
        for sql in queries
    ), queries


def _create_mixed_schema(client, table, count=12):
    rows = _create(client, table, count)
    client.command(
        f"ALTER TABLE `{DATABASE}`.`{table}` "
        "ADD COLUMN state AggregateFunction(sum, UInt64) "
        "DEFAULT initializeAggregation('sumState', toUInt64(id)), "
        "ADD COLUMN variant Variant(Int64, String) "
        "DEFAULT CAST(id AS Variant(Int64, String))"
    )
    return rows


@pytest.mark.parametrize("split", ["single", "partition", "range", "auto"])
@pytest.mark.parametrize("count", [0, 12])
def test_explicit_columns_skip_unsupported_types_and_keep_native_composition(
    clickhouse_client, connection_options, table_name, split, count
):
    rows = _create_mixed_schema(clickhouse_client, table_name, count)
    root = _source(
        connection_options, table_name, split, columns=("value", "day", "amount")
    )
    source_schema = root.schema().base_schema
    assert source_schema.names == ["value", "day", "amount"]
    assert source_schema.field("value").nullable
    assert source_schema.field("amount").type == pa.decimal128(12, 2)
    branch = root.select_columns(["amount", "value"]).filter(
        expr=col("value").is_not_null()
    )
    expected = [
        {"amount": r[3], "value": r[1]}
        for r in rows
        if 2 <= r[0] < 10 and r[1] is not None
    ]
    assert sorted(branch.take_all(), key=repr) == sorted(expected, key=repr)
    original = root.materialize()
    expected_rows = [
        {"value": r[1], "day": r[2], "amount": r[3]} for r in rows if 2 <= r[0] < 10
    ]
    assert sorted(original.take_all(), key=repr) == sorted(expected_rows, key=repr)
    assert original.count() == len(expected_rows)
    empty = root.select_columns([]).materialize()
    native_empty = (
        ray.data.from_arrow(pa.Table.from_pylist(expected_rows, schema=source_schema))
        .select_columns([])
        .materialize()
    )
    assert empty.count() == native_empty.count() == len(expected_rows)
    assert empty.take_all() == native_empty.take_all()
    clickhouse_client.command("SYSTEM FLUSH LOGS")
    probes = clickhouse_client.query(
        "SELECT query FROM system.query_log WHERE type = 'QueryFinish' "
        "AND startsWith(log_comment, 'ray-clickhouse operation=schema-probe') "
        "AND position(query, %(table)s) > 0",
        parameters={"table": table_name},
    ).result_rows
    assert probes
    for (sql,) in probes:
        projection = sql.split(" FROM ", 1)[0]
        assert "`state`" not in projection and "`variant`" not in projection
        assert "`id`" not in projection


@pytest.mark.parametrize(
    "columns,error",
    [
        (None, SchemaError),
        (("state",), SchemaError),
        (("variant",), SchemaError),
        (("missing",), ConfigurationError),
    ],
)
def test_explicit_schema_selection_keeps_unsupported_and_unknown_errors(
    clickhouse_client, connection_options, table_name, columns, error
):
    _create_mixed_schema(clickhouse_client, table_name)
    with pytest.raises(error):
        read_clickhouse(
            **connection_options,
            table=table_name,
            columns=columns,
            override_num_blocks=1,
        ).take_all()


def test_unconverted_filter_and_order_dependencies_need_no_arrow_field(
    clickhouse_client, connection_options, table_name
):
    rows = _create_mixed_schema(clickhouse_client, table_name)
    actual = read_clickhouse(
        **connection_options,
        table=table_name,
        columns=("day",),
        filter="finalizeAggregation(state) >= 2",
        order_by=(("id",), True),
        override_num_blocks=1,
    ).take_all()
    assert actual == [{"day": r[2]} for r in reversed(rows) if r[0] >= 2]


def test_view_explicit_columns_keep_schema_selection_and_single_query_contract(
    clickhouse_client, connection_options, table_name
):
    rows = _create_mixed_schema(clickhouse_client, table_name)
    view = table_name + "_view"
    clickhouse_client.command(
        f"CREATE VIEW `{DATABASE}`.`{view}` AS "
        f"SELECT * FROM `{DATABASE}`.`{table_name}`"
    )
    try:
        dataset = read_clickhouse(
            **connection_options,
            table=view,
            columns=("day", "id"),
            override_num_blocks=1,
        )
        assert sorted(dataset.take_all(), key=lambda r: r["id"]) == [
            {"day": r[2], "id": r[0]} for r in rows
        ]
    finally:
        clickhouse_client.command(f"DROP VIEW IF EXISTS `{DATABASE}`.`{view}`")
