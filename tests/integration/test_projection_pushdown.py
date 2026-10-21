"""Projection values, native row counts and real ClickHouse data-query pruning."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pyarrow as pa
import pytest
import ray

from ray_clickhouse import read_clickhouse
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
    _create(clickhouse_client, table_name, count)
    dataset = _source(connection_options, table_name, split)
    projected = dataset.select_columns([]).materialize()
    native = (
        ray.data.from_arrow(pa.table({"id": list(range(2, min(count, 10)))}))
        .select_columns([])
        .materialize()
    )
    assert projected.count() == native.count()
    assert projected.take_all() == native.take_all()
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
