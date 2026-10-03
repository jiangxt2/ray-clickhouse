"""Native filter bindings across actual discovery and worker query paths."""

from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from ray_clickhouse import read_clickhouse
from ray_clickhouse._models import ClickHouseConnection, QualifiedTable
from ray_clickhouse.datasource import ClickHouseDatasource, ClickHouseReadConfig

from .conftest import DATABASE

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("declared_type", ["Nullable(Int64)", "Nullable(UInt64)"])
@pytest.mark.parametrize("split", ["single", "partition", "range", "auto"])
@pytest.mark.parametrize("style", ["client", "server"])
def test_bindings_preserve_split_results_and_extreme_integers(
    clickhouse_client, connection_options, table_name, declared_type, split, style
):
    clickhouse_client.command(
        f"CREATE TABLE `{DATABASE}`.`{table_name}` "
        f"(id {declared_type}, tenant UInt8, value String) ENGINE=MergeTree "
        "PARTITION BY tenant ORDER BY (id, value) SETTINGS allow_nullable_key=1"
    )
    values = (
        [-(2**63), -1, 0, 0, 2**63 - 1, None]
        if "Int64" in declared_type and "UInt64" not in declared_type
        else [0, 1, 1, 2**64 - 1, None]
    )
    rows = [[value, 2, f"row-{index}"] for index, value in enumerate(values)]
    clickhouse_client.insert(
        table_name,
        rows + [[1, 1, "excluded"]],
        column_names=["id", "tenant", "value"],
        database=DATABASE,
    )
    predicate = (
        "tenant = %(tenant)s" if style == "client" else "tenant = {tenant:UInt8}"
    )
    config = ClickHouseReadConfig(
        connection=ClickHouseConnection(**connection_options),
        table=QualifiedTable(DATABASE, table_name),
        filter_sql=predicate,
        query_parameters=(("tenant", 2),),
        split=split,
        range_column="id" if split == "range" else None,
        target_tasks=4,
        discovery_policy="error",
    )
    source = ClickHouseDatasource(config)
    actual = Counter(
        (row["id"], row["value"])
        for task in source.get_read_tasks(4)
        for block in task()
        for row in block.to_pylist()
    )
    assert actual == Counter((row[0], row[2]) for row in rows)


@pytest.mark.parametrize(
    ("hint", "value", "predicate"),
    [
        ("String", "quoted' text", "text = {value:String}"),
        ("Nullable(UInt32)", None, "isNull({value:Nullable(UInt32)})"),
        (
            "DateTime64(6,'UTC')",
            datetime(2026, 10, 3, 1, 2, 3, 456789, tzinfo=timezone.utc),
            "ts = {value:DateTime64(6,'UTC')}",
        ),
        (
            "DateTime('UTC')",
            datetime(2026, 10, 3, tzinfo=timezone.utc),
            "toStartOfDay(ts) = {value:DateTime('UTC')}",
        ),
        ("Decimal(20,4)", Decimal("12.3456"), "amount = {value:Decimal(20,4)}"),
        ("Decimal32(4)", Decimal("12.3456"), "amount = {value:Decimal32(4)}"),
        ("Date32", date(2026, 10, 3), "day = {value:Date32}"),
        ("Date", date(2026, 10, 3), "day = {value:Date}"),
        ("Bool", True, "flag = {value:Bool}"),
        ("Float64", 1.5, "metric = {value:Float64}"),
        ("Float32", 1.5, "metric = {value:Float32}"),
        ("FixedString(3)", "xyz", "fixed = {value:FixedString(3)}"),
    ],
)
def test_native_scalar_null_precision_timezone_and_quoting(
    clickhouse_client, connection_options, table_name, hint, value, predicate
):
    clickhouse_client.command(
        f"CREATE TABLE `{DATABASE}`.`{table_name}` "
        "(id UInt32, text String, ts DateTime64(6,'UTC'), amount Decimal(20,4), "
        "day Date32, flag Bool, metric Float64, fixed FixedString(3)) "
        "ENGINE=MergeTree ORDER BY id"
    )
    clickhouse_client.insert(
        table_name,
        [
            [
                1,
                "quoted' text",
                datetime(2026, 10, 3, 1, 2, 3, 456789, tzinfo=timezone.utc),
                Decimal("12.3456"),
                date(2026, 10, 3),
                True,
                1.5,
                "xyz",
            ]
        ],
        column_names=["id", "text", "ts", "amount", "day", "flag", "metric", "fixed"],
        database=DATABASE,
    )
    source = ClickHouseDatasource(
        ClickHouseReadConfig(
            connection=ClickHouseConnection(**connection_options),
            table=QualifiedTable(DATABASE, table_name),
            columns=("id",),
            filter_sql=predicate,
            query_parameters=(("value", value),),
        )
    )
    assert [
        row
        for task in source.get_read_tasks(1)
        for block in task()
        for row in block.to_pylist()
    ] == [{"id": 1}]


@pytest.mark.ray
def test_native_auto_binding_executes_through_public_dataset(
    clickhouse_client, connection_options, table_name
):
    clickhouse_client.command(
        f"CREATE TABLE `{DATABASE}`.`{table_name}` "
        "(id UInt64, tenant UInt8) ENGINE=MergeTree ORDER BY id"
    )
    clickhouse_client.insert(
        table_name,
        [[index, index % 2] for index in range(16)],
        column_names=["id", "tenant"],
        database=DATABASE,
    )
    dataset = read_clickhouse(
        **connection_options,
        table=table_name,
        columns=("id",),
        filter="tenant = {tenant:UInt8}",
        query_parameters={"tenant": 1},
        split="auto",
        target_tasks=4,
        override_num_blocks=4,
        concurrency=2,
    )
    assert sorted(row["id"] for row in dataset.take_all()) == list(range(1, 16, 2))


@pytest.mark.parametrize(
    ("hint", "value"),
    [
        ("UInt64", 2**64 - 1),
        ("Int64", -(2**63)),
        ("Int64", 2**63 - 1),
        ("UInt32", 2**32 - 1),
        ("UInt16", 2**16 - 1),
        ("Int32", -(2**31)),
        ("Int16", -(2**15)),
        ("Int8", -128),
    ],
)
def test_native_integer_parameter_extremes(
    clickhouse_client, connection_options, table_name, hint, value
):
    clickhouse_client.command(
        f"CREATE TABLE `{DATABASE}`.`{table_name}` "
        f"(id {hint}) ENGINE=MergeTree ORDER BY id"
    )
    clickhouse_client.insert(
        table_name, [[value]], column_names=["id"], database=DATABASE
    )
    source = ClickHouseDatasource(
        ClickHouseReadConfig(
            connection=ClickHouseConnection(**connection_options),
            table=QualifiedTable(DATABASE, table_name),
            filter_sql=f"id = {{value:{hint}}}",
            query_parameters=(("value", value),),
        )
    )
    assert [
        row["id"]
        for task in source.get_read_tasks(1)
        for block in task()
        for row in block.to_pylist()
    ] == [value]


def test_wide_internal_integer_comparisons_keep_primary_key_pruning(
    clickhouse_client, table_name
):
    import re

    clickhouse_client.command(
        f"CREATE TABLE `{DATABASE}`.`{table_name}` "
        "(id UInt64) ENGINE=MergeTree ORDER BY id"
    )
    clickhouse_client.insert(
        table_name,
        [[index] for index in range(20000)],
        column_names=["id"],
        database=DATABASE,
    )
    explained = clickhouse_client.query(
        f"EXPLAIN indexes=1 SELECT id FROM `{DATABASE}`.`{table_name}` "
        "WHERE id >= toInt128(1000) AND id < toInt128(2000)"
    )
    text = "\n".join(row[0] for row in explained.result_rows)
    primary = text.split("PrimaryKey", 1)[1]
    match = re.search(r"Granules:\s+(\d+)/(\d+)", primary)
    assert match is not None
    selected, total = map(int, match.groups())
    assert 0 < selected < total


@pytest.mark.parametrize(
    ("split", "predicate"),
    [
        ("single", "id >= %(minimum)s /* %(note)s */"),
        ("partition", "id >= %(minimum)s # %(note)s\n AND id < 10"),
        ("auto", "id >= %(minimum)s -- %(note)s\n AND id < 10"),
    ],
)
def test_client_comment_bindings_reach_discovery_and_worker_queries(
    clickhouse_client, connection_options, table_name, split, predicate
):
    clickhouse_client.command(
        f"CREATE TABLE `{DATABASE}`.`{table_name}` "
        "(id UInt32, tenant UInt8) ENGINE=MergeTree PARTITION BY tenant ORDER BY id"
    )
    clickhouse_client.insert(
        table_name,
        [[index, index % 2] for index in range(10)],
        column_names=["id", "tenant"],
        database=DATABASE,
    )
    source = ClickHouseDatasource(
        ClickHouseReadConfig(
            connection=ClickHouseConnection(**connection_options),
            table=QualifiedTable(DATABASE, table_name),
            columns=("id",),
            filter_sql=predicate,
            query_parameters=(("minimum", 4), ("note", "trace")),
            split=split,
            target_tasks=4,
            discovery_policy="error",
        )
    )
    assert sorted(
        row["id"]
        for task in source.get_read_tasks(4)
        for block in task()
        for row in block.to_pylist()
    ) == list(range(4, 10))
