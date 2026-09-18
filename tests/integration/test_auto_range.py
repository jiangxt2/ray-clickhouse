"""Compare automatic task queries with single reads against real ClickHouse."""

from collections import Counter

import pytest

from ray_clickhouse import read_clickhouse
from ray_clickhouse._errors import DiscoveryError
from ray_clickhouse._models import ClickHouseConnection, QualifiedTable
from ray_clickhouse.datasource import ClickHouseDatasource, ClickHouseReadConfig

from .conftest import DATABASE

pytestmark = pytest.mark.integration


def _source(options, table, **kwargs):
    return ClickHouseDatasource(
        ClickHouseReadConfig(
            connection=ClickHouseConnection(**options),
            table=QualifiedTable(DATABASE, table),
            **kwargs,
        )
    )


def _rows(tasks):
    return Counter(
        tuple(row.items())
        for task in tasks
        for block in task()
        for row in block.to_pylist()
    )


@pytest.mark.parametrize(
    ("declared_type", "values"),
    [
        ("Int64", [-5, -5, 0, 0, 7, 12]),
        ("Nullable(Int64)", [-5, -5, 0, 7, None, None]),
        ("Nullable(Int64)", [None, None]),
        ("Int64", []),
        ("Int64", [4, 4, 4]),
        ("Int64", [-(2**63), -1, 0, 2**63 - 1]),
        ("UInt64", [0, 1, 2**64 - 1]),
    ],
)
def test_auto_task_queries_match_single_read(
    clickhouse_client,
    connection_options,
    table_name,
    declared_type,
    values,
):
    clickhouse_client.command(
        f"CREATE TABLE `{DATABASE}`.`{table_name}` "
        f"(id {declared_type}, value String) ENGINE = MergeTree "
        "ORDER BY (id, value) SETTINGS allow_nullable_key = 1"
    )
    if values:
        clickhouse_client.insert(
            table_name,
            [[value, f"row-{index}"] for index, value in enumerate(values)],
            column_names=["id", "value"],
            database=DATABASE,
        )
    source = _source(connection_options, table_name, split="auto", target_tasks=4)
    tasks = source.get_read_tasks(parallelism=4)
    single = _source(connection_options, table_name).get_read_tasks(parallelism=4)
    actual = _rows(tasks)
    assert actual == _rows(single)
    assert sum(actual.values()) == len(values)
    if len(set(values)) > 1:
        assert 1 < len(tasks) <= 4
    else:
        assert len(tasks) == 1


@pytest.mark.parametrize("sorting_key", ["abs(id)", "(value, id)", "tuple()"])
def test_auto_unsuitable_key_falls_back_or_raises(
    clickhouse_client,
    connection_options,
    table_name,
    sorting_key,
):
    clickhouse_client.command(
        f"CREATE TABLE `{DATABASE}`.`{table_name}` "
        f"(id Int64, value String) ENGINE = MergeTree ORDER BY {sorting_key}"
    )
    clickhouse_client.insert(
        table_name,
        [[1, "a"], [2, "b"]],
        column_names=["id", "value"],
        database=DATABASE,
    )
    source = _source(connection_options, table_name, split="auto")
    tasks = source.get_read_tasks(parallelism=4)
    assert len(tasks) == 1
    assert _rows(tasks) == _rows(
        _source(connection_options, table_name).get_read_tasks(1)
    )
    with pytest.raises(DiscoveryError):
        _source(
            connection_options, table_name, split="auto", discovery_policy="error"
        ).get_read_tasks(4)


@pytest.mark.ray
def test_auto_dataset_preserves_filter_and_projection(
    clickhouse_client,
    connection_options,
    table_name,
):
    clickhouse_client.command(
        f"CREATE TABLE `{DATABASE}`.`{table_name}` "
        "(id Int64, value String) ENGINE = MergeTree ORDER BY (id, value)"
    )
    clickhouse_client.insert(
        table_name,
        [[index // 2, f"row-{index}"] for index in range(20)],
        column_names=["id", "value"],
        database=DATABASE,
    )
    options = dict(
        **connection_options,
        table=table_name,
        columns=("value",),
        filter="id >= %(minimum)s AND id < %(maximum)s",
        query_parameters={"minimum": 2, "maximum": 8},
    )
    single = read_clickhouse(**options)
    auto = read_clickhouse(
        **options, split="auto", target_tasks=4, override_num_blocks=4, concurrency=2
    )
    expected = sorted(row["value"] for row in single.take_all())
    actual = sorted(row["value"] for row in auto.take_all())
    assert actual == expected
    assert len(actual) == 12
    assert auto.schema().base_schema == single.schema().base_schema
