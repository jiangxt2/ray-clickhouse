from unittest.mock import patch

import pyarrow as pa
import pytest

from ray_clickhouse._discovery import PartitionInfo, RangeFacts
from ray_clickhouse._errors import ConfigurationError, PermissionError
from ray_clickhouse._models import ClickHouseConnection, QualifiedTable, ResourceLimits
from ray_clickhouse._schema import SchemaPlan, TargetColumn
from ray_clickhouse.datasource import ClickHouseDatasource, ClickHouseReadConfig


def _query_from_read_fn(read_fn):
    values = dict(
        zip(
            read_fn.__code__.co_freevars,
            (cell.cell_contents for cell in read_fn.__closure__),
            strict=True,
        )
    )
    return values["query"]


def _config(**kwargs):
    return ClickHouseReadConfig(
        connection=ClickHouseConnection(host="clickhouse", database="analytics"),
        table=QualifiedTable("analytics", "events"),
        limits=ResourceLimits(),
        **kwargs,
    )


def _schema_plan():
    columns = (
        TargetColumn("id", "UInt64", "", "", 1),
        TargetColumn("value", "String", "", "", 2),
    )
    return SchemaPlan(pa.schema([("id", pa.uint64()), ("value", pa.string())]), columns)


def test_single_read_creates_one_task_without_count_estimate():
    fake_task = object()
    with (
        patch("ray_clickhouse.datasource.ensure_supported_ray_version"),
        patch("ray_clickhouse.datasource.discover_schema", return_value=_schema_plan()),
        patch("ray_clickhouse.datasource.discover_engine", return_value="MergeTree"),
        patch(
            "ray_clickhouse.datasource.make_read_task", return_value=fake_task
        ) as make_task,
    ):
        source = ClickHouseDatasource(
            _config(
                columns=("id",),
                filter_sql="id >= %(minimum)s",
                query_parameters=(("minimum", 1),),
            )
        )
        tasks = source.get_read_tasks(parallelism=8)

    assert tasks == [fake_task]
    query = _query_from_read_fn(make_task.call_args.args[0])
    assert "SELECT `id` FROM `analytics`.`events`" in query.sql
    assert "id >= %(minimum)s" in query.sql
    assert query.parameter_dict()["minimum"] == 1


def test_single_read_supports_view_engine_and_order_by():
    fake_task = object()
    with (
        patch("ray_clickhouse.datasource.ensure_supported_ray_version"),
        patch("ray_clickhouse.datasource.discover_schema", return_value=_schema_plan()),
        patch("ray_clickhouse.datasource.discover_engine", return_value="View"),
        patch(
            "ray_clickhouse.datasource.make_read_task", return_value=fake_task
        ) as make_task,
    ):
        source = ClickHouseDatasource(
            _config(order_by=(("id", True),), columns=("id",))
        )
        assert source.get_read_tasks(parallelism=4) == [fake_task]

    query = _query_from_read_fn(make_task.call_args.args[0])
    assert query.sql.endswith("ORDER BY `id` DESC")


def test_order_by_requires_single_split():
    with pytest.raises(ConfigurationError, match="order_by requires split='single'"):
        _config(split="partition", order_by=(("id", False),))


def test_partition_read_groups_physical_partitions():
    fake_task = object()
    partitions = (
        PartitionInfo("p1", 10, 2, 100),
        PartitionInfo("p2", 10, 2, 100),
        PartitionInfo("p3", 10, 2, 100),
    )
    with (
        patch("ray_clickhouse.datasource.ensure_supported_ray_version"),
        patch("ray_clickhouse.datasource.discover_schema", return_value=_schema_plan()),
        patch("ray_clickhouse.datasource.discover_engine", return_value="MergeTree"),
        patch("ray_clickhouse.datasource.discover_partitions", return_value=partitions),
        patch(
            "ray_clickhouse.datasource.make_read_task", return_value=fake_task
        ) as make_task,
    ):
        source = ClickHouseDatasource(
            _config(split="partition", target_tasks=2, max_tasks=2)
        )
        tasks = source.get_read_tasks(parallelism=2)

    assert len(tasks) == 2
    sql_values = [
        _query_from_read_fn(call.args[0]).sql for call in make_task.call_args_list
    ]
    assert all("_partition_id" in sql for sql in sql_values)


def test_selected_columns_preserve_requested_order():
    fake_task = object()
    with (
        patch("ray_clickhouse.datasource.ensure_supported_ray_version"),
        patch("ray_clickhouse.datasource.discover_schema", return_value=_schema_plan()),
        patch("ray_clickhouse.datasource.discover_engine", return_value="MergeTree"),
        patch(
            "ray_clickhouse.datasource.make_read_task", return_value=fake_task
        ) as make_task,
    ):
        source = ClickHouseDatasource(_config(columns=("value", "id")))
        source.get_read_tasks(parallelism=1)

    query = _query_from_read_fn(make_task.call_args.args[0])
    assert query.sql.startswith("SELECT `value`, `id` FROM")


def test_permission_failure_does_not_fallback_to_single():
    with (
        patch("ray_clickhouse.datasource.ensure_supported_ray_version"),
        patch("ray_clickhouse.datasource.discover_schema", return_value=_schema_plan()),
        patch("ray_clickhouse.datasource.discover_engine", return_value="MergeTree"),
        patch(
            "ray_clickhouse.datasource.discover_partitions",
            side_effect=PermissionError("denied"),
        ),
    ):
        source = ClickHouseDatasource(_config(split="partition"))
        with pytest.raises(PermissionError):
            source.get_read_tasks(parallelism=2)


def test_range_read_uses_disjoint_constraints():
    fake_task = object()
    facts = RangeFacts("id", "UInt64", 100, 0, 0, 99)
    with (
        patch("ray_clickhouse.datasource.ensure_supported_ray_version"),
        patch("ray_clickhouse.datasource.discover_schema", return_value=_schema_plan()),
        patch("ray_clickhouse.datasource.discover_engine", return_value="MergeTree"),
        patch("ray_clickhouse.datasource.discover_range_facts", return_value=facts),
        patch(
            "ray_clickhouse.datasource.make_read_task", return_value=fake_task
        ) as make_task,
    ):
        source = ClickHouseDatasource(
            _config(split="range", range_column="id", target_tasks=2, max_tasks=2)
        )
        tasks = source.get_read_tasks(parallelism=2)

    assert len(tasks) == 2
    sql_values = [
        _query_from_read_fn(call.args[0]).sql for call in make_task.call_args_list
    ]
    assert "`id` >=" in sql_values[0] and "`id` <" in sql_values[0]
    assert "`id` >=" in sql_values[1] and "`id` <" not in sql_values[1]


def test_empty_range_read_still_creates_one_task():
    fake_task = object()
    facts = RangeFacts("id", "UInt64", 0, 0, None, None)
    with (
        patch("ray_clickhouse.datasource.ensure_supported_ray_version"),
        patch("ray_clickhouse.datasource.discover_schema", return_value=_schema_plan()),
        patch("ray_clickhouse.datasource.discover_engine", return_value="MergeTree"),
        patch("ray_clickhouse.datasource.discover_range_facts", return_value=facts),
        patch("ray_clickhouse.datasource.make_read_task", return_value=fake_task),
    ):
        source = ClickHouseDatasource(
            _config(split="range", range_column="id", target_tasks=2, max_tasks=2)
        )
        tasks = source.get_read_tasks(parallelism=2)

    assert tasks == [fake_task]
