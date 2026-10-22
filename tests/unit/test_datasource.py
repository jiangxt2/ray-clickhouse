from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from ray_clickhouse._discovery import PartitionInfo, RangeFacts
from ray_clickhouse._errors import ConfigurationError, DiscoveryError, PermissionError
from ray_clickhouse._models import ClickHouseConnection, QualifiedTable, ResourceLimits
from ray_clickhouse._schema import SchemaPlan, TargetColumn
from ray_clickhouse.datasource import ClickHouseDatasource, ClickHouseReadConfig


@pytest.fixture
def projected_source():
    with ExitStack() as stack:
        stack.enter_context(
            patch(
                "ray_clickhouse.datasource.supports_v1_projection_pushdown",
                return_value=True,
            )
        )
        stack.enter_context(
            patch(
                "ray_clickhouse.datasource.discover_schema", return_value=_schema_plan()
            )
        )
        stack.enter_context(
            patch("ray_clickhouse.datasource.discover_engine", return_value="MergeTree")
        )
        yield ClickHouseDatasource(_config(columns=("id", "value")))


def test_projection_clones_keep_order_and_do_not_mutate_other_branches(
    projected_source,
):
    from ray import cloudpickle

    source = projected_source
    reordered = source.apply_projection({"value": "ignored", "id": "id"})
    narrow = reordered.apply_projection({"value": "value"})
    assert reordered.get_projection_map() == {"value": "value", "id": "id"}
    assert reordered.arrow_schema.names == ["value", "id"]
    assert narrow.arrow_schema.names == ["value"]
    assert source.arrow_schema.names == ["id", "value"]
    assert narrow.config is source.config
    assert narrow._planning_snapshot is source._planning_snapshot
    assert narrow._planning_snapshot.schema.arrow_schema.names == ["id", "value"]
    mapping = narrow.get_projection_map()
    mapping.clear()
    assert narrow.get_projection_map() == {"value": "value"}
    assert narrow.apply_projection(None).arrow_schema == narrow.arrow_schema
    assert (
        cloudpickle.loads(cloudpickle.dumps(narrow)).arrow_schema == narrow.arrow_schema
    )
    with pytest.raises(ConfigurationError, match="unknown ClickHouse columns"):
        narrow.apply_projection({"id": "id"})


@pytest.mark.parametrize("columns", [("value",), ()])
def test_projection_task_sql_schema_and_cleanup(projected_source, columns):
    from ray.data.block import BlockAccessor

    from ray_clickhouse._compat import supports_v1_projection_pushdown

    source = projected_source.apply_projection({c: c for c in columns})
    closed = []
    queries = []

    def stream(connection, query, limits, **kwargs):
        queries.append(query)
        try:
            values = {"id": [1, 2, 3], "value": ["a", "b", "c"]}
            yield pa.table(
                {c: values[c] for c in query.arrow_schema.names},
                schema=query.arrow_schema,
            )
            raise RuntimeError("stream failed")
        finally:
            closed.append(True)

    # Empty block semantics belong to the actual installed Ray contract.
    if not columns and not supports_v1_projection_pushdown():
        assert source.get_projection_map() == {}
        assert source.get_read_tasks(1)[0].schema == pa.schema([])
        return
    with patch("ray_clickhouse.datasource.stream_query", stream):
        task = source.get_read_tasks(1)[0]
        assert task.schema == source.arrow_schema
        blocks = iter(task.read_fn())
        block = next(blocks)
        assert BlockAccessor.for_block(block).num_rows() == 3
        if columns:
            assert BlockAccessor.for_block(block).schema().names == list(columns)
        else:
            native = BlockAccessor.for_block(pa.table({"id": [1, 2, 3]})).select([])
            assert block.schema == native.schema
        blocks.close()
        assert closed == [True]
        assert queries[0].sql.startswith(
            "SELECT `value` FROM" if columns else "SELECT `id` FROM"
        )
        blocks = iter(task.read_fn())
        next(blocks)
        with pytest.raises(RuntimeError, match="stream failed"):
            next(blocks)
        assert closed == [True, True]


def test_projection_preserves_explicit_visibility_and_planning_failures(
    projected_source,
):
    source = ClickHouseDatasource(_config(columns=("id",)))
    with pytest.raises(ConfigurationError, match="unknown ClickHouse columns"):
        source.apply_projection({"value": "value"})
    with patch(
        "ray_clickhouse.datasource.discover_schema",
        side_effect=DiscoveryError("bad schema"),
    ):
        with pytest.raises(DiscoveryError, match="bad schema"):
            ClickHouseDatasource(_config()).apply_projection({"id": "id"})


def test_projection_keeps_filter_order_and_range_dependencies(projected_source):
    source = ClickHouseDatasource(
        _config(
            filter_sql="id >= %(minimum)s",
            query_parameters=(("minimum", 1),),
            order_by=(("id", True),),
        )
    ).apply_projection({"value": "value"})
    with patch("ray_clickhouse.datasource.make_read_task") as make_task:
        source.get_read_tasks(2)
        query = _query_from_read_fn(make_task.call_args.args[0])
        assert query.sql.startswith("SELECT `value` FROM")
        assert "id >= %(minimum)s" in query.sql
        assert query.sql.endswith("ORDER BY `id` DESC")
        assert query.parameter_dict()["minimum"] == 1
    source = ClickHouseDatasource(_config(split="range", range_column="id"))
    with patch(
        "ray_clickhouse.datasource.discover_range_facts",
        return_value=RangeFacts("id", "UInt64", 10, 0, 0, 9),
    ):
        narrow = source.apply_projection({"value": "value"})
    assert narrow._planning_snapshot.range_facts.column == "id"
    with patch("ray_clickhouse.datasource.make_read_task") as make_task:
        narrow.get_read_tasks(2)
        assert all(
            "`id` >=" in _query_from_read_fn(c.args[0]).sql
            for c in make_task.call_args_list
        )


def test_cached_local_sources_are_projected_independently(projected_source):
    source = projected_source
    source._ensure_planning_snapshot()
    child = ClickHouseDatasource(_config())
    child._planning_snapshot = source._planning_snapshot
    source._local_sources = (child,)
    narrow = source.apply_projection({"value": "value"})
    assert narrow._local_sources[0] is not child
    assert narrow._local_sources[0].arrow_schema.names == ["value"]
    assert child.arrow_schema.names == ["id", "value"]
    assert source._planning_snapshot is narrow._local_sources[0]._planning_snapshot


def test_unverified_projection_keeps_legacy_capability():
    with patch(
        "ray_clickhouse.datasource.supports_v1_projection_pushdown", return_value=False
    ):
        source = ClickHouseDatasource(_config())
        assert not source.supports_projection_pushdown()
        with pytest.raises(ConfigurationError, match="unverified"):
            source.apply_projection({"id": "id"})
        assert source.get_projection_map() is None


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


def test_single_read_rejects_materialized_view_engine():
    with (
        patch("ray_clickhouse.datasource.ensure_supported_ray_version"),
        patch("ray_clickhouse.datasource.discover_schema", return_value=_schema_plan()),
        patch(
            "ray_clickhouse.datasource.discover_engine",
            return_value="MaterializedView",
        ),
    ):
        source = ClickHouseDatasource(_config())
        with pytest.raises(DiscoveryError, match="MaterializedView.*unsupported"):
            source.get_read_tasks(parallelism=1)


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


@pytest.mark.parametrize("split", ["range", "auto"])
def test_explicit_projection_keeps_non_output_split_key(split):
    full = _schema_plan()
    plan = SchemaPlan(pa.schema([full.arrow_schema.field("value")]), full.columns)
    config = _config(
        columns=("value",),
        split=split,
        **({"range_column": "id"} if split == "range" else {}),
    )
    source = ClickHouseDatasource(config)
    with (
        patch(
            "ray_clickhouse.datasource.discover_schema", return_value=plan
        ) as discovery,
        patch("ray_clickhouse.datasource.discover_engine", return_value="MergeTree"),
        patch("ray_clickhouse.datasource.discover_sorting_key", return_value="id"),
        patch(
            "ray_clickhouse.datasource.discover_range_facts",
            return_value=RangeFacts("id", "UInt64", 10, 0, 0, 9),
        ),
        patch("ray_clickhouse.datasource.make_read_task") as make_task,
    ):
        source.get_read_tasks(3)
        discovery.assert_called_once_with(
            config.connection, config.table, config.limits, columns=("value",)
        )
        assert source.arrow_schema.names == ["value"]
        assert all(
            "`id` >=" in _query_from_read_fn(c.args[0]).sql
            for c in make_task.call_args_list
        )


def test_explicit_projection_keeps_non_output_order_key():
    full = _schema_plan()
    plan = SchemaPlan(pa.schema([full.arrow_schema.field("value")]), full.columns)
    with (
        patch("ray_clickhouse.datasource.discover_schema", return_value=plan),
        patch("ray_clickhouse.datasource.discover_engine", return_value="MergeTree"),
        patch("ray_clickhouse.datasource.make_read_task") as make_task,
    ):
        source = ClickHouseDatasource(
            _config(columns=("value",), order_by=(("id", True),))
        )
        source.get_read_tasks(1)
        sql = _query_from_read_fn(make_task.call_args.args[0]).sql
        assert sql == "SELECT `value` FROM `analytics`.`events` ORDER BY `id` DESC"
        with pytest.raises(ConfigurationError, match="unknown order_by"):
            ClickHouseDatasource(
                _config(columns=("value",), order_by=(("missing", False),))
            ).get_read_tasks(1)


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
