from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import uuid4

import pyarrow as pa
import pytest

from ray_clickhouse import ConfigurationError, read_clickhouse
from ray_clickhouse._discovery import DiscoverySnapshot, discover_schema
from ray_clickhouse._distributed import (
    CONTEXT_DEFAULTS,
    MetadataCapabilities,
    MetadataReader,
    NotApplicable,
    ResolvedDistributed,
    _rows,
    parse_distributed_engine,
    predicate_columns,
    resolve_distributed,
)
from ray_clickhouse._errors import (
    DiscoveryError,
    ObjectNotFoundError,
    PermissionError,
    SchemaError,
    TransportError,
)
from ray_clickhouse._models import ClickHouseConnection, QualifiedTable, ResourceLimits
from ray_clickhouse._schema import SchemaPlan, TargetColumn
from ray_clickhouse.datasource import (
    ClickHouseDatasource,
    ClickHouseReadConfig,
    _group_read_tasks,
)

COLUMNS = (
    TargetColumn("id", "Int64", "", "", 1),
    TargetColumn("value", "Nullable(Int64)", "", "", 2),
    TargetColumn("alias", "Int64", "ALIAS", "id", 3),
)
SCHEMA = pa.schema([("id", pa.int64())])
CONNECTION = ClickHouseConnection("entry", "db")


@pytest.mark.parametrize(
    "engine",
    [
        "Distributed(cluster, db, data, cityHash64(id))",
        "Distributed('cluster', `db`, 'data', tuple(id, 'x,)'))",
        "CREATE TABLE t (`Distributed` Int64) ENGINE = "
        "Distributed('cluster', 'db', 'data')",
    ],
)
def test_constant_engine_targets(engine):
    assert parse_distributed_engine(engine) == ("cluster", QualifiedTable("db", "data"))


@pytest.mark.parametrize(
    "engine",
    [
        "Distributed(currentDatabase(), db, data)",
        "Distributed('{cluster}', db, data)",
        "Distributed(c, db, 'a-b')",
    ],
)
def test_engine_outside_profile(engine):
    with pytest.raises(NotApplicable):
        parse_distributed_engine(engine)


@pytest.mark.parametrize(
    "engine", ["Distributed('c', db, data", "Distributed('c, db, data)"]
)
def test_malformed_engine_is_error(engine):
    with pytest.raises(DiscoveryError):
        parse_distributed_engine(engine)


@pytest.mark.parametrize(
    "predicate",
    [
        "id >= %(lo)s AND (`value` IS NULL OR value < {hi:Int64})",
        "NOT (id < -3 OR value IS NOT NULL)",
        "id = 2 AND 'a' <> 'b'",
    ],
)
def test_predicate_profile(predicate):
    assert "id" in predicate_columns(predicate, COLUMNS)


@pytest.mark.parametrize(
    "predicate",
    [
        "hostName() = 'entry'",
        "alias = 1",
        "_shard_num = 1",
        "id + 1 > 0",
        "id > now()",
        "1 = 1",
        "id IN (SELECT id FROM other)",
        "id = 1 /* comment */",
    ],
)
def test_predicate_fallback(predicate):
    with pytest.raises(NotApplicable):
        predicate_columns(predicate, COLUMNS)


@pytest.mark.parametrize(
    "predicate,expected",
    [
        ("id BETWEEN -3 AND 20", {"id"}),
        ("id NOT BETWEEN 20 AND -3", {"id"}),
        ("value BETWEEN NULL AND 20", {"value"}),
        ("id BETWEEN {lo:Int64} AND {hi:Int64}", {"id"}),
        ("id BETWEEN %(lo)s AND %(hi)s AND value >= 0", {"id", "value"}),
        ("txt BETWEEN 'a' AND 'z'", {"txt"}),
        ("txt LIKE 'row-%'", {"txt"}),
        ("txt NOT LIKE 'row-_'", {"txt"}),
        (r"txt LIKE 'a\\%b\\_c'", {"txt"}),
        ("txt LIKE 'it''s_%'", {"txt"}),
        ("txt LIKE {pattern:String}", {"txt"}),
        ("txt NOT LIKE %(pattern)s", {"txt"}),
        ("txt LIKE 'GLOBAL IN (SELECT)'", {"txt"}),
        ("id IN (1)", {"id"}),
        ("id IN (1, -2, 1, 3.5, NULL, TRUE, FALSE)", {"id"}),
        ("id NOT IN ({one:Int64}, {two:Int64})", {"id"}),
        ("id IN (%(one)s, %(two)s)", {"id"}),
        ("txt IN ('a,b', 'it''s', 'GLOBAL', 'SELECT')", {"txt"}),
        ("`value` not in (NULL, 3)", {"value"}),
        (
            "NOT (id BETWEEN 2 AND 9 OR txt LIKE 'x%') AND value IN (NULL, 3)",
            {"id", "txt", "value"},
        ),
    ],
)
def test_simple_predicates_collect_only_physical_columns(predicate, expected):
    columns = COLUMNS + (TargetColumn("txt", "String", "", "", 4),)
    assert predicate_columns(predicate, columns) == frozenset(expected)


@pytest.mark.parametrize(
    "declared",
    [
        "String",
        "FixedString(8)",
        "Nullable(String)",
        "LowCardinality(String)",
        "LowCardinality(Nullable(String))",
    ],
)
def test_fixed_like_accepts_existing_string_types(declared):
    column = TargetColumn("txt", declared, "", "", 1)
    assert predicate_columns("txt LIKE {pattern:String}", (column,)) == {"txt"}


@pytest.mark.parametrize(
    "predicate",
    [
        "id BETWEEN value AND 20",
        "id BETWEEN 1 AND value",
        "id BETWEEN abs(-1) AND 20",
        "id BETWEEN 1 OR 20",
        "id BETWEEN 1 AND",
        "1 BETWEEN 0 AND 2",
        "id LIKE '1%'",
        "txt LIKE txt",
        "txt LIKE 3",
        "txt LIKE NULL",
        "txt LIKE concat('a', '%')",
        "txt ILIKE 'a%'",
        r"txt LIKE 'a%' ESCAPE '\\'",
        "txt NOT = 'a'",
        "id IN ()",
        "id IN (1,)",
        "id IN (1,,2)",
        "id IN (1",
        "id IN (value, 2)",
        "id IN (1, value)",
        "id IN (abs(-1))",
        "id IN ((1, 2))",
        "id IN ([1, 2])",
        "(id, value) IN ((1, 2))",
        "id IN other_set",
        "id IN {items:Array(Int64)}",
        "id GLOBAL IN (1, 2)",
        "id GLOBAL NOT IN (1, 2)",
        "NOT (id GLOBAL IN (1, 2))",
        "id IN (SELECT id FROM other)",
        "id NOT IN (SELECT id FROM other)",
        "id IN (1, (SELECT id FROM other))",
        "id BETWEEN 1 AND 3 OR id GLOBAL IN (1)",
        "txt LIKE 'a%' OR alias IN (1)",
        "_shard_num IN (1)",
        "alias BETWEEN 1 AND 3",
        "id IN (1, {column:Identifier})",
    ],
)
def test_simple_predicate_recognizer_rejects_unsupported_forms(predicate):
    columns = COLUMNS + (TargetColumn("txt", "String", "", "", 4),)
    with pytest.raises(NotApplicable):
        predicate_columns(predicate, columns)


@pytest.mark.parametrize(
    "predicate", ["id GLOBAL IN (1)", "txt LIKE txt", "id IN (SELECT id FROM other)"]
)
def test_unsupported_predicates_stop_clean_schema_before_discovery(predicate):
    columns = (COLUMNS[0], TargetColumn("txt", "String", "", "", 2))
    schema = SchemaPlan(pa.schema([("id", pa.int64()), ("txt", pa.string())]), columns)
    with (
        patch("ray_clickhouse._distributed.metadata_reader") as open_reader,
        pytest.raises(NotApplicable),
    ):
        resolve_distributed(
            CONNECTION,
            QualifiedTable("db", "t"),
            schema,
            ResourceLimits(),
            filter_sql=predicate,
            split="single",
        )
    open_reader.assert_not_called()


@pytest.mark.parametrize(
    "predicate", ["id BETWEEN 1 AND 3", "txt LIKE 'row-%'", "id IN (1, 2)"]
)
def test_supported_predicates_reach_discovery_with_clean_schema(predicate):
    columns = (COLUMNS[0], TargetColumn("txt", "String", "", "", 2))
    schema = SchemaPlan(pa.schema([("id", pa.int64()), ("txt", pa.string())]), columns)
    with (
        patch(
            "ray_clickhouse._distributed.metadata_reader",
            side_effect=RuntimeError("discovery reached"),
        ) as open_reader,
        pytest.raises(RuntimeError, match="discovery reached"),
    ):
        resolve_distributed(
            CONNECTION,
            QualifiedTable("db", "t"),
            schema,
            ResourceLimits(),
            filter_sql=predicate,
            split="single",
        )
    open_reader.assert_called_once_with(CONNECTION, ResourceLimits())


@pytest.mark.parametrize("operator", ["IN", "NOT IN", "GLOBAL IN"])
def test_public_filter_keeps_rejecting_select_subqueries(operator):
    with pytest.raises(ConfigurationError, match="filter must be a predicate"):
        read_clickhouse(
            host="entry",
            database="db",
            table="t",
            filter=f"id {operator} (SELECT id FROM other)",
            convert_distributed_to_local=True,
        )


def reader():
    return MetadataReader(Mock(), CONNECTION, ResourceLimits())


def test_capability_missing_is_distinct_from_malformed():
    metadata = reader()
    metadata.fields["clusters"] = {"name": "String"}
    with pytest.raises(NotApplicable):
        metadata.require_fields("clusters", {"shard_num": "int"})
    metadata.fields["clusters"]["shard_num"] = "String"
    with pytest.raises(DiscoveryError):
        metadata.require_fields("clusters", {"shard_num": "int"})


def test_permission_check_cannot_treat_invisibility_as_absence():
    metadata = reader()
    metadata.client.query.side_effect = RuntimeError("access denied")
    with pytest.raises(PermissionError):
        metadata.describe(QualifiedTable("db", "t"))


@pytest.mark.parametrize(
    "field,origin",
    [
        ("engine_full", "engine_full"),
        ("create_table_query", "create_table_query"),
        (None, "show-create"),
    ],
)
def test_engine_source_priority(field, origin):
    metadata = reader()
    metadata.fields["tables"] = {
        "engine": "String",
        "database": "String",
        "name": "String",
    }
    if field:
        metadata.fields["tables"][field] = "String"
    definition = "Distributed(c, db, data)"
    calls = []

    def query(sql, parameters=None):
        calls.append(sql)
        if sql.startswith("SHOW CREATE"):
            return ({"statement": "CREATE TABLE t ENGINE = " + definition},)
        return ({"engine": "Distributed", **({field: definition} if field else {})},)

    metadata.query = query
    assert "Distributed" in metadata.engine(QualifiedTable("db", "t"))[1]
    assert dict(metadata.snapshot().origins)["engine:db.t"] == origin
    assert sum(sql.startswith("SHOW CREATE") for sql in calls) == (field is None)


def test_columns_describe_normalizes_named_fields_and_order():
    metadata = reader()
    metadata.fields["columns"] = {"name": "String", "type": "String"}
    metadata.query = Mock(
        side_effect=[
            (
                {
                    "comment": "ignored",
                    "type": "Int64",
                    "name": "id",
                    "default_type": "",
                    "default_expression": "",
                },
            ),
        ]
    )
    assert metadata.columns(QualifiedTable("db", "t")) == (COLUMNS[0],)


def test_query_mode_and_non_boolean_flag_rejected():
    for options in (
        {"query": "SELECT 1 AS id", "convert_distributed_to_local": True},
        {"table": "t", "convert_distributed_to_local": 1},
    ):
        with pytest.raises(ConfigurationError):
            read_clickhouse(host="entry", database="db", **options)


def test_flag_off_does_not_discover_topology():
    with (
        patch(
            "ray_clickhouse.datasource.discover_schema",
            return_value=SchemaPlan(SCHEMA, COLUMNS[:1]),
        ),
        patch("ray_clickhouse.datasource.discover_engine", return_value="Distributed"),
        patch("ray_clickhouse.datasource.resolve_distributed") as resolve,
    ):
        source = ClickHouseDatasource(
            ClickHouseReadConfig(CONNECTION, QualifiedTable("db", "t"))
        )
        assert len(source.get_read_tasks(8)) == 1
        resolve.assert_not_called()


def test_failed_split_preparation_does_not_publish_targets_or_reuse_them():
    with (
        patch(
            "ray_clickhouse.datasource.discover_schema",
            return_value=SchemaPlan(SCHEMA, COLUMNS[:1]),
        ),
        patch("ray_clickhouse.datasource.discover_engine", return_value="Distributed"),
        patch(
            "ray_clickhouse.datasource.resolve_distributed",
            side_effect=[
                ResolvedDistributed((), MetadataCapabilities((), ())),
                NotApplicable("changed context"),
            ],
        ),
        patch.object(
            ClickHouseDatasource,
            "_prepare_local_sources",
            side_effect=DiscoveryError("split unavailable"),
        ),
    ):
        source = ClickHouseDatasource(
            ClickHouseReadConfig(
                CONNECTION,
                QualifiedTable("db", "t"),
                filter_sql="id > 0",
                convert_distributed_to_local=True,
            )
        )
        with pytest.raises(DiscoveryError):
            source.get_read_tasks(4)
        assert source._resolved_distributed is None
        assert source._planning_snapshot is None
        assert not source._local_sources
        assert len(source.get_read_tasks(4)) == 1
        assert source._logical_fallback


@pytest.mark.parametrize(
    "error",
    [PermissionError("denied"), TransportError("offline"), DiscoveryError("broken")],
)
def test_real_errors_never_trigger_logical_fallback(error):
    with (
        patch(
            "ray_clickhouse.datasource.discover_schema",
            return_value=SchemaPlan(SCHEMA, COLUMNS[:1]),
        ),
        patch("ray_clickhouse.datasource.discover_engine", return_value="Distributed"),
        patch("ray_clickhouse.datasource.resolve_distributed", side_effect=error),
    ):
        source = ClickHouseDatasource(
            ClickHouseReadConfig(
                CONNECTION,
                QualifiedTable("db", "t"),
                filter_sql="id > 0",
                convert_distributed_to_local=True,
            )
        )
        with pytest.raises(type(error)):
            source.get_read_tasks(8)


def test_group_shares_row_budget_and_closes_before_next_scan():
    events = []

    def scan(name):
        def read():
            events.append("open:" + name)
            try:
                yield pa.table({"id": [1, 2]})
                yield pa.table({"id": [3, 4]})
            finally:
                events.append("close:" + name)

        return read

    limited = _group_read_tasks((scan("a"), scan("b")), SCHEMA, 3)
    assert sum(block.num_rows for block in limited()) == 3
    assert events == ["open:a", "close:a"]
    events.clear()
    stream = iter(_group_read_tasks((scan("a"), scan("b")), SCHEMA, None)())
    next(stream)
    stream.close()
    assert events == ["open:a", "close:a"]


def test_group_failure_after_output_never_starts_another_scan():
    def broken():
        yield pa.table({"id": [1]})
        raise TransportError("offline")

    following = Mock()
    stream = iter(_group_read_tasks((broken, following), SCHEMA, None)())
    next(stream)
    with pytest.raises(TransportError):
        next(stream)
    following.assert_not_called()


def test_global_budget_regroups_cached_shards_without_dropping_any():
    schema = SchemaPlan(SCHEMA, COLUMNS[:1])
    config = ClickHouseReadConfig(
        CONNECTION, QualifiedTable("db", "t"), columns=("id",), target_tasks=4
    )
    source = ClickHouseDatasource(config)
    source._planning_snapshot = DiscoverySnapshot(schema, "Distributed")
    source._resolved_distributed = ResolvedDistributed((), MetadataCapabilities((), ()))
    local = []
    for i in range(5):
        child = ClickHouseDatasource(
            replace(config, connection=replace(CONNECTION, host=f"host{i}"))
        )
        child._planning_snapshot = DiscoverySnapshot(schema, "MergeTree")
        local.append(child)
    source._local_sources = tuple(local)
    with patch(
        "ray_clickhouse.datasource.stream_query",
        side_effect=lambda connection, *args, **kwargs: iter(
            [pa.table({"id": [int(connection.host[-1])]})]
        ),
    ):
        for parallelism in (1, 2, 4):
            tasks = source.get_read_tasks(parallelism)
            assert len(tasks) == parallelism
            assert sorted(
                row["id"]
                for task in tasks
                for block in task()
                for row in block.to_pylist()
            ) == list(range(5))


def test_empty_native_results_and_named_metadata_validation():
    assert _rows(SimpleNamespace(column_names=[], result_rows=[])) == ()
    assert _rows(
        SimpleNamespace(column_names=["type", "name"], result_rows=[["String", "id"]])
    ) == ({"type": "String", "name": "id"},)
    for names, rows in (([], [[1]]), (["x", "x"], [[1, 2]]), (["x"], [[1, 2]])):
        with pytest.raises(DiscoveryError):
            _rows(SimpleNamespace(column_names=names, result_rows=rows))


def test_native_policy_enumeration_compares_cardinality_without_parsing_sql():
    metadata = reader()
    metadata.fields["row_policies"] = {
        "id": "UUID",
        "database": "String",
        "table": "String",
    }
    metadata.query = Mock(side_effect=[({"statement": "unparsed"},), ()])
    with pytest.raises(DiscoveryError):
        metadata.policies()
    assert metadata.policy_inventory is None


def test_capability_prefers_system_columns_and_falls_back_only_for_known_absence():
    class MissingField(Exception):
        code = 47

    result = SimpleNamespace(
        column_names=["name", "type"],
        result_rows=[["name", "String"], ["type", "String"]],
    )
    metadata = reader()
    metadata.client.query.side_effect = [result, MissingField()]
    assert metadata.capability("columns") == {"name": "String", "type": "String"}
    assert (
        metadata.client.query.call_args_list[-1]
        .args[0]
        .startswith("SELECT name, type FROM system.columns")
    )
    metadata = reader()
    metadata.client.query.side_effect = RuntimeError("access denied")
    with pytest.raises(PermissionError):
        metadata.capability("columns")
    assert metadata.client.query.call_count == 1


@pytest.mark.parametrize(
    "parallel,value,eligible",
    [
        ("enable_parallel_replicas", "1000", True),
        ("allow_experimental_parallel_reading_from_replicas", "1", True),
        ("allow_experimental_parallel_reading_from_replicas", "2", False),
    ],
)
def test_effective_replica_controls_by_field_priority(parallel, value, eligible):
    metadata = reader()
    metadata.fields = {
        "row_policies": {"database": "String", "table": "String"},
        "settings": {"name": "String", "value": "String"},
    }
    effective = dict(CONTEXT_DEFAULTS, max_parallel_replicas=value)
    effective[parallel] = "0"
    metadata.query = Mock(
        side_effect=[
            tuple({"name": name, "value": value} for name, value in effective.items()),
            ({"user": "default", "timezone": "UTC"},),
        ]
    )
    metadata.policies = Mock(return_value=())
    if eligible:
        assert ("server_timezone", "UTC") in metadata.context(QualifiedTable("db", "t"))
    else:
        with pytest.raises(NotApplicable):
            metadata.context(QualifiedTable("db", "t"))


def resolved_readers():
    source = Mock(spec=MetadataReader)
    target = Mock(spec=MetadataReader)
    source.engine.return_value = ("Distributed", "Distributed(c, db, data)")
    source.columns.return_value = COLUMNS[:1]
    source.context.return_value = target.context.return_value = (("timezone", "UTC"),)
    source.capability.return_value = {"cluster": "String"}
    source.query.return_value = (
        {"shard_num": 1, "replica_num": 2, "host_name": "unused", "user": "default"},
        {"shard_num": 1, "replica_num": 1, "host_name": "first", "user": "default"},
        {"shard_num": 2, "replica_num": 1, "host_name": "second", "user": "default"},
    )
    source.origins = {}
    source.snapshot.return_value = target.snapshot.return_value = MetadataCapabilities(
        (), ()
    )
    target.engine.return_value = ("MergeTree", "MergeTree ORDER BY id")
    target.columns.return_value = COLUMNS[:1]
    target.schema.return_value = SchemaPlan(SCHEMA, COLUMNS[:1])
    return source, target


def resolve_with_readers(source, target, connection=CONNECTION, schema=None):
    def open_reader(connection, limits):
        manager = Mock()
        manager.__enter__ = Mock(
            return_value=source if connection.host == "entry" else target
        )
        manager.__exit__ = Mock(return_value=False)
        return manager

    with patch("ray_clickhouse._distributed.metadata_reader", side_effect=open_reader):
        return resolve_distributed(
            connection,
            QualifiedTable("db", "t"),
            schema if schema is not None else SchemaPlan(SCHEMA, COLUMNS[:1]),
            ResourceLimits(),
            filter_sql="id > 0",
            split="single",
        )


@pytest.mark.parametrize("comment", ["2026", "0", "-1"])
def test_numeric_describe_comments_do_not_appear_as_source_schema_drift(comment):
    client = Mock()
    client.query.return_value.result_rows = [("id", "Int64", "", "", comment, "", "")]
    client.query_arrow.return_value = pa.Table.from_batches([], schema=SCHEMA)
    with patch("ray_clickhouse._discovery._open", return_value=client):
        schema = discover_schema(
            CONNECTION, QualifiedTable("db", "t"), ResourceLimits()
        )
    assert schema.columns[0].position == 1
    source, target = resolved_readers()
    assert len(resolve_with_readers(source, target, schema=schema).targets) == 2


def test_source_position_disagreement_remains_a_schema_error():
    source, target = resolved_readers()
    source.columns.return_value = (replace(COLUMNS[0], position=2),)
    with pytest.raises(SchemaError, match="source declarations changed"):
        resolve_with_readers(source, target)


def test_projected_local_probe_keeps_complete_declarations_and_schema_checks():
    columns = COLUMNS[:1] + (
        TargetColumn("state", "AggregateFunction(sum,UInt64)", "", "", 2),
    )
    source, target = resolved_readers()
    source.columns.return_value = target.columns.return_value = columns
    target.schema.return_value = plan = SchemaPlan(SCHEMA, columns)
    resolved = resolve_with_readers(source, target, schema=plan)
    assert len(resolved.targets) == 2
    assert all(t.schema.columns == columns for t in resolved.targets)
    target.schema.assert_called_with(
        QualifiedTable("db", "data"), columns, column_names=("id",)
    )
    target.columns.return_value = (
        columns[0],
        replace(columns[1], declared_type="AggregateFunction(max,UInt64)"),
    )
    with pytest.raises(SchemaError, match="declarations disagree"):
        resolve_with_readers(source, target, schema=plan)


def test_local_schema_probe_converts_only_selected_arrow_columns():
    metadata = reader()
    columns = COLUMNS[:1] + (
        TargetColumn("state", "AggregateFunction(sum,UInt64)", "", "", 2),
    )
    metadata.client.query_arrow.return_value = pa.Table.from_batches([], schema=SCHEMA)
    result = metadata.schema(QualifiedTable("db", "t"), columns, column_names=("id",))
    assert result.columns == columns and result.arrow_schema == SCHEMA
    assert (
        metadata.client.query_arrow.call_args.args[0]
        == "SELECT `id` FROM `db`.`t` LIMIT 0"
    )


def test_resolver_selects_one_replica_and_preserves_http_tls_and_secrets():
    source, target = resolved_readers()
    connection = replace(
        CONNECTION,
        port=8443,
        secure=True,
        password_env="CH_TEST_PASSWORD",
        client_options=(("ca_cert", "/fixture/ca.crt"),),
    )
    result = resolve_with_readers(source, target, connection)
    assert [
        (item.shard, item.replica, item.connection.host) for item in result.targets
    ] == [(1, 1, "first"), (2, 1, "second")]
    assert all(
        item.connection.port == 8443
        and item.connection.secure
        and item.connection.password_env == "CH_TEST_PASSWORD"
        for item in result.targets
    )
    assert "unused" not in repr(result)


@pytest.mark.parametrize(
    "mutation,error",
    [
        ("duplicate-replica", DiscoveryError),
        ("bad-shard", DiscoveryError),
        ("bad-host", DiscoveryError),
        ("same-host", DiscoveryError),
        ("different-user", NotApplicable),
        ("empty", DiscoveryError),
        ("nested", NotApplicable),
        ("schema", SchemaError),
        ("context", NotApplicable),
    ],
)
def test_resolver_rejects_bad_topology_and_target_context(mutation, error):
    source, target = resolved_readers()
    rows = list(source.query.return_value)
    if mutation == "duplicate-replica":
        rows.append(rows[0])
    if mutation == "bad-shard":
        rows[0] = dict(rows[0], shard_num=0)
    if mutation == "bad-host":
        rows[0] = dict(rows[0], host_name="https://bad/path")
    if mutation == "same-host":
        rows[2] = dict(rows[2], host_name="first")
    if mutation == "different-user":
        rows[0] = dict(rows[0], user="internal")
    source.query.return_value = tuple(rows) if mutation != "empty" else ()
    if mutation == "nested":
        target.engine.return_value = ("Distributed", "Distributed(c,db,t)")
    if mutation == "schema":
        target.columns.return_value = (replace(COLUMNS[0], declared_type="UInt64"),)
    if mutation == "context":
        target.context.return_value = (("timezone", "other"),)
    with pytest.raises(error):
        resolve_with_readers(source, target)


def test_absent_system_columns_uses_describe_for_other_system_tables():
    metadata = reader()
    metadata.query = Mock(
        side_effect=[
            ({"name": "engine", "type": "String"},),
            NotApplicable(),
        ]
    )
    assert metadata.capability("tables") == {"engine": "String"}
    assert "system`.`columns" in metadata.query.call_args.args[0]


def test_absent_optional_parts_table_keeps_local_scan_capability():
    metadata = reader()
    metadata.capability = Mock(side_effect=NotApplicable())
    assert metadata.partitions(QualifiedTable("db", "t"), COLUMNS[:1]) is None


def test_unknown_partition_statistics_remain_unknown():
    from ray_clickhouse._discovery import PartitionInfo
    from ray_clickhouse._planning import group_partitions

    partitions = (
        PartitionInfo("a", None, None, None),
        PartitionInfo("b", 10, None, None),
    )
    assert group_partitions(partitions, target_tasks=2, max_tasks=2) == (("b",), ("a",))
    assert partitions[0].rows is None and partitions[0].bytes_on_disk is None


def test_shared_ray_fixture_does_not_reinitialize_or_shutdown_owner(monkeypatch):
    from integration.conftest import ray_runtime

    monkeypatch.setenv("RAY_CLICKHOUSE_IT_RAY_ADDRESS", "cluster:6379")
    monkeypatch.setenv("RAY_ADDRESS", "local")
    with (
        patch("ray.is_initialized", return_value=True),
        patch("ray.nodes", return_value=[{"Alive": True}] * 3),
        patch("ray.init") as initialize,
        patch("ray.shutdown") as shutdown,
    ):
        fixture = ray_runtime.__wrapped__()
        next(fixture)
        with pytest.raises(StopIteration):
            next(fixture)
        initialize.assert_not_called()
        shutdown.assert_not_called()


def test_fixed_native_policy_syntax_absence_is_capability_fallback_only():
    class ServerError(Exception):
        def __init__(self, code):
            self.code = code

    metadata = reader()
    metadata.client.query.side_effect = ServerError(62)
    with pytest.raises(NotApplicable):
        metadata.query("SHOW CREATE ROW POLICIES ON *.*")
    metadata.client.query.side_effect = ServerError(497)
    with pytest.raises(PermissionError):
        metadata.query("SHOW CREATE ROW POLICIES ON *.*")


@pytest.mark.parametrize("malformation", ["duplicate", "missing", "type", "scope"])
def test_policy_inventory_rejects_incomplete_or_malformed_rows(malformation):
    metadata = reader()
    metadata.fields["row_policies"] = {
        "id": "UUID",
        "database": "String",
        "table": "String",
    }
    row = {"id": uuid4(), "database": "db", "table": "t"}
    rows = (row, dict(row))
    if malformation == "missing":
        rows = (row,)
    elif malformation == "type":
        rows = (row, dict(row, id="not a UUID"))
    elif malformation == "scope":
        rows = (row, dict(row, id=uuid4(), database=None))
    metadata.query = Mock(side_effect=[({"statement": "private"},) * 2, rows])
    with pytest.raises(DiscoveryError):
        metadata.policies()
    assert metadata.policy_inventory is None


def test_complete_policy_inventory_is_cached_without_sql_text():
    metadata = reader()
    metadata.fields["row_policies"] = {
        "id": "UUID",
        "database": "String",
        "table": "String",
    }
    row = {"id": uuid4(), "database": "db", "table": "t"}
    metadata.query = Mock(side_effect=[({"statement": "private"},), (row,)])
    result = metadata.policies()
    assert metadata.policies() is result
    assert metadata.query.call_count == 2
    assert "private" not in repr(metadata.policy_inventory)


@pytest.mark.parametrize(
    "name,value",
    [
        ("additional_result_filter", "1"),
        ("max_result_rows", "1"),
        ("additional_table_filters", "{'t':'0'}"),
        ("read_overflow_mode", "break"),
        ("timeout_overflow_mode", "break"),
        ("group_by_overflow_mode", "any"),
        ("group_by_overflow_mode", "break"),
        ("sort_overflow_mode", "break"),
    ],
)
def test_unsafe_effective_settings_are_checked_before_policy_inventory(name, value):
    metadata = reader()
    metadata.fields["settings"] = {"name": "String", "value": "String"}
    effective = dict(CONTEXT_DEFAULTS, **{name: value})
    metadata.query = Mock(
        return_value=tuple({"name": k, "value": v} for k, v in effective.items())
    )
    metadata.policies = Mock()
    with pytest.raises(NotApplicable):
        metadata.context(QualifiedTable("db", "t"))
    metadata.policies.assert_not_called()


def test_system_columns_must_match_complete_description():
    metadata = reader()
    metadata.query = Mock(
        side_effect=[
            ({"name": "name", "type": "String"}, {"name": "type", "type": "String"}),
            ({"name": "name", "type": "String"},),
        ]
    )
    with pytest.raises(DiscoveryError, match="full DESCRIBE"):
        metadata.capability("columns")


def test_unused_complex_fields_do_not_require_type_sql_normalization():
    metadata = reader()
    metadata.query = Mock(
        side_effect=[
            (
                {"name": "name", "type": "String"},
                {
                    "name": "extra",
                    "type": "Array(Tuple(\n name String,\n type String))",
                },
            ),
            (
                {"name": "name", "type": "String"},
                {"name": "extra", "type": "Array(Tuple(name String, type String))"},
            ),
        ]
    )
    metadata.require_fields("columns", {"name": "str"})
    assert "extra" in metadata.capability("columns")


def test_consumed_system_field_type_must_agree_with_native_description():
    metadata = reader()
    metadata.fields["parts"] = {"rows": "UInt64"}
    metadata.descriptions[QualifiedTable("system", "parts")] = (
        {"name": "rows", "type": "UInt32"},
    )
    with pytest.raises(DiscoveryError, match="declarations disagree"):
        metadata.require_fields("parts", {"rows": "int"})


@pytest.mark.parametrize(
    "error", [ObjectNotFoundError("missing"), PermissionError("hidden"), None]
)
def test_empty_engine_metadata_requires_native_visibility_proof(error):
    metadata = reader()
    metadata.fields["tables"] = {
        name: "String" for name in ("engine", "engine_full", "database", "name")
    }
    metadata.query = Mock(return_value=())
    metadata.describe = Mock(side_effect=error)
    with pytest.raises(type(error) if error else DiscoveryError):
        metadata.engine(QualifiedTable("db", "t"))
    metadata.describe.assert_called_once_with(QualifiedTable("db", "t"))


def test_target_context_precedes_engine_metadata_and_native_visibility():
    source, target = resolved_readers()
    target.context.side_effect = NotApplicable("metadata row policy")
    with pytest.raises(NotApplicable):
        resolve_with_readers(source, target)
    target.engine.assert_not_called()
    target.describe.assert_not_called()
    target.schema.assert_not_called()


@pytest.mark.parametrize(
    "error", [PermissionError("hidden"), ObjectNotFoundError("missing")]
)
def test_native_target_visibility_errors_precede_filtered_engine_metadata(error):
    source, target = resolved_readers()
    target.describe.side_effect = error
    with pytest.raises(type(error)):
        resolve_with_readers(source, target)
    target.engine.assert_not_called()


def test_business_column_type_formatting_is_normalized_without_changing_model():
    metadata = reader()
    metadata.fields["columns"] = {
        name: "String"
        for name in (
            "name",
            "type",
            "default_kind",
            "default_expression",
            "database",
            "table",
        )
    }
    metadata.fields["columns"]["position"] = "UInt64"
    metadata.describe = Mock(
        return_value=(
            {
                "name": "payload",
                "type": "Tuple(\n a Int64,\n b String)",
                "default_type": "",
                "default_expression": "",
            },
        )
    )
    metadata.query = Mock(
        return_value=(
            {
                "name": "payload",
                "type": "Tuple(a Int64, b String)",
                "position": 1,
                "default_kind": "",
                "default_expression": "",
            },
        )
    )
    assert metadata.columns(QualifiedTable("db", "t")) == (
        TargetColumn("payload", "Tuple(a Int64, b String)", "", "", 1),
    )
