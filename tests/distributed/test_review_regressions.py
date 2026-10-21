"""Real-node regressions for the reviewed metadata and fallback boundaries."""

from collections import Counter
from uuid import uuid4

import clickhouse_connect
import pytest
import ray

from ray_clickhouse._distributed import NotApplicable, metadata_reader
from ray_clickhouse._errors import PermissionError, SchemaError
from ray_clickhouse._models import ClickHouseConnection, QualifiedTable, ResourceLimits
from ray_clickhouse.datasource import ClickHouseDatasource, ClickHouseReadConfig

pytestmark = [pytest.mark.integration, pytest.mark.cluster, pytest.mark.distributed]
DATABASE = "ray_clickhouse_it"
HOSTS = ("clickhouse", "ch2", "ch3", "ch4")


def source(tables, **options):
    return ClickHouseDatasource(
        ClickHouseReadConfig(
            ClickHouseConnection("clickhouse", DATABASE),
            QualifiedTable(DATABASE, tables.distributed),
            filter_sql="id >= 0",
            convert_distributed_to_local=True,
            **options,
        )
    )


def rows_from(tasks):
    return [row for task in tasks for block in task() for row in block.to_pylist()]


def assert_complete_logical_fallback(tables, datasource):
    tasks = datasource.get_read_tasks(4)
    assert len(tasks) == 1 and not datasource._local_sources
    assert datasource._resolved_distributed is None
    assert Counter(tuple(sorted(row.items())) for row in rows_from(tasks)) == Counter(
        tuple(sorted(row.items())) for row in tables.rows
    )


@pytest.mark.parametrize("index", [0, 2])
@pytest.mark.parametrize("mode", ["any", "break"])
def test_profile_aggregate_overflow_selects_complete_logical_read(
    cluster_tables, index, mode
):
    tables = cluster_tables
    admin = tables.clients[index]
    profile = f"overflow_{uuid4().hex[:12]}"
    try:
        admin.command(
            f"CREATE SETTINGS PROFILE {profile} SETTINGS "
            f"group_by_overflow_mode = '{mode}', max_rows_to_group_by = 1 TO default"
        )
        client = clickhouse_connect.get_client(host=HOSTS[index], query_retries=0)
        try:
            # Force separate input blocks to actually exercise the partial-key
            # behavior, rather than assuming a small one-block aggregate truncates.
            partial = client.query(
                "SELECT number, count() FROM numbers(128) GROUP BY number",
                settings={"max_block_size": 1, "max_threads": 1},
            ).result_rows
            assert 0 < len(partial) < 128
        finally:
            client.close()
        with metadata_reader(
            ClickHouseConnection(HOSTS[index], DATABASE), ResourceLimits()
        ) as reader:
            with pytest.raises(NotApplicable, match="result or replica settings"):
                reader.context(QualifiedTable(DATABASE, tables.local))
        # The cluster and direct-read identities are both default, so an identity
        # mismatch cannot accidentally make this regression pass.
        assert_complete_logical_fallback(tables, source(tables, split="partition"))
    finally:
        admin.command(f"DROP SETTINGS PROFILE IF EXISTS {profile}")


@pytest.mark.parametrize("index", [0, 2])
def test_profile_sort_overflow_precedes_sorted_metadata(cluster_tables, index):
    tables = cluster_tables
    admin = tables.clients[index]
    profile = f"sort_{uuid4().hex[:12]}"
    try:
        admin.command(
            f"CREATE SETTINGS PROFILE {profile} SETTINGS sort_overflow_mode = 'break', "
            "max_rows_to_sort = 1 TO default"
        )
        assert_complete_logical_fallback(tables, source(tables, split="partition"))
    finally:
        admin.command(f"DROP SETTINGS PROFILE IF EXISTS {profile}")


def add_named_tuple(tables, *, field="a Int64"):
    for index in (0, 2):
        tables.clients[index].command(
            f"ALTER TABLE {DATABASE}.{tables.local} ADD COLUMN payload "
            "Tuple(a Int64, b String) DEFAULT tuple(id, txt)"
        )
    for client in tables.clients:
        client.command(f"SYSTEM SYNC REPLICA {DATABASE}.{tables.local}")
    admin = tables.clients[0]
    admin.command(f"DROP TABLE {DATABASE}.{tables.distributed} SYNC")
    admin.command(
        f"CREATE TABLE {DATABASE}.{tables.distributed} "
        "(id Int64, value Nullable(Int64), tenant UInt8, txt String, "
        f"payload Tuple({field}, b String) DEFAULT tuple(id, txt)) "
        f"ENGINE = Distributed(local_scan, '{DATABASE}', '{tables.local}', rand())"
    )


@pytest.mark.parametrize("projection", [None, ("id",)])
def test_named_tuple_metadata_and_values_reach_ray_workers(cluster_tables, projection):
    tables = cluster_tables
    add_named_tuple(tables)
    datasource = source(tables, columns=projection)
    assert len(datasource.get_read_tasks(2)) == 2 and datasource._local_sources
    rows = ray.data.read_datasource(
        datasource, override_num_blocks=2, ray_remote_args={"max_retries": 0}
    ).take_all()
    expected = [
        {"id": row["id"]}
        if projection
        else dict(row, payload={"a": row["id"], "b": row["txt"]})
        for row in tables.rows
    ]
    assert sorted(rows, key=repr) == sorted(expected, key=repr)


@pytest.mark.parametrize("field", ["a UInt64", "different Int64"])
def test_named_tuple_type_or_field_name_disagreement_remains_error(
    cluster_tables, field
):
    add_named_tuple(cluster_tables, field=field)
    with pytest.raises(SchemaError):
        source(cluster_tables).get_read_tasks(4)


def test_target_tables_policy_falls_back_before_engine_absence(cluster_tables):
    tables = cluster_tables
    admin = tables.clients[2]
    policy = f"tables_{uuid4().hex[:12]}"
    try:
        admin.command(
            f"CREATE ROW POLICY {policy} ON system.tables USING "
            f"NOT (database = '{DATABASE}' AND name = '{tables.local}') TO default"
        )
        assert (
            admin.query(
                "SELECT name FROM system.tables WHERE database = %(db)s "
                "AND name = %(table)s",
                parameters={"db": DATABASE, "table": tables.local},
            ).result_rows
            == []
        )
        assert admin.query(f"DESCRIBE TABLE {DATABASE}.{tables.local}").result_rows
        assert (
            admin.query(f"SELECT count() FROM {DATABASE}.{tables.local}").first_row[0]
            > 0
        )
        assert_complete_logical_fallback(tables, source(tables))
    finally:
        admin.command(f"DROP ROW POLICY IF EXISTS {policy} ON system.tables")


def test_hidden_target_native_description_reports_permission(cluster_tables):
    tables = cluster_tables
    admin = tables.clients[2]
    user = f"visibility_{uuid4().hex[:12]}"
    try:
        admin.command(f"CREATE USER {user}")
        admin.command(f"GRANT SELECT ON system.* TO {user}")
        with metadata_reader(
            ClickHouseConnection("ch3", "default", username=user), ResourceLimits()
        ) as reader:
            assert (
                reader.query(
                    "SELECT name FROM system.tables WHERE database = %(db)s "
                    "AND name = %(table)s",
                    {"db": DATABASE, "table": tables.local},
                )
                == ()
            )
            with pytest.raises(PermissionError):
                reader.engine(QualifiedTable(DATABASE, tables.local))
    finally:
        admin.command(f"DROP USER IF EXISTS {user}")
