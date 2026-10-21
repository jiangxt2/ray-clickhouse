"""Exercise native metadata authorization on real nodes with both grant modes."""

from dataclasses import replace
from uuid import uuid4

import clickhouse_connect
import pytest

from ray_clickhouse._distributed import MetadataReader, NotApplicable, metadata_reader
from ray_clickhouse._errors import DiscoveryError, PermissionError
from ray_clickhouse._models import ClickHouseConnection, QualifiedTable, ResourceLimits
from ray_clickhouse.datasource import ClickHouseDatasource, ClickHouseReadConfig

pytestmark = [pytest.mark.integration, pytest.mark.cluster, pytest.mark.distributed]
DATABASE = "ray_clickhouse_it"


@pytest.mark.parametrize("index,host", [(0, "clickhouse"), (2, "ch3")])
def test_inherited_roles_disabled_roles_and_revocation(cluster_tables, index, host):
    admin = cluster_tables.clients[index]
    suffix = uuid4().hex[:12]
    user, inner, outer = (f"{name}_{suffix}" for name in ("reader", "inner", "outer"))
    try:
        admin.command(f"CREATE USER {user}")
        admin.command(f"CREATE ROLE {inner}")
        admin.command(f"CREATE ROLE {outer}")
        admin.command(f"GRANT SELECT, SHOW COLUMNS ON *.* TO {inner}")
        admin.command(f"GRANT SHOW ROW POLICIES ON *.* TO {inner}")
        admin.command(f"GRANT {inner} TO {outer}")
        admin.command(f"GRANT {outer} TO {user}")
        admin.command(f"SET DEFAULT ROLE {outer} TO {user}")
        connection = ClickHouseConnection(host, DATABASE, username=user)
        table = QualifiedTable(DATABASE, cluster_tables.local)
        with metadata_reader(connection, ResourceLimits()) as reader:
            assert reader.columns(table)
            assert reader.context(table)
            # A new reader avoids relying on the already-verified static snapshot.
            reader.client.command("SET ROLE NONE")
            fresh = MetadataReader(reader.client, connection, ResourceLimits())
            with pytest.raises(PermissionError):
                fresh.policies()
        admin.command(f"REVOKE SHOW ROW POLICIES ON *.* FROM {inner}")
        with metadata_reader(connection, ResourceLimits()) as reader:
            with pytest.raises(PermissionError):
                reader.context(table)
    finally:
        admin.command(f"DROP USER IF EXISTS {user}")
        admin.command(f"DROP ROLE IF EXISTS {outer}")
        admin.command(f"DROP ROLE IF EXISTS {inner}")


@pytest.mark.parametrize("index,host", [(0, "clickhouse"), (2, "ch3")])
def test_select_policy_metadata_is_not_enough_and_filtering_is_detected(
    cluster_tables, index, host
):
    admin = cluster_tables.clients[index]
    user, policy = (f"{name}_{uuid4().hex[:12]}" for name in ("reader", "hidden"))
    try:
        admin.command(f"CREATE USER {user}")
        admin.command(f"GRANT SELECT, SHOW COLUMNS ON *.* TO {user}")
        connection = ClickHouseConnection(host, DATABASE, username=user)
        with metadata_reader(connection, ResourceLimits()) as reader:
            with pytest.raises(PermissionError):
                reader.context(QualifiedTable(DATABASE, cluster_tables.local))
        admin.command(f"GRANT SHOW ROW POLICIES ON *.* TO {user}")
        admin.command(
            f"CREATE ROW POLICY {policy} ON system.row_policies USING 0 TO {user}"
        )
        with metadata_reader(connection, ResourceLimits()) as reader:
            assert reader.query("SELECT id FROM system.row_policies") == ()
            assert reader.query("SHOW CREATE ROW POLICIES ON *.*")
            with pytest.raises(DiscoveryError, match="inventory"):
                reader.context(QualifiedTable(DATABASE, cluster_tables.local))
    finally:
        admin.command(f"DROP ROW POLICY IF EXISTS {policy} ON system.row_policies")
        admin.command(f"DROP USER IF EXISTS {user}")


def test_partial_column_visibility_is_not_a_complete_schema(cluster_tables):
    admin = cluster_tables.clients[0]
    user = f"columns_{uuid4().hex[:12]}"
    table = QualifiedTable(DATABASE, cluster_tables.local)
    try:
        admin.command(f"CREATE USER {user}")
        admin.command(f"GRANT SELECT(id) ON {table.sql()} TO {user}")
        admin.command(f"GRANT SELECT ON system.columns TO {user}")
        with metadata_reader(
            ClickHouseConnection("clickhouse", DATABASE, username=user),
            ResourceLimits(),
        ) as reader:
            rows = reader.query(
                "SELECT name FROM system.columns WHERE database = %(db)s "
                "AND table = %(table)s",
                {"db": DATABASE, "table": table.table},
            )
            assert rows == ({"name": "id"},)
            with pytest.raises(PermissionError):
                reader.columns(table)
    finally:
        admin.command(f"DROP USER IF EXISTS {user}")


@pytest.mark.parametrize("host", ["clickhouse", "ch3"])
def test_xml_policies_share_verified_inventory_and_fallback(cluster_tables, host):
    index = 0 if host == "clickhouse" else 2
    admin = cluster_tables.clients[index]
    table = QualifiedTable(DATABASE, "xml_local")
    client = clickhouse_connect.get_client(host=host, username="xml_reader")
    try:
        admin.command(
            f"CREATE TABLE {table.sql()} (id Int64) ENGINE=MergeTree ORDER BY id"
        )
        admin.command(f"INSERT INTO {table.sql()} VALUES (1), (2)")
        assert client.query(f"SELECT id FROM {table.sql()}").result_rows == [(1,)]
        with metadata_reader(
            ClickHouseConnection(host, DATABASE), ResourceLimits()
        ) as reader:
            inventory = reader.policies()
            assert any(
                db == DATABASE and name == "xml_local" for _, db, name in inventory
            )
            with pytest.raises(NotApplicable, match="row policies"):
                reader.context(table)
    finally:
        client.close()
        admin.command(f"DROP TABLE IF EXISTS {table.sql()} SYNC")


@pytest.mark.parametrize(
    "setting,value",
    [
        ("additional_result_filter", "1"),
        ("read_overflow_mode", "break"),
        ("timeout_overflow_mode", "break"),
    ],
)
def test_effective_extra_result_filter_selects_whole_logical_plan(
    cluster_tables, setting, value
):
    admin = cluster_tables.clients[0]
    user = f"filter_{uuid4().hex[:12]}"
    try:
        # Profile settings, unlike explicit connector settings, reach discovery
        # through system.settings and must be checked before policy enumeration.
        admin.command(f"CREATE USER {user} SETTINGS {setting} = '{value}'")
        admin.command(f"GRANT SELECT, SHOW COLUMNS ON *.* TO {user}")
        admin.command(f"GRANT SHOW ROW POLICIES ON *.* TO {user}")
        config = ClickHouseReadConfig(
            ClickHouseConnection("clickhouse", DATABASE, username=user),
            QualifiedTable(DATABASE, cluster_tables.distributed),
            filter_sql="id >= 0",
            convert_distributed_to_local=True,
        )
        source = ClickHouseDatasource(config)
        with metadata_reader(config.connection, ResourceLimits()) as reader:
            with pytest.raises(NotApplicable, match="result or replica settings"):
                reader.context(config.table)
        assert len(source.get_read_tasks(4)) == 1 and not source._local_sources
        # The original user context remains unchanged; a default-user reader
        # still proves the metadata contract on the same server.
        with metadata_reader(
            replace(config.connection, username="default"), ResourceLimits()
        ) as reader:
            assert reader.context(config.table)
    finally:
        admin.command(f"DROP USER IF EXISTS {user}")


@pytest.mark.parametrize(
    "index,system_table,predicate",
    [(0, "clusters", "shard_num = 1"), (2, "parts", "0")],
)
def test_metadata_row_policies_cannot_hide_shards_or_partitions(
    cluster_tables, index, system_table, predicate
):
    from collections import Counter

    tables = cluster_tables
    admin = tables.clients[index]
    policy = f"metadata_{uuid4().hex[:12]}"
    try:
        admin.command(
            f"CREATE ROW POLICY {policy} ON system.{system_table} "
            f"USING {predicate} TO default"
        )
        if system_table == "clusters":
            assert admin.query(
                "SELECT DISTINCT shard_num FROM system.clusters "
                "WHERE cluster='local_scan'"
            ).result_rows == [(1,)]
        else:
            assert admin.query("SELECT count() FROM system.parts").first_row[0] == 0
        source = ClickHouseDatasource(
            ClickHouseReadConfig(
                ClickHouseConnection("clickhouse", DATABASE),
                QualifiedTable(DATABASE, tables.distributed),
                filter_sql="id >= 0",
                split="partition",
                convert_distributed_to_local=True,
            )
        )
        tasks = source.get_read_tasks(4)
        assert len(tasks) == 1 and not source._local_sources
        rows = [row for task in tasks for block in task() for row in block.to_pylist()]
        assert Counter(tuple(sorted(row.items())) for row in rows) == Counter(
            tuple(sorted(row.items())) for row in tables.rows
        )
    finally:
        admin.command(f"DROP ROW POLICY IF EXISTS {policy} ON system.{system_table}")
