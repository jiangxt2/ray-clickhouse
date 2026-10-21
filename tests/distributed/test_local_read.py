from __future__ import annotations

import time
from collections import Counter
from dataclasses import replace
from unittest.mock import patch

import pytest
import ray

from ray_clickhouse import read_clickhouse
from ray_clickhouse._distributed import MetadataReader, NotApplicable
from ray_clickhouse._errors import (
    DiscoveryError,
    PermissionError,
    SchemaError,
    TransportError,
)
from ray_clickhouse._models import ClickHouseConnection, QualifiedTable, ResourceLimits
from ray_clickhouse.datasource import ClickHouseDatasource, ClickHouseReadConfig

pytestmark = [pytest.mark.integration, pytest.mark.cluster, pytest.mark.distributed]
DATABASE = "ray_clickhouse_it"


def source(tables, **kwargs):
    options = dict(
        filter_sql="value IS NULL OR value >= %(lo)s",
        query_parameters=(("lo", 9),),
        convert_distributed_to_local=True,
        target_tasks=4,
        limits=ResourceLimits(batch_rows=3),
    )
    options.update(kwargs)
    return ClickHouseDatasource(
        ClickHouseReadConfig(
            ClickHouseConnection("clickhouse", DATABASE),
            QualifiedTable(DATABASE, tables.distributed),
            **options,
        )
    )


def execute(datasource, parallelism=4, limit=None):
    tasks = datasource.get_read_tasks(parallelism, per_task_row_limit=limit)
    return tasks, [
        row for task in tasks for block in task() for row in block.to_pylist()
    ]


def multiset(rows):
    return Counter(tuple(sorted(row.items())) for row in rows)


@pytest.mark.parametrize("split", ["single", "partition", "range", "auto"])
@pytest.mark.parametrize("budget", [1, 4])
def test_all_shards_duplicates_nulls_and_global_budget(cluster_tables, split, budget):
    tables = cluster_tables
    datasource = source(
        tables, split=split, **({"range_column": "value"} if split == "range" else {})
    )
    tasks, rows = execute(datasource, budget)
    expected = [row for row in tables.rows if row["value"] is None or row["value"] >= 9]
    assert multiset(rows) == multiset(expected)
    assert len(tasks) <= budget
    targets = datasource._resolved_distributed.targets
    assert [
        (target.shard, target.replica, target.connection.host) for target in targets
    ] == [(1, 1, "clickhouse"), (2, 1, "ch3")]
    for index, client in enumerate(tables.clients):
        client.command("SYSTEM FLUSH LOGS")
        count = client.query(
            "SELECT count() FROM system.query_log WHERE type = "
            "'QueryFinish' AND is_initial_query = 1 "
            "AND startsWith(log_comment, 'ray-clickhouse operation=read ') "
            "AND position(query, %(table)s) > 0",
            parameters={"table": tables.local},
        ).first_row[0]
        assert (count > 0) == (index in (0, 2))
    assert "engine_full" in dict(targets[0].capabilities.origins).values()


def test_cached_plan_regroups_and_task_row_limit(cluster_tables):
    datasource = source(cluster_tables)
    tasks, rows = execute(datasource, 1, 3)
    assert len(tasks) == 1 and len(rows) == 3
    resolved = datasource._resolved_distributed
    tasks, rows = execute(datasource, 4)
    assert len(tasks) == 2
    assert datasource._resolved_distributed is resolved
    assert len(rows) > 3


@pytest.mark.parametrize("split", ["single", "partition", "range", "auto"])
@pytest.mark.parametrize("columns", [["id"], []])
def test_dataset_projection_reaches_cached_shards(cluster_tables, split, columns):
    from ray_clickhouse._compat import supports_v1_projection_pushdown

    datasource = source(
        cluster_tables,
        split=split,
        **({"range_column": "value"} if split == "range" else {}),
    )
    datasource.get_read_tasks(4)
    children = datasource._local_sources
    original_schemas = [child.arrow_schema for child in children]
    original_maps = [child.get_projection_map() for child in children]
    dataset = ray.data.read_datasource(datasource, override_num_blocks=4, concurrency=2)
    result = dataset.select_columns(columns).materialize()
    expected = [
        row for row in cluster_tables.rows if row["value"] is None or row["value"] >= 9
    ]
    reference = ray.data.from_items(expected).select_columns(columns).materialize()
    assert result.count() == reference.count()
    assert multiset(result.take_all()) == multiset(reference.take_all())
    assert [child.arrow_schema for child in children] == original_schemas
    assert [child.get_projection_map() for child in children] == original_maps
    for client in (cluster_tables.clients[0], cluster_tables.clients[2]):
        client.command("SYSTEM FLUSH LOGS")
        queries = client.query(
            "SELECT query FROM system.query_log WHERE type = 'QueryFinish' "
            "AND is_initial_query = 1 "
            "AND startsWith(log_comment, 'ray-clickhouse operation=read ') "
            "AND position(query, %(table)s) > 0",
            parameters={"table": cluster_tables.local},
        ).result_rows
        assert queries
        if supports_v1_projection_pushdown():
            assert all(row[0].startswith("SELECT `id` FROM") for row in queries)


@pytest.mark.parametrize(
    "options",
    [
        {"filter_sql": None, "query_parameters": ()},
        {"filter_sql": "hostName() != ''", "query_parameters": ()},
        {"filter_sql": "id >= 0", "query_parameters": (), "order_by": (("id", False),)},
    ],
)
def test_logical_fallback(cluster_tables, options):
    datasource = source(cluster_tables, **options)
    tasks, rows = execute(datasource)
    assert len(tasks) == 1 and not datasource._local_sources
    assert multiset(rows) == multiset(cluster_tables.rows)


def test_default_flag_preserves_logical_read(cluster_tables):
    datasource = source(cluster_tables, convert_distributed_to_local=False)
    tasks, rows = execute(datasource)
    assert len(tasks) == 1 and not datasource._local_sources and rows


@pytest.mark.parametrize(
    "origin", ["create_table_query", "show-create", "describe-columns", "cluster-name"]
)
def test_secondary_metadata_origins_on_real_server(cluster_tables, origin):
    original = MetadataReader.capability
    cluster_fields = {
        row[0]
        for row in cluster_tables.clients[0]
        .query("DESCRIBE TABLE system.clusters")
        .result_rows
    }

    def capability(self, table):
        fields = dict(original(self, table))
        # Simulate capability differences per node while retaining real queries,
        # real result names, declarations and Arrow protocol probes.
        if self.connection.host in {"clickhouse", "ch3"}:
            if table == "tables" and origin in {"create_table_query", "show-create"}:
                fields.pop("engine_full", None)
                if origin == "show-create":
                    fields.pop("create_table_query", None)
            if table == "columns" and origin == "describe-columns":
                fields.pop("position", None)
        if table == "clusters" and origin == "cluster-name":
            fields.pop("cluster", None)
        self.fields[table] = fields
        return fields

    with patch.object(MetadataReader, "capability", capability):
        datasource = source(cluster_tables)
        _, rows = execute(datasource)
    # Some old nodes expose only cluster, with no name alias. Removing cluster
    # must then prove whole-source fallback rather than inventing an alias.
    eligible = origin != "cluster-name" or "name" in cluster_fields
    assert bool(datasource._local_sources) is eligible
    assert multiset(rows) == multiset(
        row for row in cluster_tables.rows if row["value"] is None or row["value"] >= 9
    )


def test_any_shard_missing_required_capability_falls_back_whole_source(cluster_tables):
    original = MetadataReader.context

    def context(self, table):
        if self.connection.host == "ch3":
            raise NotApplicable("required metadata unavailable")
        return original(self, table)

    with patch.object(MetadataReader, "context", context):
        datasource = source(cluster_tables)
        tasks, rows = execute(datasource)
    assert len(tasks) == 1 and not datasource._local_sources and rows


def test_row_policy_fallback_and_metadata_permission_failure(cluster_tables):
    tables = cluster_tables
    policy = f"policy_{tables.local}"
    client = tables.clients[2]
    try:
        client.command(
            f"CREATE ROW POLICY {policy} ON {DATABASE}.{tables.local} "
            f"USING id >= 0 TO ALL"
        )
        datasource = source(tables)
        assert len(datasource.get_read_tasks(4)) == 1 and not datasource._local_sources
    finally:
        client.command(
            f"DROP ROW POLICY IF EXISTS {policy} ON {DATABASE}.{tables.local}"
        )
    user = f"reader_{tables.local}"
    try:
        for client in tables.clients:
            client.command(f"CREATE USER {user}")
            client.command(f"GRANT SELECT ON *.* TO {user}")
        config = replace(
            source(tables).config,
            connection=ClickHouseConnection("clickhouse", DATABASE, username=user),
        )
        with pytest.raises(PermissionError):
            ClickHouseDatasource(config).get_read_tasks(4)
    finally:
        for client in tables.clients:
            client.command(f"DROP USER IF EXISTS {user}")


def test_target_schema_drift_is_error(cluster_tables):
    tables = cluster_tables
    tables.clients[2].command(
        f"ALTER TABLE {DATABASE}.{tables.local} ADD COLUMN extra UInt8"
    )
    with pytest.raises(SchemaError):
        source(tables).get_read_tasks(4)


def test_no_full_fallback_after_local_task_started(cluster_tables):
    datasource = source(cluster_tables)
    tasks = datasource.get_read_tasks(1)
    selected = datasource._local_sources[1]
    selected._config = replace(
        selected.config, connection=replace(selected.config.connection, port=1)
    )
    # Rebuild tasks from the same immutable target snapshot with a failing
    # transport endpoint; topology and source path remain fixed.
    tasks = datasource.get_read_tasks(1)
    stream = iter(tasks[0]())
    assert next(stream).num_rows > 0
    with pytest.raises(TransportError):
        list(stream)
    assert datasource._resolved_distributed is not None


def test_public_dataset_runs_local_reads_on_ray_workers(cluster_tables):
    tables = cluster_tables
    dataset = read_clickhouse(
        host="clickhouse",
        database=DATABASE,
        table=tables.distributed,
        filter="id >= {lo:Int64}",
        query_parameters={"lo": 0},
        convert_distributed_to_local=True,
        split="partition",
        target_tasks=4,
        batch_rows=3,
        ray_remote_args={"max_retries": 0},
    )
    assert multiset(dataset.take_all()) == multiset(tables.rows)
    assert len([node for node in ray.nodes() if node["Alive"]]) >= 3


def test_both_shards_execute_on_distinct_ray_workers(cluster_tables):
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    tables = cluster_tables
    tasks = source(tables, filter_sql="id >= 0", query_parameters=()).get_read_tasks(2)
    workers = sorted(
        (
            node
            for node in ray.nodes()
            if node["Alive"] and node["Resources"].get("CPU", 0)
        ),
        key=lambda node: node["NodeManagerAddress"],
    )
    assert len(workers) == 2

    @ray.remote(num_cpus=1, max_retries=0)
    def read_task(task):
        return [row for block in task() for row in block.to_pylist()]

    results = ray.get(
        [
            read_task.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node["NodeID"], soft=False
                )
            ).remote(task)
            for task, node in zip(tasks, workers, strict=True)
        ]
    )
    assert multiset(row for rows in results for row in rows) == multiset(tables.rows)
    for index, node in zip((0, 2), workers, strict=True):
        client = tables.clients[index]
        client.command("SYSTEM FLUSH LOGS")
        addresses = client.query(
            "SELECT DISTINCT toString(initial_address) FROM system.query_log WHERE "
            "type = 'QueryFinish' AND is_initial_query = 1 AND "
            "startsWith(log_comment, 'ray-clickhouse operation=read ') "
            "AND position(query, %(table)s) > 0",
            parameters={"table": tables.local},
        ).result_rows
        assert addresses and all(
            address[0].removeprefix("::ffff:") == node["NodeManagerAddress"]
            for address in addresses
        )


def test_https_preserves_ca_and_hostname_on_workers(cluster_tables):
    tables = cluster_tables
    dataset = read_clickhouse(
        host="clickhouse",
        port=8443,
        secure=True,
        database=DATABASE,
        table=tables.distributed,
        filter="id >= 0",
        convert_distributed_to_local=True,
        client_options={"ca_cert": "/etc/clickhouse-it-tls/ca.crt"},
        ray_remote_args={"max_retries": 0},
    )
    assert multiset(dataset.take_all()) == multiset(tables.rows)


def test_https_discovered_host_must_match_certificate(cluster_tables):
    original = MetadataReader.query

    def query(self, sql, parameters=None, **kwargs):
        rows = original(self, sql, parameters, **kwargs)
        if "FROM system.clusters" in sql:
            rows = tuple(
                dict(row, host_name="10.250.0.20")
                if row["host_name"] == "clickhouse"
                else row
                for row in rows
            )
        return rows

    datasource = source(cluster_tables)
    datasource._config = replace(
        datasource.config,
        connection=replace(
            datasource.config.connection,
            port=8443,
            secure=True,
            client_options=(("ca_cert", "/etc/clickhouse-it-tls/ca.crt"),),
        ),
    )
    with patch.object(MetadataReader, "query", query), pytest.raises(TransportError):
        datasource.get_read_tasks(4)


def test_empty_shard_is_covered_without_duplicate_replica_output(cluster_tables):
    tables = cluster_tables
    for client in tables.clients[2:]:
        client.command(f"TRUNCATE TABLE {DATABASE}.{tables.local}")
    datasource = source(tables)
    _, rows = execute(datasource, 1)
    assert multiset(rows) == multiset(
        row
        for row in tables.rows
        if row["id"] < 12 and (row["value"] is None or row["value"] >= 9)
    )
    assert len(datasource._resolved_distributed.targets) == 2


def test_optional_split_capability_keeps_local_path_and_strict_errors(cluster_tables):
    original = MetadataReader.capability

    def capability(self, table):
        fields = dict(original(self, table))
        if table == "parts":
            fields.pop("partition_id", None)
        self.fields[table] = fields
        return fields

    with patch.object(MetadataReader, "capability", capability):
        datasource = source(cluster_tables, split="partition")
        tasks, rows = execute(datasource)
        assert len(tasks) == 2 and rows and datasource._local_sources
        strict = source(cluster_tables, split="partition", discovery_policy="error")

        with pytest.raises(DiscoveryError):
            strict.get_read_tasks(4)
        assert strict._resolved_distributed is None
        # A failed plan cannot leave a usable, partially initialized cache.
        with pytest.raises(DiscoveryError):
            strict.get_read_tasks(4)
        with patch.object(
            MetadataReader, "context", side_effect=NotApplicable("new context")
        ):
            assert len(strict.get_read_tasks(4)) == 1
            assert not strict._local_sources


def test_physical_column_order_does_not_change_logical_projection(cluster_tables):
    tables = cluster_tables
    tables.clients[0].command(f"DROP TABLE {DATABASE}.{tables.distributed} SYNC")
    tables.clients[0].command(
        f"CREATE TABLE {DATABASE}.{tables.distributed} "
        "(txt String, tenant UInt8, value Nullable(Int64), id Int64) "
        f"ENGINE = Distributed(local_scan, '{DATABASE}', '{tables.local}', rand())"
    )
    datasource = source(tables, filter_sql="id >= 0", query_parameters=())
    tasks = datasource.get_read_tasks(4)
    blocks = [block for task in tasks for block in task()]
    assert all(
        block.column_names == ["txt", "tenant", "value", "id"] for block in blocks
    )
    assert multiset(row for block in blocks for row in block.to_pylist()) == multiset(
        tables.rows
    )


def test_closing_group_does_not_open_next_shard(cluster_tables):
    tables = cluster_tables
    datasource = source(tables)
    stream = iter(datasource.get_read_tasks(1)[0]())
    assert next(stream).num_rows > 0
    stream.close()
    tables.clients[2].command("SYSTEM FLUSH LOGS")
    count = (
        tables.clients[2]
        .query(
            "SELECT count() FROM system.query_log WHERE "
            "startsWith(log_comment, 'ray-clickhouse operation=read ') "
            "AND position(query, %(table)s) > 0",
            parameters={"table": tables.local},
        )
        .first_row[0]
    )
    assert count == 0


@pytest.mark.parametrize("weight", ["rows", "equal"])
def test_missing_weight_fields_keep_all_partitions(cluster_tables, weight):
    original = MetadataReader.capability

    def capability(self, table):
        fields = dict(original(self, table))
        if table == "parts":
            fields.pop("bytes_on_disk", None)
            if weight == "equal":
                fields.pop("rows", None)
        self.fields[table] = fields
        return fields

    with patch.object(MetadataReader, "capability", capability):
        datasource = source(cluster_tables, split="partition")
        _, rows = execute(datasource)
    expected = [
        row for row in cluster_tables.rows if row["value"] is None or row["value"] >= 9
    ]
    assert multiset(rows) == multiset(expected)
    assert all(
        dict(target.capabilities.origins)[f"partition-weight:{target.table}"] == weight
        for target in datasource._resolved_distributed.targets
    )


def test_worker_password_environment_failure_does_not_read_logical_source(
    cluster_tables, monkeypatch
):
    tables = cluster_tables
    monkeypatch.setenv("RAY_CH_LOCAL_READ_TEST_PASSWORD", "")
    dataset = read_clickhouse(
        host="clickhouse",
        database=DATABASE,
        table=tables.distributed,
        filter="id >= 0",
        convert_distributed_to_local=True,
        password_env="RAY_CH_LOCAL_READ_TEST_PASSWORD",
        ray_remote_args={"max_retries": 0},
    )
    with pytest.raises(Exception, match="password environment variable is unavailable"):
        dataset.take_all()
    tables.clients[0].command("SYSTEM FLUSH LOGS")
    assert (
        tables.clients[0]
        .query(
            "SELECT count() FROM system.query_log WHERE "
            "startsWith(log_comment, 'ray-clickhouse operation=read ') "
            "AND position(query, %(table)s) > 0",
            parameters={"table": tables.distributed},
        )
        .first_row[0]
        == 0
    )


def test_bounded_path_comparison_records_actual_server_and_cost(
    cluster_tables, record_property
):
    tables = cluster_tables
    results = []
    for convert in (False, True):
        start = time.perf_counter()
        datasource = source(tables, convert_distributed_to_local=convert)
        tasks = datasource.get_read_tasks(4)
        planned = time.perf_counter()
        rows = [row for task in tasks for block in task() for row in block.to_pylist()]
        finished = time.perf_counter()
        record_property(
            f"{'local' if convert else 'logical'}_planning_seconds", planned - start
        )
        record_property(
            f"{'local' if convert else 'logical'}_read_seconds", finished - planned
        )
        results.append(multiset(rows))
    assert results[0] == results[1]
    record_property(
        "clickhouse_version", tables.clients[0].query("SELECT version()").first_row[0]
    )
    record_property("rows", sum(results[0].values()))
