"""Native expressions must reach every fixed #20 shard without losing residuals."""

from unittest.mock import patch

import pytest
from distributed.test_local_read import multiset
from ray.data.expressions import col

from ray_clickhouse import read_clickhouse
from ray_clickhouse._distributed import MetadataReader, NotApplicable

pytestmark = [pytest.mark.integration, pytest.mark.cluster, pytest.mark.distributed]


@pytest.mark.parametrize("split", ["single", "partition", "range", "auto"])
@pytest.mark.parametrize("native", [False, True])
def test_ray_expression_is_composed_on_every_shard(cluster_tables, split, native):
    tables = cluster_tables
    options = {"range_column": "value"} if split == "range" else {}
    expr = (col("value") >= 9) | col("value").is_null()
    base = read_clickhouse(
        host="clickhouse",
        database="ray_clickhouse_it",
        table=tables.distributed,
        filter="tenant >= {tenant:UInt8}" if native else "tenant >= %(tenant)s",
        query_parameters={"tenant": 1},
        convert_distributed_to_local=True,
        split=split,
        target_tasks=4,
        batch_rows=3,
        diagnostic_flush_logs=True,
        **options,
    )
    rows = base.filter(expr=expr).take_all()
    expected = [
        row
        for row in tables.rows
        if row["tenant"] >= 1 and (row["value"] is None or row["value"] >= 9)
    ]
    assert multiset(rows) == multiset(expected)
    for index in (0, 2):
        client = tables.clients[index]
        client.command("SYSTEM FLUSH LOGS")
        queries = client.query(
            "SELECT query FROM system.query_log WHERE type = 'QueryFinish' "
            "AND is_initial_query = 1 AND startsWith(log_comment, "
            "'ray-clickhouse operation=read ') "
            "AND position(query, %(table)s) > 0",
            parameters={"table": tables.local},
        ).result_rows
        assert queries
        assert all(
            "value IS NULL" in query[0].replace("`", "")
            and "value >=" in query[0].replace("`", "")
            for query in queries
        )


@pytest.mark.parametrize("convert", [False, True])
def test_expression_only_filter_replans_a_previously_unfiltered_source(
    cluster_tables, convert
):
    tables = cluster_tables
    base = read_clickhouse(
        host="clickhouse",
        database="ray_clickhouse_it",
        table=tables.distributed,
        convert_distributed_to_local=convert,
        target_tasks=1,
        batch_rows=2,
        diagnostic_flush_logs=True,
    )
    assert multiset(base.filter(expr=col("id") >= 28).take_all()) == multiset(
        row for row in tables.rows if row["id"] >= 28
    )
    tables.clients[0].command("SYSTEM FLUSH LOGS")
    queries = (
        tables.clients[0]
        .query(
            "SELECT query FROM system.query_log WHERE type = 'QueryFinish' "
            "AND is_initial_query = 1 AND startsWith(log_comment, "
            "'ray-clickhouse operation=read ') "
            "AND (position(query, %(local)s) > 0 OR "
            "position(query, %(distributed)s) > 0)",
            parameters={"local": tables.local, "distributed": tables.distributed},
        )
        .result_rows
    )
    target = tables.local if convert else tables.distributed
    assert queries and any(target in row[0] for row in queries)
    assert all("id >=" in row[0].replace("`", "") for row in queries)
    expr = (col("id") >= 8) & col("id").is_in([9, 10, 29, 30])
    assert multiset(base.filter(expr=expr).take_all()) == multiset(
        row for row in tables.rows if row["id"] in {9, 10, 29, 30}
    )
    assert len(base.filter(expr=expr).limit(2).take_all()) == 2
    assert base.count() == len(tables.rows)


def test_conversion_fallback_preserves_accepted_filter(cluster_tables):
    tables = cluster_tables
    original = MetadataReader.context

    def missing(self, table):
        if self.connection.host == "ch3":
            raise NotApplicable("test capability absence")
        return original(self, table)

    with patch.object(MetadataReader, "context", missing):
        base = read_clickhouse(
            host="clickhouse",
            database="ray_clickhouse_it",
            table=tables.distributed,
            convert_distributed_to_local=True,
            diagnostic_flush_logs=True,
        )
        actual = base.filter(expr=col("id") >= 28).take_all()
    assert multiset(actual) == multiset(row for row in tables.rows if row["id"] >= 28)
    tables.clients[0].command("SYSTEM FLUSH LOGS")
    queries = (
        tables.clients[0]
        .query(
            "SELECT query FROM system.query_log WHERE type = 'QueryFinish' "
            "AND startsWith(log_comment, "
            "'ray-clickhouse operation=read ') AND position(query, %(table)s) > 0",
            parameters={"table": tables.distributed},
        )
        .result_rows
    )
    assert queries and any("id >=" in query[0].replace("`", "") for query in queries)
