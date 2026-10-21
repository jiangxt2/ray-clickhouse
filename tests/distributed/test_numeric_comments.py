"""Numeric comments cannot become positions in real metadata discovery."""

from collections import Counter

import pyarrow as pa
import pytest
import ray

from ray_clickhouse import read_clickhouse, write_clickhouse
from ray_clickhouse._discovery import discover_schema, discover_target
from ray_clickhouse._models import ClickHouseConnection, QualifiedTable, ResourceLimits
from ray_clickhouse.datasource import ClickHouseDatasource, ClickHouseReadConfig

pytestmark = [pytest.mark.integration, pytest.mark.cluster, pytest.mark.distributed]
DATABASE = "ray_clickhouse_it"


@pytest.mark.parametrize(
    "multiple", [False, True], ids=["single-column", "multiple-columns"]
)
def test_numeric_comments_preserve_metadata_reads_and_writes(cluster_tables, multiple):
    clients = cluster_tables.clients
    local = f"comment_{cluster_tables.local}"
    logical = f"comment_{cluster_tables.distributed}"
    declaration = "id Int64 COMMENT '2026'"
    if multiple:
        declaration += ", payload String COMMENT '0'"
    names = ["id", "payload"] if multiple else ["id"]
    expected = []
    try:
        for index, client in enumerate(clients):
            client.command(
                f"CREATE TABLE {DATABASE}.{local} ({declaration}) ENGINE = "
                "ReplicatedMergeTree("
                f"'/ray_clickhouse_it/{local}/shard{index // 2 + 1}', "
                f"'replica{index % 2 + 1}') ORDER BY id"
            )
        for index, values in ((0, [1, 1, 2]), (2, [3, 4])):
            rows = [(n, f"row-{n}") if multiple else (n,) for n in values]
            clients[index].insert(f"{DATABASE}.{local}", rows, column_names=names)
            expected.extend(dict(zip(names, row, strict=True)) for row in rows)
        clients[0].command(
            f"CREATE TABLE {DATABASE}.{logical} AS {DATABASE}.{local} "
            f"ENGINE = Distributed(local_scan, '{DATABASE}', '{local}', rand())"
        )
        described = clients[0].query(f"DESCRIBE TABLE {DATABASE}.{logical}")
        comment_index = described.column_names.index("comment")
        assert described.result_rows[0][comment_index] == "2026"
        if multiple:
            assert described.result_rows[1][comment_index] == "0"

        connection = ClickHouseConnection("clickhouse", DATABASE)
        table = QualifiedTable(DATABASE, logical)
        schema = discover_schema(connection, table, ResourceLimits())
        assert [(c.name, c.position) for c in schema.columns] == list(
            zip(names, range(1, len(names) + 1), strict=True)
        )
        target = discover_target(
            connection, QualifiedTable(DATABASE, local), ResourceLimits()
        )
        assert [(c.name, c.position) for c in target.columns] == [
            (c.name, c.position) for c in schema.columns
        ]

        added = {"id": [5], **({"payload": ["row-5"]} if multiple else {})}
        receipt = write_clickhouse(
            ray.data.from_arrow(pa.table(added)),
            host="clickhouse",
            database=DATABASE,
            table=local,
        )
        assert receipt.rows_written == 1 and receipt.status == "confirmed"
        expected.append({"id": 5, **({"payload": "row-5"} if multiple else {})})
        for client in clients:
            client.command(f"SYSTEM SYNC REPLICA {DATABASE}.{local}")

        def multiset(rows):
            return Counter(tuple(sorted(row.items())) for row in rows)

        for convert in (False, True):
            datasource = ClickHouseDatasource(
                ClickHouseReadConfig(
                    connection,
                    table,
                    filter_sql="id >= 0",
                    convert_distributed_to_local=convert,
                )
            )
            tasks = datasource.get_read_tasks(2)
            assert len(tasks) == (2 if convert else 1)
            assert bool(datasource._local_sources) is convert
            rows = ray.data.read_datasource(
                datasource, override_num_blocks=2, ray_remote_args={"max_retries": 0}
            ).take_all()
            assert multiset(rows) == multiset(expected)

        rows = read_clickhouse(
            host="clickhouse",
            database=DATABASE,
            query=f"SELECT * FROM {DATABASE}.{logical} WHERE id >= 0",
        ).take_all()
        assert multiset(rows) == multiset(expected)
    finally:
        for client in clients:
            client.command(f"DROP TABLE IF EXISTS {DATABASE}.{logical} SYNC")
            client.command(f"DROP TABLE IF EXISTS {DATABASE}.{local} SYNC")
