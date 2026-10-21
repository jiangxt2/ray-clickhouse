from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import clickhouse_connect
import pytest
from integration.conftest import ray_runtime as ray_runtime

DATABASE = "ray_clickhouse_it"
HOSTS = ("clickhouse", "ch2", "ch3", "ch4")


@dataclass
class ClusterTables:
    clients: tuple[Any, ...]
    local: str
    distributed: str
    rows: list[dict[str, Any]]


@pytest.fixture
def cluster_tables() -> Iterator[ClusterTables]:
    suffix = uuid.uuid4().hex[:12]
    local, distributed = f"local_{suffix}", f"dist_{suffix}"
    clients = tuple(
        clickhouse_connect.get_client(host=host, database="default", query_retries=0)
        for host in HOSTS
    )
    rows = []
    try:
        for i, client in enumerate(clients):
            client.command(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
            shard, replica = i // 2 + 1, i % 2 + 1
            deadline = time.monotonic() + 60
            while True:
                try:
                    client.command(
                        f"CREATE TABLE {DATABASE}.{local} (id Int64, value "
                        f"Nullable(Int64), tenant UInt8, txt String) ENGINE = "
                        f"ReplicatedMergeTree('/ray_clickhouse_it/{suffix}/"
                        f"shard{shard}', "
                        f"'replica{replica}') PARTITION BY tenant ORDER BY id"
                    )
                    break
                except Exception:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(1)
        for i in (0, 2):
            values = [
                (n, None if n % 3 == 0 else n, n % 3, f"row-{n}")
                for n in range(i * 10, i * 10 + 12)
            ]
            values += values[:3]
            clients[i].insert(
                f"{DATABASE}.{local}",
                values,
                column_names=["id", "value", "tenant", "txt"],
            )
            rows += [
                dict(zip(("id", "value", "tenant", "txt"), row, strict=True))
                for row in values
            ]
        for client in clients:
            client.command(f"SYSTEM SYNC REPLICA {DATABASE}.{local}")
        clients[0].command(
            f"CREATE TABLE {DATABASE}.{distributed} AS {DATABASE}.{local} "
            f"ENGINE = Distributed(local_scan, '{DATABASE}', '{local}', "
            f"rand())"
        )
        assert clients[0].query(
            f"SELECT count() FROM {DATABASE}.{distributed}"
        ).first_row[0] == len(rows)
        yield ClusterTables(clients, local, distributed, rows)
    finally:
        for client in clients:
            try:
                client.command(f"DROP TABLE IF EXISTS {DATABASE}.{distributed} SYNC")
                client.command(f"DROP TABLE IF EXISTS {DATABASE}.{local} SYNC")
            finally:
                client.close()
