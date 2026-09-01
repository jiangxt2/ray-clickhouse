from __future__ import annotations

import os
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pytest
import ray.data

from ray_clickhouse import read_clickhouse, write_clickhouse
from ray_clickhouse._errors import (
    DiscoveryError,
    PermissionError,
    SchemaError,
    WriteError,
)

from .conftest import DATABASE


def _create_events_table(client: Any, table: str) -> None:
    client.command(
        f"""
        CREATE TABLE `{DATABASE}`.`{table}` (
            id UInt64,
            tenant_id UInt32,
            event_date Date,
            payload String,
            nullable_value Nullable(UInt64),
            amount Decimal(12, 2),
            event_time DateTime64(3, 'UTC')
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (tenant_id, id)
        """
    )


def _events_table(start: int, count: int, *, tenant_count: int = 3) -> pa.Table:
    ids = list(range(start, start + count))
    event_dates = [date(2026, 1 + ((value - start) % 3), 1) for value in ids]
    return pa.table(
        {
            "id": pa.array(ids, type=pa.uint64()),
            "tenant_id": pa.array(
                [value % tenant_count for value in ids], type=pa.uint32()
            ),
            "event_date": pa.array(event_dates, type=pa.date32()),
            "payload": pa.array([f"payload-{value}" for value in ids]),
            "nullable_value": pa.array(
                [None if value % 11 == 0 else value for value in ids],
                type=pa.uint64(),
            ),
            "amount": pa.array(
                [Decimal(value).scaleb(-2) for value in ids],
                type=pa.decimal128(12, 2),
            ),
            "event_time": pa.array(
                [
                    datetime(2026, 1, 1, tzinfo=timezone.utc).replace(minute=value % 60)
                    for value in ids
                ],
                type=pa.timestamp("ms", tz="UTC"),
            ),
        }
    )


def _insert(client: Any, table: str, values: pa.Table) -> None:
    client.insert_arrow(table, values, database=DATABASE)


def _create_view(client: Any, view: str, source: str) -> None:
    client.command(
        f"CREATE VIEW `{DATABASE}`.`{view}` AS "
        f"SELECT id, tenant_id, payload FROM `{DATABASE}`.`{source}`"
    )


def _create_aggregate_view(client: Any, view: str, source: str) -> None:
    client.command(
        f"CREATE VIEW `{DATABASE}`.`{view}` AS "
        f"SELECT tenant_id, count() AS row_count FROM `{DATABASE}`.`{source}` "
        "GROUP BY tenant_id"
    )


def _create_materialized_view(client: Any, view: str, source: str) -> None:
    client.command(
        f"CREATE MATERIALIZED VIEW `{DATABASE}`.`{view}` "
        "ENGINE = MergeTree ORDER BY id AS "
        f"SELECT id, tenant_id, payload FROM `{DATABASE}`.`{source}`"
    )


def _create_distributed_table(client: Any, table: str, source: str) -> bool:
    clusters = client.query(
        "SELECT cluster FROM system.clusters WHERE shard_num = 1 LIMIT 1"
    ).result_rows
    if not clusters:
        return False
    cluster = str(clusters[0][0]).replace("'", "''")
    client.command(
        f"CREATE TABLE `{DATABASE}`.`{table}` AS `{DATABASE}`.`{source}` "
        f"ENGINE = Distributed('{cluster}', '{DATABASE}', '{source}')"
    )
    return True


def _read_rows(**kwargs: Any) -> list[dict[str, Any]]:
    return read_clickhouse(
        host=kwargs.pop("host"),
        port=kwargs.pop("port"),
        username=kwargs.pop("username", "default"),
        password=kwargs.pop("password", ""),
        database=kwargs.pop("database", DATABASE),
        **kwargs,
    ).take_all()


@pytest.mark.integration
@pytest.mark.ray
def test_single_read_streams_arrow_blocks_with_filter(
    clickhouse_client: Any, table_name: str, connection_options: dict[str, object]
) -> None:
    _create_events_table(clickhouse_client, table_name)
    values = _events_table(0, 240)
    _insert(clickhouse_client, table_name, values)

    rows = _read_rows(
        **connection_options,
        table=table_name,
        columns=("id", "tenant_id", "payload"),
        filter="tenant_id = %(tenant)s AND id >= %(minimum)s",
        query_parameters={"tenant": 1, "minimum": 30},
        order_by=(["id"], True),
        batch_rows=17,
        batch_bytes=8 * 1024,
        split="single",
    )

    expected = [
        {"id": value, "tenant_id": value % 3, "payload": f"payload-{value}"}
        for value in range(30, 240)
        if value % 3 == 1
    ]
    assert rows == sorted(expected, key=lambda row: row["id"], reverse=True)


@pytest.mark.integration
@pytest.mark.ray
def test_read_type_matrix_returns_logical_arrow_values(
    clickhouse_client: Any, table_name: str, connection_options: dict[str, object]
) -> None:
    _create_events_table(clickhouse_client, table_name)
    _insert(clickhouse_client, table_name, _events_table(1, 1))

    rows = _read_rows(
        **connection_options,
        table=table_name,
        columns=("event_date", "event_time", "amount", "nullable_value"),
        batch_rows=8,
    )

    assert len(rows) == 1
    assert rows[0]["event_date"] == date(2026, 1, 1)
    assert rows[0]["event_time"].isoformat() == "2026-01-01T00:01:00+00:00"
    assert rows[0]["amount"] == Decimal("0.01")
    assert rows[0]["nullable_value"] == 1


@pytest.mark.integration
@pytest.mark.ray
def test_empty_table_returns_no_rows(
    clickhouse_client: Any, table_name: str, connection_options: dict[str, object]
) -> None:
    _create_events_table(clickhouse_client, table_name)

    rows = _read_rows(
        **connection_options,
        table=table_name,
        columns=("id", "event_date"),
        batch_rows=8,
    )

    assert rows == []


@pytest.mark.integration
@pytest.mark.ray
def test_view_and_aggregate_view_are_readable_only_as_single_queries(
    clickhouse_client: Any, table_name: str, connection_options: dict[str, object]
) -> None:
    _create_events_table(clickhouse_client, table_name)
    _insert(clickhouse_client, table_name, _events_table(0, 6))
    view = f"{table_name}_view"
    aggregate_view = f"{table_name}_aggregate"
    _create_view(clickhouse_client, view, table_name)
    _create_aggregate_view(clickhouse_client, aggregate_view, table_name)
    try:
        rows = _read_rows(
            **connection_options,
            table=view,
            columns=("id", "tenant_id", "payload"),
            split="single",
        )
        assert len(rows) == 6
        with pytest.raises(DiscoveryError, match="unsupported for split='partition'"):
            _read_rows(
                **connection_options,
                table=view,
                columns=("id",),
                split="partition",
            )

        aggregate_rows = _read_rows(
            **connection_options,
            table=aggregate_view,
            columns=("tenant_id", "row_count"),
            split="single",
        )
        assert sorted(aggregate_rows, key=lambda row: row["tenant_id"]) == [
            {"tenant_id": 0, "row_count": 2},
            {"tenant_id": 1, "row_count": 2},
            {"tenant_id": 2, "row_count": 2},
        ]
    finally:
        clickhouse_client.command(f"DROP VIEW IF EXISTS `{DATABASE}`.`{view}`")
        clickhouse_client.command(
            f"DROP VIEW IF EXISTS `{DATABASE}`.`{aggregate_view}`"
        )


@pytest.mark.integration
@pytest.mark.ray
def test_materialized_view_is_outside_the_read_capability_profile(
    clickhouse_client: Any, table_name: str, connection_options: dict[str, object]
) -> None:
    _create_events_table(clickhouse_client, table_name)
    materialized_view = f"{table_name}_materialized"
    _create_materialized_view(clickhouse_client, materialized_view, table_name)
    try:
        with pytest.raises(DiscoveryError, match="MaterializedView.*unsupported"):
            _read_rows(
                **connection_options,
                table=materialized_view,
                columns=("id",),
                split="single",
            )
    finally:
        clickhouse_client.command(
            f"DROP TABLE IF EXISTS `{DATABASE}`.`{materialized_view}`"
        )


@pytest.mark.integration
@pytest.mark.ray
def test_distributed_table_is_single_query_only(
    clickhouse_client: Any, table_name: str, connection_options: dict[str, object]
) -> None:
    _create_events_table(clickhouse_client, table_name)
    _insert(clickhouse_client, table_name, _events_table(0, 6))
    distributed = f"{table_name}_distributed"
    if not _create_distributed_table(clickhouse_client, distributed, table_name):
        pytest.skip("ClickHouse test cluster configuration is unavailable")
    try:
        rows = _read_rows(
            **connection_options,
            table=distributed,
            columns=("id", "tenant_id"),
            split="single",
        )
        assert len(rows) == 6
        with pytest.raises(DiscoveryError, match="unsupported for split='partition'"):
            _read_rows(
                **connection_options,
                table=distributed,
                columns=("id",),
                split="partition",
            )
    finally:
        clickhouse_client.command(f"DROP TABLE IF EXISTS `{DATABASE}`.`{distributed}`")


@pytest.mark.integration
@pytest.mark.ray
@pytest.mark.cluster
def test_multi_node_ray_cluster_is_available() -> None:
    address = os.environ.get("RAY_CLICKHOUSE_IT_RAY_ADDRESS")
    if not address:
        pytest.skip("multi-node Ray address is not configured")
    import ray

    alive_nodes = [node for node in ray.nodes() if node["Alive"]]
    assert len(alive_nodes) >= 3
    assert sum(node["Resources"].get("CPU", 0) for node in alive_nodes) >= 2


@pytest.mark.integration
@pytest.mark.ray
def test_partition_and_range_splits_match_single_read(
    clickhouse_client: Any, table_name: str, connection_options: dict[str, object]
) -> None:
    _create_events_table(clickhouse_client, table_name)
    _insert(clickhouse_client, table_name, _events_table(0, 300))
    common = {
        **connection_options,
        "table": table_name,
        "columns": ("id", "tenant_id"),
        "filter": "tenant_id = %(tenant)s",
        "query_parameters": {"tenant": 2},
        "batch_rows": 23,
    }

    single = _read_rows(**common, split="single")
    partitioned = _read_rows(**common, split="partition", target_tasks=3, max_tasks=3)
    ranged = _read_rows(
        **common,
        split="range",
        range_column="id",
        target_tasks=4,
        max_tasks=4,
    )

    assert sorted(partitioned, key=lambda row: row["id"]) == sorted(
        single, key=lambda row: row["id"]
    )
    assert sorted(ranged, key=lambda row: row["id"]) == sorted(
        single, key=lambda row: row["id"]
    )


@pytest.mark.integration
@pytest.mark.ray
@pytest.mark.parametrize("insert_mode", ["sync", "async"])
def test_append_write_is_schema_checked_and_confirmed(
    clickhouse_client: Any,
    table_name: str,
    connection_options: dict[str, object],
    insert_mode: str,
) -> None:
    _create_events_table(clickhouse_client, table_name)
    values = _events_table(10, 8)
    dataset = ray.data.from_arrow(values)

    receipt = write_clickhouse(
        dataset,
        **connection_options,
        table=table_name,
        insert_mode=insert_mode,
        batch_rows=3,
        batch_bytes=8 * 1024,
    )

    result = clickhouse_client.query(
        f"SELECT count(), min(id), max(id) FROM `{DATABASE}`.`{table_name}`"
    ).result_rows[0]
    assert result == (8, 10, 17)
    assert receipt.rows_written == 8
    assert receipt.batches_written >= 3
    assert receipt.status == "confirmed"


@pytest.mark.integration
@pytest.mark.ray
def test_explicit_create_and_overwrite_modes(
    clickhouse_client: Any,
    table_name: str,
    connection_options: dict[str, object],
) -> None:
    values = _events_table(10, 3)

    create_receipt = write_clickhouse(
        ray.data.from_arrow(values),
        **connection_options,
        table=table_name,
        write_mode="create",
        order_by=("id",),
        nullable_columns=("nullable_value",),
    )
    assert create_receipt.rows_written == 3
    assert (
        clickhouse_client.query(
            f"SELECT count() FROM `{DATABASE}`.`{table_name}`"
        ).result_rows[0][0]
        == 3
    )

    overwrite_receipt = write_clickhouse(
        ray.data.from_arrow(values.slice(0, 1)),
        **connection_options,
        table=table_name,
        write_mode="overwrite",
        order_by=("id",),
        nullable_columns=("nullable_value",),
    )
    assert overwrite_receipt.rows_written == 1
    assert (
        clickhouse_client.query(
            f"SELECT count() FROM `{DATABASE}`.`{table_name}`"
        ).result_rows[0][0]
        == 1
    )


@pytest.mark.integration
@pytest.mark.ray
def test_write_rejects_arrow_type_mismatch(
    clickhouse_client: Any, table_name: str, connection_options: dict[str, object]
) -> None:
    _create_events_table(clickhouse_client, table_name)
    invalid = pa.table(
        {
            "id": pa.array([1], type=pa.int64()),
            "tenant_id": pa.array([1], type=pa.uint32()),
            "event_date": pa.array([date(2026, 1, 1)], type=pa.date32()),
            "payload": pa.array(["invalid"]),
            "nullable_value": pa.array([1], type=pa.uint64()),
            "amount": pa.array([Decimal("1.00")], type=pa.decimal128(12, 2)),
            "event_time": pa.array(
                [datetime(2026, 1, 1, tzinfo=timezone.utc)],
                type=pa.timestamp("ms", tz="UTC"),
            ),
        }
    )

    with pytest.raises((SchemaError, WriteError)):
        write_clickhouse(
            ray.data.from_arrow(invalid),
            **connection_options,
            table=table_name,
        )


@pytest.mark.integration
def test_read_permission_failure_is_not_silently_downgraded(
    clickhouse_client: Any, table_name: str, connection_options: dict[str, object]
) -> None:
    _create_events_table(clickhouse_client, table_name)
    _insert(clickhouse_client, table_name, _events_table(0, 4))
    username = f"ray_clickhouse_it_no_select_{table_name.rsplit('_', 1)[-1]}"
    clickhouse_client.command(f"CREATE USER `{username}` IDENTIFIED WITH no_password")
    try:
        no_select_options = {**connection_options, "username": username}
        with pytest.raises(PermissionError):
            _read_rows(
                **no_select_options,
                table=table_name,
                columns=("id",),
            )
    finally:
        clickhouse_client.command(f"DROP USER IF EXISTS `{username}`")
