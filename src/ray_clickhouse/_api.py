"""Public Ray Dataset read and append-write facades."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import ray.data

from ray_clickhouse._compat import ensure_supported_ray_version
from ray_clickhouse._models import (
    ClickHouseConnection,
    DiscoveryPolicy,
    InsertMode,
    QualifiedTable,
    ResourceLimits,
    SplitMode,
    WriteMode,
    WriteReceipt,
    snapshot_mapping,
)
from ray_clickhouse._sql import normalize_columns, normalize_order_by, validate_filter
from ray_clickhouse.datasink import ClickHouseDataSink, validate_write_remote_args
from ray_clickhouse.datasource import ClickHouseDatasource, ClickHouseReadConfig


def _connection(
    *,
    host: str,
    database: str,
    username: str,
    password: str,
    password_env: str | None,
    port: int,
    secure: bool,
    settings: Mapping[str, Any] | None,
    client_options: Mapping[str, Any] | None,
) -> ClickHouseConnection:
    return ClickHouseConnection.from_options(
        host=host,
        database=database,
        username=username,
        password=password,
        password_env=password_env,
        port=port,
        secure=secure,
        settings=settings,
        client_options=client_options,
    )


def read_clickhouse(
    *,
    host: str,
    database: str,
    table: str,
    port: int = 8123,
    username: str = "default",
    password: str = "",
    password_env: str | None = None,
    secure: bool = False,
    columns: Sequence[str] | None = None,
    filter: str | None = None,
    order_by: tuple[Sequence[str], bool] | None = None,
    split: SplitMode = "single",
    range_column: str | None = None,
    discovery_policy: DiscoveryPolicy = "single",
    batch_rows: int = 65_536,
    batch_bytes: int = 64 * 1024 * 1024,
    target_tasks: int = 8,
    max_tasks: int = 256,
    connect_timeout_seconds: float = 10.0,
    query_timeout_seconds: float = 300.0,
    query_parameters: Mapping[str, Any] | None = None,
    settings: Mapping[str, Any] | None = None,
    client_options: Mapping[str, Any] | None = None,
    concurrency: int | None = None,
    override_num_blocks: int | None = None,
    ray_remote_args: Mapping[str, Any] | None = None,
    num_cpus: float | None = None,
    num_gpus: float | None = None,
    memory: float | None = None,
) -> ray.data.Dataset:
    """Read a structured ClickHouse physical table into a Ray Dataset."""
    ensure_supported_ray_version()
    config = ClickHouseReadConfig(
        connection=_connection(
            host=host,
            database=database,
            username=username,
            password=password,
            password_env=password_env,
            port=port,
            secure=secure,
            settings=settings,
            client_options=client_options,
        ),
        table=QualifiedTable(database, table),
        columns=normalize_columns(tuple(columns) if columns is not None else None),
        filter_sql=validate_filter(filter),
        query_parameters=snapshot_mapping(query_parameters, name="query_parameters"),
        order_by=normalize_order_by(order_by),
        split=split,
        range_column=range_column,
        discovery_policy=discovery_policy,
        target_tasks=target_tasks,
        max_tasks=max_tasks,
        limits=ResourceLimits(
            batch_rows=batch_rows,
            batch_bytes=batch_bytes,
            target_tasks=target_tasks,
            max_tasks=max_tasks,
            connect_timeout_seconds=connect_timeout_seconds,
            query_timeout_seconds=query_timeout_seconds,
        ),
    )
    datasource = ClickHouseDatasource(config)
    kwargs: dict[str, Any] = {}
    if concurrency is not None:
        kwargs["concurrency"] = concurrency
    if override_num_blocks is not None:
        kwargs["override_num_blocks"] = override_num_blocks
    if ray_remote_args is not None:
        kwargs["ray_remote_args"] = dict(ray_remote_args)
    if num_cpus is not None:
        kwargs["num_cpus"] = num_cpus
    if num_gpus is not None:
        kwargs["num_gpus"] = num_gpus
    if memory is not None:
        kwargs["memory"] = memory
    return ray.data.read_datasource(datasource, **kwargs)


def write_clickhouse(
    dataset: ray.data.Dataset,
    *,
    host: str,
    database: str,
    table: str,
    port: int = 8123,
    username: str = "default",
    password: str = "",
    password_env: str | None = None,
    secure: bool = False,
    insert_mode: InsertMode = "sync",
    write_mode: WriteMode = "append",
    engine: str = "MergeTree",
    order_by: Sequence[str] | None = None,
    nullable_columns: Sequence[str] | None = None,
    columns: Sequence[str] | None = None,
    batch_rows: int = 50_000,
    batch_bytes: int = 64 * 1024 * 1024,
    connect_timeout_seconds: float = 10.0,
    query_timeout_seconds: float = 300.0,
    settings: Mapping[str, Any] | None = None,
    client_options: Mapping[str, Any] | None = None,
    ray_remote_args: Mapping[str, Any] | None = None,
    concurrency: int | None = None,
) -> WriteReceipt:
    """Write a Ray Dataset using append or explicit table management mode."""
    ensure_supported_ray_version()
    connection = _connection(
        host=host,
        database=database,
        username=username,
        password=password,
        password_env=password_env,
        port=port,
        secure=secure,
        settings=settings,
        client_options=client_options,
    )
    sink = ClickHouseDataSink(
        connection=connection,
        table=QualifiedTable(database, table),
        insert_mode=insert_mode,
        write_mode=write_mode,
        engine=engine,
        order_by=normalize_columns(order_by),
        nullable_columns=normalize_columns(nullable_columns),
        columns=normalize_columns(tuple(columns) if columns is not None else None),
        limits=ResourceLimits(
            batch_rows=batch_rows,
            batch_bytes=batch_bytes,
            target_tasks=1,
            max_tasks=1,
            connect_timeout_seconds=connect_timeout_seconds,
            query_timeout_seconds=query_timeout_seconds,
        ),
    )
    kwargs: dict[str, Any] = {
        "ray_remote_args": validate_write_remote_args(ray_remote_args),
    }
    if concurrency is not None:
        kwargs["concurrency"] = concurrency
    dataset.write_datasink(sink, **kwargs)
    return sink.receipt or WriteReceipt(0, 0, 0)
