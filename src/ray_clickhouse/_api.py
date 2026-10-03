"""Public Ray Dataset read and append-write facades."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import ray.data

from ray_clickhouse._compat import ensure_supported_ray_version
from ray_clickhouse._errors import ConfigurationError, WriteError
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
from ray_clickhouse._query import ClickHouseQueryDatasource, QueryReadConfig
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
    table: str | None = None,
    query: str | None = None,
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
    diagnostic_flush_logs: bool = False,
    concurrency: int | None = None,
    override_num_blocks: int | None = None,
    ray_remote_args: Mapping[str, Any] | None = None,
    num_cpus: float | None = None,
    num_gpus: float | None = None,
    memory: float | None = None,
) -> ray.data.Dataset:
    """Read a ClickHouse table or trusted query into a Ray Dataset.

    Query reads use one task with bounded Arrow streaming and automatic result
    schema discovery. Specify exactly one of table or query. Query mode accepts
    the documented SELECT/WITH subset and enforces readonly=1.

    Examples:
        Read a named table:
            read_clickhouse(host="localhost", database="analytics", table="events")

        Read an aggregate:
            read_clickhouse(
                host="localhost", database="analytics",
                query="SELECT tenant, count() AS rows FROM events GROUP BY tenant",
            )

    Args:
        host: ClickHouse hostname.
        database: Default database for the connection.
        table: Simple table name, mutually exclusive with query.
        query: Trusted SELECT or WITH/SELECT query with unique simple result aliases.
        port: ClickHouse HTTP port.
        username: ClickHouse account name.
        password: Literal password for a trusted Ray control plane.
        password_env: Environment variable resolved when each process connects.
        secure: Enable HTTPS.
        columns: Table column projection; unavailable in query mode.
        filter: Trusted table predicate; unavailable in query mode.
        order_by: Structured table ordering; unavailable in query mode.
        split: Table split mode. Query reads require single.
        range_column: Integer range column for explicit table splitting.
        discovery_policy: Table planning fallback policy.
        batch_rows: Maximum rows per emitted Arrow block.
        batch_bytes: Byte target per block; one indivisible row may exceed it.
        target_tasks: Desired table task count; does not split a query.
        max_tasks: Table task-count upper bound.
        connect_timeout_seconds: Client connection timeout.
        query_timeout_seconds: Managed query timeout.
        query_parameters: Snapshotted named bindings; query mode uses client syntax.
        settings: ClickHouse settings. Query mode requires readonly=1.
        client_options: Additional validated client options.
        diagnostic_flush_logs: Request log flushing for best-effort read diagnostics.
        concurrency: Ray read concurrency; does not multiply query tasks.
        override_num_blocks: Ray output-block count, independent of query count.
        ray_remote_args: Additional Ray task options.
        num_cpus: CPUs reserved for a read task.
        num_gpus: GPUs reserved for a read task.
        memory: Ray task heap-memory resource reservation.

    Returns:
        A Dataset containing the source rows.
    """
    ensure_supported_ray_version()
    if (table is None) == (query is None):
        raise ConfigurationError("specify exactly one of table or query")
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
    limits = ResourceLimits(
        batch_rows=batch_rows,
        batch_bytes=batch_bytes,
        target_tasks=target_tasks,
        max_tasks=max_tasks,
        connect_timeout_seconds=connect_timeout_seconds,
        query_timeout_seconds=query_timeout_seconds,
    )
    parameters = snapshot_mapping(query_parameters, name="query_parameters")
    datasource: ClickHouseDatasource | ClickHouseQueryDatasource
    if query is not None:
        if (
            columns is not None
            or filter is not None
            or order_by is not None
            or range_column is not None
            or split != "single"
        ):
            raise ConfigurationError(
                "query cannot use table projection, filter, ordering or splits"
            )
        datasource = ClickHouseQueryDatasource(
            QueryReadConfig(
                connection, query, parameters, limits, diagnostic_flush_logs
            )
        )
    else:
        assert table is not None
        config = ClickHouseReadConfig(
            connection=connection,
            table=QualifiedTable(database, table),
            columns=normalize_columns(tuple(columns) if columns is not None else None),
            filter_sql=validate_filter(filter),
            query_parameters=parameters,
            order_by=normalize_order_by(order_by),
            split=split,
            range_column=range_column,
            discovery_policy=discovery_policy,
            target_tasks=target_tasks,
            max_tasks=max_tasks,
            diagnostic_flush_logs=diagnostic_flush_logs,
            limits=limits,
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
    dataset: ray.data.Dataset = ray.data.read_datasource(datasource, **kwargs)
    return dataset


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
    receipt = sink.receipt
    if receipt is None:
        raise WriteError("Ray write completed without a ClickHouse write receipt")
    return receipt
