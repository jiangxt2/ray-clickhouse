"""ClickHouse driver-side schema, engine, partition and range discovery."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from ray_clickhouse._errors import (
    AuthenticationError,
    DiscoveryError,
    ObjectNotFoundError,
    PermissionError,
    RayClickHouseError,
    SchemaError,
    TransportError,
)
from ray_clickhouse._models import ClickHouseConnection, QualifiedTable, ResourceLimits
from ray_clickhouse._schema import (
    SchemaPlan,
    TargetColumn,
    TargetTable,
    canonical_schema,
    parse_describe_rows,
    parse_type,
    render_read_projection,
)
from ray_clickhouse._sql import build_select, validate_filter
from ray_clickhouse._transport import _new_query_id

LOCAL_MERGETREE_ENGINES = frozenset(
    {
        "MergeTree",
        "ReplicatedMergeTree",
        "ReplacingMergeTree",
        "ReplicatedReplacingMergeTree",
        "SummingMergeTree",
        "ReplicatedSummingMergeTree",
        "AggregatingMergeTree",
        "ReplicatedAggregatingMergeTree",
        "CollapsingMergeTree",
        "ReplicatedCollapsingMergeTree",
        "VersionedCollapsingMergeTree",
        "ReplicatedVersionedCollapsingMergeTree",
        "GraphiteMergeTree",
        "ReplicatedGraphiteMergeTree",
    }
)
WRITE_ENGINES = frozenset(
    {
        "MergeTree",
        "ReplicatedMergeTree",
        "ReplacingMergeTree",
        "ReplicatedReplacingMergeTree",
        "SummingMergeTree",
        "ReplicatedSummingMergeTree",
        "AggregatingMergeTree",
        "ReplicatedAggregatingMergeTree",
        "CollapsingMergeTree",
        "ReplicatedCollapsingMergeTree",
        "VersionedCollapsingMergeTree",
        "ReplicatedVersionedCollapsingMergeTree",
        "GraphiteMergeTree",
        "ReplicatedGraphiteMergeTree",
    }
)


@dataclass(frozen=True)
class PartitionInfo:
    partition_id: str
    rows: int
    marks: int
    bytes_on_disk: int


@dataclass(frozen=True)
class RangeFacts:
    column: str
    declared_type: str
    total_rows: int
    null_rows: int
    minimum: int | None
    maximum: int | None


@dataclass(frozen=True)
class DiscoverySnapshot:
    schema: SchemaPlan
    engine: str
    partitions: tuple[PartitionInfo, ...] = ()
    range_facts: RangeFacts | None = None


def _driver() -> Any:
    try:
        import clickhouse_connect
    except ImportError:
        raise TransportError(
            "ClickHouse support is not installed; install "
            '"ray-clickhouse" with its dependencies'
        ) from None
    return clickhouse_connect


def _translate(exc: BaseException, *, operation: str) -> RayClickHouseError:
    code = getattr(exc, "code", None)
    message = str(exc).lower()
    if code in {516, 193} or "authentication" in message:
        return AuthenticationError(
            f"ClickHouse authentication failed during {operation}"
        )
    if (
        code in {497, 551}
        or "not enough privileges" in message
        or "access denied" in message
    ):
        return PermissionError(f"ClickHouse permission check failed during {operation}")
    if code in {60, 81} or "unknown table" in message or "doesn't exist" in message:
        return ObjectNotFoundError(
            f"ClickHouse object was not found during {operation}"
        )
    if "timeout" in message or "connection" in message or "http" in message:
        return TransportError(f"ClickHouse transport failed during {operation}")
    if operation == "schema discovery":
        return SchemaError("ClickHouse schema discovery failed")
    return DiscoveryError(f"ClickHouse {operation} failed")


def _open(connection: ClickHouseConnection, limits: ResourceLimits) -> Any:
    try:
        return _driver().get_client(**connection.client_kwargs(limits))
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit, GeneratorExit)):
            raise
        raise _translate(exc, operation="client initialization") from None


def _query_context(
    connection: ClickHouseConnection,
    limits: ResourceLimits,
    operation: str,
) -> tuple[dict[str, Any], dict[str, str]]:
    settings = connection.query_settings(limits)
    settings.setdefault("log_comment", f"ray-clickhouse operation={operation}")
    return settings, {"query_id": _new_query_id(operation)}


def discover_schema(
    connection: ClickHouseConnection, table: QualifiedTable, limits: ResourceLimits
) -> SchemaPlan:
    client = None
    try:
        client = _open(connection, limits)
        settings, transport_settings = _query_context(
            connection, limits, "schema-describe"
        )
        describe = client.query(
            f"DESCRIBE TABLE {table.sql()}",
            settings=settings,
            transport_settings=transport_settings,
        )
        columns = parse_describe_rows(describe.result_rows)
        projection = render_read_projection(columns)
        settings, transport_settings = _query_context(
            connection, limits, "schema-probe"
        )
        arrow_table = client.query_arrow(
            f"SELECT {projection} FROM {table.sql()} LIMIT 0",
            settings=settings,
            use_strings=True,
            transport_settings=transport_settings,
        )
        if not isinstance(arrow_table, pa.Table):
            raise SchemaError("ClickHouse returned a non-Arrow schema probe")
        return canonical_schema(columns, arrow_table.schema)
    except RayClickHouseError:
        raise
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit, GeneratorExit)):
            raise
        raise _translate(exc, operation="schema discovery") from None
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass


def discover_engine(
    connection: ClickHouseConnection, table: QualifiedTable, limits: ResourceLimits
) -> str:
    client = None
    try:
        client = _open(connection, limits)
        settings, transport_settings = _query_context(
            connection, limits, "engine-discovery"
        )
        result = client.query(
            "SELECT engine FROM system.tables "
            "WHERE database = %(database)s AND name = %(table)s",
            parameters={"database": table.database, "table": table.table},
            settings=settings,
            transport_settings=transport_settings,
        )
        if len(result.result_rows) != 1 or not isinstance(
            result.result_rows[0][0], str
        ):
            raise ObjectNotFoundError(f"ClickHouse table {table} was not found")
        return result.result_rows[0][0]
    except RayClickHouseError:
        raise
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit, GeneratorExit)):
            raise
        raise _translate(exc, operation="engine discovery") from None
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass


def discover_partitions(
    connection: ClickHouseConnection, table: QualifiedTable, limits: ResourceLimits
) -> tuple[PartitionInfo, ...]:
    client = None
    try:
        client = _open(connection, limits)
        engine = discover_engine(connection, table, limits)
        if engine not in LOCAL_MERGETREE_ENGINES:
            raise DiscoveryError(
                f"engine {engine!r} is not eligible for partition splitting"
            )
        settings, transport_settings = _query_context(
            connection, limits, "partition-column-discovery"
        )
        physical = client.query(
            "SELECT name FROM system.columns "
            "WHERE database = %(database)s AND table = %(table)s "
            "AND name = '_partition_id'",
            parameters={"database": table.database, "table": table.table},
            settings=settings,
            transport_settings=transport_settings,
        )
        if physical.result_rows:
            raise DiscoveryError("table has a physical _partition_id column")
        settings, transport_settings = _query_context(
            connection, limits, "partition-discovery"
        )
        result = client.query(
            "SELECT partition_id, sum(rows), sum(marks), sum(bytes_on_disk) "
            "FROM system.parts WHERE active "
            "AND database = %(database)s AND table = %(table)s "
            "GROUP BY partition_id ORDER BY partition_id",
            parameters={"database": table.database, "table": table.table},
            settings=settings,
            transport_settings=transport_settings,
        )
        partitions = []
        for row in result.result_rows:
            if len(row) != 4 or not isinstance(row[0], str) or not row[0]:
                raise DiscoveryError(
                    "system.parts returned malformed partition metadata"
                )
            values = tuple(int(value) for value in row[1:])
            if any(value < 0 for value in values):
                raise DiscoveryError(
                    "system.parts returned negative partition metadata"
                )
            partitions.append(PartitionInfo(row[0], *values))
        return tuple(partitions)
    except RayClickHouseError:
        raise
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit, GeneratorExit)):
            raise
        raise _translate(exc, operation="partition discovery") from None
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass


def discover_range_facts(
    connection: ClickHouseConnection,
    table: QualifiedTable,
    limits: ResourceLimits,
    *,
    column: TargetColumn,
    filter_sql: str | None,
    parameters: Mapping[str, Any] | None,
) -> RangeFacts:
    parsed = parse_type(column.declared_type)
    while parsed.name in {"Nullable", "LowCardinality"}:
        parsed = parse_type(parsed.arguments[0])
    if parsed.name not in {
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
    }:
        raise DiscoveryError("range split currently supports integer columns only")
    client = None
    try:
        client = _open(connection, limits)
        base_sql, bound = build_select(
            table=table,
            columns=(column.name,),
            filter_sql=validate_filter(filter_sql),
            parameters=parameters,
        )
        # Reuse the validated table/filter renderer without parsing user SQL text.
        prefix = f"SELECT `{column.name}` FROM {table.sql()}"
        if not base_sql.startswith(prefix):
            raise DiscoveryError("range filter renderer returned an unexpected query")
        where = base_sql[len(prefix) :].strip()
        sql = (
            f"SELECT count(), min(`{column.name}`), max(`{column.name}`), "
            f"countIf(isNull(`{column.name}`)) FROM {table.sql()}"
        )
        if where:
            sql += f" {where}"
        settings, transport_settings = _query_context(
            connection, limits, "range-discovery"
        )
        result = client.query(
            sql,
            parameters=dict(bound),
            settings=settings,
            transport_settings=transport_settings,
        )
        if len(result.result_rows) != 1 or len(result.result_rows[0]) != 4:
            raise DiscoveryError("range aggregate returned malformed metadata")
        total, minimum, maximum, null_rows = result.result_rows[0]
        total = int(total)
        null_rows = int(null_rows)
        return RangeFacts(
            column=column.name,
            declared_type=column.declared_type,
            total_rows=total,
            null_rows=null_rows,
            minimum=None if minimum is None else int(minimum),
            maximum=None if maximum is None else int(maximum),
        )
    except RayClickHouseError:
        raise
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit, GeneratorExit)):
            raise
        raise _translate(exc, operation="range discovery") from None
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass


def discover_target(
    connection: ClickHouseConnection, table: QualifiedTable, limits: ResourceLimits
) -> TargetTable:
    client = None
    try:
        client = _open(connection, limits)
        engine = discover_engine(connection, table, limits)
        if engine not in WRITE_ENGINES:
            raise SchemaError(
                f"engine {engine!r} is outside the write capability profile"
            )
        settings, transport_settings = _query_context(
            connection, limits, "target-column-discovery"
        )
        result = client.query(
            "SELECT name, type, default_kind, default_expression, position "
            "FROM system.columns WHERE database = %(database)s AND table = %(table)s "
            "ORDER BY position",
            parameters={"database": table.database, "table": table.table},
            settings=settings,
            transport_settings=transport_settings,
        )
        columns = parse_describe_rows(result.result_rows)
        return TargetTable(table.database, table.table, engine, columns)
    except RayClickHouseError:
        raise
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit, GeneratorExit)):
            raise
        raise _translate(exc, operation="target discovery") from None
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass
