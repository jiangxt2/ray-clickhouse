"""Safe, explicit ClickHouse table-management helpers for write opt-ins."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pyarrow as pa

from ray_clickhouse._discovery import _open, _query_context, _translate
from ray_clickhouse._errors import (
    AmbiguousTableManagementError,
    ConfigurationError,
    RayClickHouseError,
    SchemaError,
)
from ray_clickhouse._models import (
    ClickHouseConnection,
    QualifiedTable,
    ResourceLimits,
    validate_identifier,
)

_CREATE_ENGINES = frozenset(
    {
        "MergeTree",
        "ReplacingMergeTree",
        "SummingMergeTree",
        "AggregatingMergeTree",
        "CollapsingMergeTree",
        "VersionedCollapsingMergeTree",
    }
)


def validate_create_engine(engine: str) -> str:
    validate_identifier(engine, name="engine")
    if engine not in _CREATE_ENGINES:
        raise ConfigurationError(
            "create/overwrite currently supports only local MergeTree-family engines"
        )
    return engine


def _quote_identifier(value: str, *, name: str) -> str:
    validate_identifier(value, name=name)
    return f"`{value}`"


def _arrow_type(dtype: pa.DataType) -> str:
    if pa.types.is_boolean(dtype):
        return "Bool"
    integer_types = (
        (pa.types.is_int8, "Int8"),
        (pa.types.is_int16, "Int16"),
        (pa.types.is_int32, "Int32"),
        (pa.types.is_int64, "Int64"),
        (pa.types.is_uint8, "UInt8"),
        (pa.types.is_uint16, "UInt16"),
        (pa.types.is_uint32, "UInt32"),
        (pa.types.is_uint64, "UInt64"),
    )
    for predicate, name in integer_types:
        if predicate(dtype):
            return name
    if pa.types.is_float32(dtype):
        return "Float32"
    if pa.types.is_float64(dtype):
        return "Float64"
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return "String"
    if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
        raise SchemaError("Arrow binary types require an explicit ClickHouse mapping")
    if pa.types.is_date32(dtype):
        return "Date"
    if pa.types.is_timestamp(dtype):
        precision = {"s": 0, "ms": 3, "us": 6, "ns": 9}[dtype.unit]
        timezone = f", '{dtype.tz}'" if dtype.tz else ""
        return f"DateTime64({precision}{timezone})"
    if pa.types.is_decimal(dtype):
        return f"Decimal({dtype.precision}, {dtype.scale})"
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        return f"Array({_arrow_type(dtype.value_type)})"
    if pa.types.is_map(dtype):
        return f"Map({_arrow_type(dtype.key_type)}, {_arrow_type(dtype.item_type)})"
    if pa.types.is_struct(dtype):
        fields = []
        for field in dtype:
            fields.append(
                f"{_quote_identifier(field.name, name='nested field')} "
                f"{_arrow_field_type(field)}"
            )
        return f"Tuple({', '.join(fields)})"
    raise SchemaError(f"Arrow type {dtype} cannot be mapped to ClickHouse safely")


def _arrow_field_type(
    field: pa.Field[pa.DataType], *, nullable: bool | None = None
) -> str:
    value = _arrow_type(field.type)
    if nullable is None:
        nullable = field.nullable
    return f"Nullable({value})" if nullable else value


def _default_order_by(schema: pa.Schema) -> tuple[str, ...]:
    for field in schema:
        if pa.types.is_timestamp(field.type):
            return (field.name,)
    for field in schema:
        if not pa.types.is_string(field.type) and not pa.types.is_large_string(
            field.type
        ):
            return (field.name,)
    return (schema.field(0).name,)


def _render_order_by(columns: Sequence[str]) -> str:
    if not columns:
        return "tuple()"
    quoted = [_quote_identifier(column, name="order_by column") for column in columns]
    return quoted[0] if len(quoted) == 1 else f"({', '.join(quoted)})"


def _create_sql(
    table: QualifiedTable,
    schema: pa.Schema,
    *,
    engine: str,
    order_by: Sequence[str] | None,
    nullable_columns: Sequence[str] | None,
) -> str:
    if not len(schema):
        raise SchemaError("create/overwrite requires a non-empty Arrow schema")
    nullable = set(nullable_columns or ())
    unknown_nullable = nullable.difference(schema.names)
    if unknown_nullable:
        raise ConfigurationError(
            f"nullable_columns are not in the Arrow schema: {sorted(unknown_nullable)}"
        )
    columns = ", ".join(
        f"{_quote_identifier(field.name, name='column')} "
        f"{_arrow_field_type(field, nullable=field.name in nullable)}"
        for field in schema
    )
    keys = tuple(order_by) if order_by is not None else _default_order_by(schema)
    unknown_order = set(keys).difference(schema.names)
    if unknown_order:
        raise ConfigurationError(
            f"order_by columns are not in the Arrow schema: {sorted(unknown_order)}"
        )
    nullable_key_setting = (
        " SETTINGS allow_nullable_key = 1" if set(keys).intersection(nullable) else ""
    )
    return (
        f"CREATE TABLE {table.sql()} ({columns}) ENGINE = {engine} "
        f"ORDER BY {_render_order_by(keys)}{nullable_key_setting}"
    )


def create_or_replace_table(
    connection: ClickHouseConnection,
    table: QualifiedTable,
    limits: ResourceLimits,
    schema: pa.Schema,
    *,
    mode: str,
    engine: str,
    order_by: Sequence[str] | None,
    nullable_columns: Sequence[str] | None = None,
) -> None:
    if mode not in {"create", "overwrite"}:
        raise ConfigurationError("table management mode must be create or overwrite")
    validate_create_engine(engine)
    ddl = _create_sql(
        table,
        schema,
        engine=engine,
        order_by=order_by,
        nullable_columns=nullable_columns,
    )
    client: Any | None = None
    drop_started = False
    try:
        client = _open(connection, limits)
        settings, transport_settings = _query_context(
            connection, limits, f"{mode}-table-exists"
        )
        result = client.query(
            "SELECT count() FROM system.tables "
            "WHERE database = %(database)s AND name = %(table)s",
            parameters={"database": table.database, "table": table.table},
            settings=settings,
            transport_settings=transport_settings,
        )
        exists = bool(result.result_rows and int(result.result_rows[0][0]))
        if mode == "create" and exists:
            raise ConfigurationError(f"ClickHouse table {table} already exists")
        if mode == "overwrite" and exists:
            drop_started = True
            settings, transport_settings = _query_context(
                connection, limits, "overwrite-drop"
            )
            client.command(
                f"DROP TABLE {table.sql()}",
                settings=settings,
                transport_settings=transport_settings,
            )
        settings, transport_settings = _query_context(
            connection, limits, f"{mode}-table-create"
        )
        client.command(
            ddl,
            settings=settings,
            transport_settings=transport_settings,
        )
    except AmbiguousTableManagementError:
        raise
    except RayClickHouseError:
        if drop_started:
            raise AmbiguousTableManagementError(
                f"ClickHouse overwrite for {table} may have dropped the old table "
                "without confirming the replacement"
            ) from None
        raise
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit, GeneratorExit)):
            raise
        if drop_started:
            raise AmbiguousTableManagementError(
                f"ClickHouse overwrite for {table} may have dropped the old table "
                "without confirming the replacement"
            ) from None
        raise _translate(exc, operation=f"{mode} table") from None
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass
