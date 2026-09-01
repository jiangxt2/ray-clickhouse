"""ClickHouse declaration parsing and Arrow schema validation."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from ray_clickhouse._errors import SchemaError, WriteError
from ray_clickhouse._models import QualifiedTable
from ray_clickhouse._sql import normalize_columns


@dataclass(frozen=True)
class ClickHouseType:
    name: str
    arguments: tuple[str, ...] = ()


@dataclass(frozen=True)
class TargetColumn:
    name: str
    declared_type: str
    default_kind: str
    default_expression: str
    position: int

    @property
    def insertable(self) -> bool:
        return self.default_kind.upper() not in {"MATERIALIZED", "ALIAS"}

    @property
    def has_server_default(self) -> bool:
        kind = self.default_kind.upper()
        return kind in {"MATERIALIZED", "ALIAS"} or (
            kind == "DEFAULT" and bool(self.default_expression.strip())
        )


@dataclass(frozen=True)
class SchemaPlan:
    arrow_schema: pa.Schema
    columns: tuple[TargetColumn, ...]


@dataclass(frozen=True)
class TargetTable:
    database: str
    table: str
    engine: str
    columns: tuple[TargetColumn, ...]

    @property
    def insertable_columns(self) -> tuple[TargetColumn, ...]:
        return tuple(column for column in self.columns if column.insertable)

    def __repr__(self) -> str:
        return (
            f"TargetTable(database={self.database!r}, table={self.table!r}, "
            f"engine={self.engine!r}, columns={len(self.columns)})"
        )


_TUPLE_FIELD = re.compile(
    r"^(?P<name>`(?:[^`]|``)+`|[A-Za-z_][A-Za-z0-9_]*)\s+(?P<type>.+)$"
)


def split_type_arguments(value: str) -> tuple[str, ...]:
    parts: list[str] = []
    start = 0
    depth = 0
    quote: str | None = None
    index = 0
    while index < len(value):
        char = value[index]
        if quote is not None:
            if char == "\\":
                index += 2
                continue
            if char == quote:
                quote = None
            index += 1
            continue
        if char in {"'", '"', "`"}:
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth < 0:
                raise SchemaError(f"unbalanced ClickHouse type: {value!r}")
        elif char == "," and depth == 0:
            part = value[start:index].strip()
            if not part:
                raise SchemaError(f"empty ClickHouse type argument: {value!r}")
            parts.append(part)
            start = index + 1
        index += 1
    if quote is not None or depth != 0:
        raise SchemaError(f"unbalanced ClickHouse type: {value!r}")
    final = value[start:].strip()
    if not final:
        raise SchemaError(f"empty ClickHouse type argument: {value!r}")
    parts.append(final)
    return tuple(parts)


def parse_type(value: str) -> ClickHouseType:
    text = value.strip()
    if not text:
        raise SchemaError("ClickHouse type must not be empty")
    open_index = text.find("(")
    if open_index < 0:
        return ClickHouseType(text)
    if not text.endswith(")"):
        raise SchemaError(f"invalid ClickHouse type: {value!r}")
    return ClickHouseType(
        text[:open_index].strip(), split_type_arguments(text[open_index + 1 : -1])
    )


def _transport_type_expression(value: str) -> str:
    parsed = parse_type(value)
    if parsed.name == "Date":
        return "Date32"
    if parsed.name in {"UUID", "IPv4", "IPv6", "Enum8", "Enum16"}:
        return "String"
    if parsed.name in {
        "String",
        "FixedString",
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
        "Float32",
        "Float64",
        "Bool",
        "Date32",
        "DateTime",
        "DateTime64",
        "Decimal",
        "Decimal32",
        "Decimal64",
        "Decimal128",
    }:
        return value.strip()
    if parsed.name in {"Nullable", "LowCardinality", "Array"}:
        if len(parsed.arguments) != 1:
            raise SchemaError(f"invalid ClickHouse type: {value!r}")
        return f"{parsed.name}({_transport_type_expression(parsed.arguments[0])})"
    if parsed.name == "Map":
        if len(parsed.arguments) != 2:
            raise SchemaError(f"invalid ClickHouse type: {value!r}")
        rendered_arguments = ", ".join(
            _transport_type_expression(argument) for argument in parsed.arguments
        )
        return f"Map({rendered_arguments})"
    if parsed.name == "Tuple":
        if not parsed.arguments:
            raise SchemaError(f"invalid ClickHouse type: {value!r}")
        rendered_arguments_list: list[str] = []
        for argument in parsed.arguments:
            match = _TUPLE_FIELD.fullmatch(argument)
            if match is None:
                rendered_arguments_list.append(_transport_type_expression(argument))
            else:
                rendered_arguments_list.append(
                    f"{match.group('name')} "
                    f"{_transport_type_expression(match.group('type'))}"
                )
        return f"Tuple({', '.join(rendered_arguments_list)})"
    if parsed.name == "SimpleAggregateFunction" and len(parsed.arguments) == 2:
        return (
            f"SimpleAggregateFunction({parsed.arguments[0]}, "
            f"{_transport_type_expression(parsed.arguments[1])})"
        )
    raise SchemaError(f"unsupported ClickHouse type: {value!r}")


def render_read_projection(columns: Sequence[TargetColumn]) -> str:
    """Render a stable Arrow projection for ClickHouse logical date/text types."""
    projection = []
    for column in columns:
        identifier = f"`{column.name}`"
        transport_type = _transport_type_expression(column.declared_type)
        if transport_type == column.declared_type.strip():
            projection.append(identifier)
        else:
            projection.append(f"CAST({identifier} AS {transport_type}) AS {identifier}")
    return ", ".join(projection)


def parse_describe_rows(rows: Sequence[Sequence[Any]]) -> tuple[TargetColumn, ...]:
    columns: list[TargetColumn] = []
    for row in rows:
        if len(row) < 2 or not isinstance(row[0], str) or not isinstance(row[1], str):
            raise SchemaError("ClickHouse DESCRIBE returned malformed metadata")
        position = len(columns) + 1
        if len(row) > 4 and row[4] not in (None, ""):
            try:
                position = int(row[4])
            except (TypeError, ValueError):
                # DESCRIBE TABLE returns comment in this position on some versions;
                # system.columns returns the numeric position.
                pass
        columns.append(
            TargetColumn(
                name=row[0],
                declared_type=row[1].strip(),
                default_kind=str(row[2]) if len(row) > 2 and row[2] is not None else "",
                default_expression=str(row[3])
                if len(row) > 3 and row[3] is not None
                else "",
                position=position,
            )
        )
    if not columns or len({column.name for column in columns}) != len(columns):
        raise SchemaError("ClickHouse DESCRIBE returned no or duplicate columns")
    return tuple(sorted(columns, key=lambda column: column.position))


def canonical_schema(
    columns: tuple[TargetColumn, ...], arrow_schema: pa.Schema
) -> SchemaPlan:
    names = tuple(column.name for column in columns)
    if tuple(arrow_schema.names) != names:
        raise SchemaError("ClickHouse DESCRIBE and Arrow schemas disagree")
    try:
        arrow_schema.serialize()
    except Exception:
        raise SchemaError("ClickHouse schema cannot be represented in Arrow") from None
    return SchemaPlan(arrow_schema, columns)


def _unwrap(parsed: ClickHouseType) -> tuple[ClickHouseType, bool]:
    nullable = False
    while parsed.name in {"Nullable", "LowCardinality", "SimpleAggregateFunction"}:
        if parsed.name == "Nullable":
            if len(parsed.arguments) != 1:
                return parsed, nullable
            nullable = True
            parsed = parse_type(parsed.arguments[0])
        elif parsed.name == "LowCardinality":
            if len(parsed.arguments) != 1:
                return parsed, nullable
            parsed = parse_type(parsed.arguments[0])
        elif len(parsed.arguments) == 2:
            parsed = parse_type(parsed.arguments[1])
        else:
            return parsed, nullable
    return parsed, nullable


def _timestamp_timezone(parsed: ClickHouseType) -> str | None:
    if parsed.name == "DateTime" and parsed.arguments:
        return parsed.arguments[0].strip("'\"")
    if parsed.name == "DateTime64" and len(parsed.arguments) == 2:
        return parsed.arguments[1].strip("'\"")
    return None


def _tuple_argument_type(argument: str) -> str:
    match = _TUPLE_FIELD.fullmatch(argument)
    return match.group("type") if match is not None else argument


def arrow_compatible(dtype: pa.DataType, declared_type: str) -> bool:
    parsed, _ = _unwrap(parse_type(declared_type))
    if parsed.name in {
        "String",
        "Enum8",
        "Enum16",
        "UUID",
        "IPv4",
        "IPv6",
        "FixedString",
    }:
        return bool(pa.types.is_string(dtype) or pa.types.is_binary(dtype))
    integer_types: dict[str, Callable[[pa.DataType], bool]] = {
        "Int8": pa.types.is_int8,
        "Int16": pa.types.is_int16,
        "Int32": pa.types.is_int32,
        "Int64": pa.types.is_int64,
        "UInt8": pa.types.is_uint8,
        "UInt16": pa.types.is_uint16,
        "UInt32": pa.types.is_uint32,
        "UInt64": pa.types.is_uint64,
    }
    if parsed.name in integer_types:
        return bool(integer_types[parsed.name](dtype))
    if parsed.name in {"Float32", "Float64"}:
        return bool(
            (parsed.name == "Float32" and pa.types.is_float32(dtype))
            or (parsed.name == "Float64" and pa.types.is_float64(dtype))
        )
    if parsed.name == "Bool":
        return bool(pa.types.is_boolean(dtype))
    if parsed.name in {"Date", "Date32"}:
        return bool(pa.types.is_date32(dtype))
    if parsed.name in {"DateTime", "DateTime64"}:
        if not pa.types.is_timestamp(dtype):
            return False
        if dtype.tz != _timestamp_timezone(parsed):
            return False
        if parsed.name == "DateTime":
            return dtype.unit in {"s", "ms", "us", "ns"}
        return dtype.unit in {"s", "ms", "us", "ns"}
    if parsed.name == "Decimal":
        if not pa.types.is_decimal(dtype) or len(parsed.arguments) != 2:
            return False
        try:
            precision, scale = (int(argument) for argument in parsed.arguments)
        except (TypeError, ValueError):
            return False
        return bool(dtype.precision == precision and dtype.scale == scale)
    if parsed.name == "Array" and len(parsed.arguments) == 1:
        return bool(
            (pa.types.is_list(dtype) or pa.types.is_large_list(dtype))
            and arrow_compatible(dtype.value_type, parsed.arguments[0])
        )
    if parsed.name == "Map" and len(parsed.arguments) == 2:
        return bool(
            pa.types.is_map(dtype)
            and arrow_compatible(dtype.key_type, parsed.arguments[0])
            and arrow_compatible(dtype.item_type, parsed.arguments[1])
        )
    if parsed.name == "Tuple":
        return bool(
            pa.types.is_struct(dtype)
            and len(parsed.arguments) == len(dtype)
            and all(
                arrow_compatible(field.type, _tuple_argument_type(argument))
                for field, argument in zip(dtype, parsed.arguments, strict=True)
            )
        )
    return False


def validate_source_schema(
    source_schema: pa.Schema,
    target: TargetTable,
    columns: tuple[str, ...] | None,
) -> tuple[TargetColumn, ...]:
    selected = normalize_columns(columns) or tuple(source_schema.names)
    source_fields = {field.name: field for field in source_schema}
    insertable = {column.name: column for column in target.insertable_columns}
    missing = [name for name in selected if name not in source_fields]
    if missing:
        raise SchemaError(f"source schema is missing columns: {', '.join(missing)}")
    unknown = [name for name in selected if name not in insertable]
    if unknown:
        raise SchemaError(f"source columns are not insertable: {', '.join(unknown)}")
    missing_required = [
        column.name
        for column in target.insertable_columns
        if column.name not in selected and not column.has_server_default
    ]
    if missing_required:
        raise SchemaError(
            f"source schema is missing required columns: {', '.join(missing_required)}"
        )
    for name in selected:
        column = insertable[name]
        if not arrow_compatible(source_fields[name].type, column.declared_type):
            raise WriteError(
                f"Arrow type for column {name!r} is incompatible with ClickHouse"
            )
    return tuple(insertable[name] for name in selected)


def prepare_arrow_table(
    table: pa.Table, source_schema: pa.Schema, columns: tuple[TargetColumn, ...]
) -> pa.Table:
    if tuple(table.column_names) != tuple(
        source_schema.names
    ) or not table.schema.equals(source_schema, check_metadata=False):
        raise WriteError("write micropartitions do not share one stable Arrow schema")
    projected = table.select([column.name for column in columns])
    for column in columns:
        field = projected.schema.field(column.name)
        parsed, nullable = _unwrap(parse_type(column.declared_type))
        if field.nullable and not nullable and projected[column.name].null_count:
            raise WriteError(
                f"column {column.name!r} contains NULL for a non-nullable target"
            )
        if parsed.name == "Decimal" and pa.types.is_decimal(field.type):
            if field.type.precision != int(
                parsed.arguments[0]
            ) or field.type.scale != int(parsed.arguments[1]):
                raise WriteError(
                    f"column {column.name!r} has incompatible Decimal precision/scale"
                )
    return projected


def qualified_table(database: str, table: str) -> QualifiedTable:
    return QualifiedTable(database, table)
