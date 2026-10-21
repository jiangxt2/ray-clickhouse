"""Pure, complete-predicate compilation for a deliberately small scalar profile."""

from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa

from ray_clickhouse._errors import ConfigurationError
from ray_clickhouse._models import validate_identifier
from ray_clickhouse._schema import SchemaPlan, parse_type

Scalar = str | bytes | int | bool | None


@dataclass(frozen=True)
class PredicateNode:
    op: str
    children: tuple[PredicateNode, ...] = ()
    value: Scalar = field(default=None, repr=False)


@dataclass(frozen=True, repr=False)
class CompiledPredicate:
    sql: str
    parameters: tuple[tuple[str, Scalar], ...]
    columns: frozenset[str]
    native: bool


class _Unsupported(Exception):
    pass


_COMPARISONS = {"eq": "=", "ne": "!=", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}
_REVERSE = {"eq": "eq", "ne": "ne", "lt": "gt", "le": "ge", "gt": "lt", "ge": "le"}
_INTEGER_TYPES = {
    f"{prefix}{bits}": (bits, prefix == "Int")
    for prefix in ("Int", "UInt")
    for bits in (8, 16, 32, 64)
}


def compile_predicate(
    node: PredicateNode, schema: SchemaPlan, *, native: bool
) -> CompiledPredicate | None:
    """Return a complete translation, or reject without exposing literal values."""
    definitions = {column.name: column for column in schema.columns}
    parameters: list[tuple[str, Scalar]] = []
    used: set[str] = set()
    visited = 0

    def column_type(node: PredicateNode) -> tuple[str, pa.DataType]:
        if node.op != "column" or not isinstance(node.value, str):
            raise _Unsupported
        try:
            name = validate_identifier(node.value, name="predicate column")
        except ConfigurationError:
            raise _Unsupported from None
        column = definitions.get(name)
        if (
            column is None
            or column.default_kind == "ALIAS"
            or name not in schema.arrow_schema.names
        ):
            raise _Unsupported
        parsed = parse_type(column.declared_type)
        while parsed.name in {"Nullable", "LowCardinality"}:
            if len(parsed.arguments) != 1:
                raise _Unsupported
            parsed = parse_type(parsed.arguments[0])
        if parsed.arguments or parsed.name not in {*_INTEGER_TYPES, "Bool", "String"}:
            raise _Unsupported
        dtype = schema.arrow_schema.field(name).type
        if pa.types.is_dictionary(dtype):
            dtype = dtype.value_type
        if parsed.name in _INTEGER_TYPES:
            bits, signed = _INTEGER_TYPES[parsed.name]
            expected = getattr(pa, f"{'int' if signed else 'uint'}{bits}")()
            if dtype != expected:
                raise _Unsupported
        elif parsed.name == "Bool" and not pa.types.is_boolean(dtype):
            raise _Unsupported
        elif parsed.name == "String" and not (
            pa.types.is_string(dtype) or pa.types.is_binary(dtype)
        ):
            raise _Unsupported
        used.add(name)
        return parsed.name, dtype

    def reference(node: PredicateNode) -> str:
        column_type(node)
        assert isinstance(node.value, str)
        return f"`{node.value}`"

    def bind(value: Scalar, declared: str, dtype: pa.DataType, op: str) -> str:
        if declared in _INTEGER_TYPES:
            # Arrow 19 compares UInt64 arrays to Python Int64 scalars by casting
            # the array to Int64. High column values raise, even for small
            # constants; translating this to ClickHouse would hide that failure.
            if declared == "UInt64":
                raise _Unsupported
            bits, signed = _INTEGER_TYPES[declared]
            lower = -(2 ** (bits - 1)) if signed else 0
            upper = 2 ** (bits - (1 if signed else 0))
            # Ray constructs Python-int literals as Arrow Int64 scalars. Do not
            # repair expressions that Arrow itself cannot represent or compare.
            if type(value) is not int or not lower <= value < upper:
                raise _Unsupported
            if not -(2**63) <= value < 2**63:
                raise _Unsupported
        elif declared == "Bool":
            if type(value) is not bool or op not in {"eq", "ne"}:
                raise _Unsupported
        elif op not in {"eq", "ne"} or not (
            (pa.types.is_string(dtype) and type(value) is str)
            or (pa.types.is_binary(dtype) and type(value) is bytes)
        ):
            raise _Unsupported
        name = f"__ray_clickhouse_expr_{len(parameters)}"
        parameters.append((name, value))
        return f"{{{name}:{declared}}}" if native else f"%({name})s"

    def render(node: PredicateNode, depth: int = 0) -> str:
        nonlocal visited
        visited += 1
        if visited > 256 or depth > 32:
            raise _Unsupported
        if node.op in {"and", "or"} and len(node.children) == 2:
            left_sql, right_sql = (render(child, depth + 1) for child in node.children)
            return f"({left_sql} {node.op.upper()} {right_sql})"
        if node.op == "not" and len(node.children) == 1:
            return f"(NOT {render(node.children[0], depth + 1)})"
        if node.op in {"is_null", "is_not_null"} and len(node.children) == 1:
            operand = reference(node.children[0])
            return f"({operand} IS {'NOT ' if node.op == 'is_not_null' else ''}NULL)"
        if node.op == "column":
            declared, _ = column_type(node)
            if declared != "Bool":
                raise _Unsupported
            return f"({reference(node)} = {bind(True, declared, pa.bool_(), 'eq')})"
        if node.op in _COMPARISONS and len(node.children) == 2:
            left, right = node.children
            op = node.op
            if left.op == "literal" and right.op == "column":
                left, right, op = right, left, _REVERSE[op]
            if left.op != "column" or right.op != "literal":
                raise _Unsupported
            declared, dtype = column_type(left)
            placeholder = bind(right.value, declared, dtype, op)
            return f"({reference(left)} {_COMPARISONS[op]} {placeholder})"
        raise _Unsupported

    try:
        sql = render(node)
    except _Unsupported:
        return None
    return CompiledPredicate(sql, tuple(parameters), frozenset(used), native)
