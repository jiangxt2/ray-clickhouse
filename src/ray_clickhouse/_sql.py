"""Safe table, predicate, partition and range SQL rendering."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from ray_clickhouse._errors import ConfigurationError, SchemaError
from ray_clickhouse._models import QualifiedTable, validate_identifier

_PARAMETER = re.compile(r"%\((?P<name>[A-Za-z_][A-Za-z0-9_]*)\)s")
_INTERNAL_PREFIX = "__ray_clickhouse_"


def normalize_columns(
    columns: Sequence[str] | None,
) -> tuple[str, ...] | None:
    if columns is None:
        return None
    if isinstance(columns, str):
        raise ConfigurationError("columns must be a sequence")
    result = tuple(columns)
    if not result:
        raise ConfigurationError("columns must not be empty")
    normalized = tuple(validate_identifier(column, name="column") for column in result)
    if len(set(normalized)) != len(normalized):
        raise ConfigurationError("columns must not contain duplicates")
    return normalized


def normalize_order_by(
    value: tuple[Sequence[str], bool] | None,
) -> tuple[tuple[str, bool], ...] | None:
    if value is None:
        return None
    if not isinstance(value, tuple) or len(value) != 2:
        raise ConfigurationError(
            "order_by must be a tuple of (column sequence, descending flag)"
        )
    columns, descending = value
    if isinstance(columns, str) or not isinstance(columns, Sequence):
        raise ConfigurationError("order_by columns must be a sequence")
    normalized = normalize_columns(tuple(columns))
    if normalized is None:
        raise ConfigurationError("order_by columns must not be empty")
    if not isinstance(descending, bool):
        raise ConfigurationError("order_by descending flag must be boolean")
    return tuple((column, descending) for column in normalized)


def validate_filter(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError("filter must be None or a non-empty trusted predicate")
    if ";" in value or "\x00" in value:
        raise ConfigurationError("filter must contain one predicate and no semicolon")
    if _contains_sql_clause(value):
        raise ConfigurationError(
            "filter must be a predicate; SELECT/GROUP BY/ORDER BY/LIMIT clauses "
            "are unsupported"
        )
    return value.strip()


_FORBIDDEN_CLAUSE = re.compile(
    r"\b(?:SELECT|FROM|GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|UNION|WITH|"
    r"INSERT|UPDATE|DELETE|CREATE|DROP)\b",
    re.IGNORECASE,
)


def _contains_sql_clause(value: str) -> bool:
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
            index += 1
            continue
        match = _FORBIDDEN_CLAUSE.match(value, index)
        if match is not None:
            return True
        index += 1
    return False


def _parameter_map(parameters: Mapping[str, Any] | None) -> dict[str, Any]:
    values = dict(parameters or {})
    if any(not isinstance(name, str) or not name for name in values):
        raise ConfigurationError("query parameter names must be non-empty strings")
    if any(name.startswith(_INTERNAL_PREFIX) for name in values):
        raise ConfigurationError("query parameter names use a reserved prefix")
    return values


_SERVER_PARAMETER = re.compile(r"\{(?P<name>[A-Za-z_][A-Za-z0-9_]*):(?P<type>[^{}]+)\}")
_DRIVER_SERVER_PARAMETER = re.compile(r"\{\w+:[^{}]+\}")
_SCALAR_PARAMETER_TYPES = frozenset(
    {"String", "Bool", "Float32", "Float64", "Date", "Date32"}
    | {f"{prefix}{bits}" for prefix in ("Int", "UInt") for bits in (8, 16, 32, 64)}
)


def _parameter_type(value: str) -> str:
    # The schema module uses SQL helpers; import after module initialization.
    from ray_clickhouse._schema import parse_type

    try:
        parsed = parse_type(value)
        name, arguments = parsed.name, parsed.arguments
        if name in _SCALAR_PARAMETER_TYPES and not arguments:
            return name
        if name == "Nullable" and len(arguments) == 1:
            inner = _parameter_type(arguments[0])
            if not inner.startswith("Nullable("):
                return f"Nullable({inner})"
        if name == "FixedString" and len(arguments) == 1:
            length = int(arguments[0])
            if length > 0:
                return f"FixedString({length})"
        if name in {"Decimal", "Decimal32", "Decimal64", "Decimal128"}:
            precision = {"Decimal32": 9, "Decimal64": 18, "Decimal128": 38}.get(name)
            if name == "Decimal" and len(arguments) == 2:
                precision, scale = (int(argument) for argument in arguments)
            elif precision is not None and len(arguments) == 1:
                scale = int(arguments[0])
            else:
                raise ValueError
            if (
                precision is not None
                and 1 <= precision <= 38
                and 0 <= scale <= precision
            ):
                return f"{name}({','.join(argument.strip() for argument in arguments)})"
        if name in {"DateTime", "DateTime64"}:
            if name == "DateTime":
                if len(arguments) > 1:
                    raise ValueError
                values = list(arguments)
            else:
                if len(arguments) not in {1, 2} or not 0 <= int(arguments[0]) <= 9:
                    raise ValueError
                values = [str(int(arguments[0])), *arguments[1:]]
            timezone = values[-1] if values and values[-1].startswith("'") else None
            expected = 0 if name == "DateTime" else 1
            if len(values) > expected:
                if (
                    timezone is None
                    or re.fullmatch(r"'[A-Za-z0-9_./+:-]+'", timezone) is None
                ):
                    raise ValueError
            return name if not values else f"{name}({','.join(values)})"
    except (SchemaError, ValueError):
        pass
    raise ConfigurationError("unsupported server-side parameter type")


def _filter_bindings(value: str) -> tuple[set[str], dict[str, str], bool]:
    client_names: set[str] = set()
    comment_client_names: set[str] = set()
    server_types: dict[str, str] = {}
    ambiguous = False
    index = 0
    while index < len(value):
        char = value[index]
        if char in {"'", '"', "`"}:
            start = index
            index += 1
            while index < len(value):
                if value[index] == "\\":
                    index += 2
                elif value[index] == char:
                    index += 1
                    if index < len(value) and value[index] == char:
                        index += 1
                    else:
                        break
                else:
                    index += 1
            else:
                raise ConfigurationError("unterminated filter quote")
            ambiguous |= bool(_DRIVER_SERVER_PARAMETER.search(value[start:index]))
            continue
        if value.startswith("/*", index):
            end = value.find("*/", index + 2)
            if end < 0:
                raise ConfigurationError("unterminated filter comment")
            end += 2
            comment_client_names.update(
                match.group("name") for match in _PARAMETER.finditer(value[index:end])
            )
            ambiguous |= bool(_DRIVER_SERVER_PARAMETER.search(value[index:end]))
            index = end
            continue
        if value.startswith("--", index) or char == "#":
            end = value.find("\n", index)
            end = len(value) if end < 0 else end
            comment_client_names.update(
                match.group("name") for match in _PARAMETER.finditer(value[index:end])
            )
            ambiguous |= bool(_DRIVER_SERVER_PARAMETER.search(value[index:end]))
            index = end
            continue
        match = _PARAMETER.match(value, index)
        if match is not None:
            client_names.add(match.group("name"))
            index = match.end()
            continue
        if char == "{":
            match = _SERVER_PARAMETER.match(value, index)
            if match is None:
                raise ConfigurationError("malformed server-side parameter placeholder")
            name = match.group("name")
            hint = _parameter_type(match.group("type"))
            if name in server_types and server_types[name] != hint:
                raise ConfigurationError("repeated parameter has conflicting types")
            server_types[name] = hint
            index = match.end()
            continue
        if value.startswith("%(", index) or value.startswith("%s", index):
            raise ConfigurationError("unsupported client-side parameter placeholder")
        index += 1
    # Client formatting includes comments; server binding leaves them untouched.
    if not server_types:
        client_names.update(comment_client_names)
    return client_names, server_types, ambiguous


def validate_filter_parameters(
    filter_sql: str | None,
    parameters: Mapping[str, Any] | None,
    *,
    has_internal_parameters: bool = False,
) -> bool:
    """Validate a filter binding contract and return whether it is server-side."""
    values = _parameter_map(parameters)
    if filter_sql is None:
        if values:
            raise ConfigurationError("query_parameters require filter")
        return False
    client_names, server_types, ambiguous = _filter_bindings(filter_sql)
    if client_names and server_types:
        raise ConfigurationError("mixed client-side and server-side placeholders")
    if ambiguous and (values or has_internal_parameters):
        raise ConfigurationError(
            "typed placeholder-looking text in quotes or comments is unsupported"
        )
    used = client_names | set(server_types)
    missing = used.difference(values)
    unused = set(values).difference(used)
    if missing:
        raise ConfigurationError(
            f"filter references missing parameters: {sorted(missing)}"
        )
    if unused:
        raise ConfigurationError(f"unused query parameters: {sorted(unused)}")
    return bool(server_types)


def build_select(
    *,
    table: QualifiedTable,
    columns: tuple[str, ...],
    filter_sql: str | None,
    parameters: Mapping[str, Any] | None = None,
    partition_ids: tuple[str, ...] | None = None,
    range_column: str | None = None,
    range_lower: int | None = None,
    range_upper: int | None = None,
    range_include_null: bool = False,
    limit: int | None = None,
    projection: str | None = None,
    order_by: tuple[tuple[str, bool], ...] | None = None,
) -> tuple[str, tuple[tuple[str, Any], ...]]:
    filter_sql = validate_filter(filter_sql)
    values = _parameter_map(parameters)
    server_side = validate_filter_parameters(
        filter_sql,
        values,
        has_internal_parameters=partition_ids is not None
        or range_lower is not None
        or range_upper is not None,
    )
    clauses: list[str] = []
    if filter_sql is not None:
        clauses.append(f"({filter_sql})")
    if partition_ids is not None:
        if not partition_ids:
            raise ConfigurationError("partition_ids must not be empty")
        names = []
        for index, partition_id in enumerate(partition_ids):
            key = f"{_INTERNAL_PREFIX}partition_{index}"
            values[key] = partition_id
            names.append(f"{{{key}:String}}" if server_side else f"%({key})s")
        clauses.append(f"(`_partition_id` IN ({', '.join(names)}))")
    range_sql: str | None = None
    if range_column is not None:
        column = validate_identifier(range_column, name="range_column")
        range_clauses = []
        if range_lower is not None:
            key = f"{_INTERNAL_PREFIX}range_lower"
            values[key] = range_lower
            placeholder = f"{{{key}:Int128}}" if server_side else f"%({key})s"
            range_clauses.append(f"`{column}` >= {placeholder}")
        if range_upper is not None:
            key = f"{_INTERNAL_PREFIX}range_upper"
            values[key] = range_upper
            placeholder = f"{{{key}:Int128}}" if server_side else f"%({key})s"
            range_clauses.append(f"`{column}` < {placeholder}")
        if range_clauses:
            range_sql = "(" + " AND ".join(range_clauses) + ")"
            clauses.append(range_sql)
    select_list = projection or ", ".join(f"`{column}`" for column in columns)
    sql = f"SELECT {select_list} FROM {table.sql()}"
    if clauses:
        if range_include_null and range_column is not None and range_sql is not None:
            non_range = [clause for clause in clauses if clause != range_sql]
            null_or_range = f"({range_sql} OR (`{range_column}` IS NULL))"
            if non_range:
                sql += f" WHERE ({' AND '.join([*non_range, null_or_range])})"
            else:
                sql += f" WHERE {null_or_range}"
        else:
            sql += " WHERE " + " AND ".join(clauses)
    if order_by:
        order_terms = []
        for column, descending in order_by:
            validate_identifier(column, name="order_by column")
            order_terms.append(f"`{column}` {'DESC' if descending else 'ASC'}")
        sql += " ORDER BY " + ", ".join(order_terms)
    if limit is not None:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ConfigurationError("limit must be a non-negative integer")
        sql += f" LIMIT {limit}"
    return sql, tuple(values.items())
