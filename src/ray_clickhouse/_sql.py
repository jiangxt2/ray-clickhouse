"""Safe table, predicate, partition and range SQL rendering."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from ray_clickhouse._errors import ConfigurationError
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


def _validate_filter_parameters(
    filter_sql: str | None, parameters: dict[str, Any]
) -> None:
    if filter_sql is None:
        if parameters:
            raise ConfigurationError("query_parameters require filter")
        return
    used = _filter_parameter_names(filter_sql)
    missing = used.difference(parameters)
    unused = set(parameters).difference(used)
    if missing:
        raise ConfigurationError(
            f"filter references missing parameters: {sorted(missing)}"
        )
    if unused:
        raise ConfigurationError(f"unused query parameters: {sorted(unused)}")


def _filter_parameter_names(value: str) -> set[str]:
    names: set[str] = set()
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
        match = _PARAMETER.match(value, index)
        if match is not None:
            names.add(match.group("name"))
            index = match.end()
            continue
        index += 1
    return names


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
    _validate_filter_parameters(filter_sql, values)
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
            names.append(f"%({key})s")
        clauses.append(f"(`_partition_id` IN ({', '.join(names)}))")
    range_sql: str | None = None
    if range_column is not None:
        column = validate_identifier(range_column, name="range_column")
        range_clauses = []
        if range_lower is not None:
            key = f"{_INTERNAL_PREFIX}range_lower"
            values[key] = range_lower
            range_clauses.append(f"`{column}` >= %({key})s")
        if range_upper is not None:
            key = f"{_INTERNAL_PREFIX}range_upper"
            values[key] = range_upper
            range_clauses.append(f"`{column}` < %({key})s")
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
