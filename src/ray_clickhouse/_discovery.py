"""ClickHouse driver-side schema, engine, partition and range discovery."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ray_clickhouse._predicates import CompiledPredicate

import pyarrow as pa

from ray_clickhouse._errors import (
    AuthenticationError,
    ConfigurationError,
    DiscoveryError,
    ObjectNotFoundError,
    PermissionError,
    RayClickHouseError,
    SchemaError,
    TransportError,
)
from ray_clickhouse._models import (
    ClickHouseConnection,
    QualifiedTable,
    ResourceLimits,
    validate_identifier,
)
from ray_clickhouse._schema import (
    SchemaPlan,
    TargetColumn,
    TargetTable,
    _unwrap,
    arrow_compatible,
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

_SORTING_IDENTIFIER = r"(?:[A-Za-z_][A-Za-z0-9_]*|`[A-Za-z_][A-Za-z0-9_]*`)"
_SORTING_COLUMNS = re.compile(
    rf"{_SORTING_IDENTIFIER}(?:\s*,\s*{_SORTING_IDENTIFIER})*"
)

_PREDICATE_REQUIRED_SETTINGS = {
    "final": "0",
    "max_result_rows": "0",
    "max_result_bytes": "0",
    "limit": "0",
    "offset": "0",
    "additional_result_filter": "",
    "additional_table_filters": "{}",
    "read_overflow_mode": "throw",
    "timeout_overflow_mode": "throw",
    "group_by_overflow_mode": "throw",
    "sort_overflow_mode": "throw",
}
# These query-rewriting settings are absent on older supported servers.
_PREDICATE_OPTIONAL_SETTINGS = {
    "page": "0",
    "select": "",
    "filter": "",
    "order": "",
    "sort": "",
}
_PREDICATE_NEUTRAL_SETTINGS = {
    **_PREDICATE_REQUIRED_SETTINGS,
    **_PREDICATE_OPTIONAL_SETTINGS,
}
_ZERO_SETTING = re.compile(r"[+-]?(?:0+(?:\.0*)?|\.0+)(?:[eE][+-]?[0-9]+)?")
_NEUTRAL_PREDICATE_AST = (
    "SelectWithUnionQuery (children 1)",
    "ExpressionList (children 1)",
    "SelectQuery (children 1)",
    "ExpressionList (children 1)",
    "Literal UInt64_1",
)


def _explicit_predicate_settings_are_neutral(connection: ClickHouseConnection) -> bool:
    defaults = dict(connection.client_options).get("settings", {})
    if not isinstance(defaults, Mapping):
        return False
    explicit = {**defaults, **dict(connection.settings)}
    for name, expected in _PREDICATE_NEUTRAL_SETTINGS.items():
        if name not in explicit:
            continue
        value = explicit[name]
        if expected == "0":
            if isinstance(value, (int, float)) and value == 0:
                continue
            if isinstance(value, str) and _ZERO_SETTING.fullmatch(value.strip()):
                continue
            return False
        if name == "additional_table_filters" and isinstance(value, Mapping):
            if not value:
                continue
            return False
        if not isinstance(value, str) or value != expected:
            return False
    return True


def _predicate_context_query() -> str:
    # Only fixed internal names/values enter this SQL. Aggregate on the server so
    # a usable one-row/low-byte result cap cannot be exhausted by settings rows.
    names = ", ".join(f"'{name}'" for name in _PREDICATE_NEUTRAL_SETTINGS)
    required = ", ".join(f"'{name}'" for name in _PREDICATE_REQUIRED_SETTINGS)
    neutral = " OR ".join(
        f"(name = '{name}' AND value = '{value}')"
        for name, value in _PREDICATE_NEUTRAL_SETTINGS.items()
    )
    return (
        "SELECT toUInt8(multiIf(count() != uniqExact(name), 2, "
        f"countIf(name IN ({required})) != {len(_PREDICATE_REQUIRED_SETTINGS)}, 0, "
        f"countIf(NOT ({neutral})) != 0, 0, 1)) AS predicate_context "
        f"FROM system.settings WHERE name IN ({names})"
    )


def _predicate_ast_is_neutral(result: Any) -> bool:
    if not isinstance(result, bytes):
        raise DiscoveryError("malformed predicate context AST response")
    try:
        lines = result.decode("utf-8").splitlines()
    except UnicodeDecodeError:
        # A non-text default output format cannot establish the known AST.
        return False
    # Compare only the fixed constant probe, never user SQL or source-table ASTs.
    # Unknown server formatting disables optimization instead of widening trust.
    return tuple(line.strip() for line in lines) == _NEUTRAL_PREDICATE_AST


def discover_predicate_context(
    connection: ClickHouseConnection, limits: ResourceLimits
) -> bool:
    """Reject optimization under effective result truncation or implicit rewrites."""
    if not _explicit_predicate_settings_are_neutral(connection):
        return False
    client = _open(connection, limits)
    failed = False
    try:
        settings, transport = _query_context(connection, limits, "predicate-context")
        result = client.query(
            _predicate_context_query(),
            settings=settings,
            transport_settings=transport,
        )
        rows = result.result_rows
        if tuple(result.column_names) != ("predicate_context",):
            raise DiscoveryError("malformed predicate context metadata")
        # An inherited offset/filter can suppress even this single aggregate row.
        if not rows:
            return False
        if (
            len(rows) != 1
            or len(rows[0]) != 1
            or type(rows[0][0]) is not int
            or rows[0][0] not in (0, 1)
        ):
            raise DiscoveryError("malformed predicate context metadata")
        if rows[0][0] == 0:
            return False
        # ClickHouse can materialize inherited limit/offset into a SELECT wrapper
        # and clear their context values before system.settings is read. EXPLAIN
        # preserves that wrapper without executing or optimizing a source query.
        settings, transport = _query_context(connection, limits, "predicate-shape")
        return _predicate_ast_is_neutral(
            # query() appends FORMAT Native, which EXPLAIN AST consumes as part
            # of its inner query. raw_query() preserves this fixed probe verbatim.
            client.raw_query(
                "EXPLAIN AST SELECT 1",
                fmt=None,
                settings=settings,
                transport_settings=transport,
            )
        )
    except RayClickHouseError:
        failed = True
        raise
    except BaseException as exc:
        failed = True
        if isinstance(exc, (KeyboardInterrupt, SystemExit, GeneratorExit)):
            raise
        raise _translate(exc, operation="predicate context discovery") from None
    finally:
        try:
            client.close()
        except Exception:
            if not failed:
                raise TransportError(
                    "failed to close predicate context client"
                ) from None


def _is_integer_range_column(column: TargetColumn) -> bool:
    parsed = parse_type(column.declared_type)
    while parsed.name in {"Nullable", "LowCardinality"}:
        parsed = parse_type(parsed.arguments[0])
    return parsed.name in {
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
    }


def select_auto_range_column(
    sorting_key: str, columns: tuple[TargetColumn, ...]
) -> TargetColumn:
    """Accept only a simple sorting-key list with a physical integer first column."""
    key = sorting_key.strip()
    if key.startswith("tuple(") and key.endswith(")"):
        key = key[6:-1].strip()
    if not _SORTING_COLUMNS.fullmatch(key):
        raise DiscoveryError("automatic range splitting requires a simple sorting key")
    names = tuple(part.strip().strip("`") for part in key.split(","))
    by_name = {column.name: column for column in columns}
    if any(name not in by_name for name in names):
        raise DiscoveryError("sorting key contains a column absent from the schema")
    column = by_name[names[0]]
    if column.default_kind == "ALIAS" or not _is_integer_range_column(column):
        raise DiscoveryError(
            "automatic range splitting requires a physical integer first sorting column"
        )
    return column


@dataclass(frozen=True)
class PartitionInfo:
    partition_id: str
    rows: int | None
    marks: int | None
    bytes_on_disk: int | None


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


def discover_query_schema(
    connection: ClickHouseConnection,
    query_sql: str,
    limits: ResourceLimits,
    *,
    parameters: Mapping[str, Any],
) -> SchemaPlan:
    """Align query-result declarations with a zero-row Arrow probe."""
    client = None
    failed = False
    try:
        client = _open(connection, limits)
        settings = connection.query_settings(limits)
        settings["readonly"] = 1
        settings["query_id"] = _new_query_id("query-describe")
        described = client.query(
            f"DESCRIBE (\n{query_sql}\n)",
            parameters=dict(parameters),
            settings=settings,
        )
        columns = parse_describe_rows(described.result_rows)
        try:
            for column in columns:
                validate_identifier(column.name, name="query result alias")
        except ConfigurationError:
            raise SchemaError(
                "query results require unique simple column aliases"
            ) from None
        projection = render_read_projection(columns)
        settings["query_id"] = _new_query_id("query-schema-probe")
        probe = client.query_arrow(
            f"SELECT {projection} FROM (\n{query_sql}\n) "
            "AS __ray_clickhouse_query LIMIT 0",
            parameters=dict(parameters),
            settings=settings,
            use_strings=True,
        )
        if not isinstance(probe, pa.Table):
            raise SchemaError("ClickHouse returned a non-Arrow query schema probe")
        plan = canonical_schema(columns, probe.schema)
        for column, arrow_field in zip(columns, probe.schema, strict=True):
            _, nullable = _unwrap(parse_type(column.declared_type))
            if (
                not arrow_compatible(arrow_field.type, column.declared_type)
                or arrow_field.nullable != nullable
            ):
                raise SchemaError(
                    "query declaration and Arrow types or nullability disagree"
                )
        return plan
    except RayClickHouseError:
        failed = True
        raise
    except BaseException as exc:
        failed = True
        if isinstance(exc, (KeyboardInterrupt, SystemExit, GeneratorExit)):
            raise
        raise _translate(exc, operation="schema discovery") from None
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                if not failed:
                    raise TransportError(
                        "failed to close query discovery client"
                    ) from None


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


def discover_sorting_key(
    connection: ClickHouseConnection, table: QualifiedTable, limits: ResourceLimits
) -> str:
    """Read sorting metadata without interpreting expressions or following engines."""
    client = None
    try:
        client = _open(connection, limits)
        settings, transport_settings = _query_context(
            connection, limits, "sorting-key-discovery"
        )
        result = client.query(
            "SELECT sorting_key FROM system.tables "
            "WHERE database = %(database)s AND name = %(table)s",
            parameters={"database": table.database, "table": table.table},
            settings=settings,
            transport_settings=transport_settings,
        )
        if not result.result_rows:
            raise ObjectNotFoundError(f"ClickHouse table {table} was not found")
        if (
            len(result.result_rows) != 1
            or len(result.result_rows[0]) != 1
            or not isinstance(result.result_rows[0][0], str)
        ):
            raise DiscoveryError("system.tables returned malformed sorting metadata")
        return result.result_rows[0][0]
    except RayClickHouseError:
        raise
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit, GeneratorExit)):
            raise
        raise _translate(exc, operation="sorting-key discovery") from None
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
    pushed_predicate: CompiledPredicate | None = None,
) -> RangeFacts:
    if not _is_integer_range_column(column):
        raise DiscoveryError("range split currently supports integer columns only")
    client = None
    try:
        client = _open(connection, limits)
        base_sql, bound = build_select(
            table=table,
            columns=(column.name,),
            filter_sql=validate_filter(filter_sql),
            parameters=parameters,
            pushed_predicate=pushed_predicate,
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
        columns = parse_describe_rows(result.result_rows, position_index=4)
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
