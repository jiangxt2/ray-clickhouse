"""Capability-based discovery for opt-in, static Distributed local scans.

Only NotApplicable permits a planning fallback. Metadata errors and real
connection failures retain the public error hierarchy.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from typing import Any
from uuid import UUID

import pyarrow as pa

from ray_clickhouse._discovery import (
    LOCAL_MERGETREE_ENGINES,
    PartitionInfo,
    _open,
    _query_context,
    _translate,
)
from ray_clickhouse._errors import (
    ConfigurationError,
    DiscoveryError,
    RayClickHouseError,
    SchemaError,
    TransportError,
)
from ray_clickhouse._models import (
    ClickHouseConnection,
    QualifiedTable,
    ResourceLimits,
)
from ray_clickhouse._schema import (
    SchemaPlan,
    TargetColumn,
    canonical_schema,
    render_read_projection,
    type_declaration_key,
)


class NotApplicable(Exception):
    """A known, valid configuration outside the local-read profile."""


# Resource settings do not change row membership or scalar comparison semantics.
SAFE_SETTINGS = frozenset(
    {
        "max_threads",
        "max_memory_usage",
        "max_bytes_before_external_sort",
        "max_bytes_before_external_group_by",
        "max_execution_time",
        "readonly",
        "log_queries",
        "log_query_threads",
        "log_comment",
        "send_timeout",
        "receive_timeout",
        "connect_timeout",
        "max_block_size",
    }
)
# Compare effective values, including defaults. Missing any required control is
# a capability gap, never permission to assume a value from a version string.
CONTEXT_DEFAULTS = {
    "final": "0",
    "additional_result_filter": "",
    "additional_table_filters": "{}",
    "max_result_rows": "0",
    "max_result_bytes": "0",
    "read_overflow_mode": "throw",
    "timeout_overflow_mode": "throw",
    "group_by_overflow_mode": "throw",
    "sort_overflow_mode": "throw",
    "select_sequential_consistency": "0",
    "skip_unavailable_shards": "0",
}
CONTEXT_COMPARE = frozenset({"join_use_nulls", "transform_null_in", "timezone"})
METADATA_TABLES = frozenset(
    {"columns", "tables", "clusters", "settings", "row_policies", "parts"}
)
SAFE_CLIENT_OPTIONS = frozenset(
    {"verify", "ca_cert", "client_cert", "client_cert_key", "compress"}
)


@dataclass(frozen=True)
class MetadataCapabilities:
    fields: tuple[tuple[str, tuple[tuple[str, str], ...]], ...]
    origins: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class LocalTarget:
    shard: int
    replica: int
    connection: ClickHouseConnection
    table: QualifiedTable
    schema: SchemaPlan
    capabilities: MetadataCapabilities
    partitions: tuple[PartitionInfo, ...] | None
    sorting_key: str | None


@dataclass(frozen=True)
class ResolvedDistributed:
    targets: tuple[LocalTarget, ...]
    capabilities: MetadataCapabilities


def _column_definition(
    column: TargetColumn,
) -> tuple[str, tuple[str, ...], str, str]:
    return (
        column.name,
        type_declaration_key(column.declared_type),
        column.default_kind,
        column.default_expression,
    )


_LEX = re.compile(
    r"\s+|(?P<string>'(?:[^'\\]|\\.|'')*')|"
    r'(?P<quoted>`(?:[^`\\]|\\.|``)*`|"(?:[^"\\]|\\.|"")*")|'
    r"(?P<parameter>%\([A-Za-z_][A-Za-z0-9_]*\)s|"
    r"\{[A-Za-z_][A-Za-z0-9_]*\s*:[^{}]+\})|"
    r"(?P<number>[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)|"
    r"(?P<word>[A-Za-z_][A-Za-z0-9_]*)|(?P<symbol><=|>=|!=|<>|==|[(),=<>])"
)


def _tokens(text: str) -> tuple[tuple[str, str], ...]:
    result = []
    offset = 0
    while offset < len(text):
        match = _LEX.match(text, offset)
        if match is None:
            raise NotApplicable("unsupported expression syntax")
        if match.lastgroup:
            result.append((match.lastgroup, match.group()))
        offset = match.end()
    return tuple(result)


def parse_distributed_engine(text: str) -> tuple[str, QualifiedTable]:
    """Extract three constant target arguments, ignoring the sharding expression.

    The same scanner handles engine_full and the ENGINE fragment of SHOW CREATE.
    Parentheses inside quoted text never affect argument boundaries.
    """
    quote: str | None = None
    depth = 0
    start: int | None = None
    args: list[str] = []
    begin = 0
    i = 0
    while i < len(text):
        char = text[i]
        if quote:
            if char == "\\":
                i += 2
                continue
            if char == quote:
                if i + 1 < len(text) and text[i + 1] == quote:
                    i += 2
                    continue
                quote = None
        elif char in "'`\"":
            quote = char
        elif start is None:
            match = re.match(r"\bDistributed\s*\(", text[i:])
            if match:
                start = i
                depth = 1
                i += match.end()
                begin = i
                continue
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                args.append(text[begin:i].strip())
                break
        elif char == "," and depth == 1:
            args.append(text[begin:i].strip())
            begin = i + 1
        i += 1
    if quote or (start is not None and depth):
        raise DiscoveryError("malformed Distributed engine metadata")
    if start is None or len(args) < 3:
        raise NotApplicable("Distributed target is not an explicit constant triple")
    constants = []
    for argument in args[:3]:
        if not re.fullmatch(
            r"(?:[A-Za-z_][A-Za-z0-9_]*|'[^'\\]*'|`[^`\\]*`|\"[^\"\\]*\")", argument
        ):
            raise NotApplicable("Distributed target is not a supported constant")
        value = argument.strip("'`\"")
        if "{" in value or "}" in value:
            raise NotApplicable("unresolved Distributed target macro")
        constants.append(value)
    try:
        target = QualifiedTable(constants[1], constants[2])
    except ConfigurationError:
        raise NotApplicable(
            "target identifiers are outside the simple-name profile"
        ) from None
    return constants[0], target


def predicate_columns(text: str, columns: tuple[TargetColumn, ...]) -> frozenset[str]:
    """Recognize scalar comparisons and Boolean composition without rewriting SQL."""
    tokens = _tokens(text)
    by_name = {column.name: column for column in columns}
    used: set[str] = set()
    index = 0

    def take(value: str) -> bool:
        nonlocal index
        if index < len(tokens) and tokens[index][1].upper() == value:
            index += 1
            return True
        return False

    def atom() -> None:
        nonlocal index
        if index == len(tokens):
            raise NotApplicable("incomplete predicate")
        kind, value = tokens[index]
        index += 1
        if kind == "parameter":
            if value.startswith("{") and re.search(r"\bIdentifier\b", value):
                raise NotApplicable(
                    "identifier bindings require logical-source reading"
                )
            return
        if kind in {"string", "number"}:
            return
        if kind == "word" and value.upper() in {"NULL", "TRUE", "FALSE"}:
            return
        if kind in {"word", "quoted"}:
            name = value.strip('`"')
            column = by_name.get(name)
            if column is not None and column.default_kind != "ALIAS":
                if not re.search(
                    r"\b(?:Array|Tuple|Map|Nested|JSON|Object)\b", column.declared_type
                ):
                    used.add(name)
                    return
        raise NotApplicable("predicate requires a physical scalar column")

    def term() -> None:
        if take("NOT"):
            term()
        elif take("("):
            expression()
            if not take(")"):
                raise NotApplicable("unsupported predicate parentheses")
        else:
            atom()
            if take("IS"):
                take("NOT")
                if not take("NULL"):
                    raise NotApplicable("unsupported IS predicate")
            elif any(take(op) for op in ("=", "==", "!=", "<>", "<=", ">=", "<", ">")):
                atom()
            else:
                raise NotApplicable("predicate requires a comparison")

    def conjunction() -> None:
        term()
        while take("AND"):
            term()

    def expression() -> None:
        conjunction()
        while take("OR"):
            conjunction()

    try:
        expression()
    except RecursionError:
        raise NotApplicable("predicate nesting exceeds the supported profile") from None
    if index != len(tokens) or not used:
        raise NotApplicable("predicate is outside the comparison profile")
    return frozenset(used)


def _rows(result: Any) -> tuple[dict[str, Any], ...]:
    names = tuple(result.column_names)
    if not names and not result.result_rows:
        # Empty Native results may omit the header. Callers independently prove
        # metadata visibility and distinguish valid emptiness from missing data.
        return ()
    if not names or len(set(names)) != len(names):
        raise DiscoveryError("metadata returned invalid column names")
    rows = []
    for row in result.result_rows:
        if len(row) != len(names):
            raise DiscoveryError("metadata row width disagrees with column names")
        rows.append(dict(zip(names, row, strict=True)))
    return tuple(rows)


class MetadataReader:
    """One driver-owned client, with fields and selected origins cached per node."""

    def __init__(
        self, client: Any, connection: ClickHouseConnection, limits: ResourceLimits
    ):
        self.client = client
        self.connection = connection
        self.limits = limits
        self.fields: dict[str, dict[str, str]] = {}
        self.origins: dict[str, str] = {}
        self.descriptions: dict[QualifiedTable, tuple[dict[str, Any], ...]] = {}
        self.policy_inventory: tuple[tuple[UUID, str, str], ...] | None = None

    def query(
        self,
        sql: str,
        parameters: Mapping[str, Any] | None = None,
        *,
        allow_missing: bool = False,
    ) -> tuple[dict[str, Any], ...]:
        settings, transport = _query_context(
            self.connection, self.limits, "local-discovery"
        )
        try:
            return _rows(
                self.client.query(
                    sql,
                    parameters=dict(parameters or {}),
                    settings=settings,
                    transport_settings=transport,
                )
            )
        except RayClickHouseError:
            raise
        except Exception as exc:
            if (
                sql == "SHOW CREATE ROW POLICIES ON *.*"
                and getattr(exc, "code", None) == 62
            ):
                raise NotApplicable(
                    "independent policy enumeration is unavailable"
                ) from None
            if allow_missing and getattr(exc, "code", None) in {47, 60}:
                raise NotApplicable("system metadata source is unavailable") from None
            raise _translate(exc, operation="local metadata discovery") from None

    def capability(self, table: str) -> dict[str, str]:
        if table not in self.fields:
            try:
                described = self.describe(QualifiedTable("system", table))
            except NotApplicable:
                self.fields[table] = {}
                return {}
            if table == "columns":
                try:
                    rows = self.query(
                        "SELECT name, type FROM system.columns "
                        "WHERE database = 'system' AND table = 'columns'",
                        allow_missing=True,
                    )
                except NotApplicable:
                    rows = described
                    self.origins["system-fields"] = "describe"
            else:
                column_fields = self.capability("columns")
                if {"name", "type", "database", "table"} <= column_fields.keys():
                    rows = self.query(
                        "SELECT name, type FROM system.columns "
                        "WHERE database = 'system' AND table = %(table)s",
                        {"table": table},
                    )
                else:
                    rows = described
            fields: dict[str, str] = {}
            for row in rows:
                name, declared = row.get("name"), row.get("type")
                if (
                    not isinstance(name, str)
                    or not isinstance(declared, str)
                    or name in fields
                ):
                    raise DiscoveryError("malformed system-table field capability")
                fields[name] = declared
            if not fields:
                exists = self.query(f"EXISTS TABLE system.{table}")
                if len(exists) == 1 and tuple(exists[0].values()) == (0,):
                    raise NotApplicable("required system table is unavailable")
                raise DiscoveryError("system-table metadata is absent or invisible")
            if fields.keys() != {row["name"] for row in described}:
                raise DiscoveryError("system-table fields disagree with full DESCRIBE")
            self.fields[table] = fields
        return self.fields[table]

    def require_fields(self, table: str, expected: Mapping[str, str]) -> None:
        fields = self.capability(table)
        for name, kind in expected.items():
            if name not in fields:
                raise NotApplicable("required metadata field is unavailable")
            declared = fields[name]
            description = self.descriptions.get(QualifiedTable("system", table))
            if description is not None and declared != next(
                row["type"] for row in description if row["name"] == name
            ):
                raise DiscoveryError("required metadata declarations disagree")
            valid = (
                declared == "String"
                if kind == "str"
                else declared == "UUID"
                if kind == "uuid"
                else bool(re.fullmatch(r"U?Int(?:8|16|32|64)", declared))
            )
            if not valid:
                raise DiscoveryError("metadata field has an unexpected declaration")

    def describe(self, table: QualifiedTable) -> tuple[dict[str, Any], ...]:
        # Native DESCRIBE checks whole-table SHOW COLUMNS, including active
        # roles. system.columns alone can expose only individually granted columns.
        if table not in self.descriptions:
            rows = self.query(
                f"DESCRIBE TABLE {table.sql()}",
                allow_missing=table.database == "system",
            )
            names = [row.get("name") for row in rows]
            if (
                not rows
                or any(
                    not isinstance(row.get("name"), str)
                    or not isinstance(row.get("type"), str)
                    for row in rows
                )
                or len(set(names)) != len(names)
            ):
                raise DiscoveryError("malformed full-table description")
            self.descriptions[table] = rows
        return self.descriptions[table]

    def policies(self) -> tuple[tuple[UUID, str, str], ...]:
        if self.policy_inventory is None:
            self.require_fields(
                "row_policies", {"id": "uuid", "database": "str", "table": "str"}
            )
            # Native AccessControl enumeration checks SHOW ROW POLICIES itself.
            # Its SQL text is neither parsed nor retained; only cardinality is used.
            count = len(self.query("SHOW CREATE ROW POLICIES ON *.*"))
            rows = self.query("SELECT id, database, table FROM system.row_policies")
            inventory = []
            for row in rows:
                identifier, database, table = (
                    row.get("id"),
                    row.get("database"),
                    row.get("table"),
                )
                if (
                    not isinstance(identifier, UUID)
                    or not isinstance(database, str)
                    or not isinstance(table, str)
                ):
                    raise DiscoveryError("malformed structured row policy")
                inventory.append((identifier, database, table))
            if count != len(inventory) or count != len({row[0] for row in inventory}):
                raise DiscoveryError(
                    "row-policy inventory is incomplete or inconsistent"
                )
            self.policy_inventory = tuple(inventory)
            self.origins["policy-inventory"] = "system.row_policies+native-cardinality"
        return self.policy_inventory

    def engine(self, table: QualifiedTable) -> tuple[str, str]:
        fields = self.capability("tables")
        self.require_fields(
            "tables", {"engine": "str", "database": "str", "name": "str"}
        )
        candidates = ["engine"]
        origin = "show-create"
        for field in ("engine_full", "create_table_query"):
            if field in fields:
                self.require_fields("tables", {field: "str"})
                candidates.append(field)
                origin = field
                break
        rows = self.query(
            "SELECT "
            + ", ".join(candidates)
            + " FROM system.tables WHERE database = %(database)s AND name = %(table)s",
            {"database": table.database, "table": table.table},
        )
        if not rows:
            # A filtered catalog is not proof of absence. Native DESCRIBE
            # distinguishes denied visibility and actual missing objects.
            self.describe(table)
            raise DiscoveryError("engine metadata is absent despite native visibility")
        if (
            len(rows) != 1
            or any(
                not isinstance(value, str) or not value for value in rows[0].values()
            )
            or not set(candidates) <= rows[0].keys()
        ):
            raise DiscoveryError("malformed table engine metadata")
        engine = rows[0]["engine"]
        if len(candidates) == 2:
            definition = rows[0][candidates[1]]
        else:
            created = self.query(f"SHOW CREATE TABLE {table.sql()}")
            if len(created) != 1 or len(created[0]) != 1:
                raise DiscoveryError("malformed SHOW CREATE result")
            definition = next(iter(created[0].values()))
            if not isinstance(definition, str) or not definition:
                raise DiscoveryError("invalid SHOW CREATE text")
        self.origins[f"engine:{table}"] = origin
        return engine, definition

    def columns(self, table: QualifiedTable) -> tuple[TargetColumn, ...]:
        described = self.describe(table)
        fields = self.capability("columns")
        required = {
            "name": "str",
            "type": "str",
            "position": "int",
            "default_kind": "str",
            "default_expression": "str",
            "database": "str",
            "table": "str",
        }
        if required.keys() <= fields.keys():
            self.require_fields("columns", required)
            rows = self.query(
                "SELECT name, type, position, default_kind, default_expression FROM "
                "system.columns WHERE database = %(database)s AND table = %(table)s "
                "ORDER BY position",
                {"database": table.database, "table": table.table},
            )
            origin = "system.columns"
        else:
            rows = described
            rows = tuple(
                dict(row, position=i + 1, default_kind=row.get("default_type"))
                for i, row in enumerate(rows)
            )
            origin = "describe"
        columns = []
        positions: set[int] = set()
        names: set[str] = set()
        for row in rows:
            values = [
                row.get(key)
                for key in ("name", "type", "default_kind", "default_expression")
            ]
            position = row.get("position")
            if (
                not all(isinstance(value, str) for value in values)
                or not isinstance(position, int)
                or isinstance(position, bool)
                or position < 1
                or position in positions
                or values[0] in names
            ):
                raise DiscoveryError("malformed column definition metadata")
            positions.add(position)
            name, declared, kind, expression = (str(value) for value in values)
            names.add(name)
            columns.append(TargetColumn(name, declared, kind, expression, position))
        if not columns:
            raise DiscoveryError("column metadata is absent or invisible")
        expected = tuple(
            (
                row.get("name"),
                type_declaration_key(row["type"]),
                row.get("default_type"),
                row.get("default_expression"),
            )
            for row in described
        )
        if (
            tuple(
                _column_definition(c) for c in sorted(columns, key=lambda c: c.position)
            )
            != expected
        ):
            raise DiscoveryError("column metadata disagrees with full DESCRIBE")
        self.origins[f"columns:{table}"] = origin
        return tuple(sorted(columns, key=lambda column: column.position))

    def schema(
        self, table: QualifiedTable, columns: tuple[TargetColumn, ...]
    ) -> SchemaPlan:
        settings, transport = _query_context(
            self.connection, self.limits, "local-schema-probe"
        )
        try:
            probe = self.client.query_arrow(
                f"SELECT {render_read_projection(columns)} FROM {table.sql()} LIMIT 0",
                settings=settings,
                transport_settings=transport,
                use_strings=True,
            )
        except Exception as exc:
            raise _translate(exc, operation="schema discovery") from None
        if not isinstance(probe, pa.Table):
            raise SchemaError("local schema probe returned a non-Arrow result")
        return canonical_schema(columns, probe.schema)

    def context(self, table: QualifiedTable) -> tuple[tuple[str, str], ...]:
        self.require_fields("settings", {"name": "str", "value": "str"})
        rows = self.query("SELECT name, value FROM system.settings")
        if any(not {"name", "value"} <= row.keys() for row in rows):
            raise DiscoveryError("effective settings omit required result columns")
        effective = {row["name"]: row["value"] for row in rows}
        if any(
            not isinstance(name, str) or not isinstance(value, str)
            for name, value in effective.items()
        ) or len(effective) != len(rows):
            raise DiscoveryError("malformed effective settings")
        if not CONTEXT_DEFAULTS.keys() <= effective.keys():
            raise NotApplicable("required effective setting is unavailable")
        if any(effective[name] != value for name, value in CONTEXT_DEFAULTS.items()):
            raise NotApplicable(
                "result or replica settings require logical-source reading"
            )
        parallel_setting = next(
            (
                name
                for name in (
                    "enable_parallel_replicas",
                    "allow_experimental_parallel_reading_from_replicas",
                )
                if name in effective
            ),
            None,
        )
        if parallel_setting is None or effective[parallel_setting] != "0":
            raise NotApplicable("parallel replica control is unavailable or enabled")
        if (
            "enable_parallel_replicas" not in effective
            and effective.get("max_parallel_replicas") != "1"
        ):
            raise NotApplicable(
                "legacy parallel replica routing requires logical reading"
            )
        # Only values known on both nodes participate. Optional settings must be
        # equally present as well: asymmetric capability cannot establish equality.
        for name in (
            "enable_parallel_replicas",
            "allow_experimental_parallel_reading_from_replicas",
            "automatic_parallel_replicas_mode",
        ):
            if name in effective and effective[name] != "0":
                raise NotApplicable("parallel replica routing requires logical reading")
        self.origins["parallel-replica-control"] = parallel_setting
        names = CONTEXT_DEFAULTS.keys() | CONTEXT_COMPARE
        values = tuple(
            sorted((name, effective.get(name, "<unavailable>")) for name in names)
        )
        identity = self.query("SELECT currentUser() AS user, timezone() AS timezone")
        if (
            len(identity) != 1
            or not {"user", "timezone"} <= identity[0].keys()
            or any(not isinstance(value, str) for value in identity[0].values())
        ):
            raise DiscoveryError("invalid effective identity or timezone")
        if identity[0].get("user") != self.connection.username:
            raise NotApplicable("effective identity differs from configured identity")
        # Verify neutral result settings before using inventory cardinality.
        # Static ACL/policy configuration during planning is a supported-profile
        # assumption; there is no cross-query security metadata snapshot.
        policies = self.policies()
        if any(
            database in {"", "*", "system"}
            and (name in {"", "*"} or name in METADATA_TABLES)
            for _, database, name in policies
        ):
            raise NotApplicable(
                "row policies on discovery metadata require logical reading"
            )
        if any(
            database in {"", "*", table.database} and name in {"", "*", table.table}
            for _, database, name in policies
        ):
            raise NotApplicable("related row policies require logical-source reading")
        return values + (("server_timezone", identity[0]["timezone"]),)

    def partitions(
        self, table: QualifiedTable, columns: tuple[TargetColumn, ...]
    ) -> tuple[PartitionInfo, ...] | None:
        try:
            fields = self.capability("parts")
        except NotApplicable:
            return None
        if not {"partition_id", "active", "database", "table"} <= fields.keys() or any(
            column.name == "_partition_id" for column in columns
        ):
            return None
        self.require_fields(
            "parts",
            {"partition_id": "str", "active": "int", "database": "str", "table": "str"},
        )
        weight = next(
            (name for name in ("bytes_on_disk", "rows") if name in fields), None
        )
        if weight:
            self.require_fields("parts", {weight: "int"})
        projection = f"sum({weight}) AS weight" if weight else "toUInt64(1) AS weight"
        rows = self.query(
            f"SELECT partition_id, {projection} FROM system.parts "
            "WHERE active AND database = %(database)s AND table = %(table)s "
            "GROUP BY partition_id ORDER BY partition_id",
            {"database": table.database, "table": table.table},
        )
        result = []
        for row in rows:
            if (
                not {"partition_id", "weight"} <= row.keys()
                or not isinstance(row["partition_id"], str)
                or not row["partition_id"]
                or not isinstance(row["weight"], int)
                or row["weight"] < 0
            ):
                raise DiscoveryError("malformed local partition metadata")
            result.append(
                PartitionInfo(
                    row["partition_id"],
                    row["weight"] if weight == "rows" else None,
                    None,
                    row["weight"] if weight == "bytes_on_disk" else None,
                )
            )
        self.origins[f"partition-weight:{table}"] = weight or "equal"
        return tuple(result)

    def snapshot(self) -> MetadataCapabilities:
        return MetadataCapabilities(
            tuple(
                sorted(
                    (name, tuple(sorted(fields.items())))
                    for name, fields in self.fields.items()
                )
            ),
            tuple(sorted(self.origins.items())),
        )


@contextmanager
def metadata_reader(
    connection: ClickHouseConnection, limits: ResourceLimits
) -> Iterator[MetadataReader]:
    client = _open(connection, limits)
    failed = False
    try:
        yield MetadataReader(client, connection, limits)
    except BaseException:
        failed = True
        raise
    finally:
        try:
            client.close()
        except Exception:
            if not failed:
                raise TransportError("failed to close local discovery client") from None


def resolve_distributed(
    connection: ClickHouseConnection,
    table: QualifiedTable,
    schema: SchemaPlan,
    limits: ResourceLimits,
    *,
    filter_sql: str,
    split: str,
) -> ResolvedDistributed:
    predicate_columns(filter_sql, schema.columns)
    if any(column.default_kind == "ALIAS" for column in schema.columns):
        raise NotApplicable("computed aliases require logical-source reading")
    if set(dict(connection.settings)).difference(SAFE_SETTINGS):
        raise NotApplicable("query settings are outside the local-read allowlist")
    if set(dict(connection.client_options)).difference(SAFE_CLIENT_OPTIONS):
        raise NotApplicable("client options cannot be safely reused across hosts")
    if dict(connection.client_options).get("verify") is False:
        raise NotApplicable("local HTTPS routing requires certificate verification")
    with metadata_reader(connection, limits) as source:
        engine, definition = source.engine(table)
        if engine != "Distributed":
            raise DiscoveryError("source engine changed during local-read planning")
        cluster, target_table = parse_distributed_engine(definition)
        context = source.context(table)
        source_columns = source.columns(table)
        if tuple((_column_definition(c), c.position) for c in source_columns) != tuple(
            (_column_definition(c), c.position) for c in schema.columns
        ):
            raise SchemaError("source declarations changed during local-read planning")
        fields = source.capability("clusters")
        cluster_field = "cluster" if "cluster" in fields else "name"
        source.require_fields(
            "clusters",
            {
                cluster_field: "str",
                "shard_num": "int",
                "replica_num": "int",
                "host_name": "str",
                "user": "str",
            },
        )
        topology = source.query(
            "SELECT shard_num, replica_num, host_name, user FROM system.clusters "
            f"WHERE `{cluster_field}` = %(cluster)s",
            {"cluster": cluster},
        )
        if not topology:
            raise DiscoveryError("Distributed cluster metadata is absent or invisible")
        replicas: dict[int, list[tuple[int, str]]] = {}
        seen: set[tuple[int, int]] = set()
        for row in topology:
            if not {"shard_num", "replica_num", "host_name", "user"} <= row.keys():
                raise DiscoveryError("topology omits required result columns")
            shard, replica, host = (
                row["shard_num"],
                row["replica_num"],
                row["host_name"],
            )
            if (
                not isinstance(shard, int)
                or not isinstance(replica, int)
                or isinstance(shard, bool)
                or isinstance(replica, bool)
                or shard < 1
                or replica < 1
                or not isinstance(host, str)
                or not re.fullmatch(r"[A-Za-z0-9_.:-]+", host)
                or (shard, replica) in seen
            ):
                raise DiscoveryError("malformed shard or replica topology")
            if row["user"] != connection.username:
                raise NotApplicable(
                    "cluster internal identity differs from direct identity"
                )
            seen.add((shard, replica))
            replicas.setdefault(shard, []).append((replica, host))
        source.origins["cluster"] = cluster_field
        targets = []
        selected_hosts: set[str] = set()
        for shard, options in sorted(replicas.items()):
            replica, host = min(options)
            if host in selected_hosts:
                raise DiscoveryError(
                    "distinct shards resolve to the same local endpoint"
                )
            selected_hosts.add(host)
            direct = replace(connection, host=host, database=target_table.database)
            with metadata_reader(direct, limits) as target:
                if target.context(target_table) != context:
                    raise NotApplicable(
                        "effective settings differ across selected nodes"
                    )
                target.describe(target_table)
                target_engine, _ = target.engine(target_table)
                if target_engine not in LOCAL_MERGETREE_ENGINES:
                    raise NotApplicable(
                        "physical target is not a supported local MergeTree"
                    )
                columns = target.columns(target_table)
                source_by_name = {c.name: _column_definition(c) for c in source_columns}
                target_by_name = {c.name: _column_definition(c) for c in columns}
                if source_by_name != target_by_name:
                    raise SchemaError(
                        "logical and physical column declarations disagree"
                    )
                target_schema = target.schema(target_table, columns)
                for name in schema.arrow_schema.names:
                    if target_schema.arrow_schema.field(
                        name
                    ) != schema.arrow_schema.field(name):
                        raise SchemaError("logical and physical Arrow schemas disagree")
                partitions = (
                    target.partitions(target_table, columns)
                    if split == "partition"
                    else None
                )
                sorting_key = None
                if split == "auto" and "sorting_key" in target.capability("tables"):
                    target.require_fields("tables", {"sorting_key": "str"})
                    rows = target.query(
                        "SELECT sorting_key FROM system.tables "
                        "WHERE database = %(database)s "
                        "AND name = %(table)s",
                        {
                            "database": target_table.database,
                            "table": target_table.table,
                        },
                    )
                    if len(rows) != 1 or not isinstance(
                        rows[0].get("sorting_key"), str
                    ):
                        raise DiscoveryError("malformed local sorting key")
                    sorting_key = rows[0]["sorting_key"]
                targets.append(
                    LocalTarget(
                        shard,
                        replica,
                        direct,
                        target_table,
                        target_schema,
                        target.snapshot(),
                        partitions,
                        sorting_key,
                    )
                )
        return ResolvedDistributed(tuple(targets), source.snapshot())
