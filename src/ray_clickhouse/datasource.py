"""Ray Data Datasource for structured ClickHouse physical-table reads."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
from ray.data.block import BlockMetadata
from ray.data.datasource import Datasource, ReadTask

from ray_clickhouse._compat import ensure_supported_ray_version, make_read_task
from ray_clickhouse._discovery import (
    DiscoverySnapshot,
    discover_engine,
    discover_partitions,
    discover_range_facts,
    discover_schema,
)
from ray_clickhouse._errors import ConfigurationError, DiscoveryError, PermissionError
from ray_clickhouse._models import (
    ClickHouseConnection,
    DiscoveryPolicy,
    OrderBy,
    QualifiedTable,
    QuerySpec,
    ResourceLimits,
    SplitMode,
    validate_identifier,
)
from ray_clickhouse._planning import group_partitions, plan_integer_ranges
from ray_clickhouse._schema import render_read_projection
from ray_clickhouse._sql import build_select
from ray_clickhouse._transport import stream_query


def _is_mergetree_engine(engine: str) -> bool:
    return engine.endswith("MergeTree") and engine != "Distributed"


_SINGLE_QUERY_ENGINES = frozenset({"View", "MaterializedView", "Distributed"})


@dataclass(frozen=True, repr=False)
class ClickHouseReadConfig:
    connection: ClickHouseConnection
    table: QualifiedTable
    columns: tuple[str, ...] | None = None
    filter_sql: str | None = None
    query_parameters: tuple[tuple[str, Any], ...] = ()
    order_by: OrderBy | None = None
    split: SplitMode = "single"
    range_column: str | None = None
    discovery_policy: DiscoveryPolicy = "single"
    target_tasks: int = 8
    max_tasks: int = 256
    limits: ResourceLimits = field(default_factory=ResourceLimits)

    def __post_init__(self) -> None:
        if self.split not in {"single", "partition", "range"}:
            raise ConfigurationError("split must be 'single', 'partition', or 'range'")
        if self.discovery_policy not in {"single", "error"}:
            raise ConfigurationError("discovery_policy must be 'single' or 'error'")
        if self.split == "range" and self.range_column is None:
            raise ConfigurationError("split='range' requires range_column")
        if self.order_by is not None and self.split != "single":
            raise ConfigurationError("order_by requires split='single'")
        if self.range_column is not None:
            validate_identifier(self.range_column, name="range_column")
        if not isinstance(self.target_tasks, int) or isinstance(
            self.target_tasks, bool
        ):
            raise ConfigurationError("target_tasks must be an integer")
        if not isinstance(self.max_tasks, int) or isinstance(self.max_tasks, bool):
            raise ConfigurationError("max_tasks must be an integer")
        if self.target_tasks < 1 or self.max_tasks < self.target_tasks:
            raise ConfigurationError("max_tasks must be >= target_tasks")

    def __repr__(self) -> str:
        parameter_names = tuple(name for name, _ in self.query_parameters)
        return (
            f"ClickHouseReadConfig(table={self.table!s}, columns={self.columns!r}, "
            f"split={self.split!r}, range_column={self.range_column!r}, "
            f"order_by={self.order_by!r}, "
            f"parameter_names={parameter_names!r})"
        )


class ClickHouseDatasource(Datasource):
    """Read one ClickHouse physical table through Ray's public Datasource API."""

    def __init__(self, config: ClickHouseReadConfig) -> None:
        ensure_supported_ray_version()
        super().__init__()
        self._config = config
        self._planning_snapshot: DiscoverySnapshot | None = None

    @property
    def config(self) -> ClickHouseReadConfig:
        return self._config

    @property
    def name(self) -> str:
        return f"ClickHouse:{self._config.table}"

    def estimate_inmemory_data_size(self) -> None:
        """Do not trigger an extra COUNT/size/sample query during Ray planning."""
        return None

    def _ensure_planning_snapshot(self) -> DiscoverySnapshot:
        if self._planning_snapshot is not None:
            return self._planning_snapshot
        config = self._config
        schema = discover_schema(config.connection, config.table, config.limits)
        engine = discover_engine(config.connection, config.table, config.limits)
        if not _is_mergetree_engine(engine) and not (
            config.split == "single" and engine in _SINGLE_QUERY_ENGINES
        ):
            raise DiscoveryError(
                f"ClickHouse engine {engine!r} is unsupported for "
                f"split={config.split!r}; "
                "use a direct table with split='single'"
            )
        partitions = ()
        range_facts = None
        if config.split == "partition":
            try:
                partitions = discover_partitions(
                    config.connection, config.table, config.limits
                )
            except PermissionError:
                raise
            except DiscoveryError:
                if config.discovery_policy == "error":
                    raise
        if config.split == "range":
            if config.range_column not in schema.arrow_schema.names:
                raise ConfigurationError(
                    "range_column is not present in the table schema"
                )
            range_definition = next(
                column
                for column in schema.columns
                if column.name == config.range_column
            )
            try:
                range_facts = discover_range_facts(
                    config.connection,
                    config.table,
                    config.limits,
                    column=range_definition,
                    filter_sql=config.filter_sql,
                    parameters=dict(config.query_parameters),
                )
            except PermissionError:
                raise
            except DiscoveryError:
                if config.discovery_policy == "error":
                    raise
        self._planning_snapshot = DiscoverySnapshot(
            schema=schema,
            engine=engine,
            partitions=partitions,
            range_facts=range_facts,
        )
        return self._planning_snapshot

    def _selected_columns(self, snapshot: DiscoverySnapshot) -> tuple[str, ...]:
        columns = self._config.columns or tuple(snapshot.schema.arrow_schema.names)
        unknown = set(columns).difference(snapshot.schema.arrow_schema.names)
        if unknown:
            raise ConfigurationError(f"unknown ClickHouse columns: {sorted(unknown)}")
        if self._config.order_by:
            order_columns = {column for column, _ in self._config.order_by}
            unknown_order = order_columns.difference(snapshot.schema.arrow_schema.names)
            if unknown_order:
                raise ConfigurationError(
                    f"unknown order_by columns: {sorted(unknown_order)}"
                )
        return columns

    @property
    def arrow_schema(self) -> pa.Schema:
        return self._ensure_planning_snapshot().schema.arrow_schema

    def _make_task(
        self,
        *,
        columns: tuple[str, ...],
        partition_ids: tuple[str, ...] | None = None,
        range_lower: int | None = None,
        range_upper: int | None = None,
        include_null: bool = False,
        per_task_row_limit: int | None,
    ) -> ReadTask:
        config = self._config
        snapshot = self._ensure_planning_snapshot()
        definitions_by_name = {
            column.name: column for column in snapshot.schema.columns
        }
        selected_definitions = tuple(definitions_by_name[name] for name in columns)
        sql, parameters = build_select(
            table=config.table,
            columns=columns,
            filter_sql=config.filter_sql,
            parameters=dict(config.query_parameters),
            partition_ids=partition_ids,
            range_column=config.range_column
            if range_lower is not None or range_upper is not None
            else None,
            range_lower=range_lower,
            range_upper=range_upper,
            range_include_null=include_null,
            projection=render_read_projection(selected_definitions),
            order_by=config.order_by,
        )
        query = QuerySpec(
            sql=sql,
            parameters=parameters,
            arrow_schema=pa.schema(
                [snapshot.schema.arrow_schema.field(column) for column in columns]
            ),
            operation="read",
        )

        def read_fn() -> Any:
            yield from stream_query(config.connection, query, config.limits)

        return make_read_task(
            read_fn,
            BlockMetadata(
                num_rows=None, size_bytes=None, exec_stats=None, input_files=None
            ),
            query.arrow_schema,
            per_task_row_limit,
        )

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: int | None = None,
        data_context: Any | None = None,
    ) -> list[ReadTask]:
        del data_context
        snapshot = self._ensure_planning_snapshot()
        columns = self._selected_columns(snapshot)
        if self._config.split == "single":
            return [
                self._make_task(columns=columns, per_task_row_limit=per_task_row_limit)
            ]

        task_count = min(
            max(1, parallelism), self._config.target_tasks, self._config.max_tasks
        )
        if self._config.split == "partition" and snapshot.partitions:
            groups = group_partitions(
                snapshot.partitions,
                target_tasks=task_count,
                max_tasks=self._config.max_tasks,
            )
            return [
                self._make_task(
                    columns=columns,
                    partition_ids=group,
                    per_task_row_limit=per_task_row_limit,
                )
                for group in groups
            ] or [
                self._make_task(columns=columns, per_task_row_limit=per_task_row_limit)
            ]

        if self._config.split == "range" and snapshot.range_facts is not None:
            ranges = plan_integer_ranges(
                snapshot.range_facts,
                target_tasks=task_count,
                max_tasks=self._config.max_tasks,
            )
            tasks = []
            for lower, upper, include_null in ranges:
                tasks.append(
                    self._make_task(
                        columns=columns,
                        range_lower=lower,
                        range_upper=upper,
                        include_null=include_null,
                        per_task_row_limit=per_task_row_limit,
                    )
                )
            return tasks or [
                self._make_task(columns=columns, per_task_row_limit=per_task_row_limit)
            ]

        return [self._make_task(columns=columns, per_task_row_limit=per_task_row_limit)]
