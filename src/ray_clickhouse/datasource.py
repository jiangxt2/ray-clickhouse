"""Ray Data Datasource for structured ClickHouse physical-table reads."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator
from copy import copy
from dataclasses import dataclass, field, replace
from typing import Any, cast

import pyarrow as pa
from ray.data.block import BlockMetadata
from ray.data.datasource import Datasource, ReadTask

from ray_clickhouse._compat import (
    combine_predicates,
    decode_predicate,
    empty_projection,
    ensure_supported_ray_version,
    make_read_task,
    supports_predicate_pushdown,
    supports_v1_projection_pushdown,
)
from ray_clickhouse._discovery import (
    LOCAL_MERGETREE_ENGINES,
    DiscoverySnapshot,
    PartitionInfo,
    RangeFacts,
    discover_engine,
    discover_partitions,
    discover_predicate_context,
    discover_range_facts,
    discover_schema,
    discover_sorting_key,
    select_auto_range_column,
)
from ray_clickhouse._distributed import (
    NotApplicable,
    ResolvedDistributed,
    resolve_distributed,
)
from ray_clickhouse._errors import ConfigurationError, DiscoveryError, PermissionError
from ray_clickhouse._logging import (
    LogContext,
    current_context,
    emit,
    error_type,
    runtime_fields,
    source_context,
    use_context,
)
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
from ray_clickhouse._predicates import (
    CompiledPredicate,
    PredicateNode,
    compile_predicate,
)
from ray_clickhouse._schema import SchemaPlan, render_read_projection
from ray_clickhouse._sql import (
    build_select,
    effective_filter,
    predicate_binding_protocol,
    validate_filter_parameters,
)
from ray_clickhouse._transport import stream_query


def _is_mergetree_engine(engine: str) -> bool:
    return engine.endswith("MergeTree") and engine != "Distributed"


_SINGLE_QUERY_ENGINES = frozenset({"View", "Distributed"})
logger = logging.getLogger(__name__)


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
    diagnostic_flush_logs: bool = False
    convert_distributed_to_local: bool = False

    def __post_init__(self) -> None:
        validate_filter_parameters(self.filter_sql, dict(self.query_parameters))
        if self.split not in {"single", "partition", "range", "auto"}:
            raise ConfigurationError(
                "split must be 'single', 'partition', 'range', or 'auto'"
            )
        if self.discovery_policy not in {"single", "error"}:
            raise ConfigurationError("discovery_policy must be 'single' or 'error'")
        if self.split == "range" and self.range_column is None:
            raise ConfigurationError("split='range' requires range_column")
        if self.split == "auto" and self.range_column is not None:
            raise ConfigurationError("split='auto' does not accept range_column")
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
        if not isinstance(self.diagnostic_flush_logs, bool):
            raise ConfigurationError("diagnostic_flush_logs must be a boolean")
        if not isinstance(self.convert_distributed_to_local, bool):
            raise ConfigurationError("convert_distributed_to_local must be a boolean")

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

    def __init__(
        self, config: ClickHouseReadConfig, *, log_context: LogContext | None = None
    ) -> None:
        ensure_supported_ray_version()
        initialize_datasource = cast(Callable[[], None], super().__init__)
        initialize_datasource()
        self._config = config
        self._log_context = log_context or LogContext.create(from_environment=True)
        emit(
            logger,
            "clickhouse.read.path_selected",
            self._log_context,
            level=logging.INFO,
            phase="source",
            status="selected",
            role="driver",
            read_mode="table",
        )
        self._projection_columns: tuple[str, ...] | None = config.columns
        self._planning_snapshot: DiscoverySnapshot | None = None
        self._resolved_distributed: ResolvedDistributed | None = None
        self._local_sources: tuple[ClickHouseDatasource, ...] = ()
        self._logical_fallback = False
        self._predicate_node: PredicateNode | None = None
        self._pushed_predicate: CompiledPredicate | None = None
        self._predicate_schema: SchemaPlan | None = None
        self._predicate_expr: Any = None

    def supports_predicate_pushdown(self) -> bool:
        return supports_predicate_pushdown()

    def get_current_predicate(self) -> Any:
        return self._predicate_expr

    @source_context
    def apply_predicate(self, predicate_expr: Any) -> ClickHouseDatasource:
        """Accept a complete expression on an independent plan, or preserve identity."""
        decoded = decode_predicate(predicate_expr)
        if decoded is None:
            return self
        config = self._config
        protocol = predicate_binding_protocol(
            config.filter_sql, dict(config.query_parameters)
        )
        if protocol is None:
            return self
        schema = (
            self._planning_snapshot.schema
            if self._planning_snapshot is not None
            else self._predicate_schema
        )
        if schema is None:
            schema = discover_schema(
                config.connection, config.table, config.limits, columns=config.columns
            )
        selected = self._projection_columns
        if selected is None:
            selected = config.columns or tuple(schema.arrow_schema.names)
        selected_schema = SchemaPlan(
            pa.schema([schema.arrow_schema.field(name) for name in selected]),
            tuple(column for column in schema.columns if column.name in selected),
        )
        combined = (
            PredicateNode("and", (self._predicate_node, decoded))
            if self._predicate_node is not None
            else decoded
        )
        compiled = compile_predicate(combined, selected_schema, native=protocol)
        if compiled is None:
            return self
        if not discover_predicate_context(config.connection, config.limits):
            return self
        clone = ClickHouseDatasource(config, log_context=self._log_context)
        clone._projection_columns = self._projection_columns
        clone._predicate_node = combined
        clone._pushed_predicate = compiled
        clone._predicate_schema = schema
        clone._predicate_expr = combine_predicates(self._predicate_expr, predicate_expr)
        return clone

    @property
    def config(self) -> ClickHouseReadConfig:
        return self._config

    @property
    def name(self) -> str:
        return f"ClickHouse:{self._config.table}"

    def estimate_inmemory_data_size(self) -> None:
        """Do not trigger an extra COUNT/size/sample query during Ray planning."""
        return None

    def supports_projection_pushdown(self) -> bool:
        return supports_v1_projection_pushdown()

    def get_projection_map(self) -> dict[str, str] | None:
        """Return a fresh identity map; None and an empty map remain distinct."""
        columns = self._projection_columns
        return None if columns is None else {name: name for name in columns}

    def apply_projection(
        self, projection_map: dict[str, str] | None
    ) -> ClickHouseDatasource:
        """Clone the scan without changing another Dataset branch or its metadata."""
        if not self.supports_projection_pushdown():
            raise ConfigurationError("V1 projection is unverified on this Ray release")
        snapshot = self._ensure_planning_snapshot()
        available = self._selected_columns(snapshot)
        columns = (
            self._projection_columns
            if projection_map is None
            else tuple(
                validate_identifier(name, name="column") for name in projection_map
            )
        )
        unknown = set(columns or ()).difference(available)
        if unknown:
            raise ConfigurationError(f"unknown ClickHouse columns: {sorted(unknown)}")
        clone = copy(self)
        clone._projection_columns = columns
        clone._local_sources = tuple(
            source.apply_projection(projection_map) for source in self._local_sources
        )
        return clone

    @source_context
    def _ensure_planning_snapshot(self) -> DiscoverySnapshot:
        if self._planning_snapshot is not None:
            return self._planning_snapshot
        config = self._config
        schema = self._predicate_schema or discover_schema(
            config.connection, config.table, config.limits, columns=config.columns
        )
        engine = discover_engine(config.connection, config.table, config.limits)
        filter_sql, _, _ = effective_filter(
            config.filter_sql, dict(config.query_parameters), self._pushed_predicate
        )
        if config.convert_distributed_to_local and engine in _SINGLE_QUERY_ENGINES:
            resolved = None
            reason_code = "local_conversion_ineligible"
            if engine == "Distributed" and filter_sql and not config.order_by:
                try:
                    resolved = resolve_distributed(
                        config.connection,
                        config.table,
                        schema,
                        config.limits,
                        filter_sql=filter_sql,
                        split=config.split,
                    )
                except NotApplicable as exc:
                    reason_code = exc.reason_code
            if resolved is not None:
                sources = self._prepare_local_sources(schema, resolved)
                self._resolved_distributed = resolved
                self._local_sources = sources
                emit(
                    logger,
                    "clickhouse.read.path_selected",
                    current_context(),
                    level=logging.INFO,
                    phase="selection",
                    status="selected",
                    read_mode="local_shard",
                    shard_count=len(sources),
                )
            else:
                self._logical_fallback = True
                emit(
                    logger,
                    "clickhouse.read.path_selected",
                    current_context(),
                    level=logging.INFO,
                    phase="selection",
                    status="selected",
                    read_mode="table",
                    reason_code=reason_code,
                )
            self._planning_snapshot = DiscoverySnapshot(schema=schema, engine=engine)
            return self._planning_snapshot
        if not _is_mergetree_engine(engine) and not (
            config.split in {"single", "auto"} and engine in _SINGLE_QUERY_ENGINES
        ):
            raise DiscoveryError(
                f"ClickHouse engine {engine!r} is unsupported for "
                f"split={config.split!r}; "
                "use a direct table with split='single'"
            )
        partitions: tuple[PartitionInfo, ...] = ()
        range_facts: RangeFacts | None = None
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
            if config.range_column not in {column.name for column in schema.columns}:
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
                    pushed_predicate=self._pushed_predicate,
                )
            except PermissionError:
                raise
            except DiscoveryError:
                if config.discovery_policy == "error":
                    raise
        if config.split == "auto":
            try:
                if engine not in LOCAL_MERGETREE_ENGINES:
                    raise DiscoveryError(
                        "automatic range splitting requires a direct MergeTree table"
                    )
                sorting_key = discover_sorting_key(
                    config.connection, config.table, config.limits
                )
                range_definition = select_auto_range_column(sorting_key, schema.columns)
                range_facts = discover_range_facts(
                    config.connection,
                    config.table,
                    config.limits,
                    column=range_definition,
                    filter_sql=config.filter_sql,
                    parameters=dict(config.query_parameters),
                    pushed_predicate=self._pushed_predicate,
                )
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

    def _prepare_local_sources(
        self, source_schema: SchemaPlan, resolved: ResolvedDistributed
    ) -> tuple[ClickHouseDatasource, ...]:
        sources = []
        config = self._config
        for target in resolved.targets:
            local_config = replace(
                config,
                connection=target.connection,
                table=target.table,
                convert_distributed_to_local=False,
                columns=config.columns or tuple(source_schema.arrow_schema.names),
            )
            facts = None
            partitions = target.partitions or ()
            if config.split == "partition" and target.partitions is None:
                if config.discovery_policy == "error":
                    raise DiscoveryError(
                        "local target lacks physical partition metadata"
                    )
            if config.split in {"auto", "range"}:
                try:
                    if config.split == "auto":
                        if target.sorting_key is None:
                            raise DiscoveryError(
                                "local target lacks sorting_key capability"
                            )
                        column = select_auto_range_column(
                            target.sorting_key, target.schema.columns
                        )
                    else:
                        candidate = next(
                            (
                                column
                                for column in target.schema.columns
                                if column.name == config.range_column
                            ),
                            None,
                        )
                        if candidate is None:
                            raise ConfigurationError(
                                "range_column is not present in the table schema"
                            )
                        column = candidate
                    facts = discover_range_facts(
                        target.connection,
                        target.table,
                        config.limits,
                        column=column,
                        filter_sql=config.filter_sql,
                        parameters=dict(config.query_parameters),
                        pushed_predicate=self._pushed_predicate,
                    )
                except DiscoveryError:
                    if config.discovery_policy == "error":
                        raise
            source = ClickHouseDatasource(local_config, log_context=self._log_context)
            source._pushed_predicate = self._pushed_predicate
            source._planning_snapshot = DiscoverySnapshot(
                target.schema, "MergeTree", partitions, facts
            )
            source._projection_columns = self._projection_columns
            sources.append(source)
        return tuple(sources)

    def _selected_columns(self, snapshot: DiscoverySnapshot) -> tuple[str, ...]:
        columns = self._config.columns or tuple(snapshot.schema.arrow_schema.names)
        unknown = set(columns).difference(snapshot.schema.arrow_schema.names)
        if unknown:
            raise ConfigurationError(f"unknown ClickHouse columns: {sorted(unknown)}")
        if self._config.order_by:
            order_columns = {column for column, _ in self._config.order_by}
            unknown_order = order_columns.difference(
                column.name for column in snapshot.schema.columns
            )
            if unknown_order:
                raise ConfigurationError(
                    f"unknown order_by columns: {sorted(unknown_order)}"
                )
        return columns if self._projection_columns is None else self._projection_columns

    @property
    def arrow_schema(self) -> pa.Schema:
        snapshot = self._ensure_planning_snapshot()
        return pa.schema(
            [
                snapshot.schema.arrow_schema.field(c)
                for c in self._selected_columns(snapshot)
            ]
        )

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
        # SQL requires at least one expression. Keep a permitted source column as
        # the physical row carrier, then let Ray implement the empty projection.
        scan_columns = columns or (
            (config.columns or tuple(snapshot.schema.arrow_schema.names))[0],
        )
        definitions_by_name = {
            column.name: column for column in snapshot.schema.columns
        }
        selected_definitions = tuple(definitions_by_name[name] for name in scan_columns)
        sql, parameters = build_select(
            table=config.table,
            columns=scan_columns,
            filter_sql=config.filter_sql,
            parameters=dict(config.query_parameters),
            pushed_predicate=self._pushed_predicate,
            partition_ids=partition_ids,
            range_column=snapshot.range_facts.column
            if snapshot.range_facts is not None
            and (range_lower is not None or range_upper is not None)
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
                [snapshot.schema.arrow_schema.field(column) for column in scan_columns]
            ),
            operation="read",
        )
        active = current_context()
        log_context = (
            active
            if active.operation_id == self._log_context.operation_id
            else self._log_context
        ).split()

        def read_fn() -> Any:
            blocks = stream_query(
                config.connection,
                query,
                config.limits,
                diagnostic_flush_logs=config.diagnostic_flush_logs,
                log_context=log_context,
            )
            try:
                for block in blocks:
                    yield block if columns else empty_projection(block)
            finally:
                close = getattr(blocks, "close", None)
                if close is not None:
                    close()

        return make_read_task(
            read_fn,
            BlockMetadata(
                num_rows=None, size_bytes=None, exec_stats=None, input_files=None
            ),
            pa.schema([snapshot.schema.arrow_schema.field(c) for c in columns]),
            per_task_row_limit,
        )

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: int | None = None,
        data_context: Any | None = None,
    ) -> list[ReadTask]:
        active = current_context()
        context = (
            active
            if active.operation_id == self._log_context.operation_id
            else self._log_context
        ).plan()
        cache_hit = self._planning_snapshot is not None
        started: float | None = None
        with use_context(context):
            try:
                snapshot = self._ensure_planning_snapshot()
                started = time.monotonic()
                tasks = self._get_read_tasks(
                    parallelism, per_task_row_limit, data_context
                )
            except BaseException as error:
                emit(
                    logger,
                    "clickhouse.plan.finished",
                    context,
                    level=logging.ERROR,
                    phase="discovery" if started is None else "planning",
                    status="failed",
                    role="driver",
                    error_type=error_type(error),
                )
                raise
            mode = "local_shard" if self._local_sources else "table"
            actual_split = self._config.split
            if (
                self._logical_fallback
                or (actual_split == "partition" and not snapshot.partitions)
                or (actual_split in {"range", "auto"} and snapshot.range_facts is None)
            ):
                actual_split = "single"
            emit(
                logger,
                "clickhouse.read.path_selected",
                context,
                level=logging.INFO,
                phase="selection",
                status="selected",
                role="driver",
                read_mode=mode,
                **runtime_fields(),
            )
            emit(
                logger,
                "clickhouse.plan.finished",
                context,
                level=logging.INFO,
                phase="planning",
                status="planned",
                role="driver",
                read_mode=mode,
                engine=snapshot.engine,
                split_mode=actual_split if not self._local_sources else None,
                cache_hit=cache_hit,
                task_count=len(tasks),
                shard_count=len(self._local_sources),
                budget_hit=len(tasks)
                >= min(self._config.target_tasks, self._config.max_tasks),
                duration_ms=(time.monotonic() - started) * 1000,
                **runtime_fields(),
            )
            return tasks

    def _get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: int | None,
        data_context: Any | None,
    ) -> list[ReadTask]:
        del data_context
        snapshot = self._ensure_planning_snapshot()
        columns = self._selected_columns(snapshot)
        if self._local_sources:
            task_count = min(
                max(1, parallelism), self._config.target_tasks, self._config.max_tasks
            )
            # Allocate the budget across shards before splitting within a shard.
            shard_budget = max(1, task_count // len(self._local_sources))
            scans = [
                task
                for source in self._local_sources
                for task in source.get_read_tasks(shard_budget)
            ]
            scan_groups: list[list[ReadTask]] = [
                [] for _ in range(min(task_count, len(scans)))
            ]
            for index, task in enumerate(scans):
                scan_groups[index % len(scan_groups)].append(task)
            schema = pa.schema(
                [snapshot.schema.arrow_schema.field(column) for column in columns]
            )
            emit(
                logger,
                "clickhouse.plan.finished",
                current_context(),
                level=logging.INFO,
                phase="grouping",
                status="planned",
                role="driver",
                shard_count=len(self._local_sources),
                task_count=len(scan_groups),
                scan_count=len(scans),
            )
            return [
                _group_read_tasks(tuple(group), schema, per_task_row_limit)
                for group in scan_groups
            ]
        if self._config.split == "single" or self._logical_fallback:
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

        if self._config.split in {"range", "auto"} and snapshot.range_facts is not None:
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


def _group_read_tasks(
    tasks: tuple[ReadTask, ...], schema: pa.Schema, row_limit: int | None
) -> ReadTask:
    """Stream fixed scans sequentially, sharing one task row budget and cleanup."""

    def read_fn() -> Iterator[pa.Table]:
        remaining = row_limit
        for task in tasks:
            if remaining is not None and remaining <= 0:
                return
            stream = iter(task())
            try:
                for block in stream:
                    if remaining is not None:
                        block = block.slice(0, remaining)
                        remaining -= block.num_rows
                    if block.num_rows:
                        yield block
                    if remaining is not None and remaining <= 0:
                        return
            finally:
                close = getattr(stream, "close", None)
                if close is not None:
                    close()

    return make_read_task(
        read_fn,
        BlockMetadata(
            num_rows=None, size_bytes=None, exec_stats=None, input_files=None
        ),
        schema,
        row_limit,
    )
