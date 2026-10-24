"""Ray Data Datasink for append-only ClickHouse writes."""

from __future__ import annotations

import logging
import time
from collections.abc import Iterable, Mapping
from contextlib import nullcontext
from typing import Any

import pyarrow as pa
from ray.data.block import BlockAccessor
from ray.data.datasource import Datasink

from ray_clickhouse._compat import prepare_write_remote_args
from ray_clickhouse._ddl import create_or_replace_table, validate_create_engine
from ray_clickhouse._discovery import discover_target
from ray_clickhouse._errors import (
    AmbiguousWriteError,
    ConfigurationError,
    SchemaError,
    WriteError,
)
from ray_clickhouse._logging import (
    LogContext,
    ProgressBudget,
    emit,
    error_type,
    runtime_fields,
    stage,
    use_context,
)
from ray_clickhouse._models import (
    ClickHouseConnection,
    InsertMode,
    QualifiedTable,
    ResourceLimits,
    WriteMode,
    WriteReceipt,
)
from ray_clickhouse._schema import (
    TargetTable,
    prepare_arrow_table,
    validate_source_schema,
)
from ray_clickhouse._sql import normalize_columns
from ray_clickhouse._transport import ClickHouseInsertSession, iter_batch_slices

logger = logging.getLogger(__name__)


class ClickHouseDataSink(Datasink[dict[str, object]]):
    """Write Ray blocks to a governed ClickHouse MergeTree-family table."""

    def __init__(
        self,
        *,
        connection: ClickHouseConnection,
        table: QualifiedTable,
        insert_mode: InsertMode = "sync",
        write_mode: WriteMode = "append",
        engine: str = "MergeTree",
        order_by: tuple[str, ...] | None = None,
        nullable_columns: tuple[str, ...] | None = None,
        columns: tuple[str, ...] | None = None,
        limits: ResourceLimits | None = None,
    ) -> None:
        if insert_mode not in {"sync", "async"}:
            raise ConfigurationError("insert_mode must be 'sync' or 'async'")
        if write_mode not in {"append", "create", "overwrite"}:
            raise ConfigurationError(
                "write_mode must be 'append', 'create', or 'overwrite'"
            )
        if write_mode in {"create", "overwrite"}:
            validate_create_engine(engine)
        elif nullable_columns is not None:
            raise ConfigurationError(
                "nullable_columns is only valid for create/overwrite"
            )
        self._connection = connection
        self._log_context = LogContext.create(from_environment=True)
        self._table = table
        self._insert_mode = insert_mode
        self._write_mode = write_mode
        self._engine = engine
        self._order_by = order_by
        self._nullable_columns = nullable_columns
        self._columns = normalize_columns(columns)
        self._limits = limits or ResourceLimits()
        self._target: TargetTable | None = None
        self._source_schema: pa.Schema | None = None
        self._source_columns: tuple[Any, ...] | None = None
        self._receipt: WriteReceipt | None = None

    @property
    def supports_distributed_writes(self) -> bool:
        return True

    @property
    def receipt(self) -> WriteReceipt | None:
        return self._receipt

    def get_name(self) -> str:
        return "ClickHouseWrite"

    def on_write_start(self, schema: pa.Schema | None = None) -> None:
        with use_context(self._log_context):
            self._prepare_write(schema)

    def _prepare_write(self, schema: pa.Schema | None) -> None:
        if self._write_mode in {"create", "overwrite"}:
            if schema is None:
                raise ConfigurationError(
                    "create/overwrite requires the Ray Dataset to provide "
                    "an Arrow schema"
                )
            with stage(
                logger,
                self._log_context,
                "clickhouse.write.prepare",
                "table_management",
            ):
                create_or_replace_table(
                    self._connection,
                    self._table,
                    self._limits,
                    schema,
                    mode=self._write_mode,
                    engine=self._engine,
                    order_by=self._order_by,
                    nullable_columns=self._nullable_columns,
                )
        with stage(logger, self._log_context, "clickhouse.write.prepare", "target"):
            self._target = discover_target(self._connection, self._table, self._limits)
        self._source_schema = schema
        if schema is not None:
            with stage(
                logger,
                self._log_context,
                "clickhouse.write.prepare",
                "schema",
                expected_fields=len(self._target.columns),
                actual_fields=len(schema),
            ):
                self._source_columns = validate_source_schema(
                    schema, self._target, self._columns
                )

    def write(self, blocks: Iterable[Any], ctx: Any) -> dict[str, object]:
        del ctx
        if self._target is None:
            raise ConfigurationError(
                "ClickHouseDataSink.on_write_start must run before write"
            )
        context = self._log_context.execution()
        runtime = runtime_fields(worker=True)
        progress = ProgressBudget(context, logger)
        started = time.monotonic()
        session = ClickHouseInsertSession(
            self._connection,
            self._limits,
            database=self._table.database,
            table=self._table.table,
            insert_mode=self._insert_mode,
            log_context=context,
        )
        total_rows = 0
        total_bytes = 0
        total_batches = 0
        failure: BaseException | None = None
        first_prepare = True
        mismatch_fields: dict[str, object] = {}
        try:
            session.start()
            for block in blocks:
                preparation = (
                    stage(
                        logger,
                        context,
                        "clickhouse.write.prepare",
                        "block_prepare",
                        role="worker",
                    )
                    if first_prepare
                    else nullcontext()
                )
                first_prepare = False
                arrow_table: object = None
                try:
                    with preparation:
                        arrow_table = BlockAccessor.for_block(block).to_arrow()
                        if not isinstance(arrow_table, pa.Table):
                            raise WriteError("Ray produced a non-Arrow write block")
                        if self._source_schema is None:
                            self._source_schema = arrow_table.schema
                            self._source_columns = validate_source_schema(
                                self._source_schema, self._target, self._columns
                            )
                        if self._source_columns is None:
                            raise WriteError(
                                "write schema validation did not produce a target plan"
                            )
                        prepared = prepare_arrow_table(
                            arrow_table,
                            self._source_schema,
                            self._source_columns,
                        )
                except (SchemaError, WriteError):
                    mismatch_fields = {
                        "mismatch_category": "schema_validation",
                        "expected_fields": len(self._source_schema)
                        if self._source_schema is not None
                        else None,
                        "actual_fields": len(arrow_table.schema)
                        if isinstance(arrow_table, pa.Table)
                        else None,
                    }
                    raise
                for batch in iter_batch_slices(
                    prepared,
                    max_rows=self._limits.batch_rows,
                    max_bytes=self._limits.batch_bytes,
                ):
                    result = session.insert(batch)
                    total_rows += batch.num_rows
                    total_bytes += batch.nbytes
                    total_batches += 1
                    if progress.admit():
                        emit(
                            logger,
                            "clickhouse.write.progress",
                            context,
                            phase="insert",
                            status="confirmed",
                            role="worker",
                            client_insert_id=result.get("query_id")
                            if isinstance(result, Mapping)
                            else None,
                            batch_index=total_batches,
                            batches=total_batches,
                            confirmed_rows=total_rows,
                            arrow_bytes=total_bytes,
                            insert_mode=self._insert_mode,
                            **runtime,
                        )
            return {
                "rows": total_rows,
                "bytes": total_bytes,
                "batches": total_batches,
                "status": "confirmed",
            }
        except BaseException as exc:
            failure = exc
            raise
        finally:
            cleanup_failed = False
            try:
                session.close()
            except Exception:
                cleanup_failed = True
                if failure is None:
                    raise
            finally:
                emit(
                    logger,
                    "clickhouse.write.finished",
                    context,
                    level=logging.ERROR
                    if failure is not None or cleanup_failed
                    else logging.DEBUG,
                    phase="cleanup"
                    if cleanup_failed
                    else "block_prepare"
                    if mismatch_fields
                    else "write",
                    role="worker",
                    status="ambiguous"
                    if isinstance(failure, AmbiguousWriteError)
                    else (
                        "failed"
                        if failure is not None or cleanup_failed
                        else "confirmed"
                    ),
                    error_type=error_type(failure) if failure else None,
                    batches=total_batches,
                    confirmed_rows=total_rows,
                    arrow_bytes=total_bytes,
                    suppressed_progress=progress.suppressed,
                    duration_ms=(time.monotonic() - started) * 1000,
                    **mismatch_fields,
                    **runtime,
                )

    def on_write_complete(self, write_result: Any) -> None:
        returns = getattr(write_result, "write_returns", ())
        rows = sum(
            int(item.get("rows", 0)) for item in returns if isinstance(item, Mapping)
        )
        bytes_written = sum(
            int(item.get("bytes", 0)) for item in returns if isinstance(item, Mapping)
        )
        batches = sum(
            int(item.get("batches", 0)) for item in returns if isinstance(item, Mapping)
        )
        self._receipt = WriteReceipt(rows, bytes_written, batches)
        emit(
            logger,
            "clickhouse.write.finished",
            self._log_context,
            level=logging.INFO,
            phase="completion",
            role="driver",
            status="noop"
            if not returns and getattr(write_result, "num_rows", None) == 0
            else "confirmed",
            confirmed_rows=rows,
            arrow_bytes=bytes_written,
            batches=batches,
            **runtime_fields(),
        )

    def on_write_failed(self, error: Exception) -> None:
        self._receipt = None
        emit(
            logger,
            "clickhouse.write.finished",
            self._log_context,
            level=logging.ERROR,
            phase="completion",
            role="driver",
            status="failed",
            error_type=error_type(error),
            **runtime_fields(),
        )


def validate_write_remote_args(
    ray_remote_args: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Expose the no-replay gate for the public write facade."""
    return prepare_write_remote_args(ray_remote_args)
