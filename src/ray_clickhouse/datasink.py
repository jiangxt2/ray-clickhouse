"""Ray Data Datasink for append-only ClickHouse writes."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

import pyarrow as pa
from ray.data.block import BlockAccessor
from ray.data.datasource import Datasink

from ray_clickhouse._compat import prepare_write_remote_args
from ray_clickhouse._ddl import create_or_replace_table, validate_create_engine
from ray_clickhouse._discovery import discover_target
from ray_clickhouse._errors import ConfigurationError, WriteError
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
        if self._write_mode in {"create", "overwrite"}:
            if schema is None:
                raise ConfigurationError(
                    "create/overwrite requires the Ray Dataset to provide "
                    "an Arrow schema"
                )
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
        self._target = discover_target(self._connection, self._table, self._limits)
        self._source_schema = schema
        if schema is not None:
            self._source_columns = validate_source_schema(
                schema, self._target, self._columns
            )

    def write(self, blocks: Iterator[Any], ctx: Any) -> dict[str, object]:
        del ctx
        if self._target is None:
            raise ConfigurationError(
                "ClickHouseDataSink.on_write_start must run before write"
            )
        session = ClickHouseInsertSession(
            self._connection,
            self._limits,
            database=self._table.database,
            table=self._table.table,
            insert_mode=self._insert_mode,
        )
        total_rows = 0
        total_bytes = 0
        total_batches = 0
        failure: BaseException | None = None
        try:
            session.start()
            for block in blocks:
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
                for batch in iter_batch_slices(
                    prepared,
                    max_rows=self._limits.batch_rows,
                    max_bytes=self._limits.batch_bytes,
                ):
                    session.insert(batch)
                    total_rows += batch.num_rows
                    total_bytes += batch.nbytes
                    total_batches += 1
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
            try:
                session.close()
            except Exception:
                if failure is None:
                    raise

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

    def on_write_failed(self, error: Exception) -> None:
        self._receipt = None


def validate_write_remote_args(
    ray_remote_args: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Expose the no-replay gate for the public write facade."""
    return prepare_write_remote_args(ray_remote_args)
