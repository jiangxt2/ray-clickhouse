"""Worker-owned ClickHouse Arrow streaming and insert sessions."""

from __future__ import annotations

import logging
import math
import re
import time
import uuid
from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from ray_clickhouse._errors import (
    AmbiguousWriteError,
    AuthenticationError,
    PermissionError,
    RayClickHouseError,
    ReadError,
    TransportError,
    WriteError,
)
from ray_clickhouse._models import ClickHouseConnection, QuerySpec, ResourceLimits

logger = logging.getLogger(__name__)

_QUERY_LOG_LOOKUP_ATTEMPTS = 4
_QUERY_LOG_LOOKUP_DELAY_SECONDS = 0.15
_QUERY_LOG_COLUMNS = (
    "type",
    "exception_code",
    "exception",
    "read_rows",
    "result_rows",
    "read_bytes",
    "result_bytes",
    "query_duration_ms",
)
_QUERY_LOG_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def iter_batch_slices(
    table: pa.Table, *, max_rows: int, max_bytes: int
) -> Iterator[pa.Table]:
    """Yield bounded Arrow tables without dropping an oversized single row."""
    offset = 0
    while offset < table.num_rows:
        upper = min(max_rows, table.num_rows - offset)
        if table.slice(offset, upper).nbytes <= max_bytes:
            size = upper
        else:
            low, high, size = 1, upper, 1
            while low <= high:
                candidate = (low + high) // 2
                if table.slice(offset, candidate).nbytes <= max_bytes:
                    size = candidate
                    low = candidate + 1
                else:
                    high = candidate - 1
        yield table.slice(offset, size)
        offset += size


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
        return PermissionError(f"ClickHouse permission denied during {operation}")
    if "timeout" in message or "connection" in message or "http" in message:
        return TransportError(f"ClickHouse transport failed during {operation}")
    if operation == "Arrow insert":
        return WriteError("ClickHouse rejected an Arrow insert batch")
    return ReadError("ClickHouse Arrow query failed")


def _validate_table(table: pa.Table, schema: pa.Schema) -> pa.Table:
    if tuple(table.column_names) != tuple(schema.names):
        raise ReadError("ClickHouse result columns do not match the planned schema")
    try:
        if table.schema.equals(schema, check_metadata=False):
            return pa.Table.from_arrays(table.columns, schema=schema)
        return table.cast(schema, safe=True)
    except (
        pa.ArrowInvalid,
        pa.ArrowNotImplementedError,
        pa.ArrowTypeError,
        ValueError,
    ):
        raise ReadError(
            "ClickHouse result does not match the planned Arrow schema"
        ) from None


def _new_query_id(operation: str) -> str:
    return f"ray-clickhouse-{operation}-{uuid.uuid4()}"


def _lookup_query_diagnostic(
    connection: ClickHouseConnection,
    limits: ResourceLimits,
    query_id: str,
    log_comment: str,
    *,
    flush_logs: bool,
) -> dict[str, object] | None:
    """Best-effort lookup of a failed query in ClickHouse query_log.

    This runs only on a read failure, uses a fresh client, and never replaces the
    original read exception if the diagnostic query is unavailable.
    """
    settings = connection.query_settings(limits)
    settings["max_execution_time"] = 2
    diagnostic_client: Any | None = None
    try:
        import clickhouse_connect

        diagnostic_client = clickhouse_connect.get_client(
            **connection.client_kwargs(limits)
        )
        if flush_logs:
            try:
                diagnostic_client.command("SYSTEM FLUSH LOGS")
            except Exception as exc:
                logger.debug(
                    "SYSTEM FLUSH LOGS unavailable for query_id=%s: %s",
                    query_id,
                    exc,
                )
        query_log_table = _discover_query_log_table(
            diagnostic_client,
            settings=settings,
            query_id=query_id,
        )
        if query_log_table is None:
            return None
        query = (
            "SELECT type, exception_code, exception, read_rows, result_rows, "
            "read_bytes, result_bytes, query_duration_ms "
            f"FROM system.`{query_log_table}` "
            "WHERE query_id = {query_id:String} "
            "OR log_comment = {log_comment:String} "
            "ORDER BY event_time_microseconds DESC"
        )
        for attempt in range(_QUERY_LOG_LOOKUP_ATTEMPTS):
            try:
                result = diagnostic_client.query(
                    query,
                    parameters={"query_id": query_id, "log_comment": log_comment},
                    settings=settings,
                    transport_settings={
                        "query_id": _new_query_id("query-log-diagnostic")
                    },
                )
                if result.result_rows:
                    row = result.result_rows[0]
                    return dict(zip(_QUERY_LOG_COLUMNS, row, strict=True))
            except Exception as exc:
                logger.debug(
                    "query_log diagnostic lookup failed for query_id=%s: %s",
                    query_id,
                    exc,
                )
                return None
            if attempt + 1 < _QUERY_LOG_LOOKUP_ATTEMPTS:
                time.sleep(_QUERY_LOG_LOOKUP_DELAY_SECONDS)
    except Exception as exc:
        logger.debug(
            "query_log diagnostic client failed for query_id=%s: %s",
            query_id,
            exc,
        )
    finally:
        if diagnostic_client is not None:
            try:
                diagnostic_client.close()
            except Exception:
                logger.debug(
                    "query_log diagnostic client close failed for query_id=%s",
                    query_id,
                    exc_info=True,
                )
    return None


def _discover_query_log_table(
    client: Any,
    *,
    settings: dict[str, Any],
    query_id: str,
) -> str | None:
    try:
        result = client.query(
            "SHOW TABLES FROM system LIKE '%query_log%'",
            settings=settings,
            transport_settings={"query_id": _new_query_id("query-log-show")},
        )
        names = sorted(
            {
                str(row[0])
                for row in result.result_rows
                if row and isinstance(row[0], str) and _QUERY_LOG_NAME.fullmatch(row[0])
            }
        )
        if not names:
            return None
        if "query_log" in names:
            return "query_log"
        return next(
            (name for name in names if name.lower().endswith("query_log")),
            names[0],
        )
    except Exception as exc:
        logger.debug(
            "query_log table discovery failed for query_id=%s: %s", query_id, exc
        )
        return None


def _annotate_read_error(
    error: RayClickHouseError,
    *,
    connection: ClickHouseConnection,
    limits: ResourceLimits,
    query_id: str,
    log_comment: str,
    query_started: bool,
    flush_logs: bool,
) -> RayClickHouseError:
    diagnostic = (
        _lookup_query_diagnostic(
            connection,
            limits,
            query_id,
            log_comment,
            flush_logs=flush_logs,
        )
        if query_started
        else None
    )
    error.attach_diagnostic(query_id=query_id, diagnostic=diagnostic)
    return error


def stream_query(
    connection: ClickHouseConnection,
    query: QuerySpec,
    limits: ResourceLimits,
    *,
    diagnostic_flush_logs: bool = False,
) -> Iterator[pa.Table]:
    """Read one query as bounded Arrow blocks and deterministically close the client."""
    client: Any | None = None
    failure: BaseException | None = None
    query_id = _new_query_id(query.operation)
    log_comment = f"ray-clickhouse operation={query.operation} query_id={query_id}"
    query_started = False
    try:
        try:
            import clickhouse_connect

            client = clickhouse_connect.get_client(**connection.client_kwargs(limits))
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit, GeneratorExit)):
                raise
            raise _translate(exc, operation="read client initialization") from None
        settings = connection.query_settings(limits)
        if diagnostic_flush_logs:
            settings["log_queries"] = 1
        # clickhouse-connect 1.5.x sends query settings as URL parameters.  The
        # server records this value as system.query_log.query_id; passing it via
        # transport_settings would only create an unrecognized HTTP header.
        settings["query_id"] = query_id
        settings["log_comment"] = log_comment
        query_started = True
        with client.query_arrow_stream(
            query.sql,
            parameters=query.parameter_dict(),
            settings=settings,
            use_strings=True,
        ) as stream:
            for batch in stream:
                if isinstance(batch, pa.RecordBatch):
                    batch = pa.Table.from_batches([batch])
                if not isinstance(batch, pa.Table):
                    raise ReadError("ClickHouse yielded a non-Arrow table")
                canonical = _validate_table(batch, query.arrow_schema)
                yield from iter_batch_slices(
                    canonical,
                    max_rows=limits.batch_rows,
                    max_bytes=limits.batch_bytes,
                )
    except (KeyboardInterrupt, SystemExit, GeneratorExit) as exc:
        failure = exc
        raise
    except RayClickHouseError as exc:
        failure = _annotate_read_error(
            exc,
            connection=connection,
            limits=limits,
            query_id=query_id,
            log_comment=log_comment,
            query_started=query_started,
            flush_logs=diagnostic_flush_logs,
        )
        raise failure from None
    except BaseException as exc:
        translated = _translate(exc, operation="Arrow query")
        failure = _annotate_read_error(
            translated,
            connection=connection,
            limits=limits,
            query_id=query_id,
            log_comment=log_comment,
            query_started=query_started,
            flush_logs=diagnostic_flush_logs,
        )
        raise failure from None
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                if failure is None:
                    raise TransportError(
                        "failed to close ClickHouse read client"
                    ) from None


class ClickHouseInsertSession:
    """One worker-owned client for confirmed append batches."""

    def __init__(
        self,
        connection: ClickHouseConnection,
        limits: ResourceLimits,
        *,
        database: str,
        table: str,
        insert_mode: str,
    ) -> None:
        self._connection = connection
        self._limits = limits
        self._database = database
        self._table = table
        self._insert_mode = insert_mode
        self._client: Any | None = None

    def start(self) -> None:
        try:
            import clickhouse_connect

            self._client = clickhouse_connect.get_client(
                **self._connection.client_kwargs(self._limits)
            )
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit, GeneratorExit)):
                raise
            raise _translate(exc, operation="write client initialization") from None

    def insert(self, table: pa.Table) -> dict[str, object]:
        if self._client is None:
            raise TransportError("ClickHouse write session is not started")
        settings = dict(self._connection.settings)
        settings["async_insert"] = 1 if self._insert_mode == "async" else 0
        if self._insert_mode == "async":
            settings["wait_for_async_insert"] = 1
        settings["max_execution_time"] = max(
            1, math.ceil(self._limits.query_timeout_seconds)
        )
        settings.setdefault("log_comment", "ray-clickhouse operation=insert")
        query_id = _new_query_id("insert")
        try:
            self._client.insert_arrow(
                self._table,
                table,
                database=self._database,
                settings=settings,
                transport_settings={"query_id": query_id},
            )
        except (KeyboardInterrupt, SystemExit, GeneratorExit):
            raise
        except BaseException as exc:
            translated = _translate(exc, operation="Arrow insert")
            if isinstance(translated, TransportError):
                raise AmbiguousWriteError(
                    "ClickHouse insert outcome is unknown after a transport failure"
                ) from None
            raise translated from None
        return {"status": "confirmed", "query_id": query_id}

    def close(self) -> None:
        client = self._client
        self._client = None
        if client is None:
            return
        try:
            client.close()
        except Exception:
            raise TransportError("failed to close ClickHouse write client") from None
