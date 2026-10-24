"""Single-query source configuration and public Ray ReadTask construction."""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field, replace
from typing import Any, cast

import pyarrow as pa
from ray.data.block import BlockMetadata
from ray.data.datasource import Datasource, ReadTask

from ray_clickhouse._compat import ensure_supported_ray_version, make_read_task
from ray_clickhouse._discovery import discover_query_schema
from ray_clickhouse._errors import ConfigurationError
from ray_clickhouse._logging import (
    LogContext,
    current_context,
    emit,
    error_type,
    runtime_fields,
    source_context,
    use_context,
)
from ray_clickhouse._models import ClickHouseConnection, QuerySpec, ResourceLimits
from ray_clickhouse._schema import SchemaPlan, render_read_projection
from ray_clickhouse._sql import validate_filter_parameters
from ray_clickhouse._transport import stream_query

_FORBIDDEN_CONTROL = re.compile(
    r"(?<![\w.])(?:INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|RENAME|TRUNCATE|"
    r"ATTACH|DETACH|OPTIMIZE|EXCHANGE|GRANT|REVOKE|BACKUP|RESTORE|"
    r"KILL|SET|SETTINGS|FORMAT|INTO)\b",
    re.IGNORECASE,
)


def _query_code(value: str) -> str:
    """Mask quoted text and comments while preserving positions for validation."""
    result = list(value)
    index = 0
    while index < len(value):
        start = index
        char = value[index]
        if char in {"'", '"', "`"}:
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
                raise ConfigurationError("unterminated query quote")
        elif value.startswith("/*", index):
            end = value.find("*/", index + 2)
            if end < 0:
                raise ConfigurationError("unterminated query comment")
            index = end + 2
        elif value.startswith("--", index) or char == "#":
            end = value.find("\n", index)
            index = len(value) if end < 0 else end
        else:
            index += 1
            continue
        result[start:index] = " " * (index - start)
    return "".join(result)


def validate_read_query(value: str) -> str:
    """Accept the documented trusted SELECT/WITH subset, not general statements."""
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise ConfigurationError("query must be a non-empty trusted read query")
    value = value.strip()
    code = _query_code(value)
    semicolons = [index for index, char in enumerate(code) if char == ";"]
    if semicolons:
        if len(semicolons) != 1 or code[semicolons[0] + 1 :].strip():
            raise ConfigurationError("query must contain one statement")
        index = semicolons[0]
        value = value[:index] + value[index + 1 :]
        code = code[:index] + code[index + 1 :]
    if re.match(r"\s*(?:SELECT|WITH)\b", code, re.IGNORECASE) is None:
        raise ConfigurationError("query must be a SELECT or WITH/SELECT read")
    if _FORBIDDEN_CONTROL.search(code):
        raise ConfigurationError(
            "query contains an unsupported statement or control clause"
        )
    if re.search(r"\{\w+:[^{}]+\}", code):
        raise ConfigurationError(
            "query reads currently use client-side named parameters"
        )
    return value


@dataclass(frozen=True, repr=False)
class QueryReadConfig:
    connection: ClickHouseConnection
    query_sql: str
    query_parameters: tuple[tuple[str, Any], ...] = ()
    limits: ResourceLimits = field(default_factory=ResourceLimits)
    diagnostic_flush_logs: bool = False

    def __post_init__(self) -> None:
        sql = validate_read_query(self.query_sql)
        parameters = dict(self.query_parameters)
        validate_filter_parameters(sql, parameters)
        code = _query_code(sql)
        hidden = "".join(
            original if masked == " " else " "
            for original, masked in zip(sql, code, strict=True)
        )
        if parameters and re.search(r"(?<!%)%\([A-Za-z_][A-Za-z0-9_]*\)s", hidden):
            raise ConfigurationError(
                "quoted or commented client placeholders are unsupported"
            )
        if parameters and re.search(r"\{\w+:[^{}]+\}", sql):
            raise ConfigurationError(
                "typed placeholder-looking query text is unsupported"
            )
        if not isinstance(self.diagnostic_flush_logs, bool):
            raise ConfigurationError("diagnostic_flush_logs must be a boolean")
        settings = dict(self.connection.settings)
        if settings.get("readonly", 1) != 1:
            raise ConfigurationError("query reads require readonly=1")
        settings["readonly"] = 1
        object.__setattr__(self, "query_sql", sql)
        object.__setattr__(
            self,
            "connection",
            replace(self.connection, settings=tuple(sorted(settings.items()))),
        )

    def __repr__(self) -> str:
        names = tuple(name for name, _ in self.query_parameters)
        return f"QueryReadConfig(parameter_names={names!r})"


logger = logging.getLogger(__name__)


class ClickHouseQueryDatasource(Datasource):
    """Read a trusted query through one streaming worker task."""

    def __init__(self, config: QueryReadConfig) -> None:
        ensure_supported_ray_version()
        initialize = cast(Callable[[], None], super().__init__)
        initialize()
        self._config = config
        self._log_context = LogContext.create(from_environment=True)
        emit(
            logger,
            "clickhouse.read.path_selected",
            self._log_context,
            level=logging.INFO,
            phase="source",
            status="selected",
            role="driver",
            read_mode="query",
        )
        self._schema_plan: SchemaPlan | None = None

    @property
    def config(self) -> QueryReadConfig:
        return self._config

    @property
    def name(self) -> str:
        return "ClickHouseQuery"

    def estimate_inmemory_data_size(self) -> None:
        return None

    @source_context
    def _schema(self) -> SchemaPlan:
        if self._schema_plan is None:
            self._schema_plan = discover_query_schema(
                self._config.connection,
                self._config.query_sql,
                self._config.limits,
                parameters=dict(self._config.query_parameters),
            )
        return self._schema_plan

    @property
    def arrow_schema(self) -> pa.Schema:
        return self._schema().arrow_schema

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: int | None = None,
        data_context: Any | None = None,
    ) -> list[ReadTask]:
        context = self._log_context.plan()
        cache_hit = self._schema_plan is not None
        with use_context(context):
            try:
                self._schema()
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
                    phase="planning",
                    status="failed",
                    role="driver",
                    error_type=error_type(error),
                )
                raise
            emit(
                logger,
                "clickhouse.read.path_selected",
                context,
                level=logging.INFO,
                phase="selection",
                status="selected",
                role="driver",
                read_mode="query",
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
                read_mode="query",
                task_count=len(tasks),
                cache_hit=cache_hit,
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
        del parallelism, data_context
        config = self._config
        plan = self._schema()
        projection = render_read_projection(plan.columns)
        query = QuerySpec(
            f"SELECT {projection} FROM (\n{config.query_sql}\n) "
            "AS __ray_clickhouse_query",
            config.query_parameters,
            plan.arrow_schema,
            operation="query-read",
            strict_schema=True,
        )
        log_context = current_context().split()

        def read_fn() -> Iterable[pa.Table]:
            yield from stream_query(
                config.connection,
                query,
                config.limits,
                diagnostic_flush_logs=config.diagnostic_flush_logs,
                log_context=log_context,
            )

        return [
            make_read_task(
                read_fn,
                BlockMetadata(
                    num_rows=None,
                    size_bytes=None,
                    exec_stats=None,
                    input_files=None,
                ),
                plan.arrow_schema,
                per_task_row_limit,
            )
        ]
