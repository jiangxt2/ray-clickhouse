"""Bounded, credential-free events; output configuration belongs to the caller."""

from __future__ import annotations

import logging
import math
import os
import re
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from functools import wraps
from typing import ParamSpec, TypeVar

from ray_clickhouse._errors import (
    AmbiguousTableManagementError,
    AmbiguousWriteError,
    ConfigurationError,
    RayClickHouseError,
    SchemaError,
    WriteError,
)

_P = ParamSpec("_P")
_R = TypeVar("_R")
_TOKEN = re.compile(r"^[A-Za-z0-9_.:-]+$")
_FIELDS = frozenset(
    "phase status role operation_id execution_id plan_id parent_plan_id split_id "
    "query_id client_insert_id operation read_mode reason_code engine split_mode "
    "error_type mismatch_category resource_kind ray_job_id ray_task_id ray_worker_id "
    "ray_actor_id ray_node_id duration_ms task_count shard_count scan_count cache_hit "
    "batch_index emitted_blocks emitted_rows arrow_bytes confirmed_rows batches "
    "insert_mode write_mode drop_attempted available exception_code "
    "suppressed_progress expected_fields actual_fields budget_hit".split()
)


@dataclass(frozen=True)
class LogContext:
    operation_id: str
    batches: bool = False
    plan_id: str | None = None
    split_id: str | None = None
    execution_id: str | None = None
    parent_plan_id: str | None = None

    @classmethod
    def create(cls, *, from_environment: bool = False) -> LogContext:
        value = (
            os.environ.get("RAY_CLICKHOUSE_LOG_BATCHES", "0")
            if from_environment
            else "0"
        )
        if value not in {"0", "1"}:
            raise ConfigurationError("RAY_CLICKHOUSE_LOG_BATCHES must be 0 or 1")
        return cls(uuid.uuid4().hex, value == "1")

    def plan(self) -> LogContext:
        return replace(
            self,
            plan_id=uuid.uuid4().hex,
            parent_plan_id=self.plan_id,
            split_id=None,
            execution_id=None,
        )

    def split(self) -> LogContext:
        return replace(self, split_id=uuid.uuid4().hex, execution_id=None)

    def execution(self) -> LogContext:
        return replace(self, execution_id=uuid.uuid4().hex)

    def fields(self) -> dict[str, object]:
        return {
            key: value
            for key, value in (
                ("operation_id", self.operation_id),
                ("plan_id", self.plan_id),
                ("parent_plan_id", self.parent_plan_id),
                ("split_id", self.split_id),
                ("execution_id", self.execution_id),
            )
            if value is not None
        }


_CURRENT: ContextVar[LogContext | None] = ContextVar(
    "clickhouse_log_context", default=None
)


def current_context() -> LogContext:
    return _CURRENT.get() or LogContext.create()


@contextmanager
def use_context(context: LogContext) -> Iterator[None]:
    token = _CURRENT.set(context)
    try:
        yield
    finally:
        _CURRENT.reset(token)


def error_type(error: BaseException) -> str:
    if isinstance(error, RayClickHouseError):
        # Public connector classes have fixed, credential-free names.
        from ray_clickhouse import _errors

        for name, cls in vars(_errors).items():
            if isinstance(cls, type) and type(error) is cls:
                return name
    if isinstance(error, (GeneratorExit, KeyboardInterrupt, SystemExit)):
        return type(error).__name__
    return "ExternalError"


def runtime_fields(*, worker: bool = False) -> dict[str, object]:
    """Read public context only after Ray is initialized; never start a runtime."""
    fields: dict[str, object] = {}
    try:
        import ray

        if not ray.is_initialized():
            return fields
        context = ray.get_runtime_context()
        fields["ray_job_id"] = context.get_job_id()
        fields["ray_node_id"] = context.get_node_id()
        if worker:
            fields["ray_worker_id"] = context.get_worker_id()
            fields["ray_task_id"] = context.get_task_id()
            fields["ray_actor_id"] = context.get_actor_id()
    except Exception:
        pass
    return fields


def emit(
    logger: logging.Logger,
    event: str,
    context: LogContext,
    *,
    level: object = logging.DEBUG,
    **fields: object,
) -> None:
    """Accept only simple allowlisted fields, and isolate caller handler failures."""
    try:
        if not isinstance(level, int):
            return
        if not logger.isEnabledFor(level):
            return
        if (
            len(event) > 64
            or not event.startswith("clickhouse.")
            or not _TOKEN.fullmatch(event)
        ):
            return
        extra: dict[str, object] = {"event": event}
        for key, value in (context.fields() | fields).items():
            if key not in _FIELDS or value is None:
                continue
            if type(value) in {bool, int}:
                extra[key] = value
            elif type(value) is float and math.isfinite(value):
                extra[key] = value
            elif type(value) is str:
                if len(value) <= 128 and _TOKEN.fullmatch(value):
                    extra[key] = value
                else:
                    extra["fields_truncated"] = True
        logger.log(level, event, extra=extra)
    except Exception:
        # A caller's filter/formatter/handler must not affect database execution.
        pass


@contextmanager
def stage(
    logger: logging.Logger,
    context: LogContext,
    prefix: str,
    phase: str,
    *,
    role: str = "driver",
    fallback: type[Exception] | None = None,
    **fields: object,
) -> Iterator[None]:
    started = time.monotonic()
    emit(
        logger,
        prefix + ".started",
        context,
        phase=phase,
        status="started",
        role=role,
        **fields,
    )
    try:
        yield
    except BaseException as error:
        expected_fallback = fallback is not None and isinstance(error, fallback)
        emit(
            logger,
            prefix + ".finished",
            context,
            level=logging.DEBUG if expected_fallback else logging.ERROR,
            phase=phase,
            role=role,
            status="fallback"
            if expected_fallback
            else "ambiguous"
            if isinstance(error, (AmbiguousWriteError, AmbiguousTableManagementError))
            else "failed",
            error_type=None if expected_fallback else error_type(error),
            reason_code=getattr(error, "reason_code", None)
            if expected_fallback
            else None,
            mismatch_category="schema_validation"
            if isinstance(error, SchemaError)
            or (phase in {"schema", "block_prepare"} and isinstance(error, WriteError))
            else None,
            duration_ms=(time.monotonic() - started) * 1000,
            **fields,
        )
        raise
    else:
        emit(
            logger,
            prefix + ".finished",
            context,
            phase=phase,
            role=role,
            status="succeeded",
            duration_ms=(time.monotonic() - started) * 1000,
            **fields,
        )


def discovery(
    phase: str,
    *,
    fallback: type[Exception] | None = None,
) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    def decorate(function: Callable[_P, _R]) -> Callable[_P, _R]:
        logger = logging.getLogger(function.__module__)

        @wraps(function)
        def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            with stage(
                logger,
                current_context(),
                "clickhouse.discovery",
                phase,
                fallback=fallback,
            ):
                return function(*args, **kwargs)

        return wrapped

    return decorate


def source_context(function: Callable[_P, _R]) -> Callable[_P, _R]:
    @wraps(function)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        context = getattr(args[0], "_log_context", None)
        if not isinstance(context, LogContext):
            return function(*args, **kwargs)
        active = _CURRENT.get()
        if active is not None and active.operation_id == context.operation_id:
            context = active
        with use_context(context):
            return function(*args, **kwargs)

    return wrapped


class ProgressBudget:
    """Sample the first and every 100th progress point, up to 100 per execution."""

    def __init__(self, context: LogContext, logger: logging.Logger) -> None:
        self.context = context
        self.logger = logger
        self.seen = 0
        self.emitted = 0
        self.last_time = 0.0

    def admit(self) -> bool:
        if not self.context.batches or not self.logger.isEnabledFor(logging.DEBUG):
            return False
        self.seen += 1
        if self.emitted >= 100 or (self.seen != 1 and self.seen % 100):
            return False
        now = time.monotonic()
        if self.emitted and now - self.last_time < 1:
            return False
        self.emitted += 1
        self.last_time = now
        return True

    @property
    def suppressed(self) -> int:
        return self.seen - self.emitted
