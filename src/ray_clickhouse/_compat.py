"""Small compatibility boundary for supported Ray releases."""

from __future__ import annotations

import inspect
import re
from collections.abc import Callable, Iterable, Mapping
from typing import Any

import pyarrow as pa
import ray
from ray.data.block import BlockMetadata
from ray.data.datasource import ReadTask

from ray_clickhouse._errors import ConfigurationError
from ray_clickhouse._predicates import PredicateNode

_MIN_RAY = (2, 55, 0)
_MAX_RAY = (2, 60, 0)
_RAY_VERSION = ray.__version__
_FINAL = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:\+[A-Za-z0-9]+(?:[._-][A-Za-z0-9]+)*)?$")


def supports_predicate_pushdown() -> bool:
    """Enable only expression/rejection contracts verified on both baselines."""
    match = _FINAL.fullmatch(_RAY_VERSION)
    return match is not None and tuple(int(v) for v in match.groups()) in {
        (2, 58, 0),
        (2, 59, 0),
    }


def decode_predicate(expression: Any) -> PredicateNode | None:
    """Snapshot supported public-module Ray nodes without retaining mutable values."""
    if not supports_predicate_pushdown():
        return None
    from ray.data.expressions import (
        BinaryExpr,
        ColumnExpr,
        LiteralExpr,
        Operation,
        UnaryExpr,
    )

    visited = 0

    def decode(expr: Any, depth: int) -> PredicateNode | None:
        nonlocal visited
        children: tuple[PredicateNode | None, ...]
        visited += 1
        if visited > 256 or depth > 32:
            return None
        if type(expr) is ColumnExpr:
            return PredicateNode("column", value=expr.name)
        if type(expr) is LiteralExpr:
            if type(expr.value) not in {str, bytes, int, bool, type(None)}:
                return None
            return PredicateNode("literal", value=expr.value)
        if type(expr) in {BinaryExpr, UnaryExpr} and type(expr.op) is not Operation:
            return None
        if type(expr) is BinaryExpr:
            if expr.op.value not in {"eq", "ne", "lt", "le", "gt", "ge", "and", "or"}:
                return None
            children = (decode(expr.left, depth + 1), decode(expr.right, depth + 1))
        elif type(expr) is UnaryExpr:
            if expr.op.value not in {"not", "is_null", "is_not_null"}:
                return None
            children = (decode(expr.operand, depth + 1),)
        else:
            return None
        if any(child is None for child in children):
            return None
        return PredicateNode(expr.op.value, tuple(c for c in children if c is not None))

    return decode(expression, 0)


def combine_predicates(previous: Any, expression: Any) -> Any:
    """Keep Ray's reported predicate consistent with the compiled conjunction."""
    return expression if previous is None else previous & expression


def ensure_supported_ray_version() -> None:
    """Reject Ray versions outside the verified final-release window."""
    match = _FINAL.fullmatch(_RAY_VERSION)
    version = tuple(int(part) for part in match.groups()) if match is not None else ()
    if not _MIN_RAY <= version < _MAX_RAY:
        raise ConfigurationError(
            "ray-clickhouse supports final Ray releases >=2.55,<2.60; "
            "PEP 440 local build suffixes are allowed, but prerelease, dev, and "
            f"post-release builds are unsupported; found Ray {_RAY_VERSION!r}"
        )


def make_read_task(
    read_fn: Callable[[], Iterable[pa.Table]],
    metadata: BlockMetadata,
    schema: pa.Schema,
    per_task_row_limit: int | None,
) -> ReadTask:
    ensure_supported_ray_version()
    parameters = inspect.signature(ReadTask).parameters
    if "schema" not in parameters:
        raise ConfigurationError(
            "installed Ray ReadTask lacks required schema support within the final "
            "Ray >=2.55,<2.60 compatibility window"
        )
    kwargs: dict[str, Any] = {
        "read_fn": read_fn,
        "metadata": metadata,
        "schema": schema,
    }
    if "per_task_row_limit" in parameters:
        kwargs["per_task_row_limit"] = per_task_row_limit
    elif per_task_row_limit is not None:
        raise ConfigurationError(
            "installed Ray ReadTask lacks per_task_row_limit support"
        )
    return ReadTask(**kwargs)


def prepare_write_remote_args(args: Mapping[str, Any] | None) -> dict[str, Any]:
    remote_args = dict(args or {})
    for key in ("max_retries", "max_task_retries"):
        if key in remote_args and remote_args[key] != 0:
            raise ConfigurationError(
                "ClickHouse writes require Ray task retries to be zero"
            )
    if remote_args.get("retry_exceptions"):
        raise ConfigurationError("ClickHouse writes do not support exception retries")
    remote_args["max_retries"] = 0
    return remote_args
