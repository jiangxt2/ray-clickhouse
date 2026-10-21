"""Small compatibility boundary for supported Ray releases."""

from __future__ import annotations

import inspect
import re
from collections.abc import Callable, Iterable, Mapping
from typing import Any

import pyarrow as pa
import ray
from ray.data.block import BlockAccessor, BlockMetadata
from ray.data.datasource import ReadTask

from ray_clickhouse._errors import ConfigurationError

_MIN_RAY = (2, 55, 0)
_MAX_RAY = (2, 60, 0)
_RAY_VERSION = ray.__version__
_FINAL = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:\+[A-Za-z0-9]+(?:[._-][A-Za-z0-9]+)*)?$")


def supports_v1_projection_pushdown() -> bool:
    """Enable only the verified V1 projection and empty-block contracts."""
    match = _FINAL.fullmatch(_RAY_VERSION)
    return match is not None and tuple(int(p) for p in match.groups()) in {
        (2, 58, 0),
        (2, 59, 0),
    }


def empty_projection(table: pa.Table) -> pa.Table:
    """Use Ray's public accessor to retain its native row-count placeholder."""
    result = BlockAccessor.for_block(table).select([])
    if not isinstance(result, pa.Table):
        raise ConfigurationError("Ray empty projection did not return an Arrow table")
    return result


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
