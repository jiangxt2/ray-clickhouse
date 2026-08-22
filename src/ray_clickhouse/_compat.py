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

_MIN_RAY = (2, 55, 0)
_MAX_RAY = (2, 56, 0)
_FINAL = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:\+[A-Za-z0-9._-]+)?$")


def ensure_supported_ray_version() -> None:
    match = _FINAL.fullmatch(ray.__version__)
    if match is None:
        raise ConfigurationError(
            "ray-clickhouse requires a final Ray release >=2.55,<2.56; "
            f"found {ray.__version__!r}"
        )
    version = tuple(int(part) for part in match.groups())
    if not _MIN_RAY <= version < _MAX_RAY:
        raise ConfigurationError(
            f"ray-clickhouse requires Ray >=2.55,<2.56; found {ray.__version__!r}"
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
        raise ConfigurationError("installed Ray ReadTask lacks schema support")
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
