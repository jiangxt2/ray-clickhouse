"""Bounded test-only capture of connector events in the process executing I/O."""

from __future__ import annotations

import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from ray_clickhouse._logging import _FIELDS


@contextmanager
def capture_events() -> Iterator[list[dict[str, Any]]]:
    records: list[dict[str, Any]] = []
    overflow = False

    class Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            nonlocal overflow
            if hasattr(record, "event"):
                if len(records) >= 1000:
                    overflow = True
                    return
                records.append(
                    {
                        key: value
                        for key, value in vars(record).items()
                        if key in _FIELDS or key in {"event", "levelno"}
                    }
                )

    logger = logging.getLogger("ray_clickhouse")
    level = logger.level
    handler = Capture()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    try:
        yield records
        assert not overflow, "test event capture overflow"
    finally:
        logger.removeHandler(handler)
        logger.setLevel(level)


def execute_read_task(task: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    previous = os.environ.get("RAY_CLICKHOUSE_LOG_BATCHES")
    # The task's driver snapshot must take precedence over this worker value.
    os.environ["RAY_CLICKHOUSE_LOG_BATCHES"] = "0"
    try:
        with capture_events() as records:
            rows = [row for block in task() for row in block.to_pylist()]
        return rows, records
    finally:
        if previous is None:
            os.environ.pop("RAY_CLICKHOUSE_LOG_BATCHES", None)
        else:
            os.environ["RAY_CLICKHOUSE_LOG_BATCHES"] = previous
