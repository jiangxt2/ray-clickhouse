"""Pure planning helpers for ClickHouse physical split groups."""

from __future__ import annotations

from collections.abc import Iterable

from ray_clickhouse._discovery import PartitionInfo, RangeFacts
from ray_clickhouse._errors import DiscoveryError


def group_partitions(
    partitions: Iterable[PartitionInfo], *, target_tasks: int, max_tasks: int
) -> tuple[tuple[str, ...], ...]:
    items = tuple(partitions)
    if not items:
        return ()
    group_count = min(len(items), target_tasks, max_tasks)
    buckets: list[list[str]] = [[] for _ in range(group_count)]
    weights = [0] * group_count
    for item in sorted(
        items, key=lambda value: (-value.bytes_on_disk, value.partition_id)
    ):
        index = min(range(group_count), key=lambda value: (weights[value], value))
        buckets[index].append(item.partition_id)
        weights[index] += item.bytes_on_disk
    return tuple(tuple(sorted(bucket)) for bucket in buckets if bucket)


def plan_integer_ranges(
    facts: RangeFacts, *, target_tasks: int, max_tasks: int
) -> tuple[tuple[int | None, int | None, bool], ...]:
    if facts.total_rows == 0:
        return ()
    if facts.minimum is None or facts.maximum is None:
        return ((None, None, facts.null_rows > 0),)
    task_count = min(max(1, target_tasks), max_tasks, facts.total_rows)
    if facts.minimum == facts.maximum or task_count == 1:
        return ((facts.minimum, None, facts.null_rows > 0),)
    width = facts.maximum - facts.minimum + 1
    step = max(1, (width + task_count - 1) // task_count)
    ranges: list[tuple[int | None, int | None, bool]] = []
    lower = facts.minimum
    for index in range(task_count):
        upper = (
            None if index == task_count - 1 else min(facts.maximum + 1, lower + step)
        )
        ranges.append((lower, upper, index == 0 and facts.null_rows > 0))
        if upper is None:
            break
        lower = upper
        if lower >= facts.maximum + 1:
            break
    if not ranges:
        raise DiscoveryError("range planner produced no constraints")
    return tuple(ranges)
