from ray_clickhouse._discovery import PartitionInfo, RangeFacts
from ray_clickhouse._planning import group_partitions, plan_integer_ranges


def test_group_partitions_is_deterministic_and_bounded() -> None:
    groups = group_partitions(
        [
            PartitionInfo("p1", 100, 10, 1000),
            PartitionInfo("p2", 200, 20, 2000),
            PartitionInfo("p3", 50, 5, 500),
        ],
        target_tasks=2,
        max_tasks=2,
    )
    assert len(groups) == 2
    assert sorted(partition for group in groups for partition in group) == [
        "p1",
        "p2",
        "p3",
    ]


def test_integer_ranges_cover_bounds_without_overlap() -> None:
    ranges = plan_integer_ranges(
        RangeFacts("id", "UInt64", 100, 0, 0, 99),
        target_tasks=4,
        max_tasks=4,
    )
    assert ranges[0][0] == 0
    assert ranges[-1][1] is None
    assert all(
        ranges[index][1] is not None and ranges[index][1] <= ranges[index + 1][0]
        for index in range(len(ranges) - 1)
    )


def test_integer_ranges_put_nulls_in_first_range() -> None:
    ranges = plan_integer_ranges(
        RangeFacts("id", "Nullable(UInt64)", 101, 1, 0, 99),
        target_tasks=2,
        max_tasks=2,
    )
    assert ranges[0][2] is True
    assert ranges[1][2] is False
