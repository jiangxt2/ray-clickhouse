"""Native metadata operations enable local reads across server baselines."""

from collections import Counter

import pytest

from ray_clickhouse._models import ClickHouseConnection, QualifiedTable
from ray_clickhouse.datasource import ClickHouseDatasource, ClickHouseReadConfig

pytestmark = [pytest.mark.integration, pytest.mark.cluster, pytest.mark.distributed]


def test_native_metadata_selects_local_plan_without_check_grant(
    cluster_tables, record_property
):
    tables = cluster_tables
    source = ClickHouseDatasource(
        ClickHouseReadConfig(
            ClickHouseConnection("clickhouse", "ray_clickhouse_it"),
            QualifiedTable("ray_clickhouse_it", tables.distributed),
            filter_sql="id >= 0",
            convert_distributed_to_local=True,
        )
    )
    tasks = source.get_read_tasks(4)
    rows = [row for task in tasks for block in task() for row in block.to_pylist()]
    assert Counter(tuple(sorted(row.items())) for row in rows) == Counter(
        tuple(sorted(row.items())) for row in tables.rows
    )
    assert source._local_sources
    assert len(tasks) == 2
    assert dict(source._resolved_distributed.capabilities.origins)[
        "policy-inventory"
    ] == ("system.row_policies+native-cardinality")
    record_property(
        "clickhouse_version", tables.clients[0].query("SELECT version()").first_row[0]
    )
