import pytest

from ray_clickhouse._errors import ObjectNotFoundError
from ray_clickhouse._models import ClickHouseConnection, QualifiedTable
from ray_clickhouse.datasource import ClickHouseDatasource, ClickHouseReadConfig

pytestmark = [pytest.mark.integration, pytest.mark.cluster, pytest.mark.distributed]


def test_missing_selected_physical_target_is_a_preflight_error(cluster_tables):
    tables = cluster_tables
    tables.clients[2].command(f"DROP TABLE ray_clickhouse_it.{tables.local} SYNC")
    source = ClickHouseDatasource(
        ClickHouseReadConfig(
            ClickHouseConnection("clickhouse", "ray_clickhouse_it"),
            QualifiedTable("ray_clickhouse_it", tables.distributed),
            filter_sql="id >= 0",
            convert_distributed_to_local=True,
        )
    )
    with pytest.raises(ObjectNotFoundError):
        source.get_read_tasks(4)
    assert source._planning_snapshot is None
