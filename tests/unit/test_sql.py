import pytest

from ray_clickhouse._errors import ConfigurationError
from ray_clickhouse._models import QualifiedTable
from ray_clickhouse._sql import build_select, normalize_columns


def test_build_select_binds_predicate_and_partition_values() -> None:
    sql, parameters = build_select(
        table=QualifiedTable("analytics", "events"),
        columns=("id", "score"),
        filter_sql="score >= %(minimum)s",
        parameters={"minimum": 25},
        partition_ids=("202601", "202602"),
    )

    assert sql == (
        "SELECT `id`, `score` FROM `analytics`.`events` "
        "WHERE (score >= %(minimum)s) "
        "AND (`_partition_id` IN (%(__ray_clickhouse_partition_0)s, "
        "%(__ray_clickhouse_partition_1)s))"
    )
    assert dict(parameters) == {
        "minimum": 25,
        "__ray_clickhouse_partition_0": "202601",
        "__ray_clickhouse_partition_1": "202602",
    }


def test_build_select_range_null_is_grouped_with_filter() -> None:
    sql, parameters = build_select(
        table=QualifiedTable("analytics", "events"),
        columns=("id",),
        filter_sql="tenant_id = %(tenant)s",
        parameters={"tenant": 1},
        range_column="id",
        range_lower=10,
        range_upper=20,
        range_include_null=True,
    )

    assert "WHERE ((tenant_id = %(tenant)s) AND ((`id` >=" in sql
    assert "OR (`id` IS NULL))" in sql
    assert dict(parameters)["tenant"] == 1


def test_filter_parameter_usage_is_fail_closed() -> None:
    table = QualifiedTable("analytics", "events")
    with pytest.raises(ConfigurationError):
        build_select(
            table=table,
            columns=("id",),
            filter_sql="id = %(missing)s",
            parameters={},
        )
    with pytest.raises(ConfigurationError):
        build_select(
            table=table,
            columns=("id",),
            filter_sql="id = 1",
            parameters={"unused": 1},
        )


def test_filter_rejects_multiple_statements() -> None:
    with pytest.raises(ConfigurationError):
        build_select(
            table=QualifiedTable("analytics", "events"),
            columns=("id",),
            filter_sql="id = 1; DROP TABLE events",
        )


def test_filter_rejects_full_sql_clauses() -> None:
    with pytest.raises(ConfigurationError):
        build_select(
            table=QualifiedTable("analytics", "events"),
            columns=("id",),
            filter_sql="id > 1 GROUP BY id",
        )
    sql, _ = build_select(
        table=QualifiedTable("analytics", "events"),
        columns=("id",),
        filter_sql="label = 'GROUP BY'",
    )
    assert "label = 'GROUP BY'" in sql
    sql, _ = build_select(
        table=QualifiedTable("analytics", "events"),
        columns=("id",),
        filter_sql="payload = '%(literal)s'",
    )
    assert "payload = '%(literal)s'" in sql


def test_build_select_renders_order_by_before_limit() -> None:
    sql, _ = build_select(
        table=QualifiedTable("analytics", "events"),
        columns=("id",),
        filter_sql=None,
        order_by=(("id", True),),
        limit=10,
    )

    assert sql.endswith("ORDER BY `id` DESC LIMIT 10")


def test_columns_are_normalized_and_validated() -> None:
    assert normalize_columns(["id", "score"]) == ("id", "score")
    with pytest.raises(ConfigurationError):
        normalize_columns(["id", "id"])
    with pytest.raises(ConfigurationError):
        normalize_columns(["id` OR 1=1"])
