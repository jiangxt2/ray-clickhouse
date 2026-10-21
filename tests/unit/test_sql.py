import pytest
from clickhouse_connect.driver.binding import bind_query

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


@pytest.mark.parametrize(
    "predicate",
    [
        "txt LIKE 'row-%'",
        "txt NOT LIKE 'row-%'",
        r"txt LIKE '100\\%'",
        r"txt LIKE 'a\\%%'",
        "txt IN ('100%', '%s', '%(literal)s')",
        "txt NOT IN ('100%', '%s', '%(literal)s')",
        "txt BETWEEN 'a%' AND 'z'",
        "txt NOT BETWEEN 'a%' AND 'z'",
        "txt = '%(literal)s'",
    ],
)
@pytest.mark.parametrize(
    "constraints",
    [
        {"partition_ids": ("p0", "p'1%")},
        {"range_column": "id", "range_lower": -3},
        {"range_column": "id", "range_upper": 2**64},
        {"range_column": "id", "range_lower": 0, "range_upper": 10},
    ],
)
def test_internal_split_bindings_preserve_literal_percent_sql(predicate, constraints):
    sql, parameters = build_select(
        table=QualifiedTable("analytics", "events"),
        columns=("id", "txt"),
        filter_sql=predicate,
        **constraints,
    )
    bound_sql, bindings = bind_query(sql, dict(parameters))
    assert bound_sql == sql
    assert f"({predicate})" in bound_sql
    assert set(bindings) == {f"param_{name}" for name, _ in parameters}
    assert bindings


@pytest.mark.parametrize(
    "predicate,parameters,native",
    [
        ("txt LIKE %(pattern)s", {"pattern": "row-%"}, False),
        ("txt LIKE {pattern:String}", {"pattern": "row-%"}, True),
        (None, None, False),
    ],
)
def test_explicit_binding_protocols_and_unfiltered_splits_are_preserved(
    predicate, parameters, native
):
    sql, values = build_select(
        table=QualifiedTable("analytics", "events"),
        columns=("id", "txt"),
        filter_sql=predicate,
        parameters=parameters,
        partition_ids=("p0",),
    )
    bound_sql, bindings = bind_query(sql, dict(values))
    if native:
        assert bound_sql == sql
        assert bindings["param_pattern"] == "row-%"
        assert bindings["param___ray_clickhouse_partition_0"] == "p0"
    else:
        assert not bindings
        assert "`_partition_id` IN ('p0')" in bound_sql
        if predicate is not None:
            assert "txt LIKE 'row-%'" in bound_sql
