from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from clickhouse_connect.driver.binding import bind_query

from ray_clickhouse import read_clickhouse
from ray_clickhouse._errors import ConfigurationError
from ray_clickhouse._models import QualifiedTable
from ray_clickhouse._sql import build_select


def _select(predicate, parameters, **kwargs):
    return build_select(
        table=QualifiedTable("analytics", "events"),
        columns=("id",),
        filter_sql=predicate,
        parameters=parameters,
        **kwargs,
    )


@pytest.mark.parametrize(
    "hint",
    [
        "String",
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "Float32",
        "Float64",
        "Bool",
        "Date",
        "Date32",
        "DateTime",
        "DateTime('UTC')",
        "DateTime64(6)",
        "DateTime64(6, 'Asia/Shanghai')",
        "Decimal(20,4)",
        "Decimal32(2)",
        "Decimal64(2)",
        "Decimal128(2)",
        "FixedString(3)",
        "Nullable(UInt32)",
        "Nullable(DateTime64(6,'UTC'))",
    ],
)
def test_supported_type_hints_reach_generated_sql(hint):
    sql, parameters = _select(f"id = {{value:{hint}}}", {"value": 1})
    assert f"{{value:{hint}}}" in sql
    assert dict(parameters) == {"value": 1}


@pytest.mark.parametrize(
    ("predicate", "parameters"),
    [
        ("id = {missing:UInt32}", {}),
        ("id = {value:UInt32}", {"value": 1, "unused": 2}),
        ("id = {value:UInt32} OR id = %(other)s", {"value": 1, "other": 2}),
        ("id = {value}", {"value": 1}),
        ("id = {value:}", {"value": 1}),
        ("id = {value:Array(UInt32)}", {"value": [1]}),
        ("id = {value:Identifier}", {"value": "id"}),
        ("id = {value:Nullable(Nullable(UInt32))}", {"value": 1}),
        ("id = {value:DateTime64(10)}", {"value": 1}),
        ("id = {value:DateTime64(-1)}", {"value": 1}),
        ("id = {value:DateTime(1)}", {"value": 1}),
        ("id = {value:Decimal(39,1)}", {"value": 1}),
        ("id = {value:Decimal(10,11)}", {"value": 1}),
        ("id = {value:FixedString(0)}", {"value": 1}),
        ("id = {value:UInt32(1)}", {"value": 1}),
        ("id = {value:UInt32} OR id = {value:Int32}", {"value": 1}),
        ("id = %(value)d", {"value": 1}),
        ("id = %s", {"value": 1}),
        ("id = {value:UInt32} AND text = '{other:String}'", {"value": 1}),
        ("id = %(value)s /* {other:UInt32} */", {"value": 1}),
        ("id = %(value)s -- {other:UInt32}", {"value": 1}),
        ("id = {value:UInt32} AND text = 'unterminated", {"value": 1}),
        ("id = {value:UInt32} /* unterminated", {"value": 1}),
    ],
)
def test_invalid_forms_fail_before_discovery(predicate, parameters):
    with (
        patch("ray_clickhouse.datasource.discover_schema") as discovery,
        pytest.raises(ConfigurationError),
    ):
        read_clickhouse(
            host="localhost",
            database="analytics",
            table="events",
            filter=predicate,
            query_parameters=parameters,
        )
    discovery.assert_not_called()


def test_repeated_types_and_driver_precision_are_consistent():
    value = datetime(2026, 10, 3, 1, 2, 3, 456789, tzinfo=timezone.utc)
    sql, parameters = _select(
        "ts >= {minimum:DateTime64(6, 'UTC')} OR ts = {minimum:DateTime64(6,'UTC')}",
        {"minimum": value},
    )
    bound_sql, bindings = bind_query(sql, dict(parameters))
    assert bound_sql == sql
    assert bindings["param_minimum"].endswith(".456789")


def test_generated_constraints_share_native_protocol_and_preserve_extreme_bound():
    sql, parameters = _select(
        "tenant = {tenant:UInt8}",
        {"tenant": 1},
        partition_ids=("202601",),
        range_column="id",
        range_lower=0,
        range_upper=2**64,
        range_include_null=True,
    )
    assert "%(" not in sql
    assert "{__ray_clickhouse_partition_0:String}" in sql
    assert "{__ray_clickhouse_range_upper:Int128}" in sql
    assert "IS NULL" in sql
    bound_sql, bindings = bind_query(sql, dict(parameters))
    assert bound_sql == sql
    assert bindings["param___ray_clickhouse_range_upper"] == str(2**64)


def test_literal_native_lookalike_is_rejected_when_internal_values_are_bound():
    with pytest.raises(ConfigurationError, match="quotes or comments"):
        _select("text = '{literal:UInt32}'", None, partition_ids=("1",))


def test_client_binding_and_unbound_literal_behavior_remain_available():
    sql, parameters = _select(
        "id = %(value)s", {"value": 3}, range_column="id", range_upper=4
    )
    bound_sql, bindings = bind_query(sql, dict(parameters))
    assert "id = 3" in bound_sql
    assert "`id` < 4" in bound_sql
    assert not bindings
    sql, _ = _select("text = '{literal:UInt32}'", None)
    assert "'{literal:UInt32}'" in sql


_CLIENT_COMMENT_PREDICATES = (
    "id = %(value)s /* %(note)s */",
    "id = %(value)s -- %(note)s\n AND id > 0",
    "id = %(value)s # %(note)s\n AND id > 0",
)


@pytest.mark.parametrize("predicate", _CLIENT_COMMENT_PREDICATES)
def test_client_comment_parameters_preserve_public_api_and_driver_binding(predicate):
    with patch("ray_clickhouse._api.ray.data.read_datasource", return_value="dataset"):
        assert (
            read_clickhouse(
                host="localhost",
                database="analytics",
                table="events",
                filter=predicate,
                query_parameters={"value": 3, "note": "trace"},
            )
            == "dataset"
        )
    sql, parameters = _select(predicate, {"value": 3, "note": "trace"})
    bound_sql, bindings = bind_query(sql, dict(parameters))
    assert "id = 3" in bound_sql
    assert "'trace'" in bound_sql
    assert not bindings


@pytest.mark.parametrize("predicate", _CLIENT_COMMENT_PREDICATES)
def test_missing_client_comment_parameter_fails_before_read(predicate):
    with (
        patch("ray_clickhouse._api.ray.data.read_datasource") as read,
        patch("ray_clickhouse.datasource.discover_schema") as discovery,
        pytest.raises(ConfigurationError, match="missing parameters.*note"),
    ):
        read_clickhouse(
            host="localhost",
            database="analytics",
            table="events",
            filter=predicate,
            query_parameters={"value": 3},
        )
    read.assert_not_called()
    discovery.assert_not_called()


def test_native_binding_leaves_client_comment_lookalike_unformatted():
    sql, parameters = _select("id = {value:UInt32} /* %(note)s */", {"value": 3})
    bound_sql, bindings = bind_query(sql, dict(parameters))
    assert bound_sql == sql
    assert "%(note)s" in bound_sql
    assert bindings == {"param_value": "3"}
