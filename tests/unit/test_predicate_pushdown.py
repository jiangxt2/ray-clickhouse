"""Compiler and planner rejection must preserve the native Ray filter contract."""

from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest
from clickhouse_connect import get_client
from clickhouse_connect.driver.binding import bind_query
from ray.data.expressions import col, lit

from ray_clickhouse import _compat, _discovery
from ray_clickhouse._discovery import DiscoverySnapshot, RangeFacts
from ray_clickhouse._errors import DiscoveryError, PermissionError, TransportError
from ray_clickhouse._models import ClickHouseConnection, QualifiedTable, ResourceLimits
from ray_clickhouse._predicates import PredicateNode, compile_predicate
from ray_clickhouse._schema import SchemaPlan, TargetColumn
from ray_clickhouse._sql import build_select, predicate_binding_protocol
from ray_clickhouse.datasource import ClickHouseDatasource, ClickHouseReadConfig


def schema(declaration="Nullable(Int64)", dtype=None):
    dtype = dtype if dtype is not None else pa.int64()
    columns = (
        TargetColumn("id", declaration, "", "", 1),
        TargetColumn("txt", "String", "", "", 2),
        TargetColumn("flag", "Bool", "", "", 3),
    )
    return SchemaPlan(
        pa.schema([("id", dtype), ("txt", pa.string()), ("flag", pa.bool_())]), columns
    )


@pytest.fixture(autouse=True)
def verified_ray(monkeypatch):
    monkeypatch.setattr(_compat, "_RAY_VERSION", "2.59.0")


def compiled(expr, plan=None, native=True):
    node = _compat.decode_predicate(expr)
    assert node is not None
    return compile_predicate(node, plan or schema(), native=native)


@pytest.mark.parametrize("op", ["eq", "ne", "lt", "le", "gt", "ge"])
@pytest.mark.parametrize(
    "declaration,dtype,value",
    [
        ("Int8", pa.int8(), -128),
        ("Int8", pa.int8(), 127),
        ("UInt8", pa.uint8(), 255),
        ("Int16", pa.int16(), -32768),
        ("UInt16", pa.uint16(), 65535),
        ("Int32", pa.int32(), -(2**31)),
        ("UInt32", pa.uint32(), 2**32 - 1),
        ("Int64", pa.int64(), -(2**63)),
        ("Int64", pa.int64(), 2**63 - 1),
        ("LowCardinality(Nullable(Int32))", pa.int32(), 3),
    ],
)
def test_exact_integer_types_and_reversed_operands(op, declaration, dtype, value):
    for left, right in [(col("id"), lit(value)), (lit(value), col("id"))]:
        expr = getattr(left, f"__{op}__")(right)
        result = compiled(expr, schema(declaration, dtype))
        assert result is not None
        assert result.columns == {"id"}
        assert result.parameters == (("__ray_clickhouse_expr_0", value),)
        assert result.sql.count("{__ray_clickhouse_expr_0:") == 1


@pytest.mark.parametrize(
    "expr",
    [
        col("id") == None,  # noqa: E711 - verify NULL comparison is not rewritten
        col("id") == True,  # noqa: E712 - Bool must not masquerade as an integer
        col("id") == "3",
        col("id") == col("flag"),
        col("flag") > True,
        col("txt") < "a",
        col("missing") == 1,
        col("id.bad") == 1,
        col("id"),
    ],
)
def test_reject_semantic_mismatches(expr):
    assert compiled(expr) is None


@pytest.mark.parametrize(
    "declaration,dtype,value",
    [
        ("Int8", pa.int8(), 128),
        ("UInt8", pa.uint8(), -1),
        ("UInt64", pa.uint64(), 1),
        ("FixedString(3)", pa.binary(3), "a"),
        ("Enum8('a'=1)", pa.string(), "a"),
        ("Date32", pa.date32(), 1),
        ("SimpleAggregateFunction(sum,Int64)", pa.int64(), 1),
        ("Int32", pa.int64(), 1),
    ],
)
def test_reject_unsupported_types_and_coercions(declaration, dtype, value):
    assert compiled(col("id") == value, schema(declaration, dtype)) is None


def test_uint64_null_check_does_not_introduce_a_cast():
    result = compiled(col("id").is_null(), schema("Nullable(UInt64)", pa.uint64()))
    assert result is not None and result.sql == "(`id` IS NULL)"
    assert not result.parameters


def test_boolean_composition_and_binding_do_not_inline_values():
    secret = "quote'\\\n{not_a_parameter:Int64}"
    expr = ((col("id") >= 3) | col("id").is_null()) & ~(col("txt") == secret)
    for native in (True, False):
        result = compiled(expr, native=native)
        assert result is not None
        assert secret not in repr(result) and secret not in result.sql
        sql, parameters = build_select(
            table=QualifiedTable("db", "events"),
            columns=("id",),
            filter_sql="flag = {user:Bool}" if native else "flag = %(user)s",
            parameters={"user": True},
            pushed_predicate=result,
            range_column="id",
            range_lower=0,
            range_upper=10,
            range_include_null=True,
            partition_ids=("p",),
        )
        rendered, bindings = bind_query(sql, dict(parameters))
        assert ("param_user" in bindings) is native
        assert ("%(__ray_clickhouse" in rendered) is False
        assert "IS NULL" in sql and dict(parameters)["user"] is True
        assert dict(parameters)["__ray_clickhouse_expr_1"] == secret


@pytest.mark.parametrize("expr", [col("flag"), ~col("flag"), col("flag").__ne__(False)])
def test_bool_predicates_have_distributed_comparison_shape(expr):
    from ray_clickhouse._distributed import predicate_columns

    result = compiled(expr)
    assert result is not None
    assert predicate_columns(result.sql, schema().columns) == {"flag"}


def test_decoder_rejects_unknown_children_and_bounded_traversal():
    unsupported = col("id").is_in([1, 2])
    assert _compat.decode_predicate((col("id") > 1) | unsupported) is None
    assert _compat.decode_predicate((col("id") > 1) & unsupported) is None
    assert _compat.decode_predicate(col("id") + 1 > 2) is None
    expr = col("id") > 0
    for _ in range(40):
        expr = expr & (col("id") > 1)
    assert _compat.decode_predicate(expr) is None
    node = PredicateNode("column", value="flag")
    for _ in range(40):
        node = PredicateNode("not", (node,))
    assert compile_predicate(node, schema(), native=True) is None


@pytest.mark.parametrize(
    "version,expected",
    [
        ("2.55.0", False),
        ("2.57.0", False),
        ("2.58.0", True),
        ("2.59.0+local.1", True),
        ("2.59.1", False),
        ("2.59.0rc1", False),
    ],
)
def test_exact_feature_version_gate(monkeypatch, version, expected):
    monkeypatch.setattr(_compat, "_RAY_VERSION", version)
    assert _compat.supports_predicate_pushdown() is expected


def test_ambiguous_user_sql_retains_legacy_behavior():
    assert predicate_binding_protocol("txt = '{literal:UInt32}'", {}) is None
    assert predicate_binding_protocol("id = %(p)s", {"p": 1}) is False
    assert predicate_binding_protocol("id = {p:Int32}", {"p": 1}) is True


def make_source(**kwargs):
    return ClickHouseDatasource(
        ClickHouseReadConfig(
            ClickHouseConnection("unused", "db"),
            QualifiedTable("db", "events"),
            **kwargs,
        )
    )


def test_clones_invalidate_all_filter_dependent_state_and_preserve_original():
    source = make_source()
    original_snapshot = DiscoverySnapshot(schema(), "MergeTree")
    source._planning_snapshot = original_snapshot
    source._logical_fallback = True
    with patch(
        "ray_clickhouse.datasource.discover_predicate_context", return_value=True
    ):
        first = source.apply_predicate(col("id") >= 3)
        second = source.apply_predicate(col("id") < 0)
        chained = first.apply_predicate(col("flag"))
        rejected = first.apply_predicate((col("id") > 5) | col("id").is_in([1]))
    assert first is not source and second is not first and chained is not first
    assert rejected is first
    assert source._planning_snapshot is original_snapshot and source._logical_fallback
    assert source.get_current_predicate() is None
    for clone in (first, second, chained):
        assert clone._planning_snapshot is None
        assert clone._resolved_distributed is None and not clone._local_sources
        assert not clone._logical_fallback
    assert len(chained._pushed_predicate.parameters) == 2
    assert first._pushed_predicate.parameters[0][1] == 3
    assert second._pushed_predicate.parameters[0][1] == 0


def test_columns_context_and_binding_rejection_do_not_mutate_source():
    source = make_source(columns=("txt",))
    with (
        patch("ray_clickhouse.datasource.discover_schema", return_value=schema()),
        patch("ray_clickhouse.datasource.discover_predicate_context") as context,
    ):
        assert source.apply_predicate(col("id") > 0) is source
        context.assert_not_called()
    source = make_source()
    with (
        patch("ray_clickhouse.datasource.discover_schema", return_value=schema()),
        patch(
            "ray_clickhouse.datasource.discover_predicate_context", return_value=False
        ),
    ):
        assert source.apply_predicate(col("id") > 0) is source
        assert source._planning_snapshot is None and source._pushed_predicate is None


def test_context_errors_propagate_and_range_discovery_receives_the_predicate():
    source = make_source(split="range", range_column="id")
    with (
        patch("ray_clickhouse.datasource.discover_schema", return_value=schema()),
        patch(
            "ray_clickhouse.datasource.discover_predicate_context",
            side_effect=PermissionError("denied"),
        ),
        pytest.raises(PermissionError),
    ):
        source.apply_predicate(col("id") > 0)
    facts = RangeFacts("id", "Int64", 2, 0, 1, 2)
    with (
        patch("ray_clickhouse.datasource.discover_schema", return_value=schema()),
        patch(
            "ray_clickhouse.datasource.discover_predicate_context", return_value=True
        ),
        patch("ray_clickhouse.datasource.discover_engine", return_value="MergeTree"),
        patch(
            "ray_clickhouse.datasource.discover_range_facts", return_value=facts
        ) as discover,
    ):
        clone = source.apply_predicate(col("id") > 0)
        assert clone.get_read_tasks(2)
        assert discover.call_args.kwargs["pushed_predicate"] is clone._pushed_predicate


NEUTRAL_AST_LINES = [
    "SelectWithUnionQuery (children 1)",
    " ExpressionList (children 1)",
    "  SelectQuery (children 1)",
    "   ExpressionList (children 1)",
    "    Literal UInt64_1",
]


NEUTRAL_AST_BYTES = ("\n".join(NEUTRAL_AST_LINES) + "\n").encode()


def context_client(aggregate_rows=((1,),), *, ast_result=NEUTRAL_AST_BYTES):
    client = Mock()
    client.query.return_value = SimpleNamespace(
        result_rows=aggregate_rows, column_names=["predicate_context"]
    )
    client.raw_query.return_value = ast_result
    return client


@pytest.mark.parametrize(
    "name", ["final", "limit", "offset", "page", "max_result_rows", "max_result_bytes"]
)
@pytest.mark.parametrize("value", [0, 0.0, False, "0", "0.0", "-0", "+0e2"])
def test_explicit_zero_controls_keep_the_effective_probe(name, value):
    client = context_client()
    connection = ClickHouseConnection("unused", "db", settings=((name, value),))
    with patch.object(_discovery, "_open", return_value=client):
        assert _discovery.discover_predicate_context(connection, ResourceLimits())
    assert client.query.call_args_list[0].kwargs["settings"][name] == value
    query = client.query.call_args_list[0].args[0]
    assert "toUInt8" in query and "uniqExact(name)" in query
    for setting in ("limit", "offset", "page", "select", "filter", "order", "sort"):
        assert f"name = '{setting}'" in query
    client.close.assert_called_once()


@pytest.mark.parametrize(
    "name", ["final", "limit", "offset", "page", "max_result_rows", "max_result_bytes"]
)
@pytest.mark.parametrize(
    "value", [1, -1, 0.5, True, "1", "0.5", "invalid", None, float("nan")]
)
def test_explicit_result_controls_reject_without_opening_a_probe(name, value):
    connection = ClickHouseConnection("unused", "db", settings=((name, value),))
    with patch.object(_discovery, "_open") as open_client:
        assert not _discovery.discover_predicate_context(connection, ResourceLimits())
    open_client.assert_not_called()


@pytest.mark.parametrize(
    "name,value",
    [
        ("additional_result_filter", "id > 0"),
        ("additional_table_filters", {"db.events": "id > 0"}),
        ("read_overflow_mode", "break"),
        ("timeout_overflow_mode", "break"),
        ("group_by_overflow_mode", "any"),
        ("sort_overflow_mode", "break"),
        ("select", "id"),
        ("filter", "id > 0"),
        ("order", "id"),
        ("sort", "-id"),
    ],
)
def test_explicit_rewrites_and_partial_results_remain_residual(name, value):
    connection = ClickHouseConnection(
        "unused", "db", client_options=(("settings", {name: value}),)
    )
    with patch.object(_discovery, "_open") as open_client:
        assert not _discovery.discover_predicate_context(connection, ResourceLimits())
    open_client.assert_not_called()


def test_request_settings_override_client_defaults_without_mutation():
    defaults = {"limit": 100, "additional_table_filters": {}}
    client = context_client()
    connection = ClickHouseConnection(
        "unused",
        "db",
        settings=(("limit", 0),),
        client_options=(("settings", defaults),),
    )
    with patch.object(_discovery, "_open", return_value=client):
        assert _discovery.discover_predicate_context(connection, ResourceLimits())
    assert defaults == {"limit": 100, "additional_table_filters": {}}
    assert client.query.call_args_list[0].kwargs["settings"]["limit"] == 0
    connection = ClickHouseConnection(
        "unused",
        "db",
        settings=(("limit", 100),),
        client_options=(("settings", {"limit": 0}),),
    )
    with patch.object(_discovery, "_open") as open_client:
        assert not _discovery.discover_predicate_context(connection, ResourceLimits())
    open_client.assert_not_called()


@pytest.mark.parametrize("defaults", [None, [], "limit=0"])
def test_malformed_client_settings_are_not_assumed_safe(defaults):
    connection = ClickHouseConnection(
        "unused", "db", client_options=(("settings", defaults),)
    )
    with patch.object(_discovery, "_open") as open_client:
        assert not _discovery.discover_predicate_context(connection, ResourceLimits())
    open_client.assert_not_called()


@pytest.mark.parametrize(
    "rows,expected", [([], False), ([(0,)], False), ([(1,)], True)]
)
def test_effective_aggregate_status_controls_acceptance(rows, expected):
    client = context_client(rows)
    with patch.object(_discovery, "_open", return_value=client):
        assert (
            _discovery.discover_predicate_context(
                ClickHouseConnection("unused", "db"), ResourceLimits()
            )
            is expected
        )
    client.close.assert_called_once()
    assert client.query.call_count == 1
    assert client.raw_query.call_count == (1 if expected else 0)


@pytest.mark.parametrize(
    "rows,names",
    [
        ([(2,)], ["predicate_context"]),
        ([(3,)], ["predicate_context"]),
        ([("1",)], ["predicate_context"]),
        ([(True,)], ["predicate_context"]),
        ([(1,), (1,)], ["predicate_context"]),
        ([(1, 0)], ["predicate_context"]),
        ([(1,)], ["wrong"]),
        ([], []),
    ],
)
def test_malformed_context_metadata_remains_an_error(rows, names):
    client = Mock()
    client.query.return_value = SimpleNamespace(result_rows=rows, column_names=names)
    with (
        patch.object(_discovery, "_open", return_value=client),
        pytest.raises(DiscoveryError, match="malformed"),
    ):
        _discovery.discover_predicate_context(
            ClickHouseConnection("unused", "db"), ResourceLimits()
        )
    client.close.assert_called_once()


@pytest.mark.parametrize(
    "error,expected",
    [
        (PermissionError("denied"), PermissionError),
        (RuntimeError("connection reset"), TransportError),
        (KeyboardInterrupt(), KeyboardInterrupt),
    ],
)
@pytest.mark.parametrize("stage", ["aggregate", "ast"])
def test_context_errors_survive_a_secondary_close_failure(error, expected, stage):
    client = context_client()
    if stage == "aggregate":
        client.query.side_effect = error
    else:
        client.raw_query.side_effect = error
    client.close.side_effect = RuntimeError("close")
    with (
        patch.object(_discovery, "_open", return_value=client),
        pytest.raises(expected),
    ):
        _discovery.discover_predicate_context(
            ClickHouseConnection("unused", "db"), ResourceLimits()
        )
    client.close.assert_called_once()


def test_successful_context_probe_exposes_close_failure():
    client = context_client()
    client.close.side_effect = RuntimeError("close")
    with (
        patch.object(_discovery, "_open", return_value=client),
        pytest.raises(TransportError, match="close"),
    ):
        _discovery.discover_predicate_context(
            ClickHouseConnection("unused", "db"), ResourceLimits()
        )


@pytest.mark.parametrize(
    "lines,expected",
    [
        (NEUTRAL_AST_LINES, True),
        ([], False),
        ([*NEUTRAL_AST_LINES, "    Literal Float64_10000"], False),
        (["Subquery (children 1)", *NEUTRAL_AST_LINES], False),
        ([*NEUTRAL_AST_LINES[:-1], "    Literal Float64_1"], False),
        (["UnknownFutureNode"], False),
    ],
)
def test_fixed_ast_only_accepts_the_known_neutral_shape(lines, expected):
    client = context_client(ast_result=("\n".join(lines) + "\n").encode())
    with patch.object(_discovery, "_open", return_value=client):
        assert (
            _discovery.discover_predicate_context(
                ClickHouseConnection("unused", "db"), ResourceLimits()
            )
            is expected
        )
    assert client.raw_query.call_args.args == ("EXPLAIN AST SELECT 1",)
    assert client.raw_query.call_args.kwargs["fmt"] is None
    client.close.assert_called_once()


@pytest.mark.parametrize(
    "response",
    [None, 1, "text", ["text"]],
)
def test_malformed_ast_responses_remain_discovery_errors(response):
    client = context_client(ast_result=response)
    with (
        patch.object(_discovery, "_open", return_value=client),
        pytest.raises(DiscoveryError, match="AST response"),
    ):
        _discovery.discover_predicate_context(
            ClickHouseConnection("unused", "db"), ResourceLimits()
        )
    client.close.assert_called_once()


def test_ast_probe_preserves_settings_and_uses_an_independent_query_id():
    client = context_client()
    defaults = {"offset": 0}
    connection = ClickHouseConnection(
        "unused",
        "db",
        settings=(("limit", 0), ("max_threads", 1)),
        client_options=(("settings", defaults),),
    )
    with patch.object(_discovery, "_open", return_value=client) as open_client:
        assert _discovery.discover_predicate_context(connection, ResourceLimits())
    open_client.assert_called_once_with(connection, ResourceLimits())
    aggregate, ast = client.query.call_args, client.raw_query.call_args
    for call in (aggregate, ast):
        assert call.kwargs["settings"]["limit"] == 0
        assert call.kwargs["settings"]["max_threads"] == 1
    assert (
        aggregate.kwargs["transport_settings"]["query_id"]
        != ast.kwargs["transport_settings"]["query_id"]
    )
    assert ast.kwargs["settings"]["log_comment"].endswith("operation=predicate-shape")
    assert dict(connection.settings) == {"limit": 0, "max_threads": 1}
    assert defaults == {"offset": 0}


@pytest.mark.parametrize("response", [b'[{"explain":"unknown"}]', b"\xff\x00"])
def test_unrecognized_default_output_formats_reject_optimization(response):
    client = context_client(ast_result=response)
    with patch.object(_discovery, "_open", return_value=client):
        assert not _discovery.discover_predicate_context(
            ClickHouseConnection("unused", "db"), ResourceLimits()
        )
    client.close.assert_called_once()


def test_public_driver_raw_query_does_not_append_an_inner_format():
    with patch("clickhouse_connect.driver.httpclient.HttpClient._init_common_settings"):
        client = get_client(host="unused", compress=False)
    try:
        with patch.object(
            client, "_raw_request", return_value=SimpleNamespace(data=NEUTRAL_AST_BYTES)
        ) as request:
            assert (
                client.raw_query("EXPLAIN AST SELECT 1", fmt=None) == NEUTRAL_AST_BYTES
            )
        assert request.call_args.args[0] == "EXPLAIN AST SELECT 1"
    finally:
        client.close()
