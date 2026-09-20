from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from ray_clickhouse._discovery import (
    RangeFacts,
    discover_sorting_key,
    select_auto_range_column,
)
from ray_clickhouse._errors import (
    AuthenticationError,
    ConfigurationError,
    DiscoveryError,
    ObjectNotFoundError,
    PermissionError,
    TransportError,
)
from ray_clickhouse._models import ClickHouseConnection, QualifiedTable, ResourceLimits
from ray_clickhouse._schema import SchemaPlan, TargetColumn
from ray_clickhouse.datasource import ClickHouseDatasource, ClickHouseReadConfig


def _column(name="id", declared_type="Int64", default_kind=""):
    return TargetColumn(name, declared_type, default_kind, "", 1)


@pytest.mark.parametrize("key", ["id", "`id`", " id, value ", "tuple(id, `value`)"])
def test_auto_selects_first_simple_integer_column(key):
    column = _column()
    assert select_auto_range_column(key, (column, _column("value", "String"))) == column


@pytest.mark.parametrize(
    "declared_type",
    [
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
        "Nullable(Int64)",
        "LowCardinality(Int32)",
    ],
)
def test_auto_accepts_supported_integer_types(declared_type):
    column = _column(declared_type=declared_type)
    assert select_auto_range_column("id", (column,)) == column


@pytest.mark.parametrize(
    "key",
    [
        "",
        "tuple()",
        "abs(id)",
        "id + 1",
        "tuple(abs(id), value)",
        "id, abs(id)",
        "value, id",
        "missing",
        "id, missing",
        "id; SELECT 1",
        "`id",
        "id`",
        "id DESC",
        "id -- comment",
        "tuple(id",
        "id,",
        "(id)",
        "db.id",
    ],
)
def test_auto_rejects_unproven_sorting_keys(key):
    with pytest.raises(DiscoveryError):
        select_auto_range_column(key, (_column(), _column("value", "String")))


@pytest.mark.parametrize(
    "declared_type", ["String", "Date", "Float64", "Bool", "UInt128"]
)
def test_auto_does_not_coerce_non_integer_keys(declared_type):
    with pytest.raises(DiscoveryError, match="integer first"):
        select_auto_range_column("id", (_column(declared_type=declared_type),))


def test_auto_rejects_alias_key():
    with pytest.raises(DiscoveryError, match="physical integer"):
        select_auto_range_column("id", (_column(default_kind="ALIAS"),))


def test_sorting_discovery_binds_table_and_closes_client():
    client = MagicMock()
    client.query.return_value.result_rows = [["id, value"]]
    with patch("ray_clickhouse._discovery._open", return_value=client):
        result = discover_sorting_key(
            ClickHouseConnection(host="localhost", database="analytics"),
            QualifiedTable("analytics", "events"),
            ResourceLimits(),
        )
    assert result == "id, value"
    assert client.query.call_args.kwargs["parameters"] == {
        "database": "analytics",
        "table": "events",
    }
    assert "SELECT sorting_key FROM system.tables" in client.query.call_args.args[0]
    client.close.assert_called_once()


@pytest.mark.parametrize(
    ("rows", "error"),
    [
        ([], ObjectNotFoundError),
        ([[None]], DiscoveryError),
        ([["id"], ["id"]], DiscoveryError),
        ([[]], DiscoveryError),
    ],
)
def test_sorting_discovery_rejects_missing_or_malformed_metadata(rows, error):
    client = MagicMock()
    client.query.return_value.result_rows = rows
    with patch("ray_clickhouse._discovery._open", return_value=client):
        with pytest.raises(error):
            discover_sorting_key(
                ClickHouseConnection(host="localhost", database="analytics"),
                QualifiedTable("analytics", "events"),
                ResourceLimits(),
            )
    client.close.assert_called_once()


@pytest.mark.parametrize(
    ("message", "error"),
    [
        ("authentication failed", AuthenticationError),
        ("access denied", PermissionError),
        ("connection timeout", TransportError),
    ],
)
def test_sorting_discovery_translates_errors_and_closes_client(message, error):
    client = MagicMock()
    client.query.side_effect = RuntimeError(message)
    with patch("ray_clickhouse._discovery._open", return_value=client):
        with pytest.raises(error):
            discover_sorting_key(
                ClickHouseConnection(host="localhost", database="analytics"),
                QualifiedTable("analytics", "events"),
                ResourceLimits(),
            )
    client.close.assert_called_once()


@pytest.fixture
def planner():
    schema = SchemaPlan(
        pa.schema([("id", pa.int64()), ("value", pa.string())]),
        (_column(), _column("value", "String")),
    )
    with (
        patch("ray_clickhouse.datasource.discover_schema", return_value=schema),
        patch(
            "ray_clickhouse.datasource.discover_engine", return_value="MergeTree"
        ) as engine,
        patch(
            "ray_clickhouse.datasource.discover_sorting_key", return_value="id, value"
        ) as key,
        patch(
            "ray_clickhouse.datasource.discover_range_facts",
            return_value=RangeFacts("id", "Int64", 20, 0, -10, 9),
        ) as facts,
        patch(
            "ray_clickhouse.datasource.make_read_task", side_effect=lambda fn, *args: fn
        ),
    ):
        yield SimpleNamespace(engine=engine, key=key, facts=facts)


def _source(**kwargs):
    return ClickHouseDatasource(
        ClickHouseReadConfig(
            connection=ClickHouseConnection(host="localhost", database="analytics"),
            table=QualifiedTable("analytics", "events"),
            **kwargs,
        )
    )


def _queries(source, parallelism=4):
    with patch(
        "ray_clickhouse.datasource.stream_query", return_value=iter(())
    ) as stream:
        for read_fn in source.get_read_tasks(parallelism=parallelism):
            list(read_fn())
    return [call.args[1] for call in stream.call_args_list]


def test_auto_reuses_filtered_range_planning_without_projecting_key(planner):
    source = _source(
        split="auto",
        columns=("value",),
        filter_sql="id != %(excluded)s",
        query_parameters=(("excluded", 0),),
        target_tasks=3,
        max_tasks=3,
    )
    queries = _queries(source, parallelism=8)
    assert len(queries) == 3
    assert all(query.sql.startswith("SELECT `value` FROM") for query in queries)
    assert all("`id` >=" in query.sql for query in queries)
    assert all(
        "ORDER BY" not in query.sql and "OFFSET" not in query.sql for query in queries
    )
    assert all(query.parameter_dict()["excluded"] == 0 for query in queries)
    assert planner.facts.call_args.kwargs == {
        "column": _column(),
        "filter_sql": "id != %(excluded)s",
        "parameters": {"excluded": 0},
    }
    _queries(source)
    planner.key.assert_called_once()
    planner.facts.assert_called_once()
    assert source.config.range_column is None


@pytest.mark.parametrize("split", ["single", "range"])
def test_existing_modes_do_not_discover_sorting_keys(planner, split):
    _queries(
        _source(split=split, **({"range_column": "id"} if split == "range" else {}))
    )
    planner.key.assert_not_called()
    if split == "single":
        planner.facts.assert_not_called()


@pytest.mark.parametrize("policy", ["single", "error"])
@pytest.mark.parametrize("key", ["abs(id)", "value, id", "tuple()"])
def test_auto_obeys_policy_for_unsuitable_key(planner, policy, key):
    planner.key.return_value = key
    source = _source(split="auto", discovery_policy=policy)
    if policy == "error":
        with pytest.raises(DiscoveryError):
            _queries(source)
    else:
        queries = _queries(source)
        assert len(queries) == 1
        assert "WHERE" not in queries[0].sql
    planner.facts.assert_not_called()


@pytest.mark.parametrize("engine", ["View", "Distributed"])
@pytest.mark.parametrize("policy", ["single", "error"])
def test_auto_never_discovers_or_splits_single_query_engines(planner, engine, policy):
    planner.engine.return_value = engine
    source = _source(split="auto", discovery_policy=policy)
    if policy == "error":
        with pytest.raises(DiscoveryError, match="direct MergeTree"):
            _queries(source)
    else:
        assert len(_queries(source)) == 1
    planner.key.assert_not_called()
    planner.facts.assert_not_called()


@pytest.mark.parametrize("engine", ["MaterializedView", "Memory"])
def test_auto_does_not_expand_supported_engines(planner, engine):
    planner.engine.return_value = engine
    with pytest.raises(DiscoveryError, match="unsupported"):
        _queries(_source(split="auto"))
    planner.key.assert_not_called()


@pytest.mark.parametrize("stage", ["key", "facts"])
@pytest.mark.parametrize(
    "error", [AuthenticationError, PermissionError, TransportError, ObjectNotFoundError]
)
def test_auto_does_not_swallow_operational_errors(planner, stage, error):
    getattr(planner, stage).side_effect = error("failed")
    with pytest.raises(error):
        _queries(_source(split="auto"))


@pytest.mark.parametrize("stage", ["key", "facts"])
@pytest.mark.parametrize("policy", ["single", "error"])
def test_auto_discovery_failure_obeys_policy(planner, stage, policy):
    getattr(planner, stage).side_effect = DiscoveryError("unavailable")
    source = _source(split="auto", discovery_policy=policy)
    if policy == "error":
        with pytest.raises(DiscoveryError):
            _queries(source)
    else:
        assert len(_queries(source)) == 1


@pytest.mark.parametrize(
    "kwargs", [{"range_column": "id"}, {"order_by": (("id", False),)}]
)
def test_auto_rejects_conflicting_options(kwargs):
    with pytest.raises(ConfigurationError):
        _source(split="auto", **kwargs)


@pytest.mark.parametrize(
    "values",
    [
        [],
        [None, None],
        [4, 4, 4],
        [-5, -5, 0, 7, None],
        [-(2**63), -1, 0, 2**63 - 1],
        [0, 2**64 - 1],
    ],
)
def test_auto_queries_cover_each_static_input_row_exactly_once(planner, values):
    non_null = [value for value in values if value is not None]
    planner.facts.return_value = RangeFacts(
        "id",
        "Int64",
        len(values),
        values.count(None),
        min(non_null) if non_null else None,
        max(non_null) if non_null else None,
    )
    queries = _queries(_source(split="auto"), parallelism=8)
    assert 1 <= len(queries) <= 8
    for value in values:
        matches = 0
        for query in queries:
            params = query.parameter_dict()
            lower = params.get("__ray_clickhouse_range_lower")
            upper = params.get("__ray_clickhouse_range_upper")
            if lower is None and upper is None:
                matches += 1
            elif value is None:
                matches += "IS NULL" in query.sql
            else:
                matches += (lower is None or value >= lower) and (
                    upper is None or value < upper
                )
        assert matches == 1
