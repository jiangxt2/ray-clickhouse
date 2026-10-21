from __future__ import annotations

from collections import Counter
from dataclasses import replace

import pytest
import ray

from ray_clickhouse import read_clickhouse
from ray_clickhouse._errors import RayClickHouseError
from ray_clickhouse._models import ClickHouseConnection, QualifiedTable, ResourceLimits
from ray_clickhouse.datasource import ClickHouseDatasource, ClickHouseReadConfig

pytestmark = [pytest.mark.integration, pytest.mark.cluster, pytest.mark.distributed]
DATABASE = "ray_clickhouse_it"


def source(tables, predicate, parameters=None, **options):
    config = ClickHouseReadConfig(
        ClickHouseConnection("clickhouse", DATABASE),
        QualifiedTable(DATABASE, tables.distributed),
        filter_sql=predicate,
        query_parameters=tuple((parameters or {}).items()),
        convert_distributed_to_local=True,
        limits=ResourceLimits(batch_rows=3),
        target_tasks=4,
        **options,
    )
    return ClickHouseDatasource(config)


def rows(datasource):
    return [
        row
        for task in datasource.get_read_tasks(4)
        for block in task()
        for row in block.to_pylist()
    ]


def multiset(values):
    return Counter(tuple(sorted(row.items())) for row in values)


def assert_equivalent(tables, predicate, parameters=None, **options):
    local = source(tables, predicate, parameters, **options)
    logical = ClickHouseDatasource(
        replace(local.config, convert_distributed_to_local=False, split="single")
    )
    try:
        expected = rows(logical)
        actual = rows(local)
    except RayClickHouseError as exc:
        cause = exc.__context__ or exc.__cause__
        note = getattr(exc, "add_note", None)
        if note is not None:
            # These fixtures use only synthetic values and password-free clients.
            note(
                f"predicate={predicate!r}, split={local.config.split}, "
                f"underlying={type(cause).__name__}: {cause}"
            )
        raise
    assert multiset(actual) == multiset(expected), (predicate, local.config.split)
    assert local._resolved_distributed is not None
    assert len(local._local_sources) == 2
    assert all(target.replica == 1 for target in local._resolved_distributed.targets)
    return expected


def assert_local_queries(tables):
    for index, client in enumerate(tables.clients):
        client.command("SYSTEM FLUSH LOGS")
        queries = client.query(
            "SELECT query, used_functions FROM system.query_log "
            "WHERE type = 'QueryFinish' "
            "AND is_initial_query = 1 "
            "AND startsWith(log_comment, 'ray-clickhouse operation=read ') "
            "AND position(query, %(table)s) > 0",
            parameters={"table": tables.local},
        ).result_rows
        assert bool(queries) == (index in (0, 2))
        if queries:
            # Native parameter replacement formats an AST: BETWEEN can appear
            # as comparisons in query_log rather than retaining its SQL spelling.
            functions = {name for _, names in queries for name in names}
            assert {"greaterOrEquals", "lessOrEquals"} <= functions, queries
            assert functions & {"like", "notLike"}, queries
            assert functions & {"in", "notIn", "nullIn", "notNullIn"}, queries


@pytest.mark.parametrize("split", ["single", "partition", "range", "auto"])
def test_simple_predicates_across_local_splits(cluster_tables, split):
    options = {"split": split}
    if split == "range":
        options["range_column"] = "id"
    for predicate in (
        "id BETWEEN 3 AND 24",
        "id NOT BETWEEN 3 AND 24",
        "txt LIKE 'row-_'",
        "txt NOT LIKE 'row-_'",
        "txt LIKE 'row-%'",
        "txt NOT LIKE 'row-%'",
        "txt IN ('row-1', 'row-22', '100%')",
        "txt BETWEEN 'row-%' AND 'z'",
        "value IN (NULL, 1, 9, 22, 22)",
        "value NOT IN (NULL, 1, 9, 22, 22)",
        "NOT (id BETWEEN 3 AND 24 OR txt LIKE 'row-2_') "
        "AND (value IS NULL OR value IN (1, 9, 31))",
    ):
        assert_equivalent(cluster_tables, predicate, **options)
    assert_local_queries(cluster_tables)


@pytest.mark.parametrize("split", ["single", "partition", "range", "auto"])
def test_constant_bindings_nulls_and_empty_results(cluster_tables, split):
    options = {"split": split}
    if split == "range":
        options["range_column"] = "id"
    cases = (
        ("id BETWEEN {lo:Int64} AND {hi:Int64}", {"lo": 0, "hi": 24}),
        ("id NOT BETWEEN %(lo)s AND %(hi)s", {"lo": 0, "hi": 24}),
        ("txt LIKE {pattern:String}", {"pattern": "row-2_"}),
        ("txt NOT LIKE %(pattern)s", {"pattern": "row-2_"}),
        ("id IN ({one:Int64}, {two:Int64})", {"one": 1, "two": 22}),
        ("id NOT IN (%(one)s, %(two)s)", {"one": 1, "two": 22}),
        (
            "value IN ({nil:Nullable(Int64)}, {one:Int64})",
            {"nil": None, "one": 9},
        ),
        ("value NOT IN (%(nil)s, %(one)s)", {"nil": None, "one": 9}),
        ("value IN ('1', '9')", {}),
        ("id IN (1000)", {}),
        ("id BETWEEN 24 AND 3", {}),
        ("value BETWEEN NULL AND 30", {}),
    )
    for predicate, parameters in cases:
        assert_equivalent(cluster_tables, predicate, parameters, **options)


@pytest.mark.parametrize(
    "declared", ["String", "Nullable(String)", "LowCardinality(Nullable(String))"]
)
@pytest.mark.parametrize("split", ["single", "partition", "range", "auto"])
def test_fixed_patterns_keep_escaping_and_string_types(cluster_tables, declared, split):
    tables = cluster_tables
    options = {"split": split}
    if split == "range":
        options["range_column"] = "id"
    for client in tables.clients[::2]:
        client.command(
            f"ALTER TABLE {DATABASE}.{tables.local} MODIFY COLUMN txt {declared}"
        )
    for client in tables.clients:
        client.command(f"SYSTEM SYNC REPLICA {DATABASE}.{tables.local}")
    tables.clients[0].command(
        f"ALTER TABLE {DATABASE}.{tables.distributed} MODIFY COLUMN txt {declared}"
    )
    values = ["100%", "a_b", "a\\b", "it's", "SELECT GLOBAL IN", "%s", "%(literal)s"]
    if "Nullable" in declared:
        values.append(None)
    for client in tables.clients[::2]:
        client.insert(
            f"{DATABASE}.{tables.local}",
            [(100 + index, 1, 0, value) for index, value in enumerate(values)],
            column_names=["id", "value", "tenant", "txt"],
        )
    for client in tables.clients:
        client.command(f"SYSTEM SYNC REPLICA {DATABASE}.{tables.local}")
    for predicate, expected in (
        (r"txt LIKE '100\\%'", "100%"),
        (r"txt LIKE 'a\\_b'", "a_b"),
        (r"txt LIKE 'a\\\\b'", "a\\b"),
        ("txt LIKE 'it''s'", "it's"),
        ("txt LIKE 'SELECT GLOBAL IN'", "SELECT GLOBAL IN"),
    ):
        result = assert_equivalent(tables, predicate, **options)
        assert len(result) == 2 and all(row["txt"] == expected for row in result)
    for predicate in (
        "txt NOT LIKE 'row-%'",
        "txt IN ('100%', 'a_b', NULL)",
        "txt NOT IN ('100%', 'a_b', NULL)",
        "txt IN ('%s', '%(literal)s')",
        "txt BETWEEN '100%' AND 'z'",
        "txt NOT BETWEEN '100%' AND 'z'",
    ):
        assert_equivalent(tables, predicate, **options)
    assert_equivalent(tables, "txt LIKE %(pattern)s", {"pattern": r"a\_b"}, **options)


@pytest.mark.parametrize(
    "predicate",
    ["id BETWEEN 3 AND 24", "txt LIKE 'row-%'", "value IN (NULL, 9, 22)"],
)
def test_public_ray_dataset_uses_local_predicates(cluster_tables, predicate):
    tables = cluster_tables
    expected = rows(
        ClickHouseDatasource(
            replace(
                source(tables, predicate).config, convert_distributed_to_local=False
            )
        )
    )
    dataset = read_clickhouse(
        host="clickhouse",
        database=DATABASE,
        table=tables.distributed,
        filter=predicate,
        convert_distributed_to_local=True,
        split="partition",
        target_tasks=4,
        batch_rows=3,
        ray_remote_args={"max_retries": 0},
    )
    assert multiset(dataset.take_all()) == multiset(expected)
    workers = {
        node["NodeManagerAddress"]
        for node in ray.nodes()
        if node["Alive"] and node["Resources"].get("CPU", 0)
    }
    assert len(workers) == 2
    for client in tables.clients[::2]:
        client.command("SYSTEM FLUSH LOGS")
        origins = client.query(
            "SELECT DISTINCT toString(initial_address) FROM system.query_log "
            "WHERE type = 'QueryFinish' AND is_initial_query = 1 "
            "AND startsWith(log_comment, 'ray-clickhouse operation=read ') "
            "AND position(query, %(table)s) > 0",
            parameters={"table": tables.local},
        ).result_rows
        assert origins
        assert all(address.removeprefix("::ffff:") in workers for (address,) in origins)


@pytest.mark.parametrize(
    "predicate",
    [
        "id GLOBAL IN (1, 9, 22)",
        "id GLOBAL NOT IN (1, 9, 22)",
        "txt LIKE concat('row-', '%')",
        "txt ILIKE 'ROW-%'",
        "id BETWEEN 3 AND 24 OR id GLOBAL IN (31)",
    ],
)
def test_unsupported_forms_keep_whole_logical_read(cluster_tables, predicate):
    tables = cluster_tables
    datasource = source(tables, predicate)
    expected = rows(
        ClickHouseDatasource(
            replace(datasource.config, convert_distributed_to_local=False)
        )
    )
    assert multiset(rows(datasource)) == multiset(expected)
    assert len(datasource.get_read_tasks(4)) == 1
    assert not datasource._local_sources
    assert datasource._logical_fallback
