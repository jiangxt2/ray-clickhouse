"""Compare real ClickHouse filtering with the same Ray/Arrow expression."""

from collections import Counter

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from ray.data.datatype import DataType
from ray.data.expressions import col, udf

from ray_clickhouse import read_clickhouse

pytestmark = pytest.mark.integration


@udf(return_dtype=DataType.bool())
def _late_id(values):
    return pc.greater_equal(values, 9)


def multiset(rows):
    return Counter(tuple(sorted(row.items())) for row in rows)


@pytest.fixture
def predicate_table(clickhouse_client, table_name, connection_options):
    db = connection_options["database"]
    clickhouse_client.command(
        f"CREATE TABLE {db}.{table_name} (id Int64, value Nullable(Int32), "
        "flag Nullable(Bool), txt String) ENGINE = MergeTree "
        "PARTITION BY id % 2 ORDER BY id"
    )
    rows = [
        {
            "id": i,
            "value": None if i % 3 == 0 else i,
            "flag": [None, True, False][i % 3],
            "txt": ["plain", "中文", "quote'\\tail"][i % 3],
        }
        for i in range(12)
    ]
    rows += rows[1:3]
    clickhouse_client.insert(
        f"{db}.{table_name}",
        [tuple(row.values()) for row in rows],
        column_names=["id", "value", "flag", "txt"],
    )
    table = pa.Table.from_pylist(
        rows,
        schema=pa.schema(
            [
                ("id", pa.int64()),
                ("value", pa.int32()),
                ("flag", pa.bool_()),
                ("txt", pa.string()),
            ]
        ),
    )
    return table


EXPRESSIONS = [
    col("id") >= 5,
    5 > col("id"),
    (col("value") >= 4) | col("value").is_null(),
    ~(col("value") == 2),
    col("flag"),
    ~col("flag"),
    col("txt") == "quote'\\tail",
    (col("flag") & col("value").is_not_null()) | col("flag").is_null(),
    (col("id") >= 8) | col("id").is_in([0, 1]),
    (col("id") >= 2) & col("id").is_in([7, 9]),
]


@pytest.mark.parametrize("expression", EXPRESSIONS)
def test_real_predicate_matches_arrow(
    predicate_table, connection_options, table_name, expression
):
    dataset = read_clickhouse(**connection_options, table=table_name, batch_rows=3)
    actual = dataset.filter(expr=expression).take_all()
    expected = predicate_table.filter(expression.to_pyarrow()).to_pylist()
    assert multiset(actual) == multiset(expected)


@pytest.mark.parametrize("split", ["single", "partition", "range", "auto"])
@pytest.mark.parametrize("native", [False, True])
def test_filtered_discovery_and_scans_share_binding_protocol(
    predicate_table, connection_options, clickhouse_client, table_name, split, native
):
    expr = (col("value") >= 4) | col("value").is_null()
    options = {"range_column": "id"} if split == "range" else {}
    dataset = read_clickhouse(
        **connection_options,
        table=table_name,
        filter="id >= {minimum:Int64}" if native else "id >= %(minimum)s",
        query_parameters={"minimum": 2},
        split=split,
        target_tasks=3,
        diagnostic_flush_logs=True,
        **options,
    )
    assert multiset(dataset.filter(expr=expr).take_all()) == multiset(
        predicate_table.filter(((col("id") >= 2) & expr).to_pyarrow()).to_pylist()
    )
    clickhouse_client.command("SYSTEM FLUSH LOGS")
    queries = clickhouse_client.query(
        "SELECT query FROM system.query_log WHERE type = 'QueryFinish' "
        "AND startsWith(log_comment, 'ray-clickhouse operation=read ') "
        "AND position(query, %(table)s) > 0",
        parameters={"table": table_name},
    ).result_rows
    assert queries
    for (query,) in queries:
        normalized = query.replace("`", "")
        assert "WHERE" in query and "value IS NULL" in normalized
        expected = "value >= _CAST(4, 'Int32')" if native else "value >= 4"
        assert expected in normalized


def test_view_query_mode_empty_and_all_null(
    predicate_table, connection_options, clickhouse_client, table_name
):
    db = connection_options["database"]
    view = table_name + "_view"
    clickhouse_client.command(
        f"CREATE VIEW {db}.{view} AS SELECT * FROM {db}.{table_name}"
    )
    try:
        expr = col("value").is_null()
        expected = predicate_table.filter(expr.to_pyarrow()).to_pylist()
        for options in [{"table": view}, {"query": f"SELECT * FROM {db}.{table_name}"}]:
            assert multiset(
                read_clickhouse(**connection_options, **options)
                .filter(expr=expr)
                .take_all()
            ) == multiset(expected)
        clickhouse_client.command(f"TRUNCATE TABLE {db}.{table_name}")
        assert (
            read_clickhouse(**connection_options, table=table_name)
            .filter(expr=expr)
            .take_all()
            == []
        )
        clickhouse_client.insert(
            f"{db}.{table_name}",
            [(1, None, None, "a")],
            column_names=["id", "value", "flag", "txt"],
        )
        assert (
            read_clickhouse(**connection_options, table=table_name)
            .filter(expr=expr)
            .count()
            == 1
        )
    finally:
        clickhouse_client.command(f"DROP VIEW {db}.{view}")


def test_residual_limit_and_original_dataset_branch(
    predicate_table, connection_options, table_name
):
    base = read_clickhouse(**connection_options, table=table_name, batch_rows=2)
    filtered = base.filter(expr=(col("id") >= 0) & col("id").is_in([9, 10, 11]))
    assert len(filtered.limit(2).take_all()) == 2
    assert base.count() == predicate_table.num_rows
    assert base.filter(expr=col("id") < 1).take_all() == [
        {"id": 0, "value": None, "flag": None, "txt": "plain"}
    ]


def test_accepted_predicate_with_opaque_residual_before_limit(
    predicate_table, connection_options, table_name
):
    base = read_clickhouse(**connection_options, table=table_name, batch_rows=2)
    rows = base.filter(expr=(col("id") >= 3) & _late_id(col("id"))).limit(2).take_all()
    assert len(rows) == 2 and all(row["id"] >= 9 for row in rows)


def test_numeric_extremes_dictionary_and_complete_nullable_truth_tables(
    clickhouse_client, table_name, connection_options
):
    db = connection_options["database"]
    clickhouse_client.command(
        f"CREATE TABLE {db}.{table_name} (id Int64, tiny Int8, unsigned UInt32, "
        "wide UInt64, flag Nullable(Bool), other Nullable(Bool), "
        "label LowCardinality(Nullable(String))) ENGINE=MergeTree ORDER BY id"
    )
    values = [
        (
            -(2**63) if i == 0 else 2**63 - 1 if i == 8 else i,
            -128 if i % 2 == 0 else 127,
            2**32 - 1,
            2**64 - 1,
            left,
            right,
            None if i % 2 == 0 else "雪",
        )
        for i, (left, right) in enumerate(
            (left, right)
            for left in (None, False, True)
            for right in (None, False, True)
        )
    ]
    clickhouse_client.insert(
        f"{db}.{table_name}",
        values,
        column_names=["id", "tiny", "unsigned", "wide", "flag", "other", "label"],
    )
    reference = clickhouse_client.query_arrow(
        f"SELECT * FROM {db}.{table_name}", use_strings=True
    )
    expressions = [
        col("id") == -(2**63),
        col("id") >= 2**63 - 1,
        col("tiny") == -128,
        col("tiny") >= 127,
        col("unsigned") == 2**32 - 1,
        col("wide").is_not_null(),
        col("flag") & col("other"),
        col("flag") | col("other"),
        ~(col("flag") & col("other")),
        ~(col("flag") | col("other")),
        col("label") == "雪",
        col("label").is_null(),
    ]
    for expr in expressions:
        actual = (
            read_clickhouse(**connection_options, table=table_name)
            .filter(expr=expr)
            .take_all()
        )
        assert multiset(actual) == multiset(
            reference.filter(expr.to_pyarrow()).to_pylist()
        )
    # A translation must not hide Arrow 19's full-domain UInt64 coercion error.
    from ray.exceptions import RayTaskError

    with pytest.raises(RayTaskError, match="not in range"):
        read_clickhouse(**connection_options, table=table_name).filter(
            expr=col("wide") == 1
        ).take_all()


def _create_limit_table(clickhouse_client, connection_options, table_name):
    db = connection_options["database"]
    clickhouse_client.command(
        f"CREATE TABLE {db}.{table_name} (id Int64) ENGINE=MergeTree ORDER BY id"
    )
    clickhouse_client.command(
        f"INSERT INTO {db}.{table_name} SELECT number FROM numbers(30000)"
    )


def _assert_limit_residual(
    connection_options, table_name, clickhouse_client, **options
):
    base = read_clickhouse(
        **connection_options,
        table=table_name,
        order_by=(["id"], False),
        diagnostic_flush_logs=True,
        **options,
    )
    assert base.take_all() == [{"id": i} for i in range(10000)]
    assert base.filter(expr=col("id") >= 20000).take_all() == []
    clickhouse_client.command("SYSTEM FLUSH LOGS")
    queries = clickhouse_client.query(
        "SELECT query FROM system.query_log WHERE type = 'QueryFinish' "
        "AND startsWith(log_comment, 'ray-clickhouse operation=read ') "
        "AND position(query, %(table)s) > 0",
        parameters={"table": table_name},
    ).result_rows
    assert queries
    assert all("WHERE" not in query for (query,) in queries)


def test_server_limit_preserves_ray_residual(
    clickhouse_client, connection_options, table_name
):
    _create_limit_table(clickhouse_client, connection_options, table_name)
    _assert_limit_residual(
        connection_options, table_name, clickhouse_client, settings={"limit": 10000}
    )


@pytest.mark.parametrize("columns", [["txt"], []])
@pytest.mark.parametrize("split", ["single", "range"])
def test_projection_and_predicate_share_filtered_source_facts(
    predicate_table, connection_options, table_name, clickhouse_client, columns, split
):
    options = {"range_column": "id"} if split == "range" else {}
    base = read_clickhouse(
        **connection_options,
        table=table_name,
        split=split,
        target_tasks=3,
        diagnostic_flush_logs=True,
        **options,
    )
    reference = predicate_table.filter((col("id") >= 5).to_pyarrow())
    for branch in (
        base.filter(expr=col("id") >= 5).select_columns(columns),
        base.select_columns(["txt", "id"])
        .filter(expr=col("id") >= 5)
        .select_columns(columns),
    ):
        if columns:
            assert multiset(branch.take_all()) == multiset(
                reference.select(columns).to_pylist()
            )
        else:
            assert branch.materialize().count() == reference.num_rows
    clickhouse_client.command("SYSTEM FLUSH LOGS")
    queries = clickhouse_client.query(
        "SELECT query FROM system.query_log WHERE type = 'QueryFinish' "
        "AND startsWith(log_comment, 'ray-clickhouse operation=read ') "
        "AND position(query, %(table)s) > 0",
        parameters={"table": table_name},
    ).result_rows
    assert queries
    carrier = "txt" if columns else "id"
    assert all(
        query.replace("`", "").startswith(f"SELECT {carrier} FROM")
        and "id >=" in query.replace("`", "")
        for (query,) in queries
    )
    assert base.count() == predicate_table.num_rows


def _assert_capped_read(clickhouse_client, connection_options, table_name, settings):
    db = connection_options["database"]
    clickhouse_client.command(
        f"CREATE TABLE {db}.{table_name} (id Int64) ENGINE=MergeTree ORDER BY id"
    )
    clickhouse_client.command(f"INSERT INTO {db}.{table_name} VALUES (1)")
    base = read_clickhouse(**connection_options, table=table_name, settings=settings)
    # Establish that the original read is usable with the unchanged cap first.
    assert base.take_all() == [{"id": 1}]
    assert base.filter(expr=col("id") == 1).take_all() == [{"id": 1}]


def test_result_row_cap_does_not_break_predicate_planning(
    clickhouse_client, connection_options, table_name
):
    _assert_capped_read(
        clickhouse_client, connection_options, table_name, {"max_result_rows": 1}
    )


def test_result_byte_cap_does_not_break_predicate_planning(
    clickhouse_client, connection_options, table_name
):
    _assert_capped_read(
        clickhouse_client, connection_options, table_name, {"max_result_bytes": 4096}
    )


def test_inherited_server_limit_preserves_ray_residual(
    clickhouse_client, connection_options, table_name
):
    _create_limit_table(clickhouse_client, connection_options, table_name)
    user = table_name + "_reader"
    clickhouse_client.command(f"CREATE USER {user} SETTINGS limit = 10000")
    try:
        clickhouse_client.command(f"GRANT SELECT, SHOW COLUMNS ON *.* TO {user}")
        inherited = {**connection_options, "username": user, "password": ""}
        _assert_limit_residual(inherited, table_name, clickhouse_client)
    finally:
        clickhouse_client.command(f"DROP USER IF EXISTS {user}")
