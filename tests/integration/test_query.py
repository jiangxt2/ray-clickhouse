"""Compare query results, schema, execution count and failures with ClickHouse."""

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from ray_clickhouse import read_clickhouse
from ray_clickhouse._errors import SchemaError
from ray_clickhouse._models import ClickHouseConnection
from ray_clickhouse._query import ClickHouseQueryDatasource, QueryReadConfig

from .conftest import DATABASE

pytestmark = pytest.mark.integration


def _source(options, sql, parameters=(), **kwargs):
    return ClickHouseQueryDatasource(
        QueryReadConfig(ClickHouseConnection(**options), sql, parameters, **kwargs)
    )


def _rows(source):
    tasks = source.get_read_tasks(8)
    assert len(tasks) == 1
    return [row for block in tasks[0]() for row in block.to_pylist()]


@pytest.mark.parametrize(
    "query",
    [
        "SELECT number AS id FROM numbers(12) ORDER BY id DESC LIMIT 5",
        "WITH source AS (SELECT number AS id FROM numbers(12)) "
        "SELECT id FROM source WHERE id >= %(minimum)s ORDER BY id",
        "SELECT a.number AS left_id, b.number AS right_id FROM numbers(4) AS a "
        "INNER JOIN numbers(4) AS b ON a.number = b.number ORDER BY left_id",
        "SELECT number % 3 AS bucket, count() AS rows FROM numbers(12) "
        "GROUP BY bucket ORDER BY bucket",
        "SELECT count() AS rows FROM numbers(0)",
        "SELECT number AS id FROM numbers(0)",
    ],
)
def test_query_results_match_direct_sql(clickhouse_client, connection_options, query):
    parameters = {"minimum": 7} if "%(minimum)s" in query else {}
    expected = clickhouse_client.query(query, parameters=parameters).named_results()
    source = _source(connection_options, query, tuple(parameters.items()))
    assert _rows(source) == list(expected)
    assert source.arrow_schema.names


def test_query_logical_types_are_preserved(connection_options):
    sql = (
        "SELECT toDecimal64('12.345',3) AS amount, toDate('2026-10-03') AS day, "
        "toUUID('12345678-1234-5678-1234-567812345678') AS identifier, "
        "CAST(NULL AS Nullable(UInt32)) AS missing, "
        "[toUInt32(1), toUInt32(2)] AS items"
    )
    rows = _rows(_source(connection_options, sql))
    assert rows == [
        {
            "amount": Decimal("12.345"),
            "day": date(2026, 10, 3),
            "identifier": "12345678-1234-5678-1234-567812345678",
            "missing": None,
            "items": [1, 2],
        }
    ]


def test_query_can_read_memory_engine_without_physical_planning(
    clickhouse_client, connection_options, table_name
):
    clickhouse_client.command(
        f"CREATE TABLE `{DATABASE}`.`{table_name}` (id UInt32) ENGINE=Memory"
    )
    clickhouse_client.insert(
        table_name, [[1], [2]], column_names=["id"], database=DATABASE
    )
    assert _rows(
        _source(
            connection_options,
            f"SELECT id FROM `{DATABASE}`.`{table_name}` ORDER BY id",
        )
    ) == [{"id": 1}, {"id": 2}]


def test_metadata_requests_do_not_multiply_with_task_requests(
    clickhouse_client, connection_options
):
    source = _source(
        connection_options,
        "SELECT number AS id FROM numbers(3)",
    )
    first = source.get_read_tasks(1)
    second = source.get_read_tasks(16)
    assert len(first) == len(second) == 1
    assert source._schema_plan is not None
    assert [row for block in second[0]() for row in block.to_pylist()] == [
        {"id": 0},
        {"id": 1},
        {"id": 2},
    ]


@pytest.mark.parametrize(
    "query",
    [
        "SELECT number AS `wrong alias` FROM numbers(2)",
        "SELECT 1",
        "SELECT toInt128(1) AS wide",
    ],
)
def test_unsupported_result_schema_fails_closed(connection_options, query):
    with pytest.raises(SchemaError):
        _source(connection_options, query).get_read_tasks(1)


@pytest.mark.ray
@pytest.mark.parametrize("rows", [0, 17])
def test_public_query_dataset_preserves_empty_schema_and_bounded_reads(
    connection_options, rows
):
    from ray_clickhouse._models import ResourceLimits

    dataset = read_clickhouse(
        **connection_options,
        query=f"SELECT number AS id FROM numbers({rows}) ORDER BY id",
        batch_rows=3,
        batch_bytes=1024,
        override_num_blocks=6,
        concurrency=2,
        ray_remote_args={"max_retries": 0},
    )
    assert dataset.take_all() == [{"id": index} for index in range(rows)]
    assert dataset.schema().base_schema.names == ["id"]
    source = _source(
        connection_options,
        f"SELECT number AS id FROM numbers({rows})",
        limits=ResourceLimits(batch_rows=3, batch_bytes=1024),
    )
    blocks = list(source.get_read_tasks(16)[0]())
    assert all(block.num_rows <= 3 and block.nbytes <= 1024 for block in blocks)


def test_query_metadata_before_data_read_is_explicit(
    clickhouse_client, connection_options
):
    source = _source(
        connection_options,
        "SELECT number AS id FROM numbers(9)",
    )
    source.get_read_tasks(1)
    clickhouse_client.command("SYSTEM FLUSH LOGS")
    # The discovery IDs are generated by the connector's two metadata stages.
    result = clickhouse_client.query(
        "SELECT query_id, query, read_rows, result_rows FROM system.query_log "
        "WHERE type='QueryFinish' AND "
        "(query_id LIKE 'ray-clickhouse-query-describe-%' OR "
        "query_id LIKE 'ray-clickhouse-query-schema-probe-%') "
        "ORDER BY event_time_microseconds DESC LIMIT 2"
    )
    assert len(result.result_rows) == 2
    assert any(
        row[0].startswith("ray-clickhouse-query-describe-")
        for row in result.result_rows
    )
    assert any(
        row[0].startswith("ray-clickhouse-query-schema-probe-")
        for row in result.result_rows
    )
    assert all("numbers(9)" in row[1] for row in result.result_rows)
    probe = next(
        row
        for row in result.result_rows
        if row[0].startswith("ray-clickhouse-query-schema-probe-")
    )
    assert "LIMIT 0" in probe[1]
    assert probe[3] == 0
    evidence = Path(".artifacts/implementation/query-planning-cost.json")
    evidence.parent.mkdir(parents=True, exist_ok=True)
    evidence.write_text(
        json.dumps(
            [
                {"query_id": row[0], "read_rows": row[2], "result_rows": row[3]}
                for row in result.result_rows
            ],
            indent=2,
        )
        + "\n"
    )
