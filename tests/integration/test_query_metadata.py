"""Verify query-result metadata before adopting automatic schema discovery."""

import json
from pathlib import Path

import pytest

from ray_clickhouse._schema import parse_describe_rows, render_read_projection

pytestmark = pytest.mark.integration


def test_query_metadata_feasibility(clickhouse_client):
    cases = [
        (
            "WITH source AS (SELECT number AS id FROM numbers(%(count)s)) "
            "SELECT sum(id) AS total FROM source",
            {"count": 5},
        ),
        (
            "SELECT toDecimal64('12.345',3) AS amount, "
            "toDate('2026-10-03') AS day, "
            "toUUID('12345678-1234-5678-1234-567812345678') AS identifier, "
            "CAST(NULL AS Nullable(UInt32)) AS missing",
            {},
        ),
        ("SELECT number AS id FROM numbers(0)", {}),
    ]
    evidence = []
    for sql, parameters in cases:
        described = clickhouse_client.query(
            f"DESCRIBE (\n{sql}\n)", parameters=parameters, settings={"readonly": 1}
        )
        columns = parse_describe_rows(described.result_rows)
        projection = render_read_projection(columns)
        probe = clickhouse_client.query_arrow(
            f"SELECT {projection} FROM (\n{sql}\n) AS source LIMIT 0",
            parameters=parameters,
            settings={"readonly": 1},
            use_strings=True,
        )
        assert probe.num_rows == 0
        assert probe.column_names == [column.name for column in columns]
        evidence.append(
            {
                "declared": [(column.name, column.declared_type) for column in columns],
                "arrow": [
                    (field.name, str(field.type), field.nullable)
                    for field in probe.schema
                ],
            }
        )
    output = Path(".artifacts/implementation/query-metadata-feasibility.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(evidence, indent=2) + "\n")
