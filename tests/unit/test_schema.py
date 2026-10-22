from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from ray_clickhouse._discovery import discover_schema, discover_target
from ray_clickhouse._errors import (
    ConfigurationError,
    PermissionError,
    SchemaError,
    WriteError,
)
from ray_clickhouse._models import ClickHouseConnection, QualifiedTable, ResourceLimits
from ray_clickhouse._schema import (
    TargetColumn,
    TargetTable,
    arrow_compatible,
    canonical_schema,
    parse_describe_rows,
    render_read_projection,
    select_read_columns,
    type_declaration_key,
    validate_source_schema,
)


def test_parse_describe_and_canonical_schema() -> None:
    columns = parse_describe_rows(
        [
            ("id", "UInt64", "", "", 1, ""),
            ("value", "String", "", "", 2, ""),
        ]
    )
    plan = canonical_schema(
        columns,
        pa.schema([pa.field("id", pa.uint64()), pa.field("value", pa.string())]),
    )
    assert plan.arrow_schema.names == ["id", "value"]


@pytest.mark.parametrize(
    "unsupported", ["AggregateFunction(sum, UInt64)", "Variant(Int64, String)"]
)
def test_explicit_schema_probe_retains_unconverted_source_metadata(unsupported):
    rows = [
        ("id", "Int64", "", ""),
        ("value", "Nullable(String)", "", ""),
        ("state", unsupported, "", ""),
    ]
    arrow = pa.schema(
        [pa.field("value", pa.string()), pa.field("id", pa.int64(), False)]
    )
    client = Mock()
    client.query.return_value.result_rows = rows
    client.query_arrow.return_value = pa.Table.from_batches([], schema=arrow)
    with patch("ray_clickhouse._discovery._open", return_value=client):
        plan = discover_schema(
            ClickHouseConnection("entry", "db"),
            QualifiedTable("db", "data"),
            ResourceLimits(),
            columns=("value", "id"),
        )
    assert plan.arrow_schema == arrow
    assert plan.columns == parse_describe_rows(rows)
    assert client.query_arrow.call_args.args[0] == (
        "SELECT `value`, `id` FROM `db`.`data` LIMIT 0"
    )
    client.close.assert_called_once()


@pytest.mark.parametrize(
    "names,error",
    [
        (None, SchemaError),
        (("state",), SchemaError),
        (("missing",), ConfigurationError),
        ((), ConfigurationError),
    ],
)
def test_schema_selection_errors_precede_arrow_probe_and_close_client(names, error):
    client = Mock()
    client.query.return_value.result_rows = [
        ("id", "Int64", "", ""),
        ("state", "AggregateFunction(sum, UInt64)", "", ""),
    ]
    with patch("ray_clickhouse._discovery._open", return_value=client):
        with pytest.raises(error):
            discover_schema(
                ClickHouseConnection("entry", "db"),
                QualifiedTable("db", "data"),
                ResourceLimits(),
                columns=names,
            )
    client.query_arrow.assert_not_called()
    client.close.assert_called_once()


@pytest.mark.parametrize(
    "failure", [PermissionError("denied"), SchemaError("bad probe")]
)
def test_selected_probe_propagates_error_and_closes_client(failure):
    client = Mock()
    client.query.return_value.result_rows = [("id", "Int64", "", "")]
    client.query_arrow.side_effect = failure
    with patch("ray_clickhouse._discovery._open", return_value=client):
        with pytest.raises(type(failure), match=str(failure)):
            discover_schema(
                ClickHouseConnection("entry", "db"),
                QualifiedTable("db", "data"),
                ResourceLimits(),
                columns=("id",),
            )
    client.close.assert_called_once()


def test_projected_canonical_schema_keeps_order_checks_and_full_declarations():
    columns = parse_describe_rows(
        [("id", "Int64", "", ""), ("value", "String", "", "")]
    )
    selected = select_read_columns(columns, ("value", "id"))
    arrow = pa.schema([("value", pa.string()), ("id", pa.int64())])
    plan = canonical_schema(selected, arrow, source_columns=columns)
    assert plan.columns == columns and plan.arrow_schema == arrow
    with pytest.raises(SchemaError, match="disagree"):
        canonical_schema(
            selected,
            pa.schema([("id", pa.int64()), ("value", pa.string())]),
            source_columns=columns,
        )


def test_parse_describe_accepts_comment_in_position_slot() -> None:
    columns = parse_describe_rows(
        [
            ("id", "UInt64", "", "", "comment", "", ""),
            ("value", "String", "", "", "", "", ""),
        ]
    )

    assert [column.position for column in columns] == [1, 2]


@pytest.mark.parametrize("comment", ["2026", "0", "-1", "1", "001", "comment"])
def test_describe_comments_never_supply_positions(comment):
    rows = [("id", "Int64", "", "", comment, "", "")]
    assert parse_describe_rows(rows)[0].position == 1
    rows.append(("value", "String", "", "", "0", "", ""))
    columns = parse_describe_rows(rows)
    assert [(column.name, column.position) for column in columns] == [
        ("id", 1),
        ("value", 2),
    ]


def test_explicit_system_column_positions_are_preserved_for_writes():
    rows = [
        ("value", "String", "DEFAULT", "'x'", 7),
        ("id", "UInt64", "", "", 3),
    ]
    client = Mock()
    client.query.return_value.result_rows = rows
    with (
        patch("ray_clickhouse._discovery._open", return_value=client),
        patch("ray_clickhouse._discovery.discover_engine", return_value="MergeTree"),
    ):
        target = discover_target(
            ClickHouseConnection("entry", "db"),
            QualifiedTable("db", "data"),
            ResourceLimits(),
        )
    assert [(column.name, column.position) for column in target.columns] == [
        ("id", 3),
        ("value", 7),
    ]
    assert target.columns[1].default_expression == "'x'"
    client.close.assert_called_once()


@pytest.mark.parametrize("position", [None, "2", True, 0, -1])
def test_explicit_system_column_positions_must_be_valid(position):
    with pytest.raises(SchemaError, match="invalid position"):
        parse_describe_rows([("id", "Int64", "", "", position)], position_index=4)


def test_explicit_system_column_position_cannot_be_missing():
    with pytest.raises(SchemaError, match="invalid position"):
        parse_describe_rows([("id", "Int64", "", "")], position_index=4)


def test_read_projection_normalizes_date_and_nested_date_types() -> None:
    columns = parse_describe_rows(
        [
            ("event_date", "Date", "", "", 1),
            ("dates", "Array(Nullable(Date))", "", "", 2),
            ("request_id", "UUID", "", "", 3),
        ]
    )

    assert render_read_projection(columns) == (
        "CAST(`event_date` AS Date32) AS `event_date`, "
        "CAST(`dates` AS Array(Nullable(Date32))) AS `dates`, "
        "CAST(`request_id` AS String) AS `request_id`"
    )


def test_canonical_schema_rejects_column_mismatch() -> None:
    columns = parse_describe_rows([("id", "UInt64", "", "", 1, "")])
    with pytest.raises(SchemaError):
        canonical_schema(columns, pa.schema([("other", pa.uint64())]))


@pytest.mark.parametrize(
    ("dtype", "declared"),
    [
        (pa.uint64(), "UInt64"),
        (pa.string(), "String"),
        (pa.decimal128(10, 2), "Decimal(10, 2)"),
        (pa.timestamp("us", tz="UTC"), "DateTime64(6, 'UTC')"),
        (pa.list_(pa.int32()), "Array(Int32)"),
    ],
)
def test_arrow_type_matrix(dtype: pa.DataType, declared: str) -> None:
    assert arrow_compatible(dtype, declared)


def test_tuple_type_matrix_checks_nested_fields() -> None:
    source = pa.struct([("first", pa.string()), ("second", pa.int32())])
    wrong_source = pa.struct([("first", pa.string()), ("second", pa.string())])

    assert arrow_compatible(source, "Tuple(first String, second Int32)")
    assert not arrow_compatible(wrong_source, "Tuple(first String, second Int32)")


@pytest.mark.parametrize(
    "compact,pretty",
    [
        ("Tuple(a Int64, b String)", "Tuple(\n    a Int64,\n    b String)"),
        ("Array(Tuple(a Int64, b String))", "Array( Tuple(\n a Int64,\n b String ) )"),
        ("Enum8('a b'=1, 'it\\'s'=-2)", "Enum8( 'a b' = 1, 'it\\'s' = - 2 )"),
        ("DateTime64(6,'Etc/GMT+8')", "DateTime64 ( 6 , 'Etc/GMT+8' )"),
        ("Tuple(`a b` UInt8)", "Tuple(\n `a b`   UInt8\n)"),
    ],
)
def test_type_declaration_key_ignores_only_display_whitespace(compact, pretty):
    assert type_declaration_key(compact) == type_declaration_key(pretty)


@pytest.mark.parametrize(
    "left,right",
    [
        ("UInt8", "UInt 8"),
        ("Tuple(a Int64)", "Tuple(b Int64)"),
        ("Enum8('a b'=1)", "Enum8('ab'=1)"),
        ("DateTime64(3,'UTC')", "DateTime64(6,'UTC')"),
        ("DateTime('UTC')", "DateTime('Europe/Paris')"),
        ("Date", "Date32"),
        ("Tuple(a UInt32)", "Tuple(a UInt64)"),
    ],
)
def test_type_declaration_key_preserves_semantic_differences(left, right):
    assert type_declaration_key(left) != type_declaration_key(right)


@pytest.mark.parametrize("value", ["", "Tuple(a Int64", "Enum8('unclosed=1)"])
def test_type_declaration_key_reuses_balanced_argument_validation(value):
    with pytest.raises(SchemaError):
        type_declaration_key(value)


def test_validate_source_schema_rejects_missing_required_column() -> None:
    target = TargetTable(
        "analytics",
        "events",
        "MergeTree",
        (
            TargetColumn("id", "UInt64", "", "", 1),
            TargetColumn("value", "String", "", "", 2),
        ),
    )
    with pytest.raises(SchemaError):
        validate_source_schema(pa.schema([("id", pa.uint64())]), target, None)


def test_validate_source_schema_rejects_lossy_type() -> None:
    target = TargetTable(
        "analytics",
        "events",
        "MergeTree",
        (TargetColumn("id", "UInt64", "", "", 1),),
    )
    with pytest.raises(WriteError):
        validate_source_schema(pa.schema([("id", pa.int64())]), target, None)
