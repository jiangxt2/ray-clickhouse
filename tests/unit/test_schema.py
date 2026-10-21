import pyarrow as pa
import pytest

from ray_clickhouse._errors import SchemaError, WriteError
from ray_clickhouse._schema import (
    TargetColumn,
    TargetTable,
    arrow_compatible,
    canonical_schema,
    parse_describe_rows,
    render_read_projection,
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


def test_parse_describe_accepts_comment_in_position_slot() -> None:
    columns = parse_describe_rows(
        [
            ("id", "UInt64", "", "", "comment", "", ""),
            ("value", "String", "", "", "", "", ""),
        ]
    )

    assert [column.position for column in columns] == [1, 2]


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
