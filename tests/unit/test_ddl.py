from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from ray_clickhouse._ddl import _arrow_type, create_or_replace_table
from ray_clickhouse._errors import (
    AmbiguousTableManagementError,
    ConfigurationError,
    SchemaError,
)
from ray_clickhouse._models import ClickHouseConnection, QualifiedTable, ResourceLimits


def _schema() -> pa.Schema:
    return pa.schema([("id", pa.uint64()), ("payload", pa.string())])


def test_create_table_is_safe_and_uses_structured_order_by() -> None:
    client = MagicMock()
    client.query.return_value.result_rows = [(0,)]
    connection = ClickHouseConnection(host="clickhouse", database="analytics")

    with patch("ray_clickhouse._ddl._open", return_value=client):
        create_or_replace_table(
            connection,
            QualifiedTable("analytics", "events"),
            ResourceLimits(),
            _schema(),
            mode="create",
            engine="MergeTree",
            order_by=("id",),
            nullable_columns=("id",),
        )

    ddl = client.command.call_args.args[0]
    assert "CREATE TABLE `analytics`.`events`" in ddl
    assert "`id` Nullable(UInt64)" in ddl
    assert "ORDER BY `id`" in ddl
    assert "DROP TABLE" not in ddl


def test_overwrite_drops_existing_table_before_create() -> None:
    client = MagicMock()
    client.query.return_value.result_rows = [(1,)]
    connection = ClickHouseConnection(host="clickhouse", database="analytics")

    with patch("ray_clickhouse._ddl._open", return_value=client):
        create_or_replace_table(
            connection,
            QualifiedTable("analytics", "events"),
            ResourceLimits(),
            _schema(),
            mode="overwrite",
            engine="MergeTree",
            order_by=None,
        )

    assert client.command.call_args_list[0].args[0] == (
        "DROP TABLE `analytics`.`events`"
    )


def test_create_rejects_unsafe_engine() -> None:
    with pytest.raises(ConfigurationError):
        create_or_replace_table(
            ClickHouseConnection(host="clickhouse", database="analytics"),
            QualifiedTable("analytics", "events"),
            ResourceLimits(),
            _schema(),
            mode="create",
            engine="MergeTree() ORDER BY id",
            order_by=("id",),
        )


def test_create_type_mapping_preserves_precision_and_rejects_binary() -> None:
    assert _arrow_type(pa.bool_()) == "Bool"
    assert _arrow_type(pa.timestamp("us", tz="UTC")) == "DateTime64(6, 'UTC')"
    with pytest.raises(SchemaError):
        _arrow_type(pa.binary())


def test_overwrite_validates_before_drop() -> None:
    with patch("ray_clickhouse._ddl._open") as open_client:
        with pytest.raises(ConfigurationError, match="order_by columns"):
            create_or_replace_table(
                ClickHouseConnection(host="clickhouse", database="analytics"),
                QualifiedTable("analytics", "events"),
                ResourceLimits(),
                _schema(),
                mode="overwrite",
                engine="MergeTree",
                order_by=("missing",),
            )
    open_client.assert_not_called()


def test_overwrite_drop_then_create_failure_is_ambiguous() -> None:
    client = MagicMock()
    client.query.return_value.result_rows = [(1,)]
    client.command.side_effect = [None, RuntimeError("connection reset")]
    connection = ClickHouseConnection(host="clickhouse", database="analytics")

    with patch("ray_clickhouse._ddl._open", return_value=client):
        with pytest.raises(AmbiguousTableManagementError):
            create_or_replace_table(
                connection,
                QualifiedTable("analytics", "events"),
                ResourceLimits(),
                _schema(),
                mode="overwrite",
                engine="MergeTree",
                order_by=("id",),
            )
