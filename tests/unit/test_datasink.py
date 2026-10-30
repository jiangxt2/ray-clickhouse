from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from ray_clickhouse._discovery import TargetTable
from ray_clickhouse._models import ClickHouseConnection, QualifiedTable, ResourceLimits
from ray_clickhouse._schema import TargetColumn
from ray_clickhouse.datasink import ClickHouseDataSink, validate_write_remote_args


def _target():
    return TargetTable(
        "analytics",
        "events",
        "MergeTree",
        (
            TargetColumn("id", "UInt64", "", "", 1),
            TargetColumn("value", "String", "", "", 2),
        ),
    )


def test_write_remote_args_force_no_replay():
    assert validate_write_remote_args({}) == {"max_retries": 0}


def test_datasink_validates_schema_and_writes_bounded_batches(caplog):
    caplog.set_level("DEBUG", logger="ray_clickhouse")
    client_session = MagicMock()
    with (
        patch("ray_clickhouse.datasink.discover_target", return_value=_target()),
        patch(
            "ray_clickhouse.datasink.ClickHouseInsertSession",
            return_value=client_session,
        ),
    ):
        sink = ClickHouseDataSink(
            connection=ClickHouseConnection(host="clickhouse", database="analytics"),
            table=QualifiedTable("analytics", "events"),
            limits=ResourceLimits(batch_rows=2, batch_bytes=1024),
        )
        schema = pa.schema([("id", pa.uint64()), ("value", pa.string())])
        sink.on_write_start(schema)
        table = pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.uint64()),
                "value": pa.array(["a", "b", "c"], type=pa.string()),
            },
            schema=schema,
        )
        result = sink.write([table], None)

    assert result == {
        "rows": 3,
        "bytes": table.nbytes,
        "batches": 2,
        "status": "confirmed",
    }
    assert client_session.start.call_count == 1
    assert client_session.insert.call_count == 2
    assert client_session.close.call_count == 1
    assert "write preparation ready elapsed_ms=" in caplog.text
    assert "confirmed_batches=2 confirmed_rows=3" in caplog.text
    assert f"confirmed_arrow_bytes={table.nbytes}" in caplog.text
    sink.on_write_complete(MagicMock(write_returns=[result]))
    assert "ClickHouse write complete" in caplog.text


def test_write_failure_logs_only_confirmed_batches(caplog):
    caplog.set_level("DEBUG", logger="ray_clickhouse")
    session = MagicMock()
    session.insert.side_effect = [None, RuntimeError("private insert error")]
    session.close.side_effect = RuntimeError("private close error")
    sink = ClickHouseDataSink(
        connection=ClickHouseConnection(host="clickhouse", database="analytics"),
        table=QualifiedTable("analytics", "events"),
        limits=ResourceLimits(batch_rows=1),
    )
    table = pa.table({"id": pa.array([1, 2], pa.uint64()), "value": ["a", "b"]})
    with (
        patch("ray_clickhouse.datasink.discover_target", return_value=_target()),
        patch("ray_clickhouse.datasink.ClickHouseInsertSession", return_value=session),
    ):
        sink.on_write_start(table.schema)
        with pytest.raises(RuntimeError, match="private insert error"):
            sink.write([table], None)
    assert "confirmed_batches=1 confirmed_rows=1" in caplog.text
    assert "write session close failed" in caplog.text
    assert "private" not in caplog.text
    assert session.insert.call_count == 2
    session.close.assert_called_once()
