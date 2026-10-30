from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from ray_clickhouse._errors import AmbiguousWriteError, ReadError
from ray_clickhouse._models import ClickHouseConnection, QuerySpec, ResourceLimits
from ray_clickhouse._transport import ClickHouseInsertSession, stream_query


class _Stream:
    def __init__(self, tables):
        self._tables = iter(tables)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def __iter__(self):
        return self._tables


class _FailingStream(_Stream):
    def __iter__(self):
        raise RuntimeError("Arrow stream failed")


def test_stream_query_slices_blocks_and_closes_client(caplog) -> None:
    caplog.set_level("DEBUG", logger="ray_clickhouse")
    schema = pa.schema([("id", pa.uint64()), ("value", pa.string())])
    table = pa.table(
        {
            "id": pa.array([1, 2, 3, 4, 5], type=pa.uint64()),
            "value": pa.array(["a", "b", "c", "d", "e"], type=pa.string()),
        },
        schema=schema,
    )
    client = MagicMock()
    client.query_arrow_stream.return_value = _Stream([table])
    connection = ClickHouseConnection(host="clickhouse", database="analytics")
    query = QuerySpec("SELECT id, value FROM `analytics`.`events`", (), schema)

    with patch("clickhouse_connect.get_client", return_value=client):
        blocks = list(
            stream_query(
                connection,
                query,
                ResourceLimits(batch_rows=2, batch_bytes=1024),
            )
        )

    assert [block.num_rows for block in blocks] == [2, 2, 1]
    assert [block.column("id").to_pylist() for block in blocks] == [[1, 2], [3, 4], [5]]
    client.query_arrow_stream.assert_called_once()
    client.close.assert_called_once()
    query_id = client.query_arrow_stream.call_args.kwargs["settings"]["query_id"]
    assert caplog.text.count("ClickHouse first block") == 1
    assert f"query_id={query_id} blocks=3 rows=5" in caplog.text
    assert f"arrow_bytes={sum(block.nbytes for block in blocks)}" in caplog.text
    assert "elapsed_ms=" in caplog.text
    assert query.sql not in caplog.text


def test_stream_query_uses_query_id_setting() -> None:
    schema = pa.schema([("id", pa.uint64())])
    table = pa.table({"id": pa.array([1], type=pa.uint64())}, schema=schema)
    client = MagicMock()
    client.query_arrow_stream.return_value = _Stream([table])
    connection = ClickHouseConnection(host="clickhouse", database="analytics")
    query = QuerySpec("SELECT id FROM `analytics`.`events`", (), schema)

    with patch("clickhouse_connect.get_client", return_value=client):
        list(
            stream_query(
                connection, query, ResourceLimits(batch_rows=10, batch_bytes=1024)
            )
        )

    kwargs = client.query_arrow_stream.call_args.kwargs
    assert kwargs["settings"]["query_id"].startswith("ray-clickhouse-read-")
    assert kwargs["settings"]["log_comment"].startswith(
        "ray-clickhouse operation=read query_id=ray-clickhouse-read-"
    )


def test_stream_query_attaches_query_log_diagnostic(caplog) -> None:
    caplog.set_level("DEBUG", logger="ray_clickhouse")
    schema = pa.schema([("id", pa.uint64())])
    client = MagicMock()
    client.query_arrow_stream.return_value = _FailingStream([])
    diagnostic_client = MagicMock()
    show_result = MagicMock()
    show_result.result_rows = [["custom_query_log"]]
    log_result = MagicMock()
    log_result.result_rows = [
        ["ExceptionWhileProcessing", 395, "mid-stream failure", 10, 0, 80, 0, 1]
    ]
    diagnostic_client.query.side_effect = [show_result, log_result]
    connection = ClickHouseConnection(host="clickhouse", database="analytics")
    query = QuerySpec("SELECT id FROM `analytics`.`events`", (), schema)

    with patch(
        "clickhouse_connect.get_client",
        side_effect=[client, diagnostic_client],
    ):
        with pytest.raises(ReadError) as exc_info:
            list(
                stream_query(
                    connection,
                    query,
                    ResourceLimits(),
                    diagnostic_flush_logs=True,
                )
            )

    error = exc_info.value
    assert error.query_id is not None
    assert error.query_id.startswith("ray-clickhouse-read-")
    assert error.diagnostic is not None
    assert error.diagnostic["exception_code"] == 395
    assert "query_id=" in str(error)
    assert "clickhouse_exception_code=395" in str(error)
    assert "mid-stream failure" in str(error)
    assert diagnostic_client.query.call_count == 2
    assert (
        "SHOW TABLES FROM system LIKE"
        in diagnostic_client.query.call_args_list[0].args[0]
    )
    assert (
        "FROM system.`custom_query_log`"
        in diagnostic_client.query.call_args_list[1].args[0]
    )
    diagnostic_client.command.assert_called_once_with("SYSTEM FLUSH LOGS")
    read_kwargs = client.query_arrow_stream.call_args.kwargs
    assert read_kwargs["settings"]["log_queries"] == 1
    client.close.assert_called_once()
    diagnostic_client.close.assert_called_once()
    assert f"read failed query_id={error.query_id} error_type=ReadError" in caplog.text
    assert "blocks=0 rows=0" in caplog.text
    assert "ClickHouse first block" not in caplog.text
    assert "mid-stream failure" not in caplog.text


def test_stream_query_diagnostic_failure_does_not_mask_original_error() -> None:
    schema = pa.schema([("id", pa.uint64())])
    client = MagicMock()
    client.query_arrow_stream.return_value = _FailingStream([])
    diagnostic_client = MagicMock()
    diagnostic_client.query.side_effect = RuntimeError("query_log unavailable")
    connection = ClickHouseConnection(host="clickhouse", database="analytics")
    query = QuerySpec("SELECT id FROM `analytics`.`events`", (), schema)

    with patch(
        "clickhouse_connect.get_client",
        side_effect=[client, diagnostic_client],
    ):
        with pytest.raises(ReadError) as exc_info:
            list(stream_query(connection, query, ResourceLimits()))

    error = exc_info.value
    assert error.query_id is not None
    assert error.diagnostic is None
    assert "Arrow query failed" in str(error)
    assert "query_id=" in str(error)
    diagnostic_client.close.assert_called_once()


def test_stream_query_accepts_clickhouse_record_batches() -> None:
    schema = pa.schema([("id", pa.uint64())])
    table = pa.table({"id": pa.array([1, 2], type=pa.uint64())}, schema=schema)
    client = MagicMock()
    client.query_arrow_stream.return_value = _Stream(table.to_batches())
    connection = ClickHouseConnection(host="clickhouse", database="analytics")
    query = QuerySpec("SELECT id FROM `analytics`.`events`", (), schema)

    with patch("clickhouse_connect.get_client", return_value=client):
        blocks = list(
            stream_query(
                connection, query, ResourceLimits(batch_rows=10, batch_bytes=1024)
            )
        )

    assert [block.column("id").to_pylist() for block in blocks] == [[1, 2]]


def test_insert_transport_failure_is_ambiguous(caplog) -> None:
    client = MagicMock()
    client.insert_arrow.side_effect = RuntimeError("connection reset")
    connection = ClickHouseConnection(host="clickhouse", database="analytics")
    table = pa.table({"id": pa.array([1], type=pa.uint64())})
    session = ClickHouseInsertSession(
        connection,
        ResourceLimits(),
        database="analytics",
        table="events",
        insert_mode="sync",
    )

    with patch("clickhouse_connect.get_client", return_value=client):
        session.start()
        with pytest.raises(AmbiguousWriteError):
            session.insert(table)
        session.close()
    assert "client_insert_id=ray-clickhouse-insert-" in caplog.text
    assert "outcome=unknown" in caplog.text
    assert "connection reset" not in caplog.text


@pytest.mark.parametrize("close_fails", [False, True])
def test_read_early_close_logs_partial_counts_without_success(caplog, close_fails):
    caplog.set_level("DEBUG", logger="ray_clickhouse")
    table = pa.table({"id": [1, 2, 3]})
    client = MagicMock()
    client.query_arrow_stream.return_value = _Stream([table])
    if close_fails:
        client.close.side_effect = RuntimeError("private client error")
    with patch("clickhouse_connect.get_client", return_value=client):
        blocks = stream_query(
            ClickHouseConnection(
                host="private-host", database="analytics", password="secret-password"
            ),
            QuerySpec("SELECT private_sql", (), table.schema),
            ResourceLimits(batch_rows=1),
        )
        next(blocks)
        blocks.close()
    assert "blocks=1 rows=1" in caplog.text
    assert "succeeded" not in caplog.text
    assert ("client close failed" in caplog.text) == close_fails
    assert not any(word in caplog.text for word in ("private", "secret", "SELECT"))
    client.close.assert_called_once()
