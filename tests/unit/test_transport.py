from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from ray_clickhouse._errors import AmbiguousWriteError
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


def test_stream_query_slices_blocks_and_closes_client() -> None:
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


def test_stream_query_uses_transport_query_id() -> None:
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
    assert kwargs["transport_settings"]["query_id"].startswith("ray-clickhouse-read-")
    assert kwargs["settings"]["log_comment"] == "ray-clickhouse operation=read"


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


def test_insert_transport_failure_is_ambiguous() -> None:
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
