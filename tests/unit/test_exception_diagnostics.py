import pickle
import traceback
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from ray_clickhouse._ddl import create_or_replace_table
from ray_clickhouse._discovery import discover_engine
from ray_clickhouse._errors import (
    AmbiguousTableManagementError,
    AmbiguousWriteError,
    DiscoveryError,
    ReadError,
    TransportError,
    WriteError,
)
from ray_clickhouse._models import ClickHouseConnection, QualifiedTable, ResourceLimits
from ray_clickhouse._transport import ClickHouseInsertSession


class ServerFailure(Exception):
    code = 123


def _assert_safe(error, operation):
    assert error.client_diagnostic == {
        "exception_type": f"{__name__}.ServerFailure",
        "operation": operation,
        "message": "client message omitted",
        "exception_code": "123",
    }
    assert "client_exception_code=123" in str(error)
    assert error.__cause__ is None
    assert "secret-value" not in "".join(traceback.format_exception(error))
    assert b"secret-value" not in pickle.dumps(error)
    assert (
        pickle.loads(pickle.dumps(error)).client_diagnostic == error.client_diagnostic
    )


def test_discovery_failure_retains_safe_client_metadata():
    client = MagicMock()
    client.query.side_effect = ServerFailure("SELECT secret-value")
    with (
        patch("ray_clickhouse._discovery._open", return_value=client),
        pytest.raises(DiscoveryError) as raised,
    ):
        discover_engine(
            ClickHouseConnection(host="host", database="db"),
            QualifiedTable("db", "events"),
            ResourceLimits(),
        )
    _assert_safe(raised.value, "engine discovery")
    client.close.assert_called_once()


def test_discovery_initialization_exposes_safe_metadata():
    with (
        patch(
            "clickhouse_connect.get_client", side_effect=ServerFailure("secret-value")
        ),
        pytest.raises(DiscoveryError) as raised,
    ):
        discover_engine(
            ClickHouseConnection(host="host", database="db"),
            QualifiedTable("db", "events"),
            ResourceLimits(),
        )
    _assert_safe(raised.value, "client initialization")


@pytest.mark.parametrize(
    ("phase", "expected", "message", "operation"),
    [
        pytest.param(
            "start",
            ReadError,
            "secret-value",
            "write client initialization",
            id="start",
        ),
        pytest.param(
            "insert", WriteError, "secret-value", "Arrow insert", id="rejected"
        ),
        pytest.param(
            "insert",
            AmbiguousWriteError,
            "connection secret-value",
            "Arrow insert",
            id="ambiguous",
        ),
        pytest.param(
            "close", TransportError, "secret-value", "write client close", id="close"
        ),
    ],
)
def test_write_failures_expose_safe_metadata(phase, expected, message, operation):
    client = MagicMock()
    failure = ServerFailure(message)
    if phase == "insert":
        client.insert_arrow.side_effect = failure
    elif phase == "close":
        client.close.side_effect = failure
    session = ClickHouseInsertSession(
        ClickHouseConnection(host="host", database="db"),
        ResourceLimits(),
        database="db",
        table="events",
        insert_mode="sync",
    )
    with patch(
        "clickhouse_connect.get_client",
        side_effect=failure if phase == "start" else None,
        return_value=client,
    ):
        with pytest.raises(expected) as raised:
            session.start()
            if phase == "insert":
                session.insert(pa.table({"id": [1]}))
            else:
                session.close()
    _assert_safe(raised.value, operation)
    if phase == "insert":
        client.insert_arrow.assert_called_once()
        session.close()


def test_overwrite_failure_keeps_ambiguous_outcome_and_safe_metadata():
    client = MagicMock()
    client.query.return_value.result_rows = [(1,)]
    client.command.side_effect = [None, ServerFailure("secret-value")]
    with (
        patch("ray_clickhouse._ddl._open", return_value=client),
        pytest.raises(AmbiguousTableManagementError) as raised,
    ):
        create_or_replace_table(
            ClickHouseConnection(host="host", database="db"),
            QualifiedTable("db", "events"),
            ResourceLimits(),
            pa.schema([("id", pa.int64())]),
            mode="overwrite",
            engine="MergeTree",
            order_by=("id",),
        )
    _assert_safe(raised.value, "overwrite table")
    client.close.assert_called_once()


def test_create_failure_keeps_discovery_class_and_safe_metadata():
    client = MagicMock()
    client.query.return_value.result_rows = [(0,)]
    client.command.side_effect = ServerFailure("secret-value")
    with (
        patch("ray_clickhouse._ddl._open", return_value=client),
        pytest.raises(DiscoveryError) as raised,
    ):
        create_or_replace_table(
            ClickHouseConnection(host="host", database="db"),
            QualifiedTable("db", "events"),
            ResourceLimits(),
            pa.schema([("id", pa.int64())]),
            mode="create",
            engine="MergeTree",
            order_by=("id",),
        )
    _assert_safe(raised.value, "create table")
    client.close.assert_called_once()


def test_discovery_with_unprintable_exception_keeps_original_class():
    class BadMessage(Exception):
        def __str__(self):
            raise ValueError("secret-value")

    with (
        patch("ray_clickhouse._discovery._open", side_effect=BadMessage()),
        pytest.raises(DiscoveryError) as raised,
    ):
        discover_engine(
            ClickHouseConnection(host="host", database="db"),
            QualifiedTable("db", "events"),
            ResourceLimits(),
        )
    assert raised.value.client_diagnostic["exception_type"].endswith("BadMessage")
    assert "secret-value" not in "".join(traceback.format_exception(raised.value))
