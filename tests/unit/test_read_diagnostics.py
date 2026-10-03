from __future__ import annotations

import base64
import logging
import pickle
import traceback
from http.client import HTTPConnection
from unittest.mock import MagicMock, patch
from urllib.parse import quote

import pyarrow as pa
import pytest

from ray_clickhouse._errors import ReadError, TransportError
from ray_clickhouse._models import ClickHouseConnection, QuerySpec, ResourceLimits
from ray_clickhouse._transport import stream_query


class _FailureStream:
    def __init__(self, error: BaseException, blocks=(), setup=False):
        self.error = error
        self.blocks = blocks
        self.setup = setup
        self.closed = False

    def __enter__(self):
        if self.setup:
            raise self.error
        return self

    def __exit__(self, *args):
        self.closed = True

    def __iter__(self):
        yield from self.blocks
        raise self.error


def _spec():
    return QuerySpec("SELECT id FROM events", (), pa.schema([("id", pa.int64())]))


@pytest.mark.parametrize(
    ("phase", "operation"),
    [
        ("initialization", "read client initialization"),
        ("setup", "Arrow stream setup"),
        ("iteration", "Arrow stream iteration"),
    ],
)
def test_failure_diagnostic_is_visible_and_serializable(phase, operation):
    original = pa.ArrowInvalid("Expected an IPC message")
    client = MagicMock()
    stream = _FailureStream(original, setup=phase == "setup")
    client.query_arrow_stream.return_value = stream
    with (
        patch(
            "clickhouse_connect.get_client",
            side_effect=original if phase == "initialization" else None,
            return_value=client,
        ),
        patch("ray_clickhouse._transport._lookup_query_diagnostic", return_value=None),
        pytest.raises(ReadError) as raised,
    ):
        list(
            stream_query(
                ClickHouseConnection(host="localhost", database="analytics"),
                _spec(),
                ResourceLimits(),
            )
        )
    error = raised.value
    assert error.client_diagnostic == {
        "exception_type": "pyarrow.lib.ArrowInvalid",
        "operation": operation,
        "message": "Expected an IPC message",
    }
    rendered = "".join(traceback.format_exception(error))
    assert "pyarrow.lib.ArrowInvalid" in rendered
    assert "Expected an IPC message" in rendered
    assert error.query_id in rendered
    restored = pickle.loads(pickle.dumps(error))
    assert restored.client_diagnostic == error.client_diagnostic
    assert restored.query_id == error.query_id
    assert error.__cause__ is None
    if phase != "initialization":
        client.close.assert_called_once()
    assert stream.closed == (phase == "iteration")


def test_partial_read_preserves_error_and_closes_without_replay():
    stream = _FailureStream(
        RuntimeError("client decoder failed"), (pa.table({"id": [1, 2]}),)
    )
    client = MagicMock()
    client.query_arrow_stream.return_value = stream
    client.close.side_effect = RuntimeError("secondary close failure")
    with (
        patch("clickhouse_connect.get_client", return_value=client),
        patch("ray_clickhouse._transport._lookup_query_diagnostic", return_value=None),
    ):
        iterator = stream_query(
            ClickHouseConnection(host="localhost", database="analytics"),
            _spec(),
            ResourceLimits(),
        )
        assert next(iterator).to_pylist() == [{"id": 1}, {"id": 2}]
        with pytest.raises(ReadError) as raised:
            next(iterator)
    assert raised.value.client_diagnostic["message"] == "client decoder failed"
    assert stream.closed
    client.query_arrow_stream.assert_called_once()
    client.close.assert_called_once()


def test_credentials_and_parameters_do_not_enter_serialized_diagnostics(monkeypatch):
    secret = "sentinel:/private@value"
    monkeypatch.setenv("READ_DIAGNOSTIC_PASSWORD", secret)
    connection = ClickHouseConnection(
        host="localhost", database="analytics", password_env="READ_DIAGNOSTIC_PASSWORD"
    )
    query = QuerySpec(
        "SELECT id FROM events WHERE tag = %(tag)s",
        (("tag", "private-filter-value"),),
        _spec().arrow_schema,
    )
    original = RuntimeError(
        f"decoder failed: {secret} {quote(secret, safe='')} private-filter-value"
    )
    client = MagicMock()
    client.query_arrow_stream.return_value = _FailureStream(original)
    with (
        patch("clickhouse_connect.get_client", return_value=client),
        patch("ray_clickhouse._transport._lookup_query_diagnostic", return_value=None),
        pytest.raises(ReadError) as raised,
    ):
        list(stream_query(connection, query, ResourceLimits()))
    error = raised.value
    payload = pickle.dumps(error)
    rendered = "".join(traceback.format_exception(error))
    for value in (secret, quote(secret, safe=""), "private-filter-value"):
        assert value not in rendered
        assert value.encode() not in payload
        assert value not in repr(error.client_diagnostic)
    assert "<redacted>" in error.client_diagnostic["message"]


def test_bad_exception_string_does_not_mask_failure():
    class BadMessage(Exception):
        def __str__(self):
            raise ValueError("message formatting failed")

    with (
        patch("clickhouse_connect.get_client", side_effect=BadMessage()),
        pytest.raises(ReadError) as raised,
    ):
        list(
            stream_query(
                ClickHouseConnection(host="localhost", database="analytics"),
                _spec(),
                ResourceLimits(),
            )
        )
    assert raised.value.client_diagnostic["message"] == "client message unavailable"
    assert raised.value.client_diagnostic["exception_type"].endswith("BadMessage")


def test_arrow_conversion_records_original_class():
    client = MagicMock()
    stream = _FailureStream(RuntimeError("unreachable"), (pa.table({"id": ["bad"]}),))
    client.query_arrow_stream.return_value = stream
    with (
        patch("clickhouse_connect.get_client", return_value=client),
        patch("ray_clickhouse._transport._lookup_query_diagnostic", return_value=None),
        pytest.raises(ReadError) as raised,
    ):
        list(
            stream_query(
                ClickHouseConnection(host="localhost", database="analytics"),
                _spec(),
                ResourceLimits(),
            )
        )
    assert (
        raised.value.client_diagnostic["exception_type"] == "pyarrow.lib.ArrowInvalid"
    )
    assert raised.value.client_diagnostic["operation"] == "Arrow conversion"
    assert stream.closed


def test_close_after_eof_has_its_own_diagnostic():
    client = MagicMock()
    client.query_arrow_stream.return_value.__enter__.return_value = iter(())
    client.close.side_effect = RuntimeError("close failed")
    with (
        patch("clickhouse_connect.get_client", return_value=client),
        pytest.raises(TransportError) as raised,
    ):
        list(
            stream_query(
                ClickHouseConnection(host="localhost", database="analytics"),
                _spec(),
                ResourceLimits(),
            )
        )
    assert raised.value.client_diagnostic["operation"] == "read client close"
    assert raised.value.query_id


def test_diagnostic_lookup_logs_exception_class_only(caplog):
    client = MagicMock()
    client.query_arrow_stream.return_value = _FailureStream(
        RuntimeError("decoder failed")
    )
    diagnostic = MagicMock()
    diagnostic.query.side_effect = RuntimeError("private-credential-in-diagnostic")
    caplog.set_level(logging.DEBUG, logger="ray_clickhouse._transport")
    with (
        patch("clickhouse_connect.get_client", side_effect=[client, diagnostic]),
        pytest.raises(ReadError) as raised,
    ):
        list(
            stream_query(
                ClickHouseConnection(host="localhost", database="analytics"),
                _spec(),
                ResourceLimits(),
            )
        )
    assert "private-credential-in-diagnostic" not in caplog.text
    assert raised.value.client_diagnostic["message"] == "decoder failed"
    diagnostic.close.assert_called_once()


def test_cancellation_closes_without_running_diagnostics():
    client = MagicMock()
    client.query_arrow_stream.return_value.__enter__.return_value = iter(
        [pa.table({"id": [1]}), pa.table({"id": [2]})]
    )
    with (
        patch("clickhouse_connect.get_client", return_value=client),
        patch("ray_clickhouse._transport._lookup_query_diagnostic") as diagnostic,
    ):
        iterator = stream_query(
            ClickHouseConnection(host="localhost", database="analytics"),
            _spec(),
            ResourceLimits(),
        )
        next(iterator)
        iterator.close()
    diagnostic.assert_not_called()
    client.close.assert_called_once()


def test_sql_context_is_not_echoed_in_client_diagnostic():
    client = MagicMock()
    client.query_arrow_stream.return_value = _FailureStream(
        RuntimeError("unexpected response to SELECT private_payload FROM events")
    )
    with (
        patch("clickhouse_connect.get_client", return_value=client),
        patch("ray_clickhouse._transport._lookup_query_diagnostic", return_value=None),
        pytest.raises(ReadError) as raised,
    ):
        list(
            stream_query(
                ClickHouseConnection(host="localhost", database="analytics"),
                _spec(),
                ResourceLimits(),
            )
        )
    assert "private_payload" not in str(raised.value)
    assert raised.value.client_diagnostic["message"].endswith("contains SQL")


def test_encoded_authentication_headers_are_redacted():
    secret = "header-sentinel"
    encoded = base64.b64encode(f"default:{secret}".encode()).decode()
    client = MagicMock()
    client.query_arrow_stream.return_value = _FailureStream(
        RuntimeError(f"request failed with Basic {encoded} and Bearer opaque-token")
    )
    with (
        patch("clickhouse_connect.get_client", return_value=client),
        patch("ray_clickhouse._transport._lookup_query_diagnostic", return_value=None),
        pytest.raises(ReadError) as raised,
    ):
        list(
            stream_query(
                ClickHouseConnection(
                    host="localhost", database="analytics", password=secret
                ),
                _spec(),
                ResourceLimits(),
            )
        )
    assert encoded not in str(raised.value)
    assert "opaque-token" not in str(raised.value)
    assert encoded.encode() not in pickle.dumps(raised.value)


def _invalid_header_error(value, header="X-ClickHouse-Key"):
    connection = HTTPConnection("localhost")
    try:
        connection.putrequest("GET", "/")
        with pytest.raises(ValueError) as raised:
            connection.putheader(header, value)
        return raised.value
    finally:
        connection.close()


def test_header_validation_error_redacts_escaped_connection_value():
    secret = "header-key-sentinel\n"
    original = _invalid_header_error(secret)
    connection = ClickHouseConnection(
        host="localhost",
        database="analytics",
        client_options=(("headers", {"X-ClickHouse-Key": secret}),),
    )
    with (
        patch("clickhouse_connect.get_client", side_effect=original),
        patch("ray_clickhouse._transport._lookup_query_diagnostic") as diagnostic,
        pytest.raises(ReadError) as raised,
    ):
        list(stream_query(connection, _spec(), ResourceLimits()))
    diagnostic.assert_not_called()
    error = raised.value
    assert error.client_diagnostic["exception_type"] == "builtins.ValueError"
    escaped = repr(secret)[1:-1]
    assert escaped not in "".join(traceback.format_exception(error))
    assert escaped not in repr(error.client_diagnostic)
    assert escaped.encode() not in pickle.dumps(error)
    assert "Invalid header value" in str(error)


@pytest.mark.parametrize("representation", ["string", "ascii", "utf8_bytes"])
def test_escaped_secret_representations_do_not_enter_error_payload(representation):
    secret = "repr-key'\"\\value-é\n"
    representations = {
        "string": repr(secret),
        "ascii": ascii(secret),
        "utf8_bytes": repr(secret.encode()),
    }
    client = MagicMock()
    client.query_arrow_stream.return_value = _FailureStream(
        RuntimeError(f"decoder failed with {representations[representation]}")
    )
    with (
        patch("clickhouse_connect.get_client", return_value=client),
        patch("ray_clickhouse._transport._lookup_query_diagnostic", return_value=None),
        pytest.raises(ReadError) as raised,
    ):
        list(
            stream_query(
                ClickHouseConnection(
                    host="localhost", database="analytics", password=secret
                ),
                _spec(),
                ResourceLimits(),
            )
        )
    error = raised.value
    rendered = "".join(traceback.format_exception(error))
    for value in (
        secret,
        repr(secret)[1:-1],
        ascii(secret)[1:-1],
        repr(secret.encode())[2:-1],
    ):
        assert value not in rendered
        assert value not in repr(error.client_diagnostic)
        assert value.encode() not in pickle.dumps(error)
    assert "decoder failed" in rendered
    client.close.assert_called_once()


def test_bearer_error_text_is_redacted_before_connection_strings():
    token = "header.payload.signature~+/=="
    original = _invalid_header_error(
        f"Bearer {token}\n", header="X-Gateway-Authorization"
    )
    connection = ClickHouseConnection(
        host="localhost", database="analytics", username="B"
    )
    with (
        patch("clickhouse_connect.get_client", side_effect=original),
        patch("ray_clickhouse._transport._lookup_query_diagnostic") as diagnostic,
        pytest.raises(ReadError) as raised,
    ):
        list(stream_query(connection, _spec(), ResourceLimits()))
    diagnostic.assert_not_called()
    error = raised.value
    rendered = "".join(traceback.format_exception(error))
    for segment in ("header.payload", "payload", "signature~+/=="):
        assert segment not in rendered
        assert segment not in repr(error.client_diagnostic)
        assert segment.encode() not in pickle.dumps(error)
    assert error.client_diagnostic["operation"] == "read client initialization"
    assert error.query_id in rendered


def test_redaction_encoding_failure_preserves_primary_error():
    client = MagicMock()
    client.query_arrow_stream.return_value = _FailureStream(
        RuntimeError("primary decoder failure")
    )
    with (
        patch("clickhouse_connect.get_client", return_value=client),
        patch("ray_clickhouse._transport._lookup_query_diagnostic", return_value=None),
        pytest.raises(ReadError) as raised,
    ):
        list(
            stream_query(
                ClickHouseConnection(
                    host="localhost", database="analytics", password="sentinel-\ud800"
                ),
                _spec(),
                ResourceLimits(),
            )
        )
    error = raised.value
    assert error.client_diagnostic["exception_type"] == "builtins.RuntimeError"
    assert "sanitization failed" in error.client_diagnostic["message"]
    assert error.query_id
    assert "sentinel" not in str(error)
    client.close.assert_called_once()
