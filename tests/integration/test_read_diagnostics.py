"""Real protocol and worker propagation checks for read diagnostics."""

from __future__ import annotations

import pickle
from collections.abc import Iterator

import pyarrow as pa
import pytest
import ray
from ray import cloudpickle

from ray_clickhouse._errors import ReadError, TransportError
from ray_clickhouse._models import ClickHouseConnection, QuerySpec, ResourceLimits
from ray_clickhouse._transport import stream_query

pytestmark = pytest.mark.integration


def _failed_query() -> QuerySpec:
    return QuerySpec(
        "SELECT throwIf(1, 'read diagnostic test') AS marker",
        (),
        pa.schema([("marker", pa.uint8())]),
    )


def _read_failure(connection: ClickHouseConnection) -> None:
    list(stream_query(connection, _failed_query(), ResourceLimits()))


def test_server_failure_has_diagnostic_without_query_logging(connection_options):
    connection = ClickHouseConnection(
        **connection_options, settings=(("log_queries", 0),)
    )
    with pytest.raises((ReadError, TransportError)) as raised:
        _read_failure(connection)
    error = raised.value
    assert error.query_id
    assert error.diagnostic is None
    assert error.client_diagnostic["exception_type"].endswith("DatabaseError")
    assert "DatabaseError" in str(error)
    restored = pickle.loads(pickle.dumps(error))
    assert restored.client_diagnostic == error.client_diagnostic


@pytest.fixture
def active_ray() -> Iterator[None]:
    owned = not ray.is_initialized()
    if owned:
        ray.init(address="local", include_dashboard=False, num_cpus=2)
    try:
        yield
    finally:
        if owned:
            ray.shutdown()


@pytest.mark.ray
def test_client_diagnostic_reaches_driver_from_real_worker(
    active_ray, connection_options
):
    connection = ClickHouseConnection(
        **connection_options, settings=(("log_queries", 0),)
    )
    task = ray.remote(max_retries=0)(_read_failure)
    with pytest.raises(ray.exceptions.RayTaskError) as raised:
        ray.get(task.remote(connection))
    error = raised.value.cause
    assert isinstance(error, (ReadError, TransportError))
    assert error.client_diagnostic["exception_type"].endswith("DatabaseError")
    assert "DatabaseError" in str(raised.value)
    assert error.query_id in str(raised.value)


@pytest.mark.ray
def test_real_worker_client_initialization_redacts_header_credentials(
    active_ray, clickhouse_client, connection_options
):
    token = "header.payload.signature~+/=="
    connection = ClickHouseConnection(
        **connection_options,
        client_options=(("headers", {"X-Gateway-Authorization": f"Bearer {token}\n"}),),
    )
    task = ray.remote(max_retries=0)(_read_failure)
    with pytest.raises(ray.exceptions.RayTaskError) as raised:
        ray.get(task.remote(connection))
    error = raised.value.cause
    assert isinstance(error, ReadError)
    assert error.client_diagnostic["exception_type"] == "builtins.ValueError"
    assert error.client_diagnostic["operation"] == "read client initialization"
    assert error.diagnostic is None
    rendered = str(raised.value)
    assert error.query_id in rendered
    assert "<redacted>" in rendered
    for segment in (token, "payload", "signature~+/=="):
        assert segment not in rendered
        assert segment not in repr(error.client_diagnostic)
    serialized = cloudpickle.dumps(error)
    assert token.encode() not in serialized
    assert b"signature~+/==" not in serialized
