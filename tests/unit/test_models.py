import pytest

from ray_clickhouse._errors import ConfigurationError
from ray_clickhouse._models import (
    ClickHouseConnection,
    QualifiedTable,
    QuerySpec,
    ResourceLimits,
)


def test_connection_repr_redacts_credentials() -> None:
    connection = ClickHouseConnection(
        host="clickhouse",
        database="analytics",
        username="reader",
        password="secret",
    )
    rendered = repr(connection)
    assert "secret" not in rendered
    assert "password=<redacted>" in rendered


def test_connection_resolves_password_per_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = ClickHouseConnection(
        host="clickhouse",
        database="analytics",
        password_env="RAY_CLICKHOUSE_TEST_PASSWORD",
    )
    monkeypatch.setenv("RAY_CLICKHOUSE_TEST_PASSWORD", "secret")
    assert connection.resolve_password() == "secret"


def test_connection_snapshots_environment_password_for_ray_workers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = ClickHouseConnection(
        host="clickhouse",
        database="analytics",
        password_env="RAY_CLICKHOUSE_TEST_PASSWORD",
    )
    monkeypatch.setenv("RAY_CLICKHOUSE_TEST_PASSWORD", "worker-secret")

    distributed = connection.resolve_for_distribution()
    monkeypatch.delenv("RAY_CLICKHOUSE_TEST_PASSWORD")

    assert distributed.password_env is None
    assert distributed.resolve_password() == "worker-secret"
    assert "worker-secret" not in repr(distributed)


def test_connection_rejects_reserved_client_options() -> None:
    with pytest.raises(ConfigurationError):
        ClickHouseConnection.from_options(
            host="clickhouse",
            database="analytics",
            username="reader",
            password="",
            password_env=None,
            port=8123,
            secure=False,
            settings=None,
            client_options={"query_retries": 3},
        )


def test_connection_rejects_reserved_log_comment() -> None:
    with pytest.raises(ConfigurationError):
        ClickHouseConnection.from_options(
            host="clickhouse",
            database="analytics",
            username="reader",
            password="",
            password_env=None,
            port=8123,
            secure=False,
            settings={"log_comment": "custom"},
            client_options=None,
        )


def test_connection_disables_driver_retries() -> None:
    connection = ClickHouseConnection(host="clickhouse", database="analytics")
    kwargs = connection.client_kwargs(ResourceLimits())
    assert kwargs["query_retries"] == 0


def test_resource_limits_validate_task_bounds() -> None:
    with pytest.raises(ConfigurationError):
        ResourceLimits(target_tasks=4, max_tasks=2)
    with pytest.raises(ConfigurationError):
        ResourceLimits(batch_rows=0)


def test_qualified_table_quotes_safe_identifiers() -> None:
    assert QualifiedTable("analytics", "events").sql() == "`analytics`.`events`"


def test_query_spec_repr_redacts_parameter_values() -> None:
    query = QuerySpec(
        "SELECT id FROM events WHERE token = %(token)s",
        (("token", "secret"),),
        ("id",),
    )

    rendered = repr(query)
    assert "secret" not in rendered
    assert "token" in rendered
