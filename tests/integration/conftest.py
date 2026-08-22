from __future__ import annotations

import os
import time
import uuid
from collections.abc import Iterator

import clickhouse_connect
import pytest

DATABASE = os.environ.get("RAY_CLICKHOUSE_IT_DATABASE", "ray_clickhouse_it")


@pytest.fixture(scope="session", autouse=True)
def ray_runtime() -> Iterator[None]:
    address = os.environ.get("RAY_CLICKHOUSE_IT_RAY_ADDRESS")
    if not address:
        yield
        return

    import ray

    os.environ["RAY_ADDRESS"] = address
    ray.init(address="auto", namespace="ray-clickhouse-it")
    minimum_nodes = int(os.environ.get("RAY_CLICKHOUSE_IT_MIN_NODES", "3"))
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        alive_nodes = [node for node in ray.nodes() if node["Alive"]]
        if len(alive_nodes) >= minimum_nodes:
            break
        time.sleep(1)
    else:
        ray.shutdown()
        raise RuntimeError(f"Ray cluster did not reach {minimum_nodes} alive nodes")
    yield
    ray.shutdown()


def _connection_options() -> dict[str, object]:
    return {
        "host": os.environ.get("RAY_CLICKHOUSE_IT_HOST", "127.0.0.1"),
        "port": int(os.environ.get("RAY_CLICKHOUSE_IT_PORT", "18123")),
        "username": os.environ.get("RAY_CLICKHOUSE_IT_USER", "default"),
        "password": os.environ.get("RAY_CLICKHOUSE_IT_PASSWORD", ""),
        "database": DATABASE,
        "query_retries": 0,
        "tz_mode": "schema",
        "show_clickhouse_errors": "scrub",
    }


@pytest.fixture(scope="session")
def clickhouse_client() -> Iterator[object]:
    client = clickhouse_connect.get_client(**_connection_options())
    try:
        client.command(f"CREATE DATABASE IF NOT EXISTS `{DATABASE}`")
        yield client
    finally:
        client.close()


@pytest.fixture
def table_name(clickhouse_client: object) -> Iterator[str]:
    name = f"ray_clickhouse_it_{uuid.uuid4().hex[:12]}"
    try:
        yield name
    finally:
        clickhouse_client.command(f"DROP TABLE IF EXISTS `{DATABASE}`.`{name}`")


@pytest.fixture
def connection_options() -> dict[str, object]:
    options = _connection_options()
    return {
        key: options[key]
        for key in ("host", "port", "username", "password", "database")
    }
