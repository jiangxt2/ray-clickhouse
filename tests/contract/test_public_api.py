import inspect
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
import ray.data

import ray_clickhouse
from ray_clickhouse import WriteError, read_clickhouse, write_clickhouse
from ray_clickhouse._models import WriteReceipt
from ray_clickhouse.datasink import ClickHouseDataSink
from ray_clickhouse.datasource import ClickHouseDatasource


def test_read_facade_builds_a_ray_datasource_and_snapshots_inputs() -> None:
    parameters = {"tenant": 7}
    settings = {"max_threads": 2}
    remote_args = {"num_cpus": 0.5}
    with patch(
        "ray_clickhouse._api.ray.data.read_datasource", return_value="dataset"
    ) as read:
        result = read_clickhouse(
            host="clickhouse",
            database="analytics",
            table="events",
            columns=["id"],
            filter="tenant_id = %(tenant)s",
            query_parameters=parameters,
            settings=settings,
            concurrency=3,
            override_num_blocks=4,
            ray_remote_args=remote_args,
            num_cpus=1,
            memory=1024,
        )

    assert result == "dataset"
    datasource = read.call_args.args[0]
    assert isinstance(datasource, ClickHouseDatasource)
    assert datasource.config.query_parameters == (("tenant", 7),)
    assert datasource.config.connection.settings == (("max_threads", 2),)
    assert read.call_args.kwargs == {
        "concurrency": 3,
        "override_num_blocks": 4,
        "ray_remote_args": remote_args,
        "num_cpus": 1,
        "memory": 1024,
    }
    parameters["tenant"] = 9
    settings["max_threads"] = 8
    remote_args["num_cpus"] = 4
    assert datasource.config.query_parameters == (("tenant", 7),)
    assert datasource.config.connection.settings == (("max_threads", 2),)
    assert read.call_args.kwargs["ray_remote_args"] == {"num_cpus": 0.5}


def test_public_read_api_does_not_accept_arbitrary_sql() -> None:
    signature = inspect.signature(read_clickhouse)
    assert "query" not in signature.parameters
    assert "table" in signature.parameters
    assert "order_by" in signature.parameters
    assert "num_cpus" in signature.parameters
    assert "memory" in signature.parameters
    with pytest.raises(TypeError):
        read_clickhouse(
            host="clickhouse",
            database="analytics",
            table="events",
            query="SELECT count() FROM events",
        )


def test_write_facade_uses_ray_datasink_and_returns_receipt() -> None:
    dataset = MagicMock(spec=ray.data.Dataset)
    receipt = WriteReceipt(3, 123, 1)
    sink = MagicMock(spec=ClickHouseDataSink)
    sink.receipt = receipt
    with (
        patch("ray_clickhouse._api.ClickHouseDataSink", return_value=sink),
        patch("ray_clickhouse._api.ensure_supported_ray_version"),
    ):
        result = write_clickhouse(
            dataset,
            host="clickhouse",
            database="analytics",
            table="events",
            ray_remote_args={"num_cpus": 1},
            concurrency=2,
        )

    assert result == receipt
    dataset.write_datasink.assert_called_once_with(
        sink,
        ray_remote_args={"num_cpus": 1, "max_retries": 0},
        concurrency=2,
    )


def test_write_facade_fails_if_ray_does_not_complete_the_sink() -> None:
    dataset = MagicMock(spec=ray.data.Dataset)
    sink = MagicMock(spec=ClickHouseDataSink)
    sink.receipt = None
    with (
        patch("ray_clickhouse._api.ClickHouseDataSink", return_value=sink),
        patch("ray_clickhouse._api.ensure_supported_ray_version"),
        pytest.raises(WriteError, match="without a ClickHouse write receipt"),
    ):
        write_clickhouse(
            dataset,
            host="clickhouse",
            database="analytics",
            table="events",
        )

    dataset.write_datasink.assert_called_once_with(
        sink,
        ray_remote_args={"max_retries": 0},
    )


def test_package_root_exports_only_supported_public_components() -> None:
    assert "ClickHouseDatasource" not in ray_clickhouse.__all__
    assert "ClickHouseDataSink" not in ray_clickhouse.__all__
    assert not hasattr(ray_clickhouse, "ClickHouseDatasource")
    assert not hasattr(ray_clickhouse, "ClickHouseDataSink")


def test_internal_components_follow_arrow_and_ray_contracts() -> None:
    assert issubclass(ClickHouseDatasource, ray.data.datasource.Datasource)
    assert issubclass(ClickHouseDataSink, ray.data.datasource.Datasink)
    assert pa.schema([("id", pa.uint64())]).names == ["id"]
