from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from typing import Any

import pyarrow as pa
import pytest

os.environ.setdefault("RAY_ENABLE_UV_RUN_RUNTIME_ENV", "0")
os.environ.setdefault("RAY_USAGE_STATS_ENABLED", "0")

import ray  # noqa: E402
from ray.data.block import BlockMetadata  # noqa: E402
from ray.data.datasource import (  # noqa: E402
    Datasink,
    Datasource,
    ReadTask,
    WriteResult,
)

from ray_clickhouse import write_clickhouse  # noqa: E402
from ray_clickhouse._compat import make_read_task  # noqa: E402
from ray_clickhouse._models import WriteReceipt  # noqa: E402

pytestmark = pytest.mark.ray


class _TableDatasource(Datasource):
    def estimate_inmemory_data_size(self) -> None:
        return None

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: int | None = None,
        data_context: Any | None = None,
    ) -> list[ReadTask]:
        del parallelism, data_context
        table = pa.table({"id": pa.array([1, 2, 3], type=pa.int64())})

        def read_fn() -> Iterable[pa.Table]:
            return (table,)

        return [
            make_read_task(
                read_fn,
                BlockMetadata(
                    num_rows=3,
                    size_bytes=table.nbytes,
                    input_files=None,
                    exec_stats=None,
                ),
                table.schema,
                per_task_row_limit,
            )
        ]


class _RecordingSink(Datasink[Mapping[str, int]]):
    def __init__(self) -> None:
        self.starts: list[pa.Schema | None] = []
        self.completed: WriteResult[Mapping[str, int]] | None = None

    def on_write_start(self, schema: pa.Schema | None = None) -> None:
        self.starts.append(schema)

    def write(self, blocks: Iterable[Any], ctx: Any) -> Mapping[str, int]:
        del ctx
        rows = 0
        for block in blocks:
            assert isinstance(block, pa.Table)
            rows += block.num_rows
        return {"rows": rows}

    def on_write_complete(self, write_result: WriteResult[Mapping[str, int]]) -> None:
        self.completed = write_result


@pytest.fixture(scope="module", autouse=True)
def ray_runtime() -> Iterable[None]:
    if ray.is_initialized():
        yield
        return
    ray.init(address="local", include_dashboard=False, num_cpus=2)
    try:
        yield
    finally:
        ray.shutdown()


def test_public_read_datasource_executes_read_task() -> None:
    dataset = ray.data.read_datasource(
        _TableDatasource(),
        num_cpus=0.25,
        concurrency=1,
        override_num_blocks=1,
    )

    assert dataset.take_all() == [{"id": 1}, {"id": 2}, {"id": 3}]


def test_public_write_datasink_preserves_callback_results() -> None:
    sink = _RecordingSink()
    dataset = ray.data.from_arrow(pa.table({"id": pa.array([1, 2, 3])}))

    dataset.write_datasink(sink, ray_remote_args={"max_retries": 0})

    assert len(sink.starts) == 1
    assert sink.starts[0] == pa.schema([("id", pa.int64())])
    assert sink.completed is not None
    assert sink.completed.num_rows == 3
    assert sink.completed.write_returns == [{"rows": 3}]


def test_public_write_datasink_completes_empty_dataset() -> None:
    sink = _RecordingSink()

    ray.data.range(0).write_datasink(sink, ray_remote_args={"max_retries": 0})

    assert sink.starts == []
    assert sink.completed is not None
    assert sink.completed.write_returns == []


@pytest.mark.parametrize("write_mode", ["append", "create", "overwrite"])
def test_write_facade_returns_confirmed_zero_receipt_for_empty_dataset(
    write_mode: str,
) -> None:
    receipt = write_clickhouse(
        ray.data.range(0),
        host="unused",
        database="analytics",
        table="events",
        write_mode=write_mode,
    )

    assert receipt == WriteReceipt(0, 0, 0)
