from __future__ import annotations

from collections.abc import Iterable

import pyarrow as pa
import pytest
from ray.data.block import BlockMetadata

from ray_clickhouse import _compat
from ray_clickhouse._errors import ConfigurationError


@pytest.mark.parametrize(
    "version",
    (
        "2.55.0",
        "2.56.1",
        "2.57.0",
        "2.58.0",
        "2.58.0+local.1",
        "2.58.0+vendor-build_2",
    ),
)
def test_supported_final_ray_versions(
    monkeypatch: pytest.MonkeyPatch, version: str
) -> None:
    monkeypatch.setattr(_compat, "_RAY_VERSION", version)
    _compat.ensure_supported_ray_version()


@pytest.mark.parametrize(
    "version",
    (
        "2.54.9",
        "2.59.0",
        "2.58.0rc1",
        "2.58.0.dev0",
        "2.58.0.post1",
        "2.58",
        "2.58.0+bad..suffix",
    ),
)
def test_unsupported_ray_versions_fail_closed(
    monkeypatch: pytest.MonkeyPatch, version: str
) -> None:
    monkeypatch.setattr(_compat, "_RAY_VERSION", version)
    with pytest.raises(ConfigurationError, match=r">=2\.55,<2\.59"):
        _compat.ensure_supported_ray_version()


def test_make_read_task_preserves_schema_and_row_limit() -> None:
    table = pa.table({"id": pa.array([1, 2], type=pa.int64())})

    def read_fn() -> Iterable[pa.Table]:
        return (table,)

    task = _compat.make_read_task(
        read_fn,
        BlockMetadata(
            num_rows=2,
            size_bytes=table.nbytes,
            input_files=None,
            exec_stats=None,
        ),
        table.schema,
        2,
    )

    assert task.schema == table.schema
    assert task.per_task_row_limit == 2


def test_make_read_task_rejects_missing_schema_signature(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ReadTaskWithoutSchema:
        def __init__(self, read_fn: object, metadata: object) -> None:
            del read_fn, metadata

    monkeypatch.setattr(_compat, "ReadTask", ReadTaskWithoutSchema)
    with pytest.raises(ConfigurationError, match="lacks required schema support"):
        _compat.make_read_task(
            lambda: (),
            BlockMetadata(None, None, None, None),
            pa.schema([]),
            None,
        )


def test_make_read_task_rejects_unsupported_non_null_row_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ReadTaskWithoutRowLimit:
        def __init__(
            self, read_fn: object, metadata: object, schema: object = None
        ) -> None:
            del read_fn, metadata, schema

    monkeypatch.setattr(_compat, "ReadTask", ReadTaskWithoutRowLimit)
    with pytest.raises(ConfigurationError, match="lacks per_task_row_limit support"):
        _compat.make_read_task(
            lambda: (),
            BlockMetadata(None, None, None, None),
            pa.schema([]),
            1,
        )
