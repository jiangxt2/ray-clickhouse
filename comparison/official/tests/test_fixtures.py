from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from ray_clickhouse_comparison.fixtures import (
    CorrectnessAccumulator,
    correctness_identity,
    make_fixture,
    multiset_checksum,
    write_fixture,
)


def test_fixture_is_deterministic_and_has_unique_order_key() -> None:
    first = make_fixture(32, seed=7)
    second = make_fixture(32, seed=7)

    assert first.equals(second)
    assert first.column("id").to_pylist() == list(range(32))
    assert first.schema.field("event_time").type == pa.timestamp("us", tz="UTC")
    assert all(len(value) == 32 for value in first.column("payload").to_pylist())


def test_fixture_payload_size_is_explicit_and_bounded() -> None:
    table = make_fixture(2, payload_bytes=1024)
    assert all(len(value) == 1024 for value in table.column("payload").to_pylist())
    with pytest.raises(ValueError, match="at least 32"):
        make_fixture(1, payload_bytes=31)


def test_multiset_checksum_is_order_independent_and_multiplicity_sensitive() -> None:
    table = make_fixture(16)
    reversed_table = table.take(pa.array(list(reversed(range(16)))))
    duplicated = pa.concat_tables([table, table.slice(0, 1)])

    assert multiset_checksum(table) == multiset_checksum(reversed_table)
    assert multiset_checksum(table) != multiset_checksum(duplicated)


def test_correctness_accumulator_matches_full_table_identity() -> None:
    table = make_fixture(16)
    accumulator = CorrectnessAccumulator()
    accumulator.update(table.slice(0, 5))
    accumulator.update(table.slice(5))

    assert accumulator.finish() == correctness_identity(table)


def test_write_fixture_preserves_correctness_identity(tmp_path: Path) -> None:
    table = make_fixture(8)
    path = tmp_path / "fixture.arrow"
    identity = write_fixture(path, table)

    with path.open("rb") as stream:
        restored = pa.ipc.open_file(stream).read_all()
    assert identity == correctness_identity(restored)
