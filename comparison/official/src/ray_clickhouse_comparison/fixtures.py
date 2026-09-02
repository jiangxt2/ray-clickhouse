"""Deterministic fixtures and order-independent correctness identities."""

from __future__ import annotations

import base64
import hashlib
import json
import random
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa

_MODULUS = 1 << 256


def make_fixture(
    rows: int = 256,
    *,
    seed: int = 20260901,
    payload_bytes: int = 32,
) -> pa.Table:
    if rows < 0:
        raise ValueError("rows must be non-negative")
    if payload_bytes < 32:
        raise ValueError("payload_bytes must be at least 32")
    randomizer = random.Random(seed)
    epoch = datetime(2025, 1, 1, tzinfo=UTC)
    values: dict[str, list[Any]] = {
        "id": [],
        "partition_key": [],
        "nullable_value": [],
        "amount": [],
        "event_time": [],
        "event_date": [],
        "payload": [],
    }
    for index in range(rows):
        values["id"].append(index)
        values["partition_key"].append(f"p{index % 8:02d}")
        values["nullable_value"].append(None if index % 7 == 0 else index * 3)
        values["amount"].append(Decimal(index * 101 - 17).scaleb(-2))
        values["event_time"].append(epoch + timedelta(microseconds=index * 1009))
        values["event_date"].append(date(2025, 1, 1) + timedelta(days=index % 31))
        prefix = f"row-{index:08d}-{randomizer.getrandbits(48):012x}-"
        values["payload"].append(prefix + "x" * (payload_bytes - len(prefix)))
    fields: list[pa.Field[Any]] = [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("partition_key", pa.string(), nullable=False),
        pa.field("nullable_value", pa.int64(), nullable=True),
        pa.field("amount", pa.decimal128(18, 2), nullable=False),
        pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("event_date", pa.date32(), nullable=False),
        pa.field("payload", pa.string(), nullable=False),
    ]
    schema = pa.schema(fields)
    return pa.Table.from_pydict(values, schema=schema)


def _canonical_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return {"float_hex": value.hex()}
    if isinstance(value, Decimal):
        return {"decimal": format(value, "f")}
    if isinstance(value, datetime):
        return {"datetime": value.astimezone(UTC).isoformat(timespec="microseconds")}
    if isinstance(value, date):
        return {"date": value.isoformat()}
    if isinstance(value, bytes):
        return {"bytes": base64.b64encode(value).decode("ascii")}
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _canonical_value(item) for key, item in sorted(value.items())}
    raise TypeError(f"unsupported checksum value: {type(value).__name__}")


def schema_sha256(schema: pa.Schema) -> str:
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()


class CorrectnessAccumulator:
    """Compute a schema and order-independent checksum without retaining blocks."""

    def __init__(self) -> None:
        self._schema: pa.Schema | None = None
        self._row_count = 0
        self._aggregate_sum = 0
        self._aggregate_square = 0
        self._aggregate_xor = 0

    def update(self, table: pa.Table) -> None:
        if self._schema is None:
            self._schema = table.schema
        elif not table.schema.equals(self._schema, check_metadata=True):
            raise ValueError("streamed blocks do not have one canonical Arrow schema")
        for row in table.to_pylist():
            payload = json.dumps(
                _canonical_value(row),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("ascii")
            value = int.from_bytes(hashlib.sha256(payload).digest(), "big")
            self._row_count += 1
            self._aggregate_sum = (self._aggregate_sum + value) % _MODULUS
            self._aggregate_square = (self._aggregate_square + value * value) % _MODULUS
            self._aggregate_xor ^= value

    def finish(self) -> dict[str, int | str]:
        if self._schema is None:
            raise ValueError("cannot finish an empty correctness accumulator")
        identity = (
            self._row_count.to_bytes(16, "big")
            + self._aggregate_sum.to_bytes(32, "big")
            + self._aggregate_square.to_bytes(32, "big")
            + self._aggregate_xor.to_bytes(32, "big")
        )
        return {
            "row_count": self._row_count,
            "schema_sha256": schema_sha256(self._schema),
            "multiset_sha256": hashlib.sha256(identity).hexdigest(),
        }


def multiset_checksum(table: pa.Table) -> str:
    accumulator = CorrectnessAccumulator()
    accumulator.update(table)
    return str(accumulator.finish()["multiset_sha256"])


def correctness_identity(table: pa.Table) -> dict[str, int | str]:
    accumulator = CorrectnessAccumulator()
    accumulator.update(table)
    return accumulator.finish()


def write_fixture(path: Path, table: pa.Table) -> dict[str, int | str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as stream, pa.ipc.new_file(stream, table.schema) as writer:
        writer.write_table(table)
    return correctness_identity(table)
