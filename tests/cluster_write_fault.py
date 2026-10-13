"""Lose a worker's insert acknowledgement after a real ClickHouse commit."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any
from unittest.mock import patch

import clickhouse_connect
import ray
from ray.actor import ActorHandle

from ray_clickhouse.datasink import ClickHouseDataSink


@ray.remote(num_cpus=0)
class WriteEvents:
    def __init__(self) -> None:
        self.counts: dict[str, int] = {}

    def record(self, event: str) -> None:
        self.counts[event] = self.counts.get(event, 0) + 1

    def snapshot(self) -> dict[str, int]:
        return dict(self.counts)


class _LostAckClient:
    def __init__(self, client: Any, events: ActorHandle) -> None:
        self.client = client
        self.events = events

    def insert_arrow(self, *args: Any, **kwargs: Any) -> None:
        self.client.insert_arrow(*args, **kwargs)
        ray.get(self.events.record.remote("committed_batches"))
        raise ConnectionError("Injected connection failure after a confirmed insert")

    def close(self) -> None:
        self.client.close()
        ray.get(self.events.record.remote("closed_clients"))


class LostAckDataSink(ClickHouseDataSink):
    def __init__(self, *, events: ActorHandle, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.events = events

    def write(self, blocks: Iterable[Any], ctx: Any) -> dict[str, object]:
        ray.get(self.events.record.remote("attempts"))
        original = clickhouse_connect.get_client

        def open_client(**kwargs: Any) -> _LostAckClient:
            return _LostAckClient(original(**kwargs), self.events)

        with patch("clickhouse_connect.get_client", side_effect=open_client):
            return super().write(blocks, ctx)
