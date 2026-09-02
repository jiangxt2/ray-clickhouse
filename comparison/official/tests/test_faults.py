from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import cast

import pytest

from ray_clickhouse_comparison.faults import FaultController, FaultMode, OneShotProxy


def test_fault_controller_is_one_shot_and_records_client(tmp_path: Path) -> None:
    controller = FaultController(tmp_path)
    controller.arm("drop_response", "token-1")

    event = controller.consume("10.0.0.4")
    assert event is not None
    assert event.token == "token-1"
    assert event.mode == "drop_response"
    assert controller.consume("10.0.0.5") is None
    recorded = json.loads((tmp_path / "fault-event.json").read_text(encoding="utf-8"))
    assert recorded["client_ip"] == "10.0.0.4"


def test_second_fault_cannot_be_armed_while_one_is_pending(tmp_path: Path) -> None:
    controller = FaultController(tmp_path)
    controller.arm("hold_response", "first")
    with pytest.raises(RuntimeError, match="already armed"):
        controller.arm("drop_response", "second")


@pytest.mark.parametrize("token", ["", "has space", "\t"])
def test_fault_token_is_portable(tmp_path: Path, token: str) -> None:
    with pytest.raises(ValueError, match="fault token"):
        FaultController(tmp_path).arm("drop_response", token)


async def _exercise_proxy(
    tmp_path: Path, mode: str | None
) -> tuple[bytes, dict[str, object] | None]:
    async def target(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readuntil(b"\r\n\r\n")
        if mode == "drop_response_continue":
            writer.write(b"HTTP/1.1 100 Continue\r\n\r\n")
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    target_server = await asyncio.start_server(target, "127.0.0.1", 0)
    target_port = int(target_server.sockets[0].getsockname()[1])
    control = FaultController(tmp_path)
    if mode is not None:
        fault_mode = "drop_response" if mode == "drop_response_continue" else mode
        control.arm(cast(FaultMode, fault_mode), "proxy-token")
    proxy = OneShotProxy("127.0.0.1", 0, "127.0.0.1", target_port, control)
    proxy_server = await asyncio.start_server(proxy._handle, "127.0.0.1", 0)
    proxy_port = int(proxy_server.sockets[0].getsockname()[1])
    reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
    writer.write(
        b"POST /?query=INSERT%20INTO%20t HTTP/1.1\r\n"
        b"Host: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    )
    await writer.drain()
    if mode == "hold_response":
        for _ in range(100):
            if control.event_path.exists():
                break
            await asyncio.sleep(0.01)
        writer.close()
        await writer.wait_closed()
        response = b""
    else:
        response = await asyncio.wait_for(reader.read(), timeout=2)
        writer.close()
        await writer.wait_closed()
    proxy_server.close()
    target_server.close()
    await proxy_server.wait_closed()
    await target_server.wait_closed()
    event = (
        json.loads(control.event_path.read_text(encoding="utf-8"))
        if control.event_path.exists()
        else None
    )
    return response, event


def test_proxy_forwards_normal_response(tmp_path: Path) -> None:
    response, event = asyncio.run(_exercise_proxy(tmp_path, None))
    assert response.endswith(b"OK")
    assert event is None


def test_proxy_drops_exactly_one_response_after_server_reply(tmp_path: Path) -> None:
    response, event = asyncio.run(_exercise_proxy(tmp_path, "drop_response"))
    assert response.startswith(b"HTTP/1.1 200 OK\r\n")
    assert response.endswith(b"Connection: close\r\n\r\n")
    assert event is not None
    assert event["mode"] == "drop_response"


def test_proxy_holds_response_until_client_process_disappears(tmp_path: Path) -> None:
    response, event = asyncio.run(_exercise_proxy(tmp_path, "hold_response"))
    assert response == b""
    assert event is not None
    assert event["mode"] == "hold_response"


def test_proxy_does_not_treat_interim_http_response_as_commit_boundary(tmp_path: Path) -> None:
    response, event = asyncio.run(_exercise_proxy(tmp_path, "drop_response_continue"))
    assert response.startswith(b"HTTP/1.1 100 Continue\r\n\r\n")
    assert response.endswith(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\n")
    assert event is not None
    assert event["mode"] == "drop_response"
