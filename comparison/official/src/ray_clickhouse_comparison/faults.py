"""One-shot TCP response fault proxy used by approved real-infrastructure tests."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

FaultMode = Literal["drop_response", "hold_response"]


@dataclass(frozen=True)
class FaultEvent:
    token: str
    mode: FaultMode
    client_ip: str
    observed_at: float


class FaultController:
    """File-backed one-shot controller shared by proxy, runner, and host."""

    def __init__(self, control_dir: Path) -> None:
        self.control_dir = control_dir
        self.control_path = control_dir / "fault-control.json"
        self.event_path = control_dir / "fault-event.json"

    def _atomic_write(self, path: Path, value: dict[str, object]) -> None:
        self.control_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=self.control_dir, delete=False, prefix=f".{path.name}."
        ) as stream:
            json.dump(value, stream, sort_keys=True)
            stream.write("\n")
            temporary = Path(stream.name)
        os.replace(temporary, path)

    def arm(self, mode: FaultMode, token: str) -> None:
        if mode not in {"drop_response", "hold_response"}:
            raise ValueError(f"unsupported fault mode: {mode}")
        if not token or any(character.isspace() for character in token):
            raise ValueError("fault token must be a non-empty token without whitespace")
        if self.control_path.exists():
            current = json.loads(self.control_path.read_text(encoding="utf-8"))
            if current.get("armed"):
                raise RuntimeError("a fault is already armed")
        self._atomic_write(
            self.control_path,
            {"schema_version": 1, "armed": True, "mode": mode, "token": token},
        )

    def consume(self, client_ip: str) -> FaultEvent | None:
        if not self.control_path.exists():
            return None
        value = json.loads(self.control_path.read_text(encoding="utf-8"))
        if not value.get("armed"):
            return None
        mode = value.get("mode")
        token = value.get("token")
        if mode not in {"drop_response", "hold_response"} or not isinstance(token, str):
            raise RuntimeError("malformed fault control state")
        event = FaultEvent(token, mode, client_ip, time.time())
        self._atomic_write(
            self.control_path,
            {"schema_version": 1, "armed": False, "mode": mode, "token": token},
        )
        self._atomic_write(
            self.event_path,
            {
                "schema_version": 1,
                "token": token,
                "mode": mode,
                "client_ip": client_ip,
                "observed_at": event.observed_at,
            },
        )
        return event


class OneShotProxy:
    def __init__(
        self,
        listen_host: str,
        listen_port: int,
        target_host: str,
        target_port: int,
        control: FaultController,
    ) -> None:
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.target_host = target_host
        self.target_port = target_port
        self.control = control

    async def _handle(
        self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter
    ) -> None:
        peer = client_writer.get_extra_info("peername")
        client_ip = str(peer[0]) if isinstance(peer, tuple) and peer else "unknown"
        try:
            server_reader, server_writer = await asyncio.open_connection(
                self.target_host, self.target_port
            )
        except Exception:
            client_writer.close()
            await asyncio.gather(client_writer.wait_closed(), return_exceptions=True)
            return
        saw_insert = asyncio.Event()
        client_done = asyncio.Event()
        captured = bytearray()

        async def client_to_server() -> None:
            try:
                while chunk := await client_reader.read(65536):
                    if len(captured) < 1024 * 1024:
                        captured.extend(chunk)
                        # The measured log comment contains ``role=insert`` for
                        # planning queries too.  Match the SQL verb and target
                        # clause so discovery/telemetry SELECT responses cannot
                        # consume the post-commit fault.
                        lowered = captured.lower()
                        if (
                            b"insert into" in lowered
                            or b"insert+into" in lowered
                            or b"insert%20into" in lowered
                        ):
                            saw_insert.set()
                    server_writer.write(chunk)
                    await server_writer.drain()
                try:
                    server_writer.write_eof()
                except (AttributeError, OSError):
                    pass
            finally:
                client_done.set()

        async def server_to_client() -> None:
            response_header = bytearray()
            while True:
                try:
                    chunk = await asyncio.wait_for(server_reader.read(65536), timeout=0.25)
                except TimeoutError:
                    if client_done.is_set():
                        return
                    continue
                if not chunk:
                    return
                if saw_insert.is_set():
                    response_header.extend(chunk)
                    while saw_insert.is_set():
                        header_end = response_header.find(b"\r\n\r\n")
                        if header_end < 0:
                            break
                        header_end += 4
                        status_line = bytes(response_header[:header_end]).split(b"\r\n", 1)[0]
                        fields = status_line.split()
                        status = int(fields[1]) if len(fields) >= 2 and fields[1].isdigit() else 0
                        if 100 <= status < 200:
                            client_writer.write(response_header[:header_end])
                            await client_writer.drain()
                            del response_header[:header_end]
                            continue
                        event = self.control.consume(client_ip)
                        if event is not None:
                            if event.mode == "hold_response":
                                try:
                                    await asyncio.wait_for(client_done.wait(), timeout=60)
                                except TimeoutError:
                                    pass
                            else:
                                # A clean EOF is treated by clickhouse-connect as a
                                # stale keep-alive and is retried once, which would
                                # turn an accepted insert into an apparently
                                # confirmed write.  Return a deliberately truncated
                                # successful response instead: the server has
                                # already committed the request, while the client
                                # must surface a transport error without replaying
                                # the insert.
                                client_writer.write(
                                    b"HTTP/1.1 200 OK\r\n"
                                    b"Content-Length: 1\r\n"
                                    b"Connection: close\r\n\r\n"
                                )
                                await client_writer.drain()
                            return
                        chunk = bytes(response_header)
                        response_header.clear()
                        saw_insert.clear()
                        captured.clear()
                    if saw_insert.is_set():
                        continue
                client_writer.write(chunk)
                await client_writer.drain()

        client_task = asyncio.create_task(client_to_server())
        server_task = asyncio.create_task(server_to_client())
        try:
            done, _ = await asyncio.wait(
                (client_task, server_task), return_when=asyncio.FIRST_COMPLETED
            )
            if server_task in done:
                if not client_task.done():
                    client_task.cancel()
            else:
                await server_task
            await asyncio.gather(client_task, server_task, return_exceptions=True)
        finally:
            server_writer.close()
            client_writer.close()
            await asyncio.gather(
                server_writer.wait_closed(), client_writer.wait_closed(), return_exceptions=True
            )

    async def serve(self) -> None:
        server = await asyncio.start_server(self._handle, self.listen_host, self.listen_port)
        async with server:
            await server.serve_forever()


def _parse_endpoint(value: str) -> tuple[str, int]:
    host, separator, port = value.rpartition(":")
    if not separator or not host:
        raise ValueError(f"endpoint must be host:port: {value!r}")
    return host, int(port)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    subcommands = parser.add_subparsers(dest="command", required=True)
    serve = subcommands.add_parser("serve")
    serve.add_argument("--listen", required=True)
    serve.add_argument("--target", required=True)
    serve.add_argument("--control-dir", type=Path, required=True)
    arm = subcommands.add_parser("arm")
    arm.add_argument("--mode", choices=("drop_response", "hold_response"), required=True)
    arm.add_argument("--token", required=True)
    arm.add_argument("--control-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "arm":
        FaultController(args.control_dir).arm(args.mode, args.token)
        return
    listen_host, listen_port = _parse_endpoint(args.listen)
    target_host, target_port = _parse_endpoint(args.target)
    proxy = OneShotProxy(
        listen_host,
        listen_port,
        target_host,
        target_port,
        FaultController(args.control_dir),
    )
    asyncio.run(proxy.serve())


if __name__ == "__main__":
    main()
