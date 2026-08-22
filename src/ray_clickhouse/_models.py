"""Immutable configuration and data contracts."""

from __future__ import annotations

import math
import os
import pickle
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

from ray_clickhouse._errors import ConfigurationError

SplitMode = Literal["single", "partition", "range"]
DiscoveryPolicy = Literal["single", "error"]
InsertMode = Literal["sync", "async"]
WriteMode = Literal["append", "create", "overwrite"]
OrderBy = tuple[tuple[str, bool], ...]

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RESERVED_OPTIONS = frozenset(
    {
        "host",
        "port",
        "database",
        "username",
        "password",
        "secure",
        "connect_timeout",
        "send_receive_timeout",
        "query_retries",
        "tz_mode",
        "show_clickhouse_errors",
    }
)
_RESERVED_SETTINGS = frozenset({"max_block_size", "max_execution_time", "query_id"})


def validate_identifier(value: str, *, name: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ConfigurationError(f"{name} must be a simple ClickHouse identifier")
    return value


def snapshot_mapping(
    value: Mapping[str, Any] | None,
    *,
    name: str,
    reserved: set[str] | frozenset[str] = frozenset(),
) -> tuple[tuple[str, Any], ...]:
    if value is None:
        return ()
    if not isinstance(value, Mapping):
        raise ConfigurationError(f"{name} must be a mapping")
    items = []
    for key, item in value.items():
        if not isinstance(key, str) or not key:
            raise ConfigurationError(f"{name} keys must be non-empty strings")
        if key in reserved:
            raise ConfigurationError(f"{name} cannot override managed option {key!r}")
        try:
            copied = pickle.loads(pickle.dumps(item, protocol=pickle.HIGHEST_PROTOCOL))
        except Exception:
            raise ConfigurationError(f"{name}[{key!r}] must be serializable") from None
        items.append((key, copied))
    return tuple(sorted(items))


@dataclass(frozen=True)
class QualifiedTable:
    database: str
    table: str

    def __post_init__(self) -> None:
        validate_identifier(self.database, name="database")
        validate_identifier(self.table, name="table")

    @staticmethod
    def _quote(value: str) -> str:
        return f"`{value.replace('`', '``')}`"

    def sql(self) -> str:
        return f"{self._quote(self.database)}.{self._quote(self.table)}"

    def __str__(self) -> str:
        return f"{self.database}.{self.table}"


@dataclass(frozen=True)
class ResourceLimits:
    batch_rows: int = 65_536
    batch_bytes: int = 64 * 1024 * 1024
    target_tasks: int = 8
    max_tasks: int = 256
    connect_timeout_seconds: float = 10.0
    query_timeout_seconds: float = 300.0

    def __post_init__(self) -> None:
        if not isinstance(self.batch_rows, int) or isinstance(self.batch_rows, bool):
            raise ConfigurationError("batch_rows must be an integer")
        if not 1 <= self.batch_rows <= 1_000_000:
            raise ConfigurationError("batch_rows must be between 1 and 1,000,000")
        if not isinstance(self.batch_bytes, int) or isinstance(self.batch_bytes, bool):
            raise ConfigurationError("batch_bytes must be an integer")
        if not 1 <= self.batch_bytes <= 1_073_741_824:
            raise ConfigurationError("batch_bytes must be between 1 and 1,073,741,824")
        if not isinstance(self.target_tasks, int) or isinstance(
            self.target_tasks, bool
        ):
            raise ConfigurationError("target_tasks must be an integer")
        if not 1 <= self.target_tasks <= 1_024:
            raise ConfigurationError("target_tasks must be between 1 and 1,024")
        if not isinstance(self.max_tasks, int) or isinstance(self.max_tasks, bool):
            raise ConfigurationError("max_tasks must be an integer")
        if not 1 <= self.max_tasks <= 1_024 or self.target_tasks > self.max_tasks:
            raise ConfigurationError("max_tasks must be >= target_tasks and <= 1,024")
        for name, value in (
            ("connect_timeout_seconds", self.connect_timeout_seconds),
            ("query_timeout_seconds", self.query_timeout_seconds),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or not 0 < value <= 86_400
            ):
                raise ConfigurationError(
                    f"{name} must be finite and between 0 and 86,400"
                )


@dataclass(frozen=True, repr=False)
class ClickHouseConnection:
    host: str
    database: str
    username: str = "default"
    password: str = field(default="", repr=False, compare=False)
    password_env: str | None = field(default=None, repr=False, compare=False)
    port: int = 8123
    secure: bool = False
    settings: tuple[tuple[str, Any], ...] = field(default_factory=tuple, repr=False)
    client_options: tuple[tuple[str, Any], ...] = field(
        default_factory=tuple, repr=False
    )

    def __post_init__(self) -> None:
        if not isinstance(self.host, str) or not self.host:
            raise ConfigurationError("host must not be empty")
        validate_identifier(self.database, name="database")
        if not isinstance(self.username, str) or not self.username:
            raise ConfigurationError("username must not be empty")
        if (
            isinstance(self.port, bool)
            or not isinstance(self.port, int)
            or not 1 <= self.port <= 65535
        ):
            raise ConfigurationError("port must be between 1 and 65,535")
        if self.password and self.password_env is not None:
            raise ConfigurationError("password and password_env are mutually exclusive")
        if (
            self.password_env is not None
            and _ENV_NAME.fullmatch(self.password_env) is None
        ):
            raise ConfigurationError("password_env must be a portable environment name")

    @classmethod
    def from_options(
        cls,
        *,
        host: str,
        database: str,
        username: str,
        password: str,
        password_env: str | None,
        port: int,
        secure: bool,
        settings: Mapping[str, Any] | None,
        client_options: Mapping[str, Any] | None,
    ) -> ClickHouseConnection:
        return cls(
            host=host,
            database=database,
            username=username,
            password=password,
            password_env=password_env,
            port=port,
            secure=secure,
            settings=snapshot_mapping(
                settings, name="settings", reserved=_RESERVED_SETTINGS
            ),
            client_options=snapshot_mapping(
                client_options,
                name="client_options",
                reserved=_RESERVED_OPTIONS,
            ),
        )

    def resolve_password(self) -> str:
        if self.password_env is None:
            return self.password
        try:
            return os.environ[self.password_env]
        except KeyError:
            raise ConfigurationError(
                "configured password environment variable is unavailable"
            ) from None

    def client_kwargs(self, limits: ResourceLimits) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "username": self.username,
            "password": self.resolve_password(),
            "secure": self.secure,
            "connect_timeout": limits.connect_timeout_seconds,
            "send_receive_timeout": limits.query_timeout_seconds,
            "query_retries": 0,
            "tz_mode": "schema",
            "show_clickhouse_errors": "scrub",
        }
        kwargs.update(dict(self.client_options))
        return kwargs

    def query_settings(self, limits: ResourceLimits) -> dict[str, Any]:
        settings = dict(self.settings)
        settings["max_block_size"] = limits.batch_rows
        settings["max_execution_time"] = max(1, math.ceil(limits.query_timeout_seconds))
        return settings

    def __repr__(self) -> str:
        return (
            "ClickHouseConnection("
            f"host={self.host!r}, database={self.database!r}, "
            f"username={self.username!r}, "
            "password=<redacted>, "
            f"port={self.port}, secure={self.secure}, "
            f"settings={tuple(key for key, _ in self.settings)!r}, "
            f"client_options={tuple(key for key, _ in self.client_options)!r})"
        )


@dataclass(frozen=True, repr=False)
class QuerySpec:
    sql: str
    parameters: tuple[tuple[str, Any], ...]
    arrow_schema: Any
    operation: str = "read"

    def parameter_dict(self) -> dict[str, Any]:
        return dict(self.parameters)

    def __repr__(self) -> str:
        columns = tuple(getattr(self.arrow_schema, "names", ()))
        parameter_names = tuple(name for name, _ in self.parameters)
        return (
            f"QuerySpec(operation={self.operation!r}, columns={columns!r}, "
            f"parameter_names={parameter_names!r})"
        )


@dataclass(frozen=True)
class WriteReceipt:
    rows_written: int
    bytes_written: int
    batches_written: int
    status: str = "confirmed"
    ambiguous_batches: int = 0
