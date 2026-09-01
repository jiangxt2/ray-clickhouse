"""Public Ray Data ClickHouse connector API."""

from ray_clickhouse._api import read_clickhouse, write_clickhouse
from ray_clickhouse._errors import (
    AmbiguousTableManagementError,
    AmbiguousWriteError,
    AuthenticationError,
    ConfigurationError,
    DiscoveryError,
    ObjectNotFoundError,
    PermissionError,
    RayClickHouseError,
    ReadError,
    SchemaError,
    TransportError,
    WriteError,
)
from ray_clickhouse._models import WriteReceipt

__all__ = [
    "AmbiguousWriteError",
    "AmbiguousTableManagementError",
    "AuthenticationError",
    "ConfigurationError",
    "DiscoveryError",
    "ObjectNotFoundError",
    "PermissionError",
    "RayClickHouseError",
    "ReadError",
    "SchemaError",
    "TransportError",
    "WriteError",
    "WriteReceipt",
    "read_clickhouse",
    "write_clickhouse",
]
