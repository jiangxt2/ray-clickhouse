"""Public, credential-safe error hierarchy."""

from __future__ import annotations


class RayClickHouseError(RuntimeError):
    """Base error for the ray-clickhouse connector."""


class ConfigurationError(RayClickHouseError, ValueError):
    """The connector configuration is invalid."""


class AuthenticationError(RayClickHouseError):
    """ClickHouse rejected the configured credentials."""


class PermissionError(RayClickHouseError):
    """The ClickHouse account lacks a required permission."""


class ObjectNotFoundError(RayClickHouseError):
    """The requested ClickHouse object does not exist."""


class DiscoveryError(RayClickHouseError):
    """Metadata or split discovery failed."""


class SchemaError(RayClickHouseError):
    """The ClickHouse schema cannot be represented safely in Arrow."""


class TransportError(RayClickHouseError):
    """The selected ClickHouse transport failed."""


class ReadError(RayClickHouseError):
    """ClickHouse returned an invalid or unreadable result batch."""


class WriteError(RayClickHouseError):
    """ClickHouse rejected a write batch."""


class AmbiguousWriteError(WriteError):
    """The write outcome is unknown because the response was not confirmed."""


class AmbiguousTableManagementError(WriteError):
    """A destructive table-management operation may have partially completed."""
