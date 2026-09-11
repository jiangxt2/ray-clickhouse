"""Public, credential-safe error hierarchy."""

from __future__ import annotations


class RayClickHouseError(RuntimeError):
    """Base error for the ray-clickhouse connector."""

    def __init__(
        self,
        message: str,
        *,
        query_id: str | None = None,
        diagnostic: dict[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.query_id = query_id
        self.diagnostic = diagnostic

    def attach_diagnostic(
        self,
        *,
        query_id: str,
        diagnostic: dict[str, object] | None,
    ) -> None:
        self.query_id = query_id
        self.diagnostic = diagnostic
        details = [f"query_id={query_id}"]
        if diagnostic is not None:
            code = diagnostic.get("exception_code")
            message = diagnostic.get("exception")
            if code not in (None, 0):
                details.append(f"clickhouse_exception_code={code}")
            if message:
                details.append(f"clickhouse_exception={str(message)[:1000]}")
        self.args = (f"{self.args[0]} ({', '.join(details)})",)


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
