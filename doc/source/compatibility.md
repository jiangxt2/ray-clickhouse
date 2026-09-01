# Compatibility

## Supported versions

| Component | Supported range |
| --- | --- |
| Python | `>=3.10,<3.14` |
| Ray | Final releases `>=2.55,<2.59` |
| PyArrow | `>=19,<20` |
| clickhouse-connect | `>=1.5,<1.6` with Arrow support |

Prerelease, development, and post-release Ray builds are rejected. PEP 440 local Ray build suffixes are accepted. Pandas is resolved through `ray[data]` and is not an independent compatibility promise.

The primary current baseline is Python 3.12, Ray 2.58.0, PyArrow 19, and ClickHouse 26.8.1.2041. Python 3.10–3.13 and Ray 2.55.0, 2.56.1, 2.57.0, and 2.58.0 are covered by the repository CI matrix.

## Public compatibility contract

The package root exports `read_clickhouse()`, `write_clickhouse()`, `WriteReceipt`, and the documented error hierarchy. Datasource and Datasink implementation classes are internal and do not carry independent compatibility guarantees.

Ray's `ReadTask` and Datasink lifecycle are DeveloperAPI contracts. Version-specific adaptation remains isolated in `_compat.py`; the connector does not import Ray internal modules.

## Error hierarchy

All connector exceptions derive from `RayClickHouseError`. Configuration and connection setup use `ConfigurationError`, `AuthenticationError`, `PermissionError`, and `ObjectNotFoundError`. Discovery and conversion use `DiscoveryError` and `SchemaError`. Data-path failures use `TransportError`, `ReadError`, and `WriteError`.

`AmbiguousWriteError` is a `WriteError` for an INSERT whose final server outcome is unknown. `AmbiguousTableManagementError` is a `WriteError` for generated table management that may have completed only partially. Callers must reconcile either ambiguous result before deciding whether another mutation is safe.
