# Compatibility

## Supported versions

| Component | Supported range |
| --- | --- |
| Python | `>=3.10,<3.14` |
| Ray | Final releases `>=2.55,<2.60` |
| PyArrow | `>=19,<20` |
| clickhouse-connect | `>=1.5,<1.6` with Arrow support |

Prerelease, development, and post-release Ray builds are rejected. PEP 440 local Ray build suffixes are accepted. Pandas is resolved through `ray[data]` and is not an independent compatibility promise.

The primary current baseline is Python 3.12, Ray 2.59.0, PyArrow 19, and ClickHouse 26.8.1.2041. Python 3.10–3.13 and Ray 2.55.0, 2.56.1, 2.57.0, 2.58.0, and 2.59.0 are covered by the repository CI matrix.

## Public compatibility contract

The package root exports `read_clickhouse()`, `write_clickhouse()`, `WriteReceipt`, and the documented error hierarchy. Datasource and Datasink implementation classes are internal and do not carry independent compatibility guarantees.

Ray's `ReadTask` and Datasink lifecycle are DeveloperAPI contracts. Version-specific adaptation remains isolated in `_compat.py`; the connector does not import Ray internal modules.

Automatic V1 table-read projection pushdown is enabled on Ray 2.58.0 and 2.59.0
(including accepted local-build suffixes). Other supported releases retain the
existing Ray-side column selection behavior. The verified projection contract
includes column ordering, independent Dataset branches and native empty-projection
row counts through the public `BlockAccessor` API. Query reads do not opt in.

## Error hierarchy

All connector exceptions derive from `RayClickHouseError`. Configuration and connection setup use `ConfigurationError`, `AuthenticationError`, `PermissionError`, and `ObjectNotFoundError`. Discovery and conversion use `DiscoveryError` and `SchemaError`. Data-path failures use `TransportError`, `ReadError`, and `WriteError`.

`AmbiguousWriteError` is a `WriteError` for an INSERT whose final server outcome is unknown. `AmbiguousTableManagementError` is a `WriteError` for generated table management that may have completed only partially. Callers must reconcile either ambiguous result before deciding whether another mutation is safe.

## ClickHouse metadata compatibility

Filtered Distributed conversion uses node capabilities rather than version
thresholds. Equivalent metadata origins may differ between nodes, but required
fields, declarations, schema and effective context must satisfy the same
contract. Native metadata operations enforce privileges without CHECK GRANT or
textual grant/role inference. System-table policy inventory is checked against
independent native enumeration after neutral result settings are verified.
Policy/grant configuration must remain static for the planned read; this is not a
security metadata snapshot. Unsupported mandatory capabilities select the logical source before
execution. Invalid metadata and real access or transport failures raise.

`./scripts/run_distributed_it.sh` starts two shards with two replicas each,
Keeper, HTTPS and three Ray nodes. Set `RAY_CLICKHOUSE_IT_IMAGE` to a fixed
compatible image reference and use `RAY_CLICKHOUSE_IT_BASELINE` to keep separate
artifacts. Run a minimal distributed suite on extra server baselines; do not
repeat the entire protocol suite for each image. Runtime version and image
identity belong in the test evidence, independently of capability decisions.

Local ARM64 validation covers ClickHouse 23.3.22.3, 24.8.14.39, 25.8.14.17,
25.12.2.54 and 26.8.1.2041. Each baseline covers 47 distributed cases, including
four split modes, two replicas per shard, native role authorization, SQL/XML
policies, metadata filtering, HTTPS and actual reads on both Ray workers.
Old-baseline evidence combines unchanged passed cases with targeted validation
after relevant fixes. The 26.8 primary run also covers the three existing cluster
cases and all 85 protocol cases, once, in a 135-case report. This does not claim
all 23.x patches, arbitrary mixed-version clusters or Cloud are validated.

Review corrections add 12 distributed regressions for effective aggregate/sort
profiles, named Tuple display equivalence and strict differences, target catalog
filtering and native visibility. Each older baseline passed those cases plus
the existing real missing-target case. The final 26.8 combined run passed all
147 cases: 59 distributed, three legacy cluster and 85 protocol cases, once.
