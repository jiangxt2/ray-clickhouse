# Architecture

## Responsibility split

Ray owns Dataset execution, task scheduling, and Arrow block handling. ClickHouse owns query execution, table engines, and storage. `ray-clickhouse` owns configuration validation, schema alignment, physical split planning, transport boundaries, write confirmation, and connector-specific error classification.

The driver discovers schema and engine capabilities and constructs immutable task specifications. Workers create their own ClickHouse clients, stream Arrow batches, or execute inserts. Live clients, sockets, streams, and cursors are never stored in datasource configuration.

## Read path

The default `split="single"` path executes one ClickHouse data query without COUNT, size, or sample planning queries. Optional partition and integer-range splits apply only to validated local MergeTree-family tables. Views and Distributed tables are single-query-only; MaterializedView is outside the public profile.

Reads yield bounded Arrow blocks controlled by `batch_rows` and `batch_bytes`. A single indivisible oversized row can exceed the byte target. Multiple independent read tasks do not share a ClickHouse snapshot.

### Automatic range planning

`split="auto"` explicitly opts into range planning on a validated direct
MergeTree-family table. It reads `system.tables.sorting_key` and accepts only a
simple identifier list (including simple backtick quoting or a `tuple(...)` of
identifiers). The first column must be a physical Int8/16/32/64 or UInt8/16/32/64
column; Nullable and LowCardinality wrappers supported by explicit range reads
are also accepted. Expressions, aliases, empty keys, and non-integer first
columns are ineligible. The planner does not search later key columns for a
replacement and does not require the selected column in the output projection.

Eligible reads reuse the existing range discovery query (`count`, `min`, `max`,
and null count with the user's filter and parameters) and integer-range planner.
There is no OFFSET/FETCH pagination or additional sample query. Discovery is
cached in the datasource's planning snapshot; `parallelism`, `target_tasks`, and
`max_tasks` bound the task count. Empty results, all-null keys, and constant keys
produce one query. Duplicate key values are allowed; nullable keys are included
in only one range. Task limits are upper bounds, not promises of parallelism.

With the default `discovery_policy="single"`, an ineligible key or range
discovery error falls back to a single query. Supported Views and Distributed
tables also remain single-query reads without sorting-key discovery. With
`discovery_policy="error"`, these cases raise `DiscoveryError`. Unsupported
engines such as MaterializedView remain unsupported under either policy.
Authentication, permission, missing-table, and transport failures are never
silently downgraded. Explicit `range_column` and `order_by` cannot be combined
with `split="auto"`.

Automatic selection does not guarantee balanced tasks or faster reads. In
particular, a low-cardinality first sorting column can lead to few ranges or
skewed work. Range discovery and task queries do not share a snapshot; concurrent
inserts, deletes, or key updates can change coverage. Use stable source data when
exact coverage is required. No process memory or exactly-once guarantee is added.

## Write path

Writes use `Dataset.write_datasink()` and do not monkey-patch Ray Dataset. Append to an existing supported MergeTree-family table is the default. Generated `create` and `overwrite` modes are explicit and restricted.

Ray task retries are forced to zero through the public facade. Transport failure after an INSERT may have an unknown outcome and raises `AmbiguousWriteError`; the connector does not replay it transparently. An empty Dataset is a confirmed no-op and does not perform discovery, DDL, or INSERT.

## Non-goals

The connector does not provide arbitrary SQL, arbitrary DDL, delete, update, upsert, cross-task transactions, snapshot isolation, exactly-once writes, or connector-side Distributed shard routing.
