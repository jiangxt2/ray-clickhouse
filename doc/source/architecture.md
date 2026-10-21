# Architecture

## Responsibility split

Ray owns Dataset execution, task scheduling, and Arrow block handling. ClickHouse owns query execution, table engines, and storage. `ray-clickhouse` owns configuration validation, schema alignment, physical split planning, transport boundaries, write confirmation, and connector-specific error classification.

The driver discovers schema and engine capabilities and constructs immutable task specifications. Workers create their own ClickHouse clients, stream Arrow batches, or execute inserts. Live clients, sockets, streams, and cursors are never stored in datasource configuration.

## V1 projection pushdown

Verified Ray versions can push Dataset column selections into table scans using
the public Datasource projection hooks. Each application returns a clone with an
ordered identity projection, bounded by the explicit source `columns` and earlier
selections. Renaming and expression evaluation remain Ray operations.

Clones reuse full discovery facts, including range keys, partitions, topology and
column declarations. Projected output fields are rebuilt from those declarations;
cached local shard sources are cloned independently. The data-query projection
and ReadTask schema agree on order, type and nullability. Original source facts
are never narrowed to the output schema.

An empty projection reads one permitted source column, validates and slices the
Arrow stream normally, then uses the public Ray BlockAccessor selection method to
retain native cardinality across materialization. Its physical row placeholder
is managed by Ray; the connector neither imports its private implementation nor
copies its private column name. No COUNT query or alternate empty-row semantics
are introduced. The generator closes its underlying stream on early termination
and failure, as well as normal completion.

Schema discovery still precedes Dataset optimization. This optimization does not
avoid unsupported source-schema errors, rewrite query-mode SQL, or provide a
database snapshot across requests.

## Read path

The default `split="single"` path executes one ClickHouse data query without COUNT, size, or sample planning queries. Optional partition and integer-range splits apply only to validated local MergeTree-family tables. Views are single-query-only; Distributed tables use a single query unless the filtered local-read option is enabled; MaterializedView is outside the public profile.

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

Native typed filter parameters use one server binding protocol across range
discovery and worker queries, including generated partition/range constraints.
Generated integer endpoints use Int128 to represent exclusive Int64/UInt64
maximum-plus-one boundaries. The caller's parameter types are preserved.

### Filtered Distributed local scans

`convert_distributed_to_local=False` preserves the existing path without new
cluster queries. When enabled, a Distributed table with an explicit eligible
filter can resolve to one fixed replica of every shard. Ordinary Views remain
logical queries. Global ordering, query sources, nested Distributed targets,
functions and unsupported contexts are outside this conversion profile.

The driver reads field names and declarations separately on the entry node and
each selected replica. It uses explicit projections and named result mapping:

| Information | Priority | Missing capability |
| --- | --- | --- |
| Engine target | `system.tables.engine_full`, then `create_table_query`, then SHOW CREATE ENGINE fragment | Whole-source logical fallback |
| Cluster identity | `system.clusters.cluster`, then its `name` alias | Whole-source logical fallback |
| Topology | Explicit shard, replica, hostname and user fields | Whole-source logical fallback |
| Columns | `system.columns`, then named structured DESCRIBE | Whole-source logical fallback |
| Arrow schema | Independent zero-row probe using the existing type/projection validator | Schema error on disagreement |
| Partition constraints | Active `system.parts.partition_id` | Local single scan or existing strict split error |
| Grouping weight | `bytes_on_disk`, then `rows`, then equal weights | No loss of shard coverage |
| Automatic range key | `system.tables.sorting_key` | Local single scan or existing strict split error |
| Settings and policies | Structured effective settings and visible row-policy metadata | Whole-source fallback on confirmed capability absence; errors remain errors |

System-table field probes prefer `system.columns`; confirmed unsupported
metadata can use structured DESCRIBE. Extra fields are ignored. Known fields
with invalid types or values are errors. Permission or connection failures do
not select another origin. Empty metadata is accepted only where visibility is
proven and emptiness is meaningful. View dependency arrays never map Distributed
targets. Version strings do not participate in planning.
Native DESCRIBE verifies the complete field-name set and whole-table visibility.
Only consumed field declarations participate in type agreement; formatting of
unused complex fields can differ between native metadata sources.
Business column declarations compare tokens, ignoring whitespace outside quoted
literals and identifiers. Tuple field names, widths, precision, timezones and
enum values remain significant; declarations and queries are not rewritten.

Column declarations and Arrow fields match by name; output follows the source
projection order. Source aliases are conservatively ineligible. Scalar predicate
text and bindings are preserved. No sharding expression is computed in Python.

Resolved topology, capabilities, selected origins and split facts are cached;
Ray parallelism determines fresh task groups on each call. Local datasource
planners and the shared SQL/Arrow stream construct the scans. One task may run
multiple scans under the global budget, sharing its row limit and stopping
before subsequent scans when closed. Selected replicas and endpoints remain
fixed for retries. There is no worker-time logical fallback or replica failover.

No mandatory Distributed-versus-local count scan is added. Range planning keeps
its existing aggregates. Independent planning and reads share no snapshot, and
equal counts do not establish equal row multisets. Tests wait for replication
and compare complete stable results, including duplicate multiplicity.

### Query result source

A query source uses the same public Ray Datasource/ReadTask contract and bounded
Arrow transport. The driver aligns DESCRIBE result declarations with a zero-row
Arrow probe, caches only schema facts, and builds one read task. Workers execute
a generated projection over the trusted query to preserve supported Date, UUID,
Enum and nested logical values. Physical engine and split discovery are bypassed.
Query mode compares the worker Arrow schema with the planned schema before any
cast. Changes to Arrow field types, decimal precision/scale, timestamp units or
timezones, and nullability fail closed. Declaration changes that produce the
same Arrow schema, such as DateTime64(4) to DateTime64(5), are not distinguished
by this check. Metadata and execution are separate database requests, with no
shared snapshot or promise of zero server work during planning. Ray block controls
do not shard the query.

## Write path

Writes use `Dataset.write_datasink()` and do not monkey-patch Ray Dataset. Append to an existing supported MergeTree-family table is the default. Generated `create` and `overwrite` modes are explicit and restricted.

Ray task retries are forced to zero through the public facade. Transport failure after an INSERT may have an unknown outcome and raises `AmbiguousWriteError`; the connector does not replay it transparently. An empty Dataset is a confirmed no-op and does not perform discovery, DDL, or INSERT.

## Non-goals

The connector does not provide unrestricted statement execution, arbitrary DDL, delete, update, upsert, cross-task transactions, snapshot isolation, exactly-once writes, or write-side Distributed shard routing.
