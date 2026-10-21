# Architecture

## Responsibility split

Ray owns Dataset execution, task scheduling, and Arrow block handling. ClickHouse owns query execution, table engines, and storage. `ray-clickhouse` owns configuration validation, schema alignment, physical split planning, transport boundaries, write confirmation, and connector-specific error classification.

The driver discovers schema and engine capabilities and constructs immutable task specifications. Workers create their own ClickHouse clients, stream Arrow batches, or execute inserts. Live clients, sockets, streams, and cursors are never stored in datasource configuration.

## Read path

The default `split="single"` path executes one ClickHouse data query without COUNT, size, or sample planning queries. Optional partition and integer-range splits apply only to validated local MergeTree-family tables. Views are single-query-only; Distributed tables use a single query unless the filtered local-read option is enabled; MaterializedView is outside the public profile.

Reads yield bounded Arrow blocks controlled by `batch_rows` and `batch_bytes`. A single indivisible oversized row can exceed the byte target. Multiple independent read tasks do not share a ClickHouse snapshot.

### Native predicate pushdown

Verified Ray 2.58.0 and 2.59.0 table datasources implement the native predicate
acceptance contract. Ray nodes are decoded only in `_compat.py` into immutable
connector nodes. A pure compiler validates the source declaration, Arrow field,
literal type and range before producing bound SQL. There is no private Ray
runtime import or SQL parsing of expression repr strings.

The initial profile is exact Int8/16/32/64 and UInt8/16/32 comparisons to integer
constants, Bool predicates, String equality/inequality, NULL tests and complete
AND/OR/NOT. Verified Nullable and LowCardinality wrappers retain three-valued
logic. UInt64 comparisons to Python integers remain residual because Arrow 19
casts these comparisons to Int64 and can fail on high column values; NULL tests
do not introduce that cast. Float/NaN, Decimal, timezones, casted logical types,
IN/NOT IN, arithmetic, UDFs, column-to-column comparisons and unknown nodes remain
in Ray. Missing or unselected columns are never silently added to the read.

Each complete predicate offered by Ray is accepted or rejected. Rejection returns
the original datasource unchanged, preserving the Ray Filter. Acceptance creates
an independent datasource with accumulated predicates and clears all split,
Distributed-target and fallback caches. Only immutable schema facts are reused.
Ray's own residual splitting remains Ray's responsibility; the connector never
accepts just one child of a rejected Boolean expression.

Caller SQL and parameters are validated separately from compiler-owned bindings.
Generated names use a reserved expression namespace. With no caller placeholders,
the compiler uses native typed binding; otherwise it follows the caller's native
or client protocol without mixing protocols or rewriting caller SQL. Ambiguous
placeholder-looking SQL retains its existing behavior without expression pushdown.
The same effective predicate and bindings reach range discovery, single scans,
partition/range constraints and every resolved local shard. Compiler values are
not included in repr or connector diagnostics.

Before acceptance, effective settings must exclude FINAL, implicit table/result
filters, result row/byte caps, server `limit/offset`, query-rewriting
`page/select/filter/order/sort` settings where available, and partial-result
overflow modes. Explicit client defaults and request settings are checked first
(request settings take precedence), without changing the caller's configuration.
The effective server context is then checked with a single-row UInt8 aggregate,
so the probe does not materialize all settings under a small result cap. Missing
required controls or a suppressed aggregate row reject the optimization; optional
newer controls need not exist on older servers. Only after a safe aggregate, a
fixed `EXPLAIN AST SELECT 1` probe under the original settings and a fresh query id
must match the known neutral AST. This detects inherited limit/offset already
materialized into a query wrapper and cleared from the settings context. Extra
nodes, wrappers or unfamiliar valid AST formatting leave the predicate in Ray.
The probe never explains user SQL or a source table. Authentication, permission,
malformed metadata and transport errors remain visible. This adds context
metadata only when attempting a supported expression; it adds no source-table
COUNT, sample or size query to a single read.
Schema and effective context are not transactionally pinned; source schema and
query/security configuration must remain stable during the planned read.

The fixed AST probe uses the driver's public raw-query API without appending a
FORMAT clause: EXPLAIN AST can consume such a clause as part of its inner query.
Only the known text response enables optimization. A different default output
format conservatively retains the Ray predicate; result controls are unchanged.

Distributed conversion is independent of predicate acceptance. Accepted expressions
can make an otherwise unfiltered source eligible for opt-in #20 local scans.
Conversion still validates every required capability and security context. A
planning fallback retains the accepted predicate on one logical-source query.
There is no fallback after output begins, replica failover or new shard-routing
algorithm. Views remain single queries, global ordering keeps logical reading,
and query sources do not implement this optimization.

Residual filtering remains before a downstream limit; read tasks must not truncate
raw rows before that residual. The existing per-task budget remains shared across
grouped local scans. Separately, Ray 2.58/2.59 mark Limit as predicate-pass-through,
so `limit().filter(expr=...)` may be reordered by Ray even when the datasource
rejects pushdown. This inherited behavior is characterized, not repaired here;
strict authored-order semantics require an upstream change.

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
SQL filter or accepted native predicate can resolve to one fixed replica of every
shard. Ordinary Views remain
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
