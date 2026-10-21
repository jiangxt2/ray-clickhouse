# ray-clickhouse

Ray Data read and append connector for ClickHouse physical tables.

> [!IMPORTANT]
> `ray-clickhouse` is community-maintained and is not an official Ray or ClickHouse project.
> This source builds package version `1.1`. Formal releases follow the two-component
> `major.minor` route, and each published release is tied to its own immutable candidate.

This release provides:

- structured physical-table reads returning `ray.data.Dataset`;
- single-query reads from direct tables, Views, and Distributed tables;
- Arrow streaming with bounded rows and bytes per emitted block;
- optional partition and integer range planning for direct MergeTree-family tables;
- append writes plus explicit safe `create`/`overwrite` table-management modes;
- structured ordering and Ray task resource passthrough;
- fail-closed schema, type, permission, timeout, and ambiguous-write errors.

The connector does not provide arbitrary SQL execution, arbitrary DDL, delete, update, upsert,
write-side Distributed shard routing, cross-task transactions, or exactly-once guarantees.
Distributed reads use one ClickHouse query by default.

Automatic range planning is opt-in. For a direct MergeTree-family table with a
simple sorting key starting with an integer column, use `split="auto"` to select
that column without supplying `range_column`:

```python
from ray_clickhouse import read_clickhouse

dataset = read_clickhouse(
    host="clickhouse.example",
    database="analytics",
    table="events",
    split="auto",
    target_tasks=4,
)
```

The default remains `split="single"`. Automatic planning reuses the existing
filtered range aggregate and disjoint range planner, not ordered pagination.
Unsuitable sorting keys fall back to one query; use `discovery_policy="error"`
to require an eligible range plan. Authentication, permission, and transport
failures still raise. See [Architecture](doc/source/architecture.md) for supported
keys and consistency limits.

The read API follows the Ray Data `Datasource`/`ReadTask` contract. The write API uses
`Dataset.write_datasink()` and does not monkey-patch Ray Dataset methods.
The package-root public API consists of `read_clickhouse()`, `write_clickhouse()`,
`WriteReceipt`, and the documented error hierarchy. Datasource and Datasink implementation
classes are internal and are not independent compatibility promises.

## Documentation

- [Documentation index](doc/source/index.md)
- [Compatibility](doc/source/compatibility.md)
- [Architecture](doc/source/architecture.md)
- [Security model](doc/source/security.md)
- [Troubleshooting](doc/source/troubleshooting.md)
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
- [Release notes](release-notes/v1.1.md)

## Native Dataset predicate filtering

On verified Ray 2.58.0 and 2.59.0 releases, table reads can push a restricted
`Dataset.filter(expr=...)` expression into ClickHouse automatically. Other
supported Ray releases retain Ray-side filtering. Query mode is unchanged.

```python
from ray.data.expressions import col

# This example assumes tenant_id is UInt32 and event_id is Int64.
dataset = read_clickhouse(
    host="clickhouse.example", database="analytics", table="events"
).filter(expr=(col("tenant_id") == 42) & col("event_id").is_not_null())
```

The initial profile supports exact integer comparisons (Int8–Int64 and
UInt8–UInt32), Bool predicates, String equality/inequality, NULL tests, and
complete AND/OR/NOT composition, including verified Nullable/LowCardinality
wrappers. UInt64 NULL tests are supported; UInt64 comparisons to Python integers
stay in Ray because Arrow 19 can fail when column values exceed Int64. Float,
Decimal, date/time, Enum, UUID, complex types, arithmetic, UDFs and IN/NOT IN remain
Ray filters. Comparisons with None are not rewritten as NULL tests.

An offered predicate is accepted completely or retained in Ray. User SQL filters
and their binding protocol remain compatible. Effective settings that truncate
results or imply unsupported rewrites disable this optimization; metadata access
errors remain errors. Accepted expressions can supply the filter needed by
`convert_distributed_to_local=True`; all existing conversion checks still apply.
No query, expression or parameter values are added to connector diagnostics.

Residual filters remain before downstream limits. Ray 2.58/2.59 themselves may
move an expression across an earlier `limit()`, so `limit().filter(expr=...)`
does not promise authored operation order. This connector does not change that
upstream behavior. See [architecture](doc/source/architecture.md#native-predicate-pushdown).

## Filtered Distributed local reads

Set `convert_distributed_to_local=True` to attempt direct local scans of a
single-layer Distributed table with an explicit `filter` or an accepted Dataset
predicate:

```python
dataset = read_clickhouse(
    host="clickhouse.example",
    database="analytics",
    table="events",
    filter="tenant_id = {tenant:UInt64}",
    query_parameters={"tenant": 42},
    convert_distributed_to_local=True,
    split="partition",
    target_tasks=4,
)
```

This opt-in profile requires a trusted static self-managed cluster, reachable
shard hostnames, a matching cluster query identity, and a uniform HTTP/HTTPS
port, credentials and TLS configuration.
The connector discovers explicit engine targets, validates every selected node,
and pins one replica per shard. It inherits the caller's HTTP port; the native
TCP port in `system.clusters` is never used as an HTTP port.

`split="single"` means one data query per shard when conversion succeeds.
Partition, range and auto plans reuse local planners. `target_tasks`, `max_tasks`
and Ray parallelism limit the total task count; a task may run several scans
sequentially. Task row limits apply across all scans in that task.

Eligible predicates include physical scalar comparisons, NULL tests, value
bindings and Boolean composition, plus these constant-operand forms and their
`NOT` variants:

- `id BETWEEN {lo:Int64} AND {hi:Int64}`: a physical scalar column and two
  constant bounds.
- `txt LIKE {pattern:String}`: a physical String/FixedString column (including
  supported Nullable/LowCardinality wrappers) and a fixed string pattern.
  `%`, `_` and backslash escaping retain their ClickHouse meanings.
- `id IN ({one:Int64}, {two:Int64})`: a physical scalar column and a nonempty
  parenthesized list of scalar constants or individually bound values.

String/number/Boolean/NULL literals and existing scalar value bindings are
recognized without evaluating or rewriting them. All conditions can combine
with `AND`, `OR`, `NOT` and parentheses. The original SQL and bindings reach
ClickHouse, which retains its native type conversion and NULL semantics.
For a filter without client-side placeholders, generated split constraints use
native bindings so literal percent signs and placeholder-looking strings remain
unchanged. Explicit client-side placeholders retain their existing protocol.
For client-side bindings, bind a LIKE pattern through `%(pattern)s` rather than
mixing an unescaped literal `%` with other client placeholders.

Subqueries, `GLOBAL IN`, external sets, tuple/array list forms, column-dependent
LIKE patterns, `ILIKE` and custom `ESCAPE` clauses are outside local conversion.
The public `filter` continues to reject SELECT subqueries before planning.
Missing filters, functions, virtual columns, computed
aliases, global ordering, row policies, unsafe settings/options, nested targets,
or missing required metadata capabilities use the original logical single
query. Query mode rejects the flag. Real authentication, permission, connection,
missing-target and schema errors raise. Once local tasks start, there is no full
Distributed retry or replica switch.

Metadata is selected by field capabilities, not parsed server versions. Required
permissions and supported settings are described in [Security](doc/source/security.md).
There is no shared snapshot or count-based consistency guarantee. Use stable
source data for exact comparisons. See [Architecture](doc/source/architecture.md)
for metadata priorities and fallback boundaries.

## Dataset column selections

On Ray 2.58.0 and 2.59.0, table reads automatically push eligible
`dataset.select_columns([...])` operations into the ClickHouse data-query SELECT
list through the V1 Datasource contract. Explicit `columns=` still defines the
initial visible columns. Repeated selections preserve their requested order and
do not change sibling Dataset branches. Other supported Ray versions retain
Ray-side selection; the dependency window is unchanged.

Filters, order keys and physical split metadata remain available even when those
columns are absent from the output. Empty selections preserve Ray's native row
counts using one permitted source column as a physical row carrier. Initial
schema discovery still validates the table before downstream selection; selecting
fewer columns later cannot rescue a discovery failure. Query-mode selections
remain in Ray and do not rewrite the supplied query.

## Query result reads

The read API also accepts a trusted query source. Query reads require
exactly one of `table` or `query`, use one data-read task, and infer the
result schema with DESCRIBE and a zero-row Arrow probe.

```python
dataset = read_clickhouse(
    host="clickhouse.example",
    database="analytics",
    query=(
        "SELECT tenant_id, count() AS event_count FROM events "
        "WHERE tenant_id = %(tenant)s GROUP BY tenant_id"
    ),
    query_parameters={"tenant": 42},
)
```

Query mode supports the documented SELECT/WITH subset, client-side named value
bindings, and unique simple result aliases. Table projection, filter, ordering,
range columns, and split modes are supplied in the SQL rather than connector
options. The connector enforces `readonly=1` and rejects multiple statements,
DDL/DML, output formats, output files, and inline settings. Metadata probes and
the data read are independent requests and do not share a database snapshot.
A zero-row probe can still incur server work. `override_num_blocks` changes
Ray output blocks and does not multiply the query.

## Usage

```python
from ray_clickhouse import read_clickhouse, write_clickhouse

dataset = read_clickhouse(
    host="clickhouse.example",
    database="analytics",
    table="events",
    columns=("event_id", "tenant_id", "payload"),
    filter="tenant_id = %(tenant)s",
    query_parameters={"tenant": 42},
)

write_clickhouse(
    dataset,
    host="clickhouse.example",
    database="analytics",
    table="events_copy",
    insert_mode="sync",
)

# Explicit table management is opt-in and restricted to safe generated DDL.
write_clickhouse(
    dataset,
    host="clickhouse.example",
    database="analytics",
    table="events_new",
    write_mode="create",
    order_by=("event_id",),
)
```

When diagnosing an Arrow or transport failure, set `diagnostic_flush_logs=True` on
`read_clickhouse()` if the ClickHouse account may run `SYSTEM FLUSH LOGS`:

```python
dataset = read_clickhouse(
    host="clickhouse.example",
    database="analytics",
    table="events",
    diagnostic_flush_logs=True,
)
```

Read failures carry a generated query ID and a bounded `client_diagnostic` with
the underlying exception type, operation, and safe message context. The
connector performs a bounded, best-effort lookup in the discovered ClickHouse
query-log table and appends the server exception code and message when available.
It never replays the failed SQL; if query logging is unavailable, the client
diagnostic and query ID remain available. Raw exception chains remain suppressed
to protect credentials. The diagnostic text is
server-provided and may include query context, so do not put credentials in query
predicates or parameters.

`filter` is a trusted predicate fragment, not an arbitrary SQL query.
Use `tenant_id = {tenant:Nullable(UInt32)}` with `query_parameters={"tenant": 42}`
for native server-side binding. Existing `%(tenant)s` client binding remains
supported. Mixing binding styles or using identifier parameters is rejected.
Typed parameter-looking text inside quotes/comments is rejected when bindings
are active. Supported hints are String, FixedString, Bool, Int8/16/32/64,
UInt8/16/32/64, Float32/64, Date/Date32, DateTime/DateTime64, Decimal/32/64/128,
and one Nullable wrapper. DateTime64 precision must be 0 through 9; timezone
hints use a quoted timezone name. Collection and identifier hints are unsupported. Values must be passed through
`query_parameters`; the connector rejects unrestricted statement execution and does not provide delete, update,
upsert, connector-side Distributed shard routing, or exactly-once semantics. `write_mode="overwrite"`
is destructive and must be selected explicitly. For create/overwrite, nullable columns must be
listed explicitly through `nullable_columns`; timestamp precision is preserved and unsupported
binary types are rejected.

Writing an empty Dataset is a confirmed no-op: it returns a zero-valued `WriteReceipt` without
performing ClickHouse discovery, table management, or INSERT. If Ray returns without invoking the
normal sink completion callback, `write_clickhouse()` raises `WriteError` instead of reporting a
zero-valued success. MaterializedView reads are not part of the supported capability profile.

## Compatibility

`ray-clickhouse` supports final Ray releases `>=2.55,<2.60` on Python 3.10–3.13.
Prerelease, development, and post-release Ray builds are rejected. PEP 440 local build
suffixes are allowed. Pandas is resolved through `ray[data]`; it is not an independent
`ray-clickhouse` compatibility promise.

| Python | Ray | Verification |
| --- | --- | --- |
| 3.12 | 2.55.0 | Unit and public-contract tests |
| 3.12 | 2.56.1 | Unit and public-contract tests |
| 3.12 | 2.57.0 | Unit and public-contract tests |
| 3.12 | 2.58.0 | Unit and public-contract tests for the previous minor |
| 3.12 | 2.59.0 | Unit, public-contract, and package tests |
| 3.10 | 2.59.0 | Unit, public-contract, and wheel tests |
| 3.11 | 2.59.0 | Unit, public-contract, and wheel tests |
| 3.13 | 2.59.0 | Unit, public-contract, and wheel tests |

## Development

```bash
uv sync --extra dev
uv sync --extra dev --group docs
uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run python tools/check_compatibility.py
uv run python tools/check_release.py check
uv run pytest tests/unit tests/contract
uv build
uv run twine check dist/*.whl dist/*.tar.gz
uv run python tools/check_docs.py
make -C doc html
make -C doc spelling
make -C doc linkcheck
./scripts/run_clickhouse_it.sh
./scripts/run_ray_cluster_it.sh
```

ClickHouse and multi-node Ray integration commands remain available in the repository,
but they are required only when the affected production or infrastructure behavior changes.
The pinned infrastructure baselines are ClickHouse 26.8.1.2041 and a Ray 2.59.0/Python 3.12
three-node runtime.
