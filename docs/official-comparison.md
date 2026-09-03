# Official ClickHouse comparison methodology

This page documents the methodology for comparing Ray 2.58.0's public ClickHouse APIs with the
immutable batch 2 `ray-clickhouse` runtime. Results remain pending until a separately approved
GitHub-hosted formal run and reviewed evidence import.

## Identity

Every result binds three independent identities:

- the Unit A harness commit, scenario digest, schema version, and controller lock;
- the official Ray wheel filename and SHA-256; and
- the external `ray-clickhouse` wheel built from commit
  `1b1995155dd8bd68c6e537daa8b9348f80ce7c83`, including its SHA-256 and metadata.

Editable installs, private Ray ClickHouse implementations, and worktree `PYTHONPATH` injection are
not valid comparison paths.

## Correctness before resources

Each pair uses an immutable deterministic fixture and validates row count, canonical Arrow schema,
and an order-independent multiset checksum. OFFSET/FETCH scenarios use a unique total-order key.
Failed or invalid runs never contribute to resource summaries.

Streaming consumption and full materialization are separate scenarios. Each measured Dataset has
one terminal action, so a schema preview or validation count cannot silently execute the query a
second time.

Fixture creation and write-table DDL run in an explicit preparation phase. Successful read scenarios
then run the configured public-API warmup outside the measurement window, release that driver, and
capture a fresh baseline before the measured driver starts. The runtime image is built once per
invocation. Behavior, contract, error, and transport-fault cases reuse one comparison-owned
Compose/Ray cluster per runtime side and isolated case evidence directories; each side's cluster is
torn down once at suite end. Resource scenarios use one clean cluster per side and repetition, so
the paired observations remain in one job while their baseline, Object Store, and spill observations
remain isolated. Worker-loss cases use separate clusters per side because the injected worker
termination is terminal and Ray workers are bound to one runtime environment.
The driver and readiness probe execute inside `ray-head`, where `ray.init(address="auto")` can use
the local Ray node discovery path; this follows the ray-doris cluster test layout. Driver RSS is
sampled from the measured driver process itself, not from the idle runner peer.
Formal contract, error, and transport-fault cases retain their query/task/result evidence without
starting the resource sampler; resource evidence files are required only for resource scenarios.

## Controls and interpretation

Default profiles preserve each public API's defaults. Controlled profiles align only common public
controls such as CPU requests, concurrency, ClickHouse `max_threads`, and timeouts. Ray's official
API has no connector-level `batch_rows` or `batch_bytes`; external block limits are recorded rather
than described as matched.

Relative timing, worker RSS, Object Store, and spill observations are valid only for pairs executed
inside the same recorded GitHub-hosted job. They are reference-profile observations, not production
sizing or universal superiority claims.

The default profile adds only the unique `log_comment` required for attribution. The controlled
profile aligns two workers, one CPU and the same Ray task-memory request per worker, concurrency,
block override, ClickHouse `max_threads`, and timeouts. Official OFFSET/FETCH planning is compared
with external partition or integer-range planning as a declared semantic mapping, not as identical
SQL. External `batch_rows` and `batch_bytes` remain connector-specific recorded controls. Controlled
profiles use the same explicit connection timeout and record the external connector's managed
zero-query-retry policy rather than attempting to override that reserved option.

Worker memory is reported as process-tree RSS minus shared pages, with the baseline and per-worker
peaks retained separately. Container memory is a separate observation. Object Store evidence uses
Ray's documented `ray_object_store_memory` `MMAP_SHM`, `MMAP_DISK`, `SPILLED`, and `WORKER_HEAP`
locations. The three raylet locations must be present for every Ray service in every measured sample;
missing data is rejected in resource-required formal runs and is recorded as incomplete in bounded
profiles. `WORKER_HEAP` is emitted for active core workers and is therefore a dynamic series: an
absent series is recorded per service and treated as zero for the aggregate, rather than being
mistaken for a missing raylet exporter. Idle-baseline omissions of the required locations are
retained per service and make resource telemetry incomplete. The bounded smoke and dry-run profiles
record sparse exporter series as diagnostics without publishing resource conclusions.

## Query attribution and faults

Every public API call receives a unique ClickHouse `log_comment` containing the run, side, scenario,
repetition, and query role. Matching `QueryFinish`, `ExceptionBeforeStart`, and
`ExceptionWhileProcessing` records are retained. Every query must map to a scenario-declared
planning, estimate, sample, or data role; an undeclared role invalidates the result.

Post-commit/pre-ack response loss and post-commit Ray worker loss are independent one-shot
scenarios. The first compares ambiguous-result classification. The second compares Ray task replay.
An injector that misses its boundary or fires more than once invalidates the run.

The harness records only `Write` task IDs, attempt numbers, states, nodes, and workers for write
replay scenarios through Ray's public State API after the measured terminal action. It also records
insert `QueryFinish` IDs and reconciliation row counts. A controlled worker-loss scenario sets
official Ray task retries to zero; the default worker-loss scenario preserves each public API's
retry policy and checks the expected attempt and insert counts.

The permission scenario creates the fixed `ray_clickhouse_comparison_no_select` user with
`IDENTIFIED WITH no_password`, grants no `SELECT` privilege, and drops it before the case teardown.
The runner refuses to manufacture a permission result when that isolated fixture cannot be created.

## Evidence handling

Runner-private raw evidence is sanitized before upload through a closed filename and JSON-field
allowlist. Complete sanitized logs are retained as GitHub Actions artifacts. The workflow publishes
machine-readable provenance binding workflow SHA, candidate SHA, harness wheel SHA, and artifact ID/
digest. The comparison artifact's `SHA256SUMS` covers only its sanitized evidence tree; provenance is
a separate artifact containing its own `SHA256SUMS` and a reference to the comparison artifact
identity. The repository receives only compact reviewed JSONL/CSV, an artifact index, redaction report,
checksums, and summaries sufficient to reproduce every published table.

This project is independent and community-maintained. The comparison does not imply that the Ray
project maintains, endorses, or has accepted this connector.
