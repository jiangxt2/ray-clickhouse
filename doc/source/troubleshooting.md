# Troubleshooting

## Unsupported Ray version

Install a final Ray release in `>=2.55,<2.60`. Prerelease, development, and post-release builds are intentionally rejected.

## Authentication and permission errors

Confirm that `password` and `password_env` are not both set and that the configured environment variable exists in every Ray worker process. Authentication and permission failures are never converted into empty Datasets.

## Empty write receipt

An empty Dataset returns a confirmed zero-valued receipt. If Ray returns without invoking the normal sink completion callback, `write_clickhouse()` raises `WriteError`; it does not synthesize success.

## Ambiguous writes

Do not automatically retry `AmbiguousWriteError`. Query ClickHouse using application-specific reconciliation keys and query identifiers before deciding whether another write is safe.

## Split planning

Partition and integer-range splits require a validated direct MergeTree-family table. Views, MaterializedView, and Distributed tables cannot use those split modes. Use `split="single"` for supported View and Distributed reads.

If `split="auto"` produces one task, the table may have an ineligible sorting
key, a constant or empty range, or be a supported single-query engine. Automatic
selection uses only the first column of a simple sorting-key list; it does not
interpret expressions or choose a later integer column. Set
`discovery_policy="error"` to surface eligibility or discovery errors, or use
`split="range", range_column="..."` to explicitly select a supported integer
column. Even a valid range plan may have one task when the data or task budget
does not allow further splitting.

## Query result reads

Specify exactly one source: table or query. Query mode uses a single task and
does not accept table filter, projection, ordering or physical split options.
Use explicit simple aliases for computed or duplicate result columns. Unsupported
result declarations and Arrow schema drift fail closed. Parameters currently use
the client-side named form; quoted/commented placeholder-looking text with
active bindings is rejected. Metadata discovery adds two requests and is not a
snapshot or a guarantee of zero scan work.

## Debug logging

Configure logging when the application initializes Ray:

```python
import ray

ray.init(
    logging_config=ray.LoggingConfig(
        encoding="TEXT", log_level="DEBUG", additional_log_standard_attrs=["name"]
    )
)
```

Ray configures the root logger in the driver and workers and adds its job, task,
worker and node context. Use `encoding="JSON"` for structured output. This sets
the job's root log level; applications can also use standard Python logging
configuration for more selective filtering. The connector uses module loggers
under `ray_clickhouse` and does not install handlers or initialize Ray.

Debug messages show discovery and cache reuse, actual split plans, stream setup,
first-block latency, write preparation, and DDL stages. Read messages reuse the
existing `query_id`. `client_insert_id` in insert failure messages is a local
identifier and is not a guaranteed server query-log lookup key.

Read `blocks` and `rows` count yielded blocks and rows; write `confirmed_*` counts
only batches acknowledged by ClickHouse. Byte counts are Arrow memory sizes,
not network traffic. `elapsed_ms` is client wall-clock duration measured with a
monotonic clock. A `stopped` message reports local progress, including partial
consumption or failure; it does not assert successful cleanup or a completed
Dataset. The driver logs a write completion summary only in the normal Ray
completion callback, including zero counts for an empty write.

There are no per-batch logs or additional connector logging switches. New log
messages exclude SQL, parameter values, credentials, connection configuration,
row values, full schemas, and raw exception messages. Changing the log level
does not add queries or retries or enable `diagnostic_flush_logs`.

## Arrow stream failures and query diagnostics

Inspect `error.client_diagnostic` for the underlying exception type, operation,
and bounded message. This diagnostic also appears in ordinary Ray tracebacks
and survives worker error serialization. Raw exception chains remain suppressed
to protect credentials; SQL-bearing client messages are omitted. Query-log
details are optional server-provided context.

An Arrow read failure can be reported as `ArrowInvalid`, `IncompleteRead`, or a transport error when ClickHouse fails after an Arrow response has started. Read failures carry the generated query id when available and perform a bounded, best-effort lookup in `system.query_log`. Set `diagnostic_flush_logs=True` on `read_clickhouse()` when the account is allowed to run `SYSTEM FLUSH LOGS` and immediate query-log visibility is required. The diagnostic lookup never replaces the original read exception and may be unavailable when query logging is delayed or permission is missing.

The connector first discovers the available system query-log table with
`SHOW TABLES FROM system LIKE '%query_log%'`. It uses the generated query id as the
primary correlation key and includes the same id in a unique `log_comment` as a
fallback for server rows that do not retain the query id on an exception. Use the
discovered table name and either correlation value to inspect the original error:

```sql
SELECT type, exception_code, exception, read_rows, result_rows
FROM system.`<discovered_query_log_table>`
WHERE query_id = '<query-id>'
   OR log_comment = '<ray-clickhouse operation and query id>'
ORDER BY event_time_microseconds DESC;
```

The connector never replays a failed read. If `system.query_log` is unavailable, the client diagnostic and query id remain available.

## Integration artifacts

The repository test scripts store pytest, Compose, image identity, and cleanup evidence under `.artifacts/`. They operate only on the exact project resources created by the current suite and never require global Docker prune commands.
