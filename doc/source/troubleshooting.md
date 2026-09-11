# Troubleshooting

## Unsupported Ray version

Install a final Ray release in `>=2.55,<2.59`. Prerelease, development, and post-release builds are intentionally rejected.

## Authentication and permission errors

Confirm that `password` and `password_env` are not both set and that the configured environment variable exists in every Ray worker process. Authentication and permission failures are never converted into empty Datasets.

## Empty write receipt

An empty Dataset returns a confirmed zero-valued receipt. If Ray returns without invoking the normal sink completion callback, `write_clickhouse()` raises `WriteError`; it does not synthesize success.

## Ambiguous writes

Do not automatically retry `AmbiguousWriteError`. Query ClickHouse using application-specific reconciliation keys and query identifiers before deciding whether another write is safe.

## Split planning

Partition and integer-range splits require a validated direct MergeTree-family table. Views, MaterializedView, and Distributed tables cannot use those split modes. Use `split="single"` for supported View and Distributed reads.

## Arrow stream failures and query diagnostics

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

The connector never replays a failed read. If `system.query_log` is unavailable, the original client error and query id remain the authoritative diagnostic result.

## Integration artifacts

The repository test scripts store pytest, Compose, image identity, and cleanup evidence under `.artifacts/`. They operate only on the exact project resources created by the current suite and never require global Docker prune commands.
