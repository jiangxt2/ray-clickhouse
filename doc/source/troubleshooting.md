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

## Execution logging

The connector emits standard Python logging records through its module loggers
under `ray_clickhouse`. The application owns levels, handlers, formatters and
worker initialization. The library does not install handlers, change the root
logger, write files, or start upload threads. Configure both driver and worker
output; changing a driver logger does not configure remote workers.

An independent Ray application can use an application-owned setup function:

```python
import logging
import os

import ray


def configure_connector_logging():
    logger = logging.getLogger("ray_clickhouse")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        logging.Formatter(
            "%(levelname)s %(event)s phase=%(phase)s status=%(status)s "
            "operation=%(operation_id)s execution=%(execution_id)s "
            "query=%(query_id)s insert=%(client_insert_id)s %(message)s",
            defaults={
                "event": "-",
                "phase": "-",
                "status": "-",
                "operation_id": "-",
                "execution_id": "-",
                "query_id": "-",
                "client_insert_id": "-",
            },
        )
    )
    logger.addHandler(handler)
    logger.propagate = False


configure_connector_logging()  # Once in this application's driver.
ray.init(runtime_env={"worker_process_setup_hook": configure_connector_logging})
os.environ["RAY_CLICKHOUSE_LOG_BATCHES"] = "1"  # Optional; set before construction.
# Construct read_clickhouse(...) or call write_clickhouse(...) here.
```

Handler levels and `propagate` remain application choices. The formatter's
defaults handle optional fields and existing records that have no event fields.
A JSON formatter can serialize the same event attributes; the connector does
not serialize records or replace the application's formatter.

For a Tributo application, integrate the same setup function into its
application bootstrap and worker initialization, alongside existing handlers.
Select the connector's module levels there; do not configure output within a
Provider or Binding. Leave network delivery, persistence and any upload switch
to the application. Enabling application log delivery does not enable connector
DEBUG or batch details automatically. The connector has no Tributo dependency.

`RAY_CLICKHOUSE_LOG_BATCHES` accepts exactly `0` or `1` and defaults to `0`.
Invalid values raise `ConfigurationError` before network I/O without echoing the
value. Each reader/sink freezes the setting at construction and serializes only
safe IDs and the Boolean option into its tasks. Worker environment changes do
not override the snapshot. Internal transport calls without a snapshot default
to batch details disabled. Logger levels remain controlled by the application.

INFO reports selected paths, planning summaries and driver write completion.
DEBUG reports discovery and preparation starts/completions, stream starts,
first data blocks, terminal counts, and the first INSERT's start/confirmation.
Failed events use ERROR and cleanup warnings use WARNING. Expected local-read
fallbacks finish at DEBUG with `status=fallback` and a fixed safe `reason_code`;
they are not execution failures. Progress requires both the batch option and
DEBUG: emit the first progress point, then only every
100th point if at least one second has passed, with at most 100 records per
stream or write task. A grouped shard task has independent budgets for each
actual stream. First-request stages and failures are outside this sampling.
There are no periodic heartbeats; a blocked first request retains its started
event even when batch logging is disabled.

| Fields | Meaning |
| --- | --- |
| `event`, `phase`, `status`, `role` | Fixed event/phase categories and observed outcome; driver and worker scopes differ |
| `operation_id`, `plan_id`, `split_id`, `execution_id` | Generated identities; repeated planning and actual stream executions receive new IDs |
| `query_id`, `client_insert_id` | Existing read request ID versus locally generated INSERT identity; neither implies exactly-once delivery |
| `duration_ms` | Local monotonic elapsed time; stream elapsed includes downstream pauses and INSERT elapsed includes async confirmation waiting |
| `emitted_rows`, `emitted_blocks`, `arrow_bytes` | Observed Arrow output; bytes describe memory size, not network traffic |
| `confirmed_rows`, `batches`, `suppressed_progress` | Known local confirmations and suppressed progress; a failed task's totals do not prove Dataset success |

Schema-only empty output has no first-data-block latency. Query-mode events do
not claim a table engine or physical split. A local-shard parent plan omits
`split_mode`; its child plans report the physical split mode. Write validation
failures include a safe mismatch category and known field counts, including
failures after earlier blocks were confirmed, without logging every preparation.
Cached metadata does not imply database snapshot isolation. Read terminal
outcomes distinguish EOF success,
consumer closure, cancellation and failure; a close failure cannot become
success. Driver callbacks are best effort and a killed process may leave no
terminal event. Missing events or unknown counters never prove success.

New events exclude SQL/filter text, bound values, full schemas/settings,
credentials, row data, raw exception text and diagnostic dictionaries. Identifiers
and enum fields are bounded and reject multiline values. Keep existing exception
diagnostics separate from structured event fields. Logging cannot add discovery,
diagnostic lookup, log flushing, INSERT, DDL, retries or Dataset actions.

## Integration artifacts

The repository test scripts store pytest, Compose, image identity, and cleanup evidence under `.artifacts/`. They operate only on the exact project resources created by the current suite and never require global Docker prune commands.
