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

## Integration artifacts

The repository test scripts store pytest, Compose, image identity, and cleanup evidence under `.artifacts/`. They operate only on the exact project resources created by the current suite and never require global Docker prune commands.
