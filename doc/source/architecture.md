# Architecture

## Responsibility split

Ray owns Dataset execution, task scheduling, and Arrow block handling. ClickHouse owns query execution, table engines, and storage. `ray-clickhouse` owns configuration validation, schema alignment, physical split planning, transport boundaries, write confirmation, and connector-specific error classification.

The driver discovers schema and engine capabilities and constructs immutable task specifications. Workers create their own ClickHouse clients, stream Arrow batches, or execute inserts. Live clients, sockets, streams, and cursors are never stored in datasource configuration.

## Read path

The default `split="single"` path executes one ClickHouse data query without COUNT, size, or sample planning queries. Optional partition and integer-range splits apply only to validated local MergeTree-family tables. Views and Distributed tables are single-query-only; MaterializedView is outside the public profile.

Reads yield bounded Arrow blocks controlled by `batch_rows` and `batch_bytes`. A single indivisible oversized row can exceed the byte target. Multiple independent read tasks do not share a ClickHouse snapshot.

## Write path

Writes use `Dataset.write_datasink()` and do not monkey-patch Ray Dataset. Append to an existing supported MergeTree-family table is the default. Generated `create` and `overwrite` modes are explicit and restricted.

Ray task retries are forced to zero through the public facade. Transport failure after an INSERT may have an unknown outcome and raises `AmbiguousWriteError`; the connector does not replay it transparently. An empty Dataset is a confirmed no-op and does not perform discovery, DDL, or INSERT.

## Non-goals

The connector does not provide arbitrary SQL, arbitrary DDL, delete, update, upsert, cross-task transactions, snapshot isolation, exactly-once writes, or connector-side Distributed shard routing.
