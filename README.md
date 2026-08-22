# ray-clickhouse

Ray Data read and append connector for ClickHouse physical tables.

The first release provides:

- structured physical-table reads returning `ray.data.Dataset`;
- single-query reads from direct tables, Views, and Distributed tables;
- Arrow streaming with bounded rows and bytes per emitted block;
- optional partition and integer range planning for direct MergeTree-family tables;
- append writes plus explicit safe `create`/`overwrite` table-management modes;
- structured ordering and Ray task resource passthrough;
- fail-closed schema, type, permission, timeout, and ambiguous-write errors.

The first release does not provide arbitrary SQL execution, arbitrary DDL, delete, update, upsert,
connector-side Distributed shard routing, cross-task transactions, or exactly-once guarantees.
Distributed reads use one ClickHouse query; ClickHouse owns shard routing.

The read API follows the Ray Data `Datasource`/`ReadTask` contract. The write API uses
`Dataset.write_datasink()` and does not monkey-patch Ray Dataset methods.

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

`filter` is a trusted predicate fragment, not an arbitrary SQL query. Values must be passed through
`query_parameters`; the connector does not execute arbitrary SQL or provide delete, update,
upsert, connector-side Distributed shard routing, or exactly-once semantics. `write_mode="overwrite"`
is destructive and must be selected explicitly. For create/overwrite, nullable columns must be
listed explicitly through `nullable_columns`; timestamp precision is preserved and unsupported
binary types are rejected.

## Development

```bash
uv sync --extra dev
uv run pytest tests/unit tests/contract
./scripts/run_clickhouse_it.sh
./scripts/run_ray_cluster_it.sh
```
