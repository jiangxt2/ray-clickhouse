# Integration test ledger

| Suite | Scope | Code/config state | Reason | Result |
| --- | --- | --- | --- | --- |
| `tests/integration` with ClickHouse Docker | Real ClickHouse 24.8, Ray Data local runtime, filtered single read, logical Date/Decimal/DateTime values, empty table, View/aggregate View, Distributed policy, partition/range split, sync/async append, create/overwrite, schema rejection, permission failure | Enhanced implementation after DDL preflight, explicit nullability, precision-preserving type mapping, ambiguous table-management errors, and metadata tracing | Final compatibility regression after review fixes | Passed: 11 tests, 1 expected multi-node skip, 2026-08-22 |
| `tests/integration` in Ray Docker cluster | ClickHouse 25.3 fixed digest, Ray 2.55.1 head plus two workers, Ray Jobs driver, same read/write and semantic policy suite | Same enhanced implementation; runner uses the built worktree package | Combined multi-node Ray and ClickHouse 25.3 compatibility validation after review fixes | Passed: 12 tests, 2026-08-22 |
