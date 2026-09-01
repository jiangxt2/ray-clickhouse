# Security model

## Credentials

`password_env` is preferred and is resolved independently in each process that opens a client. Literal passwords are serialized with datasource configuration and are suitable only for trusted Ray control planes and object stores. Redacted repr output does not prevent serialization.

Resolved credentials must not appear in logs, exception messages, repr output, release artifacts, benchmark evidence, or public configuration snapshots.

## SQL boundary

Database, table, column, range, and ordering identifiers must be simple validated identifiers and are quoted by connector helpers. `filter` accepts a trusted SQL scalar predicate, not an arbitrary query. Values belong in `query_parameters`.

## Side effects

Writes disable Ray task retries and exception retries. A response timeout or disconnect can leave an INSERT outcome ambiguous. Generated overwrite validates inputs before destructive operations and reports table-management ambiguity when replacement status is unknown.

The repository-root `SECURITY.md` contains the private vulnerability reporting instructions.
