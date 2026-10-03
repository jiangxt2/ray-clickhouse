# Security model

## Credentials

`password_env` is preferred and is resolved independently in each process that opens a client. Literal passwords are serialized with datasource configuration and are suitable only for trusted Ray control planes and object stores. Redacted repr output does not prevent serialization.

Resolved credentials must not appear in logs, exception messages, repr output, release artifacts, benchmark evidence, or public configuration snapshots.

Read client diagnostics omit raw exception chains and SQL-bearing messages.
They redact worker-local connection strings and parameter string values before
attaching bounded text to the public error. Query-log details are separate
server-provided context; avoid putting credentials in predicates or parameters.

## SQL boundary

Database, table, column, range, and ordering identifiers must be simple validated identifiers and are quoted by connector helpers. `filter` accepts a trusted SQL scalar predicate, not an arbitrary query. Values belong in `query_parameters`.

Query mode accepts trusted SELECT/WITH text with named value bindings and simple
result aliases. It uses a limited lexical grammar gate and enforces readonly=1;
it is not an untrusted SQL sandbox. Database privileges remain authoritative.
The API rejects inline settings, output formats/files, multiple statements and
DDL/DML. Query text and values are omitted from configuration repr output.

## Side effects

Writes disable Ray task retries and exception retries. A response timeout or disconnect can leave an INSERT outcome ambiguous. Generated overwrite validates inputs before destructive operations and reports table-management ambiguity when replacement status is unknown.

The repository-root `SECURITY.md` contains the private vulnerability reporting instructions.
