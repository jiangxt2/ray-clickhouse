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

## Distributed local-read trust boundary

Local scans are an explicit opt-in for trusted static metadata and directly
reachable hosts. Cluster hostnames become direct driver and worker connection
targets. Use a uniform HTTP/HTTPS port and credentials with appropriate access
on every selected node. The connector neither reads cluster passwords nor
copies credentials from `system.clusters`. Host-bound proxy, session, impersonation
and custom transport options are ineligible. HTTPS retains CA and hostname
verification; it never replaces a hostname with its resolved address.
Keep ACLs, row policies and topology static for the planned read. Discovery is
cached, not a live authorization monitor or a cross-query metadata snapshot.

In addition to source/target SELECT privileges, conversion needs visible table
and column metadata and `SHOW ROW POLICIES`, plus SELECT on `system.tables`,
`system.columns`, `system.clusters`, `system.settings`, `system.row_policies`
and, for partition splits, `system.parts`. Native DESCRIBE enforces whole-table
column visibility; structured column definitions must agree with it. Native
`SHOW CREATE ROW POLICIES ON *.*` checks effective privileges, including active
roles, independently of SELECT filtering on `system.row_policies`. Only its row
count is used, never its SQL text. The count, structured inventory length and
unique UUID count must agree. Disagreement raises; confirmed absence of a
required capability selects the logical source. No CHECK GRANT or grant SQL
parser is needed. Actual permission denials still raise. Related table/database row
policies conservatively select the original logical query; policy inheritance
and expression equivalence are not inferred.
Policies on discovery system tables also make conversion ineligible, preventing
hidden shards or partitions from producing an incomplete local plan.

Only explicit resource, timeout, readonly and logging settings in the internal
allowlist can accompany conversion. Result limits, FINAL, extra result/table filters,
special replica consistency, or other unverified settings select the original
query. Relevant effective defaults, identity and timezone are compared across
nodes. Settings are never silently deleted or overwritten to enable conversion.
Read, timeout, aggregate and sort overflow modes must throw; silent partial-result
modes select the logical source. Effective settings are checked before sorted
column discovery on either node. A selected target's security context and native
column visibility are checked before its filtered engine catalog is trusted.

Local topology discovery is driver-side; worker reachability is a deployment
precondition. Worker failures preserve the fixed plan and raise without executing
the entire Distributed source. Diagnostic logs contain path/reason and counts,
not predicate values, passwords or raw SQL. Existing Ray retry behavior does not
provide split continuation, snapshot isolation or exactly-once reads.
