# Ray ClickHouse official comparison harness

This repository-owned harness compares Ray 2.58.0's public ClickHouse APIs with the
immutable `ray-clickhouse` batch 2 runtime. It is comparison infrastructure, not part of the
published `ray-clickhouse` package.

The harness enforces three separate identities: the controller commit and lock, the official Ray
wheel, and the external connector wheel built from the approved runtime commit. It never imports
Ray's private ClickHouse datasource or datasink implementations.

Every declared scenario carries its correctness gate, allowed query roles, invalid-run rules,
fixture size, warmup policy, repetition count, split mode, retry mode, and resource-evidence
requirement. The runner prepares immutable fixtures before warmup, records a clean baseline, and
then executes exactly one measured terminal action.

The measured driver runs inside the comparison-owned `ray-head` container, matching Ray's public
cluster-attachment behavior and the ray-doris integration layout. Driver RSS is sampled from that
process; the separate runner container is kept only as an idle lifecycle peer and is not labeled as
the driver.

Run short validation from the repository root:

```bash
./scripts/check_official_comparison.sh
```

The Docker-backed smoke and every remote run are opt-in long-running operations. They require the
test matrix and approval recorded in `tests/it-ledger.md` before execution.

Evidence is split into runner-private raw data, sanitized Actions artifacts, and compact reviewed
repository evidence. The sanitizer publishes only the declared evidence filenames and JSON fields;
unknown files fail the run. Unsanitized logs are never uploaded or committed. Workflow provenance
records the workflow SHA, candidate SHA, harness wheel SHA, and uploaded evidence artifact identity.
The comparison evidence and provenance are separate immutable artifacts; each artifact carries its
own checksum, and provenance references the comparison artifact ID and digest.
Resource-required runs reject missing Object Store locations per service and sample; the minimal smoke
records sparse exporter series as incomplete diagnostics without publishing resource conclusions.
