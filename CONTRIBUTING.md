# Contributing

`ray-clickhouse` is a community-maintained independent connector. Planned repository development is performed through maintainer-approved internal worktrees. Use the repository's Issue templates to report bugs, suggest features, or improve documentation, and use the pull request template when submitting a contribution. Open an issue before a large API or architecture change so that scope, ownership, compatibility, and test expectations can be agreed on first. Report suspected vulnerabilities privately as described in [SECURITY.md](SECURITY.md).

## Development setup

Use Python 3.12 for the primary development environment:

```bash
uv sync --extra dev --group docs
```

Run the short validation suite before requesting review:

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run python tools/check_compatibility.py
uv run python tools/check_docs.py
uv run python tools/check_release.py check
uv run pytest tests/unit tests/contract --cov=ray_clickhouse --cov-report=term-missing
uv build
uv run twine check dist/*.whl dist/*.tar.gz
make -C doc html
make -C doc spelling
```

Run `make -C doc linkcheck` as the separately diagnosable external-link validation.

## Version route

Formal releases use exactly two components: `major.minor`. Compatible releases increment minor, for example `1.0`, `1.1`, `1.2`, and `1.10`, without a preset minor limit. Increment major only for approved breaking changes. Do not use three-component patch versions such as `1.0.1`. Package metadata, the annotated `v<version>` tag, distribution filenames, and `release-notes/v<version>.md` must agree exactly.

## Release operations

The `master`-push CI run is the only release-candidate producer. It builds and attests one wheel/source-distribution pair, records their checksums and artifact identities, and persists the ClickHouse integration evidence from the same commit. `.github/workflows/release.yml` only promotes those recorded files; it never rebuilds them.

For a local package-integrity check after `uv build`, stage only the distributions in a fresh temporary directory. The build output may also contain `.gitignore`, which is not a release asset. Create the checksum manifest and verify the exact staged file set:

```bash
release_stage=$(mktemp -d)
cp dist/*.whl dist/*.tar.gz "${release_stage}/"
(
  cd "${release_stage}"
  sha256sum -- *.whl *.tar.gz > SHA256SUMS
)
uv run python tools/check_release.py verify-files \
  --directory "${release_stage}" --sha256sums "${release_stage}/SHA256SUMS"
```

Index preflight and recovery checks use the target-version JSON endpoint. Existing older releases do not block a new version; an existing target version must match the candidate files exactly.

Promotion uses separate approved workflow runs in this fixed order: `dry-run`, `testpypi`, `release-tag`, `pypi`, and `github-release`. Each successful run records a receipt whose artifact ID and digest are required by the next operation. Missing, expired, mismatched, or out-of-order candidate artifacts and receipts stop the release.

The first release requires separate pending Trusted Publishers with these exact identities:

| Index | Project | Repository | Workflow | Environment |
| --- | --- | --- | --- | --- |
| TestPyPI | `ray-clickhouse` | `jiangxt2/ray-clickhouse` | `release.yml` | `testpypi` |
| PyPI | `ray-clickhouse` | `jiangxt2/ray-clickhouse` | `release.yml` | `pypi` |

A pending publisher does not reserve the project name. Recheck name availability before the first publication, and stop if the name or publisher identity no longer matches. Do not replace OIDC Trusted Publishing with a repository token. The `release-tag` and `github-release` operations use their own protected environments, and every mutation requires separate approval.

After downloading a candidate, verify each distribution's GitHub provenance with the recorded candidate SHA:

```bash
gh attestation verify dist/<distribution-file> \
  --repo jiangxt2/ray-clickhouse \
  --signer-workflow jiangxt2/ray-clickhouse/.github/workflows/ci.yml \
  --source-digest <candidate-sha> \
  --source-ref refs/heads/master \
  --deny-self-hosted-runners
```

The GitHub build-provenance attestation verified above is distinct from the publish attestations produced for PyPI and TestPyPI. Neither type replaces the checksum, candidate, tag, or live-index checks. An attestation binds a file to its producing identity; it does not establish that the connector is correct or safe for a particular workload.

## Scope and design

- Use only supported Ray public extension contracts; do not import `ray.data._internal`.
- Keep Ray-version adaptation in `_compat.py`.
- Preserve fail-closed schema, credential, retry, and ambiguous-result behavior.
- Keep query reads within the documented trusted SELECT/WITH profile; do not add arbitrary DDL, upsert, transaction, snapshot-isolation, or exactly-once claims.
- Do not add type-checking suppression comments.
- Keep each change limited to its approved capability and include direct success, failure, and cleanup tests.

## Integration tests

Changes to SQL, schema, discovery, transport, credentials, writes, physical splits, or Arrow batching require the real ClickHouse suite:

```bash
./scripts/run_clickhouse_it.sh
```

Changes to Ray task options, retries, resource scheduling, worker distribution, or multi-node semantics also require:

```bash
./scripts/run_ray_cluster_it.sh
```

Long-running suites require an approved execution matrix and must not be repeated on unchanged code, configuration, and environment.

## Commit contract

Commit messages use the Ray-style component prefix and DCO trailer:

```text
[Data] Short description

Signed-off-by: Your Name <your-email@example.com>
```

Internal batches are reduced to one signed squash commit before direct fast-forward integration to `master`. Commit, push, publication, and cleanup remain separately approved operations.
