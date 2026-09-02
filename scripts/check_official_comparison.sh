#!/usr/bin/env bash
set -Eeuo pipefail

repo_root=$(git rev-parse --show-toplevel)
if [[ "$PWD" != "$repo_root" ]]; then
    echo "run from repository root: $repo_root" >&2
    exit 1
fi

project="comparison/official"

uv sync --project "$project" --extra dev --frozen
uv lock --project "$project" --check
uv run --project "$project" ruff format --check "$project"
uv run --project "$project" ruff check "$project"
(cd "$project" && uv run mypy --config-file pyproject.toml)
uv run --project "$project" pytest "$project/tests"
uv run --project "$project" ray-clickhouse-comparison validate \
    --reference "$project/config/reference.toml" \
    --scenarios "$project/config/scenarios.toml" \
    --manifest-schema "$project/schema/manifest.schema.json" \
    --result-schema "$project/schema/result.schema.json"
