#!/usr/bin/env bash
set -Eeuo pipefail

project_name="ray-clickhouse-it"
compose_file="docker/clickhouse/compose.yaml"
artifact_dir="${RAY_CLICKHOUSE_IT_ARTIFACT_DIR:-.artifacts/it}"
python_bin="${RAY_CLICKHOUSE_PYTHON:-.venv/bin/python}"

mkdir -p "$artifact_dir"

compose=(docker compose --project-name "$project_name" --file "$compose_file")

cleanup() {
    status=$?
    env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
        -u http_proxy -u https_proxy -u all_proxy \
        "${compose[@]}" logs --no-color >"$artifact_dir/clickhouse.log" 2>&1 || true
    env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
        -u http_proxy -u https_proxy -u all_proxy \
        "${compose[@]}" down --volumes --remove-orphans || true
    exit "$status"
}

trap cleanup EXIT

env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy \
    "${compose[@]}" up --detach --wait

set -o pipefail
"$python_bin" -m pytest tests/integration -m integration -vv \
    --junitxml="$artifact_dir/pytest.xml" 2>&1 | tee "$artifact_dir/pytest.log"
