#!/usr/bin/env bash
set -Eeuo pipefail

project_name="ray-clickhouse-cluster-it"
compose_file="docker/ray/compose.yaml"
artifact_dir="${RAY_CLICKHOUSE_CLUSTER_IT_ARTIFACT_DIR:-.artifacts/it/ray-cluster}"

mkdir -p "$artifact_dir"
compose=(docker compose --project-name "$project_name" --file "$compose_file")

cleanup() {
    status=$?
    env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
        -u http_proxy -u https_proxy -u all_proxy \
        "${compose[@]}" cp ray-head:/workspace/ray-clickhouse/cluster-pytest.xml "$artifact_dir/pytest.xml" \
        >/dev/null 2>&1 || true
    env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
        -u http_proxy -u https_proxy -u all_proxy \
        "${compose[@]}" logs --no-color >"$artifact_dir/compose.log" 2>&1 || true
    env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
        -u http_proxy -u https_proxy -u all_proxy \
        "${compose[@]}" down --volumes --remove-orphans || true
    exit "$status"
}

trap cleanup EXIT

env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy \
    "${compose[@]}" build --pull=false
env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy \
    "${compose[@]}" up --detach --wait

set -o pipefail
runtime_env_json='{"env_vars":{"RAY_CLICKHOUSE_IT_HOST":"clickhouse","RAY_CLICKHOUSE_IT_PORT":"8123","RAY_CLICKHOUSE_IT_DATABASE":"ray_clickhouse_it","RAY_CLICKHOUSE_IT_RAY_ADDRESS":"10.250.0.10:6379","RAY_CLICKHOUSE_IT_MIN_NODES":"3","PYTHONPATH":"./src:./tests","RAY_USAGE_STATS_ENABLED":"0"}}'
env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy \
    "${compose[@]}" exec -T runner ray job submit \
    --address=http://10.250.0.10:8265 \
    --working-dir=/workspace/ray-clickhouse \
    --runtime-env-json="$runtime_env_json" \
    -- python -m pytest tests/integration -m integration -vv \
    --junitxml=/workspace/ray-clickhouse/cluster-pytest.xml \
    2>&1 | tee "$artifact_dir/pytest.log"
