#!/usr/bin/env bash
set -Eeuo pipefail

project_name="ray-clickhouse-it"
compose_file="docker/clickhouse/compose.yaml"
artifact_dir="${RAY_CLICKHOUSE_IT_ARTIFACT_DIR:-.artifacts/it}"
python_bin="${RAY_CLICKHOUSE_PYTHON:-.venv/bin/python}"

mkdir -p "$artifact_dir"

compose=(docker compose --project-name "$project_name" --file "$compose_file")

record_docker_state() {
    phase=$1
    env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
        -u http_proxy -u https_proxy -u all_proxy \
        docker image ls --filter dangling=true --no-trunc \
        >"$artifact_dir/docker-images-dangling-$phase.txt"
    env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
        -u http_proxy -u https_proxy -u all_proxy \
        docker system df >"$artifact_dir/docker-system-df-$phase.txt"
}

cleanup() {
    status=$?
    trap - EXIT
    set +e
    cleanup_status=0
    env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
        -u http_proxy -u https_proxy -u all_proxy \
        "${compose[@]}" logs --no-color >"$artifact_dir/clickhouse.log" 2>&1 || true
    env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
        -u http_proxy -u https_proxy -u all_proxy \
        "${compose[@]}" images >"$artifact_dir/compose-images.txt" 2>&1 || true
    env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
        -u http_proxy -u https_proxy -u all_proxy \
        "${compose[@]}" down --volumes --remove-orphans || cleanup_status=1
    record_docker_state after || cleanup_status=1
    if [[ "$status" -eq 0 && "$cleanup_status" -ne 0 ]]; then
        status=$cleanup_status
    fi
    exit "$status"
}

existing_resources=$(env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy \
    "${compose[@]}" ps --all --quiet)
if [[ -n "$existing_resources" ]]; then
    echo "Compose project already has resources: $project_name" >&2
    exit 1
fi

trap cleanup EXIT

record_docker_state before
env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy \
    "${compose[@]}" config --images >"$artifact_dir/compose-image-references.txt"
env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy \
    "${compose[@]}" up --detach --wait --no-build

set -o pipefail
"$python_bin" -m pytest tests/integration -m integration -vv \
    --junitxml="$artifact_dir/pytest.xml" 2>&1 | tee "$artifact_dir/pytest.log"
