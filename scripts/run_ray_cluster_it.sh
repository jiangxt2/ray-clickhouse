#!/usr/bin/env bash
set -Eeuo pipefail

project_name="ray-clickhouse-cluster-it"
compose_file="docker/ray/compose.yaml"
artifact_dir="${RAY_CLICKHOUSE_CLUSTER_IT_ARTIFACT_DIR:-.artifacts/it/ray-cluster}"
ray_base_source="${RAY_CLICKHOUSE_RAY_BASE_SOURCE:-docker.m.daocloud.io/rayproject/ray@sha256:c3c9573c5c6bfe4127885f79622d6a32064d34cafc7d156ec728aab8657be250}"
ray_base_local="${RAY_CLICKHOUSE_RAY_BASE_LOCAL_IMAGE:-ray-clickhouse-ray-base:2.58.0-py312-c3c9573c5c6b}"
runtime_image="${RAY_CLICKHOUSE_CLUSTER_RUNTIME_IMAGE:-ray-clickhouse-cluster-it-runtime:run-$$}"

mkdir -p "$artifact_dir"
export RAY_CLICKHOUSE_CLUSTER_RUNTIME_IMAGE="$runtime_image"
compose=(docker compose --project-name "$project_name" --file "$compose_file")
compose_owned=0
runtime_image_owned=0

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
        "${compose[@]}" cp ray-head:/workspace/ray-clickhouse/cluster-pytest.xml "$artifact_dir/pytest.xml" \
        >/dev/null 2>&1 || true
    env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
        -u http_proxy -u https_proxy -u all_proxy \
        "${compose[@]}" logs --no-color >"$artifact_dir/compose.log" 2>&1 || true
    if [[ "$compose_owned" -eq 1 ]]; then
        env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
            -u http_proxy -u https_proxy -u all_proxy \
            "${compose[@]}" images >"$artifact_dir/compose-images.txt" 2>&1 || true
        env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
            -u http_proxy -u https_proxy -u all_proxy \
            "${compose[@]}" down --volumes --remove-orphans || cleanup_status=1
    fi
    if [[ "$runtime_image_owned" -eq 1 ]] && env \
        -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
        -u http_proxy -u https_proxy -u all_proxy \
        docker image inspect "$runtime_image" >/dev/null 2>&1; then
        env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
            -u http_proxy -u https_proxy -u all_proxy \
            docker image rm "$runtime_image" \
            >"$artifact_dir/runtime-image-cleanup.log" 2>&1 || cleanup_status=1
    fi
    record_docker_state after || cleanup_status=1
    if [[ "$status" -eq 0 && "$cleanup_status" -ne 0 ]]; then
        status=$cleanup_status
    fi
    exit "$status"
}

trap cleanup EXIT

record_docker_state before
if env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy \
    docker image inspect "$runtime_image" >/dev/null 2>&1; then
    echo "runtime image already exists: $runtime_image" >&2
    exit 1
fi
existing_resources=$(env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy \
    "${compose[@]}" ps --all --quiet)
if [[ -n "$existing_resources" ]]; then
    echo "Compose project already has resources: $project_name" >&2
    exit 1
fi
runtime_image_owned=1

env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy \
    docker pull "$ray_base_source" 2>&1 | tee "$artifact_dir/ray-base-pull.log"
env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy \
    docker image inspect "$ray_base_source" \
    --format 'id={{.Id}} repo_digests={{json .RepoDigests}}' \
    >"$artifact_dir/ray-base-image.txt"
ray_base_id=$(env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy \
    docker image inspect "$ray_base_source" --format '{{.Id}}')
if local_base_id=$(env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy \
    docker image inspect "$ray_base_local" --format '{{.Id}}' 2>/dev/null); then
    if [[ "$local_base_id" != "$ray_base_id" ]]; then
        echo "local Ray base tag points to a different image: $ray_base_local" >&2
        exit 1
    fi
else
    env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
        -u http_proxy -u https_proxy -u all_proxy \
        docker tag "$ray_base_source" "$ray_base_local"
fi
env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy \
    docker build --pull=false \
    --build-arg "RAY_BASE_IMAGE=$ray_base_local" \
    --tag "$runtime_image" \
    --file docker/ray/Dockerfile . 2>&1 | tee "$artifact_dir/runtime-image-build.log"
env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy \
    docker image inspect "$runtime_image" \
    --format 'id={{.Id}} repo_tags={{json .RepoTags}} repo_digests={{json .RepoDigests}}' \
    >"$artifact_dir/runtime-image.txt"
env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy \
    "${compose[@]}" config --images >"$artifact_dir/compose-image-references.txt"
compose_owned=1
env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy \
    "${compose[@]}" up --detach --wait --no-build

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
