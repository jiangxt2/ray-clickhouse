#!/usr/bin/env bash
set -Eeuo pipefail

mode=${1:-smoke}
if [[ "$mode" != "smoke" && "$mode" != "dry-run" && "$mode" != "formal" ]]; then
    echo "usage: $0 [smoke|dry-run|formal]" >&2
    exit 2
fi

repo_root=$(git rev-parse --show-toplevel)
if [[ "$PWD" != "$repo_root" ]]; then
    echo "run from repository root: $repo_root" >&2
    exit 1
fi

reference_values=$(uv run --project comparison/official python -c \
    "from pathlib import Path; from ray_clickhouse_comparison.config import load_reference; c=load_reference(Path('comparison/official/config/reference.toml')); r=c.resources; print(c.runtime_base_commit, c.ray_base_image, c.clickhouse_image, c.sampling_interval_seconds, r.worker_memory_bytes, r.head_memory_bytes, r.runner_memory_bytes, r.head_object_store_memory_bytes, r.worker_object_store_memory_bytes, sep='\\t')")
IFS=$'\t' read -r base_commit base_image clickhouse_image sampling_interval \
    worker_memory head_memory runner_memory head_object_store_memory \
worker_object_store_memory <<<"$reference_values"
frontend_image="docker.m.daocloud.io/docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e"
base_digest=${base_image##*@sha256:}
base_local_image="ray-clickhouse-comparison-ray-base:2.58.0-${base_digest:0:12}"
git cat-file -e "${base_commit}^{commit}"
uv run --project comparison/official ray-clickhouse-comparison validate \
    --reference comparison/official/config/reference.toml \
    --scenarios comparison/official/config/scenarios.toml \
    --manifest-schema comparison/official/schema/manifest.schema.json \
    --result-schema comparison/official/schema/result.schema.json

run_stamp=$(date -u +%Y%m%dt%H%M%Sz)
run_id="${mode}-${run_stamp}-$$"
artifact_dir=${RAY_COMPARISON_ARTIFACT_DIR:-"$repo_root/.artifacts/comparison/$run_id"}
if [[ -e "$artifact_dir" ]]; then
    if [[ ! -d "$artifact_dir" || -n "$(find "$artifact_dir" -mindepth 1 -print -quit)" ]]; then
        echo "artifact directory must be absent or empty: $artifact_dir" >&2
        exit 1
    fi
else
    mkdir -p "$artifact_dir"
fi
artifact_dir=$(cd "$artifact_dir" && pwd)
harness_git_state=clean
if [[ -n "$(git status --porcelain --untracked-files=all)" ]]; then
    harness_git_state=dirty
fi
if [[ "$mode" != "smoke" && "$harness_git_state" != "clean" ]]; then
    echo "dry-run and formal modes require a clean committed harness" >&2
    exit 1
fi

temporary_root=$(mktemp -d "${TMPDIR:-/tmp}/ray-clickhouse-comparison.XXXXXX")
cleanup_temporary_root() {
    local path="$temporary_root"
    if [[ -z "$path" || ! -d "$path" ]]; then
        temporary_root=""
        return 0
    fi
    local prefix="${TMPDIR:-/tmp}/ray-clickhouse-comparison."
    case "$path" in
        "$prefix"*) ;;
        *) echo "refusing to clean an unexpected temporary path: $path" >&2; return 1 ;;
    esac
    if command -v trash >/dev/null 2>&1; then
        trash "$path"
    else
        find "$path" -depth -type f -delete
        find "$path" -depth -type l -delete
        find "$path" -depth -type d -empty -delete
    fi
    temporary_root=""
}
trap cleanup_temporary_root EXIT
base_source="$temporary_root/base-source"
external_context="$temporary_root/external-wheel"
harness_context="$temporary_root/harness-wheel"
mkdir -p "$base_source" "$external_context" "$harness_context"

git archive "$base_commit" | tar -x -C "$base_source"
uv build "$base_source" --wheel --force-pep517 --no-cache \
    --out-dir "$temporary_root/base-dist"
uv build comparison/official --wheel --force-pep517 --no-cache \
    --out-dir "$temporary_root/harness-dist"

shopt -s nullglob
external_wheels=("$temporary_root"/base-dist/*.whl)
harness_wheels=("$temporary_root"/harness-dist/*.whl)
shopt -u nullglob
if [[ ${#external_wheels[@]} -ne 1 || ${#harness_wheels[@]} -ne 1 ]]; then
    echo "expected exactly one external and one harness wheel" >&2
    exit 1
fi
cp "${external_wheels[0]}" "$external_context/$(basename "${external_wheels[0]}")"
cp "${harness_wheels[0]}" "$harness_context/$(basename "${harness_wheels[0]}")"
shasum -a 256 "$external_context"/*.whl "$harness_context"/*.whl \
    >"$artifact_dir/wheel-sha256.txt"
uv run --project comparison/official ray-clickhouse-comparison verify-wheel \
    --wheel "$harness_context"/*.whl \
    --source comparison/official/src/ray_clickhouse_comparison \
    --package ray_clickhouse_comparison
uv run --project comparison/official ray-clickhouse-comparison verify-wheel \
    --wheel "$external_context"/*.whl \
    --source "$base_source/src/ray_clickhouse" \
    --package ray_clickhouse

runtime_image=${RAY_COMPARISON_IMAGE:-"ray-clickhouse-comparison-runtime:${run_stamp}-$$"}
runtime_image_owned=0
baseline_recorded=0
active_project=""
active_case_dir=""

docker_cmd() {
    env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
        -u http_proxy -u https_proxy -u all_proxy docker "$@"
}

compose_cmd() {
    env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
        -u http_proxy -u https_proxy -u all_proxy \
        docker compose --project-name "$active_project" --file docker/comparison/compose.yaml "$@"
}

normalize_case_evidence() {
    local owner
    owner="$(id -u):$(id -g)"
    if [[ ! "$owner" =~ ^[0-9]+:[0-9]+$ ]]; then
        echo "unable to determine a numeric evidence owner: $owner" >&2
        return 1
    fi
    compose_cmd exec -T ray-head /bin/bash -lc \
        "find /evidence -xdev -exec chown --no-dereference '$owner' {} + \\
        && find /evidence -xdev -type d -exec chmod u+rwx {} + \\
        && find /evidence -xdev -type f -exec chmod u+rw {} +"
}

record_docker_state() {
    phase=$1
    docker_cmd image ls --filter dangling=true --no-trunc \
        --format '{{.ID}}\t{{.Repository}}\t{{.Tag}}\t{{.CreatedAt}}\t{{.Size}}' \
        | LC_ALL=C sort >"$artifact_dir/docker-images-dangling-$phase.txt"
    docker_cmd system df >"$artifact_dir/docker-system-df-$phase.txt"
}

cleanup_case() {
    if [[ -z "$active_project" ]]; then
        return
    fi
    cleanup_status=0
    compose_cmd logs --no-color >"$active_case_dir/compose.log" 2>&1 || cleanup_status=1
    compose_cmd images >"$active_case_dir/compose-images.txt" 2>&1 || cleanup_status=1
    if ! compose_cmd down --volumes --remove-orphans; then
        cleanup_status=1
        compose_cmd down --volumes --remove-orphans || true
    fi
    if [[ -n "$(docker_cmd ps --all --quiet --filter "label=com.docker.compose.project=$active_project")" \
          || -n "$(docker_cmd network ls --quiet --filter "label=com.docker.compose.project=$active_project")" \
          || -n "$(docker_cmd volume ls --quiet --filter "label=com.docker.compose.project=$active_project")" ]]; then
        cleanup_status=1
    else
        active_project=""
        active_case_dir=""
    fi
    return "$cleanup_status"
}

cleanup() {
    status=$?
    trap - EXIT
    set +e
    cleanup_status=0
    cleanup_case || cleanup_status=1
    if [[ "$runtime_image_owned" -eq 1 ]] && docker_cmd image inspect "$runtime_image" >/dev/null 2>&1; then
        docker_cmd image rm "$runtime_image" >"$artifact_dir/runtime-image-cleanup.log" 2>&1 \
            || cleanup_status=1
    fi
    record_docker_state after || cleanup_status=1
    if [[ "$baseline_recorded" -eq 1 ]]; then
        cmp "$artifact_dir/docker-images-dangling-before.txt" \
            "$artifact_dir/docker-images-dangling-after.txt" || cleanup_status=1
    fi
    cleanup_temporary_root || cleanup_status=1
    if [[ "$status" -eq 0 && "$cleanup_status" -ne 0 ]]; then
        status=$cleanup_status
    fi
    set -e
    exit "$status"
}
trap cleanup EXIT

if docker_cmd image inspect "$runtime_image" >/dev/null 2>&1; then
    echo "comparison runtime image already exists: $runtime_image" >&2
    exit 1
fi
docker_cmd pull "$base_image" 2>&1 | tee "$artifact_dir/ray-base-pull.log"
base_image_id=$(docker_cmd image inspect "$base_image" --format '{{.Id}}')
base_repo_digests=$(docker_cmd image inspect "$base_image" --format '{{json .RepoDigests}}')
if [[ "$base_repo_digests" != *"@sha256:$base_digest"* ]]; then
    echo "pulled Ray base image does not expose the approved digest" >&2
    exit 1
fi
docker_cmd image inspect "$base_image" \
    --format 'id={{.Id}} repo_digests={{json .RepoDigests}}' \
    >"$artifact_dir/ray-base-image.txt"
if local_base_id=$(docker_cmd image inspect "$base_local_image" --format '{{.Id}}' 2>/dev/null); then
    if [[ "$local_base_id" != "$base_image_id" ]]; then
        echo "local Ray base tag points to a different image: $base_local_image" >&2
        exit 1
    fi
else
    docker_cmd tag "$base_image" "$base_local_image"
fi
docker_cmd pull "$clickhouse_image" 2>&1 | tee "$artifact_dir/clickhouse-pull.log"
docker_cmd pull "$frontend_image" 2>&1 | tee "$artifact_dir/dockerfile-frontend-pull.log"
record_docker_state before
baseline_recorded=1
uv run --project comparison/official ray-clickhouse-comparison context-manifest \
    --root . --output "$artifact_dir/docker-context-manifest.json"
docker_cmd buildx build --load --pull=false \
    --build-arg "RAY_BASE_IMAGE=$base_local_image" \
    --build-context "external-wheel=$external_context" \
    --build-context "harness-wheel=$harness_context" \
    --file docker/comparison/Dockerfile \
    --tag "$runtime_image" \
    . 2>&1 | tee "$artifact_dir/runtime-image-build.log"
runtime_image_owned=1
docker_cmd image inspect "$runtime_image" \
    --format 'id={{.Id}} repo_tags={{json .RepoTags}} repo_digests={{json .RepoDigests}}' \
    >"$artifact_dir/runtime-image.txt"
docker_cmd run --rm "$runtime_image" \
    cat /opt/ray-clickhouse/official-install-report.json \
    >"$artifact_dir/official-install-report.json"
docker_cmd run --rm "$runtime_image" \
    cat /opt/ray-clickhouse/external-install-report.json \
    >"$artifact_dir/external-install-report.json"
runtime_image_id=$(docker_cmd image inspect "$runtime_image" --format '{{.Id}}')
harness_commit=$(git rev-parse HEAD)
candidate_sha=${RAY_COMPARISON_CANDIDATE_SHA:-$harness_commit}
workflow_sha=${RAY_COMPARISON_WORKFLOW_SHA:-$harness_commit}
controller_lock_sha256=$(shasum -a 256 comparison/official/uv.lock | awk '{print $1}')
official_requirements_sha256=$(shasum -a 256 \
    comparison/official/env/official-requirements.txt | awk '{print $1}')
external_requirements_sha256=$(shasum -a 256 \
    comparison/official/env/external-requirements.txt | awk '{print $1}')
result_schema_sha256=$(shasum -a 256 \
    comparison/official/schema/result.schema.json | awk '{print $1}')
uv run --project comparison/official ray-clickhouse-comparison manifest \
    --reference comparison/official/config/reference.toml \
    --scenarios comparison/official/config/scenarios.toml \
    --manifest-schema comparison/official/schema/manifest.schema.json \
    --official-report "$artifact_dir/official-install-report.json" \
    --run-id "$run_id" \
    --mode "$mode" \
    --harness-commit "$harness_commit" \
    --candidate-sha "$candidate_sha" \
    --workflow-sha "$workflow_sha" \
    --harness-git-state "$harness_git_state" \
    --harness-wheel "$harness_context"/*.whl \
    --external-wheel "$external_context"/*.whl \
    --runtime-image-id "$runtime_image_id" \
    --controller-lock-sha256 "$controller_lock_sha256" \
    --official-requirements-sha256 "$official_requirements_sha256" \
    --external-requirements-sha256 "$external_requirements_sha256" \
    --result-schema-sha256 "$result_schema_sha256" \
    --output "$artifact_dir/manifest.json"

export RAY_COMPARISON_IMAGE="$runtime_image"
export RAY_COMPARISON_CLICKHOUSE_IMAGE="$clickhouse_image"
export RAY_COMPARISON_WORKER_MEMORY_BYTES="$worker_memory"
export RAY_COMPARISON_HEAD_MEMORY_BYTES="$head_memory"
export RAY_COMPARISON_RUNNER_MEMORY_BYTES="$runner_memory"
export RAY_COMPARISON_HEAD_OBJECT_STORE_MEMORY_BYTES="$head_object_store_memory"
export RAY_COMPARISON_WORKER_OBJECT_STORE_MEMORY_BYTES="$worker_object_store_memory"

sample_resource_snapshot() {
    sample_index=$1
    strict=$2
    printf '{"sample_index":%s}\n' "$sample_index" \
        >>"$active_case_dir/docker-stats.jsonl"
    container_ids=$(compose_cmd ps --quiet \
        clickhouse proxy ray-head ray-worker-1 ray-worker-2 runner)
    if [[ -z "$container_ids" ]]; then
        if [[ "$strict" == "true" ]]; then
            return 1
        fi
    else
        container_ids=${container_ids//$'\n'/ }
        read -r -a container_id_array <<<"$container_ids"
        if ! docker_cmd stats --no-stream --format '{{json .}}' "${container_id_array[@]}" \
            >>"$active_case_dir/docker-stats.jsonl" 2>/dev/null; then
            if [[ "$strict" == "true" ]]; then
                return 1
            fi
        fi
    fi
    for service in ray-worker-1 ray-worker-2; do
        if ! compose_cmd exec -T "$service" \
            /opt/ray-clickhouse/controller/bin/python \
            -m ray_clickhouse_comparison.metrics \
            --root-pid 1 --service "$service" --sample-index "$sample_index" \
            >>"$active_case_dir/process-samples.jsonl" 2>/dev/null; then
            if [[ "$strict" == "true" ]]; then
                return 1
            fi
        fi
    done
    if [[ ! -s "$active_case_dir/driver.pid" ]]; then
        if [[ "$strict" == "true" ]]; then
            return 1
        fi
    else
        driver_pid=$(tr -d '\n' <"$active_case_dir/driver.pid")
        if [[ ! "$driver_pid" =~ ^[0-9]+$ ]]; then
            if [[ "$strict" == "true" ]]; then
                return 1
            fi
        elif ! compose_cmd exec -T ray-head \
            /opt/ray-clickhouse/controller/bin/python \
            -m ray_clickhouse_comparison.metrics \
            --root-pid "$driver_pid" --service driver --sample-index "$sample_index" \
            >>"$active_case_dir/process-samples.jsonl" 2>/dev/null; then
            if [[ "$strict" == "true" ]]; then
                return 1
            fi
        fi
    fi
    for service in ray-head ray-worker-1 ray-worker-2; do
        printf '# comparison_sample %s service %s\n' "$sample_index" "$service" \
            >>"$active_case_dir/ray-metrics-samples.prom"
        if ! compose_cmd exec -T "$service" /opt/ray-clickhouse/controller/bin/python -c \
            "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8080/metrics', timeout=2).read().decode())" \
            >>"$active_case_dir/ray-metrics-samples.prom" 2>/dev/null; then
            if [[ "$strict" == "true" ]]; then
                return 1
            fi
        fi
    done
}

sample_resources() {
    job_pid=$1
    strict=$2
    sample_index=0
    baseline_recorded=false
    while kill -0 "$job_pid" 2>/dev/null \
        && [[ ! -s "$active_case_dir/measurement-started.json" ]]; do
        if [[ "$baseline_recorded" != "true" && -s "$active_case_dir/driver.pid" ]]; then
            if ! sample_resource_snapshot 0 "$strict"; then
                return 1
            fi
            baseline_recorded=true
            printf '{"ready_at":%s}\n' "$(date +%s)" \
                >"$active_case_dir/resource-baseline-ready"
        fi
        sleep 0.05
    done
    if [[ "$baseline_recorded" != "true" ]]; then
        if ! sample_resource_snapshot 0 "$strict"; then
            return 1
        fi
        printf '{"ready_at":%s}\n' "$(date +%s)" \
            >"$active_case_dir/resource-baseline-ready"
    fi
    sample_index=1
    while kill -0 "$job_pid" 2>/dev/null \
        && [[ ! -s "$active_case_dir/measurement-complete.json" ]]; do
        if ! sample_resource_snapshot "$sample_index" "$strict"; then
            return 1
        fi
        sample_index=$((sample_index + 1))
        sleep "$sampling_interval"
    done
}

run_with_timeout() {
    timeout_seconds=$1
    shift
    "$@" &
    command_pid=$!
    for _ in $(seq 1 $((timeout_seconds * 2))); do
        if ! kill -0 "$command_pid" 2>/dev/null; then
            wait "$command_pid"
            return $?
        fi
        sleep 0.5
    done
    pkill -TERM -P "$command_pid" 2>/dev/null || true
    kill -TERM "$command_pid" 2>/dev/null || true
    wait "$command_pid" 2>/dev/null || true
    return 124
}

capture_ray_metrics() {
    for service in ray-head ray-worker-1 ray-worker-2; do
        compose_cmd exec -T "$service" /opt/ray-clickhouse/controller/bin/python -c \
            "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8080/metrics', timeout=5).read().decode())" \
            >"$active_case_dir/${service}-metrics.prom" 2>"$active_case_dir/${service}-metrics-error.log" || true
    done
}

wait_for_fault_and_kill_worker() {
    event_path=$1
    for _ in $(seq 1 300); do
        if [[ -s "$event_path" ]]; then
            client_ip=$(sed -nE 's/.*"client_ip": "([^"]+)".*/\1/p' "$event_path")
            case "$client_ip" in
                10.251.0.11) service=ray-worker-1 ;;
                10.251.0.12) service=ray-worker-2 ;;
                *) echo "fault event has unknown worker address: $client_ip" >&2; return 1 ;;
            esac
            container_id=$(compose_cmd ps --quiet "$service")
            if [[ -z "$container_id" ]]; then
                echo "fault target worker container is unavailable: $service" >&2
                return 1
            fi
            docker_cmd kill --signal KILL "$container_id" >"$active_case_dir/killed-worker.txt"
            return 0
        fi
        sleep 0.2
    done
    echo "timed out waiting for one-shot worker-loss boundary" >&2
    return 1
}

run_case() {
    side=$1
    scenario=$2
    repetition=$3
    pair_position=${4:-0}
    fault=$5
    warmup=$6
    resource_required=$7
    safe_case=$(printf '%s-%s-%s-%s' "$side" "$scenario" "$fault" "$repetition" | tr '.:' '--')
    case_hash=$(printf '%s' "$safe_case" | shasum -a 256 | cut -c1-12)
    active_project="raychcmp-${run_stamp//[!0-9]/}-${case_hash}"
    active_case_dir="$artifact_dir/$safe_case"
    mkdir -p "$active_case_dir/control"
    export RAY_COMPARISON_RUNTIME="$side"
    export RAY_COMPARISON_ARTIFACT_DIR="$active_case_dir"

    existing=$(compose_cmd ps --all --quiet)
    if [[ -n "$existing" ]]; then
        echo "comparison Compose project already has resources: $active_project" >&2
        return 1
    fi
    compose_cmd config >"$active_case_dir/compose-config.yaml"
    compose_cmd up --detach --wait --no-build
    ready=false
    : >"$active_case_dir/ray-ready.log"
    for _ in $(seq 1 6); do
        if run_with_timeout 10 compose_cmd exec -T ray-head \
            "/opt/ray-clickhouse/$side/bin/python" -c \
            "import ray; ray.init(address='auto'); assert len([n for n in ray.nodes() if n['Alive']]) >= 3; ray.shutdown()" \
            >>"$active_case_dir/ray-ready.log" 2>&1; then
            ready=true
            break
        fi
        sleep 0.5
    done
    if [[ "$ready" != "true" ]]; then
        echo "Ray comparison cluster did not become ready" >&2
        return 1
    fi

    compose_cmd exec -T ray-head "/opt/ray-clickhouse/$side/bin/python" \
        -m ray_clickhouse_comparison prepare \
        --side "$side" --scenario "$scenario" --mode "$mode" \
        --scenarios /workspace/ray-clickhouse/comparison/official/config/scenarios.toml \
        --expected-identity /evidence/expected-identity.json \
        >"$active_case_dir/prepare.log" 2>&1
    if [[ "$warmup" == "true" ]]; then
        compose_cmd exec -T ray-head "/opt/ray-clickhouse/$side/bin/python" \
            -m ray_clickhouse_comparison warmup \
            --side "$side" --scenario "$scenario" --run-id "$run_id" \
            --reference /workspace/ray-clickhouse/comparison/official/config/reference.toml \
            --scenarios /workspace/ray-clickhouse/comparison/official/config/scenarios.toml \
            >"$active_case_dir/warmup.log" 2>&1
    fi
    command=(
        compose_cmd exec -T ray-head
        env "RAY_COMPARISON_PAIR_POSITION=$pair_position"
        "RAY_COMPARISON_QUERY_EVIDENCE=/evidence/queries.jsonl"
        "RAY_COMPARISON_TASK_EVIDENCE=/evidence/tasks.jsonl"
        "RAY_COMPARISON_MEASUREMENT_STARTED=/evidence/measurement-started.json"
        "RAY_COMPARISON_MEASUREMENT_COMPLETE=/evidence/measurement-complete.json"
        "RAY_COMPARISON_DRIVER_PID_FILE=/evidence/driver.pid"
        "RAY_COMPARISON_RESOURCE_BASELINE_READY=/evidence/resource-baseline-ready"
        "/opt/ray-clickhouse/$side/bin/python" -m ray_clickhouse_comparison
        run --side "$side" --scenario "$scenario"
        --run-id "$run_id" --repetition "$repetition"
        --reference /workspace/ray-clickhouse/comparison/official/config/reference.toml
        --scenarios /workspace/ray-clickhouse/comparison/official/config/scenarios.toml
        --output "/evidence/result.json"
        --result-schema /workspace/ray-clickhouse/comparison/official/schema/result.schema.json
        --control-dir /evidence/control
        --expected-identity /evidence/expected-identity.json
    )

    set +e
    "${command[@]}" >"$active_case_dir/ray-job.log" 2>&1 &
    job_pid=$!
    sample_resources "$job_pid" "$resource_required" &
    sampler_pid=$!
    kill_status=0
    if [[ "$fault" == "hold_response" ]]; then
        wait_for_fault_and_kill_worker "$active_case_dir/control/fault-event.json" || kill_status=$?
    fi
    wait "$job_pid"
    job_status=$?
    wait "$sampler_pid" 2>/dev/null
    sampler_status=$?
    set -e

    normalize_case_evidence
    capture_ray_metrics
    if [[ "$job_status" -ne 0 || "$kill_status" -ne 0 || "$sampler_status" -ne 0 ]]; then
        echo "comparison case failed: $safe_case" >&2
        return 1
    fi
    if [[ "$resource_required" == "true" ]]; then
        uv run --project comparison/official ray-clickhouse-comparison resource-summary \
            --docker-stats "$active_case_dir/docker-stats.jsonl" \
            --process-samples "$active_case_dir/process-samples.jsonl" \
            --ray-metrics "$active_case_dir/ray-metrics-samples.prom" \
            --require-complete \
            --output "$active_case_dir/resources.json"
    else
        uv run --project comparison/official ray-clickhouse-comparison resource-summary \
            --docker-stats "$active_case_dir/docker-stats.jsonl" \
            --process-samples "$active_case_dir/process-samples.jsonl" \
            --ray-metrics "$active_case_dir/ray-metrics-samples.prom" \
            --output "$active_case_dir/resources.json"
    fi
    uv run --project comparison/official ray-clickhouse-comparison validate-result \
        --result "$active_case_dir/result.json" \
        --result-schema comparison/official/schema/result.schema.json \
        --scenario "$scenario" \
        --scenarios comparison/official/config/scenarios.toml \
        --mode "$mode"
    if [[ "$scenario" == "read.error.permission" ]]; then
        compose_cmd exec -T ray-head "/opt/ray-clickhouse/$side/bin/python" \
            -m ray_clickhouse_comparison cleanup-permission \
            >"$active_case_dir/permission-cleanup.log" 2>&1 || return 1
    fi
    cleanup_case
}

if [[ "$mode" == "smoke" ]]; then
    # The smoke fixture is intentionally small; sparse per-service Ray Object Store
    # series are recorded as incomplete diagnostics, while formal resource scenarios
    # remain strict and fail closed.
    run_case official read.default.single 0 0 none true false
    run_case external read.default.single 0 1 none true false
    run_case external write.transport.post_commit 0 0 drop_response false false
    run_case official write.worker.post_commit 0 0 hold_response false false
elif [[ "$mode" == "dry-run" ]]; then
    # Dry-run uses the bounded fixture profile to validate behavior and artifact flow;
    # formal runs are the only remote profile that makes resource conclusions.
    run_case official read.controlled.ordered 0 0 none true false
    run_case external read.controlled.ordered 0 1 none true false
else
    # Keep the scenario stream on a dedicated descriptor. Commands in run_case
    # may inherit stdin (for example docker compose exec), and must not consume
    # the remaining scenario definitions.
    while IFS=$'\t' read -r scenario fault warmup resource_required repetitions sides <&3; do
        for repetition in $(seq 0 $((repetitions - 1))); do
            IFS=',' read -r first_side second_side <<<"$sides"
            if (( repetition % 2 == 1 )); then
                swap=$first_side
                first_side=$second_side
                second_side=$swap
            fi
            run_case "$first_side" "$scenario" "$repetition" 0 "$fault" \
                "$warmup" "$resource_required"
            run_case "$second_side" "$scenario" "$repetition" 1 "$fault" \
                "$warmup" "$resource_required"
        done
    done 3< <(uv run --project comparison/official python -c \
        "from pathlib import Path; from ray_clickhouse_comparison.config import load_scenarios; [print(s.id, s.fault, str(s.warmup).lower(), str(s.resource_metrics_required).lower(), s.repetitions, ','.join(s.sides), sep='\\t') for s in load_scenarios(Path('comparison/official/config/scenarios.toml'))]")
fi

uv run --project comparison/official ray-clickhouse-comparison collect \
    --input "$artifact_dir" \
    --results "$artifact_dir/results.jsonl" \
    --queries "$artifact_dir/queries.csv" \
    --result-schema comparison/official/schema/result.schema.json
uv run --project comparison/official ray-clickhouse-comparison summarize \
    --results "$artifact_dir/results.jsonl" \
    --result-schema comparison/official/schema/result.schema.json \
    --output "$artifact_dir/summary.json"
record_docker_state completed
