#!/usr/bin/env bash
set -Eeuo pipefail
baseline="${RAY_CLICKHOUSE_IT_BASELINE:-primary}"
export RAY_CLICKHOUSE_CLUSTER_PROJECT="ray-clickhouse-distributed-it-${baseline//./-}-$$"
export RAY_CLICKHOUSE_CLUSTER_COMPOSE_EXTRA=docker/distributed/compose.yaml
export RAY_CLICKHOUSE_CLUSTER_IT_ARTIFACT_DIR="${RAY_CLICKHOUSE_CLUSTER_IT_ARTIFACT_DIR:-.artifacts/it/distributed-${RAY_CLICKHOUSE_IT_BASELINE:-primary}}"
mkdir -p "$RAY_CLICKHOUSE_CLUSTER_IT_ARTIFACT_DIR/tls"
export RAY_CLICKHOUSE_IT_TLS_DIR="$(cd "$RAY_CLICKHOUSE_CLUSTER_IT_ARTIFACT_DIR/tls" && pwd)"
# Ephemeral fixture keys are generated per run, never committed or printed.
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
    -subj /CN=ray-clickhouse-it-ca \
    -keyout "$RAY_CLICKHOUSE_IT_TLS_DIR/ca.key" \
    -out "$RAY_CLICKHOUSE_IT_TLS_DIR/ca.crt" 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj /CN=clickhouse \
    -keyout "$RAY_CLICKHOUSE_IT_TLS_DIR/server.key" \
    -out "$RAY_CLICKHOUSE_IT_TLS_DIR/server.csr" 2>/dev/null
printf '%s\n' 'subjectAltName=DNS:clickhouse,DNS:ch2,DNS:ch3,DNS:ch4' \
    >"$RAY_CLICKHOUSE_IT_TLS_DIR/extensions.txt"
openssl x509 -req -days 1 -in "$RAY_CLICKHOUSE_IT_TLS_DIR/server.csr" \
    -CA "$RAY_CLICKHOUSE_IT_TLS_DIR/ca.crt" \
    -CAkey "$RAY_CLICKHOUSE_IT_TLS_DIR/ca.key" -CAcreateserial \
    -extfile "$RAY_CLICKHOUSE_IT_TLS_DIR/extensions.txt" \
    -out "$RAY_CLICKHOUSE_IT_TLS_DIR/server.crt" 2>/dev/null
chmod 644 "$RAY_CLICKHOUSE_IT_TLS_DIR/server.key"
if [[ "$#" -eq 0 ]]; then
    set -- tests/distributed
fi
exec ./scripts/run_ray_cluster_it.sh "$@"
