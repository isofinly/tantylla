#!/usr/bin/env bash
# =========================================================================
# wait-for-services.sh: Block until all benchmark services are healthy
# =========================================================================
#
# Usage: bash scripts/wait-for-services.sh <scylla_port> <gateway_port> <es_port>
#
# Pass 0 for any port to skip that service's health check.
# Example (tantylla-only):  bash scripts/wait-for-services.sh 9043 8080 0
# Example (competitor-only): bash scripts/wait-for-services.sh 9043 0 9200

set -euo pipefail

SCYLLA_PORT="${1:?Usage: $0 <scylla_port> <gateway_port> <es_port>}"
GATEWAY_PORT="${2:?Usage: $0 <scylla_port> <gateway_port> <es_port>}"
ES_PORT="${3:?Usage: $0 <scylla_port> <gateway_port> <es_port>}"

MAX_RETRIES=60
RETRY_INTERVAL=3

wait_for_endpoint() {
    local name="$1"
    local url="$2"
    local retries=0

    printf "Waiting for %s at %s " "$name" "$url"
    while ! curl -sf "$url" > /dev/null 2>&1; do
        retries=$((retries + 1))
        if [ "$retries" -ge "$MAX_RETRIES" ]; then
            printf "\nERROR: %s did not become healthy after %d attempts.\n" "$name" "$MAX_RETRIES" >&2
            exit 1
        fi
        printf "."
        sleep "$RETRY_INTERVAL"
    done
    printf " OK\n"
}

wait_for_cql() {
    local port="$1"
    local retries=0

    printf "Waiting for ScyllaDB CQL on port %s " "$port"
    # cqlsh exits 0 on success, non-zero otherwise.
    while ! cqlsh localhost "$port" -e "DESCRIBE KEYSPACES" > /dev/null 2>&1; do
        retries=$((retries + 1))
        if [ "$retries" -ge "$MAX_RETRIES" ]; then
            printf "\nERROR: ScyllaDB did not become healthy after %d attempts.\n" "$MAX_RETRIES" >&2
            exit 1
        fi
        printf "."
        sleep "$RETRY_INTERVAL"
    done
    printf " OK\n"
}

# ScyllaDB is always required.
wait_for_cql "$SCYLLA_PORT"

# Tantylla gateway (if enabled).
if [ "$GATEWAY_PORT" -ne 0 ]; then
    wait_for_endpoint "Tantylla Gateway" "http://localhost:${GATEWAY_PORT}/api/health"
fi

# Elasticsearch (if enabled).
if [ "$ES_PORT" -ne 0 ]; then
    wait_for_endpoint "Elasticsearch" "http://localhost:${ES_PORT}/_cluster/health"
fi

echo "All services are healthy."
