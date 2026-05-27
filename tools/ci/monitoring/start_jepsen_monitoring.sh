#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(
  cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1
  pwd -P
)"

MONITORING_SERVER_HOST="${MONITORING_SERVER_HOST:-}"
MONITORING_USERNAME="${MONITORING_USERNAME:-}"
MONITORING_PASSWORD="${MONITORING_PASSWORD:-}"
TARGET_DOCKER_NETWORK="${TARGET_DOCKER_NETWORK:-jepsen_jepsen}"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-jepsen-monitoring-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}}"
CLUSTER_ID="${CLUSTER_ID:-diff-jepsen-${GITHUB_RUN_ID:-local}}"
CLUSTER_ENV="${CLUSTER_ENV:-ci-jepsen}"
SERVICE_NAME="${SERVICE_NAME:-memgraph-jepsen}"
JEPSEN_NODE_COUNT="${JEPSEN_NODE_COUNT:-}"

if [[ -n "${JEPSEN_NODE_COUNT}" ]]; then
  JEPSEN_NODES="$(docker ps --format '{{.Names}}' | awk '/^jepsen-n[0-9]+$/' | sort -V | head -n "${JEPSEN_NODE_COUNT}" | paste -sd, -)"
else
  JEPSEN_NODES="$(docker ps --format '{{.Names}}' | awk '/^jepsen-n[0-9]+$/' | sort -V | paste -sd, -)"
fi
if [[ -z "${JEPSEN_NODES}" ]]; then
  echo "No Jepsen node containers found, cannot start monitoring." >&2
  exit 1
fi

METRICS_TARGETS="$(echo "${JEPSEN_NODES}" | awk -F, '{for (i=1; i<=NF; i++) printf "%s%s:9091", (i==1?"":","), $i}')"
LOG_WS_TARGETS="$(echo "${JEPSEN_NODES}" | awk -F, '{for (i=1; i<=NF; i++) printf "%s%s:7444", (i==1?"":","), $i}')"

cd "${SCRIPT_DIR}"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME}" \
TARGET_DOCKER_NETWORK="${TARGET_DOCKER_NETWORK}" \
MONITORING_SERVER_HOST="${MONITORING_SERVER_HOST}" \
MONITORING_USERNAME="${MONITORING_USERNAME}" \
MONITORING_PASSWORD="${MONITORING_PASSWORD}" \
CLUSTER_ID="${CLUSTER_ID}" \
CLUSTER_ENV="${CLUSTER_ENV}" \
SERVICE_NAME="${SERVICE_NAME}" \
MEMGRAPH_METRICS_TARGETS="${METRICS_TARGETS}" \
MEMGRAPH_LOG_WS_TARGETS="${LOG_WS_TARGETS}" \
./up.sh
