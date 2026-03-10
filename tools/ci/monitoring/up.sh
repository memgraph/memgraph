#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(
  cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1
  pwd -P
)"
GENERATED_DIR="${SCRIPT_DIR}/generated"
MANIFESTS_DIR="${SCRIPT_DIR}/manifests"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
mkdir -p "${GENERATED_DIR}"

COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-memgraph-monitoring}"
REMOTE_WRITE_URL="${REMOTE_WRITE_URL:-}"
VLOGS_PUSH_URL="${VLOGS_PUSH_URL:-}"
MONITORING_SERVER_HOST="${MONITORING_SERVER_HOST:-}"
REMOTE_WRITE_PORT="${REMOTE_WRITE_PORT:-30091}"
VLOGS_PORT="${VLOGS_PORT:-30454}"
REMOTE_WRITE_SCHEME="${REMOTE_WRITE_SCHEME:-http}"
VLOGS_SCHEME="${VLOGS_SCHEME:-http}"
MEMGRAPH_METRICS_HOST="${MEMGRAPH_METRICS_HOST:-host.docker.internal}"
MEMGRAPH_METRICS_PORT="${MEMGRAPH_METRICS_PORT:-9091}"
MEMGRAPH_METRICS_TARGETS="${MEMGRAPH_METRICS_TARGETS:-}"
MEMGRAPH_LOGS_VOLUME="${MEMGRAPH_LOGS_VOLUME:-memgraph_logs}"
MEMGRAPH_LOGS_PATH_IN_VOLUME="${MEMGRAPH_LOGS_PATH_IN_VOLUME:-/home/mg/memgraph-logs}"
MEMGRAPH_LOG_GLOB="${MEMGRAPH_LOG_GLOB:-${MEMGRAPH_LOGS_PATH_IN_VOLUME}/**/memgraph*.log}"
CLUSTER_ID="${CLUSTER_ID:-}"
CLUSTER_ENV="${CLUSTER_ENV:-ci-standalone-victoria}"

if [[ -z "${REMOTE_WRITE_URL}" || -z "${VLOGS_PUSH_URL}" ]]; then
  if [[ -z "${MONITORING_SERVER_HOST}" ]]; then
    cat >&2 <<EOF
error: missing endpoint configuration.

Provide either:
  - REMOTE_WRITE_URL and VLOGS_PUSH_URL
or:
  - MONITORING_SERVER_HOST (with optional REMOTE_WRITE_PORT/VLOGS_PORT)
EOF
    exit 2
  fi
  REMOTE_WRITE_URL="${REMOTE_WRITE_SCHEME}://${MONITORING_SERVER_HOST}:${REMOTE_WRITE_PORT}/insert/0/prometheus/api/v1/write"
  VLOGS_PUSH_URL="${VLOGS_SCHEME}://${MONITORING_SERVER_HOST}:${VLOGS_PORT}/insert/loki/api/v1/push"
fi

if [[ "${VLOGS_PUSH_URL}" == */insert/loki/api/v1/push ]]; then
  VLOGS_INSERT_ENDPOINT="${VLOGS_PUSH_URL%/insert/loki/api/v1/push}/insert"
elif [[ "${VLOGS_PUSH_URL}" == */insert ]]; then
  VLOGS_INSERT_ENDPOINT="${VLOGS_PUSH_URL}"
else
  echo "error: VLOGS_PUSH_URL must end with /insert/loki/api/v1/push or /insert" >&2
  exit 2
fi

COMPOSE_CMD=(docker compose)

MG_EXPORTER_CONFIG="${GENERATED_DIR}/mg-exporter.yaml"
VMAGENT_SCRAPE_CONFIG="${GENERATED_DIR}/vmagent-scrape.yml"
VECTOR_CONFIG="${GENERATED_DIR}/vector.yaml"

export MEMGRAPH_METRICS_HOST MEMGRAPH_METRICS_PORT

if [[ -n "${MEMGRAPH_METRICS_TARGETS}" ]]; then
  MG_EXPORTER_DEPLOYMENT_TYPE="HA"
  MEMGRAPH_INSTANCES_BLOCK=""
  target_index=1
  IFS=',' read -r -a metrics_targets <<< "${MEMGRAPH_METRICS_TARGETS}"
  for target in "${metrics_targets[@]}"; do
    trimmed_target="$(echo "${target}" | xargs)"
    if [[ -z "${trimmed_target}" ]]; then
      continue
    fi
    target_host="${trimmed_target%:*}"
    target_port="${trimmed_target##*:}"
    if [[ "${target_host}" == "${target_port}" ]]; then
      target_host="${trimmed_target}"
      target_port="9091"
    fi
    MEMGRAPH_INSTANCES_BLOCK+=$'  - name: memgraph'"${target_index}"$'\n'
    MEMGRAPH_INSTANCES_BLOCK+=$'    url: http://'"${target_host}"$'\n'
    MEMGRAPH_INSTANCES_BLOCK+=$'    port: '"${target_port}"$'\n'
    MEMGRAPH_INSTANCES_BLOCK+=$'    type: data_instance\n'
    target_index=$((target_index + 1))
  done
  if [[ -z "${MEMGRAPH_INSTANCES_BLOCK}" ]]; then
    echo "error: MEMGRAPH_METRICS_TARGETS was provided, but no valid targets were parsed." >&2
    exit 2
  fi
  export MEMGRAPH_INSTANCES_BLOCK
  envsubst < "${MANIFESTS_DIR}/mg-exporter-ha.yaml.tmpl" > "${MG_EXPORTER_CONFIG}"
else
  MG_EXPORTER_DEPLOYMENT_TYPE="standalone"
  envsubst < "${MANIFESTS_DIR}/mg-exporter-standalone.yaml.tmpl" > "${MG_EXPORTER_CONFIG}"
fi

export COMPOSE_PROJECT_NAME REMOTE_WRITE_URL VLOGS_INSERT_ENDPOINT MG_EXPORTER_DEPLOYMENT_TYPE
export MEMGRAPH_METRICS_HOST MEMGRAPH_METRICS_PORT MEMGRAPH_LOGS_VOLUME MEMGRAPH_LOGS_PATH_IN_VOLUME
export MEMGRAPH_LOG_GLOB CLUSTER_ID CLUSTER_ENV

envsubst < "${MANIFESTS_DIR}/vmagent-scrape.yml.tmpl" > "${VMAGENT_SCRAPE_CONFIG}"
envsubst < "${MANIFESTS_DIR}/vector.yaml.tmpl" > "${VECTOR_CONFIG}"

echo "==> Starting monitoring stack (${COMPOSE_PROJECT_NAME})"
"${COMPOSE_CMD[@]}" \
  --project-name "${COMPOSE_PROJECT_NAME}" \
  -f "${COMPOSE_FILE}" \
  up -d

echo
echo "Done."
echo "  VictoriaMetrics write: ${REMOTE_WRITE_URL}"
echo "  VictoriaLogs push:     ${VLOGS_PUSH_URL}"
echo "  Labels:                cluster_id='${CLUSTER_ID}', cluster_env='${CLUSTER_ENV}'"
echo "  Logs source volume:    ${MEMGRAPH_LOGS_VOLUME} (${MEMGRAPH_LOG_GLOB})"
if [[ -n "${MEMGRAPH_METRICS_TARGETS}" ]]; then
  echo "  Metrics targets:       ${MEMGRAPH_METRICS_TARGETS}"
else
  echo "  Metrics target:        ${MEMGRAPH_METRICS_HOST}:${MEMGRAPH_METRICS_PORT}"
fi
echo
echo "Generated files:"
echo "  ${MG_EXPORTER_CONFIG}"
echo "  ${VMAGENT_SCRAPE_CONFIG}"
echo "  ${VECTOR_CONFIG}"
echo
echo "Quick checks:"
echo "  ${COMPOSE_CMD[*]} --project-name ${COMPOSE_PROJECT_NAME} -f ${COMPOSE_FILE} ps"
echo "  ${COMPOSE_CMD[*]} --project-name ${COMPOSE_PROJECT_NAME} -f ${COMPOSE_FILE} logs --tail=100 vmagent"
echo "  ${COMPOSE_CMD[*]} --project-name ${COMPOSE_PROJECT_NAME} -f ${COMPOSE_FILE} logs --tail=100 vector"
