#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(
  cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1
  pwd -P
)"
GENERATED_DIR="${SCRIPT_DIR}/generated"
MANIFESTS_DIR="${SCRIPT_DIR}/manifests"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
HOST_NETWORK_COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.host-network.yml"
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
MEMGRAPH_LOG_WS_PORT="${MEMGRAPH_LOG_WS_PORT:-7444}"
MEMGRAPH_LOG_WS_TARGETS="${MEMGRAPH_LOG_WS_TARGETS:-}"
CLUSTER_ID="${CLUSTER_ID:-}"
CLUSTER_ENV="${CLUSTER_ENV:-ci-standalone-victoria}"
SERVICE_NAME="${SERVICE_NAME:-memgraph}"
MONITORING_USERNAME="${MONITORING_USERNAME:-${CI_MONITORING_USER:-}}"
MONITORING_PASSWORD="${MONITORING_PASSWORD:-${CI_MONITORING_PASSWORD:-}}"
MONITORING_USE_HOST_NETWORK="${MONITORING_USE_HOST_NETWORK:-false}"

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
COMPOSE_FILES=(-f "${COMPOSE_FILE}")
if [[ "${MONITORING_USE_HOST_NETWORK}" == "true" ]]; then
  COMPOSE_FILES+=(-f "${HOST_NETWORK_COMPOSE_FILE}")
  MG_EXPORTER_SCRAPE_TARGET="127.0.0.1:9115"
else
  MG_EXPORTER_SCRAPE_TARGET="mg-exporter:9115"
fi

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

if [[ -z "${MEMGRAPH_LOG_WS_TARGETS}" ]]; then
  if [[ -n "${MEMGRAPH_METRICS_TARGETS}" ]]; then
    MEMGRAPH_LOG_WS_TARGETS=""
    IFS=',' read -r -a metrics_targets_for_ws <<< "${MEMGRAPH_METRICS_TARGETS}"
    for metrics_target in "${metrics_targets_for_ws[@]}"; do
      trimmed_metrics_target="$(echo "${metrics_target}" | xargs)"
      [[ -z "${trimmed_metrics_target}" ]] && continue
      metrics_host="${trimmed_metrics_target%:*}"
      metrics_port="${trimmed_metrics_target##*:}"
      if [[ "${metrics_host}" == "${metrics_port}" ]]; then
        metrics_host="${trimmed_metrics_target}"
      fi
      if [[ -n "${MEMGRAPH_LOG_WS_TARGETS}" ]]; then
        MEMGRAPH_LOG_WS_TARGETS+=","
      fi
      MEMGRAPH_LOG_WS_TARGETS+="${metrics_host}:${MEMGRAPH_LOG_WS_PORT}"
    done
  else
    MEMGRAPH_LOG_WS_TARGETS="${MEMGRAPH_METRICS_HOST}:${MEMGRAPH_LOG_WS_PORT}"
  fi
fi

if [[ -n "${MONITORING_USERNAME}" || -n "${MONITORING_PASSWORD}" ]]; then
  if [[ -z "${MONITORING_USERNAME}" || -z "${MONITORING_PASSWORD}" ]]; then
    echo "error: both MONITORING_USERNAME and MONITORING_PASSWORD are required when using basic auth." >&2
    exit 2
  fi
  VMAGENT_AUTH_ARGS="-remoteWrite.basicAuth.username=${MONITORING_USERNAME} -remoteWrite.basicAuth.password=${MONITORING_PASSWORD}"
  VLOGS_AUTH_BLOCK=$'    auth:\n      strategy: basic\n      user: "'"${MONITORING_USERNAME}"$'"\n      password: "'"${MONITORING_PASSWORD}"$'"'
else
  VMAGENT_AUTH_ARGS=""
  VLOGS_AUTH_BLOCK=""
fi

VECTOR_SOURCES_BLOCK=""
VECTOR_PREPROCESS_BLOCK=""
VECTOR_ENRICH_INPUTS=""
ws_index=1
IFS=',' read -r -a ws_targets <<< "${MEMGRAPH_LOG_WS_TARGETS}"
for target in "${ws_targets[@]}"; do
  trimmed_target="$(echo "${target}" | xargs)"
  if [[ -z "${trimmed_target}" ]]; then
    continue
  fi
  ws_url=""
  instance_label=""
  if [[ "${trimmed_target}" =~ ^wss?:// ]]; then
    ws_url="${trimmed_target}"
    target_without_scheme="${trimmed_target#*://}"
    instance_label="${target_without_scheme%%/*}"
  else
    target_host="${trimmed_target%:*}"
    target_port="${trimmed_target##*:}"
    if [[ "${target_host}" == "${target_port}" ]]; then
      target_host="${trimmed_target}"
      target_port="${MEMGRAPH_LOG_WS_PORT}"
    fi
    ws_url="ws://${target_host}:${target_port}"
    instance_label="${target_host}"
  fi

  source_name="memgraph_logs_${ws_index}"
  preprocess_name="annotate_${source_name}"
  VECTOR_SOURCES_BLOCK+="  ${source_name}:"$'\n'
  VECTOR_SOURCES_BLOCK+="    type: websocket"$'\n'
  VECTOR_SOURCES_BLOCK+="    uri: \"${ws_url}\""$'\n'

  VECTOR_PREPROCESS_BLOCK+="  ${preprocess_name}:"$'\n'
  VECTOR_PREPROCESS_BLOCK+="    type: remap"$'\n'
  VECTOR_PREPROCESS_BLOCK+="    inputs: [${source_name}]"$'\n'
  VECTOR_PREPROCESS_BLOCK+="    source: |"$'\n'
  VECTOR_PREPROCESS_BLOCK+="      .pod = \"${instance_label}\""$'\n'
  VECTOR_PREPROCESS_BLOCK+="      .source = \"${ws_url}\""$'\n'

  if [[ -n "${VECTOR_ENRICH_INPUTS}" ]]; then
    VECTOR_ENRICH_INPUTS+=", "
  fi
  VECTOR_ENRICH_INPUTS+="${preprocess_name}"
  ws_index=$((ws_index + 1))
done

if [[ -z "${VECTOR_ENRICH_INPUTS}" ]]; then
  echo "error: no valid MEMGRAPH_LOG_WS_TARGETS were parsed." >&2
  exit 2
fi

export COMPOSE_PROJECT_NAME REMOTE_WRITE_URL VLOGS_INSERT_ENDPOINT MG_EXPORTER_DEPLOYMENT_TYPE
export MEMGRAPH_METRICS_HOST MEMGRAPH_METRICS_PORT CLUSTER_ID CLUSTER_ENV SERVICE_NAME
export VECTOR_SOURCES_BLOCK VECTOR_PREPROCESS_BLOCK VECTOR_ENRICH_INPUTS
export VMAGENT_AUTH_ARGS VLOGS_AUTH_BLOCK MG_EXPORTER_SCRAPE_TARGET

envsubst < "${MANIFESTS_DIR}/vmagent-scrape.yml.tmpl" > "${VMAGENT_SCRAPE_CONFIG}"
envsubst < "${MANIFESTS_DIR}/vector.yaml.tmpl" > "${VECTOR_CONFIG}"

echo "==> Starting monitoring stack (${COMPOSE_PROJECT_NAME})"
"${COMPOSE_CMD[@]}" \
  --project-name "${COMPOSE_PROJECT_NAME}" \
  "${COMPOSE_FILES[@]}" \
  up -d

echo
echo "Done."
echo "  VictoriaMetrics write: ${REMOTE_WRITE_URL}"
echo "  VictoriaLogs push:     ${VLOGS_PUSH_URL}"
echo "  Labels:                cluster_id='${CLUSTER_ID}', service_name='${SERVICE_NAME}', cluster_env='${CLUSTER_ENV}'"
echo "  Logs websocket targets:${MEMGRAPH_LOG_WS_TARGETS}"
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
echo "  ${COMPOSE_CMD[*]} --project-name ${COMPOSE_PROJECT_NAME} ${COMPOSE_FILES[*]} ps"
echo "  ${COMPOSE_CMD[*]} --project-name ${COMPOSE_PROJECT_NAME} ${COMPOSE_FILES[*]} logs --tail=100 vmagent"
echo "  ${COMPOSE_CMD[*]} --project-name ${COMPOSE_PROJECT_NAME} ${COMPOSE_FILES[*]} logs --tail=100 vector"
