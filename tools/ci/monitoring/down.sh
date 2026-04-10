#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(
  cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1
  pwd -P
)"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-memgraph-monitoring}"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
HOST_NETWORK_COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.host-network.yml"
MONITORING_USE_HOST_NETWORK="${MONITORING_USE_HOST_NETWORK:-false}"
REMOTE_WRITE_URL="${REMOTE_WRITE_URL:-http://127.0.0.1:8428/api/v1/write}"
MG_EXPORTER_DEPLOYMENT_TYPE="${MG_EXPORTER_DEPLOYMENT_TYPE:-standalone}"
VMAGENT_AUTH_ARGS="${VMAGENT_AUTH_ARGS:-}"
MONITORING_DEBUG_DIR="${MONITORING_DEBUG_DIR:-${SCRIPT_DIR}/../../../build/monitoring}"
docker compose version >/dev/null 2>&1 || {
  echo "error: Docker Compose plugin was not found. Install and use 'docker compose'." >&2
  exit 127
}
COMPOSE_CMD=(docker compose)
COMPOSE_FILES=(-f "${COMPOSE_FILE}")
if [[ "${MONITORING_USE_HOST_NETWORK}" == "true" ]]; then
  COMPOSE_FILES=(-f "${HOST_NETWORK_COMPOSE_FILE}")
fi

export COMPOSE_PROJECT_NAME REMOTE_WRITE_URL MG_EXPORTER_DEPLOYMENT_TYPE VMAGENT_AUTH_ARGS

collect_monitoring_debug() {
  local ts run_dir
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  run_dir="${MONITORING_DEBUG_DIR}/${COMPOSE_PROJECT_NAME}-${ts}"
  mkdir -p "${run_dir}"

  "${COMPOSE_CMD[@]}" --project-name "${COMPOSE_PROJECT_NAME}" "${COMPOSE_FILES[@]}" ps > "${run_dir}/compose-ps.txt" 2>&1 || true
  docker ps -a > "${run_dir}/docker-ps-a.txt" 2>&1 || true

  for svc in vmagent vector mg-exporter; do
    "${COMPOSE_CMD[@]}" --project-name "${COMPOSE_PROJECT_NAME}" "${COMPOSE_FILES[@]}" logs --timestamps --no-color "$svc" > "${run_dir}/${svc}.log" 2>&1 || true
  done

  if [[ -f "${SCRIPT_DIR}/generated/mg-exporter.yaml" ]]; then
    cp "${SCRIPT_DIR}/generated/mg-exporter.yaml" "${run_dir}/mg-exporter.yaml"
  fi
  if [[ -f "${SCRIPT_DIR}/generated/vmagent-scrape.yml" ]]; then
    cp "${SCRIPT_DIR}/generated/vmagent-scrape.yml" "${run_dir}/vmagent-scrape.yml"
  fi
  if [[ -f "${SCRIPT_DIR}/generated/vector.yaml" ]]; then
    sed -E \
      -e 's/(password:\s*).*/\1"***REDACTED***"/' \
      -e 's/(user:\s*).*/\1"***REDACTED***"/' \
      "${SCRIPT_DIR}/generated/vector.yaml" > "${run_dir}/vector.redacted.yaml"
  fi

  echo "Monitoring diagnostics saved to ${run_dir}"
}

echo "==> Stopping monitoring stack (${COMPOSE_PROJECT_NAME})"
collect_monitoring_debug || true
"${COMPOSE_CMD[@]}" \
  --project-name "${COMPOSE_PROJECT_NAME}" \
  "${COMPOSE_FILES[@]}" \
  down --remove-orphans --volumes

echo "Done."
