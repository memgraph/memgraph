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
docker compose version >/dev/null 2>&1 || {
  echo "error: Docker Compose plugin was not found. Install and use 'docker compose'." >&2
  exit 127
}
COMPOSE_CMD=(docker compose)
COMPOSE_FILES=(-f "${COMPOSE_FILE}")
if [[ "${MONITORING_USE_HOST_NETWORK}" == "true" ]]; then
  COMPOSE_FILES+=(-f "${HOST_NETWORK_COMPOSE_FILE}")
fi

export COMPOSE_PROJECT_NAME REMOTE_WRITE_URL MG_EXPORTER_DEPLOYMENT_TYPE VMAGENT_AUTH_ARGS

echo "==> Stopping monitoring stack (${COMPOSE_PROJECT_NAME})"
"${COMPOSE_CMD[@]}" \
  --project-name "${COMPOSE_PROJECT_NAME}" \
  "${COMPOSE_FILES[@]}" \
  down --remove-orphans --volumes

echo "Done."
