#!/usr/bin/env bash
# Push a single log line to the remote VictoriaLogs monitoring server,
# carrying the HTTP URL of a Memgraph CI core-dump stack trace.
#
# This reuses the same VictoriaLogs Loki push flow and label convention as
# tools/ci/monitoring (see manifests/vector.yaml.tmpl): the log line is
# labelled app=memgraph / job=memgraph so it shows up alongside the regular
# Memgraph logs, at level=fatal so a crash is easy to alert/filter on.
#
# Best-effort: failures here must never fail the CI job.
set -euo pipefail

URL=""
MESSAGE=""
SIGNAL=""

print_usage() {
  cat <<EOF
Usage: ping_monitoring.sh --url STACK_TRACE_URL [--message TEXT]

Pushes one log line to the VictoriaLogs Loki endpoint.

Options:
  --url URL         HTTP URL of the uploaded stack trace (required)
  --message TEXT    Override the log message (default mentions the URL)
  --signal N        Signal that killed Memgraph (adds signal/exit_status labels)
  -h, --help        Show this help

Environment:
  MONITORING_HOST       Host/IP of the monitoring server (required)
  VLOGS_PORT            VictoriaLogs port (default: 30454)
  VLOGS_SCHEME          http|https (default: http)
  VLOGS_PUSH_URL        Full Loki push URL; overrides HOST/PORT/SCHEME
  MONITORING_USERNAME   Basic-auth user (optional)
  MONITORING_PASSWORD   Basic-auth password (optional)
  CLUSTER_ID            cluster_id label (default: empty)
  CLUSTER_ENV           cluster_env label (default: ci)
  SERVICE_NAME          service_name label (default: memgraph)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --url)     URL="$2"; shift 2 ;;
    --message) MESSAGE="$2"; shift 2 ;;
    --signal)  SIGNAL="$2"; shift 2 ;;
    -h|--help) print_usage; exit 0 ;;
    *) echo "Error: unknown option '$1'" >&2; print_usage >&2; exit 1 ;;
  esac
done

if [[ -z "$URL" ]]; then
  echo "Error: --url is required" >&2
  exit 1
fi

MONITORING_HOST="${MONITORING_HOST:-}"
VLOGS_PORT="${VLOGS_PORT:-30454}"
VLOGS_SCHEME="${VLOGS_SCHEME:-http}"
VLOGS_PUSH_URL="${VLOGS_PUSH_URL:-}"
CLUSTER_ID="${CLUSTER_ID:-}"
CLUSTER_ENV="${CLUSTER_ENV:-ci}"
SERVICE_NAME="${SERVICE_NAME:-memgraph}"
MONITORING_USERNAME="${MONITORING_USERNAME:-}"
MONITORING_PASSWORD="${MONITORING_PASSWORD:-}"

if [[ -z "$VLOGS_PUSH_URL" ]]; then
  if [[ -z "$MONITORING_HOST" ]]; then
    echo "monitoring host not set — skipping ping."
    exit 0
  fi
  VLOGS_PUSH_URL="${VLOGS_SCHEME}://${MONITORING_HOST}:${VLOGS_PORT}/insert/loki/api/v1/push"
fi

# Derive a human signal name and the conventional shell exit status (128 + signal).
signal_name() {
  case "$1" in
    4) echo "SIGILL" ;; 6) echo "SIGABRT" ;; 7) echo "SIGBUS" ;;
    8) echo "SIGFPE" ;; 9) echo "SIGKILL" ;; 11) echo "SIGSEGV" ;;
    15) echo "SIGTERM" ;; *) echo "SIG${1}" ;;
  esac
}
SIGNAL_NAME=""
EXIT_STATUS=""
if [[ -n "$SIGNAL" ]]; then
  SIGNAL_NAME="$(signal_name "$SIGNAL")"
  EXIT_STATUS="$((128 + SIGNAL))"
fi

if [[ -z "$MESSAGE" ]]; then
  if [[ -n "$SIGNAL" ]]; then
    MESSAGE="Memgraph core dump captured in CI (signal ${SIGNAL} ${SIGNAL_NAME}, exit status ${EXIT_STATUS}) — stack trace: ${URL}"
  else
    MESSAGE="Memgraph core dump captured in CI — stack trace: ${URL}"
  fi
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "Warning: curl not available — skipping monitoring ping." >&2
  exit 0
fi

# Loki push API expects nanosecond unix timestamps as strings.
ts_ns="$(date +%s)000000000"

# Build the JSON payload with python3 so labels/message are escaped safely.
payload="$(CLUSTER_ID="$CLUSTER_ID" CLUSTER_ENV="$CLUSTER_ENV" \
  SERVICE_NAME="$SERVICE_NAME" STACK_TRACE_URL="$URL" \
  SIGNAL="$SIGNAL" SIGNAL_NAME="$SIGNAL_NAME" EXIT_STATUS="$EXIT_STATUS" \
  MSG="$MESSAGE" TS_NS="$ts_ns" python3 - <<'PY'
import json, os
stream = {
    "app": "memgraph",
    "job": "memgraph",
    "role": "ci",
    "namespace": "ci",
    "level": "fatal",
    "cluster_id": os.environ.get("CLUSTER_ID", ""),
    "service_name": os.environ.get("SERVICE_NAME", "memgraph"),
    "cluster_env": os.environ.get("CLUSTER_ENV", "ci"),
    "stack_trace_url": os.environ.get("STACK_TRACE_URL", ""),
}
# Only attach crash-signal labels when we actually know the signal.
if os.environ.get("SIGNAL"):
    stream["signal"] = os.environ["SIGNAL"]
    stream["signal_name"] = os.environ.get("SIGNAL_NAME", "")
    stream["exit_status"] = os.environ.get("EXIT_STATUS", "")
payload = {"streams": [{"stream": stream,
                        "values": [[os.environ["TS_NS"], os.environ["MSG"]]]}]}
print(json.dumps(payload))
PY
)"

auth_args=()
if [[ -n "$MONITORING_USERNAME" && -n "$MONITORING_PASSWORD" ]]; then
  auth_args=(--user "${MONITORING_USERNAME}:${MONITORING_PASSWORD}")
fi

echo "Pinging monitoring server: $VLOGS_PUSH_URL"
http_code="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 15 \
  "${auth_args[@]}" \
  -H 'Content-Type: application/json' \
  -X POST "$VLOGS_PUSH_URL" \
  --data-binary "$payload" || echo "000")"

if [[ "$http_code" =~ ^2 ]]; then
  echo "Monitoring ping accepted (HTTP $http_code)."
else
  echo "Warning: monitoring ping returned HTTP $http_code (continuing)." >&2
fi
