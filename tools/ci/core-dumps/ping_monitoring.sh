#!/usr/bin/env bash
# Push a single log line to the remote VictoriaLogs monitoring server,
# carrying the s3:// URI of a Memgraph CI core-dump stack trace.
#
# This reuses the same VictoriaLogs Loki push flow and label convention as
# tools/ci/monitoring (see manifests/vector.yaml.tmpl): the log line is
# labelled app=memgraph / job=memgraph so it shows up alongside the regular
# Memgraph logs, at level=fatal so a crash is easy to alert/filter on.
#
# Best-effort: failures here must never fail the CI job.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"

URL=""
MESSAGE=""
SIGNAL=""
CORE_URL=""
BINARIES_URL=""

print_usage() {
  cat <<EOF
Usage: ping_monitoring.sh --url STACK_TRACE_URL [--message TEXT]

Pushes one log line to the VictoriaLogs Loki endpoint.

Options:
  --url URL         s3:// URI of the uploaded stack trace (required)
  --message TEXT    Override the log message (default mentions the URL)
  --signal N        Signal that killed Memgraph (adds signal/exit_status labels)
  --core-url URL    Core dump URL (adds a core_url label)
  --binaries-url URL  Build-artifacts URL (adds a binaries_url label)
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
    --url)          URL="$2"; shift 2 ;;
    --message)      MESSAGE="$2"; shift 2 ;;
    --signal)       SIGNAL="$2"; shift 2 ;;
    --core-url)     CORE_URL="$2"; shift 2 ;;
    --binaries-url) BINARIES_URL="$2"; shift 2 ;;
    -h|--help)      print_usage; exit 0 ;;
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
  # Guard the arithmetic: a non-numeric --signal would otherwise abort the
  # script. Drop a bad value and carry on without signal labels.
  if [[ "$SIGNAL" =~ ^[0-9]+$ ]]; then
    SIGNAL_NAME="$(signal_name "$SIGNAL")"
    EXIT_STATUS="$((128 + SIGNAL))"
  else
    echo "Warning: --signal '$SIGNAL' is not numeric; omitting signal labels." >&2
    SIGNAL=""
  fi
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

# Loki push API expects nanosecond unix timestamps as strings. Use real
# nanosecond precision (date +%s%N) so back-to-back pings in a per-trace loop
# get distinct timestamps and Loki/VictoriaLogs doesn't dedupe them; fall back
# to second precision if %N is unsupported.
ts_ns="$(date +%s%N 2>/dev/null)"
[[ "$ts_ns" =~ ^[0-9]+$ ]] || ts_ns="$(date +%s)000000000"

# Build the JSON payload with python3 (build_loki_payload.py) so labels/message
# are escaped safely. Fields are passed via the environment, not the argv/shell.
payload="$(CLUSTER_ID="$CLUSTER_ID" CLUSTER_ENV="$CLUSTER_ENV" \
  SERVICE_NAME="$SERVICE_NAME" STACK_TRACE_URL="$URL" \
  SIGNAL="$SIGNAL" SIGNAL_NAME="$SIGNAL_NAME" EXIT_STATUS="$EXIT_STATUS" \
  CORE_URL="$CORE_URL" BINARIES_URL="$BINARIES_URL" \
  MSG="$MESSAGE" TS_NS="$ts_ns" python3 "$SCRIPT_DIR/build_loki_payload.py")"

# Pass basic-auth creds to curl via a config read from stdin, never as a
# --user argument (which would expose the password in ps / /proc/<pid>/cmdline).
# Backslashes and double quotes are escaped for curl's config-file quoting.
auth_config=""
if [[ -n "$MONITORING_USERNAME" && -n "$MONITORING_PASSWORD" ]]; then
  esc_user="${MONITORING_USERNAME//\\/\\\\}"; esc_user="${esc_user//\"/\\\"}"
  esc_pass="${MONITORING_PASSWORD//\\/\\\\}"; esc_pass="${esc_pass//\"/\\\"}"
  auth_config="user = \"${esc_user}:${esc_pass}\""
fi

# Redact the monitoring host from the log line so the internal address isn't
# disclosed in (potentially public) CI logs.
if [[ -n "${MONITORING_HOST:-}" ]]; then
  echo "Pinging monitoring server: ${VLOGS_PUSH_URL/${MONITORING_HOST}/***}"
else
  echo "Pinging monitoring server (endpoint redacted)."
fi
http_code="$(printf '%s' "$auth_config" | curl -sS -o /dev/null -w '%{http_code}' --max-time 15 \
  --config - \
  -H 'Content-Type: application/json' \
  -X POST "$VLOGS_PUSH_URL" \
  --data-binary "$payload" || echo "000")"

if [[ "$http_code" =~ ^2 ]]; then
  echo "Monitoring ping accepted (HTTP $http_code)."
else
  echo "Warning: monitoring ping returned HTTP $http_code (continuing)." >&2
fi
