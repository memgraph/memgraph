#!/usr/bin/env bash
# Launch an ASan memgraph and run the parallel hammer; report crashes / ASan reports / parity.
# Re-verifies the c3.2 double-execution fix.
#
# Usage: run_asan_hammer.sh [bolt-port]
# Requires: MEMGRAPH_ORGANIZATION_NAME + MEMGRAPH_ENTERPRISE_LICENSE exported (enterprise feature).
#           An ASan build at ${MG_BIN:-./build/memgraph}. Python venv: tests/ve3.
#
# NOTE: pkill runs from inside this script file (its own command line is just `bash <script>`),
# so it does not self-match the memgraph pattern.
set -u
PORT="${1:-7699}"
MG_BIN="${MG_BIN:-./build/memgraph}"
HERE="$(cd "$(dirname "$0")" && pwd)"
DATA="${MG_DATA:-/tmp/mg_handoff_asan_data}"
LOG="${MG_LOG:-/tmp/mg_handoff_asan.log}"
ASAN_DIR="${ASAN_REPORT_DIR:-/tmp/mg_handoff_asan_report}"

: "${MEMGRAPH_ORGANIZATION_NAME:?export the licensee org name first (enterprise license)}"
: "${MEMGRAPH_ENTERPRISE_LICENSE:?export the enterprise license key first}"

pkill -9 -f "${MG_BIN} --data-directory" 2>/dev/null
sleep 3
rm -rf "$DATA"; mkdir -p "$DATA"; : > "$LOG"; rm -f "${ASAN_DIR}".*

export ASAN_OPTIONS="halt_on_error=0:detect_leaks=0:abort_on_error=0:quarantine_size_mb=1024:log_path=${ASAN_DIR}"
export ASAN_SYMBOLIZER_PATH="${ASAN_SYMBOLIZER_PATH:-/opt/toolchain-v7/bin/llvm-symbolizer}"

setsid "$MG_BIN" --data-directory="$DATA" --bolt-port="$PORT" \
  --monitoring-port=$((PORT + 100)) --metrics-port=$((PORT + 400)) \
  --log-level=WARNING > "$LOG" 2>&1 < /dev/null &
disown

for i in $(seq 1 120); do
  python3 -c "import socket;s=socket.socket();s.settimeout(1);s.connect(('127.0.0.1',${PORT}))" 2>/dev/null && { echo "PORT OPEN after ${i}s"; break; }
  sleep 1
done

# shellcheck disable=SC1091
source tests/ve3/bin/activate 2>/dev/null || true
python3 "${HERE}/parallel_hammer.py" "$PORT"
HAMMER_RC=$?

echo "crash=$(grep -acE 'DEADLYSIGNAL|AddressSanitizer:' "$LOG")"
echo "asan_reports=$(ls "${ASAN_DIR}".* 2>/dev/null | wc -l)"
echo "hammer_rc=${HAMMER_RC}  (0 = all parity OK)"
pkill -9 -f "${MG_BIN} --data-directory" 2>/dev/null
exit "$HAMMER_RC"
