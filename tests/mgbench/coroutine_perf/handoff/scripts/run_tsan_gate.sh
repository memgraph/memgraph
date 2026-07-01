#!/usr/bin/env bash
# c3.4 §6.1: run the parallel hammer against a TSan build; report data races.
# Assumes a TSan memgraph at ${MG_BIN:-./build/memgraph} (built via build.sh with the TSan profile).
#
# Usage: run_tsan_gate.sh [bolt-port]
# Requires: MEMGRAPH_ORGANIZATION_NAME + MEMGRAPH_ENTERPRISE_LICENSE exported.
#
# On a fast/bare-metal box the TSan server serves normally. On a slow/virtualized box the Bolt
# handshake may starve under TSan overhead — that is the reason c3.4 needs real hardware. If it
# starves anyway, raise DRIVER_TIMEOUT and lower NODES below.
set -u
PORT="${1:-7710}"
MG_BIN="${MG_BIN:-./build/memgraph}"
HERE="$(cd "$(dirname "$0")" && pwd)"
DATA="${MG_DATA:-/tmp/mg_handoff_tsan_data}"
LOG="${MG_LOG:-/tmp/mg_handoff_tsan.log}"
NODES="${NODES:-100000}"

: "${MEMGRAPH_ORGANIZATION_NAME:?export the licensee org name first}"
: "${MEMGRAPH_ENTERPRISE_LICENSE:?export the enterprise license key first}"

pkill -9 -f "${MG_BIN} --data-directory" 2>/dev/null
sleep 4
rm -rf "$DATA"; mkdir -p "$DATA"; : > "$LOG"

export TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:history_size=4"
setsid "$MG_BIN" --data-directory="$DATA" --bolt-port="$PORT" \
  --monitoring-port=$((PORT + 100)) --metrics-port=$((PORT + 400)) \
  --log-level=WARNING > "$LOG" 2>&1 < /dev/null &
disown

for i in $(seq 1 180); do
  python3 -c "import socket;s=socket.socket();s.settimeout(1);s.connect(('127.0.0.1',${PORT}))" 2>/dev/null && { echo "PORT OPEN after ${i}s"; break; }
  sleep 1
done

# shellcheck disable=SC1091
source tests/ve3/bin/activate 2>/dev/null || true
# Drive at parallel degree 2 and 4 (fewer nodes than the ASan hammer — TSan is slow).
python3 "${HERE}/parallel_hammer.py" "$PORT" --nodes "$NODES" --par 2 --seq 10 --threads 4 --iters 5
RC2=$?
python3 "${HERE}/parallel_hammer.py" "$PORT" --nodes "$NODES" --par 4 --seq 10 --threads 4 --iters 5
RC4=$?

RACES="$(grep -acE 'WARNING: ThreadSanitizer' "$LOG")"
echo "TSAN_RACES=${RACES}"
echo "hammer_rc par2=${RC2} par4=${RC4}"
grep -aE 'WARNING: ThreadSanitizer|SUMMARY: ThreadSanitizer' "$LOG" | head -12
pkill -9 -f "${MG_BIN} --data-directory" 2>/dev/null
# pass = 0 races and both hammers parity-clean
[ "$RACES" = "0" ] && [ "$RC2" = "0" ] && [ "$RC4" = "0" ] && exit 0 || exit 1
