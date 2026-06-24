#!/usr/bin/env bash
# A/B microbench for the coroutine-cursor pull path (P3.3 perf gate; see ADRs/008_coroutine_cursors.md).
#
# Runs the SAME binary twice -- flag-OFF (PullLegacy == byte-identical to master) vs flag-ON
# (COROUTINE_CURSORS, the coroutine pull path) -- on the chain + fanout microbenches and prints the
# overhead. Plain serial queries only: NO enterprise license required.
#
# Usage:   ./run_ab.sh [/path/to/memgraph] [port]
#   env:   PYTHON=python3   (must have mgclient installed; e.g. `source tests/ve3/bin/activate`)
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MG="${1:-${MG_BINARY:-$HERE/../../../build/memgraph}}"
PORT="${2:-${PORT:-7799}}"
PY="${PYTHON:-python3}"

if [[ ! -x "$MG" ]]; then echo "memgraph binary not found/executable: $MG" >&2; exit 1; fi
echo "binary : $MG  (version: $("$MG" --version 2>/dev/null | grep -oiE '[0-9a-f]{9,}' | head -1))"
echo "port   : $PORT"

wait_port() { for _ in $(seq 1 60); do (echo > "/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && return 0; sleep 1; done; return 1; }

run_one() {  # tag  extra_flags  script
  local tag=$1 flag=$2 script=$3
  rm -rf "/tmp/cperf_$tag"
  # shellcheck disable=SC2086
  "$MG" --bolt-port="$PORT" --data-directory="/tmp/cperf_$tag" --telemetry-enabled=false \
        --log-level=WARNING --storage-snapshot-on-exit=false $flag >"/tmp/cperf_srv_$tag.log" 2>&1 &
  local pid=$!
  wait_port || { echo "memgraph[$tag] failed to bind $PORT (see /tmp/cperf_srv_$tag.log)"; kill "$pid" 2>/dev/null||true; exit 1; }
  "$PY" "$HERE/$script" "$PORT" "$tag"
  kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; sleep 1
}

echo; echo "=== chain microbench ==="
run_one off "" p33_microbench.py
run_one on  "--experimental-enabled=coroutine-cursors" p33_microbench.py
"$PY" - <<'PY'
import json
o=json.load(open('/tmp/p33_off.json')); n=json.load(open('/tmp/p33_on.json'))
print(f"\n{'query':14s} {'OFF(ms)':>9s} {'ON(ms)':>9s} {'ovhd%':>7s}")
for k in o: print(f"{k:14s} {o[k]:9.3f} {n[k]:9.3f} {100*(n[k]-o[k])/o[k]:+7.1f}%")
PY

echo; echo "=== fanout microbench (component isolation: ~once-per-source InitEdges) ==="
run_one off "" p33_fanout.py
run_one on  "--experimental-enabled=coroutine-cursors" p33_fanout.py
"$PY" - <<'PY'
import json
o=json.load(open('/tmp/fan_off.json'))['expand_fanout']; n=json.load(open('/tmp/fan_on.json'))['expand_fanout']
print(f"\nexpand_fanout  OFF={o:.3f}ms  ON={n:.3f}ms  ovhd={100*(n-o)/o:+.1f}%")
PY
