#!/usr/bin/env bash
# 3-way microbench for the coroutine-cursor pull path (coroutine cursors v2 / PR-13 perf gate).
#
# Runs the SAME binary three times on the chain + fanout microbenches and prints the per-query overhead
# of each coroutine arm relative to the synchronous baseline:
#
#   OFF    --query-coroutine-yield-ops=""                  every cursor Sync == master (the baseline)
#   ALL    --query-coroutine-yield-ops=All                 whole plan coroutine (worst-case boundary cost;
#                                                           the regression the split knob exists to avoid)
#   SPLIT  --query-coroutine-yield-ops=Aggregate,OrderBy   coroutine only from the root down to an
#                                                           Aggregate/OrderBy split point; the deep
#                                                           scan/expand region stays Sync
#
# The point of the run: ALL shows the per-pull coroutine machinery cost on pull-dominated reads; SPLIT
# shows that cost is avoided on those same reads (scan/expand stay Sync) while still exercising coroutine
# pull where it is effectively free (the low-row aggregate/orderby region). Plain serial Cypher only:
# NO enterprise license required.
#
# Usage:   ./run_ab.sh [/path/to/memgraph] [port]
#   env:   PYTHON=python3   (must have mgclient; e.g. `source tests/ve3/bin/activate`)
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

SPLIT_FLAG='--query-coroutine-yield-ops=Aggregate,OrderBy'
ALL_FLAG='--query-coroutine-yield-ops=All'

echo; echo "=== chain microbench ==="
run_one off   ""            p33_microbench.py
run_one all   "$ALL_FLAG"   p33_microbench.py
run_one split "$SPLIT_FLAG" p33_microbench.py
"$PY" - <<'PY'
import json
o=json.load(open('/tmp/p33_off.json')); a=json.load(open('/tmp/p33_all.json')); s=json.load(open('/tmp/p33_split.json'))
print(f"\n{'query':14s} {'OFF(ms)':>9s} {'ALL(ms)':>9s} {'ALL%':>7s} {'SPLIT(ms)':>10s} {'SPLIT%':>7s}")
for k in o:
    print(f"{k:14s} {o[k]:9.3f} {a[k]:9.3f} {100*(a[k]-o[k])/o[k]:+6.1f}% {s[k]:10.3f} {100*(s[k]-o[k])/o[k]:+6.1f}%")
PY

echo; echo "=== fanout microbench (component isolation: ~once-per-source InitEdges) ==="
run_one off   ""            p33_fanout.py
run_one all   "$ALL_FLAG"   p33_fanout.py
run_one split "$SPLIT_FLAG" p33_fanout.py
"$PY" - <<'PY'
import json
o=json.load(open('/tmp/fan_off.json'))['expand_fanout']
a=json.load(open('/tmp/fan_all.json'))['expand_fanout']
s=json.load(open('/tmp/fan_split.json'))['expand_fanout']
print(f"\nexpand_fanout  OFF={o:.3f}ms  ALL={a:.3f}ms ({100*(a-o)/o:+.1f}%)  SPLIT={s:.3f}ms ({100*(s-o)/o:+.1f}%)")
PY
