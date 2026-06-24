#!/usr/bin/env bash
# Kernel A/B by HARDWARE COUNTERS (the rigorous P3.3 measurement; reproduces EXP-1/2/3/11).
#
# For each query shape, runs the SAME binary flag-OFF (PullLegacy == master) vs flag-ON
# (--experimental-enabled=coroutine-cursors) under `perf stat`, counting completed queries, and
# reports instructions/query + overhead. Deterministic instruction counts are the trustworthy signal
# (wall-clock A/B in run_ab.sh is noisy on short queries). Requires a real PMU + perf access:
#   sudo sysctl kernel.perf_event_paranoid=1   (ideally also: performance governor, turbo off)
#
# Usage:  ./kernel_counter_ab.sh [/path/to/memgraph] [port]
#   env:  SECS=15  PORT=7799  PYTHON=<python-with-mgclient>  (default: repo tests/ve3)
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
MG="${1:-${MG_BINARY:-$ROOT/build/memgraph}}"
PORT="${2:-${PORT:-7799}}"
SECS="${SECS:-15}"
PY="${PYTHON:-$ROOT/tests/ve3/bin/python}"
SCR="${SCR:-/tmp/cc_perf}"; mkdir -p "$SCR"
[[ -x "$MG" ]] || { echo "memgraph binary not found: $MG" >&2; exit 1; }

# name  loader            query
SHAPES=(
  "chain   p33_microbench.py  MATCH (n:N)-[:R]->(m) RETURN count(*)"
  "fanout  p33_fanout.py      MATCH (s:S)-[:R]->(t) RETURN count(*)"
  "varexp  p33_microbench.py  MATCH (n:N)-[:R*1..3]->(m) RETURN count(*)"
)

run() {  # name loader query mode
  local name=$1 loader=$2 query=$3 mode=$4 flag=""
  [[ "$mode" == on ]] && flag="--experimental-enabled=coroutine-cursors"
  local dir="$SCR/db_${name}_${mode}"; rm -rf "$dir"
  # shellcheck disable=SC2086
  "$MG" --bolt-port="$PORT" --data-directory="$dir" --telemetry-enabled=false \
        --log-level=ERROR --storage-snapshot-on-exit=false $flag >"$SCR/srv_${name}_${mode}.log" 2>&1 &
  local pid=$!
  for _ in $(seq 1 60); do (echo > "/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && break; sleep 1; done
  "$PY" "$HERE/$loader" "$PORT" "ld_${name}_${mode}" >/dev/null 2>&1 || true
  perf stat -e instructions,cycles,branch-misses -p "$pid" -- \
    "$PY" "$HERE/hammer_count.py" "$PORT" "$query" "$SECS" 2>"$SCR/perf_${name}_${mode}.txt"
  kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; sleep 1
}

printf "%-7s %12s %12s %8s\n" shape "OFF Mi/q" "ON Mi/q" "ovhd%"
for s in "${SHAPES[@]}"; do
  read -r name loader query <<<"$s"
  run "$name" "$loader" "$query" off
  run "$name" "$loader" "$query" on
  "$PY" - "$SCR/perf_${name}_off.txt" "$SCR/perf_${name}_on.txt" "$name" <<'PY'
import re, sys
def ipq(p):
    t=open(p).read()
    q=int(re.search(r"QUERIES=(\d+)",t).group(1))
    ins=int(re.search(r"([\d.]+)\s+instructions",t).group(1).replace(".",""))
    return ins/q
o=ipq(sys.argv[1]); n=ipq(sys.argv[2])
print(f"{sys.argv[3]:<7} {o/1e6:12.2f} {n/1e6:12.2f} {100*(n-o)/o:+8.1f}")
PY
done
