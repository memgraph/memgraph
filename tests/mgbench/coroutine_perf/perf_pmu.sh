#!/usr/bin/env bash
# PMU instructions/query + cycles/query for the coroutine pull path (perf-gate step 3.2).
# Measures the memgraph process over an EXACT number of queries (perf stat -p <pid> -- <hammer N>),
# so instructions/query = counted_instructions / N is exact and frequency-independent.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MG="${1:-build/memgraph}"
PORT="${2:-7799}"
N="${3:-100}"
DATA=/tmp/coro_pmu          # persistent across arms: graph loaded once
LOADER="$HOME/workspace/memgraph/tests/mgbench/coroutine_perf/p33_microbench.py"

EVENTS=instructions,cycles,branches,branch-misses,L1-dcache-loads,L1-dcache-load-misses
declare -A QUERIES=(
  [expand2]="MATCH (n:N)-[:R]->()-[:R]->(m) RETURN count(*)"
  [expand1]="MATCH (n:N)-[:R]->(m) RETURN count(*)"
)
SPLIT_FLAG='--query-coroutine-yield-ops=Aggregate,OrderBy'
ALL_FLAG='--query-coroutine-yield-ops=All'

LOADER="$HERE/minimal_load.py"
wait_port(){ for _ in $(seq 1 60); do (echo >"/dev/tcp/127.0.0.1/$PORT")2>/dev/null&&return 0;sleep 0.5;done;return 1;}

start_mg(){ # flag  --  fresh in-memory graph each call (graph does NOT persist with snapshot-off)
  rm -rf "$DATA"
  taskset -c 0-3 "$MG" --bolt-port="$PORT" --data-directory="$DATA" --telemetry-enabled=false \
     --log-level=WARNING --storage-snapshot-on-exit=false $1 >"/tmp/pmu_srv.log" 2>&1 &
  MGPID=$!; wait_port || { echo "FAILED to bind $PORT"; cat /tmp/pmu_srv.log; exit 1; }
}
stop_mg(){ kill "$MGPID" 2>/dev/null||true; wait "$MGPID" 2>/dev/null||true; sleep 1; }

for arm in off all split; do
  case $arm in off) FLAG="";; all) FLAG="$ALL_FLAG";; split) FLAG="$SPLIT_FLAG";; esac
  start_mg "$FLAG"
  # load graph INTO THIS process (it won't survive a restart), assert non-vacuity
  python3 "$LOADER" "$PORT" || { echo "LOAD FAILED arm=$arm"; stop_mg; exit 1; }
  # warm-up so JIT/caches/codepaths are hot before the measured window
  python3 "$HERE/hammer_fixed.py" "$PORT" "${QUERIES[expand2]}" 20 >/dev/null 2>&1
  for qn in expand2 expand1; do
    echo "=========== arm=$arm query=$qn  (N=$N) ==========="
    perf stat -e "$EVENTS" -p "$MGPID" -- \
      taskset -c 4 python3 "$HERE/hammer_fixed.py" "$PORT" "${QUERIES[$qn]}" "$N" 2>&1 \
      | grep -E 'instructions|cycles|branches|branch-misses|L1-dcache|completed|seconds' \
      | sed "s|^|  [$arm/$qn] |"
  done
  stop_mg
done
echo ">> NOTE divide the instructions/cycles/branches counts by N=$N for per-query."
