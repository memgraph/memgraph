#!/usr/bin/env bash
# c3.4 root-cause probe — exact-N PMU for the PARALLEL agg query, BLOCK vs PARK.
#
# This is the measurement that pinned the Gate 1 throughput regression to its cause: the park drive
# costs ~+29% instructions / +40% branches PER PARALLEL QUERY (see ../RESULTS_c34.md §3.3). Because
# it counts instructions over an EXACT query count on a SINGLE client, the per-query number is
# frequency- and timing-independent (immune to turbo/thermal wander and to concurrency noise).
#
# Usage:   pmu_parallel_park_vs_block.sh [port] [N]
#   env:   MEMGRAPH_ORGANIZATION_NAME + MEMGRAPH_ENTERPRISE_LICENSE  (enterprise; parallel plan)
#          MG_BIN=./build/memgraph (default).  Python venv tests/ve3 (mgclient).
#          perf usable: sudo sysctl kernel.perf_event_paranoid=1
# Run from the repo root.
set -uo pipefail
MG="${MG_BIN:-./build/memgraph}"
PORT="${1:-7715}"
N="${2:-200}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HF="$HERE/../../hammer_fixed.py"          # exact-N single-client hammer (repo)
Q="USING PARALLEL EXECUTION 4 MATCH (n:N) RETURN count(n), sum(n.p)"
EVENTS=instructions,cycles,branches,branch-misses,context-switches
DATASET=400000

for v in MEMGRAPH_ORGANIZATION_NAME MEMGRAPH_ENTERPRISE_LICENSE; do
  [ -n "${!v:-}" ] || { echo "ERROR: export $v first (enterprise feature)"; exit 1; }
done
[ -x "$MG" ] || { echo "memgraph binary not found/executable: $MG"; exit 1; }

wait_port(){ for _ in $(seq 1 60); do (echo >"/dev/tcp/127.0.0.1/$PORT")2>/dev/null&&return 0;sleep 0.5;done;return 1;}

for arm in block park; do
  case $arm in
    block) KNOB="";;                                              # sync coordinator -> WaitOrSteal blocks
    park)  KNOB="Aggregate,OrderBy,Accumulate,Distinct,HashJoin";;# coroutine coordinator parks
  esac
  rm -rf /tmp/mg_pmp
  # cores 2-7,10-15: physical cores 2-7, leaving 0-1 for OS/neighbour (see RESULTS_c34.md §1)
  taskset -c 2-7,10-15 "$MG" --bolt-port="$PORT" --data-directory=/tmp/mg_pmp --telemetry-enabled=false \
    --log-level=WARNING --storage-snapshot-on-exit=false --query-coroutine-yield-ops="$KNOB" \
    >/tmp/mg_pmp.log 2>&1 &
  MGPID=$!
  wait_port || { echo "arm=$arm FAILED to bind $PORT"; cat /tmp/mg_pmp.log; kill "$MGPID"; exit 1; }
  python3 - "$PORT" "$DATASET" <<'PY'
import sys, mgclient
port=int(sys.argv[1]); target=int(sys.argv[2])
c=mgclient.connect(host="127.0.0.1",port=port); c.autocommit=True; cur=c.cursor()
def run(q): cur.execute(q); return cur.fetchall()
made=0
while made<target:
    b=min(50000,target-made); run(f"UNWIND range({made+1},{made+b}) AS i CREATE (:N {{p:i, g:i%7}})"); made+=b
print("loaded", run("MATCH (n:N) RETURN count(*)")[0][0])
PY
  python3 "$HF" "$PORT" "$Q" 20 >/dev/null 2>&1   # warm
  echo "=================== arm=$arm  N=$N  (knob='$KNOB') ==================="
  perf stat -e "$EVENTS" -p "$MGPID" -- taskset -c 4 python3 "$HF" "$PORT" "$Q" "$N" 2>&1 \
    | grep -E 'instructions|cycles|branch|context-switches|completed|seconds' | sed "s|^|  [$arm] |"
  kill "$MGPID" 2>/dev/null; wait "$MGPID" 2>/dev/null; sleep 2
done
echo ">> per-query = counter / N=$N   (expect PARK instructions/query ~+29% vs BLOCK)"
