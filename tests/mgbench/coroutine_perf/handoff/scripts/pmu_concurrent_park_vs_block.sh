#!/usr/bin/env bash
# c3.5a re-gate root-cause probe — CONCURRENT-saturation PMU for the PARALLEL agg query, BLOCK vs PARK.
#
# Companion to pmu_parallel_park_vs_block.sh (which is SINGLE-client). c3.5a's steal-then-park only
# changes behaviour under SATURATION: when pool workers are busy with other queries this query's
# branches stay IDLE, so the coordinator steals+runs them inline (== BLOCK) instead of parking. The
# single-client probe measures the low-load path where the coordinator still parks BY DESIGN, so it
# cannot show whether the fix engaged. This probe drives C concurrent clients to saturate the pool,
# then reads server-side instructions/query — which is frequency-independent (immune to turbo/thermal
# wander) and GIL-independent (it is per-query SERVER work, valid even if the Python client under-drives).
#
# Result (2026-07-07, HEAD b15b97abc, C=8): PARK still ~+23-24% instructions / +31-33% branches per
# query vs BLOCK under saturation -> steal-then-park avoids the park/wake but the coroutine DRIVE
# machinery overhead remains. See ../RESULTS_c34.md §7.
#
# Robustness note: PARK does MORE total instructions while completing FEWER queries, so the +23% is
# genuine per-query work, not a fixed background cost divided over fewer queries.
#
# Usage:   pmu_concurrent_park_vs_block.sh [clients] [window_s]
#   env:   MEMGRAPH_ORGANIZATION_NAME + MEMGRAPH_ENTERPRISE_LICENSE  (enterprise; parallel plan)
#          MG_BIN=./build/memgraph (default).  Python venv tests/ve3 (neo4j driver).
#          perf usable: sudo sysctl kernel.perf_event_paranoid=1
# Run from the repo root.
set -uo pipefail
MG="${MG_BIN:-./build/memgraph}"
PORT=7716
C="${1:-8}"        # concurrent clients (8 = where the residual regression is most consistent)
WIN="${2:-12}"     # measurement window seconds
DATASET=400000
Q="USING PARALLEL EXECUTION 4 MATCH (n:N) RETURN count(n), sum(n.p)"
EVENTS=instructions,cycles,branches,context-switches
PY=tests/ve3/bin/python3

for v in MEMGRAPH_ORGANIZATION_NAME MEMGRAPH_ENTERPRISE_LICENSE; do
  [ -n "${!v:-}" ] || { echo "ERROR: export $v first (enterprise feature)"; exit 1; }
done
[ -x "$MG" ] || { echo "memgraph binary not found/executable: $MG"; exit 1; }
[ -x "$PY" ] || { echo "python venv not found: $PY (need neo4j driver)"; exit 1; }

wait_port(){ for _ in $(seq 1 90); do (echo >"/dev/tcp/127.0.0.1/$PORT")2>/dev/null&&return 0;sleep 0.5;done;return 1;}

for arm in block park; do
  case $arm in
    block) KNOB="";;                                              # sync coordinator -> WaitOrSteal blocks
    park)  KNOB="Aggregate,OrderBy,Accumulate,Distinct,HashJoin";;# coroutine coordinator steal-then-park
  esac
  rm -rf /tmp/mg_pmc
  # cores 2-7,10-15: physical cores 2-7, leaving 0-1 for OS/neighbour (see RESULTS_c34.md §1)
  taskset -c 2-7,10-15 "$MG" --bolt-port=$PORT --data-directory=/tmp/mg_pmc --telemetry-enabled=false \
    --log-level=WARNING --storage-snapshot-on-exit=false --monitoring-port=$((PORT+100)) --metrics-port=$((PORT+400)) \
    --query-coroutine-yield-ops="$KNOB" >/tmp/mg_pmc.log 2>&1 &
  MGPID=$!
  wait_port || { echo "arm=$arm FAILED to bind $PORT"; cat /tmp/mg_pmc.log; kill "$MGPID"; exit 1; }
  "$PY" - "$PORT" "$DATASET" <<'PY'
import sys
from neo4j import GraphDatabase
port=int(sys.argv[1]); target=int(sys.argv[2])
d=GraphDatabase.driver(f"bolt://127.0.0.1:{port}",auth=("",""))
def run(q):
    with d.session() as s: return [r.values() for r in s.run(q)]
made=0
while made<target:
    b=min(50000,target-made); run(f"UNWIND range({made+1},{made+b}) AS i CREATE (:N {{p:i, g:i%7}})"); made+=b
print("loaded", run("MATCH (n:N) RETURN count(*)")[0][0])
d.close()
PY
  echo "=================== arm=$arm  C=$C  win=${WIN}s  (knob='$KNOB') ==================="
  perf stat -e "$EVENTS" -p "$MGPID" -- taskset -c 2-7,10-15 "$PY" - "$PORT" "$C" "$WIN" "$Q" <<'PY' 2>&1 \
    | grep -E 'instructions|cycles|branch|context-switches|QUERIES_COMPLETED|seconds' | sed "s|^|  [$arm] |"
import sys, threading, time
from neo4j import GraphDatabase
port=int(sys.argv[1]); C=int(sys.argv[2]); win=float(sys.argv[3]); q=sys.argv[4]
done=[0]; lock=threading.Lock(); stop=threading.Event()
def worker():
    d=GraphDatabase.driver(f"bolt://127.0.0.1:{port}",auth=("",""))
    cnt=0
    while not stop.is_set():
        with d.session() as s: list(s.run(q))
        cnt+=1
    with lock: done[0]+=cnt
    d.close()
dw=GraphDatabase.driver(f"bolt://127.0.0.1:{port}",auth=("",""))    # warm
for _ in range(5):
    with dw.session() as s: list(s.run(q))
dw.close()
ts=[threading.Thread(target=worker) for _ in range(C)]
t0=time.perf_counter()
for t in ts: t.start()
time.sleep(win); stop.set()
for t in ts: t.join()
print(f"QUERIES_COMPLETED {done[0]}  window_s {time.perf_counter()-t0:.2f}  qps {done[0]/(time.perf_counter()-t0):.1f}")
PY
  kill "$MGPID" 2>/dev/null; wait "$MGPID" 2>/dev/null; sleep 2
done
echo ">> per-query = instructions / QUERIES_COMPLETED   (2026-07-07 C=8: PARK ~+23-24% vs BLOCK)"
