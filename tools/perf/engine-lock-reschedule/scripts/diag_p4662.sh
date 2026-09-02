#!/usr/bin/env bash
# Diagnose the p4662 fast-read regression (master vs p4662) on localhost -- NO netns/sudo needed.
# Isolates the cause across a small matrix and reports, per run: completed queries, q/s, ERRORS
# (counted WITHOUT the reader-dies bug), p50/p99, context-switches, and server-side abort/timeout log
# lines. Server pinned to 8 cores (0-7), clients to 8-11 -- the benchmark's topology, minus netns.
#
# Tests:
#   H1 (a thread steals a physical CPU): each binary at bolt-num-workers = 8,7,6 on the SAME 8 cores.
#      If p4662's gap vs master SHRINKS as workers drop (spare core for background threads), H1 holds.
#   H2 (spurious timeouts/aborts): ERR count + server abort-log count. Expected ~0 if it's not aborts.
#   replica factor: run each with and without a SYNC replica registered (the benchmark registers one).
#
# Usage:  ./diag_p4662.sh            (full matrix, ~3-4 min)
#         DUR=15 NCLIENTS=16 RMODE=auto ./diag_p4662.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; BINS="$HERE/../bins"; LD="/opt/toolchain-v8/lib:/opt/toolchain-v8/lib64"
SRV_CORES="${SRV_CORES:-0-7}"; CLI_CORES="${CLI_CORES:-8-11}"
DUR="${DUR:-12}"; NCLIENTS="${NCLIENTS:-16}"; RMODE="${RMODE:-auto}"; RQROWS="${RQROWS:-130000}"
PORT=17700; RPORT=17701

# ---- robust load counter: counts ok AND errors per client, never dies on an error ----
CNT="$(mktemp --suffix=.py)"
cat > "$CNT" <<'PY'
import sys, os, time, multiprocessing as mp
from neo4j import GraphDatabase
URI=sys.argv[1]; NC=int(sys.argv[2]); DUR=float(sys.argv[3]); RMODE=sys.argv[4]; ROWS=int(sys.argv[5])
Q=f"UNWIND range(1,{ROWS}) AS x RETURN sum(x)"
def w(ret):
    d=GraphDatabase.driver(URI,auth=None); ok=0; err=0; errs=[]; lats=[]
    end=time.monotonic()+DUR
    try:
        with d.session() as s:
            while time.monotonic()<end:
                t0=time.monotonic()
                try:
                    if RMODE=="auto":
                        s.run(Q).consume(); ok+=1
                    else:
                        tx=s.begin_transaction(); tx.run(Q).consume(); tx.commit(); ok+=1
                    lats.append((time.monotonic()-t0)*1000)
                except Exception as e:
                    err+=1
                    if len(errs)<3: errs.append(type(e).__name__+":"+str(e)[:80])
    finally:
        d.close()
    ret.put((ok,err,errs,lats))
def main():
    q=mp.Queue(); ps=[mp.Process(target=w,args=(q,)) for _ in range(NC)]
    [p.start() for p in ps]
    got=[q.get() for _ in ps]; [p.join() for p in ps]
    ok=sum(g[0] for g in got); err=sum(g[1] for g in got)
    errs=[e for g in got for e in g[2]][:3]
    lats=sorted(x for g in got for x in g[3])
    def pct(p): return lats[min(len(lats)-1,int(p/100*len(lats)))] if lats else 0
    print(f"ok={ok} err={err} qps={ok/DUR:.1f} p50={pct(50):.1f} p99={pct(99):.1f} errs={errs}")
main()
PY

start_server(){ # $1 bin  $2 workers  $3 port  $4 datadir  $5 logfile
  LD_LIBRARY_PATH="$LD" taskset -c "$SRV_CORES" "$1" --bolt-address=127.0.0.1 --bolt-port="$3" \
    --bolt-num-workers="$2" --data-directory="$4" --telemetry-enabled=false --log-level=WARNING \
    --storage-wal-enabled=true --storage-snapshot-on-exit=false >"$5" 2>&1 &
  echo $!
}
wait_up(){ # $1 port -- poll with a real bolt query (robust; matches the load client), not bash /dev/tcp
  for _ in $(seq 1 80); do
    python3 -c "from neo4j import GraphDatabase
d=GraphDatabase.driver('bolt://127.0.0.1:$1',auth=None)
with d.session() as s: s.run('RETURN 1').consume()
d.close()" 2>/dev/null && return 0
    sleep 0.5
  done
  return 1; }
register_replica(){ python3 -c "
from neo4j import GraphDatabase
r=GraphDatabase.driver('bolt://127.0.0.1:$RPORT',auth=None)
with r.session() as s: s.run('SET REPLICATION ROLE TO REPLICA WITH PORT 10001;').consume()
r.close()
m=GraphDatabase.driver('bolt://127.0.0.1:$PORT',auth=None)
with m.session() as s: s.run(\"REGISTER REPLICA r1 SYNC TO '127.0.0.1:10001';\").consume(); assert list(s.run('SHOW REPLICAS;'))
m.close()" 2>/dev/null && echo ok || echo REGFAIL; }

PSTEP=0
run_cfg(){ # $1 label  $2 bin  $3 workers  $4 repl(0/1)
  local lbl="$1" bin="$2" w="$3" repl="$4" ddm ddr log rlog mpid rpid
  PSTEP=$((PSTEP+2)); PORT=$((17700+PSTEP)); RPORT=$((17701+PSTEP))   # fresh ports each config (avoid TIME_WAIT/rebind races)
  ddm="$(mktemp -d)"; log="$(mktemp)"
  mpid=$(start_server "$bin" "$w" "$PORT" "$ddm" "$log"); wait_up "$PORT" || { echo "$lbl: server_down"; kill $mpid 2>/dev/null; return; }
  if [ "$repl" = 1 ]; then
    ddr="$(mktemp -d)"; rlog="$(mktemp)"; rpid=$(start_server "$bin" 4 "$RPORT" "$ddr" "$rlog"); wait_up "$RPORT"
    [ "$(register_replica)" = ok ] || echo "$lbl: REGFAIL"
  fi
  # run load in background; perf-stat the SERVER (where thread contention lives) during the load window
  local qf pf; qf="$(mktemp)"; pf="$(mktemp)"
  taskset -c "$CLI_CORES" python3 "$CNT" "bolt://127.0.0.1:$PORT" "$NCLIENTS" "$DUR" "$RMODE" "$RQROWS" >"$qf" 2>/dev/null &
  local loadpid=$!
  sleep 1
  perf stat -p "$mpid" -e context-switches,cpu-migrations,cache-misses -- sleep "$((DUR>3?DUR-2:1))" 2>"$pf" || true
  wait "$loadpid"
  local qline csw mig srv_abort
  qline=$(grep -E '^ok=' "$qf")
  csw=$(awk '/context-switches/{print $1}' "$pf" | tr -d '.,')
  mig=$(awk '/cpu-migrations/{print $1}' "$pf" | tr -d '.,')
  srv_abort=$(grep -ciE 'abort|timeout|deadline|exceeded|terminat' "$log" 2>/dev/null)
  rm -f "$qf" "$pf"
  printf '%-22s %s  ctxsw=%s mig=%s srv_abortlog=%s\n' "$lbl" "$qline" "${csw:-?}" "${mig:-?}" "$srv_abort"
  kill -9 $mpid ${rpid:-} 2>/dev/null; wait $mpid ${rpid:-} 2>/dev/null; rm -rf "$ddm" ${ddr:-} "$log" ${rlog:-}
  sleep 2
}

echo "=== diag_p4662 : DUR=$DUR NCLIENTS=$NCLIENTS RMODE=$RMODE RQROWS=$RQROWS  srv[$SRV_CORES] cli[$CLI_CORES] ==="
echo "--- H1: workers on 8 cores (gap should shrink for p4662 as workers drop, if a thread needs a core) ---"
for w in 8 7 6; do
  run_cfg "master-${w}w-repl"  "$BINS/master/memgraph" "$w" 1
  run_cfg "p4662-${w}w-repl"   "$BINS/p4662/memgraph"  "$w" 1
done
echo "--- ORDER test: run p4662 FIRST, master AFTER (if p4662 recovers to ~master, it's ordering/box-state) ---"
run_cfg "p4662-8w-FIRST"  "$BINS/p4662/memgraph"  8 1
run_cfg "master-8w-AFTER" "$BINS/master/memgraph" 8 1
echo "--- replica factor: 8 workers, NO replica (does the regression need the registered SYNC replica?) ---"
run_cfg "master-8w-norepl" "$BINS/master/memgraph" 8 0
run_cfg "p4662-8w-norepl"  "$BINS/p4662/memgraph"  8 0
rm -f "$CNT"
echo "=== DONE. Read: qps gap master-vs-p4662 per config; ctxsw/mig for contention; err/srv_abortlog for H2. ==="
