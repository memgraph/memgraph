#!/usr/bin/env bash
# Minimal U3 recreation — localhost, no sudo (U3 has no writers/replication). Three controlled clients on
# one server, so we can watch exactly what wedges and gdb the lock at will:
#   H (holder)  : explicit txn, BEGIN + hold main_lock READ open for the whole window (never commits early).
#   D (ddl)     : loops DROP ALL CONSTRAINTS (UNIQUE) — registers unique_pending; retries like the harness.
#   P (probe)   : every 1s issues one plain READ and reports latency / error (is it gated by unique_pending?).
# It prints a per-second timeline of P (the reader that "can't get in") and D (does UNIQUE ever land, or
# only time out?), then leaves the server UP so you can gdb it (pid printed). Ctrl-C to stop + clean.
#
# Usage:  ./u3_min.sh [BIN] [on|off] [TIMEOUT_SEC]      e.g.  ./u3_min.sh ../bins/cls/memgraph on 3
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
BIN="${1:-$HERE/../bins/cls/memgraph}"; FLAGMODE="${2:-on}"; TMO="${3:-3}"; PORT="${PORT:-17911}"
LD="${LD_LIB:-/opt/toolchain-v8/lib:/opt/toolchain-v8/lib64}"
[ -x "$BIN" ] || { echo "no binary $BIN"; exit 1; }
FLAG=""; [ "$FLAGMODE" = on ] && FLAG="--experimental-enabled=lockfree-read-snapshot"
DD="$(mktemp -d)"; trap 'kill -9 $MP $HP $DP $PP 2>/dev/null; rm -rf "$DD"' EXIT

LD_LIBRARY_PATH="$LD" taskset -c 0-7 "$BIN" --bolt-port="$PORT" --bolt-num-workers=8 --data-directory="$DD" \
  $FLAG --storage-access-timeout-sec="$TMO" --telemetry-enabled=false --log-level=WARNING \
  --storage-snapshot-on-exit=false >"$DD/log" 2>&1 & MP=$!
for i in $(seq 1 40); do python3 -c "from neo4j import GraphDatabase
d=GraphDatabase.driver('bolt://127.0.0.1:$PORT',auth=None)
with d.session() as s: s.run('RETURN 1').consume()
d.close()" 2>/dev/null && break; kill -0 $MP 2>/dev/null || { echo SERVER_DIED; cat "$DD/log"; exit 1; }; sleep 0.3; done
echo "=== u3_min  bin=$(basename "$BIN") flag=$FLAGMODE timeout=${TMO}s  server pid=$MP  (gdb -p $MP) ==="

# H — holder: hold main_lock READ open for 30s via an explicit, uncommitted txn.
python3 -c "
import time
from neo4j import GraphDatabase
d=GraphDatabase.driver('bolt://127.0.0.1:$PORT',auth=None); s=d.session()
tx=s.begin_transaction(); tx.run('UNWIND range(1,1000) AS x RETURN sum(x)').consume()  # holds READ
print('[H] holding main_lock READ (explicit txn open)',flush=True)
time.sleep(30)
try: tx.commit()
except Exception: pass
d.close()" > "$DD/h.log" 2>&1 & HP=$!
sleep 2

# D — ddl: loop UNIQUE DROP ALL CONSTRAINTS, count successes vs timeouts.
python3 -c "
import time
from neo4j import GraphDatabase
d=GraphDatabase.driver('bolt://127.0.0.1:$PORT',auth=None); s=d.session()
ok=0; to=0; end=time.monotonic()+26
while time.monotonic()<end:
    t=time.monotonic()
    try: s.run('DROP ALL CONSTRAINTS').consume(); ok+=1; res='ACQUIRED'
    except Exception as e: to+=1; res='timeout'
    print('[D] %-8s in %5.2fs  (acquired=%d timeout=%d)'%(res,time.monotonic()-t,ok,to),flush=True)
d.close()" > "$DD/d.log" 2>&1 & DP=$!
sleep 2

# P — probe: one READ per second; is the reader gated by unique_pending?
python3 -c "
import time
from neo4j import GraphDatabase
d=GraphDatabase.driver('bolt://127.0.0.1:$PORT',auth=None)
for i in range(24):
    t=time.monotonic()
    try:
        with d.session() as s: s.run('UNWIND range(1,1000) AS x RETURN sum(x)').consume()
        print('[P] t=%2d  READ ok in %6.3fs'%(i,time.monotonic()-t),flush=True)
    except Exception as e:
        print('[P] t=%2d  READ FAIL in %6.3fs : %s'%(i,time.monotonic()-t,str(e)[:60]),flush=True)
    time.sleep(max(0,1-(time.monotonic()-t)))
d.close()" > "$DD/p.log" 2>&1 & PP=$!

# stream the three logs together for ~24s
for t in $(seq 1 24); do sleep 1
  tail -n +1 "$DD/p.log" 2>/dev/null | tail -1
  d=$(tail -1 "$DD/d.log" 2>/dev/null); [ -n "$d" ] && echo "     $d"
done
echo "=== summary ==="
echo "D (ddl) — did UNIQUE ever ACQUIRE, or only timeout?"; tail -4 "$DD/d.log"
echo "P (probe) — is the READ gated?"; tail -4 "$DD/p.log"
echo "H (holder):"; cat "$DD/h.log"
if [ -n "${KEEPUP:-}" ]; then echo "server $MP left UP (KEEPUP set) — gdb -p $MP ; kill it when done"; trap - EXIT; fi
