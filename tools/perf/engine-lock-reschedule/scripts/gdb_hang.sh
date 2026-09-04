#!/usr/bin/env bash
# Bring up the minimal U3 park-hang and LEAVE IT RUNNING for interactive gdb.
#   H (holder): explicit txn holding main_lock READ.
#   D (ddl)   : ONE `DROP ALL CONSTRAINTS` (UNIQUE) — parks, then hangs (never rescued).
# Prints the server PID + the gdb attach line, then exits WITHOUT killing anything.
# Clean up after:  pkill -9 -x memgraph ; pkill -f 17921
#
# Usage:  ./scripts/gdb_hang.sh [BIN] [TMO]      e.g.  ./scripts/gdb_hang.sh bins/cls_pure/memgraph 3
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
BIN="${1:-$HERE/../bins/cls_pure/memgraph}"; TMO="${2:-3}"; PORT="${PORT:-17921}"
LD="${LD_LIB:-/opt/toolchain-v8/lib:/opt/toolchain-v8/lib64}"
[ -x "$BIN" ] || { echo "no binary $BIN"; exit 1; }
DD="$(mktemp -d /tmp/gdbhang.XXXXXX)"
pkill -9 -f "bolt-port=$PORT" 2>/dev/null; sleep 0.3

LD_LIBRARY_PATH="$LD" taskset -c 0-7 "$BIN" --bolt-port="$PORT" --bolt-num-workers=8 --data-directory="$DD" \
  --experimental-enabled=lockfree-read-snapshot --storage-access-timeout-sec="$TMO" \
  --telemetry-enabled=false --log-level=WARNING --also-log-to-stderr=true \
  --storage-snapshot-on-exit=false >"$DD/log" 2>&1 &
MP=$!
for i in $(seq 1 40); do python3 -c "from neo4j import GraphDatabase
d=GraphDatabase.driver('bolt://127.0.0.1:$PORT',auth=None)
with d.session() as s: s.run('RETURN 1').consume()
d.close()" 2>/dev/null && break; kill -0 $MP 2>/dev/null || { echo SERVER_DIED; cat "$DD/log"; exit 1; }; sleep 0.3; done

# H — hold main_lock READ open for a long time (detached; survives this script)
setsid python3 -c "import time
from neo4j import GraphDatabase
d=GraphDatabase.driver('bolt://127.0.0.1:$PORT',auth=None); s=d.session()
tx=s.begin_transaction(); tx.run('UNWIND range(1,1000) AS x RETURN sum(x)').consume()
time.sleep(3600); d.close()" >"$DD/h.log" 2>&1 &
sleep 1
# D — ONE UNIQUE; it parks and hangs (detached; survives this script)
setsid python3 -c "from neo4j import GraphDatabase
d=GraphDatabase.driver('bolt://127.0.0.1:$PORT',auth=None)
try: d.session().run('DROP ALL CONSTRAINTS').consume()
except Exception as e: print('D returned:',e)" >"$DD/d.log" 2>&1 &

echo "waiting $((TMO+4))s to confirm D hangs past its ${TMO}s deadline..."; sleep $((TMO+4))
if grep -q "D returned" "$DD/d.log" 2>/dev/null; then
  echo "NOTE: D did NOT hang this run ($(cat "$DD/d.log")). Re-run; the leak is timing-sensitive."
else
  echo "CONFIRMED: D (UNIQUE) is hung past its deadline — park-hang is live."
fi
cat <<EOF

========================================================================
  server PID = $MP     data-dir = $DD
  attach:   gdb $BIN -p $MP
  cleanup:  pkill -9 -x memgraph ; pkill -f "bolt-port=$PORT"
========================================================================
EOF
