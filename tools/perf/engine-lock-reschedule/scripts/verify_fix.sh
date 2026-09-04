#!/usr/bin/env bash
# Verify the autocommit BEGIN-park fix on the minimal U3 recreation. Two scenarios, localhost, no sudo:
#   A (timeout):  H holds main_lock READ for the whole window; D fires one UNIQUE DROP with a 3s access
#                 timeout. Correct = D FAILS with an access-timeout at ~3s (NOT a hang), lock not leaked.
#   B (acquire):  H holds READ for ~1.5s then commits; D fires one UNIQUE with a 6s timeout. Correct =
#                 D ACQUIRES shortly after H releases (~1.5s) and succeeds.
# Before the fix, A hangs forever (parked continuation dies "not parsed", unique_pending leaked).
#
# Usage:  ./verify_fix.sh [BIN]
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
BIN="${1:-$HERE/../bins/cls_pure/memgraph}"
LD="${LD_LIB:-/opt/toolchain-v8/lib:/opt/toolchain-v8/lib64}"
PORT="${PORT:-17922}"
[ -x "$BIN" ] || { echo "no binary $BIN"; exit 1; }

run_server() {
  local tmo="$1" dd="$2"
  LD_LIBRARY_PATH="$LD" taskset -c 0-7 "$BIN" --bolt-port="$PORT" --bolt-num-workers=8 --data-directory="$dd" \
    --experimental-enabled=lockfree-read-snapshot --storage-access-timeout-sec="$tmo" \
    --telemetry-enabled=false --log-level=WARNING --also-log-to-stderr=true \
    --storage-snapshot-on-exit=false >"$dd/log" 2>&1 &
  echo $!
  for i in $(seq 1 40); do python3 -c "from neo4j import GraphDatabase
d=GraphDatabase.driver('bolt://127.0.0.1:$PORT',auth=None)
with d.session() as s: s.run('RETURN 1').consume()
d.close()" 2>/dev/null && return 0; sleep 0.3; done
  return 1
}

scenario() {
  local name="$1" hold="$2" tmo="$3" expect="$4"
  for p in $(pgrep -f "bolt-port=$PORT"); do kill -9 "$p" 2>/dev/null; done; sleep 1
  local dd; dd="$(mktemp -d /tmp/verifyfix.XXXXXX)"
  local mp; mp="$(run_server "$tmo" "$dd")"
  # H: hold main_lock READ for $hold seconds via explicit txn, then commit+release
  python3 -c "
import time
from neo4j import GraphDatabase
d=GraphDatabase.driver('bolt://127.0.0.1:$PORT',auth=None); s=d.session()
tx=s.begin_transaction(); tx.run('UNWIND range(1,1000) AS x RETURN sum(x)').consume()
time.sleep($hold)
try: tx.commit()
except Exception: pass
d.close()" >"$dd/h.log" 2>&1 &
  local hp=$!
  sleep 0.5
  # D: one UNIQUE; capture outcome + wall time
  python3 -c "
import time
from neo4j import GraphDatabase
d=GraphDatabase.driver('bolt://127.0.0.1:$PORT',auth=None)
t=time.monotonic()
try:
    d.session().run('DROP ALL CONSTRAINTS').consume()
    print('OUTCOME=ACQUIRED elapsed=%.2f'%(time.monotonic()-t))
except Exception as e:
    print('OUTCOME=ERROR elapsed=%.2f msg=%s'%(time.monotonic()-t,str(e)[:80]))
d.close()" >"$dd/d.log" 2>&1 &
  local dp=$!
  # give D up to 12s to finish (must NOT hang)
  local done=0
  for i in $(seq 1 24); do kill -0 $dp 2>/dev/null || { done=1; break; }; sleep 0.5; done
  local res; res="$(cat "$dd/d.log" 2>/dev/null)"
  echo "--- scenario $name (hold=${hold}s timeout=${tmo}s) ---"
  if [ "$done" = 0 ]; then echo "  FAIL: D HUNG (>12s, no response) — leak/block-forever still present"; else echo "  D: $res"; fi
  echo "  PARKDBG:"; grep -a PARKDBG "$dd/log" 2>/dev/null | sed -E 's/.*PARKDBG/    /' | head -40
  # verdict
  case "$expect" in
    timeout)  echo "$res" | grep -q "OUTCOME=ERROR"    && echo "  => PASS (clean timeout, no hang)" || echo "  => FAIL (expected clean timeout error)";;
    acquire)  echo "$res" | grep -q "OUTCOME=ACQUIRED" && echo "  => PASS (acquired after release)"  || echo "  => FAIL (expected acquire)";;
  esac
  kill -9 $mp $hp $dp 2>/dev/null; rm -rf "$dd"; sleep 1
}

echo "=== verify_fix  bin=$(basename "$BIN") ==="
scenario A_timeout 3600 3 timeout
scenario B_acquire 1.5  6 acquire
echo "=== done ==="
for p in $(pgrep -f "bolt-port=$PORT"); do kill -9 "$p" 2>/dev/null; done
