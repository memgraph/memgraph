#!/usr/bin/env bash
# Phase-3 matrix: fast reads + slow (SYNC-replica) commits. For each "label:binary", set up the
# 3-netns topology, start main + SYNC replica (WAL ON -- required for replication), register, then
# run phase3.py over READERS x WRITERS combos, REPS times. Reports read tx/s + latency, write tx/s.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; USER_NAME="$(id -un)"
LD8="${LD_LIB:-/opt/toolchain-v8/lib:/opt/toolchain-v8/lib64}"
CLIENT_MS="${CLIENT_MS:-0.5}"; REPL_MS="${REPL_MS:-20}"; QROWS="${QROWS:-130000}"
DUR="${DUR:-8}"; REPS="${REPS:-2}"; SRV_CORES="${SRV_CORES:-0-7}"; CLI_CORES="${CLI_CORES:-8-11}"
# "R,W" combos: readers-only baseline, then readers with a few slow writers
COMBOS="${COMBOS:-16,0 16,2 16,4 32,4}"
CLEAN="--telemetry-enabled=false --log-level=ERROR --storage-wal-enabled=true --storage-gc-cycle-sec=3600 --storage-snapshot-on-exit=false"

run_one() {  # label binary
  local LABEL="$1" MG="$2" DDM DDR MPID RPID; local MODE="${MODE:-SYNC}"
  [ -x "$MG" ] || { echo "$LABEL: MISSING $MG"; return; }
  DDM="$(mktemp -d "$HERE/ddm.XXXXXX")"; DDR="$(mktemp -d "$HERE/ddr.XXXXXX")"
  "$HERE/setup_repl.sh" "$CLIENT_MS" "$REPL_MS" >/dev/null 2>&1
  sudo ip netns exec mgsrv sudo -u "$USER_NAME" env LD_LIBRARY_PATH="$LD8" taskset -c "$SRV_CORES" \
    "$MG" --bolt-address=10.0.0.2 --bolt-port=7687 --bolt-num-workers=8 --data-directory="$DDM" $CLEAN >/dev/null 2>&1 &
  MPID=$!
  sudo ip netns exec mgrepl sudo -u "$USER_NAME" env LD_LIBRARY_PATH="$LD8" taskset -c "$SRV_CORES" \
    "$MG" --bolt-address=10.0.1.3 --bolt-port=7688 --bolt-num-workers=4 --data-directory="$DDR" $CLEAN >/dev/null 2>&1 &
  RPID=$!
  for hp in "10.0.0.2 7687" "10.0.1.3 7688"; do set -- $hp
    for i in $(seq 1 60); do sudo ip netns exec mgsrv bash -c "exec 3<>/dev/tcp/$1/$2" 2>/dev/null && break; sleep 0.5; done; done
  # register SYNC replica (admin from mgsrv netns; reaches main + replica)
  sudo ip netns exec mgsrv sudo -u "$USER_NAME" python3 -c "
from neo4j import GraphDatabase
r=GraphDatabase.driver('bolt://10.0.1.3:7688',auth=None)
with r.session() as s: s.run('SET REPLICATION ROLE TO REPLICA WITH PORT 10001;').consume()
r.close()
m=GraphDatabase.driver('bolt://10.0.0.2:7687',auth=None)
with m.session() as s:
    s.run(\"REGISTER REPLICA r1 $MODE TO '10.0.1.3:10001';\").consume()
    assert list(s.run('SHOW REPLICAS;')), 'no replica'
m.close()" >/dev/null 2>&1 || { echo "$LABEL: REGISTER_FAIL"; }
  for combo in $COMBOS; do
    R="${combo%%,*}"; W="${combo##*,}"
    for rep in $(seq 1 "$REPS"); do
      line=$(sudo ip netns exec mgcli sudo -u "$USER_NAME" env \
        RMODE="${RMODE:-explicit}" RQROWS="${RQROWS:-1000000}" NQ="${NQ:-8}" \
        WMODE="${WMODE:-explicit}" WQROWS="${WQROWS:-0}" taskset -c "$CLI_CORES" \
        python3 -u "$HERE/phase3.py" bolt://10.0.0.2:7687 "$R" "$W" "$DUR" 2>/dev/null | grep -viE "unable to resolve")
      printf '%-22s %-8s %s\n' "${SCENARIO:-} $LABEL" "$MODE" "$line"
    done
  done
  sudo kill "$MPID" "$RPID" 2>/dev/null
  for ns in mgcli mgsrv mgrepl; do Pn=$(sudo ip netns pids "$ns" 2>/dev/null); [ -n "$Pn" ] && echo "$Pn"|xargs -r sudo kill -9 2>/dev/null; sudo ip netns del "$ns" 2>/dev/null; done
  rm -rf "$DDM" "$DDR"
}
for arg in "$@"; do run_one "${arg%%:*}" "${arg#*:}"; done
echo "=== PHASE3 DONE ==="
