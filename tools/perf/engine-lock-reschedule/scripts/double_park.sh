#!/usr/bin/env bash
# DOUBLE-PARK validation — RUN WITH SUDO (netns + tc netem).
#
#   sudo -v ; ./double_park.sh
#
# Proves an IMPLICIT (autocommit) WRITE composes BOTH parks in a single query without wedging:
#   * implicit BEGIN parks on main_lock_  — a READ_ONLY DDL holder (CREATE/DROP INDEX) excludes WRITE,
#   * implicit COMMIT parks on commit_mutex_ — the final PULL's Commit() blocks on the (netem-delayed)
#     SYNC replica ack while another writer holds the commit serializer.
# Topology (setup_repl.sh): fast client link, slow REPL_MS replication link so SYNC commits are slow.
#
# PASS = both park kinds fire in the server log (resource=0 main-lock BEGIN, resource=1 commit-lock
#        COMMIT) AND implicit writers keep completing (write op/s > 0, no hang). A regression wedges
#        the writers -> op/s collapses / the run stalls past the deadline.
#
# NB: uses the PARKDBG-instrumented cls_pure build to read the two park kinds from the log. With a
# stripped build the log check is skipped and only the behavioural signal (write op/s > 0) is used.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; USER_NAME="$(id -un)"
BIN="${BIN:-$HERE/../bins/cls_pure/memgraph}"
LD8="${LD_LIB:-/opt/toolchain-v8/lib:/opt/toolchain-v8/lib64}"
REPL_MS="${REPL_MS:-150}"        # one-way; ~2x = commit_mutex_ hold per SYNC commit (~300ms)
CLIENT_MS="${CLIENT_MS:-0.5}"
DUR="${DUR:-25}"
TIMEOUT="${TIMEOUT:-15}"         # storage-access timeout: legit double-parks resolve well within; a true wedge fails fast
COMBO="${COMBO:-8,2}"            # readers,writers ; writers are WMODE=auto = implicit (the double-park target)
[ -x "$BIN" ] || { echo "FATAL missing $BIN"; exit 1; }
sudo -v || { echo "FATAL need sudo for netns/netem"; exit 1; }
( while true; do sudo -n true 2>/dev/null || exit; sleep 50; done ) & KA=$!; trap 'kill $KA 2>/dev/null||true' EXIT

echo "=== double_park  bin=$(basename "$BIN")  REPL_MS=${REPL_MS} DUR=${DUR}s timeout=${TIMEOUT}s combo=${COMBO} ==="
"$HERE/setup_repl.sh" "$CLIENT_MS" "$REPL_MS" >/dev/null 2>&1 || { echo "FATAL setup_repl failed"; exit 1; }

CLEAN="--telemetry-enabled=false --storage-wal-enabled=true --storage-gc-cycle-sec=3600 --storage-snapshot-on-exit=false"
LOGM="$(mktemp)"; DDM="$(mktemp -d)"; DDR="$(mktemp -d)"
FLAGS="--experimental-enabled=lockfree-read-snapshot --storage-access-timeout-sec=${TIMEOUT} --log-level=WARNING --also-log-to-stderr=true"

# main (captures PARKDBG) + replica, in the netns
sudo ip netns exec mgsrv sudo -u "$USER_NAME" env LD_LIBRARY_PATH="$LD8" taskset -c "${SRV_CORES:-0-7}" \
  "$BIN" --bolt-address=10.0.0.2 --bolt-port=7687 --bolt-num-workers="${NWORKERS:-8}" --data-directory="$DDM" $CLEAN $FLAGS >"$LOGM" 2>&1 & MPID=$!
sudo ip netns exec mgrepl sudo -u "$USER_NAME" env LD_LIBRARY_PATH="$LD8" taskset -c "${SRV_CORES:-0-7}" \
  "$BIN" --bolt-address=10.0.1.3 --bolt-port=7688 --bolt-num-workers=4 --data-directory="$DDR" $CLEAN >/dev/null 2>&1 & RPID=$!
for hp in "10.0.0.2 7687" "10.0.1.3 7688"; do set -- $hp
  for _ in $(seq 1 60); do sudo ip netns exec mgsrv bash -c "exec 3<>/dev/tcp/$1/$2" 2>/dev/null && break; sleep 0.5; done; done
sudo ip netns exec mgsrv sudo -u "$USER_NAME" python3 -c "
from neo4j import GraphDatabase
r=GraphDatabase.driver('bolt://10.0.1.3:7688',auth=None)
with r.session() as s: s.run('SET REPLICATION ROLE TO REPLICA WITH PORT 10001;').consume()
r.close()
m=GraphDatabase.driver('bolt://10.0.0.2:7687',auth=None)
with m.session() as s:
    s.run(\"REGISTER REPLICA r1 SYNC TO '10.0.1.3:10001';\").consume()
    assert list(s.run('SHOW REPLICAS;')), 'no replica'
m.close()" >/dev/null 2>&1 || { echo 'FATAL replica register failed'; }

R="${COMBO%%,*}"; W="${COMBO##*,}"
echo "--- workload: R=$R implicit-writers=$W + 1 READ_ONLY DDL + 2 streamers, ${DUR}s (writers hit BEGIN-park AND COMMIT-park) ---"
OUT=$(timeout $((DUR+60)) sudo ip netns exec mgcli sudo -u "$USER_NAME" env \
  RMODE=auto RQROWS=1000 WMODE=auto WQROWS=0 \
  NDDL=1 DDLMODE=readonly NSTREAM=2 SQROWS=100000 SNQ=100000 SMODE=explicit taskset -c "${CLI_CORES:-8-11}" \
  python3 -u "$HERE/phase3.py" bolt://10.0.0.2:7687 "$R" "$W" "$DUR" 2>/dev/null | grep -viE "unable to resolve")
echo "  phase3: $OUT"

# ---- verdict ----
BP=$(grep -ac "PARKDBG park .* resource=0" "$LOGM" 2>/dev/null); BP=${BP:-0}   # main-lock BEGIN parks
CP=$(grep -ac "PARKDBG park .* resource=1" "$LOGM" 2>/dev/null); CP=${CP:-0}   # commit-lock COMMIT parks
WOPS=$(echo "$OUT" | sed -nE 's|.*WRITE op/s=[[:space:]]*([0-9.]+).*|\1|p')   # phase3 pads: "op/s=   0.3"
INSTRUMENTED=$(( BP + CP ))   # >0 only on a PARKDBG build; a stripped build reports behavioural-only
echo "--- parks observed: BEGIN(main-lock)=$BP  COMMIT(commit-lock)=$CP ;  write op/s=${WOPS:-?}$([ "$INSTRUMENTED" -eq 0 ] && echo '  (stripped build: park-count check skipped, behavioural-only)') ---"
verdict=PASS; reason=""
if [ "$INSTRUMENTED" -gt 0 ]; then   # instrumented build: assert BOTH park kinds actually fired
  [ "${BP:-0}" -gt 0 ] 2>/dev/null || { verdict=FAIL; reason+=" no-begin-park"; }
  [ "${CP:-0}" -gt 0 ] 2>/dev/null || { verdict=FAIL; reason+=" no-commit-park"; }
fi
awk -v w="${WOPS:-0}" 'BEGIN{exit !(w+0>0)}' || { verdict=FAIL; reason+=" writers-stalled(op/s=${WOPS:-0})"; }
echo "  first PARKDBG-write park lines:"; grep -a "PARKDBG park " "$LOGM" | awk '!seen[$0~/resource=1/]++{print "    "$0}' | sed -E 's/.*PARKDBG/    PARKDBG/' | head -2
echo "=== DOUBLE-PARK: $verdict$reason ==="

sudo kill "$MPID" "$RPID" 2>/dev/null
for ns in mgcli mgsrv mgrepl; do Pn=$(sudo ip netns pids "$ns" 2>/dev/null); [ -n "$Pn" ] && echo "$Pn"|xargs -r sudo kill -9 2>/dev/null; sudo ip netns del "$ns" 2>/dev/null; done
rm -rf "$DDM" "$DDR" "$LOGM"
