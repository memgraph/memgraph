#!/usr/bin/env bash
# Outer sweep: for each candidate binary x REPL_MS (commit-slowness), drive phase3_grid.sh
# (8 scenarios x {SYNC,STRICT_SYNC}) as a master-vs-candidate pair. One results file per grid:
#   grid_master_vs_<cand>_repl<ms>.txt  (in this scripts dir, written by phase3_grid.sh).
#
# Ordering is deliberate: the two reschedule PRs (p4669, p4684 -- the ones that move the customer
# number) run FIRST, and REPL_MS=20 (the reference delay) runs first within each candidate, so an
# early abort still yields the confirmable headline rows.
#
# Env:  CANDIDATES="p4669 p4684 ..."   REPL_MS_LIST="20 5 50"   DUR=15  REPS=3
#       (needs sudo for ip netns/tc, inherited from phase3_run.sh)
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
BINS_ROOT="$HERE/../bins"
CANDIDATES="${CANDIDATES:-p4669 p4684 p4662 p4663 p4668}"
REPL_MS_LIST="${REPL_MS_LIST:-20 5 50}"
export DUR="${DUR:-15}" REPS="${REPS:-3}"

MASTER="$BINS_ROOT/master/memgraph"
[ -x "$MASTER" ] || { echo "FATAL: missing master bundle: $MASTER (run build_binaries.sh)"; exit 1; }

for cand in $CANDIDATES; do
  cbin="$BINS_ROOT/$cand/memgraph"
  if [ ! -x "$cbin" ]; then echo "SKIP $cand: missing bundle $cbin"; continue; fi
  for ms in $REPL_MS_LIST; do
    out="grid_master_vs_${cand}_repl${ms}.txt"
    echo "########## SWEEP $cand  REPL_MS=${ms}ms  DUR=${DUR} REPS=${REPS}  ->  $out   $(date -u +%H:%M:%S) ##########"
    GRID_OUT="$out" REPL_MS="$ms" \
      BINS="master:$MASTER $cand:$cbin" \
      "$HERE/phase3_grid.sh"
  done
done
echo "=== SWEEP DONE -- results: $HERE/grid_master_vs_*.txt ==="
