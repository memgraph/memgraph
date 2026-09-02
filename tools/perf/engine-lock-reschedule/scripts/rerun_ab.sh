#!/usr/bin/env bash
# CORRECTED validation sweep. Two fixes over the old rerun.sh:
#   1) RUNNER=phase3_run_ab.sh  -> interleaved/counterbalanced A/B (kills the second-slot bias that
#      produced the phantom ~10-12% "regression" reproduced across unrelated binaries).
#   2) A master-vs-master CONTROL grid runs FIRST. On a correct harness it must read ~0% everywhere;
#      any residual delta there is the artifact floor and bounds what we can believe in the real grids.
#
# Run in tmux (survives disconnect): prompts sudo once, keeps it warm, thermal-audits.
#   tmux new -s abctl ; ./rerun_ab.sh ; <Ctrl-b> d
#
# QUICK first pass (recommended, ~2 grids): CANDIDATES="p4662" ./rerun_ab.sh
#   -> control + p4662. If control ~0% AND p4662 ~0%/small-gain, the fix is validated and p4662 vindicated.
# Full set: CANDIDATES="p4669 p4684 p4662 p4663 p4668" ./rerun_ab.sh   (long; run overnight)
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; BINS_ROOT="$HERE/../bins"
export RUNNER=phase3_run_ab.sh REPS="${REPS:-6}" DUR="${DUR:-15}" REPL_MS="${REPL_MS:-20}"
CANDS="${CANDIDATES:-p4669 p4684 p4662 p4663 p4668}"
M="$BINS_ROOT/master/memgraph"
[ -x "$M" ] || { echo "FATAL: no master bundle at $M (build first)"; exit 1; }

sudo -v || { echo "FATAL: sudo credential needed (run 'sudo -v' first)"; exit 1; }
( while true; do sudo -n true 2>/dev/null || exit; sleep 50; done ) & KA=$!
"$HERE/thermal_watch.sh" 5 "$HERE/thermal_watch.log" >/dev/null 2>&1 & TW=$!
trap 'kill "$KA" "$TW" 2>/dev/null || true' EXIT
echo "=== AB sweep start $(date -u)  REPS=$REPS DUR=$DUR REPL_MS=$REPL_MS  (keepalive=$KA thermal=$TW) ==="

# 1) CONTROL: identical binary in both slots -- MUST be ~0% on a correct harness.
echo "########## CONTROL: master-vs-master (expect ~0%)  $(date -u +%H:%M:%S) ##########"
GRID_OUT=grid_ab_CONTROL_master_vs_master.txt BINS="masterA:$M masterB:$M" "$HERE/phase3_grid.sh"

# 2) each candidate vs master, interleaved.
for c in $CANDS; do
  cb="$BINS_ROOT/$c/memgraph"
  if [ ! -x "$cb" ]; then echo "SKIP $c (no bundle at $cb)"; continue; fi
  echo "########## $c vs master (interleaved)  $(date -u +%H:%M:%S) ##########"
  GRID_OUT="grid_ab_master_vs_${c}.txt" BINS="master:$M $c:$cb" "$HERE/phase3_grid.sh"
done
echo "=== AB SWEEP DONE $(date -u); results: $HERE/grid_ab_*.txt ==="
