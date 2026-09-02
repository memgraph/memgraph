#!/usr/bin/env bash
# Full Phase-3 spectrum. For each workload scenario x commit-mode {SYNC,STRICT_SYNC}, run master vs
# #4669 across reader/writer combos (W=0 baseline + slow-writer combos). One results file.
# Scenario = NAME|RMODE|RQROWS|NQ|WMODE|WQROWS  (see phase3.py for semantics).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="$HERE/${GRID_OUT:-phase3_grid.txt}"; : > "$OUT"
export COMBOS="${COMBOS:-16,0 16,2 16,4}" DUR="${DUR:-7}" REPS="${REPS:-2}" REPL_MS="${REPL_MS:-20}"
RUNNER="${RUNNER:-phase3_run.sh}"   # set RUNNER=phase3_run_ab.sh for interleaved/counterbalanced A/B
BINS="${BINS:-master:$HERE/bins/master/memgraph p4669:$HERE/bins/p4669/memgraph}"

SCENARIOS=(
  "fastread-expl|explicit|130000|1|explicit|0"       # fast reads + short writes, explicit
  "fastread-auto|auto|130000|1|auto|0"               # fast reads + short writes, implicit
  "longread-expl|explicit|1000000|8|explicit|0"      # long reads + short writes, explicit
  "longread-auto|auto|1000000|1|auto|0"              # long reads + short writes, implicit
  "fastR-longW-expl|explicit|130000|1|explicit|20000" # fast reads + LONG writes, explicit
  "fastR-longW-auto|auto|130000|1|auto|20000"        # fast reads + LONG writes, implicit
  "longR-longW-expl|explicit|1000000|8|explicit|20000" # long reads + LONG writes, explicit
  "longR-longW-auto|auto|1000000|1|auto|20000"       # long reads + LONG writes, implicit
)

for spec in "${SCENARIOS[@]}"; do
  IFS='|' read -r NAME RMODE RQROWS NQ WMODE WQROWS <<< "$spec"
  export RMODE RQROWS NQ WMODE WQROWS SCENARIO="$NAME"
  for MODE in SYNC STRICT_SYNC; do
    export MODE
    echo "===== scenario=$NAME mode=$MODE (RQROWS=$RQROWS NQ=$NQ WQROWS=$WQROWS) $(date -u +%H:%M:%S) =====" | tee -a "$OUT"
    "$HERE/$RUNNER" $BINS 2>&1 | grep -viE "unable to resolve host|Killed" | tee -a "$OUT"
  done
done
echo "=== GRID DONE ===" | tee -a "$OUT"
