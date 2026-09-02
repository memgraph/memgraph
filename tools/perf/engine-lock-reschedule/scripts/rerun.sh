#!/usr/bin/env bash
# Manual re-run launcher (thermal-fixed sweep). RUN THIS IN YOUR TERMINAL, ideally inside tmux so it
# survives an SSH disconnect. It prompts for sudo ONCE (real tty), keeps the credential warm for the
# whole run, starts the throttle auditor (thermal_watch.log), and sweeps master vs the PRs that need
# valid data at REPL_MS=20, REPS=5. Writes grid_master_vs_<cand>_repl20.txt + sweep2.out + thermal_watch.log.
#
#   tmux new -s sweep          # persistent session
#   ./rerun.sh                 # answer the sudo prompt once
#   <Ctrl-b> d                 # detach; reattach later with:  tmux attach -t sweep
cd "$(dirname "$0")" || exit 1
# p4684 already has a complete, thermally-clean grid — skip it; rerun only the remaining supporting PRs.
export CANDIDATES="p4662 p4663 p4668" REPL_MS_LIST="20" REPS=5 DUR=15
# Start clean: archive any existing grid file for the candidates we're about to (re)run, so
# phase3_grid's `tee -a` appends onto a fresh file instead of onto stale/partial data.
mkdir -p ../results/superseded
for c in $CANDIDATES; do for ms in $REPL_MS_LIST; do
  f="grid_master_vs_${c}_repl${ms}.txt"
  [ -f "$f" ] && mv "$f" "../results/superseded/${f%.txt}.$(date +%Y%m%d_%H%M%S).txt"
done; done
./run_sweep.sh 2>&1 | tee sweep2.out
