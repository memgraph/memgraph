#!/usr/bin/env bash
# Broad S2e sweep driver -- RUN THIS (needs sudo for netns + tc netem). Runs master/s2b/s2e 3-way,
# interleaved+counterbalanced, across a wide range of scenarios and knob values. Every cell reports,
# per bundle: READ q/s + p99 (admission storm A), WRITE op/s, STREAM q/s + p50/p99 (productive work B),
# and main-server cpu=. The headline KPI throughout is STREAM q/s: does real work keep flowing while
# admission is contended? (master starves -> ~2 q/s; s2b partial; s2e flows.)
#
# The SCHED baseline (what each OFAT group varies ONE knob around):
#   4 streamers(B, past-admission) + 16 storm(A, RQROWS=1) + 1 slow writer, REPL_MS=500 (~2s holds),
#   MODE=SYNC, NWORKERS=8, SNQ=100000, SQROWS=100000.
#
# SWEEP_GROUPS (pick any subset; default = all):
#   scenarios  workload shapes at baseline: RO RW-fast RW-slow PARK SCHED UNIQ_RO UNIQ_UQ
#   repl       SCHED x REPL_MS   in {50 100 200 500 1000}   (hold length)
#   stream     SCHED x NSTREAM   in {1 2 4 8}               (amount of productive work)
#   storm      SCHED x NR        in {8 16 32 64}            (admission pressure)
#   writers    SCHED x NW        in {1 2 4}                 (concurrent slow commits)
#   workers    SCHED x NWORKERS  in {4 8 16}               (the limited pool -- core of the design)
#   mode       SCHED x MODE      in {SYNC STRICT_SYNC}      (one vs two ack phases)
#   snq        SCHED x SNQ       in {100 1000 100000}       (streamer txn length -> re-contend rate)
#
# Usage:  sudo -v ; ./sweep_s2e.sh                          # everything (long; run overnight in tmux)
#         SWEEP_GROUPS="scenarios repl workers" REPS=3 DUR=20 ./sweep_s2e.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; BINS="$HERE/../bins"
M="$BINS/master/memgraph"; B="$BINS/s2b/memgraph"; E="$BINS/s2e/memgraph"
for x in "$M" "$B" "$E"; do [ -x "$x" ] || { echo "FATAL: missing bundle $x (run build_binaries.sh first)"; exit 1; }; done
TRIO="master:$M s2b:$B s2e:$E"
[ -x "$BINS/s2e_fix/memgraph" ] && TRIO="$TRIO s2e_fix:$BINS/s2e_fix/memgraph"   # include the wake-all-commits fix if built
export REPS="${REPS:-3}" DUR="${DUR:-20}"
SWEEP_GROUPS="${SWEEP_GROUPS:-scenarios repl stream storm writers workers mode snq}"

sudo -v || { echo "FATAL: need sudo for netns/netem"; exit 1; }
( while true; do sudo -n true 2>/dev/null || exit; sleep 50; done ) & KA=$!
trap 'kill $KA 2>/dev/null || true' EXIT
echo "=== sweep_s2e start $(date -u)  REPS=$REPS DUR=$DUR groups=[$SWEEP_GROUPS] ==="
hdr(){ echo; echo "########## $1  $(date -u +%H:%M:%S) ##########"; }

# SCHED baseline env; each sched() call appends KEY=VAL overrides that win (env: last assignment wins).
SCHED_BASE=(MODE=SYNC REPL_MS=500 COMBOS=16,1 NSTREAM=4 SQROWS=100000 SNQ=100000 SMODE=explicit
            RMODE=auto RQROWS=1 WMODE=auto NWORKERS=8)
sched(){ local lbl="$1"; shift; hdr "SCHED[$lbl]"
         env SCENARIO="SCHED_$lbl" "${SCHED_BASE[@]}" "$@" "$HERE/phase3_run_ab.sh" $TRIO; }
scen(){  local lbl="$1"; shift; hdr "$lbl"; env SCENARIO="$lbl" "$@" "$HERE/phase3_run_ab.sh" $TRIO; }

for g in $SWEEP_GROUPS; do case "$g" in
  scenarios)
    scen RO       MODE=SYNC        REPL_MS=20  COMBOS=16,0 RMODE=auto RQROWS=1000000 WMODE=auto
    scen RW-fast  MODE=SYNC        REPL_MS=20  COMBOS=16,2 RMODE=auto RQROWS=1000000 WMODE=auto
    scen RW-slow  MODE=SYNC        REPL_MS=500 COMBOS=16,2 RMODE=auto RQROWS=1000000 WMODE=auto
    scen PARK     MODE=SYNC        REPL_MS=500 COMBOS=16,1 RMODE=auto RQROWS=1000     WMODE=auto
    sched base
    scen UNIQ_RO  MODE=SYNC        REPL_MS=20  COMBOS=16,0 RMODE=auto RQROWS=1000000 NDDL=1 DDLMODE=readonly
    scen UNIQ_UQ  MODE=SYNC        REPL_MS=20  COMBOS=16,0 RMODE=auto RQROWS=1000000 NDDL=1 DDLMODE=unique
    ;;
  repl)    for v in 50 100 200 500 1000; do sched "repl${v}"    REPL_MS="$v";   done ;;
  stream)  for v in 1 2 4 8;            do sched "stream${v}"  NSTREAM="$v";   done ;;
  storm)   for v in 8 16 32 64;         do sched "storm${v}"   COMBOS="${v},1"; done ;;
  writers) for v in 1 2 4;              do sched "wr${v}"      COMBOS="16,${v}"; done ;;
  workers) for v in 4 8 16;             do sched "nw${v}"      NWORKERS="$v";  done ;;
  mode)    for v in SYNC STRICT_SYNC;   do sched "mode${v}"    MODE="$v";      done ;;
  snq)     for v in 100 1000 100000;    do sched "snq${v}"     SNQ="$v";       done ;;
  *) echo "unknown group: $g" ;;
esac; done
echo "=== sweep_s2e DONE $(date -u) ==="
