#!/usr/bin/env bash
# S2e admission KNOB sweep -- RUN THIS (needs sudo for netns + tc netem).
#
# Sweeps the tunable admission knobs (exposed as startup flags in the s2e_cfg bundle) across the anchor
# scenarios, interleaved+counterbalanced. 'master' is the fixed reference (no flags); s2e_cfg with DEFAULT
# flags (busy2 / idle64 / block=false) reproduces shipped s2e = the control. Every cell reports, per
# bundle: STREAM q/s (SCHED) or READ q/s (RW-fast/UNIQ_RO/PARK), plus main-server cpu=.
#
# Knobs (flag : default) -- see SessionHL admission path:
#   K1  --admission-try-budget-busy-us  : 2     (bounded-try budget when pool has other work)
#   K2  --admission-try-budget-idle-us  : 64    (budget when pool is idle)
#   B1  --admission-block-when-no-work  : false (block like master when nothing to yield to)
#
# Anchor scenarios: RW-fast & UNIQ_RO (the do-no-harm cost we want to recover), SCHED (the win to preserve),
# PARK (pure-contention CPU). Usage:  sudo -v ; ./knob_sweep.sh        KNOBS="k1 b1" ./knob_sweep.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; BINS="$HERE/../bins"
M="$BINS/master/memgraph"; C="$BINS/s2e_cfg/memgraph"
for x in "$M" "$C"; do [ -x "$x" ] || { echo "FATAL: missing $x (build s2e_cfg first)"; exit 1; }; done
export REPS="${REPS:-3}" DUR="${DUR:-20}"
KNOBS="${KNOBS:-k1 k2 b1}"
K1_VALS="${K1_VALS:-2 16 64 256}"; K2_VALS="${K2_VALS:-64 256 1024}"

sudo -v || { echo "FATAL: need sudo"; exit 1; }
( while true; do sudo -n true 2>/dev/null || exit; sleep 50; done ) & KA=$!
trap 'kill $KA 2>/dev/null || true' EXIT
echo "=== knob_sweep start $(date -u)  REPS=$REPS DUR=$DUR knobs=[$KNOBS] ==="
hdr(){ echo; echo "########## $1  $(date -u +%H:%M:%S) ##########"; }

# anchor scenario runners: $1=tag ; rest = bundle specs (label:bin[:flags])
rw_fast(){ env SCENARIO="RWfast[$1]"  MODE=SYNC REPL_MS=20  COMBOS=16,2 RMODE=auto RQROWS=1000000 WMODE=auto \
             "$HERE/phase3_run_ab.sh" "${@:2}"; }
uniq_ro(){ env SCENARIO="UNIQro[$1]"  MODE=SYNC REPL_MS=20  COMBOS=16,0 RMODE=auto RQROWS=1000000 NDDL=1 DDLMODE=readonly \
             "$HERE/phase3_run_ab.sh" "${@:2}"; }
sched(){   env SCENARIO="SCHED[$1]"   MODE=SYNC REPL_MS=500 COMBOS=16,1 NSTREAM=4 SQROWS=100000 SNQ=100000 SMODE=explicit \
             RMODE=auto RQROWS=1 WMODE=auto "$HERE/phase3_run_ab.sh" "${@:2}"; }
park(){    env SCENARIO="PARK[$1]"    MODE=SYNC REPL_MS=500 COMBOS=16,1 RMODE=auto RQROWS=1000 WMODE=auto \
             "$HERE/phase3_run_ab.sh" "${@:2}"; }
run_anchors(){ local tag="$1"; shift; hdr "KNOB $tag"; rw_fast "$tag" "$@"; uniq_ro "$tag" "$@"; sched "$tag" "$@"; park "$tag" "$@"; }

for k in $KNOBS; do case "$k" in
  k1) specs=("master:$M"); for v in $K1_VALS; do specs+=("busy${v}:$C:--admission-try-budget-busy-us=${v}"); done
      run_anchors "K1busy" "${specs[@]}" ;;
  k2) specs=("master:$M"); for v in $K2_VALS; do specs+=("idle${v}:$C:--admission-try-budget-idle-us=${v}"); done
      run_anchors "K2idle" "${specs[@]}" ;;
  b1) run_anchors "B1block" "master:$M" \
        "blkOFF:$C:--admission-block-when-no-work=false" \
        "blkON:$C:--admission-block-when-no-work=true" ;;
  *) echo "unknown knob: $k" ;;
esac; done
echo "=== knob_sweep DONE $(date -u) ==="
