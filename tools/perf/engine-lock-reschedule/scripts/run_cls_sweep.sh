#!/usr/bin/env bash
# CLS lock-free-read-snapshot A/B sweep — RUN WITH SUDO (needs netns + tc netem).
#
#   sudo -v ; ./run_cls_sweep.sh            # full set, ~45 min, tees to ~/perf-res.cls
#   SWEEP_GROUPS="control SCHED U4" REPS=4 DUR=15 ./run_cls_sweep.sh
#
# Same cls binary, flag ON vs OFF (per-bundle flags in phase3_run_ab.sh), interleaved+counterbalanced,
# with server-CPU sampling. A shared --storage-access-timeout-sec is applied to BOTH slots so a starved
# admission fail-fasts instead of hanging (fair; only affects the DDL groups).
#
# NB — the cls binary is the interim-fixed build: UNIQUE/READ_ONLY BEGIN blocks (park gated to READ/WRITE)
# to avoid the known main_lock park deadlock. So U3ro/U3uq measure the interim BLOCK, not the target park;
# control/RO/RW/SCHED/U4 measure the real feature (READ/WRITE park + commit_mutex park).
#
# Signal (per REPORT §0): flag ON keeps READ txn/s + STREAM q/s up and bounds q_p99 under contention;
# OFF floors them. DDL op/s and write op/s should be ~equal A/B. control must read ~0% A/B.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
BIN="$HERE/../bins/cls/memgraph"
[ -x "$BIN" ] || { echo "FATAL: missing $BIN (build_binaries.sh with the cls target first)"; exit 1; }
export REPS="${REPS:-4}" DUR="${DUR:-15}"
TIMEOUT="${TIMEOUT:-5}"
# U3ro/U3uq dropped from the default: on the interim binary they WEDGE (parked-reader continuation-drop,
# not just the UNIQUE pending leak — the park path itself is unsafe under sustained contention), and they
# only measure the interim BLOCK anyway. Add them back explicitly if you accept that:  SWEEP_GROUPS="... U3uq"
SWEEP_GROUPS="${SWEEP_GROUPS:-control RO RW SCHED U4}"

COMMON="--storage-access-timeout-sec=${TIMEOUT}"
ON="on:$BIN:--experimental-enabled=lockfree-read-snapshot ${COMMON}"
OFF="off:$BIN:${COMMON}"

sudo -v || { echo "FATAL: need sudo for netns/netem"; exit 1; }
( while true; do sudo -n true 2>/dev/null || exit; sleep 50; done ) & KA=$!
trap 'kill $KA 2>/dev/null || true' EXIT
echo "=== run_cls_sweep start $(date -u)  REPS=$REPS DUR=$DUR timeout=${TIMEOUT}s groups=[$SWEEP_GROUPS] ==="
hdr(){ echo; echo "########## $1  $(date -u +%H:%M:%S) ##########"; }
ab(){ local tag="$1"; shift; env SCENARIO="$tag" "$@" "$HERE/phase3_run_ab.sh" "$ON" "$OFF"; }

for g in $SWEEP_GROUPS; do case "$g" in
  control)  # no contention — MUST read ~0% A/B (the noise floor)
    hdr "control (no contention; ~0% A/B expected)"
    ab control MODE=SYNC REPL_MS=20 COMBOS="16,0" RMODE=auto RQROWS=1000000 WMODE=auto ;;
  RO)       # read-only do-no-harm (heavy reads)
    hdr "RO (do-no-harm)"
    ab RO      MODE=SYNC REPL_MS=20 COMBOS="16,0" RMODE=auto RQROWS=1000000 ;;
  RW)       # read + slow write commits (commit_mutex contention)
    hdr "RW (readers vs slow SYNC write commits)"
    ab RW      MODE=SYNC REPL_MS=500 COMBOS="16,2 16,4" RMODE=auto RQROWS=1000000 WMODE=auto ;;
  SCHED)    # productive streamers + admission storm + slow writer — the READ/WRITE park + commit win
    hdr "SCHED (4 streamers + storm + slow writer — does productive work keep flowing?)"
    ab SCHED   MODE=SYNC REPL_MS=500 COMBOS="16,1" NSTREAM=4 SQROWS=100000 SNQ=100000 SMODE=explicit \
               RMODE=auto RQROWS=1 WMODE=auto ;;
  U4)       # commit-lock focus: writers hold commit_mutex across slow commits; streamers = productive work
    hdr "U4 commit-lock (writers + streamers; REPORT §0)"
    ab U4      MODE=SYNC REPL_MS=500 COMBOS="16,2 16,4" NSTREAM=4 SQROWS=100000 SNQ=100000 SMODE=explicit \
               RMODE=auto RQROWS=1000000 WMODE=auto ;;
  U3ro)     # main-lock READ_ONLY DDL (CREATE/DROP INDEX) + streamers  [interim: blocks]
    hdr "U3 main-lock READ_ONLY DDL + streamers  [interim BLOCK]"
    ab U3ro    MODE=SYNC REPL_MS=20 COMBOS="32,0" NDDL=1 DDLMODE=readonly NSTREAM=4 SQROWS=100000 SNQ=1000 \
               SMODE=explicit RMODE=auto RQROWS=1 ;;
  U3uq)     # main-lock UNIQUE DDL (DROP ALL CONSTRAINTS) + streamers  [interim: blocks]
    hdr "U3 main-lock UNIQUE DDL + streamers  [interim BLOCK]"
    ab U3uq    MODE=SYNC REPL_MS=20 COMBOS="32,0" NDDL=1 DDLMODE=unique NSTREAM=4 SQROWS=100000 SNQ=1000 \
               SMODE=explicit RMODE=auto RQROWS=1 ;;
  *) echo "unknown group: $g" ;;
esac; done
echo "=== run_cls_sweep DONE $(date -u) ==="
