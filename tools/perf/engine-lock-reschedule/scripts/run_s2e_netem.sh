#!/usr/bin/env bash
# S2e netem validation driver -- RUN THIS (it needs sudo for netns + tc netem).
#
# Runs master/s2b/s2e 3-way, interleaved and counterbalanced, under a REAL slow replication link so the
# writer's SYNC COMMIT holds engine_lock_ for a FINITE ~1-2s per commit (not a permanent freeze). During
# each hold the 16 readers' BEGIN admission is blocked; the question is what the main's CPU does:
#     master  busy-spins the SpinLock            -> high cpu=
#     s2b     bounded-try then reschedule-spins  -> high cpu=
#     s2e     bounded-try then PARK at ~0 CPU, drained by WakeAllParked when the commit lands -> low cpu=
# The PARK scenarios use CHEAP reads (RQROWS=1000) on purpose, so the main's cpu= is dominated by
# admission behaviour rather than reader query work -- that is where spin-vs-park shows up.
#
# Each scenario stands up its own netns (its own REPL_MS) via phase3_run_ab.sh and prints, per bundle
# per cell: READ q/s, q_p99, WRITE op/s, (DDL op/s), and main-server cpu=.
#
# Usage:  sudo -v ; ./run_s2e_netem.sh                       # full scenario set
#         SCENARIOS="PARK" REPS=4 DUR=20 ./run_s2e_netem.sh   # just the CPU-park probe
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; BINS="$HERE/../bins"
M="$BINS/master/memgraph"; B="$BINS/s2b/memgraph"; E="$BINS/s2e/memgraph"
for x in "$M" "$B" "$E"; do [ -x "$x" ] || { echo "FATAL: missing bundle $x (run build_binaries.sh first)"; exit 1; }; done
TRIO="master:$M s2b:$B s2e:$E"
export REPS="${REPS:-4}" DUR="${DUR:-15}"
SCENARIOS="${SCENARIOS:-RO SCHED PARK PARK_STRICT UNIQ_RO UNIQ_UQ}"

sudo -v || { echo "FATAL: need sudo for netns/netem"; exit 1; }
( while true; do sudo -n true 2>/dev/null || exit; sleep 50; done ) & KA=$!    # keep sudo warm
trap 'kill $KA 2>/dev/null || true' EXIT
echo "=== run_s2e_netem start $(date -u)  REPS=$REPS DUR=$DUR scenarios=[$SCENARIOS] ==="

hdr(){ echo; echo "########## $1  $(date -u +%H:%M:%S) ##########"; }
for sc in $SCENARIOS; do
 case "$sc" in
  RO)          # do-no-harm floor: reads only, no writers -> cpu= MUST be equal across the three
    hdr "RO  (read-only do-no-harm; cpu must match)"
    SCENARIO=RO     MODE=SYNC        REPL_MS=20  COMBOS="16,0" RMODE=auto RQROWS=1000000 WMODE=auto \
      "$HERE/phase3_run_ab.sh" $TRIO ;;
  SCHED)       # THE REAL TEST: productive streamers (B) + admission storm (A) + slow writer. Park can only
               # engage here (productive_pending_>0 && no idle worker). KPI = STREAM rows/s: does real work
               # keep flowing while BEGINs are contended? branch should hold it; master should starve it.
    hdr "SCHED  4 streamers(B) + 16 admission-storm(A) + 1 slow writer -- KPI: STREAM rows/s (master starves, branch flows)"
    SCENARIO=SCHED  MODE=SYNC        REPL_MS=500 COMBOS="16,1" \
      NSTREAM=4 SQROWS=100000 SNQ=100000 SMODE=explicit RMODE=auto RQROWS=1 WMODE=auto \
      "$HERE/phase3_run_ab.sh" $TRIO ;;
  PARK)        # THE PROBE: ~1s finite holds, cheap reads -> cpu= is admission behaviour (spin vs park)
    hdr "PARK  SYNC REPL_MS=500 (~1s holds), cheap reads -- watch cpu=: master/s2b high, s2e low if park works"
    SCENARIO=PARK   MODE=SYNC        REPL_MS=500 COMBOS="16,1 16,2" RMODE=auto RQROWS=1000 WMODE=auto \
      "$HERE/phase3_run_ab.sh" $TRIO ;;
  PARK_STRICT) # STRICT_SYNC holds longer (extra ack phase) -> starker gap if park works
    hdr "PARK  STRICT_SYNC REPL_MS=500 (longer holds), cheap reads"
    SCENARIO=PARKS  MODE=STRICT_SYNC REPL_MS=500 COMBOS="16,1 16,2" RMODE=auto RQROWS=1000 WMODE=auto \
      "$HERE/phase3_run_ab.sh" $TRIO ;;
  UNIQ_RO)     # READ + READ_ONLY DDL (CREATE/DROP INDEX): main_lock_ READ_ONLY excludes WRITE, not READ
    hdr "UNIQ_RO  (READ + READ_ONLY DDL)"
    SCENARIO=UNIQ_RO MODE=SYNC       REPL_MS=20  COMBOS="16,0" RMODE=auto RQROWS=1000000 NDDL=1 DDLMODE=readonly \
      "$HERE/phase3_run_ab.sh" $TRIO ;;
  UNIQ_UQ)     # READ + UNIQUE DDL (DROP ALL CONSTRAINTS): main_lock_ UNIQUE full barrier
    hdr "UNIQ_UQ  (READ + UNIQUE DDL)"
    SCENARIO=UNIQ_UQ MODE=SYNC       REPL_MS=20  COMBOS="16,0" RMODE=auto RQROWS=1000000 NDDL=1 DDLMODE=unique \
      "$HERE/phase3_run_ab.sh" $TRIO ;;
  *) echo "unknown scenario: $sc" ;;
 esac
done
echo "=== run_s2e_netem DONE $(date -u) ==="
