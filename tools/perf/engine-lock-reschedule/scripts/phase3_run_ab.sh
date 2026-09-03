#!/usr/bin/env bash
# Interleaved, counterbalanced A/B phase-3 runner. Fixes the second-slot measurement bias.
#
# The old phase3_run.sh ran ALL reps of binary A, then ALL reps of binary B -- so B always ran ~4 min
# later in each cell and was systematically disadvantaged by whatever the box drifts over that window
# (uncore/mem-controller thermal, allocator/page state) even at a pinned core clock. That produced a
# phantom ~10-12% "regression" that reproduced across UNRELATED binaries (p4662 -12%, p4663 -10.5%)
# and vanished in an isolated same-core perf test -- i.e. a harness artifact, not the binary.
#
# This runner instead INTERLEAVES the two binaries per rep and ALTERNATES which goes first
# (counterbalancing), so any monotonic drift cancels in the A-vs-B median. netns is brought up ONCE
# per cell; only memgraph is restarted per (rep,label). Pass the SAME binary under two labels for a
# master-vs-master control -- a correct harness must report ~0% there.
#
# Args: exactly two "label:binary" pairs.   Env: MODE, REPS(even is ideal), DUR, COMBOS, REPL_MS, cores...
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; USER_NAME="$(id -un)"
LD8="${LD_LIB:-/opt/toolchain-v8/lib:/opt/toolchain-v8/lib64}"
CLIENT_MS="${CLIENT_MS:-0.5}"; REPL_MS="${REPL_MS:-20}"
DUR="${DUR:-15}"; REPS="${REPS:-6}"; SRV_CORES="${SRV_CORES:-0-7}"; CLI_CORES="${CLI_CORES:-8-11}"
COMBOS="${COMBOS:-16,0 16,2 16,4}"; MODE="${MODE:-SYNC}"
CLEAN="--telemetry-enabled=false --log-level=ERROR --storage-wal-enabled=true --storage-gc-cycle-sec=3600 --storage-snapshot-on-exit=false"

LABELS=(); BINS=(); BFLAGS=()   # arg = label:bin[:server-flags]  (per-bundle flags let us A/B flag configs of one binary)
for arg in "$@"; do
  LABELS+=("${arg%%:*}"); rest="${arg#*:}"
  BINS+=("${rest%%:*}")
  if [ "$rest" = "${rest#*:}" ]; then BFLAGS+=(""); else BFLAGS+=("${rest#*:}"); fi   # flags = everything after the 2nd colon (may hold multiple, space-separated)
done
[ "${#BINS[@]}" -ge 2 ] || { echo "need at least two label:binary[:flags] (e.g. master:… s2e:…:--admission-try-budget-busy-us=32)"; exit 1; }
for b in "${BINS[@]}"; do [ -x "$b" ] || { echo "MISSING $b"; exit 1; }; done

DDM=""; DDR=""; MPID=""; RPID=""
start_mg() {  # $1=binary  $2=per-bundle server flags : start main+replica in the (up) netns, register SYNC replica
  local MG="$1" MG_FLAGS="${2:-}"
  DDM="$(mktemp -d "$HERE/ddm.XXXXXX")"; DDR="$(mktemp -d "$HERE/ddr.XXXXXX")"
  sudo ip netns exec mgsrv sudo -u "$USER_NAME" env LD_LIBRARY_PATH="$LD8" taskset -c "$SRV_CORES" \
    "$MG" --bolt-address=10.0.0.2 --bolt-port=7687 --bolt-num-workers="${NWORKERS:-8}" --data-directory="$DDM" $CLEAN $MG_FLAGS ${MGFLAGS:-} >/dev/null 2>&1 &
  MPID=$!
  sudo ip netns exec mgrepl sudo -u "$USER_NAME" env LD_LIBRARY_PATH="$LD8" taskset -c "$SRV_CORES" \
    "$MG" --bolt-address=10.0.1.3 --bolt-port=7688 --bolt-num-workers=4 --data-directory="$DDR" $CLEAN >/dev/null 2>&1 &
  RPID=$!
  local hp _w; for hp in "10.0.0.2 7687" "10.0.1.3 7688"; do set -- $hp
    for _w in $(seq 1 60); do sudo ip netns exec mgsrv bash -c "exec 3<>/dev/tcp/$1/$2" 2>/dev/null && break; sleep 0.5; done; done
  sudo ip netns exec mgsrv sudo -u "$USER_NAME" python3 -c "
from neo4j import GraphDatabase
r=GraphDatabase.driver('bolt://10.0.1.3:7688',auth=None)
with r.session() as s: s.run('SET REPLICATION ROLE TO REPLICA WITH PORT 10001;').consume()
r.close()
m=GraphDatabase.driver('bolt://10.0.0.2:7687',auth=None)
with m.session() as s:
    s.run(\"REGISTER REPLICA r1 $MODE TO '10.0.1.3:10001';\").consume()
    assert list(s.run('SHOW REPLICAS;')), 'no replica'
m.close()" >/dev/null 2>&1 || echo "REGISTER_FAIL"
}
stop_mg() {  # kill this instance's memgraphs, KEEP netns up for the next rep
  sudo kill "$MPID" "$RPID" 2>/dev/null
  local ns Pn; for ns in mgsrv mgrepl; do Pn=$(sudo ip netns pids "$ns" 2>/dev/null); [ -n "$Pn" ] && echo "$Pn"|xargs -r sudo kill -9 2>/dev/null; done
  rm -rf "$DDM" "$DDR"
}
# ---- server-CPU sampling: the s2e distinguisher (park at ~0 CPU vs master's busy-spin) ----
# /proc/<pid>/stat utime+stime is thread-group-aggregated, so it captures all 8 workers + bg threads.
# The main runs as $USER_NAME (same uid as us) so /proc is readable with no sudo.
HZ="$(getconf CLK_TCK 2>/dev/null || echo 100)"
srv_cpu_pid() {  # echo the main-server memgraph pid (the one bound to 10.0.0.2, not the replica); '' if absent
  local p
  for p in $(pgrep -x memgraph 2>/dev/null); do
    tr '\0' ' ' < "/proc/$p/cmdline" 2>/dev/null | grep -q "bolt-address=10.0.0.2" && { echo "$p"; return; }
  done
}
read_jiffies() {  # $1=pid -> utime+stime in clock ticks; '' on failure (comm has no spaces here, so #*) is safe)
  [ -n "${1:-}" ] || { echo ""; return; }
  local st; st="$(cat "/proc/$1/stat" 2>/dev/null)" || { echo ""; return; }
  st="${st#*) }"; set -- $st            # strip "pid (comm) "; now $12=utime(field14) $13=stime(field15)
  echo $(( ${12} + ${13} ))
}
cpu_pct() {  # $1=j0 $2=j1 $3=t0 $4=t1 -> process CPU% over the window (>100% => multiple cores busy)
  { [ -n "$1" ] && [ -n "$2" ]; } || { echo "?"; return; }
  awk -v dj="$(( $2 - $1 ))" -v hz="$HZ" -v t0="$3" -v t1="$4" \
    'BEGIN{ w=t1-t0; if (w>0) printf "%.0f%%", (dj/hz)/w*100; else printf "?" }'
}

run_combos() {  # $1=label : one rep of each combo, sampling the main server's CPU over each load window
  local LABEL="$1" combo R W line srv j0 j1 t0 t1 cpu
  srv="$(srv_cpu_pid)"
  for combo in $COMBOS; do
    R="${combo%%,*}"; W="${combo##*,}"
    j0="$(read_jiffies "$srv")"; t0="$(date +%s.%N)"
    line=$(sudo ip netns exec mgcli sudo -u "$USER_NAME" env \
      RMODE="${RMODE:-explicit}" RQROWS="${RQROWS:-1000000}" NQ="${NQ:-8}" \
      WMODE="${WMODE:-explicit}" WQROWS="${WQROWS:-0}" \
      NDDL="${NDDL:-0}" DDLMODE="${DDLMODE:-readonly}" \
      NSTREAM="${NSTREAM:-0}" SQROWS="${SQROWS:-100000}" SNQ="${SNQ:-100000}" SMODE="${SMODE:-explicit}" taskset -c "$CLI_CORES" \
      python3 -u "$HERE/phase3.py" bolt://10.0.0.2:7687 "$R" "$W" "$DUR" 2>/dev/null | grep -viE "unable to resolve")
    t1="$(date +%s.%N)"; j1="$(read_jiffies "$srv")"
    cpu="$(cpu_pct "$j0" "$j1" "$t0" "$t1")"
    printf '%-22s %-8s %s cpu=%s\n' "${SCENARIO:-} $LABEL" "$MODE" "$line" "$cpu"
  done
}

"$HERE/setup_repl.sh" "$CLIENT_MS" "$REPL_MS" >/dev/null 2>&1
N="${#BINS[@]}"
for rep in $(seq 1 "$REPS"); do
  idxs=""; for k in $(seq 0 $((N-1))); do idxs+=" $(( (k + rep) % N ))"; done   # rotate order each rep (counterbalance N-way)
  for i in $idxs; do
    bin="${BINS[$i]}"; lbl="${LABELS[$i]}"; flg="${BFLAGS[$i]}"   # capture BEFORE start_mg (defensive: never index by $i after a call)
    start_mg "$bin" "$flg"
    run_combos "$lbl"
    stop_mg
  done
done
for ns in mgcli mgsrv mgrepl; do Pn=$(sudo ip netns pids "$ns" 2>/dev/null); [ -n "$Pn" ] && echo "$Pn"|xargs -r sudo kill -9 2>/dev/null; sudo ip netns del "$ns" 2>/dev/null; done
echo "=== PHASE3 DONE ==="
