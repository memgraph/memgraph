#!/usr/bin/env bash
# Localhost (NO sudo, NO netns, NO netem) A/B/C validator for the S2e engine-lock work.
#
# It runs each bundle through the three workload mixes grounded in the main_lock_ access lattice
# (utils/resource_lock.hpp) and the interpreter's RWType map (query/interpreter.cpp):
#   RO       READ only ................. do-no-harm floor; s2e CPU+throughput must equal master
#   RW       READ + WRITE .............. writer COMMIT holds engine_lock_ across the SYNC replica ack;
#                                        readers' BEGIN admission collides -> the reschedule/park case
#   UNIQ_RO  READ + READ_ONLY DDL ...... CREATE/DROP INDEX takes READ_ONLY on main_lock_ (excludes WRITE)
#   UNIQ_UQ  READ + UNIQUE  DDL ........ DROP ALL CONSTRAINTS takes UNIQUE (excludes everyone)
# and samples the MAIN server's CPU over every load window -- the s2e distinguisher (park at ~0 CPU vs
# master's busy-spin / s2b's blocked worker). /proc/<pid>/stat utime+stime is thread-group-aggregated.
#
# STALL probe: a stuck writer (its SYNC COMMIT frozen by SIGSTOP-ing the replica) holds engine_lock_;
# readers then try to BEGIN. We sample main CPU during the freeze and read the readers' fate:
#   master  -> readers busy-spin (high CPU), hang until we SIGCONT
#   s2e     -> readers park (~0 CPU) and fail-fast at storage_access_timeout_sec (~10s) with ERR
#
# Usage:  ./local_validate.sh                          # master s2b s2e from ../bins, all mixes + stall
#         DUR=15 REPS=3 MIXES="RO RW" ./local_validate.sh master:/p/mg s2e:/p/mg
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; BINS_ROOT="$HERE/../bins"
LD="${LD_LIB:-/opt/toolchain-v8/lib:/opt/toolchain-v8/lib64}"
DUR="${DUR:-15}"; REPS="${REPS:-3}"; SRV_CORES="${SRV_CORES:-0-7}"; CLI_CORES="${CLI_CORES:-8-11}"
NWORKERS="${NWORKERS:-8}"; STALL_SECS="${STALL_SECS:-15}"; MIXES="${MIXES:-RO RW UNIQ_RO UNIQ_UQ STALL}"
HZ="$(getconf CLK_TCK 2>/dev/null || echo 100)"
CLEAN="--telemetry-enabled=false --log-level=ERROR --storage-wal-enabled=true --storage-gc-cycle-sec=3600 --storage-snapshot-on-exit=false"

declare -a LBL BIN
if [ "$#" -gt 0 ]; then for a in "$@"; do LBL+=("${a%%:*}"); BIN+=("${a#*:}"); done
else for l in master s2b s2e; do [ -x "$BINS_ROOT/$l/memgraph" ] && { LBL+=("$l"); BIN+=("$BINS_ROOT/$l/memgraph"); }; done; fi
[ "${#BIN[@]}" -ge 1 ] || { echo "no bundles (build first, or pass label:bin args)"; exit 1; }
for b in "${BIN[@]}"; do [ -x "$b" ] || { echo "MISSING $b"; exit 1; }; done

read_jiffies(){ [ -n "${1:-}" ]||{ echo;return;}; local st; st="$(cat /proc/$1/stat 2>/dev/null)"||{ echo;return;}; st="${st#*) }"; set -- $st; echo $(( ${12}+${13} )); }
cpu_pct(){ { [ -n "$1" ]&&[ -n "$2" ]; }||{ echo "?";return;}; awk -v dj="$(( $2-$1 ))" -v hz="$HZ" -v t0="$3" -v t1="$4" 'BEGIN{w=t1-t0; if(w>0)printf "%.0f%%",(dj/hz)/w*100; else printf "?"}'; }
wait_up(){ local i; for i in $(seq 1 60); do python3 -c "from neo4j import GraphDatabase
d=GraphDatabase.driver('bolt://127.0.0.1:$1',auth=None)
with d.session() as s: s.run('RETURN 1').consume()
d.close()" 2>/dev/null && return 0; kill -0 "${2:-$$}" 2>/dev/null || return 1; sleep 0.5; done; return 1; }

MPID=""; RPID=""; DDM=""; DDR=""
start_pair(){ # $1 bin  $2 port  $3 rport : start main+replica on localhost, register SYNC replica
  DDM="$(mktemp -d)"; DDR="$(mktemp -d)"
  LD_LIBRARY_PATH="$LD" taskset -c "$SRV_CORES" "$1" --bolt-address=127.0.0.1 --bolt-port="$2" \
    --bolt-num-workers="$NWORKERS" --data-directory="$DDM" $CLEAN >"$DDM/log" 2>&1 & MPID=$!
  LD_LIBRARY_PATH="$LD" taskset -c "$SRV_CORES" "$1" --bolt-address=127.0.0.1 --bolt-port="$3" \
    --bolt-num-workers=4 --data-directory="$DDR" $CLEAN >"$DDR/log" 2>&1 & RPID=$!
  wait_up "$2" "$MPID" || { echo "main_down"; return 1; }
  wait_up "$3" "$RPID" || { echo "repl_down"; return 1; }
  python3 -c "from neo4j import GraphDatabase
r=GraphDatabase.driver('bolt://127.0.0.1:$3',auth=None)
with r.session() as s: s.run('SET REPLICATION ROLE TO REPLICA WITH PORT 10001;').consume()
r.close()
m=GraphDatabase.driver('bolt://127.0.0.1:$2',auth=None)
with m.session() as s:
    s.run(\"REGISTER REPLICA r1 SYNC TO '127.0.0.1:10001';\").consume(); assert list(s.run('SHOW REPLICAS;'))
m.close()" 2>/dev/null || { echo "regfail"; return 1; }
}
stop_pair(){ kill -9 $MPID $RPID 2>/dev/null; wait $MPID $RPID 2>/dev/null; rm -rf "$DDM" "$DDR"; }

load(){ # $1 port $2 R $3 W : one CPU-sampled load window against the running main; env RMODE/... via caller
  local j0 j1 t0 t1 line
  j0="$(read_jiffies "$MPID")"; t0="$(date +%s.%N)"
  line=$(taskset -c "$CLI_CORES" env RMODE="${RMODE:-auto}" RQROWS="${RQROWS:-130000}" NQ="${NQ:-8}" \
      WMODE="${WMODE:-auto}" WQROWS="${WQROWS:-0}" NDDL="${NDDL:-0}" DDLMODE="${DDLMODE:-readonly}" \
      python3 -u "$HERE/phase3.py" "bolt://127.0.0.1:$1" "$2" "$3" "$DUR" 2>/dev/null)
  t1="$(date +%s.%N)"; j1="$(read_jiffies "$MPID")"
  echo "$line cpu=$(cpu_pct "$j0" "$j1" "$t0" "$t1")"
}

run_mix(){ # $1 mix : run every bundle once through the mix (bundle order rotates per rep to counterbalance)
  local mix="$1" R W port idx line rep k n="${#BIN[@]}"
  # load() reads these as shell vars (same scope); set per mix
  RMODE=auto WMODE=auto NDDL=0 DDLMODE=readonly
  case "$mix" in
    RO)      R=16 W=0;;
    RW)      R=16 W=2;;
    UNIQ_RO) R=16 W=0 NDDL=1 DDLMODE=readonly;;
    UNIQ_UQ) R=16 W=0 NDDL=1 DDLMODE=unique;;
    *) echo "unknown mix $mix"; return;;
  esac
  for rep in $(seq 1 "$REPS"); do
    for ((k=0;k<n;k++)); do
      idx=$(( (k + rep) % n ))                       # rotate which bundle leads each rep (counterbalance)
      port=$((18800 + rep*10 + idx))
      start_pair "${BIN[$idx]}" "$port" "$((port+1000))" || { echo "$mix ${LBL[$idx]} setup_fail"; stop_pair; continue; }
      line=$(load "$port" "$R" "$W")
      printf '%-9s %-8s rep%d %s\n' "$mix" "${LBL[$idx]}" "$rep" "$line"
      stop_pair; sleep 1
    done
  done
}

stall_probe(){ # $1 bin $2 label : freeze replica mid-commit, sample main CPU while readers try to BEGIN
  local port=18760 rport=19760 j0 j1 t0 t1 rf
  start_pair "$1" "$port" "$rport" || { echo "STALL ${2}: setup_fail"; stop_pair; return; }
  ( taskset -c "$CLI_CORES" env WMODE=auto python3 -u "$HERE/phase3.py" "bolt://127.0.0.1:$port" 0 1 \
      "$((STALL_SECS+20))" 2>/dev/null >"$DDM/wload" ) & local wl=$!
  sleep 2
  kill -STOP "$RPID"                                  # freeze replica -> the in-flight SYNC COMMIT hangs, holding engine_lock_
  sleep 1
  rf="$DDM/rload"
  ( taskset -c "$CLI_CORES" env RMODE=auto python3 -u "$HERE/phase3.py" "bolt://127.0.0.1:$port" 8 0 \
      "$STALL_SECS" 2>/dev/null >"$rf" ) & local rl=$!
  j0="$(read_jiffies "$MPID")"; t0="$(date +%s.%N)"
  sleep "$STALL_SECS"
  t1="$(date +%s.%N)"; j1="$(read_jiffies "$MPID")"
  printf 'STALL     %-8s main-cpu-during-%ds-freeze=%s\n' "$2" "$STALL_SECS" "$(cpu_pct "$j0" "$j1" "$t0" "$t1")"
  kill -CONT "$RPID"                                  # unfreeze -> drain
  wait "$rl" 2>/dev/null; wait "$wl" 2>/dev/null
  printf 'STALL     %-8s readers-under-stall: %s\n' "$2" "$(cat "$rf" 2>/dev/null | tr -s ' ')"
  stop_pair; sleep 1
}

echo "=== local_validate $(date -u +%H:%M:%S)  bundles=[${LBL[*]}]  DUR=${DUR} REPS=${REPS} mixes=[${MIXES}] srv[$SRV_CORES] cli[$CLI_CORES] ==="
for mix in $MIXES; do
  echo "----- mix: $mix -----"
  if [ "$mix" = STALL ]; then
    for ((i=0;i<${#BIN[@]};i++)); do stall_probe "${BIN[$i]}" "${LBL[$i]}"; done
  else
    run_mix "$mix"
  fi
done
echo "=== local_validate DONE $(date -u +%H:%M:%S) ==="
