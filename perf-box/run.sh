#!/usr/bin/env bash
# =============================================================================
# Graph Versioning v1 — branch-read allocation hot-spot investigation
# Run on a real perf box (full PMU + working call-graph unwinding).
#
# GOAL: pinpoint the exact allocation call sites that make a branch query do
#   ~6-8x more mallocs/frees than the identical query on main. On the dev VM we
#   established (via self-time) that the versioning READ logic (ResolveEdges /
#   OutEdges / To) is ~0.2-1% — essentially free — and that the residual
#   branch/main overhead is ALLOCATION + per-alloc memory-tracking hooks
#   (je_malloc, je_sdallocx, operator delete, IsQueryTracked,
#   EnsureJemallocThreadStateInitialized). We could NOT get the allocation
#   CALLERS on the VM (perf dwarf callgraph recursed / LBR unsupported / jemalloc
#   not built with --enable-prof). This script gets them on real hardware.
#
# See README.md for the full write-up, hypotheses, and how to read the output.
# =============================================================================
set -euo pipefail

# ---- config (override via env) ----------------------------------------------
MG="${MG:?set MG=/path/to/build/memgraph (RelWithDebInfo; ideally built with -fno-omit-frame-pointer)}"
HERE="$(cd "$(dirname "$0")" && pwd)"
IDX="${IDX:-$HERE/pokec/memgraph_index.cypher}"
DATA="${DATA:-$HERE/pokec/pokec_small_import.cypher}"
OUT="${OUT:-$HERE/results-$(date +%Y%m%d-%H%M%S)}"
CHURN="${CHURN:-realistic}"          # realistic | worst
CG="${CG:-dwarf}"                    # call-graph mode: dwarf | fp | lbr
EVENT="${EVENT:-cycles}"             # hardware event to sample (cycles on a PMU box; cpu-clock if none)
FREQ="${FREQ:-2999}"                 # sampling frequency
WARMUP="${WARMUP:-8}"                # seconds to let the workload spin up before recording
RECORD="${RECORD:-30}"              # seconds to perf-record
DUR="${DUR:-70}"                     # total workload duration (must exceed WARMUP+RECORD)
BASEPORT="${BASEPORT:-7740}"
PY="${PY:-python3}"                  # must have neo4j driver installed

mkdir -p "$OUT"
echo "MG=$MG  CHURN=$CHURN  CG=$CG  EVENT=$EVENT  OUT=$OUT"

# ---- alloc symbols we want the caller tree for -------------------------------
# (jemalloc-prefixed allocator + Memgraph's global new/delete wrappers)
ALLOC_SYMS=( "je_malloc" "je_mallocx" "newImpl" "operator new" "operator delete" "je_sdallocx" )

quiet_check() {
  echo "== machine load =="; uptime
  local busy
  busy=$(ps -eo pcpu,comm --sort=-pcpu | awk 'NR>1 && $1>20 {print}' | grep -vE "perf|memgraph" || true)
  [ -n "$busy" ] && echo "WARNING: other busy processes:\n$busy" || echo "machine looks quiet"
}

# profile one (mode, query) cell
profile_cell() {
  local mode="$1" query="$2" port="$3"
  local tag="${query}-${mode}"
  local ddir="$OUT/data-$tag" pidf="$OUT/$tag.pid" dlog="$OUT/$tag.driver.log"
  echo "===================================================================="
  echo ">> $tag  (mode=$mode query=$query port=$port)"
  rm -f "$pidf" "$dlog"
  ( cd "$HERE" && $PY driver.py --mg "$MG" --data-dir "$ddir" --index "$IDX" --data "$DATA" \
       --port "$port" --mode "$mode" --query "$query" --churn "$CHURN" --pidfile "$pidf" --duration "$DUR" \
       > "$dlog" 2>&1 ) &
  local dpid=$!
  # wait for the workload loop to start
  for _ in $(seq 1 "${STARTUP_WAIT:-60}"); do grep -q "looping" "$dlog" 2>/dev/null && break; sleep 1; done
  if ! grep -q "looping" "$dlog"; then echo "!! driver failed to start:"; tail -20 "$dlog"; kill $dpid 2>/dev/null || true; return 1; fi
  local mgpid; mgpid=$(cat "$pidf")
  echo "   memgraph pid=$mgpid, warmup ${WARMUP}s ..."; sleep "$WARMUP"

  # (1) hardware-counter summary over a window (needs PMU)
  perf stat -p "$mgpid" -e cycles,instructions,cache-references,cache-misses,LLC-load-misses,dTLB-load-misses \
     -o "$OUT/$tag.stat.txt" -- sleep 10 2>/dev/null || echo "   (perf stat: some counters unavailable)"

  # (2) call-graph record
  local cgopt
  case "$CG" in
    dwarf) cgopt="--call-graph dwarf,16384" ;;
    fp)    cgopt="--call-graph fp" ;;
    lbr)   cgopt="--call-graph lbr" ;;
  esac
  echo "   perf record ($EVENT, $CG) ${RECORD}s ..."
  perf record -F "$FREQ" $cgopt -e "$EVENT" -p "$mgpid" -o "$OUT/$tag.data" -- sleep "$RECORD" 2>"$OUT/$tag.record.log" || true

  # (3) reports: flat self-time + per-alloc-symbol CALLER trees (the key artifact)
  perf report --stdio -g none --no-inline -i "$OUT/$tag.data" 2>/dev/null > "$OUT/$tag.flat.txt" || true
  : > "$OUT/$tag.alloc-callers.txt"
  for sym in "${ALLOC_SYMS[@]}"; do
    echo "########## callers of: $sym ##########" >> "$OUT/$tag.alloc-callers.txt"
    perf report --stdio --no-inline -g graph,0.3,caller --symbol-filter="$sym" -i "$OUT/$tag.data" 2>/dev/null \
        | grep -vE "^#|^\s*$" | head -60 >> "$OUT/$tag.alloc-callers.txt" || true
    echo "" >> "$OUT/$tag.alloc-callers.txt"
  done

  # stop workload
  kill "$dpid" 2>/dev/null || true
  wait "$dpid" 2>/dev/null || true
  echo "   done -> $OUT/$tag.{flat,alloc-callers,stat}.txt"
}

quiet_check
echo

# tiny query (per-query overhead dominates -> highest ratio) AND big traversal
# (amortizes fixed cost -> isolates per-vertex cost). branch vs main each.
P=$BASEPORT
profile_cell main   bfs  $((P+0))
profile_cell branch bfs  $((P+1))
profile_cell main   deep $((P+2))
profile_cell branch deep $((P+3))

echo "===================================================================="
echo "ALL DONE. Key files in $OUT :"
echo "  *.alloc-callers.txt  <- THE ANSWER: what allocates, per mode. Diff branch vs main."
echo "  *.flat.txt           <- self-time hot functions per mode."
echo "  *.stat.txt           <- cycles/instructions/cache-misses (branch vs main)."
echo
echo "Quick diffs to look at first:"
echo "  diff <(grep -A40 'je_malloc' $OUT/deep-main.alloc-callers.txt) \\"
echo "       <(grep -A40 'je_malloc' $OUT/deep-branch.alloc-callers.txt)"
echo "  # the branch-only / branch-heavier CALLER frames above je_malloc are the target."
