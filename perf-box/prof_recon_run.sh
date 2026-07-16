#!/usr/bin/env bash
# Profile the read-path reconciliation cost: {main, branch-worst} x {aggregate, expansion4}.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
MG="${MG:?set MG}"
OUT="${OUT:-$HERE/results-recon-$(date +%H%M%S)}"
PY="${PY:-python3}"
WARMUP="${WARMUP:-10}"; RECORD="${RECORD:-30}"; DUR="${DUR:-70}"; FREQ="${FREQ:-2999}"
export NUM_VERTICES="${NUM_VERTICES:-100000}"
mkdir -p "$OUT"; echo "OUT=$OUT"

# reconciliation + allocation symbols to get caller trees for
SYMS=( "je_malloc" "operator new" "ResolveEdges" "ResolveVertex" "SeekNext" "UnionVertices" "branched" )

cell() {
  local mode="$1" query="$2" port="$3"
  local tag="${query}-${mode}"
  local pidf="$OUT/$tag.pid"
  local dlog="$OUT/$tag.driver.log"
  echo ">> $tag"
  ( cd "$HERE" && $PY prof_recon.py --mg "$MG" --data-dir "$OUT/data-$tag" --port "$port" \
      --mode "$mode" --query "$query" --churn worst --pidfile "$pidf" --duration "$DUR" > "$dlog" 2>&1 ) &
  local dpid=$!
  for _ in $(seq 1 180); do grep -q "looping" "$dlog" 2>/dev/null && break; sleep 1; done
  if ! grep -q "looping" "$dlog"; then echo "!! failed:"; tail -15 "$dlog"; kill $dpid 2>/dev/null||true; return 1; fi
  local mgpid; mgpid=$(cat "$pidf"); echo "   pid=$mgpid warmup ${WARMUP}s"; sleep "$WARMUP"
  perf stat -p "$mgpid" -e cycles,instructions,cache-misses,LLC-load-misses,dTLB-load-misses \
     -o "$OUT/$tag.stat.txt" -- sleep 10 2>/dev/null || echo "   (stat partial)"
  echo "   record ${RECORD}s"
  perf record -F "$FREQ" --call-graph dwarf,16384 -e cycles -p "$mgpid" -o "$OUT/$tag.data" -- sleep "$RECORD" 2>"$OUT/$tag.rec.log" || true
  perf report --stdio -g none --no-inline -i "$OUT/$tag.data" 2>/dev/null > "$OUT/$tag.flat.txt" || true
  : > "$OUT/$tag.callers.txt"
  for s in "${SYMS[@]}"; do
    echo "########## callers of: $s ##########" >> "$OUT/$tag.callers.txt"
    perf report --stdio --no-inline -g graph,0.5,caller --symbol-filter="$s" -i "$OUT/$tag.data" 2>/dev/null \
      | grep -vE "^#|^\s*$" | head -40 >> "$OUT/$tag.callers.txt" || true
    echo >> "$OUT/$tag.callers.txt"
  done
  kill "$dpid" 2>/dev/null || true; wait "$dpid" 2>/dev/null || true
  echo "   done $tag"
}

cell main       aggregate  7770
cell branch     aggregate  7771
cell main       expansion4 7772
cell branch     expansion4 7773
echo "ALL DONE -> $OUT"
