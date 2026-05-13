#!/usr/bin/env bash
# Profile EdgeUniquenessFilter under a dense 4-hop MATCH query.
#
# Usage: ./profile_edge_uniqueness.sh [output-dir]
#
# Requires: perf, mgconsole, ~/FlameGraph/stackcollapse-perf.pl
#
# What it does:
#   1. Starts a throw-away Memgraph instance (no auth, temp data dir)
#   2. Loads a dense directed graph: 150 vertices, all-pairs edges (~22k edges)
#   3. Runs PROFILE on the 4-hop query to get operator timing
#   4. perf-records Memgraph while a second timed query runs
#   5. Generates perf.data, a collapsed stack, and a flamegraph SVG
#   6. Kills Memgraph and removes temp data

set -euo pipefail

MG_BINARY="/home/gareth/checkout/memgraph2/build/memgraph"
MGCONSOLE="/usr/bin/mgconsole"
FLAMEGRAPH_DIR="$HOME/FlameGraph"
OUTPUT_DIR="${1:-/tmp/euf_profile_$(date +%Y%m%d_%H%M%S)}"

mkdir -p "$OUTPUT_DIR"

DATA_DIR="$OUTPUT_DIR/mg_data"
mkdir -p "$DATA_DIR"

echo "==> Output dir: $OUTPUT_DIR"
echo "==> Data dir:   $DATA_DIR"

# ---------- 1. start Memgraph ----------------------------------------------------
"$MG_BINARY" \
    --data-directory="$DATA_DIR" \
    --log-file="$OUTPUT_DIR/memgraph.log" \
    --bolt-port=7698 \
    --also-log-to-stderr=false \
    --log-level=WARNING \
    &
MG_PID=$!
echo "==> Memgraph PID: $MG_PID"

wait_for_mg() {
    local retries=30
    while ! mgconsole --port=7698 --output_format=csv <<< "RETURN 1;" >/dev/null 2>&1; do
        retries=$((retries - 1))
        if [[ $retries -le 0 ]]; then
            echo "ERROR: Memgraph did not start in time" >&2
            kill "$MG_PID" 2>/dev/null || true
            exit 1
        fi
        sleep 0.5
    done
}

echo "==> Waiting for Memgraph..."
wait_for_mg
echo "==> Memgraph ready."

run_query() {
    mgconsole --port=7698 --output_format=tabular "$@"
}

# ---------- 2. load dense graph --------------------------------------------------
# 150 vertices, all-pairs directed edges: 150*149 = 22,350 edges.
# Label first vertex :Start so we can anchor the query.
# Using UNWIND+MERGE to do it in a single transaction rather than 22k round-trips.
echo "==> Creating graph..."
run_query <<'CYPHER'
// Create 150 vertices, label vertex 0 :Start
UNWIND range(0, 149) AS i
CREATE (:V {id: i});
CYPHER

run_query <<'CYPHER'
MATCH (v:V {id: 0}) SET v:Start;
CYPHER

# Create all-pairs directed edges via cross-join MATCH (batched to avoid OOM)
run_query <<'CYPHER'
MATCH (a:V), (b:V) WHERE a.id <> b.id
CREATE (a)-[:E]->(b);
CYPHER

run_query <<'CYPHER'
MATCH ()-[e]->() RETURN count(e) AS edge_count;
CYPHER

# ---------- 3. PROFILE run -------------------------------------------------------
echo ""
echo "==> PROFILE output (operator-level timing):"
run_query <<'CYPHER'
PROFILE MATCH (a:Start)-[:E]->()-[:E]->()-[:E]->()-[:E]->(b)
RETURN count(DISTINCT b) AS reachable;
CYPHER

# ---------- 4. perf record while the hot query runs ------------------------------
echo ""
echo "==> Starting perf record on PID $MG_PID..."

# Attach perf in background; it records until we send SIGINT to it.
# -F 999:  ~1kHz sampling - enough for a 400ms window without too much overhead
# --call-graph fp: frame-pointer unwind (RelWithDebInfo builds with -fno-omit-frame-pointer).
# ~10x faster post-processing than dwarf on AMD Zen3 (no LBR).
perf record \
    -F 999 \
    -p "$MG_PID" \
    --call-graph fp \
    -o "$OUTPUT_DIR/perf.data" \
    &
PERF_PID=$!

# Give perf time to attach before we start the workload
sleep 0.5

echo "==> Running hot query under perf..."
for i in 1 2 3; do
    run_query --output_format=csv <<'CYPHER' > /dev/null
MATCH (a:Start)-[:E]->()-[:E]->()-[:E]->()-[:E]->(b)
RETURN count(DISTINCT b) AS reachable;
CYPHER
    echo "    run $i done"
done

# Stop perf cleanly - SIGINT causes it to flush and write perf.data
kill -SIGINT "$PERF_PID" 2>/dev/null || true
wait "$PERF_PID" 2>/dev/null || true
echo "==> perf.data written."

# ---------- 5. generate flamegraph -----------------------------------------------
echo "==> Generating flamegraph..."

perf script -i "$OUTPUT_DIR/perf.data" \
    | "$FLAMEGRAPH_DIR/stackcollapse-perf.pl" \
    > "$OUTPUT_DIR/stacks.folded"

"$FLAMEGRAPH_DIR/flamegraph.pl" \
    --title "EdgeUniquenessFilter 4-hop query" \
    --width 1600 \
    "$OUTPUT_DIR/stacks.folded" \
    > "$OUTPUT_DIR/flamegraph.svg"

# Also generate a focused flamegraph rooted at EdgeUniquenessFilter
grep "EdgeUniquenessFilter\|ContainsSameEdge\|Pull\|expansion_ok" \
    "$OUTPUT_DIR/stacks.folded" \
    > "$OUTPUT_DIR/stacks_euf.folded" 2>/dev/null || true

if [[ -s "$OUTPUT_DIR/stacks_euf.folded" ]]; then
    "$FLAMEGRAPH_DIR/flamegraph.pl" \
        --title "EdgeUniquenessFilter focus" \
        --width 1600 \
        "$OUTPUT_DIR/stacks_euf.folded" \
        > "$OUTPUT_DIR/flamegraph_euf_focus.svg"
fi

# Text summary: top functions by self time
echo ""
echo "==> Top 30 functions by self time:"
perf report \
    -i "$OUTPUT_DIR/perf.data" \
    --stdio \
    --no-children \
    --sort=dso,symbol \
    2>/dev/null | head -60

# ---------- 6. cleanup -----------------------------------------------------------
echo ""
echo "==> Stopping Memgraph..."
kill "$MG_PID" 2>/dev/null || true
wait "$MG_PID" 2>/dev/null || true

echo ""
echo "==> Done. Artifacts in $OUTPUT_DIR:"
ls -lh "$OUTPUT_DIR"/*.{svg,data,folded} 2>/dev/null || true
echo ""
echo "Open flamegraph:     xdg-open $OUTPUT_DIR/flamegraph.svg"
echo "perf TUI:            perf report -i $OUTPUT_DIR/perf.data --call-graph graph"
