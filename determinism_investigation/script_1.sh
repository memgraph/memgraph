#!/bin/bash
# Stage 1: Record memgraph execution with test queries

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MEMGRAPH_DIR="$(dirname "$SCRIPT_DIR")"
RECORDING_FILE="$SCRIPT_DIR/findings_1/memgraph_session.undo"
LIVE_RECORD=~/Downloads/Undo-Suite-Corporate-Multiarch-9.1.0/live-record

echo "=== Stage 1: Recording Memgraph Execution ==="
echo "Recording file: $RECORDING_FILE"

# Remove any existing recording
rm -f "$RECORDING_FILE"

# Start memgraph under live-record in background
echo "Starting memgraph under live-record..."
$LIVE_RECORD \
    --recording-file "$RECORDING_FILE" \
    --verbose \
    "$MEMGRAPH_DIR/build/memgraph" \
    --cartesian-product-enabled false \
    --data-recovery-on-startup false \
    --debug-query-plans true \
    --log-level DEBUG \
    --schema-info-enabled true \
    --storage-enable-edges-metadata true \
    --storage-enable-schema-metadata true \
    --storage-gc-aggressive false \
    --storage-properties-on-edges true \
    --storage-snapshot-on-exit false \
    --storage-wal-enabled false \
    --telemetry-enabled false \
    --query-modules-directory "$MEMGRAPH_DIR/build/query_modules" &

RECORD_PID=$!

echo "Waiting for memgraph to start..."
sleep 5

# Run test queries
echo "Running test queries..."
echo "CREATE (a:Person {name: 'Alice', age: 30});" | mgconsole --output-format=tabular
echo "CREATE (b:Person {name: 'Bob', age: 25});" | mgconsole --output-format=tabular
echo "CREATE (c:Person {name: 'Charlie', age: 35});" | mgconsole --output-format=tabular
echo "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b);" | mgconsole --output-format=tabular
echo "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) CREATE (b)-[:KNOWS]->(c);" | mgconsole --output-format=tabular
echo "MATCH (p:Person) RETURN p.name, p.age;" | mgconsole --output-format=tabular
echo "MATCH (a)-[r:KNOWS]->(b) RETURN a.name, b.name;" | mgconsole --output-format=tabular

echo "Queries complete. Stopping recording..."

# Find controller PID from live-record output and send SIGINT
# Alternative: just kill the background process
kill -INT $RECORD_PID 2>/dev/null || true
wait $RECORD_PID 2>/dev/null || true

echo "Recording saved to: $RECORDING_FILE"
ls -lh "$RECORDING_FILE"
