#!/bin/bash
# Stage 2: Get event statistics overview

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RECORDING="$SCRIPT_DIR/findings_1/memgraph_session.undo"
OUTPUT_DIR="$SCRIPT_DIR/findings_2"
UDB=~/Downloads/Undo-Suite-Corporate-Multiarch-9.1.0/udb

echo "=== Stage 2: Event Statistics Overview ==="

cat > /tmp/udb_stats.gdb << 'CMDS'
set pagination off
info event-stats
quit
CMDS

$UDB --batch -x /tmp/udb_stats.gdb "$RECORDING" 2>&1 | tee "$OUTPUT_DIR/event_stats.txt"

echo "Event statistics saved to: $OUTPUT_DIR/event_stats.txt"
