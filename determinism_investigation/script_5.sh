#!/bin/bash
# Stage 5: Investigate thread switching patterns

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RECORDING="$SCRIPT_DIR/findings_1/memgraph_session.undo"
OUTPUT_DIR="$SCRIPT_DIR/findings_5"
UDB=~/Downloads/Undo-Suite-Corporate-Multiarch-9.1.0/udb

echo "=== Stage 5: Thread Switching Analysis ==="

# NEWTHREAD investigation
cat > /tmp/udb_threads.gdb << 'CMDS'
set pagination off
info events name == "NEWTHREAD"
quit
CMDS

echo "Listing NEWTHREAD events..."
$UDB --batch -x /tmp/udb_threads.gdb "$RECORDING" 2>&1 | tee "$OUTPUT_DIR/newthread_events.txt"

# futex investigation
cat > /tmp/udb_futex.gdb << 'CMDS'
set pagination off
ugo end
ugo event prev name == "futex"
bt 15
ugo event prev name == "futex"
bt 15
ugo event prev name == "futex"
bt 15
quit
CMDS

echo "Capturing futex backtraces..."
$UDB --batch -x /tmp/udb_futex.gdb "$RECORDING" 2>&1 | tee "$OUTPUT_DIR/futex_traces.txt"

echo "Traces saved to: $OUTPUT_DIR/"
