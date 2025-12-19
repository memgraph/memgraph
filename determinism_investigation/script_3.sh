#!/bin/bash
# Stage 3: Investigate time operations call stacks

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RECORDING="$SCRIPT_DIR/findings_1/memgraph_session.undo"
OUTPUT_DIR="$SCRIPT_DIR/findings_3"
UDB=~/Downloads/Undo-Suite-Corporate-Multiarch-9.1.0/udb

echo "=== Stage 3: Time Operations Investigation ==="

# clock_gettime investigation
cat > /tmp/udb_clock.gdb << 'CMDS'
set pagination off
ugo end
ugo event prev name == "clock_gettime"
bt 20
ugo event prev name == "clock_gettime"
bt 20
ugo event prev name == "clock_gettime"
bt 20
ugo event prev name == "clock_gettime"
bt 20
ugo event prev name == "clock_gettime"
bt 20
quit
CMDS

echo "Capturing clock_gettime backtraces..."
$UDB --batch -x /tmp/udb_clock.gdb "$RECORDING" 2>&1 | tee "$OUTPUT_DIR/clock_gettime_traces.txt"

# gettimeofday investigation
cat > /tmp/udb_tod.gdb << 'CMDS'
set pagination off
ugo end
ugo event prev name == "gettimeofday"
bt 20
ugo event prev name == "gettimeofday"
bt 20
ugo event prev name == "gettimeofday"
bt 20
ugo event prev name == "gettimeofday"
bt 20
ugo event prev name == "gettimeofday"
bt 20
quit
CMDS

echo "Capturing gettimeofday backtraces..."
$UDB --batch -x /tmp/udb_tod.gdb "$RECORDING" 2>&1 | tee "$OUTPUT_DIR/gettimeofday_traces.txt"

echo "Traces saved to: $OUTPUT_DIR/"
