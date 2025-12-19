#!/bin/bash
# Stage 4: Investigate getrandom call stacks

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RECORDING="$SCRIPT_DIR/findings_1/memgraph_session.undo"
OUTPUT_DIR="$SCRIPT_DIR/findings_4"
UDB=~/Downloads/Undo-Suite-Corporate-Multiarch-9.1.0/udb

echo "=== Stage 4: RNG Investigation ==="

# getrandom investigation
cat > /tmp/udb_random.gdb << 'CMDS'
set pagination off
ugo start
info events name == "getrandom"
ugo event next name == "getrandom"
bt 30
ugo event next name == "getrandom"
bt 30
ugo event next name == "getrandom"
bt 30
ugo event next name == "getrandom"
bt 30
ugo event next name == "getrandom"
bt 30
quit
CMDS

echo "Capturing getrandom backtraces..."
$UDB --batch -x /tmp/udb_random.gdb "$RECORDING" 2>&1 | tee "$OUTPUT_DIR/getrandom_traces.txt"

echo "Traces saved to: $OUTPUT_DIR/"
