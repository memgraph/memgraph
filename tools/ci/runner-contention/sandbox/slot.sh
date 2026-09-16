#!/bin/bash
# Record start/end with millisecond stamps so real overlap can be reconstructed,
# and echo any CTEST_RESOURCE_GROUP_* variables ctest handed this test.
name=$1
secs=$2
log=${SLOTLOG:-/tmp/slotlog.txt}
echo "START $name $(date +%s.%N)" >> "$log"
env | grep -E '^CTEST_RESOURCE_GROUP' | sed "s/^/VAR $name /" >> "$log"
sleep "$secs"
echo "END   $name $(date +%s.%N)" >> "$log"
