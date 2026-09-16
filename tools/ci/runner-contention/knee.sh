#!/bin/bash
# Find the concurrency knee for device-heavy tests, which is the slot count a
# "disk" resource should declare.
#
# Every test in this subset would claim one disk slot, so running the subset at
# ctest -jN is exactly equivalent to declaring N slots. That makes the knee
# measurable without editing any CMakeLists.
#
# The subset is the highest sustained-write-rate tests (device pressure), kept
# short so the matrix is affordable under an emulated slow device. Start
# io_load.sh first to emulate the slow runner class; without it this box's NVMe
# has too much headroom to show a knee.
#
# One level per invocation, popped from levels.txt, because long runs get killed
# once accumulated page cache pins MemFree near zero.
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
BUILD=${BUILD:-$REPO/build}
OUT=$HERE/knee.csv
LEVELS=$HERE/levels.txt

SEL='memgraph__unit__(storage_v2|storage_v2_wal_file|rpc_file_framing|dbms_handler|storage_v2_isolation_level|cpp_api|query_dump|transaction_queue|storage_v2_decoder_encoder|storage_v2_edge_ondisk)$'

[ -s "$LEVELS" ] || { echo "NO LEVELS LEFT"; exit 0; }
[ -f "$OUT" ] || echo "slots,tag,total_s,max_test_s,sum_test_s" > "$OUT"

read -r slots tag < <(head -1 "$LEVELS")
tail -n +2 "$LEVELS" > "$LEVELS.tmp" && mv "$LEVELS.tmp" "$LEVELS"

log=$HERE/knee_${tag}_j${slots}.log
t0=$SECONDS
ctest --test-dir "$BUILD" -R "$SEL" -j"$slots" --timeout 1800 > "$log" 2>&1
rc=$?
total=$((SECONDS - t0))

# per-test durations from ctest's own report
mapfile -t durs < <(grep -oE 'Passed +[0-9.]+ sec' "$log" | grep -oE '[0-9.]+')
max=0; sum=0
for d in "${durs[@]:-0}"; do
  sum=$(echo "$sum + $d" | bc)
  max=$(echo "if ($d > $max) $d else $max" | bc)
done

echo "$slots,$tag,$total,$max,$sum" >> "$OUT"
printf 'slots=%-3s tag=%-10s wall=%4ss  slowest_test=%7ss  sum=%8ss  rc=%s  ntests=%s\n' \
  "$slots" "$tag" "$total" "$max" "$sum" "$rc" "${#durs[@]}"
echo "levels left: $(wc -l < "$LEVELS")"
