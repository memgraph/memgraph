#!/bin/bash
# Run exactly ONE ctest pass per invocation, popped from steps.txt.
#
# One pass per invocation is deliberate. Long multi-pass scripts get killed with
# a low-memory message even when the host has tens of gigabytes free, because
# page cache from the tests' file writes is charged to the calling job's cgroup;
# a single pass stays under that budget. Results accumulate in results.csv
# across invocations, so an interrupted run loses one pass, not the matrix.
#
# Usage: run repeatedly until it prints NO STEPS LEFT.
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
BUILD=${BUILD:-$REPO/build}
OUT=$HERE/results.csv
STEPS=$HERE/steps.txt
SEL='memgraph__unit__(utils_scheduler|query_expression_evaluator|interpreter|storage_v2_constraints|storage_v2_indices)$'

# Measured, not guessed: interpreter and query_expression_evaluator look
# CPU-bound by name but are slowed 10-14x by device contention, so they are
# classified with the filesystem tests.
declare -A CLASS=(
  [memgraph__unit__utils_scheduler]=timer
  [memgraph__unit__query_expression_evaluator]=fs
  [memgraph__unit__interpreter]=fs
  [memgraph__unit__storage_v2_constraints]=fs
  [memgraph__unit__storage_v2_indices]=fs
)

[ -s "$STEPS" ] || { echo "NO STEPS LEFT"; exit 0; }
[ -f "$OUT" ] || echo "arm,class,test,mode,seconds" > "$OUT"

read -r label cpus mode jn < <(head -1 "$STEPS")
tail -n +2 "$STEPS" > "$STEPS.tmp" && mv "$STEPS.tmp" "$STEPS"

echo "== $label cpus=$cpus mode=$mode -j$jn =="
log=$HERE/s_${label}_${mode}.log
taskset -c "$cpus" ctest --test-dir "$BUILD" -R "$SEL" -j"$jn" --timeout 3000 > "$log" 2>&1
echo "  ctest rc=$?"

# A "warm" pass is discarded: a first pass over a cold page cache runs several
# times slower and would inflate the isolated baseline.
if [ "$mode" != "warm" ]; then
  while read -r name secs; do
    [ -z "${CLASS[$name]:-}" ] && continue
    echo "$label,${CLASS[$name]},$name,$mode,$secs" >> "$OUT"
    printf '  %-48s %-10s %8.2fs\n' "$name" "$mode" "$secs"
  done < <(grep -oE 'memgraph__unit__[a-z0-9_]+ +\.+ +Passed +[0-9.]+ sec' "$log" \
           | sed -E 's/ +\.+ +Passed +/ /; s/ sec$//')
fi
echo "  free=$(free -m | awk '/^Mem:/{print $4}')MB  steps_left=$(wc -l < "$STEPS")"
