#!/bin/bash
# Per-test device IO and duration, one test per ctest invocation.
#
# Brackets each run with /proc/diskstats for the backing device, so the delta is
# that test's block IO with the machine otherwise idle. This is what decides
# which tests should claim a disk slot; classifying by test name gets it wrong
# (interpreter and query_expression_evaluator look CPU-bound and are not).
#
# Resumable: already-measured tests are skipped, and each invocation stops after
# a time budget, because long-running loops get killed once accumulated page
# cache pins MemFree near zero. Run repeatedly until it prints ALL MEASURED.
#
# Reads may be served from page cache, so sectors_read understates a cold run.
# Writes must reach the device, so the write column is the dependable one.
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
BUILD=${BUILD:-$REPO/build}
OUT=$HERE/io_per_test.csv
DEV=${DEV:-nvme1n1}
BUDGET=${BUDGET:-420}

[ -f "$OUT" ] || echo "test,seconds,read_mb,write_mb,status" > "$OUT"

stats() {  # sectors read, sectors written
  awk -v d="$DEV" '$3==d {print $6, $10}' /proc/diskstats
}

mapfile -t ALL < <(ctest --test-dir "$BUILD" -N 2>/dev/null \
  | grep -oE 'memgraph__unit__[a-z0-9_]+' | sort -u)
[ ${#ALL[@]} -eq 0 ] && { echo "no tests found; is the build done?"; exit 1; }

start=$SECONDS
done_any=0
for t in "${ALL[@]}"; do
  grep -q "^$t," "$OUT" && continue
  if [ $((SECONDS - start)) -ge "$BUDGET" ]; then
    echo "budget reached; $(( ${#ALL[@]} - $(tail -n +2 "$OUT" | wc -l) )) tests left"
    exit 0
  fi
  read -r r0 w0 < <(stats)
  t0=$SECONDS
  ctest --test-dir "$BUILD" -R "^${t}\$" -j1 --timeout 900 > "$HERE/.io_run.log" 2>&1
  rc=$?
  dur=$((SECONDS - t0))
  read -r r1 w1 < <(stats)
  rmb=$(( (r1 - r0) / 2048 ))   # 512-byte sectors -> MiB
  wmb=$(( (w1 - w0) / 2048 ))
  st=ok; [ $rc -ne 0 ] && st=rc$rc
  echo "$t,$dur,$rmb,$wmb,$st" >> "$OUT"
  printf '%-52s %5ss  r=%5sMB  w=%5sMB  %s\n' "$t" "$dur" "$rmb" "$wmb" "$st"
  done_any=1
done
[ $done_any -eq 0 ] && echo "ALL MEASURED"
tail -n +2 "$OUT" | wc -l | xargs echo "measured so far:"
