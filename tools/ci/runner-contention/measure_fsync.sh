#!/bin/bash
# Count latency-bound IO syscalls per test.
#
# Bytes written turned out not to predict how badly a test suffers under device
# contention: a subset writing 45 MB/s was barely affected (1.1x) while one
# writing 25-30 MB/s was slowed 13.8x. The plausible discriminator is
# synchronous, latency-bound IO - fsync, fdatasync, and file creation - rather
# than streaming volume, since a queued device punishes each round trip.
#
# Counts syscalls with strace on the test binary directly. This undercounts a
# test that ctest would give a different working directory, but the ranking is
# what matters here.
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
BUILD=${BUILD:-$REPO/build}
OUT=$HERE/fsync_per_test.csv
LIST=$HERE/fsync_targets.txt

[ -f "$OUT" ] || echo "test,seconds,fsync,fdatasync,openat,renameat,total_sync" > "$OUT"
[ -s "$LIST" ] || { echo "no targets in $LIST"; exit 1; }

while read -r t; do
  [ -z "$t" ] && continue
  grep -q "^$t," "$OUT" && continue
  # ctest test names carry a memgraph__unit__ prefix that the binaries do not.
  bin="$BUILD/tests/unit/${t#memgraph__unit__}"
  [ -x "$bin" ] || { echo "$t: binary missing"; continue; }
  t0=$SECONDS
  strace -f -c -e trace=fsync,fdatasync,openat,renameat,renameat2 \
    -o "$HERE/.strace.out" "$bin" > /dev/null 2>&1
  dur=$((SECONDS - t0))
  g() { grep -E "[[:space:]]$1$" "$HERE/.strace.out" | awk '{print $(NF-1)}' | head -1; }
  fs=$(g fsync); fds=$(g fdatasync); op=$(g openat); rn=$(g renameat)
  fs=${fs:-0}; fds=${fds:-0}; op=${op:-0}; rn=${rn:-0}
  tot=$((fs + fds + rn))
  echo "$t,$dur,$fs,$fds,$op,$rn,$tot" >> "$OUT"
  printf '%-48s %4ss  fsync=%-7s fdatasync=%-7s openat=%-8s sync_total=%s\n' \
    "$t" "$dur" "$fs" "$fds" "$op" "$tot"
done < "$LIST"
rm -f "$HERE/.strace.out"
