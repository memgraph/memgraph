#!/bin/bash
# Establish ctest scheduling semantics empirically rather than from prose docs.
# Self-contained: configures a throwaway project of sleep-based tests in ./b and
# reconstructs real concurrency from per-test start/end stamps.
set -u
S=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
chmod +x "$S/slot.sh"
cmake -S "$S" -B "$S/b" -DCMAKE_BUILD_TYPE=Release > /dev/null 2>&1 || { echo "configure failed"; exit 1; }

peak_overlap() {  # reconstruct max concurrency from START/END stamps
  python3 - "$1" "$2" <<'PY'
import sys, re
log, prefix = sys.argv[1], sys.argv[2]
ev = []
for line in open(log):
    p = line.split()
    if len(p) >= 3 and p[0] in ("START", "END") and p[1].startswith(prefix):
        ev.append((float(p[2]), 1 if p[0] == "START" else -1))
ev.sort()
cur = peak = 0
for _, d in ev:
    cur += d
    peak = max(peak, cur)
print(peak)
PY
}

echo "=== A: RESOURCE_GROUPS disk:1, spec declares 2 slots, ctest -j8 ==="
export SLOTLOG=$S/a.log; : > "$SLOTLOG"
ctest --test-dir "$S/b" -R '^disk_[0-9]+$' -j8 --resource-spec-file "$S/spec2.json" > "$S/a.out" 2>&1
echo "  ctest rc=$?  peak concurrency = $(peak_overlap "$SLOTLOG" disk_)  (spec says 2)"
echo "  resource vars seen: $(grep -c '^VAR' "$SLOTLOG")"
grep '^VAR' "$SLOTLOG" | head -2 | sed 's/^/    /'

echo "=== B: PROCESSORS 4, ctest -j8 (no spec file) ==="
export SLOTLOG=$S/b.log; : > "$SLOTLOG"
ctest --test-dir "$S/b" -R '^proc4_' -j8 > "$S/b.out" 2>&1
echo "  ctest rc=$?  peak concurrency = $(peak_overlap "$SLOTLOG" proc4_)  (expect 2)"

echo "=== C: PROCESSORS 64 with ctest -j8: does it run at all? ==="
export SLOTLOG=$S/c.log; : > "$SLOTLOG"
ctest --test-dir "$S/b" -R '^proc_huge$' -j8 > "$S/c.out" 2>&1
echo "  ctest rc=$?"
grep -E 'proc_huge|tests passed|Not Run' "$S/c.out" | head -3 | sed 's/^/    /'

echo "=== D: RESOURCE_GROUPS disk:99 against a 2-slot spec ==="
export SLOTLOG=$S/d.log; : > "$SLOTLOG"
ctest --test-dir "$S/b" -R '^disk_greedy$' -j8 --resource-spec-file "$S/spec2.json" > "$S/d.out" 2>&1
echo "  ctest rc=$?"
grep -E 'disk_greedy|Not Run|tests passed|tests failed' "$S/d.out" | head -4 | sed 's/^/    /'

echo "=== E: RESOURCE_GROUPS but NO --resource-spec-file ==="
export SLOTLOG=$S/e.log; : > "$SLOTLOG"
ctest --test-dir "$S/b" -R '^disk_[0-9]+$' -j8 > "$S/e.out" 2>&1
echo "  ctest rc=$?  peak concurrency = $(peak_overlap "$SLOTLOG" disk_)  (unlimited means the property is inert)"
