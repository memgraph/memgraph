#!/usr/bin/env python3
"""Rank unit tests by device write demand, to decide which claim a disk slot.

Writes are the dependable signal: they must reach the device, whereas reads are
often served from page cache. Both the total volume and the sustained rate
matter, so both are reported: a test that writes 500 MB over 60s pressures the
device far less than one writing 100 MB in 2s.
"""
import csv
import os

HERE = os.path.dirname(os.path.abspath(__file__))
rows = list(csv.DictReader(open(os.path.join(HERE, "io_per_test.csv"))))

recs = []
for r in rows:
    secs = float(r["seconds"])
    w = float(r["write_mb"])
    rate = w / secs if secs > 0 else w  # sub-second tests: treat as one second
    recs.append((r["test"], secs, float(r["read_mb"]), w, rate, r["status"]))

total_w = sum(r[3] for r in recs)
total_t = sum(r[1] for r in recs)
print(f"{len(recs)} tests, {total_w:.0f} MB written, {total_t:.0f}s sequential")
bad = [r for r in recs if r[5] != "ok"]
if bad:
    print(f"non-ok: {', '.join(r[0] + '=' + r[5] for r in bad)}")

print()
print("=== top 25 by write volume ===")
print(f"{'test':<52}{'secs':>6}{'writeMB':>9}{'MB/s':>8}")
for t, s, rd, w, rate, st in sorted(recs, key=lambda x: -x[3])[:25]:
    print(f"{t:<52}{s:>6.0f}{w:>9.0f}{rate:>8.1f}")

print()
print("=== top 15 by sustained write rate (device pressure) ===")
print(f"{'test':<52}{'secs':>6}{'writeMB':>9}{'MB/s':>8}")
for t, s, rd, w, rate, st in sorted(recs, key=lambda x: -x[4])[:15]:
    print(f"{t:<52}{s:>6.0f}{w:>9.0f}{rate:>8.1f}")

print()
print("=== how many tests would claim a slot, per threshold ===")
print(f"{'threshold':>12}{'tests':>8}{'% of writes covered':>22}")
for thr in (0, 1, 5, 10, 25, 50, 100, 200):
    sel = [r for r in recs if r[3] > thr]
    cov = sum(r[3] for r in sel) / total_w * 100 if total_w else 0
    print(f"{'>' + str(thr) + ' MB':>12}{len(sel):>8}{cov:>21.1f}%")
