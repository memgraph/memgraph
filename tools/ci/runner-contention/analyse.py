#!/usr/bin/env python3
"""Per-class contended/isolated degradation, per arm.

The CI signature to match: tests that touch the device degrade several times
more than the rest, while sleep-bound tests barely move. The sleep-bound test is
the control - no constraint should move it, so if one does, the harness is
distorting the measurement rather than the machine.
"""
import collections
import csv
import os
import statistics

HERE = os.path.dirname(os.path.abspath(__file__))
rows = list(csv.DictReader(open(os.path.join(HERE, "results.csv"))))
d = collections.defaultdict(dict)
for r in rows:
    d[(r["arm"], r["test"], r["class"])][r["mode"]] = float(r["seconds"])

per_arm_class = collections.defaultdict(list)
print(f"{'arm':<16}{'class':<7}{'test':<46}{'iso':>8}{'cont':>9}{'ratio':>8}")
for (arm, test, cls), v in d.items():
    if "isolated" not in v or "contended" not in v:
        continue
    ratio = v["contended"] / v["isolated"]
    per_arm_class[(arm, cls)].append(ratio)
    print(f"{arm:<16}{cls:<7}{test:<46}{v['isolated']:>8.1f}{v['contended']:>9.1f}{ratio:>8.2f}")

print()
print("=== median degradation under load, by arm and class ===")
print(f"{'arm':<16}{'fs':>8}{'cpu':>8}{'timer':>8}{'fs/cpu':>9}")
arms = []
for arm, cls in per_arm_class:
    if arm not in arms:
        arms.append(arm)
for arm in arms:
    vals = {}
    for cls in ("fs", "cpu", "timer"):
        r = per_arm_class.get((arm, cls))
        vals[cls] = statistics.median(r) if r else float("nan")
    sep = vals["fs"] / vals["cpu"] if vals["cpu"] else float("nan")
    print(f"{arm:<16}{vals['fs']:>8.2f}{vals['cpu']:>8.2f}{vals['timer']:>8.2f}{sep:>9.2f}")

print()
print("CI reference shape: fs penalised well above cpu, timer ~1.0")
print("  coverage run: fs 5.62, cpu 3.74, timer 1.46  -> fs/cpu 1.50")
print("  release  run: fs 11.67, cpu 1.77, timer 1.12 -> fs/cpu 6.59")

# The within-arm contended/isolated ratio understates the IO arm, because both
# of its passes ran under the same competing load. What identifies the scarce
# resource is each arm's absolute time against the baseline arm's.
print()
print("=== absolute time vs the 'wide' baseline, same -j and CPU count ===")
base = {t: v["contended"] for (a, t, c), v in d.items() if a == "wide" and "contended" in v}
print(f"{'arm':<16}{'test':<46}{'wide':>9}{'this':>9}{'x':>7}")
for (arm, test, cls), v in d.items():
    if arm == "wide" or "contended" not in v or test not in base:
        continue
    b = base[test]
    print(f"{arm:<16}{test:<46}{b:>9.1f}{v['contended']:>9.1f}" f"{v['contended'] / b:>7.2f}")
