#!/usr/bin/env python3
"""Print raw_results.md (markdown) from ../data/*.json."""
import json
import os
import statistics as st
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from ci_cmp import CTL, SIG  # noqa: E402

ROOT = os.path.dirname(HERE)

RUNS = [
    ("pre-#4577 `88c90e77c`", "data/ci_pre4577_88c90e77c.json"),
    ("#4577 `1233eabb6`", "data/ci_4577_1233eabb6.json"),
    ("E0 master run1", "data/tmp_perf4577-e0-master_run1_doctor-doom.json"),
    ("E0 master run2", "data/tmp_perf4577-e0-master_run2_firebird.json"),
    ("E1 revert `f9361d25c`", "data/tmp_perf4577-e1-revert.json"),
    ("E2 DoRead probe `e359aa182`", "data/tmp_perf4577-e2-doread-direct.json"),
]
PAIRS = [(0, 1), (0, 2), (1, 2), (2, 4), (3, 4), (2, 5), (3, 5), (4, 5)]

runs = [(n, json.load(open(os.path.join(ROOT, f)))) for n, f in RUNS]
keys = [k for k in runs[0][1]["qps"] if all(k in r["qps"] for _, r in runs)]


def tag(k):
    return "S" if any(k.startswith(p) for p in SIG) else "C" if any(k.startswith(p) for p in CTL) else ""


print("# Raw CI mgbench results (single iteration each, QPS)\n")
print("Benchmark-only Diff `workflow_dispatch` runs (`release_benchmark=true`). S = signal set, C = control set.\n")
print("| set | query | " + " | ".join(f"{n}<br>({r['runner']})" for n, r in runs) + " |")
print("|---|---|" + "---:|" * len(runs))
for k in sorted(keys, key=lambda k: (tag(k) == "", tag(k), k)):
    print(f"| {tag(k)} | {'/'.join(k.split('/')[:2])} | " + " | ".join(f"{r['qps'][k]:.1f}" for _, r in runs) + " |")
print("\n## Pairwise summary\n\n| comparison | machines | signal | control | S − C |\n|---|---|---:|---:|---:|")
for i, j in PAIRS:
    (na, a), (nb, b) = runs[i], runs[j]
    s = [b["qps"][k] / a["qps"][k] - 1 for k in keys if tag(k) == "S"]
    c = [b["qps"][k] / a["qps"][k] - 1 for k in keys if tag(k) == "C"]
    print(
        f"| {nb} vs {na} | {b['runner']} / {a['runner']} | {st.median(s) * 100:+.1f}% | {st.median(c) * 100:+.1f}% "
        f"| **{(st.median(s) - st.median(c)) * 100:+.1f}%** |"
    )
