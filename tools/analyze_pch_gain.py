#!/usr/bin/env python3
"""Estimate PCH gain per CMake target from Clang -ftime-trace output.

For every target (inferred from the trace file path
`build/.../CMakeFiles/<target>.dir/...`), aggregate Source events to rank
headers by total parse time and number of TUs within that target that parsed
them.

The "PCH gain estimate" for a given header in a target is approximately its
total parse time in that target: that is how much frontend time PCH'ing the
header in that target would remove (before subtracting the PCH load-tax
overhead, which scales with total PCH size).

Caveat: headers already in PCH for a target won't appear in that target's
traces (they're pre-loaded, not parsed). To evaluate headers already PCH'd,
rebuild from a state where they aren't.

Usage:
  python3 tools/analyze_pch_gain.py [BUILD_DIR] [--target GLOB] [--top N]
                                    [--min-tus M] [--external-only]

Options:
  --target GLOB     Only print targets whose name matches this shell glob.
                    Default: all targets.
  --top N           Top N headers per target. Default: 20.
  --min-tus M       Skip headers used by fewer than M TUs (reduces noise).
                    Default: 3.
  --external-only   Only show headers outside src/ (stdlib, conan, libs).
                    These are the safe PCH candidates.
"""

import argparse
import fnmatch
import glob
import json
import os
import re
import sys
from collections import defaultdict

TARGET_RE = re.compile(r"/CMakeFiles/([^/]+)\.dir/")


def target_of(path: str) -> str:
    m = TARGET_RE.search(path)
    return m.group(1) if m else "<unknown>"


def is_external(header: str) -> bool:
    """True if header is NOT a project source header — i.e. safe for PCH."""
    # project headers live under <repo>/src/ or <repo>/libs/memgraph/
    # everything else (toolchain, conan, libs/*) is external and stable
    return "/memgraph/src/" not in header


def parse_trace(path: str) -> dict:
    try:
        with open(path) as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}
    events = data if isinstance(data, list) else data.get("traceEvents", [])
    begin = {}
    out = defaultdict(float)
    for ev in events:
        if ev.get("name") != "Source":
            continue
        ph = ev.get("ph")
        eid = ev.get("id")
        if eid is None:
            if ph == "X" and "dur" in ev:
                d = ev.get("args", {}).get("detail", "")
                if d:
                    out[d] += ev["dur"]
            continue
        key = (ev.get("tid"), eid)
        if ph == "b":
            out_detail = ev.get("args", {}).get("detail", "")
            begin[key] = (out_detail, ev.get("ts", 0))
        elif ph == "e" and key in begin:
            d, ts0 = begin.pop(key)
            dur = ev.get("ts", 0) - ts0
            if d and dur > 0:
                out[d] += dur
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("build_dir", nargs="?", default="build")
    ap.add_argument("--target", default="*")
    ap.add_argument("--top", type=int, default=20)
    ap.add_argument("--min-tus", type=int, default=3)
    ap.add_argument("--external-only", action="store_true")
    args = ap.parse_args()

    build_dir = os.path.abspath(args.build_dir)
    files = [f for f in glob.glob(os.path.join(build_dir, "**/*.cpp.json"), recursive=True)]
    print(f"Found {len(files)} trace files in {build_dir}\n", file=sys.stderr)

    # target -> header -> {total_us, tus}
    per_target = defaultdict(lambda: defaultdict(lambda: {"us": 0.0, "tus": 0}))
    tu_count = defaultdict(int)

    for i, f in enumerate(files):
        tgt = target_of(f)
        tu_count[tgt] += 1
        headers = parse_trace(f)
        for h, us in headers.items():
            d = per_target[tgt][h]
            d["us"] += us
            d["tus"] += 1

    targets = sorted(
        per_target.keys(),
        key=lambda t: -sum(d["us"] for d in per_target[t].values()),
    )

    for tgt in targets:
        if not fnmatch.fnmatch(tgt, args.target):
            continue
        headers = per_target[tgt]
        rows = []
        for h, d in headers.items():
            if d["tus"] < args.min_tus:
                continue
            if args.external_only and not is_external(h):
                continue
            rows.append((h, d["us"], d["tus"]))
        rows.sort(key=lambda r: -r[1])
        if not rows:
            continue
        n_tus = tu_count[tgt]
        tgt_total = sum(d["us"] for d in headers.values()) / 1e6
        print(f"=== {tgt}  ({n_tus} TUs, {tgt_total:.1f}s total parse time) ===")
        print(f"{'Rank':>4}  {'Total(ms)':>10}  {'Avg(ms)':>8}  " f"{'TUs':>4}/{n_tus:<3}  Header")
        print("-" * 110)
        for i, (h, us, tus) in enumerate(rows[: args.top]):
            total_ms = us / 1000
            avg_ms = total_ms / tus if tus else 0
            pct_tus = 100 * tus / n_tus
            tag = f"{tus:>4}/{n_tus:<3} ({pct_tus:>3.0f}%)"
            # shorten common prefixes
            short = h
            short = short.replace(os.path.expanduser("~") + "/.conan2/p/b/", "[conan]/")
            short = short.replace(os.path.expanduser("~") + "/.conan2/p/", "[conan]/")
            short = re.sub(r"/opt/toolchain-v7/[^ ]*/include/c\+\+/[0-9.]+/", "[libstdc++]/", short)
            short = short.replace(os.path.expanduser("~") + "/work/memgraph/", "")
            print(f"{i+1:>4}  {total_ms:>10.1f}  {avg_ms:>8.1f}  {tag:>15}  {short}")
        print()


if __name__ == "__main__":
    main()
