#!/usr/bin/env python3
"""Score a fixed list of PCH candidate headers per CMake target.

For each (target, header) pair, report total parse time, number of TUs that
include it, and TU %. Use this to decide which headers are worth PCH'ing in
each specific target, instead of applying a one-size-fits-all list.

A header is a good PCH candidate for a target when:
  - TU % is high (≥70%): most TUs in the target pay for it anyway.
  - Total parse time is non-trivial relative to the target's total.

Usage:
  python3 tools/score_pch_candidates.py [BUILD_DIR]

Candidate list and target list are hard-coded below; edit as needed.
"""

import glob
import json
import os
import re
import sys
from collections import defaultdict

CANDIDATES = [
    # stdlib
    "chrono",
    "filesystem",
    "format",
    "istream",
    "memory",
    "optional",
    "ostream",
    "ranges",
    "sstream",
    "string",
    "unordered_map",
    "vector",
    # fmt
    "fmt/chrono.h",
    "fmt/format.h",
    "fmt/ostream.h",
    # spdlog
    "spdlog/common.h",
    "spdlog/spdlog.h",
    # candidates to add
    "absl/container/flat_hash_map.h",
    "boost/container_hash/hash.hpp",
    "nlohmann/json.hpp",
    "boost/asio/io_context.hpp",
    "boost/asio/ssl/context.hpp",
    "libnuraft/nuraft.hxx",
]

TARGETS = [
    "mg-query",
    "mg-storage-v2",
    "mg-dbms",
    "mg-glue",
    "mg-replication_handler",
    "mg-coordination",
    "mg-utils",
    "mg-communication",
    "mg-replication",
    "mg-auth",
]

TARGET_RE = re.compile(r"/CMakeFiles/([^/]+)\.dir/")


def target_of(path):
    m = TARGET_RE.search(path)
    return m.group(1) if m else "<unknown>"


def matches_candidate(header, candidate):
    """True if this full header path matches the candidate suffix."""
    # stdlib headers: no slash, match against libstdc++ basename
    if "/" not in candidate:
        # e.g. "chrono" matches /opt/toolchain-v7/.../include/c++/15.1.0/chrono
        return header.endswith("/" + candidate) and "include/c++" in header
    # with slash: match trailing path component
    return header.endswith("/" + candidate)


def parse_trace(path):
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
            d = ev.get("args", {}).get("detail", "")
            begin[key] = (d, ev.get("ts", 0))
        elif ph == "e" and key in begin:
            d, ts0 = begin.pop(key)
            dur = ev.get("ts", 0) - ts0
            if d and dur > 0:
                out[d] += dur
    return out


def main():
    build_dir = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else "build")
    files = glob.glob(os.path.join(build_dir, "**/*.cpp.json"), recursive=True)

    # target -> candidate -> {us, tus}; per-target total TU count
    scores = defaultdict(lambda: defaultdict(lambda: {"us": 0.0, "tus": 0}))
    tu_count = defaultdict(int)

    for f in files:
        tgt = target_of(f)
        if tgt not in TARGETS:
            continue
        tu_count[tgt] += 1
        headers = parse_trace(f)
        # For each candidate, find matching headers in this TU and sum
        for cand in CANDIDATES:
            tu_total_us = 0.0
            hit = False
            for h, us in headers.items():
                if matches_candidate(h, cand):
                    tu_total_us += us
                    hit = True
            if hit:
                scores[tgt][cand]["us"] += tu_total_us
                scores[tgt][cand]["tus"] += 1

    # Print a grid: rows = candidates, columns = targets
    # Each cell shows: "totalms/tu%%"
    print(f"{'header':<42}", end="")
    for t in TARGETS:
        print(f" {t:>20}", end="")
    print()
    print("-" * (42 + 21 * len(TARGETS)))

    for cand in CANDIDATES:
        print(f"{cand:<42}", end="")
        for t in TARGETS:
            n_tus = tu_count.get(t, 0)
            d = scores[t].get(cand)
            if not d or n_tus == 0:
                print(f" {'—':>20}", end="")
                continue
            ms = d["us"] / 1000
            pct = 100 * d["tus"] / n_tus
            cell = f"{ms:>6.0f}ms {d['tus']:>2}/{n_tus:<2}={pct:>3.0f}%"
            print(f" {cell:>20}", end="")
        print()

    # Per-target TU totals line
    print()
    print(f"{'(TUs in target)':<42}", end="")
    for t in TARGETS:
        print(f" {tu_count.get(t, 0):>20}", end="")
    print()


if __name__ == "__main__":
    main()
