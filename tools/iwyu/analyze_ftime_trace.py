#!/usr/bin/env python3
"""Analyze -ftime-trace JSON files to find the most expensive headers."""

import glob
import json
import os
from collections import defaultdict


def parse_trace_file(path):
    """Parse a single -ftime-trace JSON file, returning {header: total_us}."""
    try:
        with open(path) as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}

    events = data if isinstance(data, list) else data.get("traceEvents", [])

    # Source events use begin/end format: ph="b" and ph="e" with matching id
    begin_events = {}  # id -> (name, ts)
    header_times = defaultdict(float)

    for ev in events:
        if ev.get("name") != "Source":
            continue
        ph = ev.get("ph")
        eid = ev.get("id")  # could be int or string
        if eid is None:
            # Some versions use "dur" directly
            if ph == "X" and "dur" in ev:
                detail = ev.get("args", {}).get("detail", "")
                if detail:
                    header_times[detail] += ev["dur"]
            continue

        if ph == "b":
            detail = ev.get("args", {}).get("detail", "")
            begin_events[eid] = (detail, ev.get("ts", 0))
        elif ph == "e":
            if eid in begin_events:
                detail, begin_ts = begin_events.pop(eid)
                dur = ev.get("ts", 0) - begin_ts
                if detail and dur > 0:
                    header_times[detail] += dur

    return header_times


def main():
    build_dir = "/home/ivan.linux/work/memgraph/build"
    json_files = glob.glob(os.path.join(build_dir, "**/*.json"), recursive=True)
    # Filter to only .cpp.json files (ftime-trace output)
    json_files = [f for f in json_files if f.endswith(".cpp.json")]

    print(f"Found {len(json_files)} trace files")

    global_times = defaultdict(lambda: {"total_us": 0, "count": 0})

    for i, jf in enumerate(json_files):
        if (i + 1) % 50 == 0:
            print(f"  Processing {i+1}/{len(json_files)}...")
        header_times = parse_trace_file(jf)
        for header, us in header_times.items():
            global_times[header]["total_us"] += us
            global_times[header]["count"] += 1

    # Sort by total time
    sorted_headers = sorted(global_times.items(), key=lambda x: x[1]["total_us"], reverse=True)

    print(f"\nTop 100 most expensive headers (by total parse time across all TUs):")
    print(f"{'Rank':>4}  {'Total (ms)':>10}  {'Avg (ms)':>8}  {'TUs':>4}  Header")
    print("-" * 120)
    for i, (header, info) in enumerate(sorted_headers[:100]):
        total_ms = info["total_us"] / 1000
        avg_ms = total_ms / info["count"] if info["count"] else 0
        print(f"{i+1:>4}  {total_ms:>10.1f}  {avg_ms:>8.1f}  {info['count']:>4}  {header}")


if __name__ == "__main__":
    main()
