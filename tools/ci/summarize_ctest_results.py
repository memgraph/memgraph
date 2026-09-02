#!/usr/bin/env python3
"""Summarise repeated ctest runs into a per-test flake rate.

The flakiness workflow runs every job several times and each run uploads its
JUnit output. Reading those together says how often each test failed across the
repeats, which is the number that decides whether a test is flaky and whether a
fix worked. Reading it out of the job logs instead means reading every log.

Usage: summarize_ctest_results.py DIR [DIR ...]
"""
from __future__ import annotations

import collections
import pathlib
import sys
import xml.etree.ElementTree as ET


def collect(roots: list[str]):
    runs: collections.Counter[str] = collections.Counter()
    failures: collections.Counter[str] = collections.Counter()
    files = 0
    for root in roots:
        for path in sorted(pathlib.Path(root).rglob("*.xml")):
            try:
                tree = ET.parse(path)
            except ET.ParseError:
                print(f"skipped unparseable {path}", file=sys.stderr)
                continue
            files += 1
            for case in tree.iter("testcase"):
                name = case.get("name") or "<unnamed>"
                runs[name] += 1
                # ctest writes <failure> for a failing test and marks skipped
                # ones separately; anything with a failure child counts.
                if case.find("failure") is not None or case.find("error") is not None:
                    failures[name] += 1
    return runs, failures, files


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    runs, failures, files = collect(sys.argv[1:])
    if not files:
        print("No ctest result files found.")
        return 0

    total_runs = sum(runs.values())
    flaky = sorted(failures.items(), key=lambda kv: (-kv[1], kv[0]))

    print(f"Read {files} result file(s): {len(runs)} distinct tests, {total_runs} executions.\n")
    if not flaky:
        print("No failures across any repeat.")
        return 0

    print("| test | failed | of runs | rate |")
    print("| --- | ---: | ---: | ---: |")
    for name, failed in flaky:
        n = runs[name]
        print(f"| `{name}` | {failed} | {n} | {failed / n:.0%} |")

    always = [n for n, f in flaky if f == runs[n]]
    sometimes = [n for n, f in flaky if f != runs[n]]
    print()
    print(f"{len(sometimes)} test(s) failed on some runs but not all, which is the flaky set.")
    if always:
        print(f"{len(always)} test(s) failed on every run, which is a break rather than a flake.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
