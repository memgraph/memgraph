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


def build_of(root: pathlib.Path, path: pathlib.Path) -> str:
    """Name the build a result file came from.

    Each repeat arrives as its own artifact directory, named for the build and
    the repeat, so the first path component below the download root identifies
    the build once the repeat suffix is taken off. Counting a test's runs across
    builds instead would average a break in one build against a pass in
    another, and report neither.
    """
    try:
        head = path.relative_to(root).parts[0]
    except (ValueError, IndexError):
        return "unknown"
    head = head.removeprefix("ctest_results_")
    return head.rsplit("-", 1)[0] if "-" in head else head


def collect(roots: list[str]):
    runs: collections.Counter[tuple[str, str]] = collections.Counter()
    failures: collections.Counter[tuple[str, str]] = collections.Counter()
    files = 0
    for raw_root in roots:
        root = pathlib.Path(raw_root)
        for path in sorted(root.rglob("*.xml")):
            try:
                tree = ET.parse(path)
            except (ET.ParseError, OSError, ValueError):
                print(f"skipped unreadable {path}", file=sys.stderr)
                continue
            files += 1
            build = build_of(root, path)
            for case in tree.iter("testcase"):
                # A test ctest never ran tells us nothing about whether it
                # fails, and counting it would understate the rate of one that
                # fails whenever it does run. ctest reports an execution as
                # either run or fail and gives anything it declined to start a
                # status of its own.
                if case.find("skipped") is not None or case.get("status") not in ("run", "fail"):
                    continue
                key = (build, case.get("name") or "<unnamed>")
                runs[key] += 1
                # ctest writes <failure> for a failing test and marks skipped
                # ones separately; anything with a failure child counts.
                if case.find("failure") is not None or case.find("error") is not None:
                    failures[key] += 1
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

    print(f"Read {files} result file(s): {len(runs)} build/test pairs, {total_runs} executions.\n")
    if not flaky:
        print("No failures across any repeat.")
        return 0

    print("| build | test | failed | of runs | rate |")
    print("| --- | --- | ---: | ---: | ---: |")
    for key, failed in flaky:
        build, name = key
        n = runs[key]
        print(f"| {build} | `{name}` | {failed} | {n} | {failed / n:.0%} |")

    always = [k for k, f in flaky if f == runs[k]]
    sometimes = [k for k, f in flaky if f != runs[k]]
    print()
    print(f"{len(sometimes)} test(s) failed on some runs but not all, which is the flaky set.")
    if always:
        print(f"{len(always)} test(s) failed on every run, which is a break rather than a flake.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
