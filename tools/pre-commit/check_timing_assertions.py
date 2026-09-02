#!/usr/bin/env python3
"""Reject a test assertion that an operation finished within a wall-clock bound.

An upper bound on elapsed time asserts how fast the machine is. It passes on an
idle developer box and fails on a loaded CI runner, while saying nothing about
the code under test, which is why this class of assertion keeps producing flaky
failures. Assert the property instead: wait for the condition with a generous
liveness bound, or have the threads involved rendezvous.

Lower bounds are fine and are not flagged. "This did not happen before it could
have" stays true however slow the machine is, so it is a real assertion.

The occurrences that predate this check are listed in the baseline beside it.
They may be removed from the baseline, never added to.
"""
from __future__ import annotations

import pathlib
import re
import sys

HERE = pathlib.Path(__file__).resolve().parent
BASELINE = HERE / "timing_assertions_baseline.txt"

UPPER_BOUND = re.compile(r"\b(?:EXPECT|ASSERT)_(?:LT|LE)\s*\(")
# Names for a measured duration. `timeout` is deliberately absent: it names a
# budget, and comparing a budget against a measurement is the safe direction.
DURATION = re.compile(r"(?:[Ee]lapsed|duration|latency|took)\w*|\w*_(?:ms|time|latency|duration)\b")


def first_argument(line: str, open_paren: int) -> str:
    """The text of the macro's first argument, up to the comma separating it."""
    depth = 0
    for i in range(open_paren, len(line)):
        c = line[i]
        if c in "([":
            depth += 1
        elif c in ")]":
            depth -= 1
            if depth == 0:
                return line[open_paren + 1 : i]
        elif c == "," and depth == 1:
            return line[open_paren + 1 : i]
    return line[open_paren + 1 :]


# A bound set so far above any real duration that only a wait which never ends
# reaches it is a liveness bound, not a claim about speed. It looks identical, so
# it has to say so: put this marker on the assertion or the line above it.
LIVENESS_MARKER = "liveness-bound"


def offenders(path: pathlib.Path) -> list[tuple[int, str]]:
    try:
        lines = path.read_text(errors="ignore").splitlines()
    except OSError:
        return []
    found = []
    for i, line in enumerate(lines, 1):
        m = UPPER_BOUND.search(line)
        if not m:
            continue
        context = line + (lines[i - 2] if i >= 2 else "")
        if LIVENESS_MARKER in context:
            continue
        # Only the duration being the smaller side is a claim about speed.
        # EXPECT_LE(elapsed, budget) says the machine was fast enough;
        # EXPECT_LE(budget, elapsed) is a lower bound, which is safe.
        if DURATION.search(first_argument(line, m.end() - 1)):
            found.append((i, " ".join(line.split())))
    return found


def key(path: pathlib.Path, text: str) -> str:
    # Keyed on file and assertion text, not line number, so unrelated edits that
    # shift lines do not trip the check.
    return f"{path.as_posix()}\t{text}"


def main(argv: list[str]) -> int:
    known = set()
    if BASELINE.exists():
        known = {ln.rstrip("\n") for ln in BASELINE.read_text().splitlines() if ln.strip() and not ln.startswith("#")}

    repo = pathlib.Path.cwd()
    new: list[str] = []
    for arg in argv:
        path = pathlib.Path(arg)
        if not path.as_posix().startswith("tests/"):
            continue
        rel = path.relative_to(repo) if path.is_absolute() else path
        for line_no, text in offenders(path):
            if key(rel, text) not in known:
                new.append(f"{rel}:{line_no}: {text}")

    if new:
        print("Wall-clock upper bounds are not allowed in tests:\n", file=sys.stderr)
        for item in new:
            print(f"  {item}", file=sys.stderr)
        print(
            "\nAssert the property rather than the elapsed time: wait for the condition"
            "\nwith a generous liveness bound, or make the threads rendezvous. A lower"
            f"\nbound is fine. If this really must stand, add it to {BASELINE.name}.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
