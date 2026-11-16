#!/usr/bin/env python3
"""Fail on a literal storage::View::OLD/NEW in the plan operators or rewriters that carries no marker.

A literal view bypasses the planner-set view_ of an operator, which is how write-visibility bugs entered. Each literal
needs `// literal-view: <reason>` on its line or on a comment line directly above the statement that holds
it. Reasons:
  write       a write operator reads View::NEW by design
  constant    the expression reads no graph state (Skip, Limit, LOAD path, commit frequency, a type)
  point-scan  the point index scans read View::OLD only
  known: item N   a known defect, tracked as item N of the write-visibility follow-ups
"""
import re
import sys
from pathlib import Path

DEFAULT_FILES = ["src/query/plan/operator.cpp", *sorted(str(p) for p in Path("src/query/plan/rewrite").glob("*.hpp"))]
LITERAL = re.compile(r"\bView::(OLD|NEW)\b")
MARKER = re.compile(r"//\s*literal-view:\s*(write|constant|point-scan|known: item \d+)\s*$")


STATEMENT_END = re.compile(r"[;{}]\s*$|^\s*#")


def marked(lines, i):
    """Whether line i, or a comment line above the statement that holds it, carries the marker."""
    if MARKER.search(lines[i]):
        return True
    for j in range(i - 1, max(i - 12, -1), -1):
        text = lines[j].strip()
        if text.startswith("//"):
            if MARKER.search(text):
                return True
            continue
        if not text or STATEMENT_END.search(text):
            return False
    return False


def unmarked(path):
    lines = Path(path).read_text().splitlines()
    return [
        (i + 1, line.strip())
        for i, line in enumerate(lines)
        if LITERAL.search(line.split("//", 1)[0]) and not marked(lines, i)
    ]


def main(argv):
    files = argv[1:] or DEFAULT_FILES
    failed = False
    for path in files:
        for lineno, text in unmarked(path):
            print(f"{path}:{lineno}: literal view without a `// literal-view: <reason>` marker: {text}")
            failed = True
    if failed:
        print(
            "Use the operator's planner-set view_, or mark the literal (reasons: tools/pre-commit/check_literal_views.py)."
        )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
