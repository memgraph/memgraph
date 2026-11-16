#!/usr/bin/env python3
"""Run: python3 tools/pre-commit/test_check_literal_views.py (from the repository root)."""
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import check_literal_views  # noqa: E402


def check(text):
    with tempfile.NamedTemporaryFile("w", suffix=".cpp", delete=False) as f:
        f.write(text)
    try:
        return check_literal_views.main(["gate", f.name])
    finally:
        os.unlink(f.name)


class LiteralViewGate(unittest.TestCase):
    def test_unmarked_literal_fails(self):
        self.assertEqual(check("auto e = ExpressionEvaluator{&frame, ctx, storage::View::OLD};\n"), 1)

    def test_trailing_marker_passes(self):
        self.assertEqual(check("x.Labels(storage::View::NEW);  // literal-view: write\n"), 0)

    def test_marker_above_passes(self):
        self.assertEqual(check("  // literal-view: known: item 7\n  x.Labels(storage::View::NEW);\n"), 0)

    def test_unknown_reason_fails(self):
        self.assertEqual(check("x.Labels(storage::View::NEW);  // literal-view: because\n"), 1)

    def test_marker_above_the_statement_passes(self):
        self.assertEqual(
            check("  // literal-view: write\n  if (checker->Has(\n          v, storage::View::NEW)) {\n"), 0
        )

    def test_marker_above_an_earlier_statement_fails(self):
        self.assertEqual(check("// literal-view: write\nint a;\nx.Labels(storage::View::NEW);\n"), 1)

    def test_literal_in_a_comment_is_ignored(self):
        self.assertEqual(check("// reads storage::View::OLD here\n"), 0)

    def test_repository_files_pass(self):
        self.assertEqual(check_literal_views.main(["gate"]), 0)


if __name__ == "__main__":
    unittest.main()
