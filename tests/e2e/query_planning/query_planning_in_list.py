# Copyright 2026 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import sys

import pytest
from common import memgraph

QUERY_PLAN = "QUERY PLAN"


def get_plan(memgraph, query):
    results = list(memgraph.execute_and_fetch(f"EXPLAIN {query}"))
    return [x[QUERY_PLAN] for x in results]


def setup_graph(memgraph):
    """Create two labels with similar cardinalities and connect them.

    Both labels have 50 nodes. Without IN-list awareness the planner sees
    the IN side as expensive (Unwind factor * label count * kFilter) and
    picks the wrong starting scan. With the fix the planner uses the actual
    per-element sum and correctly starts from the selective IN side.
    """
    memgraph.execute("CREATE INDEX ON :A(prop);")
    memgraph.execute("CREATE INDEX ON :B(prop);")
    memgraph.execute("UNWIND range(1, 50) AS i CREATE (:A {prop: toString(i)})")
    memgraph.execute("UNWIND range(1, 50) AS i CREATE (:B {prop: toString(i)})")
    memgraph.execute("MATCH (a:A), (b:B) CREATE (a)-[:REL]->(b)")


def bottom_scan_operator(plan):
    """Return the bottom-most (first-to-execute) scan in the plan."""
    scans = [line for line in plan if "Scan" in line]
    return scans[-1] if scans else None


def test_in_list_picks_same_start_as_equality(memgraph):
    """IN ['x'] should produce the same starting scan as = 'x'.

    Both labels have 50 nodes. Equality on A matches 1 node, so the planner
    should start from A. IN ['x'] should behave identically.
    """
    setup_graph(memgraph)

    eq_plan = get_plan(
        memgraph,
        "MATCH (a:A)--(b:B) WHERE a.prop = '1' RETURN b",
    )
    in_plan = get_plan(
        memgraph,
        "MATCH (a:A)--(b:B) WHERE a.prop IN ['1'] RETURN b",
    )

    eq_scan = bottom_scan_operator(eq_plan)
    in_scan = bottom_scan_operator(in_plan)

    assert ":A" in eq_scan, f"Equality plan should scan A, got: {eq_scan}"
    assert ":A" in in_scan, f"IN plan should scan A, got: {in_scan}"


def test_in_list_multi_element(memgraph):
    """IN ['x', 'y'] with two matching values: planner should still start from the IN side.

    A has 50 nodes; B has 50 nodes. IN ['1', '2'] on A matches 2 nodes,
    which is far cheaper than scanning all 50 B nodes.
    """
    setup_graph(memgraph)

    plan = get_plan(
        memgraph,
        "MATCH (a:A)--(b:B) WHERE a.prop IN ['1', '2'] RETURN b",
    )

    scan = bottom_scan_operator(plan)
    assert ":A" in scan, f"Multi-element IN should scan A, got: {scan}"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
