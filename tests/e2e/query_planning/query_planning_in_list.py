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
    """Create two labels with very different cardinalities and connect them."""
    memgraph.execute("CREATE INDEX ON :Big(prop);")
    memgraph.execute("CREATE INDEX ON :Small(prop);")
    # 500 Big nodes, 5 Small nodes, each Small connected to every Big.
    memgraph.execute("UNWIND range(1, 500) AS i CREATE (:Big {prop: toString(i)})")
    memgraph.execute("UNWIND range(1, 5) AS i CREATE (:Small {prop: 'val' + toString(i)})")
    memgraph.execute("MATCH (b:Big), (s:Small) CREATE (b)-[:REL]->(s)")


def bottom_scan_operator(plan):
    """Return the bottom-most (first-to-execute) scan in the plan."""
    scans = [line for line in plan if "Scan" in line]
    return scans[-1] if scans else None


def test_in_list_picks_same_start_as_equality(memgraph):
    """IN ['x'] should produce the same starting scan as = 'x'."""
    setup_graph(memgraph)

    eq_plan = get_plan(
        memgraph,
        "MATCH (a:Big)--(b:Small) WHERE b.prop = 'val1' RETURN a",
    )
    in_plan = get_plan(
        memgraph,
        "MATCH (a:Big)--(b:Small) WHERE b.prop IN ['val1'] RETURN a",
    )

    eq_scan = bottom_scan_operator(eq_plan)
    in_scan = bottom_scan_operator(in_plan)

    # Both should start from Small (the selective side).
    assert "Small" in eq_scan, f"Equality plan should scan Small, got: {eq_scan}"
    assert "Small" in in_scan, f"IN plan should scan Small, got: {in_scan}"


def test_in_list_multi_element(memgraph):
    """IN ['x', 'y'] with two matching values should still pick the selective label."""
    setup_graph(memgraph)

    plan = get_plan(
        memgraph,
        "MATCH (a:Big)--(b:Small) WHERE b.prop IN ['val1', 'val2'] RETURN a",
    )

    scan = bottom_scan_operator(plan)
    assert "Small" in scan, f"Multi-element IN should scan Small, got: {scan}"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
