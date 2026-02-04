# Copyright 2025 Memgraph Ltd.
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


def test_bfs_cost_based_selection(memgraph):
    """Test cost-based BFS algorithm selection with different index types and cardinalities."""
    memgraph.execute("CREATE INDEX ON :Node;")
    memgraph.execute("CREATE INDEX ON :Node(id);")

    memgraph.execute("UNWIND range(1, 100) AS id CREATE (n:Node {id: id});")
    memgraph.execute("UNWIND range(1, 99) AS i MATCH (a:Node {id: i}), (b:Node {id: i + 1}) CREATE (a)-[:EDGE]->(b);")

    # Test 1: ID index - both source and destination have ScanAllById (cardinality = 1)
    # Expected: STShortestPath with ScanAllById for both
    expected_plan_1 = [
        " * Produce {r}",
        " * STShortestPath (n)-[r]-(m)",
        " * ScanAllById (m)",
        " * ScanAllById (n)",
        " * Once",
    ]
    results = list(
        memgraph.execute_and_fetch("EXPLAIN MATCH (n)-[r *BFS]-(m) WHERE id(n) = 1 AND id(m) = 100 RETURN r;")
    )
    actual_explain = [x[QUERY_PLAN] for x in results]
    assert expected_plan_1 == actual_explain, f"Expected plan 1, got: {actual_explain}"

    # Test 2: Label property index - both source and destination have ScanAllByLabelProperties with cardinality = 1
    # Expected: STShortestPath with ScanAllByLabelProperties for both
    expected_plan_2 = [
        " * Produce {r}",
        " * STShortestPath (n)-[r]-(m)",
        " * ScanAllByLabelProperties (m :Node {id})",
        " * ScanAllByLabelProperties (n :Node {id})",
        " * Once",
    ]
    results = list(memgraph.execute_and_fetch("EXPLAIN MATCH (n:Node {id: 1})-[r *BFS]-(m:Node {id: 100}) RETURN r;"))
    actual_explain = [x[QUERY_PLAN] for x in results]
    assert expected_plan_2 == actual_explain, f"Expected plan 2, got: {actual_explain}"

    # Test 3: Range filters - both have ScanAllByLabelProperties with range filters
    # Expected: STShortestPath with ScanAllByLabelProperties for both
    expected_plan_3 = [
        " * Produce {r}",
        " * STShortestPath (n)-[r]-(m)",
        " * ScanAllByLabelProperties (m :Node {id})",
        " * ScanAllByLabelProperties (n :Node {id})",
        " * Once",
    ]
    results = list(
        memgraph.execute_and_fetch("EXPLAIN MATCH (n:Node)-[r *BFS]-(m:Node) WHERE n.id < 10 AND m.id < 20 RETURN r;")
    )
    actual_explain = [x[QUERY_PLAN] for x in results]
    assert expected_plan_3 == actual_explain, f"Expected plan 3, got: {actual_explain}"

    memgraph.execute("MATCH (n:Node) DETACH DELETE n;")

    # Test 4: Large cardinality - recreate nodes with duplicate id values
    # Create 100 nodes with only 2 unique id values (0 and 1) and edges
    memgraph.execute(
        "WITH range(0, 99) AS idx "
        "UNWIND idx AS i "
        "CREATE (n:Node {id: i % 2}) "
        "WITH collect(n) AS nodes "
        "UNWIND range(0, size(nodes)-2) AS i "
        "WITH nodes[i] AS a, nodes[i+1] AS b "
        "CREATE (a)-[:NEXT]->(b);"
    )

    # Both have ScanAllByLabelProperties but with large cardinality (~50 each)
    # Expected: BFSExpand (SingleSourceShortestPath from source) with Filter for destination
    expected_plan_4 = [
        " * Produce {r}",
        " * Filter (m :Node), {m.id}",
        " * BFSExpand (n)-[r]-(m)",
        " * ScanAllByLabelProperties (n :Node {id})",
        " * Once",
    ]
    results = list(memgraph.execute_and_fetch("EXPLAIN MATCH (n:Node {id: 0})-[r *BFS]-(m:Node {id: 1}) RETURN r;"))
    actual_explain = [x[QUERY_PLAN] for x in results]
    assert expected_plan_4 == actual_explain, f"Expected plan 4, got: {actual_explain}"

    # Test 5: Only destination index exists - source has no index
    # Expected: BFSExpand (SingleSourceShortestPath from destination) with destination scan
    expected_plan_5 = [
        " * Produce {r}",
        " * Filter (n :Node1), {n.id}",
        " * BFSExpand (n)-[r]-(m)",
        " * ScanAllByLabelProperties (m :Node {id})",
        " * Once",
    ]
    results = list(memgraph.execute_and_fetch("EXPLAIN MATCH (n:Node1 {id: 1})-[r *BFS]-(m:Node {id: 0}) RETURN r;"))
    actual_explain = [x[QUERY_PLAN] for x in results]
    assert expected_plan_5 == actual_explain, f"Expected plan 5, got: {actual_explain}"

    # Test 6: Only source index exists - destination has no index
    # Expected: BFSExpand (SingleSourceShortestPath from source) with Filter for destination
    expected_plan_6 = [
        " * Produce {r}",
        " * Filter (m :Node1), {m.id}",
        " * BFSExpand (n)-[r]-(m)",
        " * ScanAllByLabelProperties (n :Node {id})",
        " * Once",
    ]
    results = list(memgraph.execute_and_fetch("EXPLAIN MATCH (n:Node {id: 0})-[r *BFS]-(m:Node1 {id: 1}) RETURN r;"))
    actual_explain = [x[QUERY_PLAN] for x in results]
    assert expected_plan_6 == actual_explain, f"Expected plan 6, got: {actual_explain}"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
