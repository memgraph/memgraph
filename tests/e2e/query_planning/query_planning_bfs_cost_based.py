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
    # Create indexes
    memgraph.execute("CREATE INDEX ON :Node;")
    memgraph.execute("CREATE INDEX ON :Node(id);")

    # Create chain of 100 nodes with unique ids: (n1)-[:EDGE]->(n2)-[:EDGE]->...->(n100)
    memgraph.execute("UNWIND range(1, 100) AS id CREATE (n:Node {id: id});")
    memgraph.execute("UNWIND range(1, 99) AS i MATCH (a:Node {id: i}), (b:Node {id: i + 1}) CREATE (a)-[:EDGE]->(b);")

    # Test 1: ID index - both source and destination have ScanAllById (cardinality = 1)
    # Should use STShortestPath (existing_node = true)
    results = list(
        memgraph.execute_and_fetch("EXPLAIN MATCH (n)-[r *BFS]-(m) WHERE id(n) = 1 AND id(m) = 100 RETURN r;")
    )
    actual_explain = [x[QUERY_PLAN] for x in results]
    assert any("STShortestPath" in line for line in actual_explain), "Expected STShortestPath to be used with ID index"
    assert any("ScanAllById" in line for line in actual_explain), "Expected ScanAllById for destination"

    # Test 2: Label property index - both source and destination have ScanAllByLabelProperties with cardinality = 1
    # Should use STShortestPath (existing_node = true)
    results = list(memgraph.execute_and_fetch("EXPLAIN MATCH (n:Node {id: 1})-[r *BFS]-(m:Node {id: 100}) RETURN r;"))
    actual_explain = [x[QUERY_PLAN] for x in results]
    assert any(
        "STShortestPath" in line for line in actual_explain
    ), "Expected STShortestPath to be used with label property index"
    assert any("ScanAllByLabelProperties" in line for line in actual_explain), "Expected ScanAllByLabelProperties"

    # Test 3: Range filters - both have ScanAllByLabelProperties with range filters
    # Should use STShortestPath (existing_node = true) - small cardinality from range filters
    results = list(
        memgraph.execute_and_fetch("EXPLAIN MATCH (n:Node)-[r *BFS]-(m:Node) WHERE n.id < 10 AND m.id < 20 RETURN r;")
    )
    actual_explain = [x[QUERY_PLAN] for x in results]
    assert any(
        "STShortestPath" in line for line in actual_explain
    ), "Expected STShortestPath to be used with range filters"
    assert any("ScanAllByLabelProperties" in line for line in actual_explain), "Expected ScanAllByLabelProperties"
    memgraph.execute("MATCH (n) DETACH DELETE n;")

    # Test 4: Large cardinality - recreate nodes with duplicate id values

    # Create 100 nodes with only 2 unique id values (0 and 1) and edges in one query
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
    # Should use BFSExpand (SingleSourceShortestPath, existing_node = false)
    results = list(memgraph.execute_and_fetch("EXPLAIN MATCH (n:Node {id: 0})-[r *BFS]-(m:Node {id: 1}) RETURN r;"))
    actual_explain = [x[QUERY_PLAN] for x in results]
    assert any(
        "BFSExpand" in line for line in actual_explain
    ), "Expected BFSExpand (SingleSourceShortestPath) due to large cardinality"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
