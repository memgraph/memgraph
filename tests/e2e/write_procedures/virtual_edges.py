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
from common import execute_and_fetch_all


def setup_chain_graph(cursor):
    """Create A-[:X]->B-[:Y]->C (a 2-hop chain)."""
    execute_and_fetch_all(cursor, "CREATE (a:Node {id: 1})-[:X]->(:Node {id: 2})-[:Y]->(:Node {id: 3});")


def setup_star_graph(cursor):
    """Create a star: center connected to 3 leaves via different types."""
    execute_and_fetch_all(cursor, "CREATE (c:Node {id: 1});")
    execute_and_fetch_all(cursor, "CREATE (n:Node {id: 2});")
    execute_and_fetch_all(cursor, "CREATE (n:Node {id: 3});")
    execute_and_fetch_all(cursor, "CREATE (n:Node {id: 4});")
    execute_and_fetch_all(cursor, "MATCH (c:Node {id:1}), (n:Node {id:2}) CREATE (c)-[:A]->(n);")
    execute_and_fetch_all(cursor, "MATCH (c:Node {id:1}), (n:Node {id:3}) CREATE (c)-[:B]->(n);")
    execute_and_fetch_all(cursor, "MATCH (c:Node {id:1}), (n:Node {id:4}) CREATE (c)-[:C]->(n);")


class TestProjectPathOptions:
    def test_basic_virtual_edge_projection(self, connection):
        """project(path, {virtualEdgeType: 'CONNECTED'}) should produce a graph
        with virtual edges between path endpoints."""
        cursor = connection.cursor()
        setup_chain_graph(cursor)

        # Path A->B->C should produce a virtual edge A->C of type CONNECTED
        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:Node {id: 1})-[*]->(:Node {id: 3})
            WITH project(p, {virtualEdgeType: 'CONNECTED'}) AS g
            RETURN g.nodes AS nodes, g.edges AS edges
            """,
        )

        assert len(results) == 1
        nodes, edges = results[0]
        # All 3 vertices from the path should be in the graph
        assert len(nodes) == 3
        # 1 virtual edge (A->C), no real edges (they aren't added by PROJECT_PATH_OPTIONS)
        assert len(edges) == 1
        # The virtual edge should be a map with from/to/type keys
        ve = edges[0]
        assert isinstance(ve, dict)
        assert ve["type"] == "CONNECTED"

    def test_virtual_edge_dedup(self, connection):
        """Multiple paths between the same endpoints should produce only one virtual edge."""
        cursor = connection.cursor()
        # Create two paths from 1 to 3: 1->2->3 and 1->4->3
        execute_and_fetch_all(cursor, "CREATE (:Node {id: 1});")
        execute_and_fetch_all(cursor, "CREATE (:Node {id: 2});")
        execute_and_fetch_all(cursor, "CREATE (:Node {id: 3});")
        execute_and_fetch_all(cursor, "CREATE (:Node {id: 4});")
        execute_and_fetch_all(cursor, "MATCH (a:Node {id:1}), (b:Node {id:2}) CREATE (a)-[:R]->(b);")
        execute_and_fetch_all(cursor, "MATCH (b:Node {id:2}), (c:Node {id:3}) CREATE (b)-[:R]->(c);")
        execute_and_fetch_all(cursor, "MATCH (a:Node {id:1}), (d:Node {id:4}) CREATE (a)-[:R]->(d);")
        execute_and_fetch_all(cursor, "MATCH (d:Node {id:4}), (c:Node {id:3}) CREATE (d)-[:R]->(c);")

        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:Node {id: 1})-[*]->(:Node {id: 3})
            WITH project(p, {virtualEdgeType: 'LINKED'}) AS g
            RETURN g.edges AS edges
            """,
        )

        assert len(results) == 1
        edges = results[0][0]
        # Both paths have same endpoints (1->3), so only 1 virtual edge after dedup
        assert len(edges) == 1
        assert edges[0]["type"] == "LINKED"

    def test_vertices_from_all_paths_collected(self, connection):
        """All intermediate vertices from all paths should be in the projected graph."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "CREATE (:Node {id: 1});")
        execute_and_fetch_all(cursor, "CREATE (:Node {id: 2});")
        execute_and_fetch_all(cursor, "CREATE (:Node {id: 3});")
        execute_and_fetch_all(cursor, "CREATE (:Node {id: 4});")
        execute_and_fetch_all(cursor, "MATCH (a:Node {id:1}), (b:Node {id:2}) CREATE (a)-[:R]->(b);")
        execute_and_fetch_all(cursor, "MATCH (b:Node {id:2}), (c:Node {id:3}) CREATE (b)-[:R]->(c);")
        execute_and_fetch_all(cursor, "MATCH (a:Node {id:1}), (d:Node {id:4}) CREATE (a)-[:R]->(d);")
        execute_and_fetch_all(cursor, "MATCH (d:Node {id:4}), (c:Node {id:3}) CREATE (d)-[:R]->(c);")

        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:Node {id: 1})-[*]->(:Node {id: 3})
            WITH project(p, {virtualEdgeType: 'X'}) AS g
            RETURN g.nodes AS nodes
            """,
        )

        assert len(results) == 1
        nodes = results[0][0]
        # All 4 nodes should be collected (1, 2, 3, 4 from both paths)
        assert len(nodes) == 4

    def test_short_path_no_virtual_edge(self, connection):
        """A single-hop path has the same start and end — no virtual edge is needed
        if the path has fewer than 2 vertices (but a 1-hop path has 2 vertices,
        so a virtual edge IS created)."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "CREATE (:Node {id: 1})-[:R]->(:Node {id: 2});")

        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:Node {id: 1})-[:R]->(:Node {id: 2})
            WITH project(p, {virtualEdgeType: 'V'}) AS g
            RETURN g.nodes AS nodes, g.edges AS edges
            """,
        )

        assert len(results) == 1
        nodes, edges = results[0]
        assert len(nodes) == 2
        # Single hop: from != to, so one virtual edge is created
        assert len(edges) == 1
        assert edges[0]["type"] == "V"

    def test_missing_virtual_edge_type_throws(self, connection):
        """project(path, {}) without virtualEdgeType key should throw."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "CREATE (:Node {id: 1})-[:R]->(:Node {id: 2});")

        with pytest.raises(Exception):
            execute_and_fetch_all(
                cursor,
                """
                MATCH p=(:Node {id: 1})-[:R]->(:Node {id: 2})
                WITH project(p, {}) AS g
                RETURN g
                """,
            )

    def test_non_path_first_arg_throws(self, connection):
        """project(non_path, {virtualEdgeType: 'X'}) should throw."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "CREATE (:Node {id: 1});")

        with pytest.raises(Exception):
            execute_and_fetch_all(
                cursor,
                """
                MATCH (n:Node {id: 1})
                WITH project(n, {virtualEdgeType: 'X'}) AS g
                RETURN g
                """,
            )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
