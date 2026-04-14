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


class TestVirtualEdgesWithProcedures:
    def test_procedure_sees_virtual_edges(self, connection):
        """A procedure iterating vertex.out_edges should see virtual edges."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "CREATE (:N {id: 1})-[:R]->(:N {id: 2})-[:R]->(:N {id: 3});")

        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})-[*]->(:N {id: 3})
            WITH derive(p, {virtualEdgeType: 'DERIVED'}) AS graph
            MATCH (n:N {id: 1})
            CALL read.subgraph_edge_info(graph, n) YIELD edge_type, weight
            RETURN edge_type, weight
            """,
        )

        assert len(results) == 1
        assert results[0][0] == "DERIVED"
        assert results[0][1] is None  # no properties set

    def test_procedure_sees_virtual_edge_properties(self, connection):
        """Virtual edge properties set via relationshipProperties should be visible to procedures."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "CREATE (:N {id: 1})-[:R]->(:N {id: 2})-[:R]->(:N {id: 3});")

        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})-[*]->(:N {id: 3})
            WITH derive(p, {virtualEdgeType: 'DERIVED', relationshipProperties: {weight: 99}}) AS graph
            MATCH (n:N {id: 1})
            CALL read.subgraph_edge_info(graph, n) YIELD edge_type, weight
            RETURN edge_type, weight
            """,
        )

        assert len(results) == 1
        assert results[0][0] == "DERIVED"
        assert results[0][1] == 99

    def test_procedure_sees_virtual_node_labels_and_properties(self, connection):
        """Virtual node labels and properties should be visible to procedures."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "CREATE (:N {id: 1})-[:R]->(:N {id: 2});")

        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})-[:R]->(:N {id: 2})
            WITH derive(p, {
                virtualEdgeType: 'V',
                sourceNodeLabels: ['Expert'],
                sourceNodeProperties: {score: 42}
            }) AS graph
            MATCH (n:N {id: 1})
            CALL read.subgraph_vertex_info(graph, n) YIELD labels, score
            RETURN labels, score
            """,
        )

        assert len(results) == 1
        assert "Expert" in results[0][0]
        assert results[0][1] == 42


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
