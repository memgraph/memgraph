# Copyright 2023 Memgraph Ltd.
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


def test_set_multiple_properties_on_vertex_via_query_module(memgraph):
    memgraph.execute(
        """
CREATE TRIGGER trigger ON () UPDATE BEFORE COMMIT EXECUTE
UNWIND updatedVertices AS updatedVertex
SET updatedVertex.vertex.updated = true;
"""
    )

    memgraph.execute("CREATE (n)")
    has_updated = list(
        memgraph.execute_and_fetch(
            "MATCH (n) CALL set_properties_module.set_multiple_properties(n) YIELD updated RETURN updated"
        )
    )

    assert len(has_updated) == 1
    assert has_updated[0]["updated"] == True

    updated_vertex = next(memgraph.execute_and_fetch("MATCH (n) RETURN n"))["n"]

    assert updated_vertex._properties["prop1"] == 1
    assert updated_vertex._properties["prop2"] == 2
    assert updated_vertex._properties["prop3"] == 3
    assert updated_vertex._properties["updated"] == True


def test_set_multiple_properties_on_edge_via_query_module(memgraph):
    memgraph.execute(
        """
CREATE TRIGGER trigger ON --> UPDATE BEFORE COMMIT EXECUTE
UNWIND updatedEdges AS updatedEdge
SET updatedEdge.edge.updated = true;
"""
    )

    memgraph.execute("CREATE (n)-[r:TYPE]->(m)")
    has_updated = list(
        memgraph.execute_and_fetch(
            "MATCH ()-[r]->() CALL set_properties_module.set_multiple_properties(r) YIELD updated RETURN updated"
        )
    )

    assert len(has_updated) == 1
    assert has_updated[0]["updated"] == True

    updated_edge = next(memgraph.execute_and_fetch("MATCH ()-[r]->() RETURN r"))["r"]

    assert updated_edge._properties["prop1"] == 1
    assert updated_edge._properties["prop2"] == 2
    assert updated_edge._properties["prop3"] == 3
    assert updated_edge._properties["updated"] == True


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
