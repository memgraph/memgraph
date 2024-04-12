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
from common import get_results_length, memgraph
from gqlalchemy import GQLAlchemyError


def test_create_everything_then_drop_graph(memgraph):
    memgraph.execute("CREATE (:Node {id:1})-[:TYPE {id:2}]->(:Node {id:3})")
    memgraph.execute("CREATE INDEX ON :Node")
    memgraph.execute("CREATE INDEX ON :Node(id)")
    memgraph.execute("CREATE EDGE INDEX ON :EdgeType")
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT n.id IS UNIQUE;")
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT EXISTS (n.id);")
    memgraph.execute("CREATE TRIGGER t1 ON () UPDATE BEFORE COMMIT EXECUTE RETURN 1")
    memgraph.execute("CREATE TRIGGER t2 ON () UPDATE AFTER COMMIT EXECUTE RETURN 1")

    assert get_results_length(memgraph, "MATCH (n) RETURN n") == 2
    assert get_results_length(memgraph, "MATCH (n)-[r]->(m) RETURN r") == 1
    assert get_results_length(memgraph, "SHOW INDEX INFO") == 3
    assert get_results_length(memgraph, "SHOW CONSTRAINT INFO") == 2
    assert get_results_length(memgraph, "SHOW TRIGGERS") == 2

    with pytest.raises(GQLAlchemyError):
        memgraph.execute("DROP GRAPH")

    memgraph.execute("STORAGE MODE IN_MEMORY_ANALYTICAL")
    memgraph.execute("DROP GRAPH")

    assert get_results_length(memgraph, "MATCH (n) RETURN n") == 0
    assert get_results_length(memgraph, "MATCH (n)-[r]->(m) RETURN r") == 0
    assert get_results_length(memgraph, "SHOW INDEX INFO") == 0
    assert get_results_length(memgraph, "SHOW CONSTRAINT INFO") == 0
    assert get_results_length(memgraph, "SHOW TRIGGERS") == 0

    storage_info = list(memgraph.execute_and_fetch("SHOW STORAGE INFO"))
    print(storage_info)
    vertex_count = [x for x in storage_info if x["storage info"] == "vertex_count"][0]
    edge_count = [x for x in storage_info if x["storage info"] == "edge_count"][0]

    assert vertex_count["value"] == 0
    assert edge_count["value"] == 0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
