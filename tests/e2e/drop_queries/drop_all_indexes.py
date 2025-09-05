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

from common import get_results_length, memgraph


def test_drop_all_indexes(memgraph):
    memgraph.execute("CREATE (:Person {name: 'John', age: 30, location: point({x: 1, y: 2}), embedding: [1.0, 2.0]})")
    memgraph.execute("CREATE (:Person {name: 'Jane', age: 25, location: point({x: 3, y: 4}), embedding: [3.0, 4.0]})")
    memgraph.execute("CREATE (:Company {name: 'Acme Corp'})")
    memgraph.execute("""
        MATCH (p:Person {name: 'John'}), (c:Company {name: 'Acme Corp'})
        CREATE (p)-[:WORKS_AT {since: 2020}]->(c)
    """)
    memgraph.execute("""
        MATCH (p1:Person {name: 'John'}), (p2:Person {name: 'Jane'})
        CREATE (p1)-[:KNOWS {strength: 0.8}]->(p2)
    """)

    memgraph.execute("CREATE INDEX ON :Person")
    memgraph.execute("CREATE INDEX ON :Person(name)")
    memgraph.execute("CREATE EDGE INDEX ON :WORKS_AT")
    memgraph.execute("CREATE EDGE INDEX ON :WORKS_AT(since)")
    memgraph.execute("CREATE GLOBAL EDGE INDEX ON :(since)")
    memgraph.execute("CREATE POINT INDEX ON :Person(location)")
    memgraph.execute("CREATE TEXT INDEX personTextIndex ON :Person")
    memgraph.execute("CREATE VECTOR INDEX personVectorIndex ON :Person(embedding) WITH CONFIG {'dimension': 2, 'capacity': 100}")
    memgraph.execute("CREATE VECTOR EDGE INDEX vector_index_name ON :WORKS_AT(embedding) WITH CONFIG {'dimension': 2, 'capacity': 100}")

    index_info = list(memgraph.execute_and_fetch("SHOW INDEX INFO"))    
    assert len(index_info) == 9, f"Expected exactly 9 indexes, but got {len(index_info)}"

    memgraph.execute("DROP ALL INDEXES")
    
    index_info_after = list(memgraph.execute_and_fetch("SHOW INDEX INFO"))
    assert len(index_info_after) == 0, f"Expected 0 indexes after DROP ALL INDEXES, but got {len(index_info_after)}: {index_info_after}"

    person_count = get_results_length(memgraph, "MATCH (n:Person) RETURN n")
    company_count = get_results_length(memgraph, "MATCH (n:Company) RETURN n")
    edge_count = get_results_length(memgraph, "MATCH ()-[r]->() RETURN r")
    
    assert person_count == 2, f"Expected 2 Person nodes, but got {person_count}"
    assert company_count == 1, f"Expected 1 Company node, but got {company_count}"
    assert edge_count == 2, f"Expected 2 edges, but got {edge_count}"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
