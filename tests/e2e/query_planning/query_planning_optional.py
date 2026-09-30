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

QUERY_PLAN = "QUERY PLAN"


def test_optional_match_doesnt_use_edge_indices(memgraph):
    memgraph.execute("CREATE (org :Org{name: 'big corp'})")
    memgraph.execute("CREATE (org)-[:WORKS_AT]->(person1 :Person{name: 'john'})")
    memgraph.execute("CREATE (org)-[:WORKS_AT]->(person2 :Person{name: 'ada'})")
    memgraph.execute("CREATE (org)-[:WORKS_AT]->(person3 :Person{name: 'mary'})")
    memgraph.execute("CREATE (org)-[:WORKS_AT]->(person4 :Person{name: 'kate'})")
    memgraph.execute("CREATE (person1)-[:HAS_KID]->(child1 :Child{name: 'alex'})")
    memgraph.execute("CREATE (person1)-[:HAS_KID]->(child2 :Child{name: 'sarah'})")
    memgraph.execute("CREATE (person4)-[:HAS_KID]->(child3 :Child{name: 'ben'})")
    memgraph.execute("CREATE EDGE INDEX ON :HAS_KID")

    expected_explain = [
        f" * Produce {{person.name, kid_names}}",
        f" * Aggregate {{COLLECT-1}} {{person}}",
        f" * Optional",
        f" |\\ ",
        f" | * Expand (person)-[anon3:HAS_KID]->(kid)",
        f" | * Once",
        f" * Expand (org)-[anon1:WORKS_AT]->(person)",
        f" * Filter (org :Org), {{org.name}}",
        f" * ScanAll (org)",
        f" * Once",
    ]

    results = list(
        memgraph.execute_and_fetch(
            "EXPLAIN MATCH (org:Org {name: 'big corp'})-[:WORKS_AT]->(person) OPTIONAL MATCH (person)-[:HAS_KID]->(kid) RETURN person.name, collect(kid.name) as kid_names;"
        )
    )
    actual_explain = [x[QUERY_PLAN] for x in results]

    assert expected_explain == actual_explain


def test_optional_match_uses_edge_indices_if_not_expanding(memgraph):
    memgraph.execute("UNWIND range(1, 10) as x CREATE ()-[:ET1]->()")
    memgraph.execute("UNWIND range(1, 100) as x CREATE ()")
    memgraph.execute("CREATE EDGE INDEX ON :ET1")

    expected_explain = [
        f" * Produce {{r}}",
        f" * Optional",
        f" |\\ ",
        f" | * ScanAllByEdgeType (anon1)-[r:ET1]-(anon2)",
        f" | * Once",
        f" * Once",
    ]

    results = list(memgraph.execute_and_fetch("EXPLAIN OPTIONAL MATCH ()-[r:ET1]-() RETURN *;"))
    actual_explain = [x[QUERY_PLAN] for x in results]

    assert expected_explain == actual_explain


def test_match_optional_match_uses_edge_indices_if_input_branch_symbols_dont_match(memgraph):
    memgraph.execute("UNWIND range(1, 10) as x CREATE ()-[:ET1]->()")
    memgraph.execute("UNWIND range(1, 100) as x CREATE ()")
    memgraph.execute("CREATE EDGE INDEX ON :ET1")

    expected_explain = [
        f" * Produce {{n, r}}",
        f" * Optional",
        f" |\\ ",
        f" | * ScanAllByEdgeType (anon2)-[r:ET1]->(anon3)",
        f" | * Once",
        f" * ScanAll (n)",
        f" * Once",
    ]

    results = list(memgraph.execute_and_fetch("EXPLAIN MATCH (n) OPTIONAL MATCH ()-[r:ET1]->() RETURN *;"))
    actual_explain = [x[QUERY_PLAN] for x in results]

    assert expected_explain == actual_explain


def test_optional_match_optional_match_uses_edge_indices_if_input_branch_symbols_dont_match(memgraph):
    memgraph.execute("UNWIND range(1, 10) as x CREATE ()-[:ET1]->()")
    memgraph.execute("UNWIND range(1, 100) as x CREATE ()")
    memgraph.execute("CREATE EDGE INDEX ON :ET1")

    expected_explain = [
        f" * Produce {{r1, r2}}",
        f" * Optional",
        f" |\\ ",
        f" | * ScanAllByEdgeType (anon4)-[r2:ET1]->(anon5)",
        f" | * Once",
        f" * Optional",
        f" |\\ ",
        f" | * ScanAllByEdgeType (anon1)-[r1:ET1]->(anon2)",
        f" | * Once",
        f" * Once",
    ]

    results = list(
        memgraph.execute_and_fetch("EXPLAIN OPTIONAL MATCH ()-[r1:ET1]->() OPTIONAL MATCH ()-[r2:ET1]->() RETURN *;")
    )
    actual_explain = [x[QUERY_PLAN] for x in results]

    assert expected_explain == actual_explain


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
