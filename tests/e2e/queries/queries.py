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


def test_indexed_join_with_indices(memgraph):
    memgraph.execute(
        "CREATE (c:A {prop: 1})-[b:TYPE]->(p:A {prop: 1}) CREATE (cf:B:A {prop : 1}) CREATE (pf:B:A {prop : 1});"
    )
    memgraph.execute("CREATE INDEX ON :A;")
    memgraph.execute("CREATE INDEX ON :B;")
    memgraph.execute("CREATE INDEX ON :A(prop);")
    memgraph.execute("CREATE INDEX ON :B(prop);")

    results = list(
        memgraph.execute_and_fetch(
            "match (c:A)-[b:TYPE]->(p:A) match (cf:B:A {prop : c.prop}) match (pf:B:A {prop : p.prop}) return c;"
        )
    )

    assert len(results) == 4
    for res in results:
        assert res["c"].prop == 1


def test_equality_against_a_null_holding_list_answers_the_same_way_whichever_plan_runs(memgraph):
    """A list holding a null compares null, so no row passes. Every way of
    answering that equality has to agree: a filter, an index scan, and a join
    that replaced the filter with a hash lookup."""
    memgraph.execute("MATCH (n) DETACH DELETE n;")
    memgraph.execute("CREATE (:V {p: [1, null]}), (:V {p: [1, null]});")
    memgraph.execute("CREATE (:W {p: [1, 2]}), (:W {p: [1, 2]});")

    def count(query):
        rows = list(memgraph.execute_and_fetch(query))
        return rows[0]["c"] if rows else 0

    joined_on_a_null = "MATCH (a:V), (b:V) WHERE a.p = b.p RETURN count(*) AS c;"
    joined_without_a_null = "MATCH (a:W), (b:W) WHERE a.p = b.p RETURN count(*) AS c;"

    # A join reads the equality through a hash lookup, which cannot answer null.
    assert count(joined_on_a_null) == 0
    # And it still joins what it should.
    assert count(joined_without_a_null) == 4

    memgraph.execute("CREATE INDEX ON :V(p);")
    memgraph.execute("CREATE INDEX ON :W(p);")

    # The index gives the planner another way to answer, which must not change it.
    assert count(joined_on_a_null) == 0
    assert count(joined_without_a_null) == 4
    assert count("MATCH (n:V) WHERE n.p = [1, null] RETURN count(n) AS c;") == 0
    assert count("MATCH (n:W) WHERE n.p = [1, 2] RETURN count(n) AS c;") == 2


def test_an_edge_property_equality_answers_the_same_way_with_and_without_an_index(memgraph):
    """An edge property-value scan reads the equality from the index, and has to
    find nothing where the filter it stands in for keeps nothing."""
    memgraph.execute("MATCH (n) DETACH DELETE n;")
    memgraph.execute("CREATE (a:From), (b:To);")
    memgraph.execute("MATCH (a:From), (b:To) CREATE (a)-[:T {p: [1, null]}]->(b);")
    memgraph.execute("MATCH (a:From), (b:To) CREATE (a)-[:T {p: [1, 2]}]->(b);")

    def count(query):
        rows = list(memgraph.execute_and_fetch(query))
        return rows[0]["c"] if rows else 0

    sought_holding_a_null = "MATCH ()-[r:T]->() WHERE r.p = [1, null] RETURN count(r) AS c;"
    sought_holding_none = "MATCH ()-[r:T]->() WHERE r.p = [1, 2] RETURN count(r) AS c;"

    assert count(sought_holding_a_null) == 0
    assert count(sought_holding_none) == 1

    memgraph.execute("CREATE EDGE INDEX ON :T(p);")

    assert count(sought_holding_a_null) == 0
    assert count(sought_holding_none) == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
