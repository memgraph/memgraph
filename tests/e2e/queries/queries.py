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


def test_equality_against_an_unstorable_value_holding_a_null_raises_on_no_plan(memgraph):
    """A value holding a null answers every equality null, so nothing matches it
    and no plan needs to convert it to a property. One holding a graph entity
    beside the null cannot be converted at all, and a scan must not be the reason
    the query raises when the filter it stands in for does not."""
    memgraph.execute("MATCH (n) DETACH DELETE n;")
    memgraph.execute("CREATE (:V {p: [1, 2]}), (:Anchor);")
    memgraph.execute("CREATE (a:From), (b:To);")
    memgraph.execute("MATCH (a:From), (b:To) CREATE (a)-[:T {p: [1, 2]}]->(b);")

    def count(query):
        rows = list(memgraph.execute_and_fetch(query))
        return rows[0]["c"] if rows else 0

    sought_holding_a_node = "MATCH (x:Anchor), (n:V) WHERE n.p = [null, x] RETURN count(n) AS c;"
    sought_on_an_edge = "MATCH (x:Anchor), ()-[r:T]->() WHERE r.p = [null, x] RETURN count(r) AS c;"

    # Without an index the filter reads the equality directly and keeps nothing.
    assert count(sought_holding_a_node) == 0
    assert count(sought_on_an_edge) == 0

    memgraph.execute("CREATE INDEX ON :V(p);")
    memgraph.execute("CREATE EDGE INDEX ON :T(p);")

    # With one, the scan answers the same way rather than raising over a value it
    # would never have had to store.
    assert count(sought_holding_a_node) == 0
    assert count(sought_on_an_edge) == 0


def test_a_scan_does_not_raise_over_a_sought_value_no_filter_needs_stored(memgraph):
    """A graph element is not equal to any stored property and orders against
    none, so a filter answers without ever needing it as a property value. A
    scan standing in for that filter must answer too: converting the value first
    makes the query fail only once an index exists."""
    memgraph.execute("MATCH (n) DETACH DELETE n;")
    memgraph.execute("CREATE (:V {p: 1}), (:V {p: 'abc'}), (:Anchor);")
    memgraph.execute("CREATE (a:From), (b:To);")
    memgraph.execute("MATCH (a:From), (b:To) CREATE (a)-[:T {p: 1}]->(b);")

    def count(query):
        rows = list(memgraph.execute_and_fetch(query))
        return rows[0]["c"] if rows else 0

    sought = "MATCH (x:Anchor), (n:V) WHERE n.p = x RETURN count(n) AS c;"
    # The element that is not storable settles for nothing; the one beside it
    # still matches, so this membership test keeps a row rather than failing.
    membership = "MATCH (x:Anchor), (n:V) WHERE n.p IN [1, x] RETURN count(n) AS c;"

    # An edge scan reads the same value through its own conversion, for an
    # equality and for a range.
    edge_sought = "MATCH (x:Anchor), ()-[r:T]->() WHERE r.p = x RETURN count(r) AS c;"
    edge_ranged = "MATCH (x:Anchor), ()-[r:T]->() WHERE r.p < x RETURN count(r) AS c;"

    assert count(sought) == 0
    assert count(membership) == 1
    assert count(edge_sought) == 0
    assert count(edge_ranged) == 0

    memgraph.execute("CREATE INDEX ON :V(p);")
    memgraph.execute("CREATE EDGE INDEX ON :T(p);")

    assert count(sought) == 0
    assert count(membership) == 1
    assert count(edge_sought) == 0
    assert count(edge_ranged) == 0


def test_negated_membership_keeps_no_row_whose_sought_value_is_null(memgraph):
    """Membership of a null in a list holding anything is undecided, and a filter
    keeps no row it cannot decide. Negating it keeps none either, since `NOT` of
    an undecided answer is undecided. The set the operator caches the list in is
    filled by the first row and read by every row after it, so a row holding a
    null has to be answered the same way whichever of those it is."""
    memgraph.execute("MATCH (n) DETACH DELETE n;")
    memgraph.execute("UNWIND range(1, 5) AS i CREATE (:R {v: i});")
    # A node with no `v` at all, so reading it gives null.
    memgraph.execute("CREATE (:R {w: 1});")

    def count(query):
        rows = list(memgraph.execute_and_fetch(query))
        return rows[0]["c"] if rows else 0

    # Four rows hold a value that is decidedly not 1, and the row holding none is
    # undecided rather than kept.
    assert count("MATCH (n:R) WHERE NOT (n.v IN [1]) RETURN count(n) AS c;") == 4
    assert count("MATCH (n:R) WHERE n.v IN [1] RETURN count(n) AS c;") == 1

    # A null in the list leaves every row it does not otherwise match undecided,
    # so only the rows the list does hold survive.
    assert count("MATCH (n:R) WHERE n.v IN [1, 2, null] RETURN count(n) AS c;") == 2
    assert count("MATCH (n:R) WHERE NOT (n.v IN [1, 2, null]) RETURN count(n) AS c;") == 0

    # The row holding a null reaches the operator first here, so it fills the set
    # rather than reading one already filled.
    assert count("MATCH (n:R) WITH n ORDER BY n.v DESC WHERE NOT (n.v IN [1]) RETURN count(n) AS c;") == 4


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
