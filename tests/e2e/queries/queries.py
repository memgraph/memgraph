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

import math
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


def test_a_range_bound_is_read_once_however_the_scan_is_planned(memgraph):
    """A bound is an expression, and asking it twice both repeats whatever it
    does and risks building the range from a different value than the one whose
    type was judged. `counter` answers a new value each call, so a bound reading
    it settles the range on its first answer and the row count says which."""
    memgraph.execute("MATCH (n) DETACH DELETE n;")
    memgraph.execute("UNWIND range(1, 5) AS i CREATE (:C {p: i});")

    def count(query):
        rows = list(memgraph.execute_and_fetch(query))
        return rows[0]["c"] if rows else 0

    # The counter starts at 0, so a bound of its first answer keeps all five
    # rows. A second call would bound at 1 and keep four.
    ranged = "MATCH (n:C) WHERE n.p > counter('bound', 0) RETURN count(n) AS c;"

    assert count(ranged) == 5

    memgraph.execute("CREATE INDEX ON :C(p);")

    assert count(ranged) == 5

    # The row count above settles on the first answer whether the bound was read
    # once or twice, so it cannot tell the two apart. Reading the counter after
    # the scan can: a scan that asked once leaves it at one.
    asked = "MATCH (n:C) WHERE n.p > counter('asked', 0) WITH count(n) AS c RETURN counter('asked', 0) AS after;"
    assert list(memgraph.execute_and_fetch(asked))[0]["after"] == 1


def test_a_temporal_range_answers_for_its_own_kind_however_the_scan_is_planned(memgraph):
    """A date, a local time, a local date time and a duration are four types no
    comparison places against each other, so a range over one keeps no row of
    another. They share one stored type, so an index whose band is that type
    would hand back the other three."""
    memgraph.execute("MATCH (n) DETACH DELETE n;")
    memgraph.execute(
        """CREATE (:T {t: DATE('2020-01-01')}),
                  (:T {t: DATE('2024-06-01')}),
                  (:T {t: LOCALTIME('12:00:00')}),
                  (:T {t: LOCALDATETIME('2024-06-01T12:00:00')}),
                  (:T {t: DURATION('P5D')}),
                  (:T {t: DURATION('P400D')});"""
    )

    def answers(query):
        return sorted(str(row["t"]) for row in memgraph.execute_and_fetch(query))

    one_sided = "MATCH (n:T) WHERE n.t > DATE('2020-01-01') RETURN n.t AS t;"
    other_way = "MATCH (n:T) WHERE n.t < DURATION('P100D') RETURN n.t AS t;"
    two_sided = "MATCH (n:T) WHERE n.t >= DATE('2020-01-01') AND n.t <= DATE('2024-06-01') RETURN n.t AS t;"
    mixed_kinds = "MATCH (n:T) WHERE n.t > DATE('2020-01-01') AND n.t < DURATION('P100D') RETURN n.t AS t;"

    without_index = [answers(q) for q in (one_sided, other_way, two_sided, mixed_kinds)]

    memgraph.execute("CREATE INDEX ON :T(t);")

    assert [answers(q) for q in (one_sided, other_way, two_sided, mixed_kinds)] == without_index

    # Non-vacuous: the unindexed answers are the ones the comparison gives, so a scan
    # matching them is answering rather than both returning everything.
    assert without_index[0] == ["2024-06-01"]
    assert len(without_index[1]) == 1
    assert without_index[3] == []


def test_a_nan_bound_keeps_no_row_however_the_scan_is_planned(memgraph):
    """A NaN has no order against any number, itself included, so all four
    ordered comparisons answer false and a filter keeps no row. The stored order
    still puts a NaN somewhere, so a band drawn around one would hand back
    whatever sits on its side of that position.

    The column holds no NaN of its own here. Whether an index can keep one and
    find it again is a question about the stored order rather than about the
    bound, and it is asked where that order is settled."""
    memgraph.execute("MATCH (n) DETACH DELETE n;")
    memgraph.execute("UNWIND range(1, 100) AS i CREATE (:N {v: toFloat(i)});")

    def count(query):
        rows = list(memgraph.execute_and_fetch(query))
        return rows[0]["c"] if rows else 0

    bounded_by_nan = [
        "MATCH (n:N) WHERE n.v < sqrt(-1) RETURN count(n) AS c;",
        "MATCH (n:N) WHERE n.v <= sqrt(-1) RETURN count(n) AS c;",
        "MATCH (n:N) WHERE n.v > sqrt(-1) RETURN count(n) AS c;",
        "MATCH (n:N) WHERE n.v >= sqrt(-1) RETURN count(n) AS c;",
        "MATCH (n:N) WHERE n.v > 0.0 AND n.v < sqrt(-1) RETURN count(n) AS c;",
    ]
    # Ordinary bounds over the same column, so that agreement is shown to mean
    # something other than both sides answering nothing.
    bounded_by_a_number = [
        "MATCH (n:N) WHERE n.v < 50.0 RETURN count(n) AS c;",
        "MATCH (n:N) WHERE n.v > 50.0 RETURN count(n) AS c;",
        "MATCH (n:N) WHERE n.v > 10.0 AND n.v < 20.0 RETURN count(n) AS c;",
    ]

    without_index = [count(q) for q in bounded_by_nan + bounded_by_a_number]

    memgraph.execute("CREATE INDEX ON :N(v);")

    assert [count(q) for q in bounded_by_nan + bounded_by_a_number] == without_index

    # Non-vacuous: a NaN bound keeps nothing, and an ordinary one keeps only the
    # rows it should rather than everything or nothing.
    assert without_index[: len(bounded_by_nan)] == [0] * len(bounded_by_nan)
    assert without_index[len(bounded_by_nan) :] == [49, 50, 9]


def test_an_index_over_a_column_holding_a_nan_answers_as_the_filter_does(memgraph):
    """A sorted container needs an answer for every pair it is handed, and IEEE
    leaves a NaN unordered against everything. An entry placed with no order
    goes where no later search finds it, and it can put other entries out of
    reach too, so a range over the column answers differently from the filter it
    stands in for, and differently from one run to the next.

    A NaN sits after every number and alongside another NaN, so the column holds
    its NaN rows and an ordinary bound reaches past them to the numbers."""
    memgraph.execute("MATCH (n) DETACH DELETE n;")
    memgraph.execute("UNWIND range(1, 100) AS i CREATE (:M {v: toFloat(i)});")
    memgraph.execute("UNWIND range(1, 3) AS i CREATE (:M {v: sqrt(-1)});")

    def count(query):
        rows = list(memgraph.execute_and_fetch(query))
        return rows[0]["c"] if rows else 0

    # An ordinary bound answers over a column holding a NaN, and keeps none of
    # the NaN rows: no comparison against one is true.
    bounded_by_a_number = [
        "MATCH (n:M) WHERE n.v < 50.0 RETURN count(n) AS c;",
        "MATCH (n:M) WHERE n.v > 50.0 RETURN count(n) AS c;",
        "MATCH (n:M) WHERE n.v > 10.0 AND n.v < 20.0 RETURN count(n) AS c;",
        "MATCH (n:M) WHERE n.v IS NOT NULL RETURN count(n) AS c;",
    ]

    without_index = [count(q) for q in bounded_by_a_number]

    memgraph.execute("CREATE INDEX ON :M(v);")

    assert [count(q) for q in bounded_by_a_number] == without_index

    # Non-vacuous, and the last of the four is the one that fails when the index
    # cannot hold a NaN: every row is still reachable, the three NaN rows among
    # them, while no ordered bound returns one.
    assert without_index == [49, 50, 9, 103]


def test_equality_against_a_nan_keeps_no_row_however_the_scan_is_planned(memgraph):
    """A NaN is equal to nothing, itself included, so an equality against one
    answers null for every row and a filter keeps none. An index orders two NaNs
    alongside each other so that it can find an entry again, which is a different
    question, and a scan reading that order for an answer would hand back rows no
    filter keeps."""
    memgraph.execute("MATCH (n) DETACH DELETE n;")
    memgraph.execute("UNWIND range(1, 50) AS i CREATE (:E {p: toFloat(i)});")
    memgraph.execute("UNWIND range(1, 3) AS i CREATE (:E {p: sqrt(-1)});")

    def count(query):
        rows = list(memgraph.execute_and_fetch(query))
        return rows[0]["c"] if rows else 0

    # Each paired with what it must answer. A NaN matches nothing; a number
    # beside one still matches what it should, so the whole predicate is not
    # being thrown away.
    queries_and_answers = [
        ("MATCH (n:E) WHERE n.p = sqrt(-1) RETURN count(n) AS c;", 0),
        ("MATCH (n:E) WHERE n.p IN [sqrt(-1)] RETURN count(n) AS c;", 0),
        ("MATCH (n:E) WHERE n.p IN [1.0, sqrt(-1)] RETURN count(n) AS c;", 1),
        ("MATCH (a:E), (b:E) WHERE a.p = b.p AND a.p = sqrt(-1) RETURN count(*) AS c;", 0),
        ("MATCH (n:E) WHERE n.p = 1.0 RETURN count(n) AS c;", 1),
        ("MATCH (n:E) WHERE n.p IN [1.0, 2.0] RETURN count(n) AS c;", 2),
    ]
    expected = [answer for _, answer in queries_and_answers]

    without_index = [count(q) for q, _ in queries_and_answers]
    assert without_index == expected

    memgraph.execute("CREATE INDEX ON :E(p);")

    assert [count(q) for q, _ in queries_and_answers] == expected


def test_an_edge_equality_against_a_nan_keeps_no_row_however_the_scan_is_planned(memgraph):
    """An edge property-value scan converts the sought value itself and reads the
    equality from the index. A NaN is equal to nothing, itself included, so the
    filter the scan stands in for keeps no row, while the index holds two NaNs
    alongside each other so that it can find an entry again. The scan has to
    answer as the filter does rather than as the index orders."""
    memgraph.execute("MATCH (n) DETACH DELETE n;")
    memgraph.execute("CREATE (a:From), (b:To);")
    memgraph.execute("MATCH (a:From), (b:To) UNWIND range(1, 5) AS i CREATE (a)-[:T {p: toFloat(i)}]->(b);")
    memgraph.execute("MATCH (a:From), (b:To) UNWIND range(1, 3) AS i CREATE (a)-[:T {p: sqrt(-1)}]->(b);")

    def count(query):
        rows = list(memgraph.execute_and_fetch(query))
        return rows[0]["c"] if rows else 0

    # Each paired with what it must answer. The last two are what the scan must
    # still find, so a predicate answering nothing at all would not pass here.
    queries_and_answers = [
        ("MATCH ()-[r:T]->() WHERE r.p = sqrt(-1) RETURN count(r) AS c;", 0),
        ("MATCH ()-[r:T]->() WHERE r.p IN [sqrt(-1)] RETURN count(r) AS c;", 0),
        ("MATCH ()-[r:T]->() WHERE r.p IN [1.0, sqrt(-1)] RETURN count(r) AS c;", 1),
        ("MATCH ()-[r:T]->() WHERE r.p = 1.0 RETURN count(r) AS c;", 1),
        ("MATCH ()-[r:T]->() WHERE r.p IS NOT NULL RETURN count(r) AS c;", 8),
    ]
    expected = [answer for _, answer in queries_and_answers]

    assert [count(q) for q, _ in queries_and_answers] == expected

    memgraph.execute("CREATE EDGE INDEX ON :T(p);")

    assert [count(q) for q, _ in queries_and_answers] == expected


def test_a_hash_join_over_a_nan_keeps_no_row_however_the_join_is_planned(memgraph):
    """A hash join reads its table by equivalence, which holds two NaNs alike so
    that a lookup can find an entry again. What the join answers is an equality,
    and a NaN is equal to nothing, itself included, so a pair of them joins no
    rows whichever way the planner chooses to answer."""
    memgraph.execute("MATCH (n) DETACH DELETE n;")
    memgraph.execute("CREATE (:X {v: sqrt(-1)}), (:Y {v: sqrt(-1)});")
    memgraph.execute("CREATE (:X {v: 7.0}), (:Y {v: 7.0});")

    def count(query):
        rows = list(memgraph.execute_and_fetch(query))
        return rows[0]["c"] if rows else 0

    joined = "MATCH (a:X), (b:Y) WHERE a.v = b.v RETURN count(*) AS c;"
    # The same question asked so that a filter reads the equality instead.
    filtered = "MATCH (a:X) WITH a MATCH (b:Y) WITH a, b WHERE a.v = b.v RETURN count(*) AS c;"

    # Naming the operator the test is here for: without it the join is answered
    # some other way and the test asks nothing.
    plan = "\n".join(row["QUERY PLAN"] for row in memgraph.execute_and_fetch(f"EXPLAIN {joined}"))
    assert "HashJoin" in plan, plan

    # Only the pair holding 7.0 joins: no equality against a NaN is true.
    assert count(filtered) == 1
    assert count(joined) == 1


DUMP_FLOAT_SETUP = [
    "CREATE (:Scalar {big: 1e20, integral: 2.0, large: 3e38, negzero: -0.0})",
    "CREATE (:Lists {doubles: [1e20, 2.0, 3e38], numerics: [1, 1e20, 2.0], "
    "special: [1.0/0.0, -1.0/0.0, 0.0/0.0], negzero: [1.0, -0.0]})",
    "CREATE VECTOR INDEX f32_idx ON :Vec32(v) WITH CONFIG {'dimension': 2, 'capacity': 10}",
    "CREATE (:Vec32 {tag: 'plain', v: [123456.789, 0.1]})",
    "CREATE (:Vec32 {tag: 'negzero', v: [-0.0, 0.1]})",
    "CREATE VECTOR INDEX f16_idx ON :Vec16(v) WITH CONFIG {'dimension': 1, 'capacity': 10, 'scalar_kind': 'f16'}",
    "CREATE (:Vec16 {v: [70000.0]})",
    "CREATE (:Pts {big: POINT({x: 1e20, y: 2, srid: 7203}), "
    "pinf: POINT({x: 1.0/0.0, y: 2, srid: 7203}), "
    "ninf: POINT({x: -1.0/0.0, y: 2, srid: 7203}), "
    "nan: POINT({x: 0.0/0.0, y: 2, srid: 7203}), "
    "negzero: POINT({x: -0.0, y: 1.0, srid: 7203}), "
    "negzero3d: POINT({x: 1.0, y: -0.0, z: -0.0, srid: 9757}), "
    "negzerowgs: POINT({longitude: -0.0, latitude: 1.0, srid: 4326})})",
]

DUMP_FLOAT_PROBES = [
    "MATCH (n:Scalar) RETURN n.big AS big, n.integral AS integral, n.large AS large, n.negzero AS negzero",
    "MATCH (n:Lists) RETURN n.doubles AS doubles, n.numerics AS numerics, n.special AS special, n.negzero AS negzero",
    "MATCH (n:Vec32 {tag: 'plain'}) RETURN n.v AS v",
    "MATCH (n:Vec16) RETURN n.v AS v",
    "MATCH (n:Pts) RETURN n.big.x AS big, n.pinf.x AS pinf, n.ninf.x AS ninf, n.nan.x AS nan, "
    "n.negzero.x AS negzero, n.negzero3d.y AS negzero3d_y, n.negzero3d.z AS negzero3d_z, "
    "n.negzerowgs.longitude AS negzerowgs",
    "MATCH (n:Vec32 {tag: 'negzero'}) RETURN n.v AS v",
]


def dump_float_read(memgraph, query):
    rows = list(memgraph.execute_and_fetch(query))
    assert rows, f"no row for {query}"
    return rows[0]


def dump_float_assert_negative_zero(value, what):
    assert value == 0.0 and math.copysign(1.0, value) == -1.0, f"{what}: expected -0.0, got {value!r}"


def dump_float_drop_vector_indexes(memgraph):
    # gqlalchemy's drop_indexes() renders a vector index as `DROP INDEX ON ::Label(prop)`,
    # which the server rejects, so the test removes its own indexes instead.
    # Dropping an absent index is only a notification, so a real failure surfaces here.
    for index in ("f32_idx", "f16_idx"):
        memgraph.execute(f"DROP VECTOR INDEX {index}")


def test_dump_float_roundtrip(memgraph):
    """Every float the dumper writes has to be a literal the parser accepts and
    re-parses to the stored value and kind: max_digits10, an integral float
    keeps its `.0`, and a non-finite value is written as an expression that
    produces it. Index-backed floats are spelled at the index's precision."""
    for query in DUMP_FLOAT_SETUP:
        memgraph.execute(query)
    try:
        before = [dump_float_read(memgraph, query) for query in DUMP_FLOAT_PROBES]

        dump = list(memgraph.execute_and_fetch("DUMP DATABASE"))
        statements = [row["QUERY"] for row in dump]
        assert statements, "DUMP DATABASE returned no statements"

        memgraph.execute("MATCH (n) DETACH DELETE n")
        dump_float_drop_vector_indexes(memgraph)

        failed = []
        for statement in statements:
            try:
                memgraph.execute(statement)
            except Exception as error:  # noqa: BLE001 - the parser error is the assertion
                failed.append((statement, error))
        assert not failed, "dumped statements did not re-import:\n" + "\n".join(
            f"{statement}\n  -> {error}" for statement, error in failed
        )

        after = [dump_float_read(memgraph, query) for query in DUMP_FLOAT_PROBES]

        scalar_before, scalar_after = before[0], after[0]
        for name in ("big", "large"):
            assert isinstance(scalar_after[name], float), f"{name} came back as {type(scalar_after[name])}"
            assert scalar_after[name] == scalar_before[name], f"{name}: {scalar_before[name]} -> {scalar_after[name]}"
        dump_float_assert_negative_zero(scalar_after["negzero"], "scalar property")
        assert isinstance(scalar_after["integral"], float), f"integral came back as {type(scalar_after['integral'])}"
        assert scalar_after["integral"] == 2.0

        lists_before, lists_after = before[1], after[1]
        assert lists_after["doubles"] == lists_before["doubles"]
        assert all(isinstance(value, float) for value in lists_after["doubles"])
        assert lists_after["numerics"] == lists_before["numerics"]
        assert isinstance(lists_after["numerics"][0], int)
        assert all(isinstance(value, float) for value in lists_after["numerics"][1:])
        special = lists_after["special"]
        assert len(special) == 3
        assert math.isinf(special[0]) and special[0] > 0
        assert math.isinf(special[1]) and special[1] < 0
        assert math.isnan(special[2])
        assert lists_after["negzero"][0] == 1.0
        dump_float_assert_negative_zero(lists_after["negzero"][1], "list element")

        assert after[2]["v"] == before[2]["v"], f"vector f32: {before[2]['v']} -> {after[2]['v']}"
        vec16 = after[3]["v"]
        assert len(vec16) == 1 and math.isinf(vec16[0]), f"vector f16: {vec16}"
        assert vec16 == before[3]["v"], f"vector f16: {before[3]['v']} -> {vec16}"
        assert math.copysign(1.0, vec16[0]) == math.copysign(1.0, before[3]["v"][0])

        vec_negzero = after[5]["v"]
        assert len(vec_negzero) == 2 and vec_negzero[1] == before[5]["v"][1]
        dump_float_assert_negative_zero(vec_negzero[0], "vector f32 element")

        points_before, points_after = before[4], after[4]
        assert points_after["big"] == points_before["big"] == 1e20
        assert math.isinf(points_after["pinf"]) and points_after["pinf"] > 0
        assert math.isinf(points_after["ninf"]) and points_after["ninf"] < 0
        assert math.isnan(points_after["nan"])
        dump_float_assert_negative_zero(points_after["negzero"], "point x")
        dump_float_assert_negative_zero(points_after["negzero3d_y"], "3d point y")
        dump_float_assert_negative_zero(points_after["negzero3d_z"], "3d point z")
        dump_float_assert_negative_zero(points_after["negzerowgs"], "wgs point longitude")
    finally:
        dump_float_drop_vector_indexes(memgraph)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
