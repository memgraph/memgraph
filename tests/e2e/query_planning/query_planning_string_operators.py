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
from common import memgraph

QUERY_PLAN = "QUERY PLAN"


def get_plan(memgraph, query, params=None):
    args = (f"EXPLAIN {query}",) if params is None else (f"EXPLAIN {query}", params)
    return [x[QUERY_PLAN] for x in memgraph.execute_and_fetch(*args)]


def _count(memgraph, query):
    return list(memgraph.execute_and_fetch(query))[0]["c"]


def operator_names(plan):
    return [line.strip().lstrip("* ").split(" ")[0] for line in plan]


@pytest.fixture(autouse=True)
def setup_graph(memgraph):
    memgraph.execute("CREATE INDEX ON :N(type);")
    memgraph.execute("FOREACH (x IN ['alpha', 'beta', 'gamma'] | CREATE (:N {type: x}));")
    yield
    memgraph.drop_database()


def test_contains_uses_label_property_scan(memgraph):
    plan = get_plan(memgraph, "MATCH (n:N) WHERE n.type CONTAINS 'alp' RETURN n")
    ops = operator_names(plan)
    assert "ScanAllByLabelProperties" in ops, f"Expected ScanAllByLabelProperties, got: {plan}"
    assert "ScanAll" not in ops, f"ScanAll should not appear, got: {plan}"


def test_starts_with_uses_label_property_scan(memgraph):
    plan = get_plan(memgraph, "MATCH (n:N) WHERE n.type STARTS WITH 'al' RETURN n")
    ops = operator_names(plan)
    assert "ScanAllByLabelProperties" in ops, f"Expected ScanAllByLabelProperties, got: {plan}"
    assert "ScanAll" not in ops, f"ScanAll should not appear, got: {plan}"


def test_ends_with_uses_label_property_scan(memgraph):
    plan = get_plan(memgraph, "MATCH (n:N) WHERE n.type ENDS WITH 'ha' RETURN n")
    ops = operator_names(plan)
    assert "ScanAllByLabelProperties" in ops, f"Expected ScanAllByLabelProperties, got: {plan}"
    assert "ScanAll" not in ops, f"ScanAll should not appear, got: {plan}"


def test_contains_correctness(memgraph):
    result = list(memgraph.execute_and_fetch("MATCH (n:N) WHERE n.type CONTAINS 'ph' RETURN n.type AS t"))
    assert [r["t"] for r in result] == ["alpha"]


def test_starts_with_correctness(memgraph):
    result = list(memgraph.execute_and_fetch("MATCH (n:N) WHERE n.type STARTS WITH 'be' RETURN n.type AS t"))
    assert [r["t"] for r in result] == ["beta"]


def test_ends_with_correctness(memgraph):
    result = list(memgraph.execute_and_fetch("MATCH (n:N) WHERE n.type ENDS WITH 'ma' RETURN n.type AS t"))
    assert [r["t"] for r in result] == ["gamma"]


def test_and_composition_two_string_ops(memgraph):
    plan = get_plan(memgraph, "MATCH (n:N) WHERE n.type CONTAINS 'a' AND n.type STARTS WITH 'a' RETURN n")
    ops = operator_names(plan)
    assert "ScanAllByLabelProperties" in ops, f"Expected ScanAllByLabelProperties, got: {plan}"
    assert "ScanAll" not in ops, f"ScanAll should not appear, got: {plan}"
    result = list(
        memgraph.execute_and_fetch(
            "MATCH (n:N) WHERE n.type CONTAINS 'a' AND n.type STARTS WITH 'a' RETURN n.type AS t ORDER BY t"
        )
    )
    assert [r["t"] for r in result] == ["alpha"]


def test_not_falls_back_to_scan_all(memgraph):
    plan = get_plan(memgraph, "MATCH (n:N) WHERE NOT n.type CONTAINS 'alp' RETURN n")
    ops = operator_names(plan)
    assert "ScanAll" in ops or "ScanAllByLabel" in ops, f"Expected ScanAll or ScanAllByLabel, got: {plan}"
    result = list(
        memgraph.execute_and_fetch("MATCH (n:N) WHERE NOT n.type CONTAINS 'alp' RETURN n.type AS t ORDER BY t")
    )
    assert [r["t"] for r in result] == ["beta", "gamma"]


def test_no_index_falls_back_to_scan_all(memgraph):
    memgraph.execute("CREATE (:M {name: 'hello'});")
    plan = get_plan(memgraph, "MATCH (n:M) WHERE n.name CONTAINS 'ell' RETURN n")
    ops = operator_names(plan)
    assert (
        "ScanAllByLabelProperties" not in ops
    ), f"ScanAllByLabelProperties should not appear without index, got: {plan}"
    result = list(memgraph.execute_and_fetch("MATCH (n:M) WHERE n.name CONTAINS 'ell' RETURN n.name AS t"))
    assert [r["t"] for r in result] == ["hello"]


def test_starts_with_empty_string_matches_all(memgraph):
    result = list(memgraph.execute_and_fetch("MATCH (n:N) WHERE n.type STARTS WITH '' RETURN n.type AS t ORDER BY t"))
    assert [r["t"] for r in result] == ["alpha", "beta", "gamma"]


def test_starts_with_exact_match(memgraph):
    result = list(memgraph.execute_and_fetch("MATCH (n:N) WHERE n.type STARTS WITH 'alpha' RETURN n.type AS t"))
    assert [r["t"] for r in result] == ["alpha"]


def test_starts_with_no_match(memgraph):
    result = list(memgraph.execute_and_fetch("MATCH (n:N) WHERE n.type STARTS WITH 'z' RETURN n.type AS t"))
    assert [r["t"] for r in result] == []


def test_starts_with_shared_prefix(memgraph):
    """Values 'alpha', 'beta', 'gamma' — STARTS WITH 'al' should only match 'alpha'."""
    result = list(memgraph.execute_and_fetch("MATCH (n:N) WHERE n.type STARTS WITH 'al' RETURN n.type AS t"))
    assert [r["t"] for r in result] == ["alpha"]


def test_starts_with_mixed_types(memgraph):
    """Non-string property values should not be returned."""
    memgraph.execute("CREATE (:N {type: 123});")
    memgraph.execute("CREATE (:N {type: true});")
    result = list(memgraph.execute_and_fetch("MATCH (n:N) WHERE n.type STARTS WITH 'al' RETURN n.type AS t"))
    assert [r["t"] for r in result] == ["alpha"]


def test_starts_with_null_returns_empty(memgraph):
    result = list(memgraph.execute_and_fetch("MATCH (n:N) WHERE n.type STARTS WITH null RETURN n.type AS t"))
    assert result == []


def test_starts_with_null_param_returns_empty(memgraph):
    result = list(memgraph.execute_and_fetch("MATCH (n:N) WHERE n.type STARTS WITH $p RETURN n.type AS t", {"p": None}))
    assert result == []


def test_starts_with_string_param_uses_index(memgraph):
    plan = get_plan(memgraph, "MATCH (n:N) WHERE n.type STARTS WITH $p RETURN n", {"p": "al"})
    ops = operator_names(plan)
    assert "ScanAllByLabelProperties" in ops, f"Expected ScanAllByLabelProperties, got: {plan}"
    assert "ScanAll" not in ops, f"ScanAll should not appear, got: {plan}"
    result = list(memgraph.execute_and_fetch("MATCH (n:N) WHERE n.type STARTS WITH $p RETURN n.type AS t", {"p": "al"}))
    assert [r["t"] for r in result] == ["alpha"]


# --- Edge string-op index tests ---


@pytest.fixture
def edge_graph(memgraph):
    memgraph.execute("CREATE EDGE INDEX ON :REL(kind);")
    memgraph.execute("FOREACH (x IN ['alpha', 'beta', 'gamma'] | CREATE ({id: x})-[:REL {kind: x}]->({id: x + '_t'}));")
    yield
    # teardown handled by setup_graph's drop_database


def test_edge_contains_uses_edge_type_property_range(memgraph, edge_graph):
    plan = get_plan(memgraph, "MATCH (a)-[r:REL]->(b) WHERE r.kind CONTAINS 'lph' RETURN r.kind AS k")
    ops = operator_names(plan)
    assert "ScanAllByEdgeTypePropertyRange" in ops, f"Expected ScanAllByEdgeTypePropertyRange, got: {plan}"
    result = list(memgraph.execute_and_fetch("MATCH (a)-[r:REL]->(b) WHERE r.kind CONTAINS 'lph' RETURN r.kind AS k"))
    assert [r["k"] for r in result] == ["alpha"]


def test_edge_starts_with_uses_edge_type_property_range(memgraph, edge_graph):
    plan = get_plan(memgraph, "MATCH (a)-[r:REL]->(b) WHERE r.kind STARTS WITH 'be' RETURN r.kind AS k")
    ops = operator_names(plan)
    assert "ScanAllByEdgeTypePropertyRange" in ops, f"Expected ScanAllByEdgeTypePropertyRange, got: {plan}"
    result = list(memgraph.execute_and_fetch("MATCH (a)-[r:REL]->(b) WHERE r.kind STARTS WITH 'be' RETURN r.kind AS k"))
    assert [r["k"] for r in result] == ["beta"]


def test_edge_ends_with_uses_edge_type_property_range(memgraph, edge_graph):
    plan = get_plan(memgraph, "MATCH (a)-[r:REL]->(b) WHERE r.kind ENDS WITH 'ma' RETURN r.kind AS k")
    ops = operator_names(plan)
    assert "ScanAllByEdgeTypePropertyRange" in ops, f"Expected ScanAllByEdgeTypePropertyRange, got: {plan}"
    result = list(memgraph.execute_and_fetch("MATCH (a)-[r:REL]->(b) WHERE r.kind ENDS WITH 'ma' RETURN r.kind AS k"))
    assert [r["k"] for r in result] == ["gamma"]


def test_edge_starts_with_null_returns_empty(memgraph, edge_graph):
    result = list(memgraph.execute_and_fetch("MATCH (a)-[r:REL]->(b) WHERE r.kind STARTS WITH null RETURN r.kind AS k"))
    assert result == []


# --- regression tests for defects found in review ---


@pytest.fixture
def cartesian_graph(memgraph):
    memgraph.execute("CREATE INDEX ON :CA(t);")
    memgraph.execute("CREATE INDEX ON :CB(t);")
    memgraph.execute("FOREACH (x IN ['alpha', 'beta', 'gamma'] | CREATE (:CA {t: x}));")
    memgraph.execute("FOREACH (x IN ['alpha', 'beta', 'gamma'] | CREATE (:CB {t: x}));")
    yield memgraph
    memgraph.execute("DROP INDEX ON :CA(t);")
    memgraph.execute("DROP INDEX ON :CB(t);")


def test_starts_with_across_cartesian_keeps_all_rows(memgraph, cartesian_graph):
    # A Cartesian pulls its right branch once per pass, so a seek keyed on the left branch is only
    # sound once the Cartesian has become an IndexedJoin. STARTS WITH keeps a post-filter, so the
    # conversion cannot be driven by expression removal alone.
    result = list(
        memgraph.execute_and_fetch("MATCH (a:CA), (b:CB) WHERE b.t STARTS WITH a.t RETURN b.t AS t ORDER BY t")
    )
    assert [r["t"] for r in result] == ["alpha", "beta", "gamma"]


def test_starts_with_across_two_matches_keeps_all_rows(memgraph, cartesian_graph):
    result = list(
        memgraph.execute_and_fetch("MATCH (a:CA) MATCH (b:CB) WHERE b.t STARTS WITH a.t RETURN b.t AS t ORDER BY t")
    )
    assert [r["t"] for r in result] == ["alpha", "beta", "gamma"]


@pytest.fixture
def global_index_graph(memgraph):
    memgraph.execute("CREATE (:GI {gp: 'hello'}), (:GI {gp: 'world'}), (:GI {gp: 'help'});")
    memgraph.execute("CREATE GLOBAL INDEX ON :(gp);")
    yield memgraph
    memgraph.execute("DROP GLOBAL INDEX ON :(gp);")


@pytest.mark.parametrize(
    "predicate,expected",
    [
        ("n.gp CONTAINS 'ell'", ["hello"]),
        ("n.gp ENDS WITH 'lo'", ["hello"]),
        ("n.gp STARTS WITH ''", ["hello", "help", "world"]),
        ("n.gp STARTS WITH 'hel'", ["hello", "help"]),
        ("n.gp =~ 'hel.*'", ["hello", "help"]),
    ],
)
def test_global_property_index_string_predicates(memgraph, global_index_graph, predicate, expected):
    # A whole-type-band range has a String lower bound and a List upper bound; the global property
    # index must accept that pair rather than treating it as an empty range.
    plan = get_plan(memgraph, f"MATCH (n:GI) WHERE {predicate} RETURN n.gp AS p")
    assert "ScanAllByVertexProperty" in operator_names(plan), f"Expected the global index, got: {plan}"
    result = list(memgraph.execute_and_fetch(f"MATCH (n:GI) WHERE {predicate} RETURN n.gp AS p ORDER BY p"))
    assert [r["p"] for r in result] == expected


def test_far_smaller_band_index_still_wins(memgraph):
    # A far smaller index wins on cardinality even though its filter is a band scan: 5 entries read
    # 5 rows where an equality seek on a 10000-entry index with two distinct values reads 5000.
    memgraph.execute("CREATE INDEX ON :BAND(lo);")
    memgraph.execute("CREATE INDEX ON :BAND(hi);")
    memgraph.execute(
        "FOREACH (i IN range(1, 10000) | CREATE (:BAND {lo: CASE WHEN i % 2 = 0 THEN 'A' ELSE 'B' END, "
        "hi: CASE WHEN i <= 5 THEN 'match' + toString(i) ELSE null END}));"
    )
    plan = get_plan(memgraph, "MATCH (n:BAND) WHERE n.lo = 'A' AND n.hi CONTAINS 'atch' RETURN n")
    assert any("{hi}" in line for line in plan), f"Expected the far smaller hi index to win, got: {plan}"
    memgraph.execute("DROP INDEX ON :BAND(lo);")
    memgraph.execute("DROP INDEX ON :BAND(hi);")


def test_cross_type_range_on_global_index_returns_nothing(memgraph):
    # `1 < ''` is null, so nothing qualifies. The global property index deletes the predicate rather
    # than keeping a post-filter, so an incomparable bound pair has to be rejected as an empty range
    # and must not be mistaken for the marker that spans a whole type.
    memgraph.execute("CREATE (:XT {xp: 1}), (:XT {xp: 2}), (:XT {xp: 'aa'});")
    without_index = list(memgraph.execute_and_fetch("MATCH (n:XT) WHERE n.xp >= 1 AND n.xp < '' RETURN n.xp AS v"))
    memgraph.execute("CREATE GLOBAL INDEX ON :(xp);")
    with_index = list(memgraph.execute_and_fetch("MATCH (n) WHERE n.xp >= 1 AND n.xp < '' RETURN n.xp AS v"))
    memgraph.execute("DROP GLOBAL INDEX ON :(xp);")
    assert without_index == []
    assert with_index == []


def test_cross_type_range_on_edge_index_returns_nothing(memgraph):
    # An edge scan carries raw bounds and has no invalid-range marking, so the whole-type marker must
    # not be admitted there: `-inf .. ''` is exactly that marker for numbers, but written by a user it
    # is an incomparable range and nothing qualifies.
    memgraph.execute("CREATE ()-[:EW {w: 1}]->();")
    memgraph.execute("CREATE ()-[:EW {w: 2.5}]->();")
    memgraph.execute("CREATE ()-[:EW {w: 'a'}]->();")
    without_index = list(
        memgraph.execute_and_fetch("MATCH ()-[e:EW]->() WHERE e.w >= -1.0/0.0 AND e.w < '' RETURN e.w AS v")
    )
    memgraph.execute("CREATE EDGE INDEX ON :EW(w);")
    with_index = list(
        memgraph.execute_and_fetch("MATCH ()-[e:EW]->() WHERE e.w >= -1.0/0.0 AND e.w < '' RETURN e.w AS v")
    )
    still_works = list(
        memgraph.execute_and_fetch("MATCH ()-[e:EW]->() WHERE e.w >= 0 AND e.w < 3 RETURN e.w AS v ORDER BY v")
    )
    memgraph.execute("DROP EDGE INDEX ON :EW(w);")
    assert without_index == []
    assert with_index == []
    assert [r["v"] for r in still_works] == [1, 2.5]


def test_correlated_edge_predicate_still_answers_with_an_edge_index(memgraph):
    # A correlated string predicate on an edge is not plannable as an edge-index seek: the source node
    # gets absorbed into the scan that its own seek key then reads, and inside OPTIONAL the plan is
    # refused. Both must fall back to an expansion rather than lose rows or fail to plan.
    memgraph.execute("CREATE (:LC {r: 'aa'})-[:EC {w: 'aax'}]->(:MC);")
    memgraph.execute("CREATE (:LC {r: 'bb'})-[:EC {w: 'bby'}]->(:MC);")
    memgraph.execute("CREATE (:DC {s: 'aa'});")
    absorbed = "MATCH (n:LC)-[e:EC]->() WHERE e.w STARTS WITH n.r RETURN e.w AS w ORDER BY w"
    optional = "MATCH (a:DC) OPTIONAL MATCH ()-[e:EC]->() WHERE e.w STARTS WITH a.s RETURN e.w AS w ORDER BY w"
    before_absorbed = [r["w"] for r in memgraph.execute_and_fetch(absorbed)]
    before_optional = [r["w"] for r in memgraph.execute_and_fetch(optional)]
    memgraph.execute("CREATE EDGE INDEX ON :EC(w);")
    after_absorbed = [r["w"] for r in memgraph.execute_and_fetch(absorbed)]
    after_optional = [r["w"] for r in memgraph.execute_and_fetch(optional)]
    memgraph.execute("DROP EDGE INDEX ON :EC(w);")
    assert before_absorbed == ["aax", "bby"]
    assert after_absorbed == before_absorbed
    assert after_optional == before_optional


def test_non_string_search_term_matches_nothing(memgraph):
    # A non-string search term compares to Null, so it matches nothing and never raises. Were it to
    # raise, an index would decide whether the query errored at all: it would raise on a label whose
    # rows hold a string and stay silent on one whose rows do not.
    memgraph.execute("CREATE INDEX ON :TS(p);")
    memgraph.execute("CREATE (:TX {n: 1});")
    memgraph.execute("CREATE (:TM {m: 1});")
    assert list(memgraph.execute_and_fetch("MATCH (n:TS) WHERE n.p STARTS WITH 5 RETURN n.p AS v")) == []
    union = "MATCH (n:TS) WHERE n.p STARTS WITH 5 RETURN 1 AS v UNION MATCH (m:TM) RETURN 2 AS v"
    assert [r["v"] for r in memgraph.execute_and_fetch(union)] == [2]
    optional = "MATCH (x:TX) OPTIONAL MATCH (n:TS) WHERE n.p STARTS WITH 5 RETURN x.n AS v"
    assert [r["v"] for r in memgraph.execute_and_fetch(optional)] == [1]
    # a row that does reach the predicate still matches nothing, and still does not raise
    memgraph.execute("CREATE (:TS {p: 'aa'});")
    assert list(memgraph.execute_and_fetch("MATCH (n:TS) WHERE n.p STARTS WITH 5 RETURN n.p AS v")) == []
    memgraph.execute("DROP INDEX ON :TS(p);")


def test_correlated_string_predicate_is_not_indexed(memgraph):
    # Indexing a correlated string predicate turns a Cartesian into an IndexedJoin that re-seeks the
    # index once per outer row. It stays a filter over a scan, as it was before the feature.
    memgraph.execute("CREATE INDEX ON :CS(t);")
    memgraph.execute("FOREACH (i IN range(1, 5) | CREATE (:CO {t: ''}));")
    memgraph.execute("FOREACH (i IN range(1, 5) | CREATE (:CS {t: 'v' + toString(i)}));")
    plan = get_plan(memgraph, "MATCH (a:CO), (b:CS) WHERE b.t STARTS WITH a.t RETURN b.t")
    ops = operator_names(plan)
    assert "ScanAllByLabelProperties" not in ops, f"A correlated prefix must not key an index seek: {plan}"
    assert "IndexedJoin" not in ops, f"Expected the Cartesian to stand: {plan}"
    rows = list(memgraph.execute_and_fetch("MATCH (a:CO), (b:CS) WHERE b.t STARTS WITH a.t RETURN count(*) AS c"))
    assert rows[0]["c"] == 25
    memgraph.execute("DROP INDEX ON :CS(t);")


def test_non_string_property_does_not_depend_on_index(memgraph):
    # The index excludes non-string values before any filter runs, so a non-string subject must not
    # raise -- otherwise the same query errors without an index and returns rows with one.
    memgraph.execute("CREATE (:MIX {v: 'alpha'}), (:MIX {v: 123}), (:MIX {v: true});")
    without_index = list(memgraph.execute_and_fetch("MATCH (n:MIX) WHERE n.v STARTS WITH 'al' RETURN n.v AS v"))
    memgraph.execute("CREATE INDEX ON :MIX(v);")
    with_index = list(memgraph.execute_and_fetch("MATCH (n:MIX) WHERE n.v STARTS WITH 'al' RETURN n.v AS v"))
    memgraph.execute("DROP INDEX ON :MIX(v);")
    assert [r["v"] for r in without_index] == ["alpha"]
    assert [r["v"] for r in with_index] == ["alpha"]


@pytest.mark.parametrize("op", ["STARTS WITH", "CONTAINS", "ENDS WITH"])
@pytest.mark.parametrize(
    "rows",
    [
        pytest.param("CREATE (:{L} {{other: 1}}), (:{L} {{other: 2}});", id="property-absent"),
        pytest.param("CREATE (:{L} {{v: 1}}), (:{L} {{v: 2}});", id="only-numbers"),
        pytest.param("CREATE (:{L} {{v: 'abc'}});", id="strings"),
    ],
)
def test_non_string_search_term_is_index_independent(memgraph, op, rows):
    # The search term is the same for every row, but which rows an index hands to the filter is not.
    # A non-string term therefore has to compare to Null rather than raise, or the presence of an
    # index changes the outcome: raising on a property holding a string, silent on one holding none.
    memgraph.execute(rows.format(L="NOIDX"))
    memgraph.execute(rows.format(L="IDX"))
    memgraph.execute("CREATE INDEX ON :IDX(v);")
    q = "MATCH (n:{L}) WHERE n.v {op} 5 RETURN count(n) AS c"
    without_index = list(memgraph.execute_and_fetch(q.format(L="NOIDX", op=op)))
    with_index = list(memgraph.execute_and_fetch(q.format(L="IDX", op=op)))
    memgraph.execute("DROP INDEX ON :IDX(v);")
    assert without_index[0]["c"] == 0
    assert with_index[0]["c"] == 0


@pytest.mark.parametrize(
    "predicate",
    [
        "n.a = 'k050' AND n.b = 50",
        "n.a >= 'k050' AND n.b = 50",
        "n.a STARTS WITH 'k05' AND n.b = 50",
        "n.a STARTS WITH 'k05' AND n.b >= 50",
        "n.a CONTAINS '05' AND n.b = 50",
        "n.a ENDS WITH '50' AND n.b = 50",
        "n.a STARTS WITH 'k05'",
    ],
)
def test_composite_index_agrees_with_an_unindexed_scan(memgraph, predicate):
    # A prefix range is a range, so it can key the leading slot of a composite index. Whatever the
    # combination of predicate kinds, the index may change the plan but never the rows.
    for label in ("KIDX", "KPLAIN"):
        memgraph.execute(
            f"FOREACH (i IN range(0,99) | FOREACH (j IN range(0,99) | "
            f"CREATE (:{label} {{a:'k'+right('00'+toString(i),3), b:j}})));"
        )
    memgraph.execute("CREATE INDEX ON :KIDX(a, b);")
    q = "MATCH (n:{L}) WHERE {p} RETURN n.a AS a, n.b AS b ORDER BY a, b"
    indexed = [(r["a"], r["b"]) for r in memgraph.execute_and_fetch(q.format(L="KIDX", p=predicate))]
    plain = [(r["a"], r["b"]) for r in memgraph.execute_and_fetch(q.format(L="KPLAIN", p=predicate))]
    memgraph.execute("DROP INDEX ON :KIDX(a, b);")
    assert indexed == plain
    assert indexed


UNICODE_VALUES = [
    "a",
    "é",
    "éz",
    "ÿ",
    "ÿz",
    "ÿÿ",
    "߿",
    "߿z",
    "ࠀ",
    "ࠀz",
    "日",
    "日本",
    "日本語",
    "🎈",
    "🎉",
    "🎉x",
]


@pytest.mark.parametrize("prefix", ["a", "é", "ÿ", "߿", "ࠀ", "日", "日本", "🎉", ""])
def test_unicode_prefix_bounds_match_an_unindexed_scan(memgraph, prefix):
    # The prefix upper bound is the prefix with its last BYTE incremented, which is only sound
    # because the index orders strings bytewise -- for UTF-8 that is also codepoint order. These
    # prefixes sit on the boundaries where that matters: a trailing 0xBF carrying into the next lead
    # byte, codepoints either side of a change in encoded length, and four-byte characters.
    for value in UNICODE_VALUES:
        memgraph.execute("CREATE (:UIDX {s: $s}), (:UPLAIN {s: $s});", {"s": value})
    memgraph.execute("CREATE INDEX ON :UIDX(s);")
    q = "MATCH (n:{L}) WHERE n.s STARTS WITH $p RETURN n.s AS s ORDER BY s"
    indexed = [r["s"] for r in memgraph.execute_and_fetch(q.format(L="UIDX"), {"p": prefix})]
    plain = [r["s"] for r in memgraph.execute_and_fetch(q.format(L="UPLAIN"), {"p": prefix})]
    memgraph.execute("DROP INDEX ON :UIDX(s);")
    assert indexed == plain
    assert all(s.startswith(prefix) for s in indexed)
    assert sorted(indexed) == sorted(v for v in UNICODE_VALUES if v.startswith(prefix))


@pytest.mark.parametrize("op", ["STARTS WITH", "CONTAINS", "ENDS WITH"])
def test_non_string_subject_compares_to_null(memgraph, op):
    memgraph.execute("CREATE (:SUBJ {v: 1}), (:SUBJ {v: true}), (:SUBJ {v: [1, 2]}), (:SUBJ {v: 'abc'});")
    q = f"MATCH (n:SUBJ) WHERE n.v {op} 'zzz' RETURN count(n) AS c"
    assert list(memgraph.execute_and_fetch(q))[0]["c"] == 0


@pytest.mark.parametrize(
    "predicate,expected",
    [
        ("n.type CONTAINS 'lph'", ["alpha"]),
        ("n.type ENDS WITH 'ha'", ["alpha"]),
        ("n.type =~ 'al.*'", ["alpha"]),
    ],
)
def test_is_not_null_upgraded_to_string_predicate(memgraph, predicate, expected):
    """When IS NOT NULL and a string predicate target the same indexed property,
    the planner upgrades to the string predicate so the skip-scan ValuePredicate
    reaches the storage layer."""
    query = f"MATCH (n:N) WHERE n.type IS NOT NULL AND {predicate} RETURN n.type AS t ORDER BY t"
    plan = get_plan(memgraph, query)
    ops = operator_names(plan)
    assert "ScanAllByLabelProperties" in ops, f"Expected index scan, got: {plan}"
    result = list(memgraph.execute_and_fetch(query))
    assert [r["t"] for r in result] == expected


@pytest.mark.parametrize(
    "predicate",
    [
        "n.a = 'k000' AND n.b CONTAINS '5'",
        "n.a = 'k000' AND n.b ENDS WITH '0'",
        "n.a = 'k000' AND n.b =~ '.*5'",
    ],
)
def test_composite_index_non_leading_string_predicate(memgraph, predicate):
    for label in ("CIDX", "CPLAIN"):
        memgraph.execute(
            f"FOREACH (i IN range(0,99) | FOREACH (j IN range(0,99) | "
            f"CREATE (:{label} {{a:'k'+right('00'+toString(i),3), b:'v'+right('00'+toString(j),3)}})));"
        )
    memgraph.execute("CREATE INDEX ON :CIDX(a, b);")
    q = "MATCH (n:{L}) WHERE {p} RETURN n.a AS a, n.b AS b ORDER BY a, b"
    indexed = [(r["a"], r["b"]) for r in memgraph.execute_and_fetch(q.format(L="CIDX", p=predicate))]
    plain = [(r["a"], r["b"]) for r in memgraph.execute_and_fetch(q.format(L="CPLAIN", p=predicate))]
    memgraph.execute("DROP INDEX ON :CIDX(a, b);")
    assert indexed == plain
    assert indexed


@pytest.fixture
def duplicate_leading_graph(memgraph):
    memgraph.execute("CREATE INDEX ON :DUP(type);")
    memgraph.execute(
        "FOREACH (i IN range(1, 100) | "
        "FOREACH (t IN ['alpha', 'beta', 'gamma'] | "
        "CREATE (:DUP {type: t, seq: i})));"
    )
    yield memgraph
    memgraph.execute("DROP INDEX ON :DUP(type);")


@pytest.mark.parametrize(
    "predicate,expected_type",
    [
        ("n.type CONTAINS 'lph'", "alpha"),
        ("n.type ENDS WITH 'ta'", "beta"),
        ("n.type =~ 'gam.*'", "gamma"),
    ],
)
def test_skip_scan_with_duplicate_leading_values(memgraph, duplicate_leading_graph, predicate, expected_type):
    q = f"MATCH (n:DUP) WHERE {predicate} RETURN DISTINCT n.type AS t"
    result = [r["t"] for r in memgraph.execute_and_fetch(q)]
    assert result == [expected_type]
    q_count = f"MATCH (n:DUP) WHERE {predicate} RETURN count(n) AS c"
    assert list(memgraph.execute_and_fetch(q_count))[0]["c"] == 100


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
