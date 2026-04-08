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


def get_plan(memgraph, query):
    results = list(memgraph.execute_and_fetch(f"EXPLAIN {query}"))
    return [x[QUERY_PLAN] for x in results]


# ---------------------------------------------------------------------------
# Plan smoke tests -- verify the optimization fires (or doesn't) in key cases.
# ---------------------------------------------------------------------------


def test_plan_basic_elimination(memgraph):
    """ORDER BY n.prop eliminated when index scan provides ascending order."""
    memgraph.execute("CREATE INDEX ON :L(prop);")

    expected = [
        " * Produce {n}",
        " * ScanAllByLabelProperties (n :L {prop})",
        " * Once",
    ]

    actual = get_plan(memgraph, "MATCH (n:L) WHERE n.prop > 5 RETURN n ORDER BY n.prop")
    assert expected == actual


def test_plan_desc_not_eliminated(memgraph):
    """ORDER BY DESC not eliminated (index is ASC only)."""
    memgraph.execute("CREATE INDEX ON :L(prop);")

    plan = get_plan(memgraph, "MATCH (n:L) WHERE n.prop > 5 RETURN n ORDER BY n.prop DESC")
    assert any("OrderBy" in step for step in plan), "OrderBy should NOT be eliminated (DESC)"


def test_plan_with_renaming_allows_elimination(memgraph):
    """WITH renaming (n AS m) allows elimination -- rename is tracked through Produce."""
    memgraph.execute("CREATE INDEX ON :L(prop);")

    expected = [
        " * Produce {m}",
        " * Produce {m}",
        " * ScanAllByLabelProperties (n :L {prop})",
        " * Once",
    ]

    actual = get_plan(memgraph, "MATCH (n:L) WHERE n.prop > 5 WITH n AS m RETURN m ORDER BY m.prop")
    assert expected == actual


def test_plan_equality_skip_elimination(memgraph):
    """WHERE a = 5 ORDER BY b eliminated when index is (a, b) -- equality-pinned skip."""
    memgraph.execute("CREATE INDEX ON :L(a, b);")

    expected = [
        " * Produce {n}",
        " * ScanAllByLabelProperties (n :L {a, b})",
        " * Once",
    ]

    actual = get_plan(memgraph, "MATCH (n:L) WHERE n.a = 5 RETURN n ORDER BY n.b")
    assert expected == actual


def test_plan_reverse_column_order_not_eliminated(memgraph):
    """ORDER BY n.b, n.a not eliminated when index is (a, b)."""
    memgraph.execute("CREATE INDEX ON :L(a, b);")

    plan = get_plan(memgraph, "MATCH (n:L) WHERE n.a > 5 RETURN n ORDER BY n.b, n.a")
    assert any("OrderBy" in step for step in plan), "OrderBy should NOT be eliminated (wrong column order)"


# ---------------------------------------------------------------------------
# Correctness tests -- insert real data and verify result ordering is correct
# after ORDER BY elimination.
# ---------------------------------------------------------------------------


def test_correctness_basic_ascending(memgraph):
    """Results are correctly ordered ascending after OrderBy elimination."""
    memgraph.execute("CREATE INDEX ON :L(prop);")
    memgraph.execute("UNWIND [30, 10, 50, 20, 40] AS v CREATE (:L {prop: v})")

    results = list(memgraph.execute_and_fetch("MATCH (n:L) WHERE n.prop > 5 RETURN n ORDER BY n.prop"))
    values = [r["n"]._properties["prop"] for r in results]
    assert values == [10, 20, 30, 40, 50]


def test_correctness_with_limit(memgraph):
    """ORDER BY eliminated with LIMIT still returns first N sorted results."""
    memgraph.execute("CREATE INDEX ON :L(prop);")
    memgraph.execute("UNWIND [30, 10, 50, 20, 40] AS v CREATE (:L {prop: v})")

    results = list(memgraph.execute_and_fetch("MATCH (n:L) WHERE n.prop > 5 RETURN n ORDER BY n.prop LIMIT 3"))
    values = [r["n"]._properties["prop"] for r in results]
    assert values == [10, 20, 30]


def test_correctness_equality_skip(memgraph):
    """Equality on first column, ORDER BY second -- elimination fires via equality-pinned skip."""
    memgraph.execute("CREATE INDEX ON :L(a, b);")
    memgraph.execute("UNWIND [3, 1, 4, 1, 5] AS b CREATE (:L {a: 10, b: b})")
    memgraph.execute("CREATE (:L {a: 20, b: 0})")

    results = list(memgraph.execute_and_fetch("MATCH (n:L) WHERE n.a = 10 RETURN n ORDER BY n.b"))
    values = [r["n"]._properties["b"] for r in results]
    assert values == [1, 1, 3, 4, 5]


def test_correctness_composite_order(memgraph):
    """ORDER BY n.a, n.b with composite index (a, b) -- lexicographic order."""
    memgraph.execute("CREATE INDEX ON :L(a, b);")
    memgraph.execute(
        "UNWIND [{a: 2, b: 3}, {a: 1, b: 2}, {a: 2, b: 1}, {a: 1, b: 1}, {a: 3, b: 1}] AS d "
        "CREATE (:L {a: d.a, b: d.b})"
    )

    results = list(memgraph.execute_and_fetch("MATCH (n:L) WHERE n.a > 0 RETURN n ORDER BY n.a, n.b"))
    pairs = [(r["n"]._properties["a"], r["n"]._properties["b"]) for r in results]
    assert pairs == [(1, 1), (1, 2), (2, 1), (2, 3), (3, 1)]


def test_correctness_with_expand(memgraph):
    """ORDER BY on scan symbol preserved through Expand."""
    memgraph.execute("CREATE INDEX ON :L(prop);")
    memgraph.execute("CREATE (:L {prop: 30})-[:R]->(:M)")
    memgraph.execute("CREATE (:L {prop: 10})-[:R]->(:M)")
    memgraph.execute("CREATE (:L {prop: 20})-[:R]->(:M)")

    results = list(memgraph.execute_and_fetch("MATCH (n:L)-[r]->(m) WHERE n.prop > 5 RETURN n, m ORDER BY n.prop"))
    values = [r["n"]._properties["prop"] for r in results]
    assert values == [10, 20, 30]


def test_correctness_equality_plus_range(memgraph):
    """WHERE a = val AND b > val ORDER BY b -- elimination fires, correct order via equality-pinned skip."""
    memgraph.execute("CREATE INDEX ON :L(a, b);")
    memgraph.execute("UNWIND [50, 20, 40, 10, 30] AS b CREATE (:L {a: 1, b: b})")
    memgraph.execute("CREATE (:L {a: 2, b: 5})")

    results = list(memgraph.execute_and_fetch("MATCH (n:L) WHERE n.a = 1 AND n.b > 15 RETURN n ORDER BY n.b"))
    values = [r["n"]._properties["b"] for r in results]
    assert values == [20, 30, 40, 50]


def test_correctness_with_rename(memgraph):
    """Results correctly ordered after WITH rename and OrderBy elimination."""
    memgraph.execute("CREATE INDEX ON :L(prop);")
    memgraph.execute("UNWIND [30, 10, 50, 20, 40] AS v CREATE (:L {prop: v})")

    results = list(memgraph.execute_and_fetch("MATCH (n:L) WHERE n.prop > 5 WITH n AS m RETURN m ORDER BY m.prop"))
    values = [r["m"]._properties["prop"] for r in results]
    assert values == [10, 20, 30, 40, 50]


def test_correctness_return_rename_input_scope(memgraph):
    """RETURN n AS m ORDER BY n.prop -- ORDER BY uses input scope, results still correct."""
    memgraph.execute("CREATE INDEX ON :L(prop);")
    memgraph.execute("UNWIND [30, 10, 50, 20, 40] AS v CREATE (:L {prop: v})")

    results = list(memgraph.execute_and_fetch("MATCH (n:L) WHERE n.prop > 5 RETURN n AS m ORDER BY n.prop"))
    values = [r["m"]._properties["prop"] for r in results]
    assert values == [10, 20, 30, 40, 50]


def test_plan_in_filter_not_eliminated(memgraph):
    """ORDER BY not eliminated when IN filter drives the scan (multi-value, not globally sorted)."""
    memgraph.execute("CREATE INDEX ON :L(a, b);")

    plan = get_plan(memgraph, "MATCH (n:L) WHERE n.a IN [3, 1] RETURN n ORDER BY n.b")
    assert any("OrderBy" in step for step in plan), "OrderBy should NOT be eliminated (IN is multi-valued)"


def test_correctness_in_filter_order_preserved(memgraph):
    """IN filter with ORDER BY -- OrderBy must remain to guarantee correct ordering."""
    memgraph.execute("CREATE INDEX ON :L(a, b);")
    memgraph.execute(
        "UNWIND [{a: 1, b: 30}, {a: 1, b: 10}, {a: 3, b: 20}, {a: 3, b: 5}, {a: 2, b: 15}] AS d "
        "CREATE (:L {a: d.a, b: d.b})"
    )

    results = list(memgraph.execute_and_fetch("MATCH (n:L) WHERE n.a IN [3, 1] RETURN n ORDER BY n.b"))
    values = [r["n"]._properties["b"] for r in results]
    assert values == [5, 10, 20, 30]


# ---------------------------------------------------------------------------
# Alias resolution tests -- ORDER BY on projected aliases (WITH n.prop AS a)
# ---------------------------------------------------------------------------


def test_plan_with_property_alias_elimination(memgraph):
    """ORDER BY a eliminated when WITH n.prop AS a projects from indexed property."""
    memgraph.execute("CREATE INDEX ON :L(prop);")

    plan = get_plan(memgraph, "MATCH (n:L) WHERE n.prop > 5 WITH n.prop AS a RETURN a ORDER BY a")
    assert not any("OrderBy" in step for step in plan), "OrderBy should be eliminated (alias resolved through Produce)"


def test_plan_return_property_alias_elimination(memgraph):
    """ORDER BY a eliminated when RETURN n.prop AS a defines the alias."""
    memgraph.execute("CREATE INDEX ON :L(prop);")

    plan = get_plan(memgraph, "MATCH (n:L) WHERE n.prop > 5 RETURN n.prop AS a ORDER BY a")
    assert not any("OrderBy" in step for step in plan), "OrderBy should be eliminated (RETURN alias resolved)"


def test_plan_composite_alias_elimination(memgraph):
    """ORDER BY a, b eliminated when WITH projects both from composite index (a, b)."""
    memgraph.execute("CREATE INDEX ON :L(a, b);")

    plan = get_plan(memgraph, "MATCH (n:L) WHERE n.a > 0 WITH n.a AS a, n.b AS b RETURN a, b ORDER BY a, b")
    assert not any("OrderBy" in step for step in plan), "OrderBy should be eliminated (composite alias resolved)"


def test_plan_composite_alias_wrong_order_not_eliminated(memgraph):
    """ORDER BY b, a not eliminated when index is (a, b) -- alias order matters."""
    memgraph.execute("CREATE INDEX ON :L(a, b);")

    plan = get_plan(memgraph, "MATCH (n:L) WHERE n.a > 0 WITH n.b AS b, n.a AS a RETURN b, a ORDER BY b, a")
    assert any("OrderBy" in step for step in plan), "OrderBy should NOT be eliminated (wrong order)"


def test_correctness_with_property_alias(memgraph):
    """Results correctly ordered after WITH property alias and OrderBy elimination."""
    memgraph.execute("CREATE INDEX ON :L(prop);")
    memgraph.execute("UNWIND [30, 10, 50, 20, 40] AS v CREATE (:L {prop: v})")

    results = list(memgraph.execute_and_fetch("MATCH (n:L) WHERE n.prop > 5 WITH n.prop AS a RETURN a ORDER BY a"))
    values = [r["a"] for r in results]
    assert values == [10, 20, 30, 40, 50]


def test_correctness_composite_alias(memgraph):
    """Composite index alias projection -- lexicographic order preserved."""
    memgraph.execute("CREATE INDEX ON :L(a, b);")
    memgraph.execute(
        "UNWIND [{a: 2, b: 3}, {a: 1, b: 2}, {a: 2, b: 1}, {a: 1, b: 1}, {a: 3, b: 1}] AS d "
        "CREATE (:L {a: d.a, b: d.b})"
    )

    results = list(
        memgraph.execute_and_fetch("MATCH (n:L) WHERE n.a > 0 WITH n.a AS x, n.b AS y RETURN x, y ORDER BY x, y")
    )
    pairs = [(r["x"], r["y"]) for r in results]
    assert pairs == [(1, 1), (1, 2), (2, 1), (2, 3), (3, 1)]


def test_plan_distinct_alias_elimination(memgraph):
    """RETURN DISTINCT n.prop AS a ORDER BY a -- Distinct between Produce and OrderBy, alias resolved."""
    memgraph.execute("CREATE INDEX ON :L(prop);")

    plan = get_plan(memgraph, "MATCH (n:L) WHERE n.prop > 5 RETURN DISTINCT n.prop AS a ORDER BY a")
    assert not any("OrderBy" in step for step in plan), "OrderBy should be eliminated (DISTINCT + alias)"


def test_correctness_distinct_alias(memgraph):
    """RETURN DISTINCT with alias -- correct order after elimination."""
    memgraph.execute("CREATE INDEX ON :L(prop);")
    memgraph.execute("UNWIND [30, 10, 50, 20, 40, 30, 10] AS v CREATE (:L {prop: v})")

    results = list(memgraph.execute_and_fetch("MATCH (n:L) WHERE n.prop > 5 RETURN DISTINCT n.prop AS a ORDER BY a"))
    values = [r["a"] for r in results]
    assert values == [10, 20, 30, 40, 50]


def test_plan_with_orderby_return(memgraph):
    """WITH ... ORDER BY ... RETURN -- OrderBy eliminated, Produce(RETURN) above OrderBy(WITH)."""
    memgraph.execute("CREATE INDEX ON :L(prop);")

    expected = [
        " * Produce {p}",
        " * Produce {n}",
        " * ScanAllByLabelProperties (n :L {prop})",
        " * Once",
    ]

    actual = get_plan(memgraph, "MATCH (n:L) WHERE n.prop > 5 WITH n ORDER BY n.prop RETURN n.prop AS p")
    assert expected == actual


def test_correctness_with_orderby_return(memgraph):
    """WITH ... ORDER BY ... RETURN -- correct order after elimination."""
    memgraph.execute("CREATE INDEX ON :L(prop);")
    memgraph.execute("UNWIND [30, 10, 50, 20, 40] AS v CREATE (:L {prop: v})")

    results = list(memgraph.execute_and_fetch("MATCH (n:L) WHERE n.prop > 5 WITH n ORDER BY n.prop RETURN n.prop AS p"))
    values = [r["p"] for r in results]
    assert values == [10, 20, 30, 40, 50]


def test_plan_with_distinct_orderby_return(memgraph):
    """WITH DISTINCT ... ORDER BY ... RETURN -- OrderBy eliminated, Distinct kept."""
    memgraph.execute("CREATE INDEX ON :L(prop);")

    expected = [
        " * Produce {p}",
        " * Distinct",
        " * Produce {n}",
        " * ScanAllByLabelProperties (n :L {prop})",
        " * Once",
    ]

    actual = get_plan(memgraph, "MATCH (n:L) WHERE n.prop > 5 WITH DISTINCT n ORDER BY n.prop RETURN n.prop AS p")
    assert expected == actual


def test_correctness_with_distinct_orderby_return(memgraph):
    """WITH DISTINCT prop ORDER BY prop RETURN -- correct order and dedup after elimination."""
    memgraph.execute("CREATE INDEX ON :L(prop);")
    memgraph.execute("UNWIND [50, 40, 30, 20, 10, 50, 40, 30, 20, 10] AS v CREATE (:L {prop: v})")

    results = list(
        memgraph.execute_and_fetch("MATCH (n:L) WHERE n.prop > 5 WITH DISTINCT n.prop AS p ORDER BY p RETURN p")
    )
    values = [r["p"] for r in results]
    assert values == [10, 20, 30, 40, 50]


def test_correctness_equality_pinned_alias(memgraph):
    """Equality-pinned skip works through alias projection."""
    memgraph.execute("CREATE INDEX ON :L(a, b);")
    memgraph.execute("UNWIND [50, 20, 40, 10, 30] AS b CREATE (:L {a: 1, b: b})")
    memgraph.execute("CREATE (:L {a: 2, b: 5})")

    results = list(memgraph.execute_and_fetch("MATCH (n:L) WHERE n.a = 1 WITH n.b AS b RETURN b ORDER BY b"))
    values = [r["b"] for r in results]
    assert values == [10, 20, 30, 40, 50]


# ---------------------------------------------------------------------------
# DESC index tests — ORDER BY DESC elimination with DESC index
# ---------------------------------------------------------------------------


def test_plan_desc_index_desc_order_eliminated(memgraph):
    """ORDER BY n.prop DESC eliminated when DESC index exists."""
    memgraph.execute('CREATE INDEX ON :L(prop) WITH CONFIG {"order": "DESC"};')

    expected = [
        " * Produce {n}",
        " * ScanAllByLabelProperties (n :L {prop})",
        " * Once",
    ]

    actual = get_plan(memgraph, "MATCH (n:L) WHERE n.prop > 5 RETURN n ORDER BY n.prop DESC")
    assert expected == actual


def test_plan_desc_index_asc_order_not_eliminated(memgraph):
    """ORDER BY ASC not eliminated when only DESC index exists."""
    memgraph.execute('CREATE INDEX ON :L(prop) WITH CONFIG {"order": "DESC"};')

    plan = get_plan(memgraph, "MATCH (n:L) WHERE n.prop > 5 RETURN n ORDER BY n.prop")
    assert any("OrderBy" in step for step in plan), "OrderBy ASC should NOT be eliminated with DESC index"


def test_plan_asc_index_desc_order_not_eliminated(memgraph):
    """ORDER BY DESC not eliminated when only ASC index exists."""
    memgraph.execute("CREATE INDEX ON :L(prop);")

    plan = get_plan(memgraph, "MATCH (n:L) WHERE n.prop > 5 RETURN n ORDER BY n.prop DESC")
    assert any("OrderBy" in step for step in plan), "OrderBy DESC should NOT be eliminated with ASC index"


def test_correctness_desc_index_descending_order(memgraph):
    """Results correctly ordered descending with DESC index and ORDER BY DESC."""
    memgraph.execute('CREATE INDEX ON :L(prop) WITH CONFIG {"order": "DESC"};')
    for v in [30, 10, 50, 20, 40]:
        memgraph.execute(f"CREATE (:L {{prop: {v}}})")

    results = list(memgraph.execute_and_fetch("MATCH (n:L) WHERE n.prop > 5 RETURN n ORDER BY n.prop DESC"))
    values = [r["n"]._properties["prop"] for r in results]
    assert values == [50, 40, 30, 20, 10]


def test_correctness_desc_index_with_limit(memgraph):
    """DESC index + ORDER BY DESC + LIMIT returns top-N in descending order."""
    memgraph.execute('CREATE INDEX ON :L(prop) WITH CONFIG {"order": "DESC"};')
    for v in [30, 10, 50, 20, 40]:
        memgraph.execute(f"CREATE (:L {{prop: {v}}})")

    results = list(memgraph.execute_and_fetch("MATCH (n:L) WHERE n.prop > 5 RETURN n ORDER BY n.prop DESC LIMIT 3"))
    values = [r["n"]._properties["prop"] for r in results]
    assert values == [50, 40, 30]


def test_correctness_desc_composite_order(memgraph):
    """DESC composite index (a, b) + ORDER BY a DESC, b DESC — reverse lexicographic."""
    memgraph.execute('CREATE INDEX ON :L(a, b) WITH CONFIG {"order": "DESC"};')
    data = [(2, 3), (1, 2), (2, 1), (1, 1), (3, 1)]
    for a, b in data:
        memgraph.execute(f"CREATE (:L {{a: {a}, b: {b}}})")

    results = list(memgraph.execute_and_fetch("MATCH (n:L) WHERE n.a > 0 RETURN n ORDER BY n.a DESC, n.b DESC"))
    pairs = [(r["n"]._properties["a"], r["n"]._properties["b"]) for r in results]
    assert pairs == [(3, 1), (2, 3), (2, 1), (1, 2), (1, 1)]


def test_correctness_desc_equality_pinned(memgraph):
    """Equality on first column + ORDER BY second DESC — elimination with DESC index."""
    memgraph.execute('CREATE INDEX ON :L(a, b) WITH CONFIG {"order": "DESC"};')
    for b in [50, 20, 40, 10, 30]:
        memgraph.execute(f"CREATE (:L {{a: 1, b: {b}}})")
    memgraph.execute("CREATE (:L {a: 2, b: 5})")

    results = list(memgraph.execute_and_fetch("MATCH (n:L) WHERE n.a = 1 RETURN n ORDER BY n.b DESC"))
    values = [r["n"]._properties["b"] for r in results]
    assert values == [50, 40, 30, 20, 10]


def test_plan_mixed_order_not_eliminated(memgraph):
    """Mixed ASC/DESC in ORDER BY not eliminated even with DESC index."""
    memgraph.execute('CREATE INDEX ON :L(a, b) WITH CONFIG {"order": "DESC"};')

    plan = get_plan(memgraph, "MATCH (n:L) WHERE n.a > 0 RETURN n ORDER BY n.a ASC, n.b DESC")
    assert any("OrderBy" in step for step in plan), "Mixed ASC/DESC ORDER BY should NOT be eliminated"


def test_desc_index_used_for_filter_scan(memgraph):
    """DESC index can still be used for filter scans (range queries work correctly)."""
    memgraph.execute('CREATE INDEX ON :L(prop) WITH CONFIG {"order": "DESC"};')
    for v in [10, 20, 30, 40, 50]:
        memgraph.execute(f"CREATE (:L {{prop: {v}}})")

    results = list(memgraph.execute_and_fetch("MATCH (n:L) WHERE n.prop > 20 AND n.prop < 50 RETURN n.prop AS val"))
    values = sorted([r["val"] for r in results])
    assert values == [30, 40]


def test_both_asc_and_desc_index_coexist(memgraph):
    """Both ASC and DESC indices on same property — each eliminates its matching ORDER BY."""
    memgraph.execute("CREATE INDEX ON :L(prop);")
    memgraph.execute('CREATE INDEX ON :L(prop) WITH CONFIG {"order": "DESC"};')
    for v in [30, 10, 50, 20, 40]:
        memgraph.execute(f"CREATE (:L {{prop: {v}}})")

    # ASC order — should be eliminated
    plan_asc = get_plan(memgraph, "MATCH (n:L) WHERE n.prop > 5 RETURN n ORDER BY n.prop")
    assert not any("OrderBy" in step for step in plan_asc), "OrderBy ASC should be eliminated with ASC index"

    results_asc = list(memgraph.execute_and_fetch("MATCH (n:L) WHERE n.prop > 5 RETURN n ORDER BY n.prop"))
    values_asc = [r["n"]._properties["prop"] for r in results_asc]
    assert values_asc == [10, 20, 30, 40, 50]

    # DESC order — should be eliminated
    plan_desc = get_plan(memgraph, "MATCH (n:L) WHERE n.prop > 5 RETURN n ORDER BY n.prop DESC")
    assert not any("OrderBy" in step for step in plan_desc), "OrderBy DESC should be eliminated with DESC index"

    results_desc = list(memgraph.execute_and_fetch("MATCH (n:L) WHERE n.prop > 5 RETURN n ORDER BY n.prop DESC"))
    values_desc = [r["n"]._properties["prop"] for r in results_desc]
    assert values_desc == [50, 40, 30, 20, 10]


def test_plan_triple_scan_all_eliminated(memgraph):
    """ORDER BY c.id, b.id, a.id eliminated -- three nested scans in outermost-first order."""
    memgraph.execute("SET DATABASE SETTING 'cartesian-product-enabled' TO 'false';")
    try:
        memgraph.execute("CREATE INDEX ON :I(id);")
        memgraph.execute("CREATE INDEX ON :J(id);")
        memgraph.execute("CREATE INDEX ON :K(id);")

        expected = [
            " * Produce {c, b, a}",
            " * ScanAllByLabelProperties (a :I {id})",
            " * ScanAllByLabelProperties (b :J {id})",
            " * ScanAllByLabelProperties (c :K {id})",
            " * Once",
        ]

        actual = get_plan(
            memgraph,
            "MATCH (c:K), (b:J), (a:I) WHERE c.id > 0 AND b.id > 0 AND a.id > 0 "
            "RETURN c, b, a ORDER BY c.id, b.id, a.id",
        )
        assert expected == actual
    finally:
        memgraph.execute("SET DATABASE SETTING 'cartesian-product-enabled' TO 'true';")


def test_plan_triple_scan_outermost_only_eliminated(memgraph):
    """ORDER BY only on outermost scan -- eliminated even with two inner scans."""
    memgraph.execute("SET DATABASE SETTING 'cartesian-product-enabled' TO 'false';")
    try:
        memgraph.execute("CREATE INDEX ON :L2(id);")
        memgraph.execute("CREATE INDEX ON :M2(id);")
        memgraph.execute("CREATE INDEX ON :N2(id);")

        expected = [
            " * Produce {c, b, a}",
            " * ScanAllByLabelProperties (a :L2 {id})",
            " * ScanAllByLabelProperties (b :M2 {id})",
            " * ScanAllByLabelProperties (c :N2 {id})",
            " * Once",
        ]

        actual = get_plan(
            memgraph,
            "MATCH (c:N2), (b:M2), (a:L2) WHERE c.id > 0 AND b.id > 0 AND a.id > 0 RETURN c, b, a ORDER BY c.id",
        )
        assert expected == actual
    finally:
        memgraph.execute("SET DATABASE SETTING 'cartesian-product-enabled' TO 'true';")


def test_plan_triple_scan_reordered_to_eliminate(memgraph):
    """ORDER BY a.id, c.id, b.id -- planner reorders scans to match ORDER BY and eliminates it."""
    memgraph.execute("SET DATABASE SETTING 'cartesian-product-enabled' TO 'false';")
    try:
        memgraph.execute("CREATE INDEX ON :O(id);")
        memgraph.execute("CREATE INDEX ON :P(id);")
        memgraph.execute("CREATE INDEX ON :Q(id);")

        expected = [
            " * Produce {a, c, b}",
            " * ScanAllByLabelProperties (b :P {id})",
            " * ScanAllByLabelProperties (c :Q {id})",
            " * ScanAllByLabelProperties (a :O {id})",
            " * Once",
        ]

        actual = get_plan(
            memgraph,
            "MATCH (c:Q), (b:P), (a:O) WHERE c.id > 0 AND b.id > 0 AND a.id > 0 "
            "RETURN a, c, b ORDER BY a.id, c.id, b.id",
        )
        assert expected == actual
    finally:
        memgraph.execute("SET DATABASE SETTING 'cartesian-product-enabled' TO 'true';")


def test_plan_variable_start_prefers_elimination(memgraph):
    """Variable-start planner should pick the plan that eliminates OrderBy.

    Two candidate plans exist (start from n vs start from m). Starting from m
    lets ScanAllByLabelProperties provide the ORDER BY m.id order, eliminating
    the OrderBy operator.
    """
    memgraph.execute("CREATE INDEX ON :X(id);")

    plan = get_plan(memgraph, "MATCH (n:X)-[r]->(m:X) WHERE n.id > 0 AND m.id > 0 RETURN n, m ORDER BY m.id")
    assert not any(
        "OrderBy" in step for step in plan
    ), "OrderBy should be eliminated -- planner should start from m so the index provides order"


def test_correctness_variable_start_elimination(memgraph):
    """Results correct when variable-start planner eliminates OrderBy via starting vertex choice."""
    memgraph.execute("CREATE INDEX ON :X(id);")
    memgraph.execute("CREATE (:X {id: 30})-[:R]->(:X {id: 3})")
    memgraph.execute("CREATE (:X {id: 10})-[:R]->(:X {id: 1})")
    memgraph.execute("CREATE (:X {id: 20})-[:R]->(:X {id: 2})")

    results = list(
        memgraph.execute_and_fetch("MATCH (n:X)-[r]->(m:X) WHERE n.id > 0 AND m.id > 0 RETURN n, m ORDER BY m.id")
    )
    values = [r["m"]._properties["id"] for r in results]
    assert values == [1, 2, 3]


# ---------------------------------------------------------------------------
# Parallel execution -- ORDER BY must not be eliminated
# ---------------------------------------------------------------------------


def test_plan_parallel_execution_no_elimination(memgraph):
    """ORDER BY not eliminated under USING PARALLEL EXECUTION -- index order not preserved."""
    memgraph.execute("CREATE INDEX ON :L(prop);")

    plan = get_plan(memgraph, "USING PARALLEL EXECUTION MATCH (n:L) WHERE n.prop > 5 RETURN n ORDER BY n.prop")
    assert any("OrderBy" in step for step in plan), "OrderBy should NOT be eliminated under parallel execution"


def test_correctness_parallel_execution_ordering(memgraph):
    """Results correctly ordered under parallel execution despite no elimination."""
    memgraph.execute("CREATE INDEX ON :L(prop);")
    memgraph.execute("UNWIND [30, 10, 50, 20, 40] AS v CREATE (:L {prop: v})")

    results = list(
        memgraph.execute_and_fetch("USING PARALLEL EXECUTION MATCH (n:L) WHERE n.prop > 5 RETURN n ORDER BY n.prop")
    )
    values = [r["n"]._properties["prop"] for r in results]
    assert values == [10, 20, 30, 40, 50]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
