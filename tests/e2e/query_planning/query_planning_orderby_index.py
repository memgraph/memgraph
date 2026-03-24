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


def plan_contains(plan, operator):
    return any(operator in line for line in plan)


def test_basic_orderby_elimination(memgraph):
    """ORDER BY n.prop eliminated when index scan on :L(prop) with range filter."""
    memgraph.execute("CREATE INDEX ON :L(prop);")

    plan = get_plan(memgraph, "MATCH (n:L) WHERE n.prop > 5 RETURN n ORDER BY n.prop")

    assert plan_contains(plan, "ScanAllByLabelProperties"), f"Expected index scan in plan: {plan}"
    assert not plan_contains(plan, "OrderBy"), f"OrderBy should be eliminated: {plan}"


def test_orderby_desc_not_eliminated(memgraph):
    """ORDER BY n.prop DESC should not be eliminated (index is ASC only)."""
    memgraph.execute("CREATE INDEX ON :L(prop);")

    plan = get_plan(memgraph, "MATCH (n:L) WHERE n.prop > 5 RETURN n ORDER BY n.prop DESC")

    assert plan_contains(plan, "OrderBy"), f"OrderBy should NOT be eliminated for DESC: {plan}"


def test_orderby_elimination_with_limit(memgraph):
    """ORDER BY eliminated but LIMIT preserved."""
    memgraph.execute("CREATE INDEX ON :L(prop);")

    plan = get_plan(memgraph, "MATCH (n:L) WHERE n.prop > 5 RETURN n ORDER BY n.prop LIMIT 10")

    assert not plan_contains(plan, "OrderBy"), f"OrderBy should be eliminated: {plan}"
    assert plan_contains(plan, "Limit"), f"Limit should remain: {plan}"


def test_orderby_with_expand(memgraph):
    """ORDER BY eliminated when Expand is between OrderBy and index scan."""
    memgraph.execute("CREATE INDEX ON :L(prop);")

    plan = get_plan(memgraph, "MATCH (n:L)-[r]->(m) WHERE n.prop > 5 RETURN n, m ORDER BY n.prop")

    assert plan_contains(plan, "ScanAllByLabelProperties"), f"Expected index scan in plan: {plan}"
    assert not plan_contains(plan, "OrderBy"), f"OrderBy should be eliminated (Expand preserves order): {plan}"


def test_orderby_different_property_not_eliminated(memgraph):
    """ORDER BY n.other not eliminated when index is on :L(prop)."""
    memgraph.execute("CREATE INDEX ON :L(prop);")

    plan = get_plan(memgraph, "MATCH (n:L) WHERE n.prop > 5 RETURN n ORDER BY n.other")

    assert plan_contains(plan, "OrderBy"), f"OrderBy should NOT be eliminated (different property): {plan}"


def test_orderby_no_property_index(memgraph):
    """ORDER BY not eliminated when only a label index exists (no property index)."""
    memgraph.execute("CREATE INDEX ON :L;")

    plan = get_plan(memgraph, "MATCH (n:L) RETURN n ORDER BY n.prop")

    assert plan_contains(plan, "OrderBy"), f"OrderBy should NOT be eliminated (label-only index): {plan}"


def test_orderby_aggregate_blocks(memgraph):
    """Aggregate between OrderBy and scan blocks elimination."""
    memgraph.execute("CREATE INDEX ON :L(prop);")

    plan = get_plan(
        memgraph,
        "MATCH (n:L) WHERE n.prop > 5 RETURN n.prop AS p, count(*) AS c ORDER BY p",
    )

    assert plan_contains(plan, "OrderBy"), f"OrderBy should NOT be eliminated (Aggregate blocks): {plan}"


def test_orderby_with_renaming_blocks(memgraph):
    """WITH renaming (n AS m) blocks elimination."""
    memgraph.execute("CREATE INDEX ON :L(prop);")

    plan = get_plan(
        memgraph,
        "MATCH (n:L) WHERE n.prop > 5 WITH n AS m RETURN m ORDER BY m.prop",
    )

    assert plan_contains(plan, "OrderBy"), f"OrderBy should NOT be eliminated (WITH renames variable): {plan}"


def test_orderby_equality_filter(memgraph):
    """ORDER BY on equality-filtered property is trivially satisfied."""
    memgraph.execute("CREATE INDEX ON :L(prop);")

    plan = get_plan(memgraph, "MATCH (n:L) WHERE n.prop = 5 RETURN n ORDER BY n.prop")

    assert not plan_contains(plan, "OrderBy"), f"OrderBy should be eliminated (equality filter): {plan}"


def test_orderby_non_property_expr_not_eliminated(memgraph):
    """ORDER BY n.prop + 1 should not be eliminated."""
    memgraph.execute("CREATE INDEX ON :L(prop);")

    plan = get_plan(memgraph, "MATCH (n:L) WHERE n.prop > 5 RETURN n ORDER BY n.prop + 1")

    assert plan_contains(plan, "OrderBy"), f"OrderBy should NOT be eliminated (non-property expr): {plan}"
