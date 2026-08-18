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
    return [x[QUERY_PLAN] for x in memgraph.execute_and_fetch(f"EXPLAIN {query}")]


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
    plan = get_plan(memgraph, "MATCH (n:N) WHERE n.type STARTS WITH $p RETURN n")
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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
