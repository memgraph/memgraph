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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
