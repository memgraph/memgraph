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
from common import extract_query_plan, memgraph


def test_given_constraint_when_querying_then_index_scanning(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:label) ASSERT n.prop IS UNIQUE;")

    expected_results = [" * Produce {n}", " * ScanAllByLabelPropertyValue (n :label {prop})", " * Once"]

    actual_results = extract_query_plan(
        memgraph.execute_and_fetch("EXPLAIN MATCH (n:label) WHERE n.prop = 1 RETURN n;")
    )

    assert expected_results == actual_results


def test_given_multiprop_constraint_when_querying_then_sequential_scanning(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:label) ASSERT n.prop1, n.prop2 IS UNIQUE;")

    expected_results = [" * Produce {n}", " * Filter", " * ScanAll (n)", " * Once"]
    actual_results = extract_query_plan(
        memgraph.execute_and_fetch("EXPLAIN MATCH (n:label) WHERE n.prop1 = 1 AND n.prop2 = 2 RETURN n;")
    )

    assert expected_results == actual_results


def test_given_constraint_and_index_when_querying_then_index_scanning(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:label) ASSERT n.prop1 IS UNIQUE;")
    memgraph.execute("CREATE INDEX ON :label(prop1);")

    expected_results = [" * Produce {n}", " * ScanAllByLabelPropertyValue (n :label {prop1})", " * Once"]

    actual_results = extract_query_plan(
        memgraph.execute_and_fetch("EXPLAIN MATCH (n:label) WHERE n.prop1 = 1 RETURN n;")
    )

    assert expected_results == actual_results


def test_given_constraint_and_index_with_different_distribution_when_querying_then_prefer_constraint(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:label) ASSERT n.prop1 IS UNIQUE;")
    memgraph.execute("CREATE INDEX ON :label(prop2);")
    memgraph.execute("FOREACH (i IN range(1, 1000) | CREATE (:Node {prop1: i, prop2: i % 2}))")

    expected_results = [" * Produce {n}", " * Filter", " * ScanAllByLabelPropertyValue (n :label {prop1})", " * Once"]

    actual_results = extract_query_plan(
        memgraph.execute_and_fetch("EXPLAIN MATCH (n:label) WHERE n.prop1 = 500 and n.prop2 = 0 RETURN n;")
    )

    assert expected_results == actual_results


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
