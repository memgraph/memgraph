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

import mgclient
import pytest
from common import memgraph
from gqlalchemy import Memgraph


def import_dataset(memgraph: Memgraph):
    memgraph.execute("create edge index on :TYPE;")
    memgraph.execute("create edge index on :TYPE(prop);")
    memgraph.execute("create ({id: 1})-[:TYPE {prop: 1}]->({id: 2});")
    memgraph.execute("create ({id: 1})-[:TYPE {prop: 2}]->({id: 2});")
    memgraph.execute("create ({id: 1})-[:TYPE {prop: 3}]->({id: 2});")
    memgraph.execute("create ({id: 1})-[:TYPE {prop: 4}]->({id: 2});")
    memgraph.execute("create ({id: 1})-[:TYPE {prop: 5}]->({id: 2});")
    memgraph.execute("create ({id: 1})-[:TYPE {prop: 6}]->({id: 2})-[:TYPE {prop: 7}]->({id: 3});")


def get_explain(memgraph, query):
    return [row["QUERY PLAN"] for row in memgraph.execute_and_fetch(f"EXPLAIN {query}")]


def test_scan_all_by_edge_type_index_plan(memgraph):
    import_dataset(memgraph)

    query = "match (n)-[r:TYPE]->(m) return r.prop as prop;"

    expected_explain = [
        " * Produce {prop}",
        " * ScanAllByEdgeType (n)-[r:TYPE]->(m)",
        " * Once",
    ]
    expected_results = {1, 2, 3, 4, 5, 6, 7}
    actual_explain = get_explain(memgraph, query)

    assert expected_explain == actual_explain

    actual_results = {x["prop"] for x in memgraph.execute_and_fetch(query)}
    assert expected_results == actual_results


def test_scan_all_by_edge_type_property_index_plan(memgraph):
    import_dataset(memgraph)

    query = "match (n)-[r:TYPE]->(m) where r.prop is not null return r.prop as prop;"

    expected_explain = [
        " * Produce {prop}",
        " * ScanAllByEdgeTypeProperty (n)-[r:TYPE {prop}]->(m)",
        " * Once",
    ]
    expected_results = {1, 2, 3, 4, 5, 6, 7}
    actual_explain = get_explain(memgraph, query)

    assert expected_explain == actual_explain

    actual_results = {x["prop"] for x in memgraph.execute_and_fetch(query)}
    assert expected_results == actual_results


def test_scan_all_by_edge_type_property_value_index_plan(memgraph):
    import_dataset(memgraph)

    query = "match (n)-[r:TYPE]->(m) where r.prop = 2 return r.prop as prop;"

    expected_explain = [
        " * Produce {prop}",
        " * ScanAllByEdgeTypePropertyValue (n)-[r:TYPE {prop}]->(m)",
        " * Once",
    ]
    expected_results = {2}
    actual_explain = get_explain(memgraph, query)

    assert expected_explain == actual_explain

    actual_results = {x["prop"] for x in memgraph.execute_and_fetch(query)}
    assert expected_results == actual_results


def test_scan_all_by_edge_type_property_range_index_plan(memgraph):
    import_dataset(memgraph)

    query = "match (n)-[r:TYPE]->(m) where r.prop > 1 and r.prop < 3 return r.prop as prop;"

    expected_explain = [
        " * Produce {prop}",
        " * ScanAllByEdgeTypePropertyRange (n)-[r:TYPE {prop}]->(m)",
        " * Once",
    ]
    expected_results = {2}
    actual_explain = get_explain(memgraph, query)

    assert expected_explain == actual_explain

    actual_results = {x["prop"] for x in memgraph.execute_and_fetch(query)}
    assert expected_results == actual_results


def test_scan_all_by_edge_type_property_range_cartesian_index_plan(memgraph):
    import_dataset(memgraph)

    query = "match (n)-[r:TYPE]->(m), (a)-[b:TYPE]->(c) where r.prop = 1 and b.prop = 2 return r.prop as prop1, b.prop as prop2;"

    expected_explain = [
        " * Produce {prop1, prop2}",
        " * EdgeUniquenessFilter {b : r}",
        " * Cartesian {n, m, r : b, a, c}",
        " |\\ ",
        " | * ScanAllByEdgeTypePropertyValue (a)-[b:TYPE {prop}]->(c)",
        " | * Once",
        " * ScanAllByEdgeTypePropertyValue (n)-[r:TYPE {prop}]->(m)",
        " * Once",
    ]
    expected_results = {(1, 2)}
    actual_explain = get_explain(memgraph, query)

    assert expected_explain == actual_explain

    actual_results = {(x["prop1"], x["prop2"]) for x in memgraph.execute_and_fetch(query)}
    assert expected_results == actual_results


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
