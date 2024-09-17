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
        " * ScanAllByEdgeType (m)<-[r:TYPE]-(n)",
        " * Once",
    ]
    expected_results = [1, 2, 3, 4, 5, 6, 7]
    actual_explain = get_explain(memgraph, query)

    assert expected_explain == actual_explain

    actual_results = [x["prop"] for x in memgraph.execute_and_fetch(query)]
    assert expected_results == actual_results


def test_scan_all_by_edge_type_property_index_plan(memgraph):
    import_dataset(memgraph)

    query = "match (n)-[r:TYPE]->(m) where r.prop is not null return r.prop as prop;"

    expected_explain = [
        " * Produce {prop}",
        " * ScanAllByEdgeTypeProperty (m)<-[r:TYPE {prop}]-(n)",
        " * Once",
    ]
    expected_results = [1, 2, 3, 4, 5, 6, 7]
    actual_explain = get_explain(memgraph, query)

    assert expected_explain == actual_explain

    actual_results = [x["prop"] for x in memgraph.execute_and_fetch(query)]
    assert expected_results == actual_results


def test_scan_all_by_edge_type_property_value_index_plan(memgraph):
    import_dataset(memgraph)

    query = "match (n)-[r:TYPE]->(m) where r.prop = 2 return r.prop as prop;"

    expected_explain = [
        " * Produce {prop}",
        " * ScanAllByEdgeTypePropertyValue (m)<-[r:TYPE {prop}]-(n)",
        " * Once",
    ]
    expected_results = [2]
    actual_explain = get_explain(memgraph, query)

    assert expected_explain == actual_explain

    actual_results = [x["prop"] for x in memgraph.execute_and_fetch(query)]
    assert expected_results == actual_results


def test_scan_all_by_edge_type_property_range_index_plan(memgraph):
    import_dataset(memgraph)

    query = "match (n)-[r:TYPE]->(m) where r.prop > 1 and r.prop < 3 return r.prop as prop;"

    expected_explain = [
        " * Produce {prop}",
        " * ScanAllByEdgeTypePropertyRange (m)<-[r:TYPE {prop}]-(n)",
        " * Once",
    ]
    expected_results = [2]
    actual_explain = get_explain(memgraph, query)

    assert expected_explain == actual_explain

    actual_results = [x["prop"] for x in memgraph.execute_and_fetch(query)]
    assert expected_results == actual_results


def test_scan_all_by_edge_type_property_range_cartesian_index_plan(memgraph):
    import_dataset(memgraph)

    query = "match (n)-[r:TYPE]->(m), (a)-[b:TYPE]->(c) where r.prop = 1 and b.prop = 2 return r.prop as prop1, b.prop as prop2;"

    expected_explain = [
        " * Produce {prop1, prop2}",
        " * EdgeUniquenessFilter {b : r}",
        " * Cartesian {r, n, m : b, a, c}",
        " |\\ ",
        " | * ScanAllByEdgeTypePropertyValue (a)-[b:TYPE {prop}]->(c)",
        " | * Once",
        " * ScanAllByEdgeTypePropertyValue (n)-[r:TYPE {prop}]->(m)",
        " * Once",
    ]
    expected_results = [(1, 2)]
    actual_explain = get_explain(memgraph, query)

    assert expected_explain == actual_explain

    actual_results = [(x["prop1"], x["prop2"]) for x in memgraph.execute_and_fetch(query)]
    assert expected_results == actual_results


def test_set_from_applies_on_the_edge_index(memgraph):
    memgraph.execute("create ({id: 1})-[:TYPE {prop: 1}]->({id: 2});")
    memgraph.execute("create ({id: 3})")
    memgraph.execute("create edge index on :TYPE;")
    memgraph.execute("create edge index on :TYPE(prop);")

    actual_results = list(memgraph.execute_and_fetch("MATCH (n)-[r:TYPE]->(m) return n.id as nid, m.id as mid"))
    assert len(actual_results) == 1
    actual_results = actual_results[0]
    assert actual_results["nid"] == 1
    assert actual_results["mid"] == 2

    memgraph.execute("MATCH (n)-[r]->(m), (z {id: 3}) CALL edge_indices.set_from(r, z);")

    actual_results = list(memgraph.execute_and_fetch("MATCH (n)-[r:TYPE]->(m) return n.id as nid, m.id as mid"))
    assert len(actual_results) == 1
    actual_results = actual_results[0]
    assert actual_results["nid"] == 3
    assert actual_results["mid"] == 2

    memgraph.execute("MATCH (n)-[r]->(m), (z {id: 1}) CALL edge_indices.set_from(r, z);")

    actual_results = list(memgraph.execute_and_fetch("MATCH (n)-[r:TYPE]->(m) return n.id as nid, m.id as mid"))
    assert len(actual_results) == 1
    actual_results = actual_results[0]
    assert actual_results["nid"] == 1
    assert actual_results["mid"] == 2


def test_set_to_applies_on_the_edge_index(memgraph):
    memgraph.execute("create ({id: 1})-[:TYPE {prop: 1}]->({id: 2});")
    memgraph.execute("create ({id: 3})")
    memgraph.execute("create edge index on :TYPE;")
    memgraph.execute("create edge index on :TYPE(prop);")

    actual_results = list(memgraph.execute_and_fetch("MATCH (n)-[r:TYPE]->(m) return n.id as nid, m.id as mid"))
    assert len(actual_results) == 1
    actual_results = actual_results[0]
    assert actual_results["nid"] == 1
    assert actual_results["mid"] == 2

    memgraph.execute("MATCH (n)-[r]->(m), (z {id: 3}) CALL edge_indices.set_to(r, z);")

    actual_results = list(memgraph.execute_and_fetch("MATCH (n)-[r:TYPE]->(m) return n.id as nid, m.id as mid"))
    assert len(actual_results) == 1
    actual_results = actual_results[0]
    assert actual_results["nid"] == 1
    assert actual_results["mid"] == 3

    memgraph.execute("MATCH (n)-[r]->(m), (z {id: 2}) CALL edge_indices.set_to(r, z);")

    actual_results = list(memgraph.execute_and_fetch("MATCH (n)-[r:TYPE]->(m) return n.id as nid, m.id as mid"))
    assert len(actual_results) == 1
    actual_results = actual_results[0]
    assert actual_results["nid"] == 1
    assert actual_results["mid"] == 2


def test_change_type_applies_on_the_edge_index(memgraph):
    memgraph.execute("create ({id: 1})-[:TYPE {prop: 1}]->({id: 2});")
    memgraph.execute("create ({id: 3})")
    memgraph.execute("create edge index on :TYPE;")
    memgraph.execute("create edge index on :TYPE(prop);")

    actual_results = list(memgraph.execute_and_fetch("MATCH (n)-[r:TYPE]->(m) return n.id as nid, m.id as mid"))
    assert len(actual_results) == 1
    actual_results = actual_results[0]
    assert actual_results["nid"] == 1
    assert actual_results["mid"] == 2

    memgraph.execute("MATCH (n)-[r]->(m) CALL edge_indices.change_type(r, 'TYPE2');")

    # TODO: This behaviour is invalid and should be corrected in the storage
    # There should be actually 0 results after this
    # Deltas used are only for adding and removing in and out edges.
    # There is no delta for changing the edge type so far
    actual_results = list(memgraph.execute_and_fetch("MATCH (n)-[r:TYPE]->(m) return n.id as nid, m.id as mid"))
    actual_results = actual_results[0]
    assert actual_results["nid"] == 1
    assert actual_results["mid"] == 2

    memgraph.execute("MATCH (n)-[r]->(m) CALL edge_indices.change_type(r, 'TYPE');")

    actual_results = list(memgraph.execute_and_fetch("MATCH (n)-[r:TYPE]->(m) return n.id as nid, m.id as mid"))
    assert len(actual_results) == 1
    actual_results = actual_results[0]
    assert actual_results["nid"] == 1
    assert actual_results["mid"] == 2


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
