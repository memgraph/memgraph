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

# Two or more property lookups on one identifier inside a map literal are evaluated by loading the
# record's properties in one read. Every query here binds that identifier to a succession of records,
# so each answers correctly only while the loaded properties follow the identifier's current record.
# The map projection and single-lookup forms alongside them read the same data without taking that path.

import sys

import pytest
from common import connect, execute_and_fetch_all


@pytest.fixture(scope="function")
def cursor():
    connection = connect()
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")
    yield cursor
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")


@pytest.fixture(scope="function")
def three_nodes(cursor):
    execute_and_fetch_all(cursor, "CREATE ({a: 1, b: 30}), ({a: 2, b: 20}), ({a: 3, b: 10})")
    return cursor


@pytest.fixture(scope="function")
def diamond(cursor):
    # The direct edge is the heavier of the two routes, so a weight read from the wrong edge changes
    # which path is shortest, and a filter on it changes which edges are walkable.
    execute_and_fetch_all(
        cursor,
        """CREATE (a:A), (b:B), (c:C),
                  (a)-[:E {w: 10, z: 0}]->(b),
                  (a)-[:E {w: 1, z: 0}]->(c),
                  (c)-[:E {w: 1, z: 0}]->(b)""",
    )
    return cursor


def test_list_comprehension_over_collected_nodes(three_nodes):
    results = execute_and_fetch_all(
        three_nodes,
        """MATCH (n) WITH n ORDER BY n.a WITH collect(n) AS xs
           RETURN [x IN xs | {a: x.a, b: x.b}] AS maps""",
    )
    assert results[0][0] == [{"a": 1, "b": 30}, {"a": 2, "b": 20}, {"a": 3, "b": 10}]


def test_list_comprehension_agrees_with_map_projection(three_nodes):
    results = execute_and_fetch_all(
        three_nodes,
        """MATCH (n) WITH n ORDER BY n.a WITH collect(n) AS xs
           RETURN [x IN xs | {a: x.a, b: x.b}] AS maps, [x IN xs | x {.a, .b}] AS projections""",
    )
    assert results[0][0] == results[0][1]


def test_reduce_over_collected_nodes(three_nodes):
    results = execute_and_fetch_all(
        three_nodes,
        """MATCH (n) WITH n ORDER BY n.a WITH collect(n) AS xs
           RETURN reduce(acc = [], x IN xs | acc + [{a: x.a, b: x.b}]) AS maps""",
    )
    assert results[0][0] == [{"a": 1, "b": 30}, {"a": 2, "b": 20}, {"a": 3, "b": 10}]


def test_filter_on_a_map_literal(three_nodes):
    results = execute_and_fetch_all(three_nodes, "MATCH (n) WHERE {p: n.a, q: n.b}.p >= 2 RETURN n.a AS a ORDER BY a")
    assert [row[0] for row in results] == [2, 3]


def test_order_by_a_map_literal(three_nodes):
    results = execute_and_fetch_all(three_nodes, "MATCH (n) RETURN n.a AS a ORDER BY {p: n.b, q: n.a}.p")
    assert [row[0] for row in results] == [3, 2, 1]


def test_variable_length_expansion_filter_lambda(diamond):
    results = execute_and_fetch_all(
        diamond,
        "MATCH p=(:A)-[*1..3 (e, n | {w: e.w, z: e.z}.w < 5)]->(:B) RETURN size(nodes(p)) AS hops",
    )
    assert [row[0] for row in results] == [3]


def test_bfs_filter_lambda(diamond):
    results = execute_and_fetch_all(
        diamond,
        "MATCH p=(:A)-[*BFS (e, n | {w: e.w, z: e.z}.w < 5)]->(:B) RETURN size(nodes(p)) AS hops",
    )
    assert [row[0] for row in results] == [3]


def test_weighted_shortest_path_weight_lambda(diamond):
    results = execute_and_fetch_all(
        diamond,
        "MATCH (:A)-[*WSHORTEST (e, n | {w: e.w, z: e.z}.w) total]->(:B) RETURN total",
    )
    assert [row[0] for row in results] == [2]


def test_map_literal_over_edges(diamond):
    results = execute_and_fetch_all(
        diamond,
        """MATCH ()-[e]->() WITH e ORDER BY e.w WITH collect(e) AS es
           RETURN [x IN es | {w: x.w, z: x.z}] AS maps""",
    )
    assert results[0][0] == [{"w": 1, "z": 0}, {"w": 1, "z": 0}, {"w": 10, "z": 0}]


def test_one_symbol_holding_a_node_then_an_edge(diamond):
    # Nodes and edges are numbered apart, so the first node and the first edge can carry the same id
    # while being different records.
    results = execute_and_fetch_all(
        diamond,
        "MATCH (a:A)-[e]->(:B) RETURN [x IN [a, e] | {first: x.w, second: x.z}] AS maps",
    )
    assert results[0][0] == [{"first": None, "second": None}, {"first": 10, "second": 0}]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
