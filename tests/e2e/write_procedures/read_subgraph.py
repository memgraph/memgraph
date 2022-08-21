# Copyright 2022 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import typing
import mgclient
import sys
import pytest
from common import execute_and_fetch_all, has_n_result_row


def create_subgraph(cursor):
    execute_and_fetch_all(cursor, "CREATE (n:Person {id: 1});")
    execute_and_fetch_all(cursor, "CREATE (n:Person {id: 2});")
    execute_and_fetch_all(cursor, "CREATE (n:Person {id: 3});")
    execute_and_fetch_all(cursor, "CREATE (n:Person {id: 4});")
    execute_and_fetch_all(cursor, "CREATE (n:Team {id: 5});")
    execute_and_fetch_all(cursor, "CREATE (n:Team {id: 6});")
    execute_and_fetch_all(cursor, "MATCH (p:Person {id: 1}) MATCH (t:Team {id:5}) CREATE (p)-[:SUPPORTS]->(t);")
    execute_and_fetch_all(cursor, "MATCH (p:Person {id: 1}) MATCH (t:Team {id:6}) CREATE (p)-[:SUPPORTS]->(t);")
    execute_and_fetch_all(cursor, "MATCH (p:Person {id: 2}) MATCH (t:Team {id:6}) CREATE (p)-[:SUPPORTS]->(t);")
    execute_and_fetch_all(cursor, "MATCH (p1:Person {id: 1}) MATCH (p2:Person {id:2}) CREATE (p1)-[:KNOWS]->(p2);")
    execute_and_fetch_all(cursor, "MATCH (t1:Team {id: 5}) MATCH (t2:Team {id:6}) CREATE (t1)-[:IS_RIVAL_TO]->(t2);")
    execute_and_fetch_all(cursor, "MATCH (p1:Person {id: 3}) MATCH (p2:Person {id:4}) CREATE (p1)-[:KNOWS]->(p2);")


def create_smaller_subgraph(cursor):
    execute_and_fetch_all(cursor, "CREATE (n:Person {id: 1});")
    execute_and_fetch_all(cursor, "CREATE (n:Person {id: 2});")
    execute_and_fetch_all(cursor, "CREATE (n:Team {id: 5});")
    execute_and_fetch_all(cursor, "CREATE (n:Team {id: 6});")
    execute_and_fetch_all(cursor, "MATCH (p:Person {id: 1}) MATCH (t:Team {id:5}) CREATE (p)-[:SUPPORTS]->(t);")
    execute_and_fetch_all(cursor, "MATCH (p:Person {id: 1}) MATCH (t:Team {id:6}) CREATE (p)-[:SUPPORTS]->(t);")
    execute_and_fetch_all(cursor, "MATCH (p:Person {id: 2}) MATCH (t:Team {id:6}) CREATE (p)-[:SUPPORTS]->(t);")


def test_get_vertices(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
    create_subgraph(cursor)

    assert has_n_result_row(cursor, "MATCH (n) RETURN n;", 6)
    assert has_n_result_row(
        cursor,
        f"MATCH p=(n:Person)-[:SUPPORTS]->(m:Team) WITH project(p) AS graph CALL read.subgraph_get_vertices(graph) YIELD node RETURN node;",
        4,
    )

    execute_and_fetch_all(
        cursor,
        f"MATCH (n) DETACH DELETE n;",
    )


def test_get_out_edges(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
    create_subgraph(cursor)

    assert has_n_result_row(cursor, "MATCH (n) RETURN n;", 6)
    assert has_n_result_row(
        cursor,
        f"MATCH p=(n:Person)-[:SUPPORTS]->(m:Team) WITH project(p) AS graph MATCH (n1:Person {{id:1}}) CALL read.subgraph_get_out_edges(graph, n1) YIELD edge RETURN edge;",
        2,
    )

    execute_and_fetch_all(
        cursor,
        f"MATCH (n) DETACH DELETE n;",
    )


def test_get_in_edges(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
    create_subgraph(cursor)

    assert has_n_result_row(cursor, "MATCH (n) RETURN n;", 6)
    assert has_n_result_row(
        cursor,
        f"MATCH p=(n:Person)-[:SUPPORTS]->(m:Team) WITH project(p) AS graph MATCH (t1:Team {{id:6}}) CALL read.subgraph_get_in_edges(graph, t1) YIELD edge RETURN edge;",
        2,
    )

    execute_and_fetch_all(
        cursor,
        f"MATCH (n) DETACH DELETE n;",
    )


def test_get_2_hop_edges(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
    create_subgraph(cursor)

    assert has_n_result_row(cursor, "MATCH (n) RETURN n;", 6)
    assert has_n_result_row(
        cursor,
        f"MATCH p=(n:Person)-[:SUPPORTS]->(m:Team) WITH project(p) AS graph MATCH (n1:Person {{id:1}}) CALL read.subgraph_get_2_hop_edges(graph, n1) YIELD edge RETURN edge;",
        0,
    )
    execute_and_fetch_all(
        cursor,
        f"MATCH (n) DETACH DELETE n;",
    )


def test_get_out_edges_vertex_id(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
    create_subgraph(cursor=cursor)

    assert has_n_result_row(cursor, "MATCH (n) RETURN n;", 6)
    assert has_n_result_row(
        cursor,
        f"MATCH p=(n:Person)-[:SUPPORTS]->(m:Team) WITH project(p) AS graph MATCH (n1:Person {{id:1}}) CALL read.subgraph_get_out_edges_vertex_id(graph, n1) YIELD edge RETURN edge;",
        2,
    )

    execute_and_fetch_all(
        cursor,
        f"MATCH (n) DETACH DELETE n;",
    )


def test_subgraph_get_path_vertices(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
    create_subgraph(cursor)

    assert has_n_result_row(cursor, "MATCH (n) RETURN n;", 6)
    assert has_n_result_row(
        cursor,
        f"MATCH p=(n:Person)-[:SUPPORTS]->(m:Team) WITH project(p) as graph MATCH path=(a:Person {{id: 1}})-[:SUPPORTS]->(b:Team {{id:5}}) CALL read.subgraph_get_path_vertices(graph, path) YIELD node RETURN node;",
        2,
    )
    execute_and_fetch_all(
        cursor,
        f"MATCH (n) DETACH DELETE n;",
    )


def test_subgraph_get_path_edges(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
    create_subgraph(cursor)

    assert has_n_result_row(cursor, "MATCH (n) RETURN n;", 6)
    assert has_n_result_row(
        cursor,
        f"MATCH p=(n:Person)-[:SUPPORTS]->(m:Team) WITH project(p) as graph MATCH path=(:Person {{id: 1}})-[:SUPPORTS]->(:Team {{id:5}}) CALL read.subgraph_get_path_edges(graph, path) YIELD edge RETURN edge;",
        1,
    )
    execute_and_fetch_all(
        cursor,
        f"MATCH (n) DETACH DELETE n;",
    )


def test_subgraph_get_path_vertices_in_subgraph(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
    create_subgraph(cursor)
    assert has_n_result_row(cursor, "MATCH (n) RETURN n;", 6)
    assert has_n_result_row(
        cursor,
        f"MATCH p=(n:Person)-[:SUPPORTS]->(m:Team) WITH project(p) as graph MATCH path=(:Person {{id: 1}})-[:SUPPORTS]->(:Team {{id:5}}) CALL read.subgraph_get_path_vertices_in_subgraph(graph, path) YIELD node RETURN node;",
        2,
    )
    execute_and_fetch_all(
        cursor,
        f"MATCH (n) DETACH DELETE n;",
    )


def test_subgraph_insert_vertex_get_vertices(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
    create_subgraph(cursor)
    assert has_n_result_row(cursor, "MATCH (n) RETURN n;", 6)
    assert has_n_result_row(
        cursor,
        f"MATCH p=(n:Person)-[:SUPPORTS]->(m:Team) WITH project(p) as graph CALL write.subgraph_insert_vertex_get_vertices(graph) YIELD node RETURN node;",
        5,
    )
    execute_and_fetch_all(
        cursor,
        f"MATCH (n) DETACH DELETE n;",
    )


def test_subgraph_insert_edge_get_vertex_out_edges(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
    create_subgraph(cursor)
    assert has_n_result_row(cursor, "MATCH (n) RETURN n;", 6)
    assert has_n_result_row(
        cursor,
        f"MATCH p=(n:Person)-[:SUPPORTS]->(m:Team) WITH project(p) as graph MATCH (p1:Person {{id:2}}) MATCH (t1:Team {{id:6}}) CALL write.subgraph_insert_edge_get_vertex_out_edges(graph, p1, t1) YIELD edge RETURN edge;",
        2,
    )
    execute_and_fetch_all(
        cursor,
        f"MATCH (n) DETACH DELETE n;",
    )


def test_subgraph_remove_edge_get_vertex_out_edges(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
    create_subgraph(cursor)
    assert has_n_result_row(cursor, "MATCH (n) RETURN n;", 6)
    assert has_n_result_row(
        cursor,
        f"MATCH p=(n:Person)-[:SUPPORTS]->(m:Team) WITH project(p) as graph MATCH (p1:Person {{id:1}})-[e:SUPPORTS]->(t1:Team {{id:5}}) CALL write.subgraph_remove_edge_get_vertex_out_edges(graph, e) YIELD edge RETURN edge;",
        1,
    )
    execute_and_fetch_all(
        cursor,
        f"MATCH (n) DETACH DELETE n;",
    )


def test_subgraph_remove_vertex_and_out_edges_get_vertices(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
    create_smaller_subgraph(cursor)
    assert has_n_result_row(cursor, "MATCH (n) RETURN n;", 4)
    assert has_n_result_row(
        cursor,
        f"MATCH p=(n:Person)-[:SUPPORTS]->(m:Team) WITH project(p) as graph MATCH (p1:Person {{id:1}}) CALL write.subgraph_remove_vertex_and_out_edges_get_vertices(graph, p1) YIELD node RETURN node;",
        3,
    )
    execute_and_fetch_all(
        cursor,
        f"MATCH (n) DETACH DELETE n;",
    )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
