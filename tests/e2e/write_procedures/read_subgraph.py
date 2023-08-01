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
import typing

import mgclient
import pytest
from common import execute_and_fetch_all, has_n_result_row


@pytest.fixture(scope="function")
def multi_db(request, connection):
    cursor = connection.cursor()
    if request.param:
        execute_and_fetch_all(cursor, "CREATE DATABASE clean")
        execute_and_fetch_all(cursor, "USE DATABASE clean")
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")
    pass
    yield connection


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


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
def test_is_callable(multi_db):
    cursor = multi_db.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
    create_subgraph(cursor)

    assert has_n_result_row(cursor, "MATCH (n) RETURN n;", 6)
    assert has_n_result_row(
        cursor,
        f"MATCH p=(n:Person)-[:SUPPORTS]->(m:Team) WITH project(p) AS graph CALL read.subgraph_empty(graph, 2, 3) YIELD result RETURN result;",
        1,
    )

    execute_and_fetch_all(
        cursor,
        f"MATCH (n) DETACH DELETE n;",
    )


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
def test_incorrect_graph_argument_placement(multi_db):
    cursor = multi_db.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
    create_subgraph(cursor)

    assert has_n_result_row(cursor, "MATCH (n) RETURN n;", 6)
    queries = [
        f"MATCH p=(n:Person)-[:SUPPORTS]->(m:Team) WITH project(p) AS graph CALL read.subgraph_empty(2, graph, 3) YIELD result RETURN result;",
        f"MATCH p=(n:Person)-[:SUPPORTS]->(m:Team) WITH project(p) AS graph CALL read.subgraph_empty(2, 3, graph) YIELD result RETURN result;",
    ]
    for query in queries:
        with pytest.raises(mgclient.DatabaseError):
            execute_and_fetch_all(cursor, query)

    execute_and_fetch_all(
        cursor,
        f"MATCH (n) DETACH DELETE n;",
    )


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
def test_get_vertices(multi_db):
    cursor = multi_db.cursor()
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


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
def test_get_out_edges(multi_db):
    cursor = multi_db.cursor()
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


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
def test_get_in_edges(multi_db):
    cursor = multi_db.cursor()
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


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
def test_get_2_hop_edges(multi_db):
    cursor = multi_db.cursor()
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


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
def test_get_out_edges_vertex_id(multi_db):
    cursor = multi_db.cursor()
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


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
def test_subgraph_get_path_vertices(multi_db):
    cursor = multi_db.cursor()
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


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
def test_subgraph_get_path_edges(multi_db):
    cursor = multi_db.cursor()
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


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
def test_subgraph_get_path_vertices_in_subgraph(multi_db):
    cursor = multi_db.cursor()
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


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
def test_subgraph_insert_vertex_get_vertices(multi_db):
    cursor = multi_db.cursor()
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


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
def test_subgraph_insert_edge_get_vertex_out_edges(multi_db):
    cursor = multi_db.cursor()
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


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
def test_subgraph_create_edge_both_vertices_not_in_projected_graph_error(multi_db):
    cursor = multi_db.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
    create_subgraph(cursor)
    assert has_n_result_row(cursor, "MATCH (n) RETURN n;", 6)
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(
            cursor,
            f"MATCH p=(n:Person)-[:SUPPORTS]->(m:Team) WITH project(p) as graph MATCH (p1:Person {{id:2}}) MATCH (p2:Person {{id:4}}) CALL write.subgraph_insert_edge_get_vertex_out_edges(graph, p1, p2) YIELD edge RETURN edge;",
        )

    execute_and_fetch_all(
        cursor,
        f"MATCH (n) DETACH DELETE n;",
    )


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
def test_subgraph_remove_edge_get_vertex_out_edges(multi_db):
    cursor = multi_db.cursor()
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


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
def test_subgraph_remove_edge_not_in_subgraph_error(multi_db):
    cursor = multi_db.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
    create_subgraph(cursor)
    assert has_n_result_row(cursor, "MATCH (n) RETURN n;", 6)
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(
            cursor,
            f"MATCH p=(n:Person)-[:SUPPORTS]->(m:Team) WITH project(p) as graph MATCH (p1:Person {{id:1}})-[e:KNOWS]->(p2:Person {{id:2}}) CALL write.subgraph_remove_edge_get_vertex_out_edges(graph, e) YIELD edge RETURN edge;",
        )
    execute_and_fetch_all(
        cursor,
        f"MATCH (n) DETACH DELETE n;",
    )


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
def test_subgraph_remove_vertex_and_out_edges_get_vertices(multi_db):
    cursor = multi_db.cursor()
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
