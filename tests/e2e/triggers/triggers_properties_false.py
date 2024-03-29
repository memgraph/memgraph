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
from common import connect, execute_and_fetch_all


@pytest.fixture(scope="function")
def multi_db(request, connect):
    cursor = connect.cursor()
    if request.param:
        execute_and_fetch_all(cursor, "CREATE DATABASE clean")
        execute_and_fetch_all(cursor, "USE DATABASE clean")
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")
    pass
    yield connect


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
@pytest.mark.parametrize("ba_commit", ["BEFORE COMMIT", "AFTER COMMIT"])
def test_create_on_create(ba_commit, multi_db):
    """
    Args:
        ba_commit (str): BEFORE OR AFTER commit
    """
    cursor = multi_db.cursor()
    QUERY_TRIGGER_CREATE = f"""
        CREATE TRIGGER CreateTriggerEdgesCount
        ON --> CREATE
        {ba_commit}
        EXECUTE
        CREATE (n:CreatedEdge {{count: size(createdEdges)}})
    """

    execute_and_fetch_all(cursor, QUERY_TRIGGER_CREATE)
    execute_and_fetch_all(cursor, "CREATE (n:Node {id: 1})")
    execute_and_fetch_all(cursor, "CREATE (n:Node {id: 2})")
    res = execute_and_fetch_all(cursor, "MATCH (n:Node) RETURN n")
    assert len(res) == 2
    res2 = execute_and_fetch_all(cursor, "MATCH (n:CreatedEdge) RETURN n")
    assert len(res2) == 0
    QUERY_CREATE_EDGE = """
        MATCH (n:Node {id: 1}), (m:Node {id: 2})
        CREATE (n)-[r:TYPE]->(m);
    """
    execute_and_fetch_all(cursor, QUERY_CREATE_EDGE)
    # See if trigger was triggered
    nodes = execute_and_fetch_all(cursor, "MATCH (n:Node) RETURN n")
    assert len(nodes) == 2
    created_edges = execute_and_fetch_all(cursor, "MATCH (n:CreatedEdge) RETURN n")
    assert len(created_edges) == 1
    # execute_and_fetch_all(cursor, "DROP TRIGGER CreateTriggerEdgesCount")
    # execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")

    # check that there is no cross contamination between databases
    nodes = execute_and_fetch_all(cursor, "SHOW DATABASES")
    if len(nodes) == 2:  # multi db mode
        execute_and_fetch_all(cursor, "USE DATABASE memgraph")
        created_edges = execute_and_fetch_all(cursor, "MATCH (n:CreatedEdge) RETURN n")
        assert len(created_edges) == 0


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
@pytest.mark.parametrize("ba_commit", ["AFTER COMMIT", "BEFORE COMMIT"])
def test_create_on_delete(ba_commit, multi_db):
    """
    Args:
        ba_commit (str): BEFORE OR AFTER commit
    """
    cursor = multi_db.cursor()
    QUERY_TRIGGER_CREATE = f"""
        CREATE TRIGGER DeleteTriggerEdgesCount
        ON --> DELETE
        {ba_commit}
        EXECUTE
        CREATE (n:DeletedEdge {{count: size(deletedEdges)}})
    """
    # Setup queries
    execute_and_fetch_all(cursor, QUERY_TRIGGER_CREATE)
    execute_and_fetch_all(cursor, "CREATE (n:Node {id: 1})")
    execute_and_fetch_all(cursor, "CREATE (n:Node {id: 2})")
    execute_and_fetch_all(cursor, "CREATE (n:Node {id: 3})")
    execute_and_fetch_all(cursor, "CREATE (n:Node {id: 4})")
    res = execute_and_fetch_all(cursor, "MATCH (n:Node) RETURN n")
    assert len(res) == 4
    res2 = execute_and_fetch_all(cursor, "MATCH (n:DeletedEdge) RETURN n")
    assert len(res2) == 0
    # create an edge that will be deleted
    QUERY_CREATE_EDGE = """
        MATCH (n:Node {id: 1}), (m:Node {id: 2})
        CREATE (n)-[r:TYPE]->(m);
    """
    execute_and_fetch_all(cursor, QUERY_CREATE_EDGE)
    # create an edge that won't be deleted
    QUERY_CREATE_EDGE_NO_DELETE = """
        MATCH (n:Node {id: 3}), (m:Node {id: 4})
        CREATE (n)-[r:NO_DELETE_EDGE]->(m);
    """
    execute_and_fetch_all(cursor, QUERY_CREATE_EDGE_NO_DELETE)
    # Delete only one type of the edger
    QUERY_DELETE_EDGE = """
        MATCH ()-[r:TYPE]->()
        DELETE r;
    """
    execute_and_fetch_all(cursor, QUERY_DELETE_EDGE)
    # See if trigger was triggered
    nodes = execute_and_fetch_all(cursor, "MATCH (n:Node) RETURN n")
    assert len(nodes) == 4
    # Check how many edges got deleted
    deleted_edges = execute_and_fetch_all(cursor, "MATCH (n:DeletedEdge) RETURN n")
    assert len(deleted_edges) == 1
    # execute_and_fetch_all(cursor, "DROP TRIGGER DeleteTriggerEdgesCount")
    # execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")``

    # check that there is no cross contamination between databases
    nodes = execute_and_fetch_all(cursor, "SHOW DATABASES")
    if len(nodes) == 2:  # multi db mode
        execute_and_fetch_all(cursor, "USE DATABASE memgraph")
        created_edges = execute_and_fetch_all(cursor, "MATCH (n:CreatedEdge) RETURN n")
        assert len(created_edges) == 0


# @pytest.mark.parametrize("multi_db", [False, True], indirect=True)
@pytest.mark.parametrize("ba_commit", ["BEFORE COMMIT", "AFTER COMMIT"])
def test_create_on_delete_explicit_transaction(ba_commit):
    """
    Args:
        ba_commit (str): BEFORE OR AFTER commit
    """
    connection_with_autocommit = mgclient.connect(host="localhost", port=7687)
    connection_with_autocommit.autocommit = True
    cursor_autocommit = connection_with_autocommit.cursor()
    QUERY_TRIGGER_CREATE = f"""
        CREATE TRIGGER DeleteTriggerEdgesCountExplicit
        ON --> DELETE
        {ba_commit}
        EXECUTE
        CREATE (n:DeletedEdge {{count: size(deletedEdges)}})
    """
    # Setup queries
    execute_and_fetch_all(cursor_autocommit, QUERY_TRIGGER_CREATE)
    # Start explicit transaction on the execution of the first command
    connection_without_autocommit = mgclient.connect(host="localhost", port=7687)
    connection_without_autocommit.autocommit = False
    cursor = connection_without_autocommit.cursor()
    execute_and_fetch_all(cursor, "CREATE (n:Node {id: 1})")
    execute_and_fetch_all(cursor, "CREATE (n:Node {id: 2})")
    execute_and_fetch_all(cursor, "CREATE (n:Node {id: 3})")
    execute_and_fetch_all(cursor, "CREATE (n:Node {id: 4})")
    res = execute_and_fetch_all(cursor, "MATCH (n:Node) RETURN n")
    assert len(res) == 4
    res2 = execute_and_fetch_all(cursor, "MATCH (n:DeletedEdge) RETURN n;")
    assert len(res2) == 0
    QUERY_CREATE_EDGE = """
        MATCH (n:Node {id: 1}), (m:Node {id: 2})
        CREATE (n)-[r:TYPE]->(m);
    """
    execute_and_fetch_all(cursor, QUERY_CREATE_EDGE)
    # create an edge that won't be deleted
    QUERY_CREATE_EDGE_NO_DELETE = """
        MATCH (n:Node {id: 3}), (m:Node {id: 4})
        CREATE (n)-[r:NO_DELETE_EDGE]->(m);
    """
    execute_and_fetch_all(cursor, QUERY_CREATE_EDGE_NO_DELETE)
    # Delete only one type of the edger
    QUERY_DELETE_EDGE = """
        MATCH ()-[r:TYPE]->()
        DELETE r;
    """
    execute_and_fetch_all(cursor, QUERY_DELETE_EDGE)
    connection_without_autocommit.commit()  # finish explicit transaction
    nodes = execute_and_fetch_all(cursor, "MATCH (n:Node) RETURN n")
    assert len(nodes) == 4
    # Check how many edges got deleted
    deleted_nodes_edges = execute_and_fetch_all(cursor, "MATCH (n:DeletedEdge) RETURN n")
    assert len(deleted_nodes_edges) == 0
    # Delete with the original cursor because triggers aren't allowed in multi-transaction environment
    execute_and_fetch_all(cursor_autocommit, "DROP TRIGGER DeleteTriggerEdgesCountExplicit")
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")
    connection_without_autocommit.commit()  # finish explicit transaction
    nodes = execute_and_fetch_all(cursor_autocommit, "MATCH (n) RETURN n")
    assert len(nodes) == 0
    connection_with_autocommit.close()
    connection_without_autocommit.close()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
