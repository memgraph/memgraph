# Copyright 2024 Memgraph Ltd.
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
from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
AUTH = ("", "")


def execute_query(query: str):
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            session.run(query)


def get_response(query: str):
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            result = session.run(query)
            records = []
            for record in result:
                records.append(record)
            return records, result.consume().metadata


def prepare_graph():
    # prepare simple graph
    execute_query("MATCH (n) DETACH DELETE n")
    execute_query(
        "CREATE (a)-[:CONNECTED]->(b) "
        "CREATE (a)-[:CONNECTED]->(c) "
        "CREATE (b)-[:CONNECTED]->(d) "
        "CREATE (b)-[:CONNECTED]->(e) "
        "CREATE (c)-[:CONNECTED]->(f) "
        "CREATE (c)-[:CONNECTED]->(g) "
        "CREATE (d)-[:CONNECTED]->(h) "
        "CREATE (d)-[:CONNECTED]->(i) "
        "CREATE (e)-[:CONNECTED]->(j) "
        "CREATE (e)-[:CONNECTED]->(k) "
        "CREATE (f)-[:CONNECTED]->(l)"
    )


def prepare_supernode_graph():
    # prepare graph with supernode
    execute_query("MATCH (n) DETACH DELETE n")
    execute_query(
        "CREATE (a)-[:CONNECTED]->(b) "
        "CREATE (a)-[:CONNECTED]->(c) "
        "CREATE (a)-[:CONNECTED]->(d) "
        "CREATE (a)-[:CONNECTED]->(e) "
        "CREATE (a)-[:CONNECTED]->(f)"
    )


def test_hops_limit_dfs():
    prepare_graph()

    response, summary = get_response("USING HOPS LIMIT 5 MATCH (a)-[:CONNECTED *]->(e) RETURN e")
    assert len(response) == 5

    assert summary["number_of_hops"] == 5

    # both directions
    response, summary = get_response("USING HOPS LIMIT 5 MATCH (a)-[:CONNECTED *]-(e) RETURN e")
    assert len(response) == 4

    assert summary["number_of_hops"] == 5


def test_hops_limit_bfs():
    prepare_graph()

    response, summary = get_response("USING HOPS LIMIT 5 MATCH (a)-[:CONNECTED *]->(e) RETURN e")
    assert len(response) == 5

    assert summary["number_of_hops"] == 5

    # both directions
    response, summary = get_response("USING HOPS LIMIT 5 MATCH (a)-[:CONNECTED *]-(e) RETURN e")
    assert len(response) == 4

    assert summary["number_of_hops"] == 5


def test_simple_expand():
    prepare_graph()

    response, summary = get_response("USING HOPS LIMIT 5 MATCH (a)-[:CONNECTED]->(b) RETURN b")
    assert len(response) == 5
    assert summary["number_of_hops"] == 5

    response, summary = get_response("USING HOPS LIMIT 5 MATCH (a)-[:CONNECTED]-(b) RETURN b")
    assert len(response) == 3

    assert summary["number_of_hops"] == 5


def test_hops_limit_flag():
    prepare_supernode_graph()

    execute_query("SET DATABASE SETTING 'hops_limit_partial_results' TO 'false'")

    ### failing scenarios

    with pytest.raises(Exception):
        get_response("USING HOPS LIMIT 2 MATCH (a)-[:CONNECTED *]->(e) SET e:Test RETURN e")

    response, summary = get_response("MATCH (n:Test) RETURN n")

    assert len(response) == 0

    with pytest.raises(Exception):
        get_response("USING HOPS LIMIT 2 MATCH (a)-[:CONNECTED *BFS]->(e) SET e:Test RETURN e")

    response, summary = get_response("MATCH (n:Test) RETURN n")
    assert len(response) == 0

    with pytest.raises(Exception):
        get_response("USING HOPS LIMIT 2 MATCH (a)-[:CONNECTED]->(e) SET e:Test RETURN e")

    response, summary = get_response("MATCH (n:Test) RETURN n")
    assert len(response) == 0

    ### passing scenarios

    response, summary = get_response("USING HOPS LIMIT 5 MATCH (a)-[:CONNECTED *]->(e) RETURN e")
    assert len(response) == 5

    response, summary = get_response("USING HOPS LIMIT 5 MATCH (a)-[:CONNECTED *BFS]->(e) RETURN e")
    assert len(response) == 5

    response, summary = get_response("USING HOPS LIMIT 5 MATCH (a)-[:CONNECTED]->(e) RETURN e")
    assert len(response) == 5


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
