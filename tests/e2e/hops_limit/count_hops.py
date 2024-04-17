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


def get_summary(query: str):
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            result = session.run(query)
            return result.consume().metadata


def test_hops_count_1():
    # prepare simple graph
    execute_query("MATCH (n) DETACH DELETE n")
    execute_query(
        "CREATE (a:Person {name: 'Alice'}) "
        "CREATE (b:Person {name: 'Bob'}) "
        "CREATE (c:Person {name: 'Charlie'}) "
        "CREATE (d:Person {name: 'David'}) "
        "CREATE (e:Person {name: 'Eve'}) "
        "CREATE (a)-[:KNOWS]->(b) "
        "CREATE (b)-[:KNOWS]->(c) "
        "CREATE (c)-[:KNOWS]->(d) "
        "CREATE (d)-[:KNOWS]->(e)"
    )

    # check hops count

    # expand variable
    summary = get_summary("MATCH (a:Person {name: 'Alice'})-[:KNOWS*]->(e:Person {name: 'Eve'}) RETURN e")
    assert summary["number_of_hops"] == 4

    summary = get_summary("MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(e:Person {name: 'Eve'}) RETURN e")
    assert summary["number_of_hops"] == 2

    summary = get_summary("MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..3]->(e:Person {name: 'Eve'}) RETURN e")
    assert summary["number_of_hops"] == 3

    summary = get_summary("MATCH (a:Person)-[:KNOWS*]->(e:Person) RETURN e")
    assert summary["number_of_hops"] == 10

    # bfs expand
    summary = get_summary("MATCH (a:Person {name: 'Alice'})-[:KNOWS *BFS]->(e:Person {name: 'Eve'}) RETURN e")
    assert summary["number_of_hops"] == 4

    summary = get_summary("MATCH (a:Person {name: 'Alice'})-[:KNOWS *BFS 1..2]->(e:Person {name: 'Eve'}) RETURN e")
    assert summary["number_of_hops"] == 2

    summary = get_summary("MATCH (a:Person {name: 'Alice'})-[:KNOWS *BFS 1..3]->(e:Person {name: 'Eve'}) RETURN e")
    assert summary["number_of_hops"] == 3

    # expand
    summary = get_summary("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(e:Person {name: 'Eve'}) RETURN e")
    assert summary["number_of_hops"] == 1


def test_hops_count_2():
    # prepare simple graph
    execute_query("MATCH (n) DETACH DELETE n")
    execute_query(
        "CREATE (a:Person {name: 'Alice'}) "
        "CREATE (b:Person {name: 'Bob'}) "
        "CREATE (c:Person {name: 'Charlie'}) "
        "CREATE (d:Person {name: 'David'}) "
        "CREATE (e:Person {name: 'Eve'}) "
        "CREATE (a)-[:KNOWS]->(b) "
        "CREATE (a)-[:KNOWS]->(c) "
        "CREATE (a)-[:KNOWS]->(d) "
        "CREATE (d)-[:KNOWS]->(e)"
    )

    # check hops count

    # expand variable
    summary = get_summary("MATCH (a:Person {name: 'Alice'})-[:KNOWS*]->(e:Person {name: 'Eve'}) RETURN e")
    assert summary["number_of_hops"] == 2

    summary = get_summary("MATCH (a:Person {name: 'Alice'})-[:KNOWS*..1]->(e:Person) RETURN e")
    assert summary["number_of_hops"] == 4

    summary = get_summary("MATCH (a:Person {name: 'Alice'})-[:KNOWS*..2]->(e:Person) RETURN e")
    assert summary["number_of_hops"] == 5

    summary = get_summary("MATCH (a:Person)-[:KNOWS*]->(e:Person) RETURN e")
    assert summary["number_of_hops"] == 5

    # bfs expand
    summary = get_summary("MATCH (a:Person {name: 'Alice'})-[:KNOWS *BFS]->(e:Person {name: 'Eve'}) RETURN e")
    assert summary["number_of_hops"] == 4  # first does scan by a and then expand to e

    summary = get_summary("MATCH (a:Person {name: 'Alice'})-[:KNOWS *BFS ..1]->(e:Person) RETURN e")
    assert summary["number_of_hops"] == 3

    summary = get_summary("MATCH (a:Person {name: 'Alice'})-[:KNOWS *BFS ..2]->(e:Person) RETURN e")
    assert summary["number_of_hops"] == 4

    # expand
    summary = get_summary("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(e:Person {name: 'Eve'}) RETURN e")
    assert summary["number_of_hops"] == 1

    summary = get_summary("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(e:Person) RETURN e")
    assert summary["number_of_hops"] == 4  # scans by e and then expand to a


def test_hops_count_3():
    # prepare simple graph
    execute_query("MATCH (n) DETACH DELETE n")
    execute_query(
        "CREATE (a:Person {name: 'Alice'}) "
        "CREATE (b:Person {name: 'Bob'}) "
        "CREATE (c:Person {name: 'Charlie'}) "
        "CREATE (d:Car {name: 'Audi'}) "
        "CREATE (e:Car {name: 'BMW'}) "
        "CREATE (a)-[:DRIVES {since: 2010}]->(d) "
        "CREATE (a)-[:DRIVES {since: 2015}]->(e) "
        "CREATE (b)-[:DRIVES {since: 2015}]->(e) "
        "CREATE (c)-[:DRIVES {since: 2015}]->(e)"
    )

    # check hops count

    # expand variable
    summary = get_summary("MATCH (a:Person {name: 'Alice'})-[:DRIVES*]->(e:Car {name: 'BMW'}) RETURN e")
    assert summary["number_of_hops"] == 3

    # bfs expand
    summary = get_summary("MATCH (a:Person {name: 'Alice'})-[:DRIVES *BFS]->(e:Car {name: 'BMW'}) RETURN e")
    assert summary["number_of_hops"] == 2  # first does scan by a and then expand to e

    summary = get_summary("MATCH (a:Person)-[:DRIVES *BFS (r, n | r.since = 2015)]->(e:Car) RETURN e;")
    assert summary["number_of_hops"] == 4

    summary = get_summary("MATCH (a:Person)-[:DRIVES *BFS (r, n | r.since = 2015)]-(e:Car) RETURN e;")
    assert summary["number_of_hops"] == 12

    # expand
    summary = get_summary("MATCH (a:Person {name: 'Alice'})-[:DRIVES]->(e:Car {name: 'BMW'}) RETURN e")
    assert summary["number_of_hops"] == 3  # scans by e and then expand to a

    summary = get_summary("MATCH (a:Person)-[:DRIVES]->(e:Car) RETURN e")
    assert summary["number_of_hops"] == 4  # scans by e and then expand to a


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
