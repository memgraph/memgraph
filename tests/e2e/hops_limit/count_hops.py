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


def test_hops_count():
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
    summary = get_summary("MATCH (a:Person {name: 'Alice'})-[:KNOWS*]->(e:Person {name: 'Eve'}) RETURN e")
    print(summary)
    assert summary["query"]["hops"] == 4


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
