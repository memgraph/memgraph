# Copyright 2025 Memgraph Ltd.
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
from common import connect, execute_and_fetch_all, get_file_path


def test_small_file_nodes():
    cursor = connect(host="localhost", port=7687).cursor()
    load_query = f"LOAD PARQUET FROM '{get_file_path('nodes_100.parquet')}' AS row CREATE (n:N {{id: row.id, name: row.name, age: row.age, city: row.city}})"
    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n) return count(n)")[0][0] == 100
    execute_and_fetch_all(cursor, "match (n) detach delete n")


def test_small_file_edges():
    cursor = connect(host="localhost", port=7687).cursor()
    load_query = f"LOAD PARQUET FROM '{get_file_path('nodes_100.parquet')}' AS row CREATE (n:Person {{id: row.id, name: row.name, age: row.age, city: row.city}})"
    print(f"Nodes query: {load_query}")
    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n) return count(n)")[0][0] == 100
    load_query = f"LOAD PARQUET FROM '{get_file_path('edges_100.parquet')}' AS row MATCH (a:Person {{id: row.START_ID}}) MATCH (b:Person {{id: row.END_ID}}) CREATE (a)-[r:KNOWS {{type: row.TYPE, since: row.since, strength: row.strength}}]->(b);"
    print(f"Edges query: {load_query}")
    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n)-[e]->(m) return count (e)")[0][0] == 100
    execute_and_fetch_all(cursor, "match (n) detach delete n")


def test_null_nodes():
    cursor = connect(host="localhost", port=7687).cursor()
    load_query = f"LOAD PARQUET FROM '{get_file_path('nodes_with_nulls.parquet')}' AS row CREATE (n:N {{id: row.id, name: row.name, age: row.age, city: row.city}})"
    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n) return count(n)")[0][0] == 100
    execute_and_fetch_all(cursor, "match (n) detach delete n")


def test_null_edges():
    cursor = connect(host="localhost", port=7687).cursor()
    load_query = f"LOAD PARQUET FROM '{get_file_path('nodes_with_nulls.parquet')}' AS row CREATE (n:Person {{id: row.id, name: row.name, age: row.age, city: row.city}})"
    print(f"Nodes query: {load_query}")
    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n) return count(n)")[0][0] == 100
    load_query = f"LOAD PARQUET FROM '{get_file_path('edges_with_nulls.parquet')}' AS row MATCH (a:Person {{id: row.START_ID}}) MATCH (b:Person {{id: row.END_ID}}) CREATE (a)-[r:KNOWS {{type: row.TYPE, since: row.since, strength: row.strength}}]->(b);"
    print(f"Edges query: {load_query}")
    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n)-[e]->(m) return count (e)")[0][0] == 100
    execute_and_fetch_all(cursor, "match (n) detach delete n")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
