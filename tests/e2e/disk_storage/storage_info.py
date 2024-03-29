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

import pytest
from common import connect, execute_and_fetch_all


def test_empty_show_storage_info(connect):
    cursor = connect.cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    results = execute_and_fetch_all(cursor, "SHOW STORAGE INFO")
    results = dict(map(lambda pair: (pair[0], pair[1]), results))
    assert results["vertex_count"] == 0
    assert results["edge_count"] == 0


def test_show_storage_info_after_initialization(connect):
    cursor = connect.cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    execute_and_fetch_all(cursor, "CREATE (n:User {id: 1})")
    execute_and_fetch_all(cursor, "CREATE (n:User {id: 2})")
    execute_and_fetch_all(cursor, "MATCH (n:User {id: 1}), (m:User {id: 2}) CREATE (n)-[r:FRIEND {id: 1}]->(m)")
    results = execute_and_fetch_all(cursor, "SHOW STORAGE INFO")
    results = dict(map(lambda pair: (pair[0], pair[1]), results))
    assert results["vertex_count"] == 2
    assert results["edge_count"] == 1
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")


def test_show_storage_info_detach_delete_vertex(connect):
    cursor = connect.cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    execute_and_fetch_all(cursor, "CREATE (n:User {id: 1})")
    execute_and_fetch_all(cursor, "CREATE (n:User {id: 2})")
    execute_and_fetch_all(cursor, "MATCH (n:User {id: 1}), (m:User {id: 2}) CREATE (n)-[r:FRIEND {id: 1}]->(m)")
    execute_and_fetch_all(cursor, "MATCH (n:User {id: 1}) DETACH DELETE n;")
    results = execute_and_fetch_all(cursor, "SHOW STORAGE INFO")
    results = dict(map(lambda pair: (pair[0], pair[1]), results))
    assert results["vertex_count"] == 1
    assert results["edge_count"] == 0
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")


def test_show_storage_info_delete_edge(connect):
    cursor = connect.cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    execute_and_fetch_all(cursor, "CREATE (n:User {id: 1})")
    execute_and_fetch_all(cursor, "CREATE (n:User {id: 2})")
    execute_and_fetch_all(cursor, "MATCH (n:User {id: 1}), (m:User {id: 2}) CREATE (n)-[r:FRIEND {id: 1}]->(m)")
    execute_and_fetch_all(cursor, "MATCH (n:User {id: 1})-[r]->(m:User {id: 2}) DELETE r;")
    results = execute_and_fetch_all(cursor, "SHOW STORAGE INFO")
    results = dict(map(lambda pair: (pair[0], pair[1]), results))
    assert results["vertex_count"] == 2
    assert results["edge_count"] == 0
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
