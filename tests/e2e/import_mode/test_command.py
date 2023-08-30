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

import sys

import pytest
from common import connect, execute_and_fetch_all


def test_import_mode_disabled_for_in_memory_storages():
    cursor = connect().cursor()
    with pytest.raises(Exception):
        execute_and_fetch_all(cursor, "EDGE IMPORT MODE ACTIVE")


def test_import_mode_on_off():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE ACTIVE")
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE INACTIVE")


def test_creating_vertices():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    execute_and_fetch_all(cursor, "CREATE (u:User {id: 1})")
    execute_and_fetch_all(cursor, "CREATE (u:User {id: 2})")
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE ACTIVE")
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n) RETURN n"))) == 2
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n) RETURN n"))) == 2
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE INACTIVE")
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n) RETURN n"))) == 2
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n) RETURN n"))) == 2
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")


def test_creating_edges():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    execute_and_fetch_all(cursor, "CREATE (u:User {id: 1})")
    execute_and_fetch_all(cursor, "CREATE (u:User {id: 2})")
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE ACTIVE")
    execute_and_fetch_all(cursor, "MATCH (n:User {id: 1}), (m:User {id: 2}) CREATE (n)-[r:FRIENDS {id: 3}]->(m)")
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n)-[r]->(m) RETURN n, r, m"))) == 1
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE INACTIVE")
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n)-[r]->(m) RETURN n, r, m"))) == 1
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")


def test_label_index_vertices_loading():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    execute_and_fetch_all(cursor, "CREATE (u:User {id: 1})")
    execute_and_fetch_all(cursor, "CREATE (u:User {id: 2})")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :User")
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE ACTIVE")
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n:User) RETURN n"))) == 2
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n:User) RETURN n"))) == 2
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE INACTIVE")
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n:User) RETURN n"))) == 2
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n:User) RETURN n"))) == 2
    execute_and_fetch_all(cursor, "MATCH (n:User) DETACH DELETE n")


def test_label_index_edges_creation():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :User")
    execute_and_fetch_all(cursor, "CREATE (u:User {id: 1})")
    execute_and_fetch_all(cursor, "CREATE (u:User {id: 2})")
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE ACTIVE")
    execute_and_fetch_all(cursor, "MATCH (n:User {id: 1}), (m:User {id: 2}) CREATE (n)-[r:FRIENDS {id: 3}]->(m)")
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n)-[r]->(m) RETURN n, r, m"))) == 1
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE INACTIVE")
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n:User) RETURN n"))) == 2
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n:User) RETURN n"))) == 2
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n)-[r]->(m) RETURN n, r, m"))) == 1
    execute_and_fetch_all(cursor, "MATCH (n:User) DETACH DELETE n")


def test_label_property_index_vertices_loading():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :User(id)")
    execute_and_fetch_all(cursor, "CREATE (u:User {id: 1})")
    execute_and_fetch_all(cursor, "CREATE (u:User {id: 2})")
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE ACTIVE")
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n:User) WHERE n.id IS NOT NULL RETURN n"))) == 2
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n:User) WHERE n.id IS NOT NULL RETURN n"))) == 2
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE INACTIVE")
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n:User) WHERE n.id IS NOT NULL RETURN n"))) == 2
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n:User) WHERE n.id IS NOT NULL RETURN n"))) == 2
    execute_and_fetch_all(cursor, "MATCH (n:User) DETACH DELETE n")


def test_label_property_index_edges_creation():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :User(id)")
    execute_and_fetch_all(cursor, "CREATE (u:User {id: 1})")
    execute_and_fetch_all(cursor, "CREATE (u:User {id: 2})")
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE ACTIVE")
    execute_and_fetch_all(cursor, "MATCH (n:User {id: 1}), (m:User {id: 2}) CREATE (n)-[r:FRIENDS {id: 3}]->(m)")
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n)-[r]->(m) RETURN n, r, m"))) == 1
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE INACTIVE")
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n:User) RETURN n"))) == 2
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n:User) RETURN n"))) == 2
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n)-[r]->(m) RETURN n, r, m"))) == 1
    execute_and_fetch_all(cursor, "MATCH (n:User) DETACH DELETE n")


def test_edge_deletion_in_edge_import_mode():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :User(id)")
    execute_and_fetch_all(cursor, "CREATE (u:User {id: 1})")
    execute_and_fetch_all(cursor, "CREATE (u:User {id: 2})")
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE ACTIVE")
    execute_and_fetch_all(cursor, "MATCH (n:User {id: 1}), (m:User {id: 2}) CREATE (n)-[r:FRIENDS {id: 3}]->(m)")
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n)-[r]->(m) RETURN n, r, m"))) == 1
    execute_and_fetch_all(cursor, "MATCH (n)-[r:FRIENDS {id: 3}]->(m) DELETE r")
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n)-[r]->(m) RETURN n, r, m"))) == 0
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE INACTIVE")
    execute_and_fetch_all(cursor, "MATCH (n:User) DETACH DELETE n")


def test_modification_of_edge_properties_in_edge_import_mode():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :User(id)")
    execute_and_fetch_all(cursor, "CREATE (u:User {id: 1})")
    execute_and_fetch_all(cursor, "CREATE (u:User {id: 2})")
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE ACTIVE")
    execute_and_fetch_all(
        cursor, "MATCH (n:User {id: 1}), (m:User {id: 2}) CREATE (n)-[r:FRIENDS {id: 3, balance: 1000}]->(m)"
    )
    assert list(execute_and_fetch_all(cursor, "MATCH (n)-[r]->(m) RETURN r.balance"))[0][0] == 1000
    execute_and_fetch_all(cursor, "MATCH (n)-[r:FRIENDS {id: 3}]->(m) SET r.balance = 2000")
    assert list(execute_and_fetch_all(cursor, "MATCH (n)-[r]->(m) RETURN r.balance"))[0][0] == 2000
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE INACTIVE")
    assert list(execute_and_fetch_all(cursor, "MATCH (n)-[r]->(m) RETURN r.balance"))[0][0] == 2000
    execute_and_fetch_all(cursor, "MATCH (n:User) DETACH DELETE n")


def test_throw_on_vertex_add_label_during_edge_import_mode():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    execute_and_fetch_all(cursor, "CREATE (u:User {id: 1})")
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE ACTIVE")
    with pytest.raises(Exception):
        execute_and_fetch_all(cursor, "MATCH (u:User {id: 1}) SET u:User:Person RETURN u")
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE INACTIVE")
    execute_and_fetch_all(cursor, "MATCH (n:User) DETACH DELETE n")


def test_throw_on_vertex_remove_label_during_edge_import_mode():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    execute_and_fetch_all(cursor, "CREATE (u:User:Person {id: 1})")
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE ACTIVE")
    with pytest.raises(Exception):
        execute_and_fetch_all(cursor, "MATCH (u:User {id: 1}) REMOVE u:Person RETURN u")
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE INACTIVE")
    execute_and_fetch_all(cursor, "MATCH (n:User) DETACH DELETE n")


def test_throw_on_vertex_set_property_during_edge_import_mode():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    execute_and_fetch_all(cursor, "CREATE (u:User {id: 1, balance: 1000})")
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE ACTIVE")
    with pytest.raises(Exception):
        execute_and_fetch_all(cursor, "MATCH (u:User {id: 1}) SET u.balance = 2000")
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE INACTIVE")
    execute_and_fetch_all(cursor, "MATCH (n:User) DETACH DELETE n")


def test_throw_on_vertex_create_vertex_during_edge_import_mode():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    execute_and_fetch_all(cursor, "CREATE (u:User {id: 1, balance: 1000})")
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE ACTIVE")
    with pytest.raises(Exception):
        execute_and_fetch_all(cursor, "CREATE (m:Mother {id: 10})")
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE INACTIVE")
    execute_and_fetch_all(cursor, "MATCH (n:User) DETACH DELETE n")


def test_throw_changing_import_mode_while_in_explicit_tx():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    execute_and_fetch_all(cursor, "CREATE (u:User {id: 1, balance: 1000})")
    execute_and_fetch_all(cursor, "BEGIN")
    with pytest.raises(Exception):
        execute_and_fetch_all(cursor, "EDGE IMPORT MODE ACTIVE")
    execute_and_fetch_all(cursor, "MATCH (n:User) DETACH DELETE n")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
