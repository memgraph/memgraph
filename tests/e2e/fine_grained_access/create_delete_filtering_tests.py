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

import common
import pytest
from mgclient import DatabaseError


def reset_and_prepare(admin_cursor):
    common.execute_and_fetch_all(admin_cursor, "REVOKE * ON NODES CONTAINING LABELS * FROM user;")
    common.execute_and_fetch_all(admin_cursor, "REVOKE * ON EDGES OF TYPE * FROM user;")
    common.execute_and_fetch_all(admin_cursor, "MATCH(n) DETACH DELETE n;")
    common.execute_and_fetch_all(admin_cursor, "CREATE (n:test_delete {name: 'test1'});")
    common.execute_and_fetch_all(admin_cursor, "CREATE (n:test_delete_1)-[r:edge_type_delete]->(m:test_delete_2);")


@pytest.mark.parametrize("switch", [False, True])
def test_create_node_all_labels_granted(switch):
    admin_connection = common.connect(username="admin", password="test")
    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE ON NODES CONTAINING LABELS * TO user;")
    user_connection = common.connect(username="user", password="test")
    if switch:
        common.switch_db(user_connection.cursor())
    results = common.execute_and_fetch_all(user_connection.cursor(), "CREATE (n:label1) RETURN n;")

    assert len(results) == 1


@pytest.mark.parametrize("switch", [False, True])
def test_create_node_all_labels_denied(switch):
    admin_connection = common.connect(username="admin", password="test")
    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON NODES CONTAINING LABELS * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(user_connection.cursor(), "CREATE (n:label1) RETURN n;")


@pytest.mark.parametrize("switch", [False, True])
def test_create_node_specific_label_granted(switch):
    admin_connection = common.connect(username="admin", password="test")
    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE ON NODES CONTAINING LABELS :label1 TO user;")
    user_connection = common.connect(username="user", password="test")
    if switch:
        common.switch_db(user_connection.cursor())
    results = common.execute_and_fetch_all(user_connection.cursor(), "CREATE (n:label1) RETURN n;")

    assert len(results) == 1


@pytest.mark.parametrize("switch", [False, True])
def test_create_node_specific_label_denied(switch):
    admin_connection = common.connect(username="admin", password="test")
    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON NODES CONTAINING LABELS :label1 TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(user_connection.cursor(), "CREATE (n:label1) RETURN n;")


@pytest.mark.parametrize("switch", [False, True])
def test_delete_node_all_labels_granted(switch):
    admin_connection = common.connect(username="admin", password="test")
    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(
        admin_connection.cursor(),
        "GRANT READ ON NODES CONTAINING LABELS *, DELETE ON NODES CONTAINING LABELS * TO user;",
    )
    user_connection = common.connect(username="user", password="test")
    if switch:
        common.switch_db(user_connection.cursor())
    common.execute_and_fetch_all(user_connection.cursor(), "MATCH (n:test_delete) DELETE n;")

    results = common.execute_and_fetch_all(user_connection.cursor(), "MATCH (n:test_delete) RETURN n;")

    assert len(results) == 0


@pytest.mark.parametrize("switch", [False, True])
def test_delete_node_all_labels_denied(switch):
    admin_connection = common.connect(username="admin", password="test")
    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON NODES CONTAINING LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON NODES CONTAINING LABELS * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(user_connection.cursor(), "MATCH (n:test_delete) DELETE n;")


@pytest.mark.parametrize("switch", [False, True])
def test_delete_node_specific_label_granted(switch):
    admin_connection = common.connect(username="admin", password="test")
    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(
        admin_connection.cursor(),
        "GRANT DELETE ON NODES CONTAINING LABELS :test_delete, READ ON NODES CONTAINING LABELS :test_delete TO user;",
    )
    user_connection = common.connect(username="user", password="test")
    if switch:
        common.switch_db(user_connection.cursor())
    results = common.execute_and_fetch_all(user_connection.cursor(), "MATCH (n:test_delete) DELETE n;")

    if switch:
        common.switch_db(admin_connection.cursor())
    results = common.execute_and_fetch_all(admin_connection.cursor(), "MATCH (n:test_delete) RETURN n;")

    assert len(results) == 0


@pytest.mark.parametrize("switch", [False, True])
def test_delete_node_specific_label_denied(switch):
    admin_connection = common.connect(username="admin", password="test")
    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON NODES CONTAINING LABELS :test_delete TO user;"
    )
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT UPDATE ON NODES CONTAINING LABELS :test_delete TO user;"
    )
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(user_connection.cursor(), "MATCH (n:test_delete) DELETE n;")


@pytest.mark.parametrize("switch", [False, True])
def test_create_edge_all_labels_all_edge_types_granted(switch):
    admin_connection = common.connect(username="admin", password="test")
    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON NODES CONTAINING LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE ON NODES CONTAINING LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE ON EDGES OF TYPE * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    results = common.execute_and_fetch_all(
        user_connection.cursor(),
        "CREATE (n:label1)-[r:edge_type]->(m:label2) RETURN n,r,m;",
    )

    assert len(results) == 1


@pytest.mark.parametrize("switch", [False, True])
def test_create_edge_all_labels_all_edge_types_denied(switch):
    admin_connection = common.connect(username="admin", password="test")
    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON NODES CONTAINING LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON EDGES OF TYPE * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "CREATE (n:label1)-[r:edge_type]->(m:label2) RETURN n,r,m;",
        )


@pytest.mark.parametrize("switch", [False, True])
def test_create_edge_all_labels_denied_all_edge_types_granted(switch):
    admin_connection = common.connect(username="admin", password="test")
    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON NODES CONTAINING LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON EDGES OF TYPE * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "CREATE (n:label1)-[r:edge_type]->(m:label2) RETURN n,r,m;",
        )


@pytest.mark.parametrize("switch", [False, True])
def test_create_edge_all_labels_granted_all_edge_types_denied(switch):
    admin_connection = common.connect(username="admin", password="test")
    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON NODES CONTAINING LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON EDGES OF TYPE * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "CREATE (n:label1)-[r:edge_type]->(m:label2) RETURN n,r,m;",
        )


@pytest.mark.parametrize("switch", [False, True])
def test_create_edge_all_labels_granted_specific_edge_types_denied(switch):
    admin_connection = common.connect(username="admin", password="test")
    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE ON NODES CONTAINING LABELS * TO user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(),
        "GRANT UPDATE ON EDGES OF TYPE :edge_type TO user;",
    )
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "CREATE (n:label1)-[r:edge_type]->(m:label2) RETURN n,r,m;",
        )


@pytest.mark.parametrize("switch", [False, True])
def test_create_edge_first_node_label_granted(switch):
    admin_connection = common.connect(username="admin", password="test")
    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE ON NODES CONTAINING LABELS :label1 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON NODES CONTAINING LABELS :label2 TO user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(),
        "GRANT CREATE ON EDGES OF TYPE :edge_type TO user;",
    )
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "CREATE (n:label1)-[r:edge_type]->(m:label2) RETURN n,r,m;",
        )


@pytest.mark.parametrize("switch", [False, True])
def test_create_edge_second_node_label_granted(switch):
    admin_connection = common.connect(username="admin", password="test")
    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE ON NODES CONTAINING LABELS :label2 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON NODES CONTAINING LABELS :label1 TO user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(),
        "GRANT CREATE ON EDGES OF TYPE :edge_type TO user;",
    )
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "CREATE (n:label1)-[r:edge_type]->(m:label2) RETURN n,r,m;",
        )


@pytest.mark.parametrize("switch", [False, True])
def test_delete_edge_all_labels_denied_all_edge_types_granted(switch):
    admin_connection = common.connect(username="admin", password="test")
    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON NODES CONTAINING LABELS * TO user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON EDGES OF TYPE *, DELETE ON EDGES OF TYPE * TO user;"
    )
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "MATCH (n:test_delete_1)-[r:edge_type_delete]->(m:test_delete_2) DELETE r",
        )


@pytest.mark.parametrize("switch", [False, True])
def test_delete_edge_all_labels_granted_all_edge_types_denied(switch):
    admin_connection = common.connect(username="admin", password="test")
    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON NODES CONTAINING LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGES OF TYPE * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON EDGES OF TYPE * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "MATCH (n:test_delete_1)-[r:edge_type_delete]->(m:test_delete_2) DELETE r",
        )


@pytest.mark.parametrize("switch", [False, True])
def test_delete_edge_all_labels_granted_specific_edge_types_denied(switch):
    admin_connection = common.connect(username="admin", password="test")
    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(
        admin_connection.cursor(),
        "GRANT READ ON NODES CONTAINING LABELS *, DELETE ON NODES CONTAINING LABELS * TO user;",
    )
    common.execute_and_fetch_all(
        admin_connection.cursor(),
        "GRANT READ ON EDGES OF TYPE :edge_type_delete TO user;",
    )
    common.execute_and_fetch_all(
        admin_connection.cursor(),
        "GRANT UPDATE ON EDGES OF TYPE :edge_type_delete TO user;",
    )
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "MATCH (n:test_delete_1)-[r:edge_type_delete]->(m:test_delete_2) DELETE r",
        )


@pytest.mark.parametrize("switch", [False, True])
def test_delete_edge_first_node_label_granted(switch):
    admin_connection = common.connect(username="admin", password="test")
    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON NODES CONTAINING LABELS :test_delete_1 TO user;"
    )
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT UPDATE ON NODES CONTAINING LABELS :test_delete_1 TO user;"
    )
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON NODES CONTAINING LABELS :test_delete_2 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGES OF TYPE :edge_type_delete TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT DELETE ON EDGES OF TYPE :edge_type_delete TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "MATCH (n:test_delete_1)-[r:edge_type_delete]->(m:test_delete_2) DELETE r",
        )


@pytest.mark.parametrize("switch", [False, True])
def test_delete_edge_second_node_label_granted(switch):
    admin_connection = common.connect(username="admin", password="test")
    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON NODES CONTAINING LABELS :test_delete_2 TO user;"
    )
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT UPDATE ON NODES CONTAINING LABELS :test_delete_2 TO user;"
    )
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON NODES CONTAINING LABELS :test_delete_1 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGES OF TYPE :edge_type_delete TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT DELETE ON EDGES OF TYPE :edge_type_delete TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "MATCH (n:test_delete_1)-[r:edge_type_delete]->(m:test_delete_2) DELETE r",
        )


@pytest.mark.parametrize("switch", [False, True])
def test_delete_node_with_edge_label_denied(switch):
    admin_connection = common.connect(username="admin", password="test")
    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(
        admin_connection.cursor(),
        "GRANT READ ON NODES CONTAINING LABELS :test_delete_1 TO user;",
    )
    common.execute_and_fetch_all(
        admin_connection.cursor(),
        "GRANT UPDATE ON NODES CONTAINING LABELS :test_delete_1 TO user;",
    )
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(user_connection.cursor(), "MATCH (n) DETACH DELETE n;")


@pytest.mark.parametrize("switch", [False, True])
def test_delete_node_with_edge_label_granted(switch):
    admin_connection = common.connect(username="admin", password="test")
    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(
        admin_connection.cursor(),
        "GRANT READ ON NODES CONTAINING LABELS :test_delete_1, DELETE ON NODES CONTAINING LABELS :test_delete_1 TO user;",
    )
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    common.execute_and_fetch_all(user_connection.cursor(), "MATCH (n) DETACH DELETE n;")

    if switch:
        common.switch_db(admin_connection.cursor())
    results = common.execute_and_fetch_all(admin_connection.cursor(), "MATCH (n:test_delete_1) RETURN n;")

    assert len(results) == 0


@pytest.mark.parametrize("switch", [False, True])
def test_set_label_when_label_granted(switch):
    admin_connection = common.connect(username="admin", password="test")
    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(
        admin_connection.cursor(),
        "GRANT READ ON NODES CONTAINING LABELS :update_label_2, UPDATE ON NODES CONTAINING LABELS :update_label_2 TO user;",
    )
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    common.execute_and_fetch_all(user_connection.cursor(), "MATCH (p:test_delete) SET p:update_label_2;")


@pytest.mark.parametrize("switch", [False, True])
def test_set_label_when_label_denied(switch):
    admin_connection = common.connect(username="admin", password="test")

    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT UPDATE ON NODES CONTAINING LABELS :update_label_2 TO user;"
    )
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON NODES CONTAINING LABELS :test_delete TO user;"
    )
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(user_connection.cursor(), "MATCH (p:test_delete) SET p:update_label_2;")


@pytest.mark.parametrize("switch", [False, True])
def test_remove_label_when_label_granted(switch):
    admin_connection = common.connect(username="admin", password="test")

    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON NODES CONTAINING LABELS :test_delete TO user;"
    )
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT UPDATE ON NODES CONTAINING LABELS :test_delete TO user;"
    )
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT DELETE ON NODES CONTAINING LABELS :test_delete TO user;"
    )
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    common.execute_and_fetch_all(user_connection.cursor(), "MATCH (p:test_delete) REMOVE p:test_delete;")


@pytest.mark.parametrize("switch", [False, True])
def test_remove_label_when_label_denied(switch):
    admin_connection = common.connect(username="admin", password="test")

    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT UPDATE ON NODES CONTAINING LABELS :update_label_2 TO user;"
    )
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON NODES CONTAINING LABELS :test_delete TO user;"
    )
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(user_connection.cursor(), "MATCH (p:test_delete) REMOVE p:test_delete;")


@pytest.mark.parametrize("switch", [False, True])
def test_merge_nodes_pass_when_having_read(switch):
    admin_connection = common.connect(username="admin", password="test")

    reset_and_prepare(admin_connection.cursor())
    common.create_multi_db(admin_connection.cursor(), switch, reset_and_prepare)
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON NODES CONTAINING LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE ON NODES CONTAINING LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGES OF TYPE * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    results = common.execute_and_fetch_all(
        user_connection.cursor(),
        "UNWIND [{id: '1', lat: 10, lng: 10}, {id: '2', lat: 10, lng: 10}, {id: '3', lat: 10, lng: 10}] AS row MERGE (o:Location {id: row.id}) RETURN o;",
    )

    assert len(results) == 3


@pytest.mark.parametrize("switch", [False, True])
def test_unlabeled_nodes_with_system_create_only(switch):
    admin_connection = common.connect(username="admin", password="test")
    admin_cursor = admin_connection.cursor()
    reset_and_prepare(admin_cursor)
    common.create_multi_db(admin_cursor, switch, reset_and_prepare)

    common.execute_and_fetch_all(admin_cursor, "REVOKE * ON NODES CONTAINING LABELS * FROM user;")
    common.execute_and_fetch_all(admin_cursor, "REVOKE * ON EDGES OF TYPE * FROM user;")

    user_connection = common.connect(username="user", password="test")
    user_cursor = user_connection.cursor()

    if switch:
        common.switch_db(user_cursor)

    results = common.execute_and_fetch_all(user_cursor, "CREATE (n) RETURN n;")
    assert len(results) == 1

    results = common.execute_and_fetch_all(user_cursor, "CREATE (a), (b), (c) RETURN a, b, c;")
    assert len(results) == 1

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(user_cursor, "CREATE (n:Person) RETURN n;")

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(user_cursor, "CREATE (a)-[r:KNOWS]->(b) RETURN a, r, b;")


@pytest.mark.parametrize("switch", [False, True])
def test_create_on_multiple_matching_with_same_label(switch):
    admin_connection = common.connect(username="admin", password="test")
    admin_cursor = admin_connection.cursor()
    reset_and_prepare(admin_cursor)
    common.create_multi_db(admin_cursor, switch, reset_and_prepare)

    common.execute_and_fetch_all(
        admin_cursor, "GRANT CREATE ON NODES CONTAINING LABELS :Person, :Actor MATCHING EXACTLY TO user;"
    )
    common.execute_and_fetch_all(
        admin_cursor, "GRANT CREATE ON NODES CONTAINING LABELS :Person, :Director MATCHING EXACTLY TO user;"
    )

    user_connection = common.connect(username="user", password="test")
    user_cursor = user_connection.cursor()
    if switch:
        common.switch_db(user_cursor)

    results = common.execute_and_fetch_all(user_cursor, "CREATE (n:Person:Actor) RETURN n;")
    assert len(results) == 1

    results = common.execute_and_fetch_all(user_cursor, "CREATE (n:Person:Director) RETURN n;")
    assert len(results) == 1

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(user_cursor, "CREATE (n:Person:Director:Actor) RETURN n;")

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(user_cursor, "CREATE (n:Director:Actor) RETURN n;")

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(user_cursor, "CREATE (n:Person) RETURN n;")


if __name__ == "__main__":
    common.setup_db()
    sys.exit(pytest.main([__file__, "-rA"]))
