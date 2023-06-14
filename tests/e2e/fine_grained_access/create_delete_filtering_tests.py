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

import common
import pytest
from mgclient import DatabaseError


def test_create_node_all_labels_granted():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON LABELS * TO user;")
    results = common.execute_and_fetch_all(user_connection.cursor(), "CREATE (n:label1) RETURN n;")

    assert len(results) == 1


def test_create_node_all_labels_denied():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON LABELS * TO user;")

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(user_connection.cursor(), "CREATE (n:label1) RETURN n;")


def test_create_node_specific_label_granted():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON LABELS :label1 TO user;")
    results = common.execute_and_fetch_all(user_connection.cursor(), "CREATE (n:label1) RETURN n;")

    assert len(results) == 1


def test_create_node_specific_label_denied():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON LABELS :label1 TO user;")

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(user_connection.cursor(), "CREATE (n:label1) RETURN n;")


def test_delete_node_all_labels_granted():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON LABELS * TO user;")
    common.execute_and_fetch_all(user_connection.cursor(), "MATCH (n:test_delete) DELETE n;")

    results = common.execute_and_fetch_all(user_connection.cursor(), "MATCH (n:test_delete) RETURN n;")

    assert len(results) == 0


def test_delete_node_all_labels_denied():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON LABELS * TO user;")

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(user_connection.cursor(), "MATCH (n:test_delete) DELETE n")


def test_delete_node_specific_label_granted():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON LABELS :test_delete TO user;")
    results = common.execute_and_fetch_all(user_connection.cursor(), "MATCH (n:test_delete) DELETE n;")

    results = common.execute_and_fetch_all(admin_connection.cursor(), "MATCH (n:test_delete) RETURN n;")

    assert len(results) == 0


def test_delete_node_specific_label_denied():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON LABELS :test_delete TO user;")

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(user_connection.cursor(), "MATCH (n:test_delete) DELETE n;")


def test_create_edge_all_labels_all_edge_types_granted():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON EDGE_TYPES * TO user;")

    results = common.execute_and_fetch_all(
        user_connection.cursor(),
        "CREATE (n:label1)-[r:edge_type]->(m:label2) RETURN n,r,m;",
    )

    assert len(results) == 1


def test_create_edge_all_labels_all_edge_types_denied():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON EDGE_TYPES * TO user;")

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "CREATE (n:label1)-[r:edge_type]->(m:label2) RETURN n,r,m;",
        )


def test_create_edge_all_labels_denied_all_edge_types_granted():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON EDGE_TYPES * TO user;")

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "CREATE (n:label1)-[r:edge_type]->(m:label2) RETURN n,r,m;",
        )


def test_create_edge_all_labels_granted_all_edge_types_denied():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON EDGE_TYPES * TO user;")

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "CREATE (n:label1)-[r:edge_type]->(m:label2) RETURN n,r,m;",
        )


def test_create_edge_all_labels_granted_specific_edge_types_denied():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON LABELS * TO user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(),
        "GRANT UPDATE ON EDGE_TYPES :edge_type TO user;",
    )

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "CREATE (n:label1)-[r:edge_type]->(m:label2) RETURN n,r,m;",
        )


def test_create_edge_first_node_label_granted():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON LABELS :label1 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON LABELS :label2 TO user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(),
        "GRANT CREATE_DELETE ON EDGE_TYPES :edge_type TO user;",
    )

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "CREATE (n:label1)-[r:edge_type]->(m:label2) RETURN n,r,m;",
        )


def test_create_edge_second_node_label_granted():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON LABELS :label2 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON LABELS :label1 TO user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(),
        "GRANT CREATE_DELETE ON EDGE_TYPES :edge_type TO user;",
    )

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "CREATE (n:label1)-[r:edge_type]->(m:label2) RETURN n,r,m;",
        )


def test_delete_edge_all_labels_denied_all_edge_types_granted():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON EDGE_TYPES * TO user;")

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "MATCH (n:test_delete_1)-[r:edge_type_delete]->(m:test_delete_2) DELETE r",
        )


def test_delete_edge_all_labels_granted_all_edge_types_denied():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON EDGE_TYPES * TO user;")

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "MATCH (n:test_delete_1)-[r:edge_type_delete]->(m:test_delete_2) DELETE r",
        )


def test_delete_edge_all_labels_granted_specific_edge_types_denied():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON LABELS * TO user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(),
        "GRANT UPDATE ON EDGE_TYPES :edge_type_delete TO user;",
    )

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "MATCH (n:test_delete_1)-[r:edge_type_delete]->(m:test_delete_2) DELETE r",
        )


def test_delete_edge_first_node_label_granted():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON LABELS :test_delete_1 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS :test_delete_2 TO user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(),
        "GRANT CREATE_DELETE ON EDGE_TYPES :edge_type_delete TO user;",
    )

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "MATCH (n:test_delete_1)-[r:edge_type_delete]->(m:test_delete_2) DELETE r",
        )


def test_delete_edge_second_node_label_granted():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON LABELS :test_delete_2 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS :test_delete_1 TO user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(),
        "GRANT CREATE_DELETE ON EDGE_TYPES :edge_type_delete TO user;",
    )

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "MATCH (n:test_delete_1)-[r:edge_type_delete]->(m:test_delete_2) DELETE r",
        )


def test_delete_node_with_edge_label_denied():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(
        admin_connection.cursor(),
        "GRANT UPDATE ON LABELS :test_delete_1 TO user;",
    )

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(user_connection.cursor(), "MATCH (n) DETACH DELETE n;")


def test_delete_node_with_edge_label_granted():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(
        admin_connection.cursor(),
        "GRANT CREATE_DELETE ON LABELS :test_delete_1 TO user;",
    )

    common.execute_and_fetch_all(user_connection.cursor(), "MATCH (n) DETACH DELETE n;")

    results = common.execute_and_fetch_all(admin_connection.cursor(), "MATCH (n:test_delete_1) RETURN n;")

    assert len(results) == 0


def test_merge_node_all_labels_granted():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON LABELS * TO user;")
    results = common.execute_and_fetch_all(user_connection.cursor(), "MERGE (n:label1) RETURN n;")

    assert len(results) == 1


def test_merge_node_all_labels_denied():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON LABELS * TO user;")

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(user_connection.cursor(), "MERGE (n:label1) RETURN n;")


def test_merge_node_specific_label_granted():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON LABELS :label1 TO user;")
    results = common.execute_and_fetch_all(user_connection.cursor(), "MERGE (n:label1) RETURN n;")

    assert len(results) == 1


def test_merge_node_specific_label_denied():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON LABELS :label1 TO user;")

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(user_connection.cursor(), "MERGE (n:label1) RETURN n;")


def test_merge_edge_all_labels_all_edge_types_granted():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON EDGE_TYPES * TO user;")
    results = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MERGE (n:label1)-[r:edge_type]->(m:label2) RETURN n,r,m;",
    )

    assert len(results) == 1


def test_merge_edge_all_labels_all_edge_types_denied():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON EDGE_TYPES * TO user;")

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "MERGE (n:label1)-[r:edge_type]->(m:label2) RETURN n,r,m;",
        )


def test_merge_edge_all_labels_denied_all_edge_types_granted():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON EDGE_TYPES * TO user;")

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "MERGE (n:label1)-[r:edge_type]->(m:label2) RETURN n,r,m;",
        )


def test_merge_edge_all_labels_granted_all_edge_types_denied():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON EDGE_TYPES * TO user;")

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "MERGE (n:label1)-[r:edge_type]->(m:label2) RETURN n,r,m;",
        )


def test_merge_edge_all_labels_granted_specific_edge_types_denied():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON LABELS * TO user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(),
        "GRANT UPDATE ON EDGE_TYPES :edge_type TO user;",
    )

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "MERGE (n:label1)-[r:edge_type]->(m:label2) RETURN n,r,m;",
        )


def test_merge_edge_first_node_label_granted():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON LABELS :label1 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON LABELS :label2 TO user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(),
        "GRANT CREATE_DELETE ON EDGE_TYPES :edge_type TO user;",
    )

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "MERGE (n:label1)-[r:edge_type]->(m:label2) RETURN n,r,m;",
        )


def test_merge_edge_second_node_label_granted():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON LABELS :label2 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON LABELS :label1 TO user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(),
        "GRANT CREATE_DELETE ON EDGE_TYPES :edge_type TO user;",
    )

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(
            user_connection.cursor(),
            "MERGE (n:label1)-[r:edge_type]->(m:label2) RETURN n,r,m;",
        )


def test_set_label_when_label_granted():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON LABELS :update_label_2 TO user;")

    common.execute_and_fetch_all(user_connection.cursor(), "MATCH (p:test_delete) SET p:update_label_2;")


def test_set_label_when_label_denied():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")

    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON LABELS :update_label_2 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS :test_delete TO user;")

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(user_connection.cursor(), "MATCH (p:test_delete) SET p:update_label_2;")


def test_remove_label_when_label_granted():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")

    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON LABELS :test_delete TO user;")

    common.execute_and_fetch_all(user_connection.cursor(), "MATCH (p:test_delete) REMOVE p:test_delete;")


def test_remove_label_when_label_denied():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")

    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT UPDATE ON LABELS :update_label_2 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS :test_delete TO user;")

    with pytest.raises(DatabaseError):
        common.execute_and_fetch_all(user_connection.cursor(), "MATCH (p:test_delete) REMOVE p:test_delete;")


def test_merge_nodes_pass_when_having_create_delete():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")

    common.reset_and_prepare(admin_connection.cursor())
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT CREATE_DELETE ON EDGE_TYPES * TO user;")

    results = common.execute_and_fetch_all(
        user_connection.cursor(),
        "UNWIND [{id: '1', lat: 10, lng: 10}, {id: '2', lat: 10, lng: 10}, {id: '3', lat: 10, lng: 10}] AS row MERGE (o:Location {id: row.id}) RETURN o;",
    )

    assert len(results) == 3


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
