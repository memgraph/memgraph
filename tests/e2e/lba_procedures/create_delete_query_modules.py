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
from common import *

AUTHORIZATION_ERROR_IDENTIFIER = "AuthorizationError"

create_vertex_query = "CALL create_delete.create_vertex() YIELD created_node RETURN labels(created_node);"
remove_label_vertex_query = "CALL create_delete.remove_label('create_delete_label') YIELD node RETURN labels(node);"
set_label_vertex_query = "CALL create_delete.set_label('new_create_delete_label') YIELD node RETURN labels(node);"
create_edge_query = "MATCH (n:create_delete_label_1), (m:create_delete_label_2) CALL create_delete.create_edge(n, m) YIELD nr_of_edges RETURN nr_of_edges;"
delete_edge_query = "CALL create_delete.delete_edge() YIELD * RETURN *;"


@pytest.mark.parametrize("switch", [False, True])
def test_can_not_create_vertex_when_given_nothing(switch):
    admin_cursor = connect(username="admin", password="test").cursor()
    create_multi_db(admin_cursor)
    if switch:
        switch_db(admin_cursor)
    reset_create_delete_permissions(admin_cursor)

    test_cursor = connect(username="user", password="test").cursor()
    if switch:
        switch_db(test_cursor)

    with pytest.raises(mgclient.DatabaseError, match=AUTHORIZATION_ERROR_IDENTIFIER):
        execute_and_fetch_all(test_cursor, create_vertex_query)


@pytest.mark.parametrize("switch", [False, True])
def test_can_create_vertex_when_given_global_create(switch):
    admin_cursor = connect(username="admin", password="test").cursor()
    create_multi_db(admin_cursor)
    if switch:
        switch_db(admin_cursor)
    reset_create_delete_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT CREATE ON NODES CONTAINING LABELS * TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    if switch:
        switch_db(test_cursor)

    result = execute_and_fetch_all(test_cursor, create_vertex_query)

    len(result[0][0]) == 1


@pytest.mark.parametrize("switch", [False, True])
def test_can_not_create_vertex_when_given_global_read(switch):
    admin_cursor = connect(username="admin", password="test").cursor()
    create_multi_db(admin_cursor)
    if switch:
        switch_db(admin_cursor)
    reset_create_delete_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT READ ON NODES CONTAINING LABELS * TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    if switch:
        switch_db(test_cursor)

    with pytest.raises(mgclient.DatabaseError, match=AUTHORIZATION_ERROR_IDENTIFIER):
        execute_and_fetch_all(test_cursor, create_vertex_query)


@pytest.mark.parametrize("switch", [False, True])
def test_can_not_create_vertex_when_given_global_update(switch):
    admin_cursor = connect(username="admin", password="test").cursor()
    create_multi_db(admin_cursor)
    if switch:
        switch_db(admin_cursor)
    reset_create_delete_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT UPDATE ON NODES CONTAINING LABELS :create_delete_label TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    if switch:
        switch_db(test_cursor)

    with pytest.raises(mgclient.DatabaseError, match=AUTHORIZATION_ERROR_IDENTIFIER):
        execute_and_fetch_all(test_cursor, create_vertex_query)


@pytest.mark.parametrize("switch", [False, True])
def test_can_add_vertex_label_when_given_create(switch):
    admin_cursor = connect(username="admin", password="test").cursor()
    create_multi_db(admin_cursor)
    if switch:
        switch_db(admin_cursor)
    reset_create_delete_permissions(admin_cursor)

    execute_and_fetch_all(
        admin_cursor,
        "GRANT CREATE ON NODES CONTAINING LABELS :new_create_delete_label, READ, UPDATE ON NODES CONTAINING LABELS: create_delete_label TO user;",
    )

    test_cursor = connect(username="user", password="test").cursor()
    if switch:
        switch_db(test_cursor)
    result = execute_and_fetch_all(test_cursor, set_label_vertex_query)

    assert "create_delete_label" in result[0][0]
    assert "new_create_delete_label" in result[0][0]


@pytest.mark.parametrize("switch", [False, True])
def test_can_not_add_vertex_label_when_given_update(switch):
    admin_cursor = connect(username="admin", password="test").cursor()
    create_multi_db(admin_cursor)
    if switch:
        switch_db(admin_cursor)
    reset_create_delete_permissions(admin_cursor)

    execute_and_fetch_all(
        admin_cursor, "GRANT UPDATE ON NODES CONTAINING LABELS :new_create_delete_label, :create_delete_label TO user;"
    )

    test_cursor = connect(username="user", password="test").cursor()
    if switch:
        switch_db(test_cursor)
    with pytest.raises(mgclient.DatabaseError, match=AUTHORIZATION_ERROR_IDENTIFIER):
        execute_and_fetch_all(test_cursor, set_label_vertex_query)


@pytest.mark.parametrize("switch", [False, True])
def test_can_not_add_vertex_label_when_given_read(switch):
    admin_cursor = connect(username="admin", password="test").cursor()
    create_multi_db(admin_cursor)
    if switch:
        switch_db(admin_cursor)
    reset_create_delete_permissions(admin_cursor)

    execute_and_fetch_all(
        admin_cursor,
        "GRANT READ ON NODES CONTAINING LABELS :new_create_delete_label, UPDATE ON NODES CONTAINING LABELS :create_delete_label TO user;",
    )

    test_cursor = connect(username="user", password="test").cursor()
    if switch:
        switch_db(test_cursor)
    with pytest.raises(mgclient.DatabaseError, match=AUTHORIZATION_ERROR_IDENTIFIER):
        execute_and_fetch_all(test_cursor, set_label_vertex_query)


@pytest.mark.parametrize("switch", [False, True])
def test_can_remove_vertex_label_when_given_delete(switch):
    admin_cursor = connect(username="admin", password="test").cursor()
    create_multi_db(admin_cursor)
    if switch:
        switch_db(admin_cursor)
    reset_create_delete_permissions(admin_cursor)

    execute_and_fetch_all(
        admin_cursor,
        "GRANT READ ON NODES CONTAINING LABELS :create_delete_label, UPDATE ON NODES CONTAINING LABELS :create_delete_label, DELETE ON NODES CONTAINING LABELS :create_delete_label TO user;",
    )

    test_cursor = connect(username="user", password="test").cursor()
    if switch:
        switch_db(test_cursor)
    result = execute_and_fetch_all(test_cursor, remove_label_vertex_query)

    assert result[0][0] != ":create_delete_label"


@pytest.mark.parametrize("switch", [False, True])
def test_can_remove_vertex_label_when_given_global_delete(switch):
    admin_cursor = connect(username="admin", password="test").cursor()
    create_multi_db(admin_cursor)
    if switch:
        switch_db(admin_cursor)
    reset_create_delete_permissions(admin_cursor)

    execute_and_fetch_all(
        admin_cursor,
        "GRANT READ ON NODES CONTAINING LABELS *, UPDATE ON NODES CONTAINING LABELS *, DELETE ON NODES CONTAINING LABELS * TO user;",
    )

    test_cursor = connect(username="user", password="test").cursor()
    if switch:
        switch_db(test_cursor)
    result = execute_and_fetch_all(test_cursor, remove_label_vertex_query)

    assert result[0][0] != ":create_delete_label"


@pytest.mark.parametrize("switch", [False, True])
def test_can_not_remove_vertex_label_when_given_update(switch):
    admin_cursor = connect(username="admin", password="test").cursor()
    create_multi_db(admin_cursor)
    if switch:
        switch_db(admin_cursor)
    reset_create_delete_permissions(admin_cursor)

    execute_and_fetch_all(
        admin_cursor,
        "GRANT READ ON NODES CONTAINING LABELS :create_delete_label, UPDATE ON NODES CONTAINING LABELS :create_delete_label TO user;",
    )

    test_cursor = connect(username="user", password="test").cursor()
    if switch:
        switch_db(test_cursor)

    with pytest.raises(mgclient.DatabaseError, match=AUTHORIZATION_ERROR_IDENTIFIER):
        execute_and_fetch_all(test_cursor, remove_label_vertex_query)


@pytest.mark.parametrize("switch", [False, True])
def test_can_not_remove_vertex_label_when_given_global_update(switch):
    admin_cursor = connect(username="admin", password="test").cursor()
    create_multi_db(admin_cursor)
    if switch:
        switch_db(admin_cursor)
    reset_create_delete_permissions(admin_cursor)

    execute_and_fetch_all(
        admin_cursor, "GRANT READ ON NODES CONTAINING LABELS *, UPDATE ON NODES CONTAINING LABELS * TO user;"
    )

    test_cursor = connect(username="user", password="test").cursor()
    if switch:
        switch_db(test_cursor)

    with pytest.raises(mgclient.DatabaseError, match=AUTHORIZATION_ERROR_IDENTIFIER):
        execute_and_fetch_all(test_cursor, remove_label_vertex_query)


@pytest.mark.parametrize("switch", [False, True])
def test_can_not_remove_vertex_label_when_given_read(switch):
    admin_cursor = connect(username="admin", password="test").cursor()
    create_multi_db(admin_cursor)
    if switch:
        switch_db(admin_cursor)
    reset_create_delete_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT READ ON NODES CONTAINING LABELS :create_delete_label TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    if switch:
        switch_db(test_cursor)

    with pytest.raises(mgclient.DatabaseError, match=AUTHORIZATION_ERROR_IDENTIFIER):
        execute_and_fetch_all(test_cursor, remove_label_vertex_query)


@pytest.mark.parametrize("switch", [False, True])
def test_can_not_remove_vertex_label_when_given_global_read(switch):
    admin_cursor = connect(username="admin", password="test").cursor()
    create_multi_db(admin_cursor)
    if switch:
        switch_db(admin_cursor)
    reset_create_delete_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT READ ON NODES CONTAINING LABELS * TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    if switch:
        switch_db(test_cursor)

    with pytest.raises(mgclient.DatabaseError, match=AUTHORIZATION_ERROR_IDENTIFIER):
        execute_and_fetch_all(test_cursor, remove_label_vertex_query)


@pytest.mark.parametrize("switch", [False, True])
def test_can_not_create_edge_when_given_nothing(switch):
    admin_cursor = connect(username="admin", password="test").cursor()
    create_multi_db(admin_cursor)
    if switch:
        switch_db(admin_cursor)
    reset_create_delete_permissions(admin_cursor)

    test_cursor = connect(username="user", password="test").cursor()
    if switch:
        switch_db(test_cursor)

    with pytest.raises(mgclient.DatabaseError, match=AUTHORIZATION_ERROR_IDENTIFIER):
        execute_and_fetch_all(test_cursor, create_edge_query)


@pytest.mark.parametrize("switch", [False, True])
def test_can_not_create_edge_when_given_read(switch):
    admin_cursor = connect(username="admin", password="test").cursor()
    create_multi_db(admin_cursor)
    if switch:
        switch_db(admin_cursor)
    reset_create_delete_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT READ ON EDGES OF TYPE :new_create_delete_edge_type TO user")

    test_cursor = connect(username="user", password="test").cursor()
    if switch:
        switch_db(test_cursor)

    with pytest.raises(mgclient.DatabaseError, match=AUTHORIZATION_ERROR_IDENTIFIER):
        execute_and_fetch_all(test_cursor, create_edge_query)


@pytest.mark.parametrize("switch", [False, True])
def test_can_not_create_edge_when_given_update(switch):
    admin_cursor = connect(username="admin", password="test").cursor()
    create_multi_db(admin_cursor)
    if switch:
        switch_db(admin_cursor)
    reset_create_delete_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT UPDATE ON EDGES OF TYPE :new_create_delete_edge_type TO user")

    test_cursor = connect(username="user", password="test").cursor()
    if switch:
        switch_db(test_cursor)

    with pytest.raises(mgclient.DatabaseError, match=AUTHORIZATION_ERROR_IDENTIFIER):
        execute_and_fetch_all(test_cursor, create_edge_query)


@pytest.mark.parametrize("switch", [False, True])
def test_can_create_edge_when_given_create(switch):
    admin_cursor = connect(username="admin", password="test").cursor()
    create_multi_db(admin_cursor)
    if switch:
        switch_db(admin_cursor)
    reset_create_delete_permissions(admin_cursor)

    execute_and_fetch_all(
        admin_cursor,
        "GRANT READ ON NODES CONTAINING LABELS :create_delete_label_1, :create_delete_label_2, CREATE ON EDGES OF TYPE :new_create_delete_edge_type, READ ON EDGES OF TYPE :new_create_delete_edge_type TO user",
    )

    test_cursor = connect(username="user", password="test").cursor()
    if switch:
        switch_db(test_cursor)

    no_of_edges = execute_and_fetch_all(test_cursor, create_edge_query)

    assert no_of_edges[0][0] == 2


@pytest.mark.parametrize("switch", [False, True])
def test_can_not_delete_edge_when_given_nothing(switch):
    admin_cursor = connect(username="admin", password="test").cursor()
    create_multi_db(admin_cursor)
    if switch:
        switch_db(admin_cursor)
    reset_create_delete_permissions(admin_cursor)

    test_cursor = connect(username="user", password="test").cursor()
    if switch:
        switch_db(test_cursor)

    with pytest.raises(mgclient.DatabaseError, match=AUTHORIZATION_ERROR_IDENTIFIER):
        execute_and_fetch_all(test_cursor, delete_edge_query)


@pytest.mark.parametrize("switch", [False, True])
def test_can_not_delete_edge_when_given_read(switch):
    admin_cursor = connect(username="admin", password="test").cursor()
    create_multi_db(admin_cursor)
    if switch:
        switch_db(admin_cursor)
    reset_create_delete_permissions(admin_cursor)

    execute_and_fetch_all(
        admin_cursor,
        "GRANT READ ON EDGES OF TYPE :create_delete_edge_type TO user",
    )

    test_cursor = connect(username="user", password="test").cursor()
    if switch:
        switch_db(test_cursor)

    with pytest.raises(mgclient.DatabaseError, match=AUTHORIZATION_ERROR_IDENTIFIER):
        execute_and_fetch_all(test_cursor, delete_edge_query)


@pytest.mark.parametrize("switch", [False, True])
def test_can_not_delete_edge_when_given_update(switch):
    admin_cursor = connect(username="admin", password="test").cursor()
    create_multi_db(admin_cursor)
    if switch:
        switch_db(admin_cursor)
    reset_create_delete_permissions(admin_cursor)

    execute_and_fetch_all(
        admin_cursor,
        "GRANT READ ON EDGES OF TYPE :create_delete_edge_type, UPDATE ON EDGES OF TYPE :create_delete_edge_type TO user",
    )

    test_cursor = connect(username="user", password="test").cursor()
    if switch:
        switch_db(test_cursor)

    with pytest.raises(mgclient.DatabaseError, match=AUTHORIZATION_ERROR_IDENTIFIER):
        execute_and_fetch_all(test_cursor, delete_edge_query)


@pytest.mark.parametrize("switch", [False, True])
def test_can_delete_edge_when_given_delete(switch):
    admin_cursor = connect(username="admin", password="test").cursor()
    create_multi_db(admin_cursor)
    if switch:
        switch_db(admin_cursor)
    reset_create_delete_permissions(admin_cursor)

    execute_and_fetch_all(
        admin_cursor,
        "GRANT DELETE ON EDGES OF TYPE :create_delete_edge_type TO user",
    )

    test_cursor = connect(username="user", password="test").cursor()
    if switch:
        switch_db(test_cursor)

    no_of_edges = execute_and_fetch_all(test_cursor, delete_edge_query)

    assert no_of_edges[0][0] == 0


@pytest.mark.parametrize("switch", [False, True])
def test_create_multiple_labels_matching_exactly(switch):
    from mgclient import DatabaseError

    admin_cursor = connect(username="admin", password="test").cursor()
    create_multi_db(admin_cursor)
    if switch:
        switch_db(admin_cursor)
    reset_create_delete_permissions(admin_cursor)

    execute_and_fetch_all(
        admin_cursor, "GRANT CREATE ON NODES CONTAINING LABELS :Person, :Employee MATCHING EXACTLY TO user;"
    )

    test_cursor = connect(username="user", password="test").cursor()
    if switch:
        switch_db(test_cursor)

    results = execute_and_fetch_all(test_cursor, "CREATE (n:Person:Employee) RETURN n;")
    assert len(results) == 1

    with pytest.raises(DatabaseError):
        execute_and_fetch_all(test_cursor, "CREATE (n:Person) RETURN n;")

    with pytest.raises(DatabaseError):
        execute_and_fetch_all(test_cursor, "CREATE (n:Employee) RETURN n;")

    with pytest.raises(DatabaseError):
        execute_and_fetch_all(test_cursor, "CREATE (n:Person:Employee:Manager) RETURN n;")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
