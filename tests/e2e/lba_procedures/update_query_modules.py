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

import pytest
import sys
from common import (
    connect,
    execute_and_fetch_all,
    reset_update_permissions,
)

# The idea here is to try to set the property to int value of 2
# Initial value is set to int value of 1

set_vertex_property_query = "MATCH (n:update_label) CALL update.set_property(n) YIELD * RETURN n.prop;"

set_edge_property_query = "MATCH (n:update_label_1)-[r:update_edge_type]->(m:update_label_2) CALL update.set_property(r) YIELD * RETURN r.prop;"


def test_can_not_update_vertex_when_given_read():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_update_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT READ ON LABELS :update_label TO user;")

    test_cursor = connect(username="user", password="test").cursor()

    result = execute_and_fetch_all(test_cursor, set_vertex_property_query)

    assert result[0][0] == 1


def test_can_update_vertex_when_given_update_grant_on_label():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_update_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT UPDATE ON LABELS :update_label TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, set_vertex_property_query)

    assert result[0][0] == 2


def test_can_update_vertex_when_given_create_delete_grant_on_label():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_update_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT CREATE_DELETE ON LABELS :update_label TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, set_vertex_property_query)

    assert result[0][0] == 2


def test_can_update_vertex_when_given_update_global_grant_on_label():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_update_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT UPDATE ON LABELS * TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, set_vertex_property_query)

    assert result[0][0] == 2


def test_can_update_vertex_when_given_create_delete_global_grant_on_label():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_update_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT CREATE_DELETE ON LABELS * TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, set_vertex_property_query)

    assert result[0][0] == 2


def test_can_not_update_vertex_when_denied_update_and_granted_global_update_on_label():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_update_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "DENY UPDATE ON LABELS :update_label TO user;")
    execute_and_fetch_all(admin_cursor, "GRANT UPDATE ON LABELS * TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, set_vertex_property_query)

    assert result[0][0] == 1


def test_can_not_update_vertex_when_denied_update_and_granted_global_create_delete_on_label():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_update_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "DENY UPDATE ON LABELS :update_label TO user;")
    execute_and_fetch_all(admin_cursor, "GRANT CREATE_DELETE ON LABELS * TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, set_vertex_property_query)

    assert result[0][0] == 1


def test_can_update_edge_when_given_update_grant_on_edge_type():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_update_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT READ ON LABELS :update_label_1 TO user;")
    execute_and_fetch_all(admin_cursor, "GRANT READ ON LABELS :update_label_2 TO user;")
    execute_and_fetch_all(admin_cursor, "GRANT UPDATE ON EDGE_TYPES :update_edge_type TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, set_edge_property_query)

    assert result[0][0] == 2


def test_can_not_update_edge_when_given_read_grant_on_edge_type():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_update_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT READ ON LABELS :update_label_1 TO user;")
    execute_and_fetch_all(admin_cursor, "GRANT READ ON LABELS :update_label_2 TO user;")
    execute_and_fetch_all(admin_cursor, "GRANT READ ON EDGE_TYPES :update_edge_type TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, set_edge_property_query)

    assert result[0][0] == 1


def test_can_update_edge_when_given_create_delete_grant_on_edge_type():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_update_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT READ ON LABELS :update_label_1 TO user;")
    execute_and_fetch_all(admin_cursor, "GRANT READ ON LABELS :update_label_2 TO user;")
    execute_and_fetch_all(admin_cursor, "GRANT CREATE_DELETE ON EDGE_TYPES :update_edge_type TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, set_edge_property_query)

    assert result[0][0] == 2


def test_can_not_update_edge_when_denied_update_edge_type_but_granted_global_update_on_edge_type():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_update_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT READ ON LABELS :update_label_1 TO user;")
    execute_and_fetch_all(admin_cursor, "GRANT READ ON LABELS :update_label_2 TO user;")
    execute_and_fetch_all(admin_cursor, "DENY UPDATE ON EDGE_TYPES :update_edge_type TO user;")
    execute_and_fetch_all(admin_cursor, "DENY UPDATE ON EDGE_TYPES * TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, set_edge_property_query)

    assert result[0][0] == 1


def test_can_not_update_edge_when_denied_update_edge_type_but_granted_global_create_delete_on_edge_type():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_update_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT READ ON LABELS :update_label_1 TO user;")
    execute_and_fetch_all(admin_cursor, "GRANT READ ON LABELS :update_label_2 TO user;")
    execute_and_fetch_all(admin_cursor, "DENY UPDATE ON EDGE_TYPES :update_edge_type TO user;")
    execute_and_fetch_all(admin_cursor, "DENY CREATE_DELETE ON EDGE_TYPES * TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, set_edge_property_query)

    assert result[0][0] == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
