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
from common import connect, execute_and_fetch_all, reset_permissions

get_number_of_vertices_query = "CALL read.number_of_visible_nodes() YIELD nr_of_nodes RETURN nr_of_nodes;"
get_number_of_edges_query = "CALL read.number_of_visible_edges() YIELD nr_of_edges RETURN nr_of_edges;"


def test_can_read_vertex_through_c_api_when_given_grant_on_label():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT READ ON LABELS :read_label TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, get_number_of_vertices_query)

    assert result[0][0] == 1


def test_can_read_vertex_through_c_api_when_given_update_grant_on_label():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT UPDATE ON LABELS :read_label TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, get_number_of_vertices_query)

    assert result[0][0] == 1


def test_can_read_vertex_through_c_api_when_given_create_delete_grant_on_label():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT CREATE_DELETE ON LABELS :read_label TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, get_number_of_vertices_query)

    assert result[0][0] == 1


def test_can_not_read_vertex_through_c_api_when_given_nothing():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, get_number_of_vertices_query)

    assert result[0][0] == 0


def test_can_not_read_vertex_through_c_api_when_given_deny_on_label():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "DENY READ ON LABELS :read_label TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, get_number_of_vertices_query)

    assert result[0][0] == 0


def test_can_read_partial_vertices_through_c_api_when_given_global_read_but_deny_on_label():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "DENY READ ON LABELS :read_label TO user;")
    execute_and_fetch_all(admin_cursor, "GRANT READ ON LABELS * TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, get_number_of_vertices_query)

    assert result[0][0] == 2


def test_can_read_partial_vertices_through_c_api_when_given_global_update_but_deny_on_label():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "DENY READ ON LABELS :read_label TO user;")
    execute_and_fetch_all(admin_cursor, "GRANT UPDATE ON LABELS * TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, get_number_of_vertices_query)

    assert result[0][0] == 2


def test_can_read_partial_vertices_through_c_api_when_given_global_create_delete_but_deny_on_label():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "DENY READ ON LABELS :read_label TO user;")
    execute_and_fetch_all(admin_cursor, "GRANT CREATE_DELETE ON LABELS * TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, get_number_of_vertices_query)

    assert result[0][0] == 2


def test_can_read_edge_through_c_api_when_given_grant_on_edge_type():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT READ ON LABELS :read_label_1, :read_label_2 TO user;")
    execute_and_fetch_all(admin_cursor, "GRANT READ ON EDGE_TYPES :read_edge_type TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, get_number_of_edges_query)

    assert result[0][0] == 1


def test_can_not_read_edge_through_c_api_when_given_deny_on_edge_type():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT READ ON LABELS :read_label_1, :read_label_2 TO user;")
    execute_and_fetch_all(admin_cursor, "DENY READ ON EDGE_TYPES :read_edge_type TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, get_number_of_edges_query)

    assert result[0][0] == 0


def test_can_read_edge_through_c_api_when_given_grant_on_edge_type():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT READ ON LABELS :read_label_1, :read_label_2 TO user;")
    execute_and_fetch_all(admin_cursor, "GRANT READ ON EDGE_TYPES :read_edge_type TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, get_number_of_edges_query)

    assert result[0][0] == 1


def test_can_read_edge_through_c_api_when_given_update_on_edge_type():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT READ ON LABELS :read_label_1, :read_label_2 TO user;")
    execute_and_fetch_all(admin_cursor, "GRANT UPDATE ON EDGE_TYPES :read_edge_type TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, get_number_of_edges_query)

    assert result[0][0] == 1


def test_can_read_edge_through_c_api_when_given_create_delete_on_edge_type():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT READ ON LABELS :read_label_1, :read_label_2 TO user;")
    execute_and_fetch_all(admin_cursor, "GRANT CREATE_DELETE ON EDGE_TYPES :read_edge_type TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, get_number_of_edges_query)

    assert result[0][0] == 1


def test_can_not_read_edge_through_c_api_when_given_read_global_but_deny_on_edge_type():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT READ ON LABELS :read_label_1, :read_label_2 TO user;")
    execute_and_fetch_all(admin_cursor, "DENY READ ON EDGE_TYPES :read_edge_type TO user;")
    execute_and_fetch_all(admin_cursor, "GRANT READ ON EDGE_TYPES * TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, get_number_of_edges_query)

    assert result[0][0] == 0


def test_can_not_read_edge_through_c_api_when_given_update_global_but_deny_on_edge_type():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT READ ON LABELS :read_label_1, :read_label_2 TO user;")
    execute_and_fetch_all(admin_cursor, "DENY READ ON EDGE_TYPES :read_edge_type TO user;")
    execute_and_fetch_all(admin_cursor, "GRANT UPDATE ON EDGE_TYPES * TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, get_number_of_edges_query)

    assert result[0][0] == 0


def test_can_not_read_edge_through_c_api_when_given_create_delete_global_but_deny_on_edge_type():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)

    execute_and_fetch_all(admin_cursor, "GRANT READ ON LABELS :read_label_1, :read_label_2 TO user;")
    execute_and_fetch_all(admin_cursor, "DENY READ ON EDGE_TYPES :read_edge_type TO user;")
    execute_and_fetch_all(admin_cursor, "GRANT CREATE_DELETE ON EDGE_TYPES * TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    result = execute_and_fetch_all(test_cursor, get_number_of_edges_query)

    assert result[0][0] == 0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
