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
from common import connect, execute_and_fetch_all, reset_permissions, DatabaseError

update_property_query = "MATCH (n:update_label) SET n.prop = 'prop' RETURN n;"
update_properties_query = "MATCH (n:update_label) SET n = {prop1: 'prop1', prop2: 10} RETURN n;"
remove_property_query = "MATCH (n:update_label) REMOVE n.prop RETURN n;"


def test_can_read_node_when_given_update_grant():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)
    execute_and_fetch_all(admin_cursor, "GRANT UPDATE ON LABELS :update_label TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    results = execute_and_fetch_all(test_cursor, "MATCH (n:update_label) RETURN n;")

    assert len(results) == 1


def test_can_update_node_when_given_update_grant():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)
    execute_and_fetch_all(admin_cursor, "GRANT UPDATE ON LABELS :update_label TO user;")

    test_cursor = connect(username="user", password="test").cursor()

    update_property_actual = execute_and_fetch_all(test_cursor, update_property_query)
    update_properties_actual = execute_and_fetch_all(test_cursor, update_properties_query)
    remove_property_actual = execute_and_fetch_all(test_cursor, remove_property_query)

    assert len(update_property_actual) == 1
    assert len(update_properties_actual) == 1
    assert len(remove_property_actual) == 1


def test_can_not_update_node_when_given_deny():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)
    execute_and_fetch_all(admin_cursor, "DENY UPDATE ON LABELS :update_label TO user;")

    test_cursor = connect(username="user", password="test").cursor()

    with pytest.raises(DatabaseError):
        execute_and_fetch_all(test_cursor, update_property_query)


def test_can_not_update_node_when_given_nothing():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)

    test_cursor = connect(username="user", password="test").cursor()

    update_property_actual = execute_and_fetch_all(test_cursor, update_property_query)
    update_properties_actual = execute_and_fetch_all(test_cursor, update_properties_query)
    remove_property_actual = execute_and_fetch_all(test_cursor, remove_property_query)

    assert len(update_property_actual) == 0
    assert len(update_properties_actual) == 0
    assert len(remove_property_actual) == 0


def test_can_not_update_node_when_given_read():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)
    execute_and_fetch_all(admin_cursor, "GRANT READ ON LABELS :update_label TO user;")

    test_cursor = connect(username="user", password="test").cursor()

    with pytest.raises(DatabaseError):
        execute_and_fetch_all(test_cursor, update_property_query)


def test_can_not_update_node_when_given_read_globally():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)
    execute_and_fetch_all(admin_cursor, "GRANT READ ON LABELS * TO user;")

    test_cursor = connect(username="user", password="test").cursor()

    with pytest.raises(DatabaseError):
        execute_and_fetch_all(test_cursor, update_property_query)


def test_can_update_node_when_given_update_globally():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)
    execute_and_fetch_all(admin_cursor, "GRANT UPDATE ON LABELS * TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    update_property_actual = execute_and_fetch_all(test_cursor, update_property_query)
    update_properties_actual = execute_and_fetch_all(test_cursor, update_properties_query)
    remove_property_actual = execute_and_fetch_all(test_cursor, remove_property_query)

    assert len(update_property_actual) == 1
    assert len(update_properties_actual) == 1
    assert len(remove_property_actual) == 1


def test_can_update_node_when_given_create_delete_globally():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)
    execute_and_fetch_all(admin_cursor, "GRANT CREATE_DELETE ON LABELS * TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    update_property_actual = execute_and_fetch_all(test_cursor, update_property_query)
    update_properties_actual = execute_and_fetch_all(test_cursor, update_properties_query)
    remove_property_actual = execute_and_fetch_all(test_cursor, remove_property_query)

    assert len(update_property_actual) == 1
    assert len(update_properties_actual) == 1
    assert len(remove_property_actual) == 1


def test_can_update_node_when_given_create_delete():
    admin_cursor = connect(username="admin", password="test").cursor()
    reset_permissions(admin_cursor)
    execute_and_fetch_all(admin_cursor, "GRANT CREATE_DELETE ON LABELS :update_label TO user;")

    test_cursor = connect(username="user", password="test").cursor()
    update_property_actual = execute_and_fetch_all(test_cursor, update_property_query)
    update_properties_actual = execute_and_fetch_all(test_cursor, update_properties_query)
    remove_property_actual = execute_and_fetch_all(test_cursor, remove_property_query)

    assert len(update_property_actual) == 1
    assert len(update_properties_actual) == 1
    assert len(remove_property_actual) == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
