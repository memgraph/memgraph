# Copyright 2026 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

"""
Tests all 16 combinations of {CREATE, READ, UPDATE, DELETE} permissions
against each operation type to verify exact permission requirements.
"""

import sys
from itertools import combinations

import pytest
from common import connect, execute_and_fetch_all
from mgclient import DatabaseError

ALL_PERMISSIONS = ["CREATE", "READ", "UPDATE", "DELETE"]


def generate_permission_combinations():
    result = [set()]
    for r in range(1, len(ALL_PERMISSIONS) + 1):
        for combo in combinations(ALL_PERMISSIONS, r):
            result.append(set(combo))
    return result


PERMISSION_COMBINATIONS = generate_permission_combinations()


OPERATIONS = [
    {
        "name": "create_vertex",
        "setup": None,
        "query": "CREATE (:Target)",
        "required_on_target": {"CREATE"},
        "required_on_existing": set(),
        "expect_empty_match": False,
    },
    {
        "name": "add_label",
        "setup": "CREATE (:Existing)",
        "query": "MATCH (n:Existing) SET n:Target",
        "required_on_target": {"CREATE"},
        "required_on_existing": {"READ", "UPDATE"},
        "expect_empty_match": False,
    },
    {
        "name": "remove_label",
        "setup": "CREATE (:Target)",
        "query": "MATCH (n:Target) REMOVE n:Target",
        "required_on_target": {"READ", "UPDATE", "DELETE"},
        "required_on_existing": set(),
        "expect_empty_match": True,
    },
    {
        "name": "set_property",
        "setup": "CREATE (:Target)",
        "query": "MATCH (n:Target) SET n.prop = 1",
        "required_on_target": {"READ", "UPDATE"},
        "required_on_existing": set(),
        "expect_empty_match": True,
    },
    {
        "name": "delete_vertex",
        "setup": "CREATE (:Target)",
        "query": "MATCH (n:Target) DELETE n",
        "required_on_target": {"READ", "DELETE"},
        "required_on_existing": set(),
        "expect_empty_match": True,
    },
]


def get_admin_cursor():
    return connect(username="admin", password="test").cursor()


def get_user_cursor():
    return connect(username="user", password="test").cursor()


def reset_and_grant(admin_cursor, permissions_on_target, permissions_on_existing):
    execute_and_fetch_all(admin_cursor, "REVOKE * ON NODES CONTAINING LABELS * FROM user;")
    execute_and_fetch_all(admin_cursor, "REVOKE * ON EDGES OF TYPE * FROM user;")
    execute_and_fetch_all(admin_cursor, "MATCH (n) DETACH DELETE n;")

    if permissions_on_target:
        perms = ", ".join(permissions_on_target)
        execute_and_fetch_all(admin_cursor, f"GRANT {perms} ON NODES CONTAINING LABELS :Target TO user;")

    if permissions_on_existing:
        perms = ", ".join(permissions_on_existing)
        execute_and_fetch_all(admin_cursor, f"GRANT {perms} ON NODES CONTAINING LABELS :Existing TO user;")


def expect_success(
    granted_on_target, granted_on_existing, required_on_target, required_on_existing, expect_empty_match
):
    if expect_empty_match and "READ" not in granted_on_target:
        return True
    target_ok = required_on_target.issubset(granted_on_target)
    existing_ok = required_on_existing.issubset(granted_on_existing)
    return target_ok and existing_ok


@pytest.mark.parametrize("permissions_on_target", PERMISSION_COMBINATIONS)
@pytest.mark.parametrize("op", OPERATIONS, ids=[op["name"] for op in OPERATIONS])
def test_permission_matrix(permissions_on_target, op):
    admin_cursor = get_admin_cursor()
    permissions_on_existing = op["required_on_existing"]

    reset_and_grant(admin_cursor, permissions_on_target, permissions_on_existing)

    if op["setup"]:
        execute_and_fetch_all(admin_cursor, op["setup"])

    user_cursor = get_user_cursor()

    expected_success = expect_success(
        permissions_on_target,
        permissions_on_existing,
        op["required_on_target"],
        op["required_on_existing"],
        op["expect_empty_match"],
    )

    if expected_success:
        execute_and_fetch_all(user_cursor, op["query"])
    else:
        with pytest.raises(DatabaseError):
            execute_and_fetch_all(user_cursor, op["query"])


@pytest.mark.parametrize("permissions_on_existing", PERMISSION_COMBINATIONS)
def test_add_label_existing_permissions(permissions_on_existing):
    admin_cursor = get_admin_cursor()
    permissions_on_target = {"CREATE"}
    required_on_existing = {"READ", "UPDATE"}

    reset_and_grant(admin_cursor, permissions_on_target, permissions_on_existing)
    execute_and_fetch_all(admin_cursor, "CREATE (:Existing)")

    user_cursor = get_user_cursor()

    if "READ" not in permissions_on_existing:
        expected_success = True
    else:
        expected_success = required_on_existing.issubset(permissions_on_existing)

    if expected_success:
        execute_and_fetch_all(user_cursor, "MATCH (n:Existing) SET n:Target")
    else:
        with pytest.raises(DatabaseError):
            execute_and_fetch_all(user_cursor, "MATCH (n:Existing) SET n:Target")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
