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

match_query = "MATCH (n) RETURN n;"
match_by_id_query = "MATCH (n) WHERE ID(n) >= 0 RETURN n;"

match_by_label_query = "MATCH (n:read_label) RETURN n;"
match_by_label_property_range_query = "MATCH (n:read_label) WHERE n.prop < 7 RETURN n;"
match_by_label_property_value_query = "MATCH (n:read_label {prop: 5}) RETURN n;"
match_by_label_property_query = "MATCH (n:read_label) WHERE n.prop IS NOT NULL RETURN n;"


def get_admin_cursor():
    return connect(username="admin", password="test").cursor()


def get_user_cursor():
    return connect(username="user", password="test").cursor()


def test_can_read_node_without_index():
    admin_cursor = get_admin_cursor()
    user_cursor = get_user_cursor()

    grant_groups = [
        ["GRANT READ ON LABELS :read_label TO user;"],
        ["GRANT READ ON LABELS * TO user;"],
        ["GRANT UPDATE ON LABELS :read_label TO user;"],
        ["GRANT UPDATE ON LABELS * TO user;"],
        ["GRANT CREATE_DELETE ON LABELS :read_label TO user;"],
        ["GRANT CREATE_DELETE ON LABELS * TO user;"],
    ]

    match_queries = [match_query, match_by_id_query]

    for grant_group in grant_groups:
        reset_permissions(admin_cursor)
        [execute_and_fetch_all(admin_cursor, x) for x in grant_group]

        for mq in match_queries:
            results = execute_and_fetch_all(user_cursor, mq)
            assert len(results) == 1


def test_can_read_node_with_index():
    admin_cursor = get_admin_cursor()
    user_cursor = get_user_cursor()

    grant_groups = [
        ["GRANT READ ON LABELS :read_label TO user;"],
        ["GRANT READ ON LABELS * TO user;"],
        ["GRANT UPDATE ON LABELS :read_label TO user;"],
        ["GRANT UPDATE ON LABELS * TO user;"],
        ["GRANT CREATE_DELETE ON LABELS :read_label TO user;"],
        ["GRANT CREATE_DELETE ON LABELS * TO user;"],
    ]

    match_queries = [
        match_by_label_query,
        match_by_label_property_query,
        match_by_label_property_range_query,
        match_by_label_property_value_query,
    ]

    for grant_group in grant_groups:
        reset_permissions(admin_cursor)
        execute_and_fetch_all(admin_cursor, "CREATE INDEX ON :read_label;")
        execute_and_fetch_all(admin_cursor, "CREATE INDEX ON :read_label(prop);")

        [execute_and_fetch_all(admin_cursor, x) for x in grant_group]
        for mq in match_queries:
            results = execute_and_fetch_all(user_cursor, mq)
            assert len(results) == 1


def test_can_not_read_node_without_index():
    admin_cursor = get_admin_cursor()
    user_cursor = get_user_cursor()

    grant_groups = [
        [],
        ["DENY READ ON LABELS :read_label TO user;"],
        ["DENY READ ON LABELS * TO user;"],
        [
            "GRANT UPDATE ON LABELS :read_label TO user;",
            "DENY READ ON LABELS :read_label TO user",
        ],
        [
            "GRANT UPDATE ON LABELS * TO user;",
            "DENY READ ON LABELS :read_label TO user",
        ],
        [
            "GRANT CREATE_DELETE ON LABELS :read_label TO user;",
            "DENY READ ON LABELS :read_label TO user",
        ],
        [
            "GRANT CREATE_DELETE ON LABELS * TO user;",
            "DENY READ ON LABELS :read_label TO user",
        ],
    ]

    match_queries = [match_query, match_by_id_query]

    for grant_group in grant_groups:
        reset_permissions(admin_cursor)

        [execute_and_fetch_all(admin_cursor, x) for x in grant_group]
        for mq in match_queries:
            results = execute_and_fetch_all(user_cursor, mq)
            assert len(results) == 0


def test_can_not_read_node_with_index():
    admin_cursor = get_admin_cursor()
    user_cursor = get_user_cursor()

    grant_groups = [
        [],
        ["DENY READ ON LABELS :read_label TO user;"],
        ["DENY READ ON LABELS * TO user;"],
        [
            "GRANT UPDATE ON LABELS :read_label TO user;",
            "DENY READ ON LABELS :read_label TO user",
        ],
        [
            "GRANT UPDATE ON LABELS * TO user;",
            "DENY READ ON LABELS :read_label TO user",
        ],
        [
            "GRANT CREATE_DELETE ON LABELS :read_label TO user;",
            "DENY READ ON LABELS :read_label TO user",
        ],
        [
            "GRANT CREATE_DELETE ON LABELS * TO user;",
            "DENY READ ON LABELS :read_label TO user",
        ],
    ]

    match_queries = [
        match_by_label_query,
        match_by_label_property_query,
        match_by_label_property_range_query,
        match_by_label_property_value_query,
    ]

    for grant_group in grant_groups:
        reset_permissions(admin_cursor)
        execute_and_fetch_all(admin_cursor, "CREATE INDEX ON :read_label;")
        execute_and_fetch_all(admin_cursor, "CREATE INDEX ON :read_label(prop);")

        [execute_and_fetch_all(admin_cursor, x) for x in grant_group]
        for mq in match_queries:
            results = execute_and_fetch_all(user_cursor, mq)
            assert len(results) == 0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
