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

from typing import List

from common import connect, execute_and_fetch_all, reset_permissions

match_query = "MATCH (n) RETURN n;"
match_by_id_query = "MATCH (n) WHERE ID(n) >= 0 RETURN n;"

match_by_label_query = "MATCH (n:read_label) RETURN n;"
match_by_label_property_range_query = "MATCH (n:read_label) WHERE n.prop < 7 RETURN n;"
match_by_label_property_value_query = "MATCH (n:read_label {prop: 5}) RETURN n;"
match_by_label_property_query = "MATCH (n:read_label) WHERE n.prop IS NOT NULL RETURN n;"


def get_read_node_without_index_operation_cases() -> List[str]:
    return [
        ["GRANT READ ON LABELS :read_label TO user;"],
        ["GRANT READ ON LABELS * TO user;"],
        ["GRANT UPDATE ON LABELS :read_label TO user;"],
        ["GRANT UPDATE ON LABELS * TO user;"],
        ["GRANT CREATE_DELETE ON LABELS :read_label TO user;"],
        ["GRANT CREATE_DELETE ON LABELS * TO user;"],
    ]


def get_read_node_with_index_operation_cases() -> List[str]:
    return [
        ["GRANT READ ON LABELS :read_label TO user;"],
        ["GRANT READ ON LABELS * TO user;"],
        ["GRANT UPDATE ON LABELS :read_label TO user;"],
        ["GRANT UPDATE ON LABELS * TO user;"],
        ["GRANT CREATE_DELETE ON LABELS :read_label TO user;"],
        ["GRANT CREATE_DELETE ON LABELS * TO user;"],
    ]


def get_not_read_node_without_index_operation_cases() -> List[str]:
    return [
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


def get_not_read_node_with_index_operation_cases() -> List[str]:
    return [
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


def get_admin_cursor():
    return connect(username="admin", password="test").cursor()


def get_user_cursor():
    return connect(username="user", password="test").cursor()


def execute_read_node_assertion(
    operation_case: List[str], queries: List[str], create_index: bool, can_read: bool
) -> None:
    admin_cursor = get_admin_cursor()
    user_cursor = get_user_cursor()

    reset_permissions(admin_cursor, create_index)

    for operation in operation_case:
        execute_and_fetch_all(admin_cursor, operation)

    read_size = 1 if can_read else 0
    for mq in queries:
        results = execute_and_fetch_all(user_cursor, mq)
        assert len(results) == read_size


def test_can_read_node_when_authorized():
    operation_cases_without_index = get_read_node_without_index_operation_cases()
    match_queries_without_index = [match_query, match_by_id_query]

    operation_cases_with_index = get_read_node_with_index_operation_cases()
    match_queries_with_index = [
        match_by_label_query,
        match_by_label_property_query,
        match_by_label_property_range_query,
        match_by_label_property_value_query,
    ]

    for operation_case in operation_cases_without_index:
        execute_read_node_assertion(operation_case, match_queries_without_index, False, True)
    for operation_case in operation_cases_with_index:
        execute_read_node_assertion(operation_case, match_queries_with_index, True, True)


def test_can_not_read_node_when_authorized():
    operation_cases_without_index = get_not_read_node_without_index_operation_cases()
    match_queries_without_index = [match_query, match_by_id_query]

    operation_cases_with_index = get_not_read_node_with_index_operation_cases()
    match_queries_with_index = [
        match_by_label_query,
        match_by_label_property_query,
        match_by_label_property_range_query,
        match_by_label_property_value_query,
    ]

    for operation_case in operation_cases_without_index:
        execute_read_node_assertion(operation_case, match_queries_without_index, False, False)
    for operation_case in operation_cases_with_index:
        execute_read_node_assertion(operation_case, match_queries_with_index, True, False)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
