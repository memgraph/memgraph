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
from typing import List

import pytest
from common import *

match_query = "MATCH (n) RETURN n;"
match_by_id_query = "MATCH (n) WHERE ID(n) >= 0 RETURN n;"

match_by_label_query = "MATCH (n) RETURN n;"
match_by_label_property_range_query = "MATCH (n) WHERE n.prop < 7 RETURN n;"
match_by_label_property_value_query = "MATCH (n {prop: 5}) RETURN n;"
match_by_label_property_query = "MATCH (n) WHERE n.prop IS NOT NULL RETURN n;"


read_node_without_index_operation_cases = [
    ["GRANT READ ON LABELS :read_label TO user;"],
    ["GRANT READ ON LABELS * TO user;"],
    ["GRANT UPDATE ON LABELS :read_label TO user;"],
    ["GRANT UPDATE ON LABELS * TO user;"],
    ["GRANT CREATE_DELETE ON LABELS :read_label TO user;"],
    ["GRANT CREATE_DELETE ON LABELS * TO user;"],
]

read_node_without_index_operation_cases_expected_size = [1, 3, 1, 3, 1, 3]

read_node_with_index_operation_cases = [
    ["GRANT READ ON LABELS :read_label TO user;"],
    ["GRANT READ ON LABELS * TO user;"],
    ["GRANT UPDATE ON LABELS :read_label TO user;"],
    ["GRANT UPDATE ON LABELS * TO user;"],
    ["GRANT CREATE_DELETE ON LABELS :read_label TO user;"],
    ["GRANT CREATE_DELETE ON LABELS * TO user;"],
]

read_node_with_index_operation_cases_expected_sizes = [1, 3, 1, 3, 1, 3]

not_read_node_without_index_operation_cases = [
    [],
    ["GRANT NOTHING ON LABELS :read_label TO user;"],
    ["GRANT NOTHING ON LABELS * TO user;"],
    [
        "GRANT UPDATE ON LABELS :read_label TO user;",
        "GRANT NOTHING ON LABELS :read_label TO user",
    ],
    [
        "GRANT UPDATE ON LABELS * TO user;",
        "GRANT NOTHING ON LABELS :read_label TO user",
    ],
    [
        "GRANT CREATE_DELETE ON LABELS :read_label TO user;",
        "GRANT NOTHING ON LABELS :read_label TO user",
    ],
    [
        "GRANT CREATE_DELETE ON LABELS * TO user;",
        "GRANT NOTHING ON LABELS :read_label TO user",
    ],
]

not_read_node_without_index_operation_cases_expected_sizes = [0, 0, 0, 0, 2, 0, 2]

not_read_node_with_index_operation_cases = [
    [],
    ["GRANT NOTHING ON LABELS :read_label TO user;"],
    ["GRANT NOTHING ON LABELS * TO user;"],
    [
        "GRANT UPDATE ON LABELS :read_label TO user;",
        "GRANT NOTHING ON LABELS :read_label TO user",
    ],
    [
        "GRANT UPDATE ON LABELS * TO user;",
        "GRANT NOTHING ON LABELS :read_label TO user",
    ],
    [
        "GRANT CREATE_DELETE ON LABELS :read_label TO user;",
        "GRANT NOTHING ON LABELS :read_label TO user",
    ],
    [
        "GRANT CREATE_DELETE ON LABELS * TO user;",
        "GRANT NOTHING ON LABELS :read_label TO user",
    ],
]

not_read_node_with_index_operation_cases_expexted_sizes = [0, 0, 0, 0, 2, 0, 2]


def get_admin_cursor():
    return connect(username="admin", password="test").cursor()


def get_user_cursor():
    return connect(username="user", password="test").cursor()


def execute_read_node_assertion(
    operation_case: List[str], queries: List[str], create_index: bool, expected_size: int, switch: bool
) -> None:
    admin_cursor = get_admin_cursor()

    if switch:
        create_multi_db(admin_cursor)
        switch_db(admin_cursor)

    reset_permissions(admin_cursor, create_index)

    for operation in operation_case:
        execute_and_fetch_all(admin_cursor, operation)

    # Connect after possible auth changes
    user_cursor = get_user_cursor()
    if switch:
        switch_db(user_cursor)

    for mq in queries:
        results = execute_and_fetch_all(user_cursor, mq)
        assert len(results) == expected_size


@pytest.mark.parametrize("switch", [False, True])
def test_can_read_node_when_authorized(switch):
    match_queries_without_index = [match_query, match_by_id_query]
    match_queries_with_index = [
        match_by_label_query,
        match_by_label_property_query,
        match_by_label_property_range_query,
        match_by_label_property_value_query,
    ]

    for expected_size, operation_case in zip(
        read_node_without_index_operation_cases_expected_size, read_node_without_index_operation_cases
    ):
        execute_read_node_assertion(operation_case, match_queries_without_index, False, expected_size, switch)
    for expected_size, operation_case in zip(
        read_node_with_index_operation_cases_expected_sizes, read_node_with_index_operation_cases
    ):
        execute_read_node_assertion(operation_case, match_queries_with_index, True, expected_size, switch)


@pytest.mark.parametrize("switch", [False, True])
def test_can_not_read_node_when_authorized(switch):
    match_queries_without_index = [match_query, match_by_id_query]
    match_queries_with_index = [
        match_by_label_query,
        match_by_label_property_query,
        match_by_label_property_range_query,
        match_by_label_property_value_query,
    ]

    for expected_size, operation_case in zip(
        not_read_node_without_index_operation_cases_expected_sizes, not_read_node_without_index_operation_cases
    ):
        execute_read_node_assertion(operation_case, match_queries_without_index, False, expected_size, switch)
    for expected_size, operation_case in zip(
        not_read_node_with_index_operation_cases_expexted_sizes, not_read_node_with_index_operation_cases
    ):
        execute_read_node_assertion(operation_case, match_queries_with_index, True, expected_size, switch)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
