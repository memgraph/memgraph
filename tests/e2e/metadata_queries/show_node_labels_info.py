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
from common import are_results_equal, cursor, execute_and_fetch_all


# Helper functions
def create_nodes(cursor):
    execute_and_fetch_all(
        cursor, "CREATE (charlie:Person:Actor {name: 'Charlie Sheen'}), (oliver:Person:Director {name: 'Oliver Stone'})"
    )


def node_labels_info(cursor):
    return execute_and_fetch_all(cursor, "SHOW NODE_LABELS INFO")


def default_expected_result(cursor):
    return [("Person",), ("Actor",), ("Director",)]


# Tests
def test_return_empty(cursor):
    node_labels = node_labels_info(cursor)
    expected = []
    assert are_results_equal(expected, node_labels)


def test_return_node_labels_simple(cursor):
    create_nodes(cursor)

    node_labels = node_labels_info(cursor)
    expected = default_expected_result(cursor)
    assert are_results_equal(expected, node_labels)


def test_return_node_labels_repeating_identical_labels(cursor):
    for _ in range(100):
        create_nodes(cursor)

    node_labels = node_labels_info(cursor)
    expected = default_expected_result(cursor)
    assert are_results_equal(expected, node_labels)


def test_return_node_labels_obtainable_after_vertex_deletion(cursor):
    create_nodes(cursor)
    execute_and_fetch_all(cursor, "MATCH(n) DELETE n")

    node_labels = node_labels_info(cursor)
    expected = default_expected_result(cursor)
    assert are_results_equal(expected, node_labels)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
