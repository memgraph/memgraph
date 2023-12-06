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


def create_edges(cursor):
    execute_and_fetch_all(
        cursor,
        "MATCH (charlie:Person {name: 'Charlie Sheen'}), (oliver:Person {name: 'Oliver Stone'}) CREATE (charlie)-[:ACTED_IN {role: 'Bud Fox'}]->(wallStreet:Movie {title: 'Wall Street'})<-[:DIRECTED]-(oliver)",
    )


def edge_types_info(cursor):
    return execute_and_fetch_all(cursor, "SHOW EDGE_TYPES INFO")


def default_expected_result(cursor):
    return [("DIRECTED",), ("ACTED_IN",)]


# Tests
def test_return_empty(cursor):
    create_nodes(cursor)

    edge_types = edge_types_info(cursor)
    expected = []
    assert are_results_equal(expected, edge_types)


def test_return_edge_types_simple(cursor):
    create_nodes(cursor)
    create_edges(cursor)

    edge_types = edge_types_info(cursor)
    expected = default_expected_result(cursor)
    assert are_results_equal(expected, edge_types)


def test_return_edge_types_repeating_identical_edges(cursor):
    create_nodes(cursor)

    for _ in range(100):
        create_edges(cursor)

    edge_types = edge_types_info(cursor)
    expected = default_expected_result(cursor)
    assert are_results_equal(expected, edge_types)


def test_return_edge_types_obtainable_after_edge_deletion(cursor):
    create_nodes(cursor)
    create_edges(cursor)
    execute_and_fetch_all(cursor, "MATCH(n) DETACH DELETE n")

    edge_types = edge_types_info(cursor)
    expected = default_expected_result(cursor)
    assert are_results_equal(expected, edge_types)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
