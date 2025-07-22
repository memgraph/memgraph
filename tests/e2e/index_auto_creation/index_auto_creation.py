# Copyright 2024 Memgraph Ltd.
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
import time

import pytest
from common import cursor, execute_and_fetch_all


# Helper functions
def get_index_stats(cursor):
    return execute_and_fetch_all(cursor, "SHOW INDEX INFO")


def index_exists(indices, index_name):
    for index in indices:
        if index[1] == index_name:
            return True

    return False


def index_count_is(indices, index_name, count):
    return count == index_count(indices, index_name)


def number_of_index_structures_are(indices, predicted_size):
    return len(indices) == predicted_size


def index_count(indices, index_name):
    for index in indices:
        if index[1] == index_name:
            return index[3]

    return 0


def wait_for_index_condition(cursor, condition_func, timeout_seconds=4, poll_interval=0.1):
    """
    Poll for an index condition to be true within the timeout period.

    Args:
        cursor: Database cursor
        condition_func: Function that takes index_stats and returns bool
        timeout_seconds: Maximum time to wait in seconds (default: 4)
        poll_interval: Time between polls in seconds (default: 0.1)

    Returns:
        True if condition is met, False if timeout
    """
    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        index_stats = get_index_stats(cursor)
        if condition_func(index_stats):
            return True
        time.sleep(poll_interval)
    return False


def assert_index_condition(cursor, condition_func, error_message="Index condition not met within timeout"):
    """
    Assert that an index condition becomes true within the timeout period.

    Args:
        cursor: Database cursor
        condition_func: Function that takes index_stats and returns bool
        error_message: Custom error message for assertion failure
    """
    if not wait_for_index_condition(cursor, condition_func):
        index_stats = get_index_stats(cursor)
        assert False, f"{error_message}. Final index stats: {index_stats}"


def assert_single_label_index(cursor, label):
    """Assert that a single label index exists with count 1."""

    def condition(stats):
        return (
            number_of_index_structures_are(stats, 1) and index_exists(stats, label) and index_count_is(stats, label, 1)
        )

    assert_index_condition(cursor, condition, f"Expected single index for label {label}")


def assert_no_indexes(cursor):
    """Assert that no indexes exist."""

    def condition(stats):
        return number_of_index_structures_are(stats, 0)

    assert_index_condition(cursor, condition, "Expected no indexes")


def assert_index_count(cursor, expected_count):
    """Assert that the specified number of indexes exist."""

    def condition(stats):
        return number_of_index_structures_are(stats, expected_count)

    assert_index_condition(cursor, condition, f"Expected {expected_count} indexes")


def assert_multiple_labels_with_counts(cursor, label_counts_dict, expected_total):
    """Assert multiple labels exist with their expected counts."""

    def condition(stats):
        if not number_of_index_structures_are(stats, expected_total):
            return False
        for label, expected_count in label_counts_dict.items():
            if not (index_exists(stats, label) and index_count_is(stats, label, expected_count)):
                return False
        return True

    labels_str = ", ".join(f"{label}({count})" for label, count in label_counts_dict.items())
    assert_index_condition(cursor, condition, f"Expected {expected_total} indexes: {labels_str}")


def assert_edge_and_label_indexes(cursor, label_from, label_to, edge_type, edge_count):
    """Assert that label and edge indexes exist with correct counts."""

    def condition(stats):
        return (
            index_exists(stats, label_from)
            and index_exists(stats, label_to)
            and index_exists(stats, edge_type)
            and index_count_is(stats, edge_type, edge_count)
            and number_of_index_structures_are(stats, 3)
        )  # 2 labels + 1 edge-type

    assert_index_condition(
        cursor, condition, f"Expected 3 indexes (2 labels + 1 edge-type) with edge count {edge_count}"
    )


def assert_multiple_edges_with_labels(cursor, label_from, label_to, edge_counts_dict, expected_total):
    """Assert multiple edge types exist with labels and their expected counts."""

    def condition(stats):
        if not number_of_index_structures_are(stats, expected_total):
            return False
        if not (index_exists(stats, label_from) and index_exists(stats, label_to)):
            return False
        for edge_type, expected_count in edge_counts_dict.items():
            if not (index_exists(stats, edge_type) and index_count_is(stats, edge_type, expected_count)):
                return False
        return True

    edges_str = ", ".join(f"{edge}({count})" for edge, count in edge_counts_dict.items())
    assert_index_condition(
        cursor,
        condition,
        f"Expected {expected_total} indexes (2 labels + {len(edge_counts_dict)} edge-types): {edges_str}",
    )


def cleanup_indexes_and_data(cursor):
    """Clean up all indexes and data from the database."""
    try:
        # Use DROP GRAPH for efficient cleanup of everything
        execute_and_fetch_all(cursor, "DROP GRAPH")
        execute_and_fetch_all(cursor, "FREE MEMORY")
        time.sleep(1)
    except Exception:
        # If DROP GRAPH fails (e.g., due to concurrent transactions),
        # fall back to manual cleanup
        try:
            # Get all current indexes
            index_stats = get_index_stats(cursor)

            # Drop all label and edge indexes
            for index_info in index_stats:
                index_type = index_info[0]
                index_name = index_info[1]

                try:
                    if index_type == "label":
                        execute_and_fetch_all(cursor, f"DROP INDEX ON :{index_name}")
                    elif index_type == "edge-type":
                        execute_and_fetch_all(cursor, f"DROP EDGE INDEX ON :{index_name}")
                except Exception:
                    pass  # Index might already be dropped

            # Delete all data
            execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")
        except Exception:
            # If something goes wrong, still try to delete all data
            try:
                execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")
            except Exception:
                pass


def ensure_clean_state(cursor):
    """Ensure database is in a clean state before test."""
    cleanup_indexes_and_data(cursor)


#####################
# Label index tests #
#####################


# def test_auto_create_single_label_index(cursor):
#     ensure_clean_state(cursor)
#     label = "SOMELABEL"
#
#     try:
#         execute_and_fetch_all(cursor, f"CREATE (n:{label})")
#
#         assert_single_label_index(cursor, label)
#
#         execute_and_fetch_all(cursor, f"DROP INDEX ON :{label}")
#         assert_no_indexes(cursor)
#     finally:
#         cleanup_indexes_and_data(cursor)
#
#
# def test_auto_create_several_different_label_index(cursor):
#     ensure_clean_state(cursor)
#     label1 = "SOMELABEL1"
#     label2 = "SOMELABEL2"
#     label3 = "SOMELABEL3"
#
#     try:
#         execute_and_fetch_all(cursor, f"CREATE (n:{label1}:{label2}:{label3})")
#
#         assert_index_count(cursor, 3)
#
#         execute_and_fetch_all(cursor, f"DROP INDEX ON :{label1}")
#         execute_and_fetch_all(cursor, f"DROP INDEX ON :{label2}")
#         execute_and_fetch_all(cursor, f"DROP INDEX ON :{label3}")
#
#         assert_no_indexes(cursor)
#     finally:
#         cleanup_indexes_and_data(cursor)
#
#
# def test_auto_create_multiple_label_index(cursor):
#     ensure_clean_state(cursor)
#     label1 = "SOMELABEL1"
#     label2 = "SOMELABEL2"
#     label3 = "SOMELABEL3"
#
#     try:
#         execute_and_fetch_all(cursor, f"CREATE (n:{label1})")
#         execute_and_fetch_all(cursor, f"CREATE (n:{label2})")
#         execute_and_fetch_all(cursor, f"CREATE (n:{label3})")
#
#         assert_multiple_labels_with_counts(cursor, {label1: 1, label2: 1, label3: 1}, 3)
#
#         execute_and_fetch_all(cursor, f"DROP INDEX ON :{label1}")
#         assert_index_count(cursor, 2)
#
#         execute_and_fetch_all(cursor, f"DROP INDEX ON :{label2}")
#         assert_index_count(cursor, 1)
#
#         execute_and_fetch_all(cursor, f"DROP INDEX ON :{label3}")
#         assert_no_indexes(cursor)
#     finally:
#         cleanup_indexes_and_data(cursor)
#
#
# def test_auto_create_single_label_index_with_multiple_entries(cursor):
#     ensure_clean_state(cursor)
#     label = "SOMELABEL"
#
#     try:
#         for _ in range(100):
#             execute_and_fetch_all(cursor, f"CREATE (n:{label})")
#
#         assert_multiple_labels_with_counts(cursor, {label: 100}, 1)
#
#         execute_and_fetch_all(cursor, f"DROP INDEX ON :{label}")
#         assert_no_indexes(cursor)
#     finally:
#         cleanup_indexes_and_data(cursor)


def test_auto_create_multiple_label_index_with_multiple_entries(cursor):
    ensure_clean_state(cursor)
    label1 = "SOMELABEL1"
    label2 = "SOMELABEL2"
    label3 = "SOMELABEL3"

    try:
        for _ in range(200):
            execute_and_fetch_all(cursor, f"CREATE (n:{label1})")

        for _ in range(200):
            execute_and_fetch_all(cursor, f"CREATE (n:{label2})")

        for _ in range(200):
            execute_and_fetch_all(cursor, f"CREATE (n:{label3})")

        assert_multiple_labels_with_counts(cursor, {label1: 200, label2: 200, label3: 200}, 3)

        execute_and_fetch_all(cursor, f"DROP INDEX ON :{label1}")
        assert_index_count(cursor, 2)

        execute_and_fetch_all(cursor, f"DROP INDEX ON :{label2}")
        assert_index_count(cursor, 1)

        execute_and_fetch_all(cursor, f"DROP INDEX ON :{label3}")
        assert_no_indexes(cursor)
    finally:
        cleanup_indexes_and_data(cursor)


def test_auto_create_label_index_plan_invalidation(cursor):
    label = "Label"

    # populate the plan cache
    execute_and_fetch_all(cursor, f"MATCH (n:{label}) RETURN n")

    execute_and_fetch_all(cursor, f"CREATE (n:{label})")
    assert index_exists(get_index_stats(cursor), label)
    results = execute_and_fetch_all(cursor, f"EXPLAIN MATCH (n:{label}) RETURN n")
    assert results == [(" * Produce {n}",), (" * ScanAllByLabel (n :Label)", ), (" * Once", )]

#########################
# Edge-type index tests #
#########################
#
#
# def test_auto_create_single_edge_type_index(cursor):
#     ensure_clean_state(cursor)
#     label_from = "LABEL_FROM"
#     label_to = "LABEL_TO"
#     edge_type = "SOMEEDGETYPE"
#
#     try:
#         execute_and_fetch_all(cursor, f"CREATE (:{label_from})-[:{edge_type}]->(:{label_to})")
#
#         assert_edge_and_label_indexes(cursor, label_from, label_to, edge_type, 1)
#
#         execute_and_fetch_all(cursor, f"DROP INDEX ON :{label_from}")
#         execute_and_fetch_all(cursor, f"DROP INDEX ON :{label_to}")
#         execute_and_fetch_all(cursor, f"DROP EDGE INDEX ON :{edge_type}")
#
#         assert_no_indexes(cursor)
#     finally:
#         cleanup_indexes_and_data(cursor)
#
#
# def test_auto_create_multiple_edge_type_index(cursor):
#     ensure_clean_state(cursor)
#     label_from = "LABEL_FROM"
#     label_to = "LABEL_TO"
#     edge_type1 = "SOMEEDGETYPE1"
#     edge_type2 = "SOMEEDGETYPE2"
#     edge_type3 = "SOMEEDGETYPE3"
#
#     try:
#         execute_and_fetch_all(cursor, f"CREATE (n:{label_from})")
#         execute_and_fetch_all(cursor, f"CREATE (n:{label_to})")
#         execute_and_fetch_all(cursor, f"MATCH (n:{label_from}), (m:{label_to}) CREATE (n)-[:{edge_type1}]->(m)")
#         execute_and_fetch_all(cursor, f"MATCH (n:{label_from}), (m:{label_to}) CREATE (n)-[:{edge_type2}]->(m)")
#         execute_and_fetch_all(cursor, f"MATCH (n:{label_from}), (m:{label_to}) CREATE (n)-[:{edge_type3}]->(m)")
#
#         assert_multiple_edges_with_labels(
#             cursor, label_from, label_to, {edge_type1: 1, edge_type2: 1, edge_type3: 1}, 5
#         )
#
#         execute_and_fetch_all(cursor, f"DROP INDEX ON :{label_from}")
#         assert_index_count(cursor, 4)
#
#         execute_and_fetch_all(cursor, f"DROP INDEX ON :{label_to}")
#         assert_index_count(cursor, 3)
#
#         execute_and_fetch_all(cursor, f"DROP EDGE INDEX ON :{edge_type1}")
#         assert_index_count(cursor, 2)
#
#         execute_and_fetch_all(cursor, f"DROP EDGE INDEX ON :{edge_type2}")
#         assert_index_count(cursor, 1)
#
#         execute_and_fetch_all(cursor, f"DROP EDGE INDEX ON :{edge_type3}")
#         assert_no_indexes(cursor)
#     finally:
#         cleanup_indexes_and_data(cursor)
#
#
# def test_auto_create_single_edge_type_index_with_multiple_entries(cursor):
#     ensure_clean_state(cursor)
#     label_from = "LABEL_FROM"
#     label_to = "LABEL_TO"
#     edge_type = "SOMEEDGETYPE"
#
#     try:
#         execute_and_fetch_all(cursor, f"CREATE (n:{label_from})")
#         execute_and_fetch_all(cursor, f"CREATE (n:{label_to})")
#         for _ in range(100):
#             execute_and_fetch_all(cursor, f"MATCH (n:{label_from}), (m:{label_to}) CREATE (n)-[:{edge_type}]->(m)")
#
#         assert_edge_and_label_indexes(cursor, label_from, label_to, edge_type, 100)
#
#         execute_and_fetch_all(cursor, f"DROP INDEX ON :{label_from}")
#         execute_and_fetch_all(cursor, f"DROP INDEX ON :{label_to}")
#         execute_and_fetch_all(cursor, f"DROP EDGE INDEX ON :{edge_type}")
#
#         assert_no_indexes(cursor)
#     finally:
#         cleanup_indexes_and_data(cursor)


def test_auto_create_multiple_edge_type_index_with_multiple_entries(cursor):
    ensure_clean_state(cursor)
    label_from = "LABEL_FROM"
    label_to = "LABEL_TO"
    edge_type1 = "SOMEEDGETYPE1"
    edge_type2 = "SOMEEDGETYPE2"
    edge_type3 = "SOMEEDGETYPE3"

    try:
        execute_and_fetch_all(cursor, f"CREATE (n:{label_from})")
        execute_and_fetch_all(cursor, f"CREATE (n:{label_to})")
        for _ in range(2):
            execute_and_fetch_all(cursor, f"MATCH (n:{label_from}), (m:{label_to}) CREATE (n)-[:{edge_type1}]->(m)")
        for _ in range(2):
            execute_and_fetch_all(cursor, f"MATCH (n:{label_from}), (m:{label_to}) CREATE (n)-[:{edge_type2}]->(m)")
        for _ in range(2):
            execute_and_fetch_all(cursor, f"MATCH (n:{label_from}), (m:{label_to}) CREATE (n)-[:{edge_type3}]->(m)")

        assert_multiple_edges_with_labels(
            cursor, label_from, label_to, {edge_type1: 2, edge_type2: 2, edge_type3: 2}, 5
        )

        execute_and_fetch_all(cursor, f"DROP INDEX ON :{label_from}")
        assert_index_count(cursor, 4)

        execute_and_fetch_all(cursor, f"DROP INDEX ON :{label_to}")
        assert_index_count(cursor, 3)

        execute_and_fetch_all(cursor, f"DROP EDGE INDEX ON :{edge_type1}")
        assert_index_count(cursor, 2)

        execute_and_fetch_all(cursor, f"DROP EDGE INDEX ON :{edge_type2}")
        assert_index_count(cursor, 1)

        execute_and_fetch_all(cursor, f"DROP EDGE INDEX ON :{edge_type3}")
        assert_no_indexes(cursor)
    finally:
        cleanup_indexes_and_data(cursor)

def test_auto_create_edge_type_index_plan_invalidation(cursor):
    edge_type = "Label"

    # populate the plan cache
    execute_and_fetch_all(cursor, f"MATCH ()-[r:Label]->() RETURN r")

    execute_and_fetch_all(cursor, f"CREATE ()-[r:{edge_type}]->()")

    execute_and_fetch_all(cursor, f"MATCH ()-[r:Label]->() RETURN r")
    assert index_exists(get_index_stats(cursor), edge_type)
    
    results = execute_and_fetch_all(cursor, f"EXPLAIN MATCH ()-[r:Label]->() RETURN r")
    assert results == [(" * Produce {r}",), (" * ScanAllByEdgeType (anon1)-[r:Label]->(anon2)", ), (" * Once", )]



if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
