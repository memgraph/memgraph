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
from common import connect, execute_and_fetch_all

# E2E tests for checking query semantic
# ------------------------------------


@pytest.mark.parametrize(
    "delete_query",
    [
        "ANALYZE GRAPH DELETE STATISTICS",
        "ANALYZE GRAPH ON LABELS * DELETE STATISTICS",
        "ANALYZE GRAPH ON LABELS :Label DELETE STATISTICS",
        "ANALYZE GRAPH ON LABELS :Label, :NONEXISTING DELETE STATISTICS",
    ],
)
def test_analyze_graph_delete_statistics(delete_query, connect):
    """Tests that all variants of delete queries work as expected."""
    cursor = connect.cursor()
    execute_and_fetch_all(cursor, "FOREACH (i IN range(1, 100) | CREATE (n:Label {id1: i}));")
    execute_and_fetch_all(cursor, "FOREACH (i IN range(1, 50) | CREATE (n:Label {id2: i % 5}));")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Label(id1);")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Label(id2);")
    analyze_graph_results = execute_and_fetch_all(cursor, "ANALYZE GRAPH")
    assert len(analyze_graph_results) == 2
    delete_stats_results = execute_and_fetch_all(cursor, delete_query)
    assert len(delete_stats_results) == 2
    if delete_stats_results[0][1] == "id1":
        first_index = 0
    else:
        first_index = 1
    assert delete_stats_results[first_index] == ("Label", "id1")
    assert delete_stats_results[1 - first_index] == ("Label", "id2")
    # After deleting statistics, id2 should be chosen because it has less vertices
    expected_explain_after_delete_analysis = [
        (f" * Produce {{n}}",),
        (f" * Filter",),
        (f" * ScanAllByLabelPropertyValue (n :Label {{id2}})",),
        (f" * Once",),
    ]
    assert (
        execute_and_fetch_all(cursor, "EXPLAIN MATCH (n:Label) WHERE n.id2 = 3 AND n.id1 = 3 RETURN n;")
        == expected_explain_after_delete_analysis
    )
    execute_and_fetch_all(cursor, "DROP INDEX ON :Label(id1);")
    execute_and_fetch_all(cursor, "DROP INDEX ON :Label(id2);")


@pytest.mark.parametrize(
    "analyze_query",
    [
        "ANALYZE GRAPH",
        "ANALYZE GRAPH ON LABELS *",
        "ANALYZE GRAPH ON LABELS :Label",
        "ANALYZE GRAPH ON LABELS :Label, :NONEXISTING",
    ],
)
def test_analyze_full_graph(analyze_query, connect):
    """Tests analyzing full graph and choosing better index based on the smaller average group size.
    It also tests querying based on labels and that nothing bad will happen by providing non-existing label.
    """
    cursor = connect.cursor()
    execute_and_fetch_all(cursor, "FOREACH (i IN range(1, 100) | CREATE (n:Label {id1: i}));")
    execute_and_fetch_all(cursor, "FOREACH (i IN range(1, 50) | CREATE (n:Label {id2: i % 5}));")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Label(id1);")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Label(id2);")
    # Choose id2 before tha analysis because it has less vertices
    expected_explain_before_analysis = [
        (f" * Produce {{n}}",),
        (f" * Filter",),
        (f" * ScanAllByLabelPropertyValue (n :Label {{id2}})",),
        (f" * Once",),
    ]
    assert (
        execute_and_fetch_all(cursor, "EXPLAIN MATCH (n:Label) WHERE n.id2 = 3 AND n.id1 = 3 RETURN n;")
        == expected_explain_before_analysis
    )
    # Run analyze query
    analyze_graph_results = execute_and_fetch_all(cursor, analyze_query)
    assert len(analyze_graph_results) == 2
    if analyze_graph_results[0][1] == "id1":
        first_index = 0
    else:
        first_index = 1
    # Check results
    assert analyze_graph_results[first_index] == ("Label", "id1", 100, 100, 1, 0)
    assert analyze_graph_results[1 - first_index] == ("Label", "id2", 50, 5, 10, 0)
    # After analyzing graph, id1 index should be chosen because it has smaller average group size
    expected_explain_after_analysis = [
        (f" * Produce {{n}}",),
        (f" * Filter",),
        (f" * ScanAllByLabelPropertyValue (n :Label {{id1}})",),
        (f" * Once",),
    ]
    assert (
        execute_and_fetch_all(cursor, "EXPLAIN MATCH (n:Label) WHERE n.id2 = 3 AND n.id1 = 3 RETURN n;")
        == expected_explain_after_analysis
    )
    assert len(execute_and_fetch_all(cursor, "ANALYZE GRAPH DELETE STATISTICS")) == 2
    execute_and_fetch_all(cursor, "DROP INDEX ON :Label(id1);")
    execute_and_fetch_all(cursor, "DROP INDEX ON :Label(id2);")


# Explicit index choosing tests
# -----------------------------


def test_cardinality_different_avg_group_size_uniform_dist(connect):
    """Tests index optimization with indices both having uniform distribution but one has smaller avg. group size."""
    cursor = connect.cursor()
    execute_and_fetch_all(cursor, "FOREACH (i IN range(1, 100) | CREATE (n:Label {id1: i}));")
    execute_and_fetch_all(cursor, "FOREACH (i IN range(1, 100) | CREATE (n:Label {id2: i % 20}));")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Label(id1);")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Label(id2);")
    analyze_graph_results = execute_and_fetch_all(cursor, "ANALYZE GRAPH")
    if analyze_graph_results[0][1] == "id1":
        first_index = 0
    else:
        first_index = 1
    # Check results
    assert analyze_graph_results[first_index] == ("Label", "id1", 100, 100, 1, 0)
    assert analyze_graph_results[1 - first_index] == ("Label", "id2", 100, 20, 5, 0)
    expected_explain_after_analysis = [
        (f" * Produce {{n}}",),
        (f" * Filter",),
        (f" * ScanAllByLabelPropertyValue (n :Label {{id1}})",),
        (f" * Once",),
    ]
    assert (
        execute_and_fetch_all(cursor, "EXPLAIN MATCH (n:Label) WHERE n.id2 = 3 AND n.id1 = 3 RETURN n;")
        == expected_explain_after_analysis
    )
    assert len(execute_and_fetch_all(cursor, "ANALYZE GRAPH DELETE STATISTICS")) == 2
    execute_and_fetch_all(cursor, "DROP INDEX ON :Label(id1);")
    execute_and_fetch_all(cursor, "DROP INDEX ON :Label(id2);")


def test_cardinality_same_avg_group_size_uniform_dist_diff_vertex_count(connect):
    """Tests index choosing where both indices have uniform key distribution with same avg. group size but one has less vertices."""
    cursor = connect.cursor()
    execute_and_fetch_all(cursor, "FOREACH (i IN range(1, 100) | CREATE (n:Label {id1: i}));")
    execute_and_fetch_all(cursor, "FOREACH (i IN range(1, 50) | CREATE (n:Label {id2: i}));")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Label(id1);")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Label(id2);")
    analyze_graph_results = execute_and_fetch_all(cursor, "ANALYZE GRAPH")
    if analyze_graph_results[0][1] == "id1":
        first_index = 0
    else:
        first_index = 1
    # Check results
    assert analyze_graph_results[first_index] == ("Label", "id1", 100, 100, 1, 0)
    assert analyze_graph_results[1 - first_index] == ("Label", "id2", 50, 50, 1, 0)
    expected_explain_after_analysis = [
        (f" * Produce {{n}}",),
        (f" * Filter",),
        (f" * ScanAllByLabelPropertyValue (n :Label {{id2}})",),
        (f" * Once",),
    ]
    assert (
        execute_and_fetch_all(cursor, "EXPLAIN MATCH (n:Label) WHERE n.id2 = 3 AND n.id1 = 3 RETURN n;")
        == expected_explain_after_analysis
    )
    assert len(execute_and_fetch_all(cursor, "ANALYZE GRAPH DELETE STATISTICS")) == 2
    execute_and_fetch_all(cursor, "DROP INDEX ON :Label(id1);")
    execute_and_fetch_all(cursor, "DROP INDEX ON :Label(id2);")


def test_large_diff_in_num_vertices_v1(connect):
    """Tests that when one index has > 10x vertices than the other one, it should be chosen no matter avg group size and uniform distribution."""
    cursor = connect.cursor()
    execute_and_fetch_all(cursor, "FOREACH (i IN range(1, 1000) | CREATE (n:Label {id1: i}));")
    execute_and_fetch_all(cursor, "FOREACH (i IN range(1, 99) | CREATE (n:Label {id2: 1}));")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Label(id1);")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Label(id2);")
    analyze_graph_results = execute_and_fetch_all(cursor, "ANALYZE GRAPH")
    if analyze_graph_results[0][1] == "id1":
        first_index = 0
    else:
        first_index = 1
    # Check results
    assert analyze_graph_results[first_index] == ("Label", "id1", 1000, 1000, 1, 0)
    assert analyze_graph_results[1 - first_index] == ("Label", "id2", 99, 1, 99, 0)
    expected_explain_after_analysis = [
        (f" * Produce {{n}}",),
        (f" * Filter",),
        (f" * ScanAllByLabelPropertyValue (n :Label {{id2}})",),
        (f" * Once",),
    ]
    assert (
        execute_and_fetch_all(cursor, "EXPLAIN MATCH (n:Label) WHERE n.id2 = 3 AND n.id1 = 3 RETURN n;")
        == expected_explain_after_analysis
    )
    assert len(execute_and_fetch_all(cursor, "ANALYZE GRAPH DELETE STATISTICS")) == 2
    execute_and_fetch_all(cursor, "DROP INDEX ON :Label(id1);")
    execute_and_fetch_all(cursor, "DROP INDEX ON :Label(id2);")


def test_large_diff_in_num_vertices_v2(connect):
    """Tests that when one index has > 10x vertices than the other one, it should be chosen no matter avg group size and uniform distribution."""
    cursor = connect.cursor()
    execute_and_fetch_all(cursor, "FOREACH (i IN range(1, 99) | CREATE (n:Label {id1: 1}));")
    execute_and_fetch_all(cursor, "FOREACH (i IN range(1, 1000) | CREATE (n:Label {id2: i}));")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Label(id1);")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Label(id2);")
    analyze_graph_results = execute_and_fetch_all(cursor, "ANALYZE GRAPH")
    if analyze_graph_results[0][1] == "id1":
        first_index = 0
    else:
        first_index = 1
    # Check results
    assert analyze_graph_results[first_index] == ("Label", "id1", 99, 1, 99, 0)
    assert analyze_graph_results[1 - first_index] == ("Label", "id2", 1000, 1000, 1, 0)
    expected_explain_after_analysis = [
        (f" * Produce {{n}}",),
        (f" * Filter",),
        (f" * ScanAllByLabelPropertyValue (n :Label {{id1}})",),
        (f" * Once",),
    ]
    assert (
        execute_and_fetch_all(cursor, "EXPLAIN MATCH (n:Label) WHERE n.id2 = 3 AND n.id1 = 3 RETURN n;")
        == expected_explain_after_analysis
    )
    assert len(execute_and_fetch_all(cursor, "ANALYZE GRAPH DELETE STATISTICS")) == 2
    execute_and_fetch_all(cursor, "DROP INDEX ON :Label(id1);")
    execute_and_fetch_all(cursor, "DROP INDEX ON :Label(id2);")


def test_same_avg_group_size_diff_distribution(connect):
    """Tests index choice decision based on key distribution."""
    cursor = connect.cursor()
    # Setup first key distribution
    execute_and_fetch_all(cursor, "FOREACH (i IN range(1, 10) | CREATE (n:Label {id1: 1}));")
    execute_and_fetch_all(cursor, "FOREACH (i IN range(1, 30) | CREATE (n:Label {id1: 2}));")
    execute_and_fetch_all(cursor, "FOREACH (i IN range(1, 20) | CREATE (n:Label {id1: 3}));")
    execute_and_fetch_all(cursor, "FOREACH (i IN range(1, 35) | CREATE (n:Label {id1: 4}));")
    execute_and_fetch_all(cursor, "FOREACH (i IN range(1, 5) | CREATE (n:Label  {id1: 5}));")
    # Setup second key distribution
    execute_and_fetch_all(cursor, "FOREACH (i IN range(1, 20) | CREATE (n:Label {id2: 1}));")
    execute_and_fetch_all(cursor, "FOREACH (i IN range(1, 20) | CREATE (n:Label {id2: 2}));")
    execute_and_fetch_all(cursor, "FOREACH (i IN range(1, 20) | CREATE (n:Label {id2: 3}));")
    execute_and_fetch_all(cursor, "FOREACH (i IN range(1, 20) | CREATE (n:Label {id2: 4}));")
    execute_and_fetch_all(cursor, "FOREACH (i IN range(1, 20) | CREATE (n:Label {id2: 5}));")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Label(id1);")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Label(id2);")
    analyze_graph_results = execute_and_fetch_all(cursor, "ANALYZE GRAPH")
    if analyze_graph_results[0][1] == "id1":
        first_index = 0
    else:
        first_index = 1
    # Check results
    assert analyze_graph_results[first_index] == ("Label", "id1", 100, 5, 20, 32.5)
    assert analyze_graph_results[1 - first_index] == ("Label", "id2", 100, 5, 20, 0)
    expected_explain_after_analysis = [
        (f" * Produce {{n}}",),
        (f" * Filter",),
        (f" * ScanAllByLabelPropertyValue (n :Label {{id2}})",),
        (f" * Once",),
    ]
    assert (
        execute_and_fetch_all(cursor, "EXPLAIN MATCH (n:Label) WHERE n.id2 = 3 AND n.id1 = 3 RETURN n;")
        == expected_explain_after_analysis
    )
    assert len(execute_and_fetch_all(cursor, "ANALYZE GRAPH DELETE STATISTICS")) == 2
    execute_and_fetch_all(cursor, "DROP INDEX ON :Label(id1);")
    execute_and_fetch_all(cursor, "DROP INDEX ON :Label(id2);")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
