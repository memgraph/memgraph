#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

import time

from neo4j import GraphDatabase

NUM_NODES = 1000


def setup_data(session):
    print("Setting up data...")
    session.run("MATCH (n:ParallelNode) DETACH DELETE n").consume()
    # Batch create nodes
    query = """
    UNWIND range(1, $count) AS i
    CREATE (:ParallelNode {id: i, val: i % 100})
    """
    session.run(query, count=NUM_NODES).consume()

    # Create some edges for hops testing
    query_edges = """
    MATCH (n:ParallelNode), (m:ParallelNode)
    WHERE n.id % 2 = 0 AND m.id = n.id + 1
    CREATE (n)-[:REL]->(m)
    """
    session.run(query_edges).consume()

    print(f"Created {NUM_NODES} nodes and edges.")


def assert_read_only_summary(summary):
    # Timers (not supported)
    # assert summary.result_available_after is not None, "result_available_after is None (missing from server response)"
    # assert summary.result_available_after >= 0, f"result_available_after should be >= 0, got {summary.result_available_after}"
    # assert summary.result_consumed_after is not None, "result_consumed_after is None (missing from server response)"
    # assert summary.result_consumed_after >= 0, f"result_consumed_after should be >= 0, got {summary.result_consumed_after}"

    # Type
    # Note: query_type might be None depending on driver/server version negotiation,
    # but strictly it should be 'r' for these queries.
    if summary.query_type is not None:
        assert summary.query_type == "r", f"Expected query_type 'r', got '{summary.query_type}'"

    # Counters
    c = summary.counters
    assert c.nodes_created == 0, f"nodes_created should be 0, got {c.nodes_created}"
    assert c.nodes_deleted == 0, f"nodes_deleted should be 0, got {c.nodes_deleted}"
    assert c.relationships_created == 0, f"relationships_created should be 0, got {c.relationships_created}"
    assert c.relationships_deleted == 0, f"relationships_deleted should be 0, got {c.relationships_deleted}"
    assert c.properties_set == 0, f"properties_set should be 0, got {c.properties_set}"
    assert c.labels_added == 0, f"labels_added should be 0, got {c.labels_added}"
    assert c.labels_removed == 0, f"labels_removed should be 0, got {c.labels_removed}"
    assert c.indexes_added == 0, f"indexes_added should be 0, got {c.indexes_added}"
    assert c.indexes_removed == 0, f"indexes_removed should be 0, got {c.indexes_removed}"
    assert c.constraints_added == 0, f"constraints_added should be 0, got {c.constraints_added}"
    assert c.constraints_removed == 0, f"constraints_removed should be 0, got {c.constraints_removed}"
    assert c.system_updates == 0, f"system_updates should be 0, got {c.system_updates}"

    assert not c.contains_updates, "contains_updates should be False"
    assert not c.contains_system_updates, "contains_system_updates should be False"


def test_parallel_count(session):
    print("Testing Parallel Count...")
    query = "USING PARALLEL EXECUTION MATCH (n:ParallelNode) RETURN count(n) AS cnt"
    result = session.run(query)
    record = result.single()
    assert record["cnt"] == NUM_NODES, f"Expected {NUM_NODES}, got {record['cnt']}"

    summary = result.consume()
    assert_read_only_summary(summary)

    print("Parallel Count passed.")


def test_parallel_orderby(session):
    print("Testing Parallel Order By...")
    query = "USING PARALLEL EXECUTION MATCH (n:ParallelNode) RETURN n.id AS id ORDER BY n.id DESC LIMIT 10"
    result = session.run(query)
    records = list(result)
    assert len(records) == 10

    expected_ids = list(range(NUM_NODES, NUM_NODES - 10, -1))
    actual_ids = [r["id"] for r in records]
    assert actual_ids == expected_ids, f"Expected {expected_ids}, got {actual_ids}"

    summary = result.consume()
    assert_read_only_summary(summary)
    print("Parallel Order By passed.")


def test_parallel_filter_agg(session):
    print("Testing Parallel Filter + Aggregation...")
    cutoff = 500
    expected_count = NUM_NODES - cutoff
    query = "USING PARALLEL EXECUTION MATCH (n:ParallelNode) WHERE n.id > $cutoff RETURN count(n) AS cnt"
    result = session.run(query, cutoff=cutoff)
    record = result.single()
    assert record["cnt"] == expected_count, f"Expected {expected_count}, got {record['cnt']}"

    summary = result.consume()
    assert_read_only_summary(summary)
    print("Parallel Filter + Aggregation passed.")


def test_cost_estimate(session):
    print("Testing Cost Estimate (Single vs Parallel)...")
    query = "MATCH (n:ParallelNode) RETURN count(n) AS cnt"

    # Parallel
    res_p = session.run("USING PARALLEL EXECUTION " + query)
    res_p.consume()
    summary_p = res_p.consume()
    cost_p = summary_p.metadata.get("cost_estimate")

    # Single
    res_s = session.run(query)
    res_s.consume()
    summary_s = res_s.consume()
    cost_s = summary_s.metadata.get("cost_estimate")

    # print(f"Cost Estimate - Parallel: {cost_p}, Single: {cost_s}")

    assert cost_p is not None, "Parallel cost estimate is None"
    assert cost_s is not None, "Single cost estimate is None"
    # Relaxed check
    assert cost_p <= cost_s, f"Parallel cost ({cost_p}) should be <= Single cost ({cost_s})"
    print("Cost Estimate test passed.")


def test_metadata_fields(session):
    print("Testing Metadata Fields (hops, execution time)...")

    query_hops = "USING PARALLEL EXECUTION MATCH (n:ParallelNode)-[:REL]->(m) RETURN count(*)"
    start_time = time.time()
    res_hops = session.run(query_hops)
    summary_hops = res_hops.consume()
    end_time = time.time()
    execution_time = end_time - start_time
    assert_read_only_summary(summary_hops)

    hops = summary_hops.metadata.get("number_of_hops")
    # print(f"Number of hops: {hops}")
    assert hops is not None, "number_of_hops is None"
    assert hops > 0, "number_of_hops should be > 0 for this query"

    # Execution Time test
    exec_time = summary_hops.metadata.get("plan_execution_time")
    # print(f"Plan Execution Time: {exec_time}")
    assert exec_time is not None, "plan_execution_time is None"
    assert exec_time > 0, "plan_execution_time should be > 0"

    # Note: plan_execution_time is internal engine time, execution_time is client side.
    # We verify internal time is plausible (<= client time).
    assert exec_time <= execution_time, f"Plan execution time ({exec_time}) > Client side time ({execution_time})"

    print("Metadata Fields test passed.")


def test_profile(session):
    print("Testing PROFILE (Single vs Parallel)...")

    # Single
    query_s = "PROFILE MATCH (n:ParallelNode) RETURN count(n)"
    res_s = session.run(query_s)
    summary_s = res_s.consume()
    profile_s = summary_s.profile  # This is a dict-like structure from the driver

    # Parallel
    query_p = "PROFILE USING PARALLEL EXECUTION MATCH (n:ParallelNode) RETURN count(n)"
    res_p = session.run(query_p)
    summary_p = res_p.consume()
    profile_p = summary_p.profile

    assert profile_s is not None, "Single profile is None"
    assert profile_p is not None, "Parallel profile is None"

    print("PROFILE test passed.")


if __name__ == "__main__":
    print("Running Parallel Executions Tests...")
    with GraphDatabase.driver("bolt://localhost:7687", auth=None, encrypted=False) as driver:
        with driver.session() as session:
            setup_data(session)
            test_parallel_count(session)
            test_parallel_orderby(session)
            test_parallel_filter_agg(session)
            test_cost_estimate(session)
            test_metadata_fields(session)
            test_profile(session)

            # Cleanup
            session.run("MATCH (n:ParallelNode) DETACH DELETE n").consume()

    print("ALL TESTS PASSED.")
