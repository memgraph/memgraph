# Copyright 2025 Memgraph Ltd.
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
Tests for Trigger Context Collector in parallel execution.

The Trigger Context Collector collects information about created, deleted, and
updated vertices/edges during a transaction so that triggers can fire with the
appropriate context. When parallel execution is used, the context must be merged
from multiple execution branches.

This module tests:
    1. Triggers fire correctly when queries have both write and parallel parts
    2. All created/updated/deleted objects are captured in the trigger context
    3. Context is correctly merged from parallel execution branches
    4. Trigger context is complete even with complex parallel query patterns

Query patterns tested (from unit tests in query_parallel_plan.cpp):
    - MATCH(n) SET n:A WITH n MATCH(m) RETURN COUNT(*)
    - MATCH(n) SET n.prop = val WITH n MATCH(m) RETURN COUNT(*)
    - Queries with CREATE followed by parallel aggregation
    - Queries with DELETE followed by parallel aggregation
"""

import sys
import time
from typing import Any, Dict, List

import pytest
from common import clear_database, pq

# =============================================================================
# Helper Functions
# =============================================================================


def wait_for_trigger(memgraph, trigger_marker_label: str, max_wait_sec: float = 5.0) -> List[Dict[str, Any]]:
    """
    Wait for a trigger to execute by checking for marker nodes.
    Triggers in Memgraph run BEFORE or AFTER commit, so we need to wait.
    """
    start_time = time.time()
    while time.time() - start_time < max_wait_sec:
        result = memgraph.fetch_all(f"MATCH (n:{trigger_marker_label}) RETURN n")
        if result:
            return result
        time.sleep(0.1)
    return []


def get_trigger_results(memgraph, result_label: str = "TriggerResult") -> List[Dict[str, Any]]:
    """Get all trigger result marker nodes."""
    return memgraph.fetch_all(f"MATCH (n:{result_label}) RETURN n.count AS count, n.type AS type ORDER BY n.type")


def clear_triggers(memgraph):
    """Remove all triggers from the database."""
    triggers = memgraph.fetch_all("SHOW TRIGGERS")
    for trigger in triggers:
        trigger_name = trigger.get("trigger name")
        if trigger_name:
            memgraph.execute_query(f"DROP TRIGGER {trigger_name}")


def setup_test(memgraph):
    """Setup helper that clears database and triggers."""
    # IMPORTANT: Clear triggers FIRST, then database.
    # Otherwise, DELETE triggers would fire during clear_database
    # and create unwanted TriggerResult nodes.
    clear_triggers(memgraph)
    clear_database(memgraph)


# =============================================================================
# Test Classes
# =============================================================================


class TestTriggerWithParallelAggregation:
    """
    Tests for triggers firing when queries have write operations
    followed by parallel aggregation.

    Pattern: MATCH(n) SET n:Label WITH n MATCH(m) RETURN COUNT(*)
    The SET operation should trigger, and the aggregation runs in parallel.
    """

    @pytest.fixture(autouse=True)
    def setup(self, memgraph):
        """Set up test data and trigger."""
        self.memgraph = memgraph
        setup_test(memgraph)

        # Create initial nodes
        memgraph.execute_query(
            """
            UNWIND range(1, 100) AS i
            CREATE (:Node {id: i, value: i % 10})
            """
        )

    def test_set_label_with_parallel_count(self):
        """
        Test trigger fires correctly when SET label is followed by parallel COUNT.
        Pattern: MATCH(n) SET n:NewLabel WITH n MATCH(m) RETURN COUNT(*)
        """
        # Create trigger that counts updated vertices
        # Note: setVertexLabels is a list of {label: "name", vertices: [v1, v2, ...]}
        # We need to unwind the vertices inside each label entry to count nodes
        self.memgraph.execute_query(
            """
            CREATE TRIGGER test_trigger
            ON UPDATE BEFORE COMMIT EXECUTE
            UNWIND setVertexLabels AS labelInfo
            UNWIND labelInfo.vertices AS v
            WITH count(v) AS cnt
            CREATE (:TriggerResult {type: 'setLabels', count: cnt})
            """
        )

        # Execute query with write (SET) followed by parallel aggregation
        result = self.memgraph.fetch_all(
            """
            USING PARALLEL EXECUTION
            MATCH (n:Node)
            WHERE n.id <= 50
            SET n:UpdatedNode
            WITH n
            MATCH (m:Node)
            RETURN count(m) AS total
            """
        )

        # Verify the query returned correct count (cartesian: 50 nodes × 100 nodes)
        assert result[0]["total"] == 50 * 100

        # Verify trigger captured all label changes
        trigger_results = get_trigger_results(self.memgraph)
        assert len(trigger_results) == 1
        assert trigger_results[0]["type"] == "setLabels"
        assert trigger_results[0]["count"] == 50  # 50 nodes got the new label

    def test_set_property_with_parallel_aggregation(self):
        """
        Test trigger fires correctly when SET property is followed by parallel aggregation.
        """
        self.memgraph.execute_query(
            """
            CREATE TRIGGER prop_trigger
            ON UPDATE BEFORE COMMIT EXECUTE
            UNWIND setVertexProperties AS propInfo
            WITH count(propInfo) AS cnt
            CREATE (:TriggerResult {type: 'setProps', count: cnt})
            """
        )

        # Execute query with property SET followed by parallel aggregation
        result = self.memgraph.fetch_all(
            """
            USING PARALLEL EXECUTION
            MATCH (n:Node)
            SET n.updated = true
            WITH n
            MATCH (m:Node)
            RETURN count(*) AS total, sum(m.value) AS sum_val
            """
        )

        assert result[0]["total"] == 100 * 100  # 100 nodes * 100 iterations

        # Verify trigger captured all property changes
        trigger_results = get_trigger_results(self.memgraph)
        assert len(trigger_results) == 1
        assert trigger_results[0]["type"] == "setProps"
        assert trigger_results[0]["count"] == 100  # All 100 nodes got property set

    def test_remove_property_with_parallel_aggregation(self):
        """
        Test trigger fires when property removal is followed by parallel aggregation.
        """
        self.memgraph.execute_query(
            """
            CREATE TRIGGER remove_prop_trigger
            ON UPDATE BEFORE COMMIT EXECUTE
            UNWIND removedVertexProperties AS propInfo
            WITH count(propInfo) AS cnt
            CREATE (:TriggerResult {type: 'removedProps', count: cnt})
            """
        )

        # Execute query with REMOVE followed by parallel aggregation
        result = self.memgraph.fetch_all(
            """
            USING PARALLEL EXECUTION
            MATCH (n:Node)
            WHERE n.id <= 30
            REMOVE n.value
            WITH n
            MATCH (m:Node)
            RETURN count(m) AS total
            """
        )

        assert result[0]["total"] == 30 * 100  # 30 nodes * 100 iterations

        # Verify trigger captured property removals
        trigger_results = get_trigger_results(self.memgraph)
        assert len(trigger_results) == 1
        assert trigger_results[0]["type"] == "removedProps"
        assert trigger_results[0]["count"] == 30


class TestTriggerWithParallelOrderBy:
    """
    Tests for triggers firing when queries have write operations
    followed by parallel ORDER BY.

    Pattern: MATCH (n) SET n:Label WITH n RETURN n ORDER BY n.prop
    """

    @pytest.fixture(autouse=True)
    def setup(self, memgraph):
        """Set up test data."""
        self.memgraph = memgraph
        setup_test(memgraph)

        memgraph.execute_query(
            """
            UNWIND range(1, 100) AS i
            CREATE (:Item {id: i, priority: i % 10, name: 'item_' + toString(i)})
            """
        )

    def test_set_label_with_parallel_orderby(self):
        """
        Test trigger fires correctly when SET label is followed by parallel ORDER BY.
        """
        # Note: setVertexLabels is a list of {label: "name", vertices: [v1, v2, ...]}
        self.memgraph.execute_query(
            """
            CREATE TRIGGER orderby_trigger
            ON UPDATE BEFORE COMMIT EXECUTE
            UNWIND setVertexLabels AS labelInfo
            UNWIND labelInfo.vertices AS v
            WITH count(v) AS cnt
            CREATE (:TriggerResult {type: 'setLabels', count: cnt})
            """
        )

        # Execute query with SET followed by parallel ORDER BY
        result = self.memgraph.fetch_all(
            """
            USING PARALLEL EXECUTION
            MATCH (n:Item)
            WHERE n.priority < 5
            SET n:HighPriority
            WITH n
            RETURN n.id AS id
            ORDER BY n.priority, n.id
            """
        )

        # Should return items with priority 0-4 (50 items)
        assert len(result) == 50

        # Verify trigger captured all updates
        trigger_results = get_trigger_results(self.memgraph)
        assert len(trigger_results) == 1
        assert trigger_results[0]["count"] == 50


class TestTriggerWithCreateAndParallel:
    """
    Tests for triggers when CREATE is followed by parallel operations.
    """

    @pytest.fixture(autouse=True)
    def setup(self, memgraph):
        """Set up test data."""
        self.memgraph = memgraph
        setup_test(memgraph)

        # Create some base nodes
        memgraph.execute_query(
            """
            UNWIND range(1, 50) AS i
            CREATE (:Base {id: i})
            """
        )

    def test_create_with_parallel_count(self):
        """
        Test CREATE trigger fires when followed by parallel COUNT.
        Note: CREATE inhibits parallelization of the scan, but aggregation after WITH can be parallel.
        """
        self.memgraph.execute_query(
            """
            CREATE TRIGGER create_trigger
            ON () CREATE BEFORE COMMIT EXECUTE
            UNWIND createdVertices AS v
            WITH count(v) AS cnt
            CREATE (:TriggerResult {type: 'created', count: cnt})
            """
        )

        # This query creates nodes, then does aggregation
        # The aggregation part after accumulate can potentially be parallelized
        result = self.memgraph.fetch_all(
            """
            USING PARALLEL EXECUTION
            MATCH (b:Base)
            CREATE (n:NewNode {baseId: b.id})
            WITH n
            RETURN count(n) AS created_count
            """
        )

        assert result[0]["created_count"] == 50

        # Verify trigger captured all creates
        trigger_results = get_trigger_results(self.memgraph)
        assert len(trigger_results) == 1
        assert trigger_results[0]["type"] == "created"
        assert trigger_results[0]["count"] == 50

    def test_create_edge_with_parallel_aggregation(self):
        """
        Test edge CREATE trigger fires correctly.
        """
        # Create more base nodes for edge creation
        self.memgraph.execute_query(
            """
            UNWIND range(1, 50) AS i
            CREATE (:Target {id: i})
            """
        )

        self.memgraph.execute_query(
            """
            CREATE TRIGGER edge_create_trigger
            ON --> CREATE BEFORE COMMIT EXECUTE
            UNWIND createdEdges AS e
            WITH count(e) AS cnt
            CREATE (:TriggerResult {type: 'createdEdges', count: cnt})
            """
        )

        # Create edges between Base and Target nodes
        result = self.memgraph.fetch_all(
            """
            USING PARALLEL EXECUTION
            MATCH (b:Base), (t:Target)
            WHERE b.id = t.id
            CREATE (b)-[:CONNECTS]->(t)
            WITH b, t
            RETURN count(*) AS connections
            """
        )

        assert result[0]["connections"] == 50

        # Verify trigger captured edge creates
        trigger_results = get_trigger_results(self.memgraph)
        assert len(trigger_results) == 1
        assert trigger_results[0]["type"] == "createdEdges"
        assert trigger_results[0]["count"] == 50


class TestTriggerWithDeleteAndParallel:
    """
    Tests for triggers when DELETE is followed by parallel operations.
    """

    @pytest.fixture(autouse=True)
    def setup(self, memgraph):
        """Set up test data."""
        self.memgraph = memgraph
        setup_test(memgraph)

        # Create nodes to be deleted
        memgraph.execute_query(
            """
            UNWIND range(1, 100) AS i
            CREATE (:ToDelete {id: i, category: i % 5})
            """
        )

        # Create nodes that will remain
        memgraph.execute_query(
            """
            UNWIND range(1, 100) AS i
            CREATE (:Permanent {id: i})
            """
        )

    def test_delete_with_parallel_count(self):
        """
        Test DELETE trigger fires when followed by parallel COUNT.
        """
        self.memgraph.execute_query(
            """
            CREATE TRIGGER delete_trigger
            ON () DELETE BEFORE COMMIT EXECUTE
            UNWIND deletedVertices AS v
            WITH count(v) AS cnt
            CREATE (:TriggerResult {type: 'deleted', count: cnt})
            """
        )

        # Delete some nodes, then count remaining
        # Use aggregation in WITH to avoid cartesian product
        result = self.memgraph.fetch_all(
            """
            USING PARALLEL EXECUTION
            MATCH (d:ToDelete)
            WHERE d.category = 0
            DELETE d
            WITH count(*) AS deleted_count
            MATCH (p:Permanent)
            RETURN count(p) AS remaining
            """
        )

        assert result[0]["remaining"] == 100

        # Verify trigger captured deletes (category 0 = 20 nodes)
        trigger_results = get_trigger_results(self.memgraph)
        assert len(trigger_results) == 1
        assert trigger_results[0]["type"] == "deleted"
        assert trigger_results[0]["count"] == 20


class TestTriggerContextMerging:
    """
    Tests that verify trigger context is correctly merged from
    parallel execution branches.
    """

    @pytest.fixture(autouse=True)
    def setup(self, memgraph):
        """Set up test data."""
        self.memgraph = memgraph
        setup_test(memgraph)

        # Create nodes in different groups for parallel processing
        memgraph.execute_query(
            """
            UNWIND range(1, 200) AS i
            CREATE (:TestNode {
                id: i,
                group: i % 10,
                value: i * 10
            })
            """
        )

    def test_multiple_property_updates_merged(self):
        """
        Test that property updates from multiple parallel workers are all captured.
        """
        self.memgraph.execute_query(
            """
            CREATE TRIGGER merge_test_trigger
            ON UPDATE BEFORE COMMIT EXECUTE
            UNWIND setVertexProperties AS propInfo
            WITH count(propInfo) AS cnt
            CREATE (:TriggerResult {type: 'mergedProps', count: cnt})
            """
        )

        # Update all nodes - this should be distributed across workers
        result = self.memgraph.fetch_all(
            """
            USING PARALLEL EXECUTION
            MATCH (n:TestNode)
            SET n.processed = true, n.timestamp = timestamp()
            WITH n
            RETURN count(n) AS updated_count
            """
        )

        assert result[0]["updated_count"] == 200

        # All 200 nodes * 2 properties = 400 property changes
        trigger_results = get_trigger_results(self.memgraph)
        assert len(trigger_results) == 1
        assert trigger_results[0]["type"] == "mergedProps"
        # Each node gets 2 properties set
        assert trigger_results[0]["count"] == 400

    def test_mixed_operations_merged(self):
        """
        Test that a query with multiple types of updates has all captured in trigger.
        """
        # Note: setVertexLabels is [{label, vertices: [...]}], so we need to sum vertex counts
        # setVertexProperties is a flat list of property changes
        self.memgraph.execute_query(
            """
            CREATE TRIGGER any_trigger
            BEFORE COMMIT EXECUTE
            WITH reduce(total = 0, li IN setVertexLabels | total + size(li.vertices)) AS labels,
                 size(setVertexProperties) AS props
            CREATE (:TriggerResult {type: 'labels', count: labels})
            CREATE (:TriggerResult {type: 'props', count: props})
            """
        )

        # Set both labels and properties
        result = self.memgraph.fetch_all(
            """
            USING PARALLEL EXECUTION
            MATCH (n:TestNode)
            WHERE n.group < 5
            SET n:ProcessedGroup, n.groupProcessed = true
            WITH n
            RETURN count(n) AS processed
            """
        )

        # Groups 0-4 = 100 nodes
        assert result[0]["processed"] == 100

        trigger_results = get_trigger_results(self.memgraph)
        results_by_type = {r["type"]: r["count"] for r in trigger_results}

        assert results_by_type.get("labels") == 100
        assert results_by_type.get("props") == 100


class TestTriggerPhases:
    """
    Tests for BEFORE COMMIT and AFTER COMMIT trigger phases with parallel execution.
    """

    @pytest.fixture(autouse=True)
    def setup(self, memgraph):
        """Set up test data."""
        self.memgraph = memgraph
        setup_test(memgraph)

        memgraph.execute_query(
            """
            UNWIND range(1, 100) AS i
            CREATE (:DataNode {id: i, status: 'initial'})
            """
        )

    def test_before_commit_trigger_with_parallel(self):
        """
        Test BEFORE COMMIT trigger fires with complete context from parallel execution.
        """
        self.memgraph.execute_query(
            """
            CREATE TRIGGER before_commit_trigger
            ON UPDATE BEFORE COMMIT EXECUTE
            UNWIND updatedVertices AS updateInfo
            WITH count(updateInfo) AS cnt
            CREATE (:TriggerResult {type: 'beforeCommit', count: cnt})
            """
        )

        self.memgraph.fetch_all(
            """
            USING PARALLEL EXECUTION
            MATCH (n:DataNode)
            SET n.status = 'processed'
            WITH n
            RETURN count(n) AS processed
            """
        )

        trigger_results = get_trigger_results(self.memgraph)
        assert len(trigger_results) == 1
        assert trigger_results[0]["type"] == "beforeCommit"
        # updatedVertices contains all property changes
        assert trigger_results[0]["count"] == 100

    def test_after_commit_trigger_with_parallel(self):
        """
        Test AFTER COMMIT trigger fires with complete context from parallel execution.
        """
        self.memgraph.execute_query(
            """
            CREATE TRIGGER after_commit_trigger
            ON UPDATE AFTER COMMIT EXECUTE
            UNWIND setVertexProperties AS propInfo
            WITH count(propInfo) AS cnt
            CREATE (:TriggerResult {type: 'afterCommit', count: cnt})
            """
        )

        self.memgraph.fetch_all(
            """
            USING PARALLEL EXECUTION
            MATCH (n:DataNode)
            SET n.finalStatus = 'complete'
            WITH n
            RETURN count(n) AS processed
            """
        )

        # Wait a bit for AFTER COMMIT trigger
        time.sleep(0.5)

        trigger_results = get_trigger_results(self.memgraph)
        assert len(trigger_results) == 1
        assert trigger_results[0]["type"] == "afterCommit"
        assert trigger_results[0]["count"] == 100


class TestComplexParallelTriggerScenarios:
    """
    Complex scenarios testing trigger context collection with various
    parallel query patterns.
    """

    @pytest.fixture(autouse=True)
    def setup(self, memgraph):
        """Set up test data."""
        self.memgraph = memgraph
        setup_test(memgraph)

        # Create a graph with relationships
        memgraph.execute_query(
            """
            UNWIND range(1, 50) AS i
            CREATE (a:Person {id: i, name: 'Person_' + toString(i)})
            """
        )

        memgraph.execute_query(
            """
            MATCH (p1:Person), (p2:Person)
            WHERE p1.id < p2.id AND p1.id % 5 = p2.id % 5
            CREATE (p1)-[:KNOWS {since: 2020}]->(p2)
            """
        )

    def test_update_with_cartesian_and_aggregation(self):
        """
        Test trigger with query that has update, cartesian product, and aggregation.
        Pattern: MATCH(n) SET n:A WITH n MATCH(m) RETURN count(*)
        """
        # Note: setVertexLabels is [{label, vertices: [...]}]
        self.memgraph.execute_query(
            """
            CREATE TRIGGER cartesian_trigger
            ON UPDATE BEFORE COMMIT EXECUTE
            UNWIND setVertexLabels AS labelInfo
            UNWIND labelInfo.vertices AS v
            WITH count(v) AS cnt
            CREATE (:TriggerResult {type: 'cartesianUpdate', count: cnt})
            """
        )

        # This creates a cartesian product after the update
        result = self.memgraph.fetch_all(
            """
            USING PARALLEL EXECUTION
            MATCH (n:Person)
            WHERE n.id <= 10
            SET n:VIP
            WITH n
            MATCH (m:Person)
            RETURN count(m) AS total_persons
            """
        )

        # Cartesian: 10 updated nodes × 50 total persons = 500 rows, count(m) per row is 1
        # Total count is 500 (cartesian product result)
        assert result[0]["total_persons"] == 10 * 50

        trigger_results = get_trigger_results(self.memgraph)
        assert len(trigger_results) == 1
        assert trigger_results[0]["count"] == 10

    def test_update_relationship_with_parallel_aggregation(self):
        """
        Test edge property updates are captured with parallel aggregation.
        """
        self.memgraph.execute_query(
            """
            CREATE TRIGGER edge_update_trigger
            ON --> UPDATE BEFORE COMMIT EXECUTE
            UNWIND setEdgeProperties AS propInfo
            WITH count(propInfo) AS cnt
            CREATE (:TriggerResult {type: 'edgeUpdate', count: cnt})
            """
        )

        result = self.memgraph.fetch_all(
            """
            USING PARALLEL EXECUTION
            MATCH (p1:Person)-[r:KNOWS]->(p2:Person)
            SET r.updated = true
            WITH r
            RETURN count(r) AS updated_edges
            """
        )

        edge_count = result[0]["updated_edges"]

        trigger_results = get_trigger_results(self.memgraph)
        assert len(trigger_results) == 1
        assert trigger_results[0]["type"] == "edgeUpdate"
        assert trigger_results[0]["count"] == edge_count

    def test_multiple_with_clauses_and_trigger(self):
        """
        Test trigger context is complete with multiple WITH clauses.
        """
        # Note: setVertexLabels is [{label, vertices: [...]}], so we need to sum vertex counts
        self.memgraph.execute_query(
            """
            CREATE TRIGGER multi_with_trigger
            ON UPDATE BEFORE COMMIT EXECUTE
            WITH size(setVertexProperties) AS props,
                 reduce(total = 0, li IN setVertexLabels | total + size(li.vertices)) AS labels
            CREATE (:TriggerResult {type: 'props', count: props})
            CREATE (:TriggerResult {type: 'labels', count: labels})
            """
        )

        result = self.memgraph.fetch_all(
            """
            USING PARALLEL EXECUTION
            MATCH (n:Person)
            WHERE n.id <= 20
            SET n.step1 = true
            WITH n
            SET n:Step1Complete
            WITH n
            SET n.step2 = true
            WITH n
            RETURN count(n) AS processed
            """
        )

        assert result[0]["processed"] == 20

        trigger_results = get_trigger_results(self.memgraph)
        results_by_type = {r["type"]: r["count"] for r in trigger_results}

        # Each of 20 nodes gets: step1=true, step2=true (2 props) and one label
        assert results_by_type.get("props") == 40  # 20 nodes * 2 properties
        assert results_by_type.get("labels") == 20


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
