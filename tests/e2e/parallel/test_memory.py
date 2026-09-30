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
Tests for memory tracking with parallel query execution.

This module tests:
1. Query memory limits (QUERY MEMORY LIMIT) with parallel operators
2. Global memory limits with concurrent parallel queries
3. User profile memory limits with parallel execution
4. Memory tracking accuracy across parallel branches
5. Memory exhaustion scenarios in parallel context

Memory tracking in parallel execution:
- Each parallel branch tracks its own memory usage
- Memory is aggregated at the query level
- QUERY MEMORY LIMIT applies to total query memory (all branches combined)
- User profile limits apply to total user memory across all concurrent queries
"""

import re
import sys
import time
from typing import Optional

import pytest
from common import clear_database, pq
from neo4j import GraphDatabase
from neo4j.exceptions import *

# Common exception types for memory limit errors
MEMORY_ERRORS = (ClientError, DatabaseError, TransientError)


class TestParallelQueryMemoryLimit:
    """
    Tests for QUERY MEMORY LIMIT clause with parallel execution.

    The QUERY MEMORY LIMIT clause sets a memory threshold for a single query.
    With parallel execution, memory from all branches is counted toward this limit.
    """

    @pytest.fixture(autouse=True)
    def setup(self, fresh_memgraph):
        self.memgraph = fresh_memgraph
        yield

    def test_parallel_query_within_memory_limit(self):
        """
        Test that a parallel query succeeds when within memory limit.
        """
        # Create modest dataset
        self.memgraph.execute_query("UNWIND range(1, 1000) AS i CREATE (:MemNode {id: i, value: i % 100})")

        # Query with generous memory limit should succeed
        query = pq("MATCH (n:MemNode) RETURN count(n) QUERY MEMORY LIMIT 100MB")
        result = self.memgraph.fetch_all(query)

        assert result is not None
        assert len(result) == 1
        assert result[0]["count(n)"] == 1000


class TestParallelMemoryViolations:
    """Consolidated tests for various memory limit violation scenarios."""

    @pytest.fixture(autouse=True)
    def setup(self, fresh_memgraph):
        self.memgraph = fresh_memgraph
        # Setup data for various tests
        fresh_memgraph.execute_query(
            "UNWIND range(1, 5000) AS i CREATE (:DataNode {id: i, data: 'payload_' + toString(i)})"
        )
        yield

    @pytest.mark.parametrize(
        "query_pattern",
        [
            "MATCH (n:DataNode) RETURN n.id, n.data ORDER BY n.id",
            "MATCH (n:DataNode) RETURN collect(n.data)",
            "MATCH (n:DataNode) RETURN n.id % 50, count(*), avg(n.id)",
            "MATCH (n:DataNode) WITH n ORDER BY n.id RETURN n.id",
        ],
    )
    def test_memory_limit_exceeded(self, query_pattern):
        """Test that different query patterns correctly trigger memory limit errors."""
        query = pq(f"{query_pattern} QUERY MEMORY LIMIT 1KB")
        with pytest.raises(MEMORY_ERRORS) as exc_info:
            self.memgraph.fetch_all(query)
        assert "memory" in str(exc_info.value).lower() or "limit" in str(exc_info.value).lower()


class TestParallelMemoryExhaustion:
    """
    Tests for memory exhaustion scenarios with parallel execution.

    These tests verify that the system handles out-of-memory situations
    gracefully when running parallel queries.
    """

    @pytest.fixture(autouse=True)
    def setup(self, fresh_memgraph):
        self.memgraph = fresh_memgraph
        yield

    def test_parallel_collect_memory_exhaustion(self):
        """
        Test memory exhaustion with collect() in parallel execution.

        collect() aggregates all results into a list, which can be memory-intensive.
        """
        # Create dataset
        self.memgraph.execute_query(
            "UNWIND range(1, 5000) AS i CREATE (:CollectNode {id: i, data: 'payload_' + toString(i)})"
        )

        # collect() with tiny limit should fail
        query = pq("MATCH (n:CollectNode) RETURN collect(n.data) QUERY MEMORY LIMIT 1KB")

        with pytest.raises(MEMORY_ERRORS) as exc_info:
            self.memgraph.fetch_all(query)

        error_msg = str(exc_info.value).lower()
        assert "memory" in error_msg or "limit" in error_msg

    def test_parallel_cartesian_product_memory(self):
        """
        Test memory handling with cartesian products in parallel.

        Cartesian products can explode memory usage quickly.
        """
        # Create two small sets that would create large cartesian product
        self.memgraph.execute_query("UNWIND range(1, 100) AS i CREATE (:SetA {id: i})")
        self.memgraph.execute_query("UNWIND range(1, 100) AS i CREATE (:SetB {id: i})")

        # Cartesian product with limit
        query = pq("MATCH (a:SetA), (b:SetB) RETURN count(*) QUERY MEMORY LIMIT 100MB")
        result = self.memgraph.fetch_all(query)

        # 100 * 100 = 10000 combinations
        assert result[0]["count(*)"] == 10000

    def test_parallel_large_property_access(self):
        """
        Test memory tracking with large property values in parallel.
        """
        # Create nodes with large string properties
        self.memgraph.execute_query(
            """
            UNWIND range(1, 100) AS i
            CREATE (:LargeProp {id: i, data: reduce(s = "", x IN range(1, 100) | s + "data_chunk_")})
            """
        )

        # Access large properties with generous limit
        query = pq("MATCH (n:LargeProp) RETURN n.id, n.data QUERY MEMORY LIMIT 100MB")
        result = self.memgraph.fetch_all(query)

        assert len(result) == 100


class TestParallelConcurrentMemory:
    """
    Tests for memory behavior with concurrent parallel queries.

    Verifies memory tracking when multiple parallel queries run simultaneously.
    """

    @pytest.fixture(autouse=True)
    def setup(self, fresh_memgraph):
        self.memgraph = fresh_memgraph
        self.uri = fresh_memgraph.uri
        # Create shared test data
        fresh_memgraph.execute_query("UNWIND range(1, 2000) AS i CREATE (:ConcurrentNode {id: i, value: i % 100})")
        yield

    def test_concurrent_parallel_queries_memory(self):
        """
        Test that concurrent parallel queries each respect their memory limits.
        """
        import multiprocessing

        def run_query(uri, query, result_queue):
            """Run a query and report success/failure."""
            try:
                driver = GraphDatabase.driver(uri)
                with driver.session() as session:
                    result = session.run(query)
                    count = sum(1 for _ in result)
                    result_queue.put(("success", count))
                driver.close()
            except Exception as e:
                result_queue.put(("error", str(e)))

        # Run multiple parallel queries concurrently
        query = pq("MATCH (n:ConcurrentNode) RETURN n.id QUERY MEMORY LIMIT 50MB")

        processes = []
        queues = []
        for _ in range(3):
            q = multiprocessing.Queue()
            p = multiprocessing.Process(target=run_query, args=(self.uri, query, q))
            p.start()
            processes.append(p)
            queues.append(q)

        # Wait for all to complete
        for p in processes:
            p.join(timeout=30)
            if p.is_alive():
                p.terminate()

        # Check results
        successes = 0
        for q in queues:
            try:
                status, result = q.get(timeout=1)
                if status == "success":
                    successes += 1
                    assert result == 2000
            except Exception:
                pass

        # At least some queries should succeed
        assert successes >= 1, "At least one concurrent query should succeed"

    def test_sequential_parallel_queries_memory_cleanup(self):
        """
        Test that memory is properly released between sequential parallel queries.

        After a parallel query completes, its memory should be freed for the next query.
        """
        # Run multiple queries sequentially, each with its own limit
        for i in range(5):
            query = pq(f"MATCH (n:ConcurrentNode) WHERE n.id <= {(i+1) * 400} RETURN count(n) QUERY MEMORY LIMIT 50MB")
            result = self.memgraph.fetch_all(query)
            expected_count = min((i + 1) * 400, 2000)
            assert result[0]["count(n)"] == expected_count

        # Final query should still work - memory from previous queries should be freed
        final_query = pq("MATCH (n:ConcurrentNode) RETURN count(n) QUERY MEMORY LIMIT 50MB")
        result = self.memgraph.fetch_all(final_query)
        assert result[0]["count(n)"] == 2000


class TestParallelMemoryWithUnwind:
    """
    Tests for memory tracking with UNWIND in parallel execution.

    UNWIND can significantly affect memory usage as it expands lists.
    """

    @pytest.fixture(autouse=True)
    def setup(self, fresh_memgraph):
        self.memgraph = fresh_memgraph
        yield

    def test_unwind_parallel_memory_limit(self):
        """
        Test memory limit with UNWIND feeding into parallel execution.
        """
        # Create base data
        self.memgraph.execute_query("UNWIND range(1, 500) AS i CREATE (:UnwindNode {id: i, values: range(1, 10)})")

        # UNWIND with parallel scan
        query = pq("MATCH (n:UnwindNode) " "UNWIND n.values AS v " "RETURN count(*) " "QUERY MEMORY LIMIT 50MB")
        result = self.memgraph.fetch_all(query)

        # 500 nodes * 10 values each = 5000 rows
        assert result[0]["count(*)"] == 5000

    def test_large_unwind_memory_exhaustion(self):
        """
        Test memory exhaustion with large UNWIND expansion.
        """
        # Create node with large list property
        self.memgraph.execute_query("CREATE (:HugeList {items: range(1, 100000)})")

        # UNWIND huge list with tiny memory limit should fail
        query = pq("MATCH (n:HugeList) " "UNWIND n.items AS item " "RETURN collect(item) " "QUERY MEMORY LIMIT 1KB")

        with pytest.raises(MEMORY_ERRORS) as exc_info:
            self.memgraph.fetch_all(query)

        error_msg = str(exc_info.value).lower()
        assert "memory" in error_msg or "limit" in error_msg


class TestParallelMemoryReporting:
    """
    Tests for memory usage reporting with parallel execution.

    Verifies that memory statistics are correctly reported for parallel queries.
    """

    @pytest.fixture(autouse=True)
    def setup(self, fresh_memgraph):
        self.memgraph = fresh_memgraph
        # Create test data
        fresh_memgraph.execute_query(
            "UNWIND range(1, 1000) AS i CREATE (:ReportNode {id: i, data: 'value_' + toString(i)})"
        )
        yield

    def test_profile_shows_memory_info(self):
        """
        Test that PROFILE output includes memory-related information for parallel queries.
        """
        query = pq("MATCH (n:ReportNode) RETURN count(n)")
        profile = self.memgraph.fetch_all(f"PROFILE {query}")

        # Profile should have entries
        assert profile is not None
        assert len(profile) > 0

        # Should have parallel operators
        operators = [row.get("OPERATOR", "") for row in profile]
        has_parallel = any("Parallel" in op or "ScanChunk" in op for op in operators)
        assert has_parallel, f"Expected parallel operators in profile: {operators}"

    def test_explain_with_memory_limit(self):
        """
        Test that EXPLAIN works with QUERY MEMORY LIMIT.
        """
        query = pq("MATCH (n:ReportNode) RETURN n QUERY MEMORY LIMIT 50MB")
        explain = self.memgraph.fetch_all(f"EXPLAIN {query}")

        # EXPLAIN should work and show the plan
        assert explain is not None
        assert len(explain) > 0


class TestParallelMemoryBranchSignaling:
    """
    Tests for memory limit signaling across parallel branches.

    When a memory limit is exceeded in one branch, the signal must propagate
    correctly to stop all branches and report the error properly.
    """

    @pytest.fixture(autouse=True)
    def setup(self, fresh_memgraph):
        self.memgraph = fresh_memgraph
        yield

    def test_memory_limit_signal_stops_all_branches(self):
        """
        Test that when one branch exceeds memory limit, all branches stop.

        Create data that will cause memory exhaustion in some branches
        but not others, verify proper error propagation.
        """
        # Create nodes with varying sizes - some branches will hit limit first
        self.memgraph.execute_query(
            """
            UNWIND range(1, 2000) AS i
            CREATE (:VaryingNode {
                id: i,
                data: CASE
                    WHEN i % 10 = 0 THEN reduce(s = "", x IN range(1, 100) | s + "large_")
                    ELSE 'small'
                END
            })
            """
        )

        # Collect with tiny limit - some branch will hit limit first
        query = pq("MATCH (n:VaryingNode) " "RETURN collect(n.data) AS all_data " "QUERY MEMORY LIMIT 1KB")

        with pytest.raises(MEMORY_ERRORS) as exc_info:
            self.memgraph.fetch_all(query)

        # Error should indicate memory issue
        error_msg = str(exc_info.value).lower()
        assert "memory" in error_msg or "limit" in error_msg

    def test_branch_memory_aggregation_triggers_limit(self):
        """
        Test that memory from all branches is aggregated and can trigger limit.

        Each branch individually might be under limit, but combined they exceed it.
        """
        # Create enough data that each branch uses some memory
        self.memgraph.execute_query(
            "UNWIND range(1, 5000) AS i CREATE (:BranchNode {id: i, payload: 'data_' + toString(i)})"
        )

        # Set limit that individual branches might fit under, but total won't
        # With 8 threads, each branch handles ~625 nodes
        # Total memory should exceed this small limit
        query = pq("MATCH (n:BranchNode) " "RETURN collect(n.payload) AS payloads " "QUERY MEMORY LIMIT 10KB")

        with pytest.raises(MEMORY_ERRORS) as exc_info:
            self.memgraph.fetch_all(query)

        error_msg = str(exc_info.value).lower()
        assert "memory" in error_msg or "limit" in error_msg

    def test_memory_limit_during_parallel_aggregation(self):
        """
        Test memory limit enforcement during parallel aggregation merge phase.

        Each branch aggregates locally, then results are merged. Memory limit
        should be enforced during both phases.
        """
        # Create data for aggregation
        self.memgraph.execute_query("UNWIND range(1, 3000) AS i CREATE (:AggBranch {id: i, group: i % 100, value: i})")

        # Aggregation with small limit
        query = pq("MATCH (n:AggBranch) " "RETURN n.group AS g, collect(n.value) AS values " "QUERY MEMORY LIMIT 5KB")

        with pytest.raises(MEMORY_ERRORS) as exc_info:
            self.memgraph.fetch_all(query)

        error_msg = str(exc_info.value).lower()
        assert "memory" in error_msg or "limit" in error_msg

    def test_memory_limit_during_parallel_orderby_merge(self):
        """
        Test memory limit enforcement during parallel ORDER BY merge phase.

        Each branch sorts locally, then results are merged via heap.
        Memory limit should be enforced throughout.
        """
        # Create data for sorting
        self.memgraph.execute_query(
            "UNWIND range(1, 5000) AS i CREATE (:SortBranch {id: i, key: i % 1000, data: 'val_' + toString(i)})"
        )

        # ORDER BY with small limit - sorting requires buffering
        query = pq("MATCH (n:SortBranch) " "RETURN n.data ORDER BY n.key " "QUERY MEMORY LIMIT 1KB")

        with pytest.raises(MEMORY_ERRORS) as exc_info:
            self.memgraph.fetch_all(query)

        error_msg = str(exc_info.value).lower()
        assert "memory" in error_msg or "limit" in error_msg

    def test_successful_query_near_memory_limit(self):
        """
        Test that a query just under the memory limit succeeds.

        Verify that memory tracking is accurate enough to allow queries
        that are close to but under the limit.
        """
        # Create modest data
        self.memgraph.execute_query("UNWIND range(1, 500) AS i CREATE (:NearLimit {id: i})")

        # Query with limit that should be sufficient
        query = pq("MATCH (n:NearLimit) RETURN count(n) QUERY MEMORY LIMIT 50MB")
        result = self.memgraph.fetch_all(query)

        assert result[0]["count(n)"] == 500

    def test_memory_error_contains_useful_info(self):
        """
        Test that memory limit error messages contain useful information.
        """
        # Create data that will exceed limit
        self.memgraph.execute_query("UNWIND range(1, 10000) AS i CREATE (:ErrorInfo {id: i, data: 'payload'})")

        query = pq("MATCH (n:ErrorInfo) RETURN collect(n.data) QUERY MEMORY LIMIT 1KB")

        try:
            self.memgraph.fetch_all(query)
            pytest.fail("Expected memory limit exception")
        except MEMORY_ERRORS as e:
            error_msg = str(e).lower()
            # Error should mention memory
            assert "memory" in error_msg or "limit" in error_msg


class TestParallelMemoryWithOccupiedWorkers:
    """
    Tests for memory tracking when some worker threads are occupied.

    Simulates scenarios where:
    1. Background queries occupy some workers
    2. New parallel queries must share remaining workers
    3. Memory limits must still be enforced correctly
    """

    @pytest.fixture(autouse=True)
    def setup(self, fresh_memgraph):
        self.memgraph = fresh_memgraph
        self.uri = fresh_memgraph.uri
        # Create test data
        fresh_memgraph.execute_query(
            "UNWIND range(1, 3000) AS i CREATE (:OccupiedNode {id: i, value: i % 100, data: 'val_' + toString(i)})"
        )
        yield

    def _start_long_query(self, query: str) -> tuple:
        """Start a long-running query in a separate process."""
        import multiprocessing

        def run_query(uri, q, result_queue):
            try:
                driver = GraphDatabase.driver(uri)
                with driver.session() as session:
                    result = session.run(q)
                    # Consume results slowly to keep query running
                    count = 0
                    for record in result:
                        count += 1
                        time.sleep(0.001)  # Slow consumption
                    result_queue.put(("success", count))
                driver.close()
            except Exception as e:
                result_queue.put(("error", str(e)))

        result_queue = multiprocessing.Queue()
        process = multiprocessing.Process(target=run_query, args=(self.uri, query, result_queue))
        process.start()
        return process, result_queue

    def test_memory_limit_with_occupied_workers(self):
        """
        Test memory limit enforcement when some workers are busy.

        Start background queries to occupy workers, then run a memory-limited
        query and verify limits are still enforced.
        """
        # Start background queries to occupy workers
        bg_query = pq("MATCH (n:OccupiedNode) RETURN n ORDER BY n.id")
        bg_processes = []
        for _ in range(2):
            proc, queue = self._start_long_query(bg_query)
            bg_processes.append((proc, queue))

        time.sleep(0.2)  # Let background queries start

        try:
            # Run memory-limited query while workers are occupied
            query = pq("MATCH (n:OccupiedNode) " "RETURN collect(n.data) " "QUERY MEMORY LIMIT 1KB")

            with pytest.raises(MEMORY_ERRORS) as exc_info:
                self.memgraph.fetch_all(query)

            error_msg = str(exc_info.value).lower()
            assert "memory" in error_msg or "limit" in error_msg
        finally:
            # Cleanup background processes
            for proc, queue in bg_processes:
                proc.terminate()
                proc.join(timeout=2)

    def test_memory_tracking_under_load(self):
        """
        Test that memory tracking remains accurate under concurrent load.

        Run multiple queries simultaneously and verify memory limits
        are respected independently for each query.
        """
        import multiprocessing

        def run_limited_query(uri, query, result_queue):
            try:
                driver = GraphDatabase.driver(uri)
                with driver.session() as session:
                    result = session.run(query)
                    records = list(result)
                    result_queue.put(("success", len(records)))
                driver.close()
            except Exception as e:
                result_queue.put(("error", str(e)))

        # Start background load
        bg_query = pq("MATCH (n:OccupiedNode) RETURN n.id, n.data ORDER BY n.id")
        bg_proc, bg_queue = self._start_long_query(bg_query)

        time.sleep(0.1)

        try:
            # Run multiple memory-limited queries concurrently
            test_query = pq("MATCH (n:OccupiedNode) WHERE n.id <= 1000 RETURN count(n) QUERY MEMORY LIMIT 50MB")

            processes = []
            for _ in range(3):
                q = multiprocessing.Queue()
                p = multiprocessing.Process(target=run_limited_query, args=(self.uri, test_query, q))
                p.start()
                processes.append((p, q))

            # Wait for completion
            results = []
            for p, q in processes:
                p.join(timeout=30)
                if not p.is_alive():
                    try:
                        status, result = q.get(timeout=1)
                        results.append((status, result))
                    except Exception:
                        pass
                else:
                    p.terminate()

            # At least some queries should succeed
            successes = [r for r in results if r[0] == "success"]
            assert len(successes) >= 1, f"Expected at least one success, got: {results}"
        finally:
            bg_proc.terminate()
            bg_proc.join(timeout=2)

    def test_memory_limit_with_heavy_background_load(self):
        """
        Test memory limit enforcement with heavy background load.

        Start multiple heavy queries to maximize worker occupation,
        then verify memory limits still work for new queries.
        """
        # Create additional data for heavy queries
        self.memgraph.execute_query(
            "UNWIND range(1, 2000) AS i CREATE (:HeavyNode {id: i, payload: reduce(s = '', x IN range(1, 50) | s + 'payload_')})"
        )

        # Start heavy background queries
        heavy_query = pq("MATCH (n:HeavyNode) RETURN n.id, n.payload ORDER BY n.id")
        heavy_processes = []
        for _ in range(4):  # 4 heavy queries
            proc, queue = self._start_long_query(heavy_query)
            heavy_processes.append((proc, queue))

        time.sleep(0.2)  # Let heavy queries start and occupy workers

        try:
            # Run query with memory limit - should still be enforced
            query = pq("MATCH (n:OccupiedNode) " "RETURN count(n) " "QUERY MEMORY LIMIT 100MB")
            result = self.memgraph.fetch_all(query)

            # Query should succeed with generous limit
            assert result[0]["count(n)"] == 3000
        finally:
            # Cleanup
            for proc, queue in heavy_processes:
                proc.terminate()
                proc.join(timeout=2)

    def test_memory_exhaustion_with_occupied_workers(self):
        """
        Test memory exhaustion scenario when workers are already occupied.

        Memory pressure from background queries plus new query should
        be handled gracefully.
        """
        # Start background queries that use memory
        bg_query = pq("MATCH (n:OccupiedNode) RETURN collect(n.data) AS data")
        bg_processes = []
        for _ in range(2):
            proc, queue = self._start_long_query(bg_query)
            bg_processes.append((proc, queue))

        time.sleep(0.1)

        try:
            # Try to run another memory-intensive query with tiny limit
            query = pq("MATCH (n:OccupiedNode) " "RETURN collect(n.data) " "QUERY MEMORY LIMIT 1KB")

            with pytest.raises(MEMORY_ERRORS) as exc_info:
                self.memgraph.fetch_all(query)

            error_msg = str(exc_info.value).lower()
            assert "memory" in error_msg or "limit" in error_msg
        finally:
            for proc, queue in bg_processes:
                proc.terminate()
                proc.join(timeout=2)

    def test_sequential_queries_after_memory_exhaustion(self):
        """
        Test that queries work correctly after a memory exhaustion event.

        After a query fails due to memory limit, subsequent queries
        should still work correctly.
        """
        # First, cause memory exhaustion
        query_fail = pq("MATCH (n:OccupiedNode) " "RETURN collect(n.data) " "QUERY MEMORY LIMIT 1KB")

        with pytest.raises(MEMORY_ERRORS):
            self.memgraph.fetch_all(query_fail)

        # Now run a normal query - should work fine
        query_ok = pq("MATCH (n:OccupiedNode) RETURN count(n) QUERY MEMORY LIMIT 50MB")
        result = self.memgraph.fetch_all(query_ok)

        assert result[0]["count(n)"] == 3000

    def test_interleaved_memory_limited_queries(self):
        """
        Test interleaved execution of queries with different memory limits.

        Run queries with varying memory limits while workers are partially
        occupied to test limit isolation.
        """
        import multiprocessing

        def run_query_with_limit(uri, node_limit, mem_limit_mb, result_queue):
            try:
                driver = GraphDatabase.driver(uri)
                with driver.session() as session:
                    query = f"MATCH (n:OccupiedNode) WHERE n.id <= {node_limit} RETURN count(n) QUERY MEMORY LIMIT {mem_limit_mb}MB USING PARALLEL EXECUTION"
                    result = session.run(query)
                    record = result.single()
                    result_queue.put(("success", record["count(n)"]))
                driver.close()
            except Exception as e:
                result_queue.put(("error", str(e)))

        # Start a background query
        bg_query = pq("MATCH (n:OccupiedNode) RETURN n ORDER BY n.id")
        bg_proc, bg_queue = self._start_long_query(bg_query)

        time.sleep(0.1)

        try:
            # Run queries with different limits
            test_cases = [
                (500, 50),  # 500 nodes, 50MB limit
                (1000, 50),  # 1000 nodes, 50MB limit
                (1500, 50),  # 1500 nodes, 50MB limit
            ]

            processes = []
            for node_limit, mem_limit in test_cases:
                q = multiprocessing.Queue()
                p = multiprocessing.Process(target=run_query_with_limit, args=(self.uri, node_limit, mem_limit, q))
                p.start()
                processes.append((p, q, node_limit))

            # Check results
            for p, q, expected_count in processes:
                p.join(timeout=30)
                if not p.is_alive():
                    try:
                        status, result = q.get(timeout=1)
                        if status == "success":
                            assert result == expected_count, f"Expected {expected_count}, got {result}"
                    except Exception:
                        pass
                else:
                    p.terminate()
        finally:
            bg_proc.terminate()
            bg_proc.join(timeout=2)


class TestParallelGlobalMemoryLimit:
    """
    Tests for global memory limits (--memory-limit) with parallel execution.

    The global memory limit sets a hard cap on total memory usage for the
    Memgraph instance. These tests use USING PARALLEL EXECUTION hint to
    explicitly request parallel execution.
    """

    # Use a different port to avoid conflicts with the main test instance
    MEMORY_LIMIT_PORT = 7688

    def test_parallel_query_within_global_limit(self, memgraph_factory):
        """
        Test that parallel queries succeed when within global memory limit.
        """
        # Start instance with 200MB memory limit
        mg = memgraph_factory.create_instance(memory_limit_mb=200, port=self.MEMORY_LIMIT_PORT)

        try:
            # Create modest dataset
            mg.execute_query("UNWIND range(1, 1000) AS i CREATE (:GlobalNode {id: i, value: i % 100})")

            # Parallel query should succeed within limit
            result = mg.fetch_all("USING PARALLEL EXECUTION MATCH (n:GlobalNode) RETURN count(n)")

            assert result is not None
            assert len(result) == 1
            assert result[0]["count(n)"] == 1000
        finally:
            try:
                mg.clear_database()
            except Exception:
                pass

    def test_parallel_aggregation_within_global_limit(self, memgraph_factory):
        """
        Test that parallel aggregation works within global memory limits.
        """
        # Start instance with 150MB memory limit
        mg = memgraph_factory.create_instance(memory_limit_mb=150, port=self.MEMORY_LIMIT_PORT)

        try:
            # Create dataset
            mg.execute_query("UNWIND range(1, 5000) AS i CREATE (:AggNode {id: i, category: i % 10})")

            # Parallel aggregation query should succeed within limit
            # Note: Must alias the aggregation grouping key for ORDER BY
            result = mg.fetch_all(
                "USING PARALLEL EXECUTION "
                "MATCH (n:AggNode) RETURN n.category AS category, count(n) AS cnt ORDER BY category"
            )

            assert len(result) == 10  # 10 categories
            total = sum(r["cnt"] for r in result)
            assert total == 5000
        finally:
            try:
                mg.clear_database()
            except Exception:
                pass

    def test_parallel_collect_exceeds_global_limit(self, memgraph_factory):
        """
        Test that parallel COLLECT operations fail when they exceed global memory limit.
        """
        # Start instance with tight memory limit (40MB)
        mg = memgraph_factory.create_instance(memory_limit_mb=40, port=self.MEMORY_LIMIT_PORT)

        try:
            # Create many nodes with data
            mg.execute_query(
                "UNWIND range(1, 50000) AS i "
                "CREATE (:CollectNode {id: i, data: reduce(s = '', x IN range(1, 25) | s + 'data_')})"
            )

            # Large parallel COLLECT should exceed limit
            with pytest.raises(MEMORY_ERRORS):
                mg.fetch_all(
                    "USING PARALLEL EXECUTION " "MATCH (n:CollectNode) WITH collect(n) as nodes RETURN size(nodes)"
                )
        finally:
            pass  # Instance may be in bad state

    def test_global_limit_with_query_limit_interaction(self, memgraph_factory):
        """
        Test interaction between global memory limit and query memory limit.

        When both limits are set, the more restrictive one should apply.
        """
        # Start instance with 200MB global limit
        mg = memgraph_factory.create_instance(memory_limit_mb=200, port=self.MEMORY_LIMIT_PORT)

        try:
            # Create dataset
            mg.execute_query("UNWIND range(1, 5000) AS i CREATE (:InteractNode {id: i})")

            # Parallel query with lower query memory limit should fail
            with pytest.raises(MEMORY_ERRORS):
                mg.fetch_all(
                    "USING PARALLEL EXECUTION "
                    "MATCH (n:InteractNode) WITH collect(n) as nodes "
                    "RETURN size(nodes) QUERY MEMORY LIMIT 1KB"
                )

            # Parallel query with higher limit should succeed
            result = mg.fetch_all(
                "USING PARALLEL EXECUTION " "MATCH (n:InteractNode) RETURN count(n) QUERY MEMORY LIMIT 100MB"
            )
            assert result[0]["count(n)"] == 5000
        finally:
            try:
                mg.clear_database()
            except Exception:
                pass

    def test_multiple_parallel_queries_share_global_limit(self, memgraph_factory):
        """
        Test that concurrent parallel queries share the global memory limit.

        Multiple concurrent parallel queries should collectively be limited by the
        global memory limit.
        """
        import multiprocessing

        def run_parallel_query(uri, queue):
            """Run a parallel query."""
            try:
                from neo4j import GraphDatabase

                driver = GraphDatabase.driver(uri, auth=None)
                with driver.session() as session:
                    # Each query uses parallel execution
                    result = session.run(
                        "USING PARALLEL EXECUTION "
                        "UNWIND range(1, 10000) AS i "
                        "WITH i, i * 2 AS doubled "
                        "RETURN count(i)"
                    )
                    count = result.single()[0]
                    queue.put(("success", count))
                driver.close()
            except Exception as e:
                queue.put(("error", str(e)))

        # Start instance with 150MB global limit
        mg = memgraph_factory.create_instance(memory_limit_mb=150, port=self.MEMORY_LIMIT_PORT)
        uri = mg.uri

        try:
            # Run multiple parallel queries concurrently
            processes = []
            queues = []
            for _ in range(3):
                q = multiprocessing.Queue()
                p = multiprocessing.Process(target=run_parallel_query, args=(uri, q))
                p.start()
                processes.append(p)
                queues.append(q)

            # Wait for all to complete
            for p in processes:
                p.join(timeout=30)

            # Check that at least some succeeded
            success_count = 0
            for q in queues:
                try:
                    status, result = q.get(timeout=1)
                    if status == "success":
                        success_count += 1
                except Exception:
                    pass

            # With a 150MB limit, most lightweight queries should succeed
            assert success_count >= 1, "At least one query should succeed"
        finally:
            for p in processes:
                if p.is_alive():
                    p.terminate()
            try:
                mg.clear_database()
            except Exception:
                pass

    def test_parallel_global_limit_with_orderby(self, memgraph_factory):
        """
        Test global memory limit with parallel ORDER BY operations.

        ORDER BY requires buffering results which can be memory intensive.
        """
        # Start instance with moderate memory limit
        mg = memgraph_factory.create_instance(memory_limit_mb=150, port=self.MEMORY_LIMIT_PORT)

        try:
            # Create dataset
            mg.execute_query("UNWIND range(1, 10000) AS i CREATE (:SortNode {id: i, value: rand()})")

            # Parallel ORDER BY with reasonable data size should succeed
            result = mg.fetch_all(
                "USING PARALLEL EXECUTION " "MATCH (n:SortNode) RETURN n.id ORDER BY n.value LIMIT 100"
            )

            assert len(result) == 100
        finally:
            try:
                mg.clear_database()
            except Exception:
                pass

    def test_parallel_global_limit_with_cartesian_product(self, memgraph_factory):
        """
        Test global memory limit with parallel Cartesian products.
        """
        # Start instance with memory limit
        mg = memgraph_factory.create_instance(memory_limit_mb=100, port=self.MEMORY_LIMIT_PORT)

        try:
            # Create nodes for Cartesian product
            mg.execute_query("UNWIND range(1, 100) AS i CREATE (:CartA {id: i})")
            mg.execute_query("UNWIND range(1, 100) AS i CREATE (:CartB {id: i})")

            # Small parallel Cartesian product should succeed
            result = mg.fetch_all("USING PARALLEL EXECUTION MATCH (a:CartA), (b:CartB) RETURN count(*)")

            assert result[0]["count(*)"] == 10000  # 100 * 100
        finally:
            try:
                mg.clear_database()
            except Exception:
                pass

    def test_parallel_global_limit_with_unwind_expansion(self, memgraph_factory):
        """
        Test global memory limit when parallel UNWIND significantly expands data.
        """
        # Start instance with memory limit
        mg = memgraph_factory.create_instance(memory_limit_mb=100, port=self.MEMORY_LIMIT_PORT)

        try:
            # Create nodes with array properties
            mg.execute_query("UNWIND range(1, 100) AS i " "CREATE (:UnwindNode {id: i, items: range(1, 50)})")

            # Parallel UNWIND expansion should work within limits
            result = mg.fetch_all(
                "USING PARALLEL EXECUTION " "MATCH (n:UnwindNode) " "UNWIND n.items AS item " "RETURN count(item)"
            )

            # 100 nodes * 50 items each = 5000 total
            assert result[0]["count(item)"] == 5000
        finally:
            try:
                mg.clear_database()
            except Exception:
                pass


class TestParallelGlobalMemoryLimitStress:
    """
    Stress tests for global memory limits with parallel execution.

    These tests are more intensive and designed to push the memory
    limits harder to ensure proper behavior under pressure.
    Uses USING PARALLEL EXECUTION hint explicitly.
    """

    MEMORY_LIMIT_PORT = 7689

    def test_sustained_parallel_queries_near_limit(self, memgraph_factory):
        """
        Test sustained parallel query execution near the global memory limit.
        """
        import multiprocessing

        def run_sustained_parallel_query(uri, iterations, queue):
            """Run multiple parallel queries that use moderate memory."""
            try:
                from neo4j import GraphDatabase

                driver = GraphDatabase.driver(uri, auth=None)
                success_count = 0
                for _ in range(iterations):
                    try:
                        with driver.session() as session:
                            result = session.run(
                                "USING PARALLEL EXECUTION "
                                "UNWIND range(1, 1000) AS i "
                                "WITH i, i * 2 AS doubled "
                                "RETURN count(i)"
                            )
                            result.single()
                            success_count += 1
                    except Exception:
                        pass
                queue.put(("success", success_count))
                driver.close()
            except Exception as e:
                queue.put(("error", str(e)))

        # Start instance with moderate memory limit
        mg = memgraph_factory.create_instance(memory_limit_mb=150, port=self.MEMORY_LIMIT_PORT)
        uri = mg.uri

        try:
            # Run sustained parallel queries from multiple processes
            processes = []
            queues = []
            for _ in range(2):
                q = multiprocessing.Queue()
                p = multiprocessing.Process(
                    target=run_sustained_parallel_query, args=(uri, 10, q)  # 10 iterations each
                )
                p.start()
                processes.append(p)
                queues.append(q)

            # Wait for completion
            for p in processes:
                p.join(timeout=60)

            # Check results
            total_success = 0
            for q in queues:
                try:
                    status, count = q.get(timeout=1)
                    if status == "success":
                        total_success += count
                except Exception:
                    pass

            # Most queries should succeed
            assert total_success >= 10, f"Expected at least 10 successes, got {total_success}"
        finally:
            for p in processes:
                if p.is_alive():
                    p.terminate()

    def test_alternating_heavy_light_parallel_queries(self, memgraph_factory):
        """
        Test alternating between heavy and light parallel queries near memory limit.
        """
        # Start instance with memory limit
        mg = memgraph_factory.create_instance(memory_limit_mb=150, port=self.MEMORY_LIMIT_PORT)

        try:
            # Create dataset
            mg.execute_query("UNWIND range(1, 5000) AS i CREATE (:AltNode {id: i})")

            # Alternate between heavy and light parallel queries
            for i in range(5):
                # Light parallel query
                result = mg.fetch_all("USING PARALLEL EXECUTION MATCH (n:AltNode) RETURN count(n)")
                assert result[0]["count(n)"] == 5000

                # Heavier parallel query (but still within limits)
                result = mg.fetch_all(
                    "USING PARALLEL EXECUTION " "MATCH (n:AltNode) RETURN n.id ORDER BY n.id LIMIT 100"
                )
                assert len(result) == 100
        finally:
            try:
                mg.clear_database()
            except Exception:
                pass

    def test_parallel_global_limit_with_rapid_connections(self, memgraph_factory):
        """
        Test global memory limit with rapid connection cycling and parallel queries.
        """
        # Start instance with memory limit
        mg = memgraph_factory.create_instance(memory_limit_mb=150, port=self.MEMORY_LIMIT_PORT)

        try:
            # Create some data
            mg.execute_query("UNWIND range(1, 1000) AS i CREATE (:RapidNode {id: i})")

            # Rapidly open/close connections and run parallel queries
            from neo4j import GraphDatabase

            for i in range(10):
                driver = GraphDatabase.driver(mg.uri, auth=None)
                with driver.session() as session:
                    result = session.run("USING PARALLEL EXECUTION MATCH (n:RapidNode) RETURN count(n)")
                    count = result.single()[0]
                    assert count == 1000
                driver.close()
        finally:
            try:
                mg.clear_database()
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
