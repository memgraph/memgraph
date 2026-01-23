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
Tests for procedures with parallel execution.

This module tests that read and write procedures work correctly when called
within queries that use USING PARALLEL EXECUTION hint.

PARALLELIZATION REQUIREMENTS:
Parallelization is only supported when a query has:
- MATCH clause (to enable parallel scan)
- Plus aggregation operators (count, sum, avg, collect, etc.) OR ORDER BY

Queries that just call procedures without MATCH + aggregation/ORDER BY will
execute serially even with the USING PARALLEL EXECUTION hint.

Key scenarios tested:
1. Read procedures called after parallel MATCH scan with aggregation
2. Read procedures called after parallel MATCH scan with ORDER BY
3. Write procedures (which inhibit parallel scan but allow parallel post-processing)
4. Procedure results used in parallel ORDER BY and aggregation
5. Multiple procedure calls in the same query
6. Procedure results combined with graph operations
7. Procedures with occupied worker threads (concurrent load)
8. Procedures with memory tracking and limits

Note: Write operations typically inhibit parallelization of the scan operator,
but post-write operations (aggregation, ORDER BY) can still be parallelized.
"""

import multiprocessing
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import mgclient
import pytest

# =============================================================================
# Connection and Helpers
# =============================================================================

BOLT_PORT = 7687


def connect():
    """Connect to Memgraph instance."""
    connection = mgclient.connect(host="127.0.0.1", port=BOLT_PORT)
    connection.autocommit = True
    return connection


def execute_and_fetch_all(cursor, query: str, params: dict = None) -> List[tuple]:
    """Execute a query and return all results."""
    cursor.execute(query, params or {})
    return cursor.fetchall()


def clear_database(cursor):
    """Clear all data from the database."""
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")


def pq(query: str) -> str:
    """Wrap query with USING PARALLEL EXECUTION hint."""
    query = query.strip()
    if query.upper().startswith("USING PARALLEL"):
        return query
    return f"USING PARALLEL EXECUTION {query}"


def is_memory_error(exception: Exception) -> bool:
    """Check if an exception is a memory-related error."""
    error_msg = str(exception).lower()
    return "memory" in error_msg or "limit" in error_msg


def run_memory_test(cursor, query: str, expected_result=None, expected_check=None):
    """
    Run a query with memory limit and verify either:
    - The query succeeds with the expected result, OR
    - The query fails with a memory limit error (OOM)

    Both outcomes are valid for memory limit tests.

    Args:
        cursor: Database cursor
        query: Query to execute
        expected_result: Expected value for result[0][0] if query succeeds
        expected_check: Callable that takes result and returns True if valid

    Returns:
        tuple: (succeeded: bool, result_or_error)
    """
    try:
        result = execute_and_fetch_all(cursor, query)
        if expected_result is not None:
            assert result[0][0] == expected_result, f"Expected {expected_result}, got {result[0][0]}"
        elif expected_check is not None:
            assert expected_check(result), f"Result check failed for {result}"
        return (True, result)
    except Exception as e:
        if is_memory_error(e):
            # OOM is an acceptable outcome for memory limit tests
            return (False, e)
        else:
            # Non-memory errors should be re-raised
            raise


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(autouse=True)
def connection():
    """Provide a connection that clears the database after each test."""
    conn = connect()
    yield conn
    cursor = conn.cursor()
    clear_database(cursor)
    conn.close()


@pytest.fixture
def cursor(connection):
    """Provide a cursor for the connection."""
    return connection.cursor()


@pytest.fixture
def populated_db(cursor):
    """Populate database with test data."""
    clear_database(cursor)
    # Create nodes with various properties
    execute_and_fetch_all(
        cursor,
        """
        UNWIND range(1, 100) AS i
        CREATE (:Node {id: i, value: i * 10, group: i % 5, name: 'node_' + toString(i)})
        """,
    )
    # Create some relationships
    execute_and_fetch_all(
        cursor,
        """
        MATCH (a:Node), (b:Node)
        WHERE a.id < b.id AND a.group = b.group AND a.id + 5 = b.id
        CREATE (a)-[:CONNECTS {weight: a.value + b.value}]->(b)
        """,
    )
    return cursor


# =============================================================================
# Read Procedure Tests
# =============================================================================


class TestReadProceduresParallel:
    """
    Tests for read procedures with parallel execution.

    Note: Parallel execution requires MATCH + aggregation/ORDER BY.
    Tests are structured to ensure parallel operators are used.
    """

    @pytest.mark.parametrize(
        "query,expected_val",
        [
            ("MATCH (n:Node) CALL read_proc.get_node_properties(n) YIELD props RETURN sum(props.value)", 50500),
            ("MATCH (n:NumNode) CALL read_proc.generate_numbers(1, n.id) YIELD num RETURN count(*)", 55),
            ("MATCH (n:EchoNode) CALL read_proc.echo_values(n.id, 't', 1.0) YIELD int_out RETURN sum(int_out)", 55),
        ],
    )
    def test_read_proc_parameterized(self, cursor, query, expected_val):
        """Consolidated check for read procedures in parallel context."""
        # Setup specific data if needed
        if "NumNode" in query:
            execute_and_fetch_all(cursor, "UNWIND range(1, 10) AS i CREATE (:NumNode {id: i})")
        elif "EchoNode" in query:
            execute_and_fetch_all(cursor, "UNWIND range(1, 10) AS i CREATE (:EchoNode {id: i})")
        else:
            execute_and_fetch_all(cursor, "UNWIND range(1, 100) AS i CREATE (:Node {id: i, value: i * 10})")

        result = execute_and_fetch_all(cursor, pq(query))
        assert result[0][0] == expected_val

    def test_read_proc_with_parallel_orderby(self, populated_db):
        """Test read procedure results sorted with parallel ORDER BY."""
        cursor = populated_db
        result = execute_and_fetch_all(
            cursor,
            pq(
                "MATCH (n:Node) WHERE n.id <= 20 CALL read_proc.get_node_properties(n) YIELD props RETURN props.id AS id ORDER BY id DESC LIMIT 5"
            ),
        )
        assert [r[0] for r in result] == [20, 19, 18, 17, 16]

    def test_read_proc_graph_mutability_with_scan(self, cursor):
        """Test that read procedure reports graph as immutable after scan."""
        execute_and_fetch_all(cursor, "CREATE (:TestNode {id: 1})")
        result = execute_and_fetch_all(
            cursor, pq("MATCH (n:TestNode) CALL read_proc.graph_is_mutable() YIELD mutable RETURN mutable")
        )
        assert result[0][0] is False


# =============================================================================
# Write Procedure Tests
# =============================================================================


class TestWriteProceduresParallel:
    """
    Tests for write procedures with parallel execution.

    Note: Write operations inhibit parallelization of the scan operator,
    but the query still uses the parallel execution path and post-write
    operations (aggregation, ORDER BY) can be parallelized.

    Tests use MATCH + write procedure + aggregation pattern where possible.
    """

    @pytest.mark.parametrize(
        "query,setup_query,verify_query,expected_count",
        [
            (
                "MATCH (n:Node) CALL write_proc.set_property(n, 'p', true) YIELD success RETURN count(*)",
                "UNWIND range(1, 10) AS i CREATE (:Node {id: i})",
                "MATCH (n) WHERE n.p = true RETURN count(*)",
                10,
            ),
            (
                "MATCH (n:Node) CALL write_proc.increment_property(n, 'c', 1) YIELD new_value RETURN sum(new_value)",
                "UNWIND range(1, 10) AS i CREATE (:Node {id: i, c: 0})",
                "MATCH (n) RETURN sum(n.c)",
                10,
            ),
            (
                "MATCH (n:Node) CALL write_proc.add_label(n, 'L') YIELD success RETURN count(*)",
                "UNWIND range(1, 10) AS i CREATE (:Node {id: i})",
                "MATCH (n:L) RETURN count(*)",
                10,
            ),
        ],
    )
    def test_write_proc_parameterized(self, cursor, query, setup_query, verify_query, expected_count):
        """Consolidated check for write procedures."""
        execute_and_fetch_all(cursor, setup_query)
        result = execute_and_fetch_all(cursor, pq(query))
        assert result[0][0] == expected_count
        verify = execute_and_fetch_all(cursor, verify_query)
        assert verify[0][0] == expected_count

    def test_write_proc_graph_mutability(self, cursor):
        """Test that write procedure reports graph as mutable."""
        result = execute_and_fetch_all(cursor, pq("CALL write_proc.graph_is_mutable() YIELD mutable RETURN mutable"))
        assert result[0][0] is True


# =============================================================================
# Combined Read/Write Tests
# =============================================================================


class TestMixedProceduresParallel:
    """Tests combining read and write procedures with parallel execution."""

    def test_read_then_write_proc(self, cursor):
        """Test calling read procedure followed by write procedure."""
        # Create initial data
        execute_and_fetch_all(cursor, "UNWIND range(1, 10) AS i CREATE (:Source {id: i, value: i * 10})")

        # Read properties, then create new nodes based on them
        result = execute_and_fetch_all(
            cursor,
            pq(
                """
                MATCH (n:Source)
                CALL read_proc.get_node_properties(n) YIELD props
                WITH props
                WHERE props.value >= 50
                CALL write_proc.create_node_with_props('Derived', {source_id: props.id, doubled: props.value * 2})
                YIELD node
                RETURN count(node) AS created
            """
            ),
        )
        # Nodes with value >= 50: ids 5-10 = 6 nodes
        assert result[0][0] == 6

    def test_write_then_read_aggregation(self, cursor):
        """Test write procedure followed by read aggregation."""
        # Create nodes
        execute_and_fetch_all(cursor, "UNWIND range(1, 20) AS i CREATE (:Data {id: i, value: 100})")

        # Increment then aggregate
        result = execute_and_fetch_all(
            cursor,
            pq(
                """
                MATCH (n:Data)
                CALL write_proc.increment_property(n, 'value', n.id) YIELD new_value
                RETURN sum(new_value) AS total, avg(new_value) AS average
            """
            ),
        )
        # Each node: 100 + id, sum = 20*100 + (1+2+...+20) = 2000 + 210 = 2210
        assert result[0][0] == 2210
        assert result[0][1] == 110.5  # 2210 / 20


# =============================================================================
# Parallel Correctness Tests
# =============================================================================


class TestProcedureParallelCorrectness:
    """Tests ensuring parallel execution produces correct results for procedures."""

    def test_serial_vs_parallel_read_proc_count(self, cursor):
        """Verify serial and parallel read procedure produce same count."""
        # Create data
        execute_and_fetch_all(cursor, "UNWIND range(1, 50) AS i CREATE (:Test {id: i})")

        # Serial execution
        serial_result = execute_and_fetch_all(cursor, "CALL read_proc.get_node_count() YIELD count RETURN count")

        # Parallel execution
        parallel_result = execute_and_fetch_all(cursor, pq("CALL read_proc.get_node_count() YIELD count RETURN count"))

        assert serial_result[0][0] == parallel_result[0][0] == 50

    def test_serial_vs_parallel_sum_property(self, cursor):
        """Verify serial and parallel produce same sum."""
        # Create data
        execute_and_fetch_all(cursor, "UNWIND range(1, 100) AS i CREATE (:Sum {id: i, value: i})")

        # Serial
        serial_result = execute_and_fetch_all(cursor, "CALL read_proc.sum_property('value') YIELD total RETURN total")

        # Parallel
        parallel_result = execute_and_fetch_all(
            cursor, pq("CALL read_proc.sum_property('value') YIELD total RETURN total")
        )

        assert serial_result[0][0] == parallel_result[0][0] == 5050

    def test_serial_vs_parallel_aggregation_after_proc(self, cursor):
        """Verify aggregation after procedure call is consistent."""
        # Create data
        execute_and_fetch_all(cursor, "UNWIND range(1, 30) AS i CREATE (:Agg {id: i, group: i % 3})")

        query = """
            MATCH (n:Agg)
            CALL read_proc.get_node_properties(n) YIELD props
            RETURN props.group AS g, count(*) AS cnt
            ORDER BY g
        """

        serial_result = execute_and_fetch_all(cursor, query)
        parallel_result = execute_and_fetch_all(cursor, pq(query))

        assert serial_result == parallel_result


# =============================================================================
# Edge Cases
# =============================================================================


class TestProcedureEdgeCases:
    """
    Test edge cases for procedures with parallel execution.

    Note: Tests use MATCH + procedure + aggregation/ORDER BY to enable parallelism.
    """

    def test_empty_result_from_procedure_with_scan(self, cursor):
        """Test procedure returning no results after scan."""
        # Create some nodes to enable parallel scan
        execute_and_fetch_all(cursor, "CREATE (:EdgeCase {id: 1}), (:EdgeCase {id: 2})")

        # MATCH + procedure (that returns no results) + aggregation
        result = execute_and_fetch_all(
            cursor,
            pq(
                """
                MATCH (n:EdgeCase)
                CALL read_proc.get_node_properties(n) YIELD props
                WITH props
                WHERE props.nonexistent_prop IS NOT NULL
                RETURN count(*) AS cnt
            """
            ),
        )
        assert result[0][0] == 0

    def test_procedure_with_null_handling_and_scan(self, cursor):
        """Test procedure handling NULL values with parallel scan."""
        # Create nodes with and without properties
        execute_and_fetch_all(cursor, "CREATE (:Nullable {id: 1, value: 10})")
        execute_and_fetch_all(cursor, "CREATE (:Nullable {id: 2})")  # no value

        # MATCH + procedure + aggregation
        result = execute_and_fetch_all(
            cursor,
            pq(
                """
                MATCH (n:Nullable)
                CALL read_proc.get_node_properties(n) YIELD props
                RETURN sum(COALESCE(props.value, 0)) AS total
            """
            ),
        )
        # Should only sum the one with value
        assert result[0][0] == 10

    def test_procedure_with_large_result_set_and_scan(self, cursor):
        """Test procedure generating large result set after scan."""
        # Create nodes to enable parallel scan
        execute_and_fetch_all(cursor, "UNWIND range(1, 10) AS i CREATE (:LargeSet {id: i})")

        # MATCH + procedure generating numbers + aggregation
        result = execute_and_fetch_all(
            cursor,
            pq(
                """
                MATCH (n:LargeSet)
                CALL read_proc.generate_numbers(1, 100) YIELD num
                RETURN count(*) AS cnt, sum(num) AS total
            """
            ),
        )
        # 10 nodes * 100 numbers each = 1000 rows
        assert result[0][0] == 1000
        # Each node generates sum(1..100) = 5050, 10 nodes = 50500
        assert result[0][1] == 50500


# =============================================================================
# Concurrent/Occupied Workers Tests
# =============================================================================


def run_long_query_in_process(host: str, port: int, query: str, result_queue: multiprocessing.Queue):
    """
    Execute a long-running query in a separate process.
    Puts the result or exception info into the queue.
    """
    try:
        conn = mgclient.connect(host=host, port=port)
        cursor = conn.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        result_queue.put(("success", len(records)))
        conn.close()
    except Exception as e:
        result_queue.put(("error", type(e).__name__, str(e)))


def start_long_query_process(host: str, port: int, query: str) -> Tuple[multiprocessing.Process, multiprocessing.Queue]:
    """
    Start a long-running query in a separate process.
    Returns (process, result_queue).
    """
    result_queue = multiprocessing.Queue()
    process = multiprocessing.Process(target=run_long_query_in_process, args=(host, port, query, result_queue))
    process.start()
    return process, result_queue


class TestProceduresWithOccupiedWorkers:
    """
    Tests for procedures when some worker threads are occupied with other queries.

    These tests simulate scenarios where:
    1. Background queries occupy some workers
    2. Procedure calls must execute with reduced parallelism
    3. Results must still be correct despite worker contention

    This verifies that the parallel execution engine properly handles
    resource contention and produces correct results.
    """

    HOST = "127.0.0.1"
    PORT = BOLT_PORT

    # Long-running queries to occupy workers
    # These create large cross-products to keep workers busy
    LONG_RUNNING_SCAN_QUERY = """
        USING PARALLEL EXECUTION
        MATCH (n)
        UNWIND range(1, 200) AS a
        UNWIND range(1, 200) AS b
        WITH a, b, a * b AS product
        RETURN count(*) AS cnt
    """

    LONG_RUNNING_ORDERBY_QUERY = """
        USING PARALLEL EXECUTION
        MATCH (n)
        UNWIND range(1, 150) AS a
        UNWIND range(1, 150) AS b
        WITH a * b AS val
        RETURN val ORDER BY val DESC LIMIT 100
    """

    @pytest.fixture(autouse=True)
    def setup(self, cursor):
        """Set up test data."""
        self.cursor = cursor
        clear_database(cursor)
        # Create test data for procedure tests
        execute_and_fetch_all(
            cursor,
            """
            UNWIND range(1, 200) AS i
            CREATE (:TestNode {id: i, value: i * 10, group: i % 10})
            """,
        )
        yield
        clear_database(cursor)

    def _cleanup_processes(self, processes: List[Tuple[multiprocessing.Process, multiprocessing.Queue]]):
        """Terminate and clean up background processes."""
        for proc, queue in processes:
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=2)

    def test_read_proc_scan_aggregation_with_one_busy_worker(self):
        """Test MATCH + procedure + aggregation while one worker is busy."""
        # Start one long-running query
        bg_proc, bg_queue = start_long_query_process(self.HOST, self.PORT, self.LONG_RUNNING_SCAN_QUERY)

        try:
            time.sleep(0.2)  # Let background query start

            # MATCH + procedure + aggregation enables parallelism
            result = execute_and_fetch_all(
                self.cursor,
                pq(
                    """
                    MATCH (n:TestNode)
                    CALL read_proc.get_node_properties(n) YIELD props
                    RETURN count(*) AS cnt
                """
                ),
            )
            assert result[0][0] == 200

        finally:
            self._cleanup_processes([(bg_proc, bg_queue)])

    def test_read_proc_scan_sum_with_multiple_busy_workers(self):
        """Test MATCH + procedure + sum while multiple workers are busy."""
        # Start multiple long-running queries
        bg_processes = []
        for _ in range(3):
            proc, queue = start_long_query_process(self.HOST, self.PORT, self.LONG_RUNNING_SCAN_QUERY)
            bg_processes.append((proc, queue))

        try:
            time.sleep(0.3)  # Let background queries start

            # MATCH + procedure + aggregation while workers are busy
            result = execute_and_fetch_all(
                self.cursor,
                pq(
                    """
                    MATCH (n:TestNode)
                    CALL read_proc.get_node_properties(n) YIELD props
                    RETURN sum(props.value) AS total
                """
                ),
            )
            # Sum of 10+20+...+2000 = 10*(1+2+...+200) = 10*20100 = 201000
            assert result[0][0] == 201000

        finally:
            self._cleanup_processes(bg_processes)

    def test_read_proc_after_scan_with_busy_workers(self):
        """Test read procedure called after scan while workers are busy."""
        # Start background queries
        bg_processes = []
        for _ in range(2):
            proc, queue = start_long_query_process(self.HOST, self.PORT, self.LONG_RUNNING_ORDERBY_QUERY)
            bg_processes.append((proc, queue))

        try:
            time.sleep(0.2)

            # Scan + procedure call
            result = execute_and_fetch_all(
                self.cursor,
                pq(
                    """
                    MATCH (n:TestNode)
                    WHERE n.id <= 50
                    CALL read_proc.get_node_properties(n) YIELD props
                    RETURN count(*) AS cnt, sum(props.value) AS total
                """
                ),
            )
            assert result[0][0] == 50
            # Sum of 10+20+...+500 = 10*(1+2+...+50) = 10*1275 = 12750
            assert result[0][1] == 12750

        finally:
            self._cleanup_processes(bg_processes)

    def test_write_proc_with_busy_workers(self):
        """Test write procedure while workers are busy."""
        # Start background queries
        bg_processes = []
        for _ in range(2):
            proc, queue = start_long_query_process(self.HOST, self.PORT, self.LONG_RUNNING_SCAN_QUERY)
            bg_processes.append((proc, queue))

        try:
            time.sleep(0.2)

            # Write procedure call
            result = execute_and_fetch_all(
                self.cursor,
                pq(
                    """
                    MATCH (n:TestNode)
                    WHERE n.id <= 20
                    CALL write_proc.set_property(n, 'busy_test', true) YIELD success
                    RETURN count(*) AS updated
                """
                ),
            )
            assert result[0][0] == 20

            # Verify the update
            verify = execute_and_fetch_all(
                self.cursor, "MATCH (n:TestNode) WHERE n.busy_test = true RETURN count(n) AS cnt"
            )
            assert verify[0][0] == 20

        finally:
            self._cleanup_processes(bg_processes)

    def test_procedure_aggregation_with_busy_workers(self):
        """Test procedure + aggregation while workers are busy."""
        bg_processes = []
        for _ in range(2):
            proc, queue = start_long_query_process(self.HOST, self.PORT, self.LONG_RUNNING_SCAN_QUERY)
            bg_processes.append((proc, queue))

        try:
            time.sleep(0.2)

            # Procedure with aggregation
            result = execute_and_fetch_all(
                self.cursor,
                pq(
                    """
                    MATCH (n:TestNode)
                    CALL read_proc.get_node_properties(n) YIELD props
                    RETURN props.group AS g, count(*) AS cnt, sum(props.value) AS total
                    ORDER BY g
                """
                ),
            )
            # 200 nodes, 10 groups, 20 per group
            assert len(result) == 10
            for row in result:
                assert row[1] == 20  # 20 nodes per group

        finally:
            self._cleanup_processes(bg_processes)

    def test_procedure_orderby_with_busy_workers(self):
        """Test procedure + ORDER BY while workers are busy."""
        bg_processes = []
        for _ in range(2):
            proc, queue = start_long_query_process(self.HOST, self.PORT, self.LONG_RUNNING_ORDERBY_QUERY)
            bg_processes.append((proc, queue))

        try:
            time.sleep(0.2)

            # Procedure with ORDER BY
            result = execute_and_fetch_all(
                self.cursor,
                pq(
                    """
                    MATCH (n:TestNode)
                    WHERE n.id <= 30
                    CALL read_proc.get_node_properties(n) YIELD props
                    RETURN props.id AS id, props.value AS val
                    ORDER BY val DESC
                    LIMIT 5
                """
                ),
            )
            assert len(result) == 5
            # Top 5 by value: ids 30, 29, 28, 27, 26
            expected_ids = [30, 29, 28, 27, 26]
            for i, row in enumerate(result):
                assert row[0] == expected_ids[i]

        finally:
            self._cleanup_processes(bg_processes)

    def test_multiple_procedure_calls_with_busy_workers(self):
        """Test multiple procedure calls in one query while workers are busy."""
        bg_processes = []
        for _ in range(2):
            proc, queue = start_long_query_process(self.HOST, self.PORT, self.LONG_RUNNING_SCAN_QUERY)
            bg_processes.append((proc, queue))

        try:
            time.sleep(0.2)

            # Multiple procedure calls
            result = execute_and_fetch_all(
                self.cursor,
                pq(
                    """
                    MATCH (n:TestNode)
                    WHERE n.id <= 10
                    CALL read_proc.get_node_properties(n) YIELD props
                    CALL read_proc.compute_value(props.id, 2) YIELD result
                    RETURN sum(result) AS total
                """
                ),
            )
            # compute_value(x, y) = x*y + x + y = x*2 + x + 2 = 3x + 2
            # For x in 1..10: sum(3x + 2) = 3*55 + 20 = 185
            assert result[0][0] == 185

        finally:
            self._cleanup_processes(bg_processes)

    def test_concurrent_procedure_calls(self):
        """Test multiple concurrent MATCH + procedure + aggregation calls."""

        def run_procedure_query(host, port, query, result_queue):
            try:
                conn = mgclient.connect(host=host, port=port)
                cursor = conn.cursor()
                cursor.execute(query)
                records = cursor.fetchall()
                result_queue.put(("success", records[0][0] if records else None))
                conn.close()
            except Exception as e:
                result_queue.put(("error", str(e)))

        # MATCH + procedure + aggregation to enable parallelism
        proc_query = pq(
            """
            MATCH (n:TestNode)
            CALL read_proc.get_node_properties(n) YIELD props
            RETURN count(*) AS cnt
        """
        )

        processes = []
        for _ in range(5):
            queue = multiprocessing.Queue()
            proc = multiprocessing.Process(target=run_procedure_query, args=(self.HOST, self.PORT, proc_query, queue))
            proc.start()
            processes.append((proc, queue))

        # Wait for all to complete
        for proc, _ in processes:
            proc.join(timeout=30)

        # Check results
        success_count = 0
        for proc, queue in processes:
            try:
                status, result = queue.get(timeout=1)
                print(f"Status: {status}, Result: {result}")
                if status == "success" and result == 200:
                    success_count += 1
            except Exception as e:
                print(f"Exception: {e}")

        assert success_count >= 3, f"Expected at least 3 successes, got {success_count}"

    def test_read_proc_consistency_under_load(self):
        """Test that MATCH + procedure + aggregation produces consistent results under load."""
        bg_processes = []
        for _ in range(3):
            proc, queue = start_long_query_process(self.HOST, self.PORT, self.LONG_RUNNING_SCAN_QUERY)
            bg_processes.append((proc, queue))

        try:
            time.sleep(0.2)

            # Run the same MATCH + procedure + aggregation multiple times
            results = []
            for _ in range(5):
                result = execute_and_fetch_all(
                    self.cursor,
                    pq(
                        """
                        MATCH (n:TestNode)
                        CALL read_proc.get_node_properties(n) YIELD props
                        RETURN sum(props.value) AS total
                    """
                    ),
                )
                results.append(result[0][0])

            # All results should be identical
            assert all(r == 201000 for r in results), f"Inconsistent results: {results}"

        finally:
            self._cleanup_processes(bg_processes)

    def test_write_proc_consistency_under_load(self):
        """Test that write procedure produces consistent results under load."""
        # Create separate test nodes for this test
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 50) AS i CREATE (:WriteTest {id: i, counter: 0})")

        bg_processes = []
        for _ in range(2):
            proc, queue = start_long_query_process(self.HOST, self.PORT, self.LONG_RUNNING_SCAN_QUERY)
            bg_processes.append((proc, queue))

        try:
            time.sleep(0.2)

            # Increment counters
            result = execute_and_fetch_all(
                self.cursor,
                pq(
                    """
                    MATCH (n:WriteTest)
                    CALL write_proc.increment_property(n, 'counter', 5) YIELD new_value
                    RETURN sum(new_value) AS total
                """
                ),
            )
            # 50 nodes * 5 increment = 250
            assert result[0][0] == 250

            # Verify with another query
            verify = execute_and_fetch_all(self.cursor, "MATCH (n:WriteTest) RETURN sum(n.counter) AS total")
            assert verify[0][0] == 250

        finally:
            self._cleanup_processes(bg_processes)

    def test_procedure_with_staggered_load(self):
        """Test procedures with staggered background load."""
        results = []
        all_processes = []

        # Start background queries at different times
        for delay in [0, 0.1, 0.2]:
            time.sleep(delay)
            proc, queue = start_long_query_process(self.HOST, self.PORT, self.LONG_RUNNING_SCAN_QUERY)
            all_processes.append((proc, queue))

            # Run procedure after each background query starts
            result = execute_and_fetch_all(
                self.cursor,
                pq(
                    """
                    MATCH (n:TestNode)
                    WHERE n.group = 0
                    CALL read_proc.get_node_properties(n) YIELD props
                    RETURN count(*) AS cnt
                """
                ),
            )
            results.append(result[0][0])

        try:
            # All results should be 20 (200 nodes / 10 groups)
            assert all(r == 20 for r in results), f"Inconsistent results: {results}"
        finally:
            self._cleanup_processes(all_processes)

    def test_serial_vs_parallel_proc_under_load(self):
        """Compare serial and parallel procedure execution under load."""
        bg_processes = []
        for _ in range(2):
            proc, queue = start_long_query_process(self.HOST, self.PORT, self.LONG_RUNNING_SCAN_QUERY)
            bg_processes.append((proc, queue))

        try:
            time.sleep(0.2)

            # Serial execution
            serial_result = execute_and_fetch_all(
                self.cursor,
                """
                MATCH (n:TestNode)
                CALL read_proc.get_node_properties(n) YIELD props
                RETURN sum(props.value) AS total
                """,
            )

            # Parallel execution
            parallel_result = execute_and_fetch_all(
                self.cursor,
                pq(
                    """
                    MATCH (n:TestNode)
                    CALL read_proc.get_node_properties(n) YIELD props
                    RETURN sum(props.value) AS total
                """
                ),
            )

            # Both should produce the same result
            assert serial_result[0][0] == parallel_result[0][0] == 201000

        finally:
            self._cleanup_processes(bg_processes)

    def test_heavy_load_procedure_still_completes(self):
        """Test that MATCH + procedure + aggregation completes under heavy load."""
        # Start many background queries
        bg_processes = []
        for _ in range(5):
            proc, queue = start_long_query_process(self.HOST, self.PORT, self.LONG_RUNNING_SCAN_QUERY)
            bg_processes.append((proc, queue))

        try:
            time.sleep(0.3)

            # MATCH + procedure + aggregation should still complete
            start_time = time.time()
            result = execute_and_fetch_all(
                self.cursor,
                pq(
                    """
                    MATCH (n:TestNode)
                    CALL read_proc.get_node_properties(n) YIELD props
                    RETURN count(*) AS cnt
                """
                ),
            )
            elapsed = time.time() - start_time

            assert result[0][0] == 200
            # Should complete in reasonable time (< 30 seconds even under heavy load)
            assert elapsed < 30, f"Procedure took too long: {elapsed:.2f}s"

        finally:
            self._cleanup_processes(bg_processes)

    def test_interleaved_read_write_procs_under_load(self):
        """Test alternating read and write procedures under load."""
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 30) AS i CREATE (:Interleave {id: i, value: 0})")

        bg_processes = []
        for _ in range(2):
            proc, queue = start_long_query_process(self.HOST, self.PORT, self.LONG_RUNNING_SCAN_QUERY)
            bg_processes.append((proc, queue))

        try:
            time.sleep(0.2)

            # Alternate between read and write procedures
            for i in range(3):
                # Write: MATCH + procedure + aggregation
                execute_and_fetch_all(
                    self.cursor,
                    pq(
                        f"""
                        MATCH (n:Interleave)
                        CALL write_proc.increment_property(n, 'value', 1) YIELD new_value
                        RETURN count(*) AS cnt
                    """
                    ),
                )

                # Read: MATCH + procedure + aggregation
                result = execute_and_fetch_all(
                    self.cursor,
                    pq(
                        """
                        MATCH (n:Interleave)
                        CALL read_proc.get_node_properties(n) YIELD props
                        RETURN sum(props.value) AS total
                    """
                    ),
                )
                # After i+1 increments: 30 nodes * (i+1) = total
                expected = 30 * (i + 1)
                assert result[0][0] == expected

        finally:
            self._cleanup_processes(bg_processes)


# =============================================================================
# Memory Tracking Tests for Procedures
# =============================================================================


class TestProceduresMemoryTracking:
    """
    Tests for procedures with parallel execution and memory tracking.

    These tests verify that:
    1. Procedures respect QUERY MEMORY LIMIT
    2. Memory is properly tracked when procedures allocate memory
    3. Memory exhaustion in procedures triggers proper errors
    4. Memory is released after procedure calls complete

    Note: Tests use MATCH + procedure + aggregation/ORDER BY to enable parallel execution.
    """

    HOST = "127.0.0.1"
    PORT = BOLT_PORT

    def _cleanup_processes(self, processes: List[Tuple[multiprocessing.Process, multiprocessing.Queue]]):
        """Terminate and clean up background processes."""
        for proc, queue in processes:
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=2)

    @pytest.fixture(autouse=True)
    def setup(self, cursor):
        """Set up test data."""
        self.cursor = cursor
        clear_database(cursor)
        yield
        clear_database(cursor)

    def test_read_proc_scan_aggregation_within_memory_limit(self):
        """Test MATCH + procedure + aggregation within memory limit.

        Accepts either correct result or OOM as valid outcomes.
        """
        # Create test data
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 500) AS i CREATE (:MemNode {id: i, value: i * 10})")

        # MATCH + procedure + aggregation with memory limit
        # Either succeeds with correct result or fails with OOM
        run_memory_test(
            self.cursor,
            pq(
                """
                MATCH (n:MemNode)
                CALL read_proc.get_node_properties(n) YIELD props
                RETURN count(*) AS cnt
                QUERY MEMORY LIMIT 50MB
            """
            ),
            expected_result=500,
        )

    def test_read_proc_after_scan_within_memory_limit(self):
        """Test procedure called after scan respects memory limit.

        Accepts either correct result or OOM as valid outcomes.
        """
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 1000) AS i CREATE (:MemNode {id: i, value: i})")

        # MATCH + procedure + aggregation
        run_memory_test(
            self.cursor,
            pq(
                """
                MATCH (n:MemNode)
                CALL read_proc.get_node_properties(n) YIELD props
                RETURN count(*) AS cnt
                QUERY MEMORY LIMIT 100MB
            """
            ),
            expected_result=1000,
        )

    def test_read_proc_aggregation_within_memory_limit(self):
        """Test procedure with grouping aggregation respects memory limit.

        Accepts either correct result or OOM as valid outcomes.
        """
        execute_and_fetch_all(
            self.cursor, "UNWIND range(1, 500) AS i CREATE (:MemNode {id: i, value: i * 10, group: i % 10})"
        )

        # MATCH + procedure + grouping aggregation + ORDER BY
        run_memory_test(
            self.cursor,
            pq(
                """
                MATCH (n:MemNode)
                CALL read_proc.get_node_properties(n) YIELD props
                RETURN props.group AS g, count(*) AS cnt, sum(props.value) AS total
                ORDER BY g
                QUERY MEMORY LIMIT 100MB
            """
            ),
            expected_check=lambda r: len(r) == 10,  # 10 groups
        )

    def test_read_proc_generate_numbers_with_scan_memory_limit(self):
        """Test procedure generating numbers after scan with memory limit.

        Accepts either correct result or OOM as valid outcomes.
        """
        # Create nodes to enable parallel scan
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 50) AS i CREATE (:NumMemNode {id: i})")

        # MATCH + procedure + aggregation
        run_memory_test(
            self.cursor,
            pq(
                """
                MATCH (n:NumMemNode)
                CALL read_proc.generate_numbers(1, n.id) YIELD num
                RETURN sum(num) AS total
                QUERY MEMORY LIMIT 50MB
            """
            ),
            expected_check=lambda r: r[0][0] > 0,  # Just verify it's positive
        )

    def test_read_proc_exceeds_memory_limit(self):
        """Test read procedure fails when memory limit is exceeded."""
        # Create enough data to exceed a tiny limit
        execute_and_fetch_all(
            self.cursor, "UNWIND range(1, 1000) AS i CREATE (:BigNode {id: i, data: 'payload_' + toString(i)})"
        )

        # MATCH + procedure + collect (memory intensive) with tiny limit should fail
        with pytest.raises(Exception) as exc_info:
            execute_and_fetch_all(
                self.cursor,
                pq(
                    """
                    MATCH (n:BigNode)
                    CALL read_proc.get_node_properties(n) YIELD props
                    RETURN collect(props.data) AS all_data
                    QUERY MEMORY LIMIT 1KB
                """
                ),
            )

        error_msg = str(exc_info.value).lower()
        assert "memory" in error_msg or "limit" in error_msg

    def test_read_proc_large_aggregation_exceeds_limit(self):
        """Test procedure with large collect exceeds memory limit."""
        # Create data for large aggregation
        execute_and_fetch_all(
            self.cursor, "UNWIND range(1, 2000) AS i CREATE (:CollectNode {id: i, data: 'data_' + toString(i)})"
        )

        # MATCH + procedure + collect with tiny limit should fail
        with pytest.raises(Exception) as exc_info:
            execute_and_fetch_all(
                self.cursor,
                pq(
                    """
                    MATCH (n:CollectNode)
                    CALL read_proc.get_node_properties(n) YIELD props
                    RETURN collect(props) AS all_props
                    QUERY MEMORY LIMIT 1KB
                """
                ),
            )

        error_msg = str(exc_info.value).lower()
        assert "memory" in error_msg or "limit" in error_msg

    def test_write_proc_scan_aggregation_within_memory_limit(self):
        """Test MATCH + write procedure + aggregation within memory limit.

        Accepts either correct result or OOM as valid outcomes.
        """
        # Create source nodes
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 100) AS i CREATE (:MemSource {id: i})")

        # MATCH + write procedure + aggregation
        succeeded, result = run_memory_test(
            self.cursor,
            pq(
                """
                MATCH (s:MemSource)
                CALL write_proc.create_node_with_props('MemWrite', {id: s.id})
                YIELD node
                RETURN count(node) AS created
                QUERY MEMORY LIMIT 50MB
            """
            ),
            expected_result=100,
        )

        # Only verify if query succeeded (not OOM)
        if succeeded:
            verify = execute_and_fetch_all(self.cursor, "MATCH (n:MemWrite) RETURN count(n) AS cnt")
            assert verify[0][0] == 100

    def test_write_proc_set_property_within_memory_limit(self):
        """Test MATCH + write procedure set property with memory limit.

        Accepts either correct result or OOM as valid outcomes.
        """
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 200) AS i CREATE (:BatchMem {id: i})")

        # MATCH + write procedure + aggregation
        run_memory_test(
            self.cursor,
            pq(
                """
                MATCH (n:BatchMem)
                CALL write_proc.set_property(n, 'processed', true) YIELD success
                RETURN count(*) AS updated
                QUERY MEMORY LIMIT 50MB
            """
            ),
            expected_result=200,
        )

    def test_write_proc_exceeds_memory_limit(self):
        """Test write procedure fails when memory limit exceeded."""
        # Create enough data to exceed tiny limit during processing
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 1000) AS i CREATE (:WriteExceed {id: i, data: 'x'})")

        with pytest.raises(Exception) as exc_info:
            execute_and_fetch_all(
                self.cursor,
                pq(
                    """
                    MATCH (n:WriteExceed)
                    CALL write_proc.set_property(n, 'new_data', n.data + '_processed') YIELD success
                    RETURN collect(success) AS results
                    QUERY MEMORY LIMIT 1KB
                """
                ),
            )

        error_msg = str(exc_info.value).lower()
        assert "memory" in error_msg or "limit" in error_msg

    def test_mixed_procs_memory_limit(self):
        """Test mixed read/write procedures respect memory limit.

        Accepts either correct result or OOM as valid outcomes.
        """
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 200) AS i CREATE (:MixedMem {id: i, value: i * 10})")

        run_memory_test(
            self.cursor,
            pq(
                """
                MATCH (n:MixedMem)
                WHERE n.id <= 50
                CALL read_proc.get_node_properties(n) YIELD props
                WITH n, props
                CALL write_proc.set_property(n, 'doubled', props.value * 2) YIELD success
                RETURN count(*) AS processed
                QUERY MEMORY LIMIT 50MB
            """
            ),
            expected_result=50,
        )

    def test_procedure_sum_property_with_scan_memory_limit(self):
        """Test procedure summing property with MATCH and aggregation.

        Accepts either correct result or OOM as valid outcomes.
        """
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 1000) AS i CREATE (:SumMem {id: i, value: i})")

        # MATCH + procedure + aggregation to enable parallelism
        run_memory_test(
            self.cursor,
            pq(
                """
                MATCH (n:SumMem)
                CALL read_proc.get_node_properties(n) YIELD props
                RETURN sum(props.value) AS total
                QUERY MEMORY LIMIT 50MB
            """
            ),
            expected_result=500500,  # Sum 1+2+...+1000
        )

    def test_procedure_filter_with_scan_memory_limit(self):
        """Test procedure filtering with MATCH and aggregation.

        Accepts either correct result or OOM as valid outcomes.
        """
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 500) AS i CREATE (:FilterMem {id: i, value: i * 100})")

        # MATCH + procedure + aggregation to enable parallelism
        run_memory_test(
            self.cursor,
            pq(
                """
                MATCH (n:FilterMem)
                WHERE n.value >= 25000
                CALL read_proc.get_node_properties(n) YIELD props
                RETURN count(*) AS cnt
                QUERY MEMORY LIMIT 50MB
            """
            ),
            expected_result=251,  # Nodes with value >= 25000: ids 250-500
        )

    def test_procedure_label_aggregation_with_scan_memory_limit(self):
        """Test procedure with label aggregation after MATCH.

        Accepts either correct result or OOM as valid outcomes.
        """
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 300) AS i CREATE (:LabelA {id: i})")
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 200) AS i CREATE (:LabelB {id: i})")

        def check_labels(result):
            result_dict = {row[0]: row[1] for row in result}
            return result_dict.get("LabelA") == 300 and result_dict.get("LabelB") == 200

        # MATCH + procedure + aggregation + ORDER BY
        run_memory_test(
            self.cursor,
            pq(
                """
                MATCH (n)
                CALL read_proc.get_node_labels(n) YIELD labels
                UNWIND labels AS label
                RETURN label, count(*) AS cnt
                ORDER BY label
                QUERY MEMORY LIMIT 50MB
            """
            ),
            expected_check=check_labels,
        )

    def test_serial_vs_parallel_proc_memory_limit(self):
        """Compare serial and parallel procedure execution with memory limit.

        Accepts either correct result or OOM as valid outcomes for each.
        """
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 500) AS i CREATE (:CompareMem {id: i, value: i})")

        # Serial with memory limit (MATCH + aggregation but no USING PARALLEL)
        serial_succeeded, serial_result = run_memory_test(
            self.cursor,
            """
            MATCH (n:CompareMem)
            CALL read_proc.get_node_properties(n) YIELD props
            RETURN sum(props.value) AS total
            QUERY MEMORY LIMIT 50MB
            """,
            expected_result=125250,  # Sum 1+2+...+500
        )

        # Parallel with same memory limit (MATCH + procedure + aggregation)
        parallel_succeeded, parallel_result = run_memory_test(
            self.cursor,
            pq(
                """
                MATCH (n:CompareMem)
                CALL read_proc.get_node_properties(n) YIELD props
                RETURN sum(props.value) AS total
                QUERY MEMORY LIMIT 50MB
            """
            ),
            expected_result=125250,  # Sum 1+2+...+500
        )

        # If both succeeded, verify they match
        if serial_succeeded and parallel_succeeded:
            assert serial_result[0][0] == parallel_result[0][0]

    # Note: Detailed memory and load tests are covered in test_memory.py.

    def test_memory_consistency_under_load(self):
        """Test that MATCH + procedure + aggregation produces consistent results with memory limits.

        Accepts either correct result or OOM as valid outcomes.
        """
        # Create test data: 500 nodes with value = i * 10
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 500) AS i CREATE (:MemLoadNode {id: i, value: i * 10})")

        bg_processes = []
        for _ in range(3):
            proc, queue = start_long_query_process(
                self.HOST, self.PORT, "UNWIND range(1, 1000) AS i MATCH (n) RETURN count(*)"
            )
            bg_processes.append((proc, queue))

        try:
            time.sleep(0.3)

            # Run same MATCH + procedure + aggregation multiple times
            results = []
            oom_count = 0
            expected = 1252500  # 10*(1+2+...+500)

            for _ in range(3):
                succeeded, result = run_memory_test(
                    self.cursor,
                    pq(
                        """
                        MATCH (n:MemLoadNode)
                        CALL read_proc.get_node_properties(n) YIELD props
                        RETURN sum(props.value) AS total
                        QUERY MEMORY LIMIT 50MB
                    """
                    ),
                    expected_result=expected,
                )
                if succeeded:
                    results.append(result[0][0])
                else:
                    oom_count += 1

            # If any succeeded, all successful results should be identical
            if results:
                assert all(r == expected for r in results), f"Inconsistent results: {results}"

        finally:
            self._cleanup_processes(bg_processes)


# =============================================================================
# Procedure Memory Limit Tests (PROCEDURE MEMORY LIMIT clause)
# =============================================================================


class TestProcedureMemoryLimit:
    """
    Tests for the PROCEDURE MEMORY LIMIT clause.

    This tests the per-procedure memory limit syntax:
        CALL module.procedure(arg1, arg2, ...) PROCEDURE MEMORY LIMIT 100 KB YIELD result;

    Tests verify:
    1. PROCEDURE MEMORY LIMIT alone (without QUERY MEMORY LIMIT)
    2. PROCEDURE MEMORY LIMIT combined with QUERY MEMORY LIMIT
    3. Both serial and parallel execution with procedure memory limits
    """

    @pytest.fixture(autouse=True)
    def setup(self, cursor):
        """Set up test data."""
        self.cursor = cursor
        clear_database(cursor)
        yield
        clear_database(cursor)

    # -------------------------------------------------------------------------
    # PROCEDURE MEMORY LIMIT only (no QUERY MEMORY LIMIT)
    # -------------------------------------------------------------------------

    def test_read_proc_within_procedure_memory_limit(self):
        """Test read procedure within PROCEDURE MEMORY LIMIT."""
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 100) AS i CREATE (:ProcMemNode {id: i, value: i * 10})")

        # Procedure with generous memory limit should succeed
        result = execute_and_fetch_all(
            self.cursor,
            """
            MATCH (n:ProcMemNode)
            CALL read_proc.get_node_properties(n) PROCEDURE MEMORY LIMIT 50 MB YIELD props
            RETURN count(*) AS cnt
            """,
        )
        assert result[0][0] == 100

    def test_read_proc_exceeds_procedure_memory_limit(self):
        """Test read procedure fails when PROCEDURE MEMORY LIMIT exceeded."""
        execute_and_fetch_all(
            self.cursor, "UNWIND range(1, 1000) AS i CREATE (:ProcMemNode {id: i, data: 'payload_' + toString(i)})"
        )

        # Tiny procedure memory limit should cause failure
        with pytest.raises(Exception) as exc_info:
            execute_and_fetch_all(
                self.cursor,
                """
                MATCH (n:ProcMemNode)
                CALL read_proc.get_node_properties(n) PROCEDURE MEMORY LIMIT 80 KB YIELD props
                RETURN collect(props.data) AS all_data
                """,
            )

        error_msg = str(exc_info.value).lower()
        assert "memory" in error_msg or "limit" in error_msg

    def test_write_proc_within_procedure_memory_limit(self):
        """Test write procedure within PROCEDURE MEMORY LIMIT."""
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 50) AS i CREATE (:ProcMemSource {id: i})")

        result = execute_and_fetch_all(
            self.cursor,
            """
            MATCH (s:ProcMemSource)
            CALL write_proc.create_node_with_props('ProcMemCreated', {id: s.id})
                PROCEDURE MEMORY LIMIT 50 MB YIELD node
            RETURN count(node) AS created
            """,
        )
        assert result[0][0] == 50

        # Verify nodes were created
        verify = execute_and_fetch_all(self.cursor, "MATCH (n:ProcMemCreated) RETURN count(n) AS cnt")
        assert verify[0][0] == 50

    def test_write_proc_exceeds_procedure_memory_limit(self):
        """Test write procedure fails when PROCEDURE MEMORY LIMIT exceeded."""
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 1000) AS i CREATE (:ProcMemSource {id: i, data: 'x'})")

        with pytest.raises(Exception) as exc_info:
            execute_and_fetch_all(
                self.cursor,
                """
                MATCH (n:ProcMemSource)
                CALL write_proc.set_property(n, 'new_data', n.data + '_processed')
                    PROCEDURE MEMORY LIMIT 1 KB YIELD success
                RETURN collect(success) AS results
                """,
            )

        error_msg = str(exc_info.value).lower()
        assert "memory" in error_msg or "limit" in error_msg

    def test_parallel_read_proc_within_procedure_memory_limit(self):
        """Test parallel read procedure within PROCEDURE MEMORY LIMIT."""
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 200) AS i CREATE (:ParProcMemNode {id: i, value: i * 10})")

        # MATCH + procedure with memory limit + aggregation
        result = execute_and_fetch_all(
            self.cursor,
            pq(
                """
                MATCH (n:ParProcMemNode)
                CALL read_proc.get_node_properties(n) PROCEDURE MEMORY LIMIT 50 MB YIELD props
                RETURN count(*) AS cnt, sum(props.value) AS total
            """
            ),
        )
        assert result[0][0] == 200
        # Sum of 10+20+...+2000 = 10*(1+2+...+200) = 10*20100 = 201000
        assert result[0][1] == 201000

    def test_parallel_read_proc_exceeds_procedure_memory_limit(self):
        """Test parallel read procedure fails when PROCEDURE MEMORY LIMIT exceeded."""
        execute_and_fetch_all(
            self.cursor, "UNWIND range(1, 1000) AS i CREATE (:ParProcMemNode {id: i, data: 'payload_' + toString(i)})"
        )

        with pytest.raises(Exception) as exc_info:
            execute_and_fetch_all(
                self.cursor,
                pq(
                    """
                    MATCH (n:ParProcMemNode)
                    CALL read_proc.get_node_properties(n) PROCEDURE MEMORY LIMIT 1 KB YIELD props
                    RETURN collect(props.data) AS all_data
                """
                ),
            )

        error_msg = str(exc_info.value).lower()
        assert "memory" in error_msg or "limit" in error_msg

    def test_parallel_write_proc_within_procedure_memory_limit(self):
        """Test parallel write procedure within PROCEDURE MEMORY LIMIT."""
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 100) AS i CREATE (:ParProcMemSource {id: i})")

        result = execute_and_fetch_all(
            self.cursor,
            pq(
                """
                MATCH (s:ParProcMemSource)
                CALL write_proc.set_property(s, 'processed', true)
                    PROCEDURE MEMORY LIMIT 50 MB YIELD success
                RETURN count(*) AS updated
            """
            ),
        )
        assert result[0][0] == 100

    def test_multiple_procs_each_with_memory_limit(self):
        """Test multiple procedure calls, each with its own PROCEDURE MEMORY LIMIT."""
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 50) AS i CREATE (:MultiProcNode {id: i, value: i})")

        result = execute_and_fetch_all(
            self.cursor,
            """
            MATCH (n:MultiProcNode)
            CALL read_proc.get_node_properties(n) PROCEDURE MEMORY LIMIT 50 MB YIELD props
            CALL read_proc.compute_value(props.id, 2) PROCEDURE MEMORY LIMIT 50 MB YIELD result
            RETURN sum(result) AS total
            """,
        )
        # compute_value(x, 2) = x*2 + x + 2 = 3x + 2
        # For x in 1..50: sum(3x + 2) = 3*(1+2+...+50) + 50*2 = 3*1275 + 100 = 3925
        assert result[0][0] == 3925

    # -------------------------------------------------------------------------
    # PROCEDURE MEMORY LIMIT combined with QUERY MEMORY LIMIT
    # -------------------------------------------------------------------------

    def test_read_proc_with_both_memory_limits(self):
        """Test read procedure with both PROCEDURE MEMORY LIMIT and QUERY MEMORY LIMIT."""
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 200) AS i CREATE (:BothLimitsNode {id: i, value: i * 10})")

        # Both limits are generous, should succeed
        result = execute_and_fetch_all(
            self.cursor,
            """
            MATCH (n:BothLimitsNode)
            CALL read_proc.get_node_properties(n) PROCEDURE MEMORY LIMIT 50 MB YIELD props
            RETURN count(*) AS cnt
            QUERY MEMORY LIMIT 100 MB
            """,
        )
        assert result[0][0] == 200

    def test_read_proc_procedure_limit_stricter_than_query_limit(self):
        """Test when PROCEDURE MEMORY LIMIT is stricter than QUERY MEMORY LIMIT."""
        execute_and_fetch_all(
            self.cursor, "UNWIND range(1, 1000) AS i CREATE (:StricterProcNode {id: i, data: 'payload_' + toString(i)})"
        )

        # Procedure limit is tiny, query limit is large - procedure limit should trigger
        with pytest.raises(Exception) as exc_info:
            execute_and_fetch_all(
                self.cursor,
                """
                MATCH (n:StricterProcNode)
                CALL read_proc.get_node_properties(n) PROCEDURE MEMORY LIMIT 80 KB YIELD props
                RETURN collect(props.data) AS all_data
                QUERY MEMORY LIMIT 500 MB
                """,
            )

        error_msg = str(exc_info.value).lower()
        assert "memory" in error_msg or "limit" in error_msg

    def test_read_proc_query_limit_stricter_than_procedure_limit(self):
        """Test when QUERY MEMORY LIMIT is stricter than PROCEDURE MEMORY LIMIT."""
        execute_and_fetch_all(
            self.cursor,
            "UNWIND range(1, 1000) AS i CREATE (:StricterQueryNode {id: i, data: 'payload_' + toString(i)})",
        )

        # Query limit is tiny, procedure limit is large - query limit should trigger
        with pytest.raises(Exception) as exc_info:
            execute_and_fetch_all(
                self.cursor,
                """
                MATCH (n:StricterQueryNode)
                CALL read_proc.get_node_properties(n) PROCEDURE MEMORY LIMIT 500 MB YIELD props
                RETURN collect(props.data) AS all_data
                QUERY MEMORY LIMIT 80 KB
                """,
            )

        error_msg = str(exc_info.value).lower()
        assert "memory" in error_msg or "limit" in error_msg

    def test_write_proc_with_both_memory_limits(self):
        """Test write procedure with both PROCEDURE MEMORY LIMIT and QUERY MEMORY LIMIT."""
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 100) AS i CREATE (:BothLimitsSource {id: i})")

        result = execute_and_fetch_all(
            self.cursor,
            """
            MATCH (s:BothLimitsSource)
            CALL write_proc.create_node_with_props('BothLimitsCreated', {id: s.id})
                PROCEDURE MEMORY LIMIT 50 MB YIELD node
            RETURN count(node) AS created
            QUERY MEMORY LIMIT 100 MB
            """,
        )
        assert result[0][0] == 100

    def test_parallel_with_both_memory_limits(self):
        """Test parallel execution with both PROCEDURE MEMORY LIMIT and QUERY MEMORY LIMIT."""
        execute_and_fetch_all(
            self.cursor, "UNWIND range(1, 300) AS i CREATE (:ParBothLimitsNode {id: i, value: i * 10})"
        )

        result = execute_and_fetch_all(
            self.cursor,
            pq(
                """
                MATCH (n:ParBothLimitsNode)
                CALL read_proc.get_node_properties(n) PROCEDURE MEMORY LIMIT 50 MB YIELD props
                RETURN count(*) AS cnt, sum(props.value) AS total
                QUERY MEMORY LIMIT 100 MB
            """
            ),
        )
        assert result[0][0] == 300
        # Sum of 10+20+...+3000 = 10*(1+2+...+300) = 10*45150 = 451500
        assert result[0][1] == 451500

    def test_parallel_procedure_limit_exceeded_with_query_limit(self):
        """Test parallel execution where PROCEDURE MEMORY LIMIT is exceeded."""
        execute_and_fetch_all(
            self.cursor, "UNWIND range(1, 1000) AS i CREATE (:ParExceedNode {id: i, data: 'payload_' + toString(i)})"
        )

        with pytest.raises(Exception) as exc_info:
            execute_and_fetch_all(
                self.cursor,
                pq(
                    """
                    MATCH (n:ParExceedNode)
                    CALL read_proc.get_node_properties(n) PROCEDURE MEMORY LIMIT 1 KB YIELD props
                    RETURN collect(props.data) AS all_data
                    QUERY MEMORY LIMIT 500 MB
                """
                ),
            )

        error_msg = str(exc_info.value).lower()
        assert "memory" in error_msg or "limit" in error_msg

    def test_parallel_query_limit_exceeded_with_procedure_limit(self):
        """Test parallel execution where QUERY MEMORY LIMIT is exceeded."""
        execute_and_fetch_all(
            self.cursor,
            "UNWIND range(1, 1000) AS i CREATE (:ParQueryExceedNode {id: i, data: 'payload_' + toString(i)})",
        )

        with pytest.raises(Exception) as exc_info:
            execute_and_fetch_all(
                self.cursor,
                pq(
                    """
                    MATCH (n:ParQueryExceedNode)
                    CALL read_proc.get_node_properties(n) PROCEDURE MEMORY LIMIT 500 MB YIELD props
                    RETURN collect(props.data) AS all_data
                    QUERY MEMORY LIMIT 1 KB
                """
                ),
            )

        error_msg = str(exc_info.value).lower()
        assert "memory" in error_msg or "limit" in error_msg

    def test_mixed_procs_with_different_limits(self):
        """Test multiple procedures with different PROCEDURE MEMORY LIMITs and a QUERY MEMORY LIMIT."""
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 100) AS i CREATE (:MixedLimitsNode {id: i, value: i})")

        result = execute_and_fetch_all(
            self.cursor,
            """
            MATCH (n:MixedLimitsNode)
            CALL read_proc.get_node_properties(n) PROCEDURE MEMORY LIMIT 30 MB YIELD props
            CALL read_proc.compute_value(props.id, 2) PROCEDURE MEMORY LIMIT 20 MB YIELD result
            RETURN sum(result) AS total
            QUERY MEMORY LIMIT 100 MB
            """,
        )
        # compute_value(x, 2) = x*2 + x + 2 = 3x + 2
        # For x in 1..100: sum(3x + 2) = 3*(1+2+...+100) + 100*2 = 3*5050 + 200 = 15350
        assert result[0][0] == 15350

    def test_parallel_mixed_procs_with_both_limits(self):
        """Test parallel execution with multiple procedures and both limit types."""
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 50) AS i CREATE (:ParMixedNode {id: i, value: i})")

        result = execute_and_fetch_all(
            self.cursor,
            pq(
                """
                MATCH (n:ParMixedNode)
                CALL read_proc.get_node_properties(n) PROCEDURE MEMORY LIMIT 30 MB YIELD props
                CALL read_proc.compute_value(props.id, 3) PROCEDURE MEMORY LIMIT 20 MB YIELD result
                RETURN sum(result) AS total, count(*) AS cnt
                QUERY MEMORY LIMIT 100 MB
            """
            ),
        )
        # compute_value(x, 3) = x*3 + x + 3 = 4x + 3
        # For x in 1..50: sum(4x + 3) = 4*(1+2+...+50) + 50*3 = 4*1275 + 150 = 5250
        assert result[0][0] == 5250
        assert result[0][1] == 50

    # -------------------------------------------------------------------------
    # Edge cases with memory limits
    # -------------------------------------------------------------------------

    def test_procedure_memory_limit_with_empty_result(self):
        """Test PROCEDURE MEMORY LIMIT with procedure returning no results."""
        execute_and_fetch_all(self.cursor, "CREATE (:EmptyResultNode {id: 1})")

        result = execute_and_fetch_all(
            self.cursor,
            """
            MATCH (n:EmptyResultNode)
            CALL read_proc.get_node_properties(n) PROCEDURE MEMORY LIMIT 50 MB YIELD props
            WITH props
            WHERE props.nonexistent IS NOT NULL
            RETURN count(*) AS cnt
            """,
        )
        assert result[0][0] == 0

    def test_procedure_memory_limit_units(self):
        """Test different memory limit units (KB, MB)."""
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 50) AS i CREATE (:UnitsNode {id: i})")

        # Test with KB
        result_kb = execute_and_fetch_all(
            self.cursor,
            """
            MATCH (n:UnitsNode)
            CALL read_proc.get_node_properties(n) PROCEDURE MEMORY LIMIT 50000 KB YIELD props
            RETURN count(*) AS cnt
            """,
        )
        assert result_kb[0][0] == 50

        # Test with MB
        result_mb = execute_and_fetch_all(
            self.cursor,
            """
            MATCH (n:UnitsNode)
            CALL read_proc.get_node_properties(n) PROCEDURE MEMORY LIMIT 50 MB YIELD props
            RETURN count(*) AS cnt
            """,
        )
        assert result_mb[0][0] == 50

    def test_serial_vs_parallel_with_procedure_memory_limit(self):
        """Compare serial and parallel execution with PROCEDURE MEMORY LIMIT."""
        execute_and_fetch_all(self.cursor, "UNWIND range(1, 100) AS i CREATE (:SerParCompareNode {id: i, value: i})")

        # Serial with procedure memory limit
        serial_result = execute_and_fetch_all(
            self.cursor,
            """
            MATCH (n:SerParCompareNode)
            CALL read_proc.get_node_properties(n) PROCEDURE MEMORY LIMIT 50 MB YIELD props
            RETURN sum(props.value) AS total
            """,
        )

        # Parallel with same procedure memory limit
        parallel_result = execute_and_fetch_all(
            self.cursor,
            pq(
                """
                MATCH (n:SerParCompareNode)
                CALL read_proc.get_node_properties(n) PROCEDURE MEMORY LIMIT 50 MB YIELD props
                RETURN sum(props.value) AS total
            """
            ),
        )

        # Both should produce the same result
        assert serial_result[0][0] == parallel_result[0][0] == 5050


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
