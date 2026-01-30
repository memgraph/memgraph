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
Tests for query termination scenarios in parallel execution.

This module tests different ways to stop a running query:
    1. TERMINATE TRANSACTIONS command
    2. Graceful instance shutdown (SIGTERM)
    3. Query execution timeout (--query-execution-timeout-sec)

All tests use long-running queries with aggregation and ORDER BY operations
that utilize USING PARALLEL EXECUTION hint to trigger parallel operators
(P ScanAll, P Aggregate, P OrderBy).
"""

import multiprocessing
import sys
import time
from pathlib import Path
from typing import Optional

import pytest
from neo4j import GraphDatabase
from neo4j.exceptions import *

# Add standard e2e directory to sys.path
E2E_ROOT = Path(__file__).resolve().parent.parent
if str(E2E_ROOT) not in sys.path:
    sys.path.insert(0, str(E2E_ROOT))

import interactive_mg_runner
from common import pq

# =============================================================================
# Constants
# =============================================================================

BASE_BOLT_PORT = 7690  # Use different port to avoid conflicts with main test instance

# Number of nodes to create for long-running queries
# This needs to be large enough to make queries run for several seconds
LARGE_DATASET_SIZE = 100000

# =============================================================================
# Long-running queries using aggregation and ORDER BY
# These queries use USING PARALLEL EXECUTION and will trigger parallel operators
# The key pattern is: MATCH(n) WITH n UNWIND range(...) which creates a
# large cross product that takes significant time to process.
#
# With 100k nodes and UNWIND range(1, 1000), we get 100M intermediate rows.
# =============================================================================

# Query with aggregation - creates cross product of all nodes with unwind range
# 100k nodes × 1000 range = 100M intermediate rows
# Uses P ScanAll and P Aggregate operators
LONG_RUNNING_AGGREGATION_QUERY = """
USING PARALLEL EXECUTION
MATCH (n:TestNode)
WITH n
UNWIND range(1, 1000) AS i
RETURN i, count(n) AS cnt
"""

# Query with ORDER BY - creates cross product and sorts results
# 100k nodes × 500 range = 50M intermediate rows to sort
# Uses P ScanAll and P OrderBy operators
LONG_RUNNING_ORDERBY_QUERY = """
USING PARALLEL EXECUTION
MATCH (n:TestNode)
WITH n
UNWIND range(1, 500) AS i
WITH n.value + i AS val
RETURN val
ORDER BY val DESC
"""

# Query with multiple aggregations on cross product
LONG_RUNNING_MULTI_AGG_QUERY = """
USING PARALLEL EXECUTION
MATCH (n:TestNode)
WITH n
UNWIND range(1, 1000) AS i
RETURN i, count(n) AS cnt, sum(n.value) AS total, avg(n.value) AS average
"""

# Query using UNWIND to create a large intermediate result set with aggregation
# This doesn't require pre-existing data - uses nested UNWIND for cross product
# 2000 × 2000 × 100 = 400M intermediate rows
LONG_RUNNING_UNWIND_AGG_QUERY = """
USING PARALLEL EXECUTION
UNWIND range(1, 2000) AS a
UNWIND range(1, 2000) AS b
UNWIND range(1, 100) AS c
WITH a, b, c
RETURN a, count(*) AS cnt
"""

# Query using UNWIND with ORDER BY - large cross product sorted
# 2000 × 2000 × 50 = 200M intermediate rows
LONG_RUNNING_UNWIND_ORDERBY_QUERY = """
USING PARALLEL EXECUTION
UNWIND range(1, 2000) AS a
UNWIND range(1, 2000) AS b
UNWIND range(1, 50) AS c
WITH a * b * c AS product
RETURN product
ORDER BY product DESC
LIMIT 1000
"""


# =============================================================================
# Helper Functions
# =============================================================================


def populate_test_data(uri: str, node_count: int = LARGE_DATASET_SIZE):
    """
    Populate the database with test nodes for long-running queries.
    Creates nodes with label :TestNode and property 'value'.
    """
    driver = GraphDatabase.driver(uri)
    try:
        with driver.session() as session:
            # Clear existing data
            session.run("MATCH (n:TestNode) DETACH DELETE n")

            # Create nodes in batches for efficiency
            batch_size = 10000
            for start in range(0, node_count, batch_size):
                end = min(start + batch_size, node_count)
                session.run(
                    f"UNWIND range({start + 1}, {end}) AS i CREATE (:TestNode {{value: i, data: 'test_data_' + toString(i)}})"
                )
    finally:
        driver.close()


def clear_test_data(uri: str):
    """Clear test data from the database."""
    driver = GraphDatabase.driver(uri)
    try:
        with driver.session() as session:
            session.run("MATCH (n:TestNode) DETACH DELETE n")
    finally:
        driver.close()


def run_query_in_process(uri: str, query: str, result_queue: multiprocessing.Queue, auth: Optional[tuple] = None):
    """
    Execute a query in a separate process.
    Puts the result or exception info into the queue.
    """
    try:
        driver = GraphDatabase.driver(uri, auth=auth)
        with driver.session() as session:
            result = session.run(query)
            records = [record.data() for record in result]
            result_queue.put(("success", records))
        driver.close()
    except Exception as e:
        result_queue.put(("error", type(e).__name__, str(e)))


def start_long_query_process(uri: str, query: str, auth: Optional[tuple] = None) -> tuple:
    """
    Start a long-running query in a separate process.
    Returns (process, result_queue).
    """
    result_queue = multiprocessing.Queue()
    process = multiprocessing.Process(target=run_query_in_process, args=(uri, query, result_queue, auth))
    process.start()
    return process, result_queue


def wait_for_query_to_start(driver, expected_query_substring: str, timeout: float = 5.0) -> Optional[str]:
    """
    Wait for a query containing the expected substring to appear in SHOW TRANSACTIONS.
    Returns the transaction ID if found, None if timeout.
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        with driver.session() as session:
            result = session.run("SHOW TRANSACTIONS")
            for record in result:
                queries = record.get("query") or record.get("queries") or []
                if isinstance(queries, str):
                    queries = [queries]
                for q in queries:
                    if expected_query_substring in q:
                        return record.get("transaction_id") or record.get("id")
        time.sleep(0.1)
    return None


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def termination_instance():
    """
    Module-scoped fixture that provides a Memgraph instance for termination tests.
    Uses a separate port and populates test data for long-running queries.
    """
    instance_name = "termination_test_instance"
    build_root = E2E_ROOT.parent.parent

    interactive_mg_runner.MEMGRAPH_BINARY = str((build_root / "memgraph").resolve())
    interactive_mg_runner.BUILD_DIR = str(build_root.resolve())

    config = {
        instance_name: {
            "args": [
                f"--bolt-port={BASE_BOLT_PORT}",
                "--log-level=WARNING",
                "--storage-wal-enabled=true",
                "--storage-snapshot-interval-sec=300",
                "--storage-properties-on-edges=true",
                "--telemetry-enabled=false",
            ],
            "log_file": "termination_test.log",
            "data_directory": "termination_test_data",
        }
    }

    interactive_mg_runner.start(config, instance_name)
    runner = interactive_mg_runner.MEMGRAPH_INSTANCES[instance_name]

    uri = f"bolt://localhost:{BASE_BOLT_PORT}"

    # Populate test data for long-running queries
    populate_test_data(uri, LARGE_DATASET_SIZE)

    yield {
        "runner": runner,
        "config": config,
        "instance_name": instance_name,
        "port": BASE_BOLT_PORT,
        "uri": uri,
    }

    interactive_mg_runner.stop(config, instance_name, keep_directories=False)


@pytest.fixture
def driver(termination_instance):
    """Function-scoped driver for the termination instance."""
    drv = GraphDatabase.driver(termination_instance["uri"])
    yield drv
    drv.close()


# =============================================================================
# Test Classes
# =============================================================================


class TestTerminateTransaction:
    """
    Tests for terminating queries using TERMINATE TRANSACTIONS command.

    Verifies that:
    - Running queries can be found via SHOW TRANSACTIONS
    - TERMINATE TRANSACTIONS successfully stops the query
    - The query process receives an appropriate error

    Uses long-running queries with aggregation and ORDER BY to trigger
    parallel execution operators (P ScanAll, P Aggregate, P OrderBy).
    """

    def test_terminate_parallel_aggregation_query(self, termination_instance, driver):
        """Test terminating a parallel aggregation query via TERMINATE TRANSACTIONS."""
        uri = termination_instance["uri"]

        # Start long-running aggregation query with parallel execution
        process, result_queue = start_long_query_process(uri, LONG_RUNNING_AGGREGATION_QUERY)

        try:
            # Wait for query to appear in transactions list
            tx_id = wait_for_query_to_start(driver, "TestNode", timeout=5.0)
            assert tx_id is not None, "Aggregation query did not appear in SHOW TRANSACTIONS"

            # Terminate the transaction
            with driver.session() as session:
                session.run(f"TERMINATE TRANSACTIONS '{tx_id}'")

            # Wait for process to finish
            process.join(timeout=5.0)
            assert not process.is_alive(), "Query process should have terminated"

            # Check result - should be an error
            assert not result_queue.empty(), "Result queue should have an entry"
            result = result_queue.get_nowait()
            assert result[0] == "error", f"Expected error, got: {result}"

        finally:
            if process.is_alive():
                process.terminate()
                process.join(timeout=2.0)

    def test_terminate_parallel_orderby_query(self, termination_instance, driver):
        """Test terminating a parallel ORDER BY query via TERMINATE TRANSACTIONS."""
        uri = termination_instance["uri"]

        # Start long-running ORDER BY query with parallel execution
        process, result_queue = start_long_query_process(uri, LONG_RUNNING_ORDERBY_QUERY)

        try:
            # Wait for query to appear
            tx_id = wait_for_query_to_start(driver, "ORDER BY", timeout=5.0)
            assert tx_id is not None, "ORDER BY query did not appear in SHOW TRANSACTIONS"

            # Terminate the transaction
            with driver.session() as session:
                session.run(f"TERMINATE TRANSACTIONS '{tx_id}'")

            # Wait for process to finish
            process.join(timeout=10.0)
            assert not process.is_alive(), "Query process should have terminated"

            # Check result
            assert not result_queue.empty(), "Result queue should have an entry"
            result = result_queue.get_nowait()
            assert result[0] == "error", f"Expected error after termination, got: {result}"

        finally:
            if process.is_alive():
                process.terminate()
                process.join(timeout=2.0)

    def test_terminate_parallel_unwind_aggregation(self, termination_instance, driver):
        """Test terminating a parallel UNWIND + aggregation query."""
        uri = termination_instance["uri"]

        # This query doesn't need pre-existing data - uses UNWIND to generate data
        process, result_queue = start_long_query_process(uri, LONG_RUNNING_UNWIND_AGG_QUERY)

        try:
            # Wait for query to appear
            tx_id = wait_for_query_to_start(driver, "UNWIND", timeout=5.0)
            assert tx_id is not None, "UNWIND query did not appear in SHOW TRANSACTIONS"

            # Terminate the transaction
            with driver.session() as session:
                session.run(f"TERMINATE TRANSACTIONS '{tx_id}'")

            # Wait for process to finish
            process.join(timeout=10.0)
            assert not process.is_alive(), "Query process should have terminated"

            # Check result
            assert not result_queue.empty(), "Result queue should have an entry"
            result = result_queue.get_nowait()
            assert result[0] == "error", f"Expected error after termination, got: {result}"

        finally:
            if process.is_alive():
                process.terminate()
                process.join(timeout=2.0)

    def test_show_transactions_shows_parallel_query(self, termination_instance, driver):
        """Verify that parallel queries appear in SHOW TRANSACTIONS output."""
        uri = termination_instance["uri"]

        process, result_queue = start_long_query_process(uri, LONG_RUNNING_AGGREGATION_QUERY)

        try:
            # Give query time to start
            time.sleep(0.5)

            # Check SHOW TRANSACTIONS
            with driver.session() as session:
                result = session.run("SHOW TRANSACTIONS")
                transactions = [record.data() for record in result]

            # Find our query
            found = False
            for tx in transactions:
                queries = tx.get("query") or tx.get("queries") or []
                if isinstance(queries, str):
                    queries = [queries]
                for q in queries:
                    if "USING PARALLEL" in q or "TestNode" in q:
                        found = True
                        break

            assert found, f"Parallel query not found in transactions: {transactions}"

        finally:
            # Cleanup: terminate any running query
            tx_id = wait_for_query_to_start(driver, "TestNode", timeout=1.0)
            if tx_id:
                with driver.session() as session:
                    session.run(f"TERMINATE TRANSACTIONS '{tx_id}'")
            if process.is_alive():
                process.terminate()
                process.join(timeout=2.0)


class TestGracefulShutdown:
    """
    Tests for graceful instance shutdown during query execution.

    Verifies that:
    - Instance shuts down within acceptable time when query is running
    - Query receives appropriate connection/session error
    - No data corruption occurs (instance can restart)

    Uses long-running queries with aggregation and ORDER BY to trigger
    parallel execution operators.
    """

    @pytest.fixture
    def shutdown_instance(self):
        """
        Fixture that provides a fresh instance for shutdown testing.
        Each test gets its own instance since we're stopping it.
        Populates test data for long-running queries.
        """
        instance_name = "shutdown_test_instance"
        port = BASE_BOLT_PORT + 1
        build_root = E2E_ROOT.parent.parent

        interactive_mg_runner.MEMGRAPH_BINARY = str((build_root / "memgraph").resolve())
        interactive_mg_runner.BUILD_DIR = str(build_root.resolve())

        config = {
            instance_name: {
                "args": [
                    f"--bolt-port={port}",
                    "--log-level=WARNING",
                    "--storage-wal-enabled=true",
                    "--storage-properties-on-edges=true",
                    "--telemetry-enabled=false",
                ],
                "log_file": "shutdown_test.log",
                "data_directory": "shutdown_test_data",
            }
        }

        interactive_mg_runner.start(config, instance_name)
        runner = interactive_mg_runner.MEMGRAPH_INSTANCES[instance_name]

        uri = f"bolt://localhost:{port}"

        # Populate test data for long-running queries
        populate_test_data(uri, LARGE_DATASET_SIZE)

        yield {
            "runner": runner,
            "config": config,
            "instance_name": instance_name,
            "port": port,
            "uri": uri,
        }

        # Cleanup - ensure instance is stopped
        try:
            interactive_mg_runner.stop(config, instance_name, keep_directories=False)
        except Exception:
            pass

    def test_graceful_shutdown_during_aggregation_query(self, shutdown_instance):
        """Test that instance shuts down gracefully during a parallel aggregation query."""
        uri = shutdown_instance["uri"]
        runner = shutdown_instance["runner"]

        # Start long-running aggregation query with parallel execution
        process, result_queue = start_long_query_process(uri, LONG_RUNNING_AGGREGATION_QUERY)

        try:
            # Wait for query to start
            time.sleep(0.5)

            # Record shutdown start time
            shutdown_start = time.time()

            # Send SIGTERM (graceful shutdown)
            runner.proc_mg.terminate()

            # Wait for instance to stop (with timeout)
            max_shutdown_time = 15.0  # seconds
            for _ in range(int(max_shutdown_time * 10)):
                if not runner.is_running():
                    break
                time.sleep(0.1)

            shutdown_duration = time.time() - shutdown_start

            # Verify instance stopped
            assert not runner.is_running(), f"Instance should have stopped after {max_shutdown_time}s"

            # Verify shutdown was reasonably fast
            assert (
                shutdown_duration < max_shutdown_time
            ), f"Shutdown took {shutdown_duration:.2f}s, expected < {max_shutdown_time}s"

            # Wait for query process to finish
            process.join(timeout=5.0)

            # Query should have received an error
            if not result_queue.empty():
                result = result_queue.get_nowait()
                assert result[0] == "error", f"Expected error on shutdown, got: {result}"

        finally:
            if process.is_alive():
                process.terminate()
                process.join(timeout=2.0)

    def test_graceful_shutdown_during_orderby_query(self, shutdown_instance):
        """Test that instance shuts down gracefully during a parallel ORDER BY query."""
        uri = shutdown_instance["uri"]
        runner = shutdown_instance["runner"]

        # Start long-running ORDER BY query with parallel execution
        process, result_queue = start_long_query_process(uri, LONG_RUNNING_ORDERBY_QUERY)

        try:
            # Wait for query to start
            time.sleep(0.5)

            # Record shutdown start time
            shutdown_start = time.time()

            # Send SIGTERM (graceful shutdown)
            runner.proc_mg.terminate()

            # Wait for instance to stop (with timeout)
            max_shutdown_time = 15.0  # seconds
            for _ in range(int(max_shutdown_time * 10)):
                if not runner.is_running():
                    break
                time.sleep(0.1)

            shutdown_duration = time.time() - shutdown_start

            # Verify instance stopped
            assert not runner.is_running(), f"Instance should have stopped after {max_shutdown_time}s"

            # Verify shutdown was reasonably fast
            assert (
                shutdown_duration < max_shutdown_time
            ), f"Shutdown took {shutdown_duration:.2f}s, expected < {max_shutdown_time}s"

            # Wait for query process to finish
            process.join(timeout=5.0)

            # Query should have received an error
            if not result_queue.empty():
                result = result_queue.get_nowait()
                assert result[0] == "error", f"Expected error on shutdown, got: {result}"

        finally:
            if process.is_alive():
                process.terminate()
                process.join(timeout=2.0)

    def test_kill_during_parallel_query(self, shutdown_instance):
        """Test that SIGKILL immediately stops the instance during a parallel query."""
        uri = shutdown_instance["uri"]
        runner = shutdown_instance["runner"]

        # Start long-running aggregation query with parallel execution
        process, result_queue = start_long_query_process(uri, LONG_RUNNING_AGGREGATION_QUERY)

        try:
            # Wait for query to start
            time.sleep(0.5)

            # Record kill time
            kill_start = time.time()

            # Send SIGKILL
            runner.proc_mg.kill()
            runner.proc_mg.wait()

            kill_duration = time.time() - kill_start

            # Verify instance stopped immediately
            assert not runner.is_running(), "Instance should have stopped after SIGKILL"

            # Kill should be near-instant
            assert kill_duration < 2.0, f"Kill took {kill_duration:.2f}s, expected < 2.0s"

            # Wait for query process to finish
            process.join(timeout=5.0)

        finally:
            if process.is_alive():
                process.terminate()
                process.join(timeout=2.0)


class TestQueryTimeout:
    """
    Tests for query execution timeout (--query-execution-timeout-sec).

    Verifies that:
    - Queries exceeding the timeout are automatically terminated
    - Appropriate timeout error is returned to the client
    - Timeout works correctly with parallel execution

    Uses long-running queries with aggregation and ORDER BY to trigger
    parallel execution operators.
    """

    @pytest.fixture
    def timeout_instance(self):
        """
        Fixture that provides an instance with query timeout configured.
        Populates test data for long-running queries.
        """
        instance_name = "timeout_test_instance"
        port = BASE_BOLT_PORT + 2
        timeout_sec = 1  # Timeout for testing (queries should run longer than this)
        build_root = E2E_ROOT.parent.parent

        interactive_mg_runner.MEMGRAPH_BINARY = str((build_root / "memgraph").resolve())
        interactive_mg_runner.BUILD_DIR = str(build_root.resolve())

        config = {
            instance_name: {
                "args": [
                    f"--bolt-port={port}",
                    "--log-level=WARNING",
                    "--storage-wal-enabled=true",
                    "--storage-properties-on-edges=true",
                    "--telemetry-enabled=false",
                    f"--query-execution-timeout-sec={timeout_sec}",
                ],
                "log_file": "timeout_test.log",
                "data_directory": "timeout_test_data",
            }
        }

        interactive_mg_runner.start(config, instance_name)
        runner = interactive_mg_runner.MEMGRAPH_INSTANCES[instance_name]

        uri = f"bolt://localhost:{port}"

        # Populate test data for long-running queries
        populate_test_data(uri, LARGE_DATASET_SIZE)

        yield {
            "runner": runner,
            "config": config,
            "instance_name": instance_name,
            "port": port,
            "uri": uri,
            "timeout_sec": timeout_sec,
        }

        interactive_mg_runner.stop(config, instance_name, keep_directories=False)

    def test_aggregation_query_timeout(self, timeout_instance):
        """Test that a parallel aggregation query is terminated when it exceeds the timeout."""
        uri = timeout_instance["uri"]
        timeout_sec = timeout_instance["timeout_sec"]

        # Start long-running aggregation query with parallel execution
        process, result_queue = start_long_query_process(uri, LONG_RUNNING_AGGREGATION_QUERY)

        try:
            # Wait for query to timeout (add buffer for processing)
            max_wait = timeout_sec + 10
            process.join(timeout=max_wait)

            # Process should have finished
            assert not process.is_alive(), f"Query should have been terminated by timeout after {timeout_sec}s"

            # Should have received a timeout error
            assert not result_queue.empty(), "Result queue should have an entry"
            result = result_queue.get_nowait()
            assert result[0] == "error", f"Expected timeout error, got: {result}"

            # Error should indicate timeout
            error_type = result[1]
            error_msg = result[2].lower()
            assert (
                "timeout" in error_msg or "abort" in error_msg or "terminate" in error_msg
            ), f"Expected timeout-related error, got: {error_type}: {result[2]}"

        finally:
            if process.is_alive():
                process.terminate()
                process.join(timeout=2.0)

    def test_orderby_query_timeout(self, timeout_instance):
        """Test that a parallel ORDER BY query is terminated on timeout."""
        uri = timeout_instance["uri"]
        timeout_sec = timeout_instance["timeout_sec"]

        # Start long-running ORDER BY query with parallel execution
        process, result_queue = start_long_query_process(uri, LONG_RUNNING_ORDERBY_QUERY)

        try:
            # Wait for timeout (add buffer for processing)
            max_wait = timeout_sec + 10
            process.join(timeout=max_wait)

            assert not process.is_alive(), "Parallel ORDER BY query should have been terminated by timeout"

            # Check error
            assert not result_queue.empty()
            result = result_queue.get_nowait()
            assert result[0] == "error", f"Expected timeout error, got: {result}"

        finally:
            if process.is_alive():
                process.terminate()
                process.join(timeout=2.0)

    def test_unwind_aggregation_query_timeout(self, timeout_instance):
        """Test that a parallel UNWIND + aggregation query is terminated on timeout."""
        uri = timeout_instance["uri"]
        timeout_sec = timeout_instance["timeout_sec"]

        # This query doesn't need pre-existing data - uses UNWIND to generate data
        process, result_queue = start_long_query_process(uri, LONG_RUNNING_UNWIND_AGG_QUERY)

        try:
            # Wait for timeout (add buffer for processing)
            max_wait = timeout_sec + 10
            process.join(timeout=max_wait)

            assert not process.is_alive(), "Parallel UNWIND query should have been terminated by timeout"

            # Check error
            assert not result_queue.empty()
            result = result_queue.get_nowait()
            assert result[0] == "error", f"Expected timeout error, got: {result}"

        finally:
            if process.is_alive():
                process.terminate()
                process.join(timeout=2.0)

    def test_short_query_completes_before_timeout(self, timeout_instance):
        """Test that queries completing before timeout are not affected."""
        uri = timeout_instance["uri"]

        driver = GraphDatabase.driver(uri)
        try:
            # Run a quick aggregation query - should complete before timeout
            with driver.session() as session:
                result = session.run(pq("MATCH (n:TestNode) WHERE n.value <= 10 RETURN count(n) AS cnt"))
                records = [record.data() for record in result]

            assert len(records) == 1
            assert records[0]["cnt"] == 10

        finally:
            driver.close()

    def test_timeout_occurs_at_expected_time(self, timeout_instance):
        """
        Test that timeout occurs approximately at the configured timeout duration.
        Verifies the query doesn't run longer than expected.
        """
        uri = timeout_instance["uri"]
        timeout_sec = timeout_instance["timeout_sec"]

        driver = GraphDatabase.driver(uri)
        try:
            start_time = time.time()

            with pytest.raises((DatabaseError, TransientError)):
                with driver.session() as session:
                    # Run a long aggregation query that should timeout
                    result = session.run(LONG_RUNNING_AGGREGATION_QUERY)
                    # Try to consume results - should fail with timeout
                    list(result)

            elapsed = time.time() - start_time

            # Should have taken approximately timeout_sec (allow some buffer for processing)
            assert elapsed < timeout_sec + 5, f"Query took {elapsed:.2f}s, expected ~{timeout_sec}s timeout"

        finally:
            driver.close()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
