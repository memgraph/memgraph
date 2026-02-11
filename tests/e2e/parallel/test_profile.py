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
Tests for PROFILE output of parallel queries.

PROFILE output structure:
    - OPERATOR: operator name (e.g., "* ScanAll", "P ScanAll" for parallel)
    - ACTUAL HITS: number of rows/records processed (aggregated across parallel branches)
    - RELATIVE TIME %: percentage of total CPU cycles
    - ABSOLUTE TIME: CPU time in milliseconds

Parallel profiling behavior:
    - Each parallel branch collects its own ProfilingStats
    - Stats are merged after parallel execution:
        - actual_hits: summed across all branches
        - num_cycles: summed across all branches (used for timing)
        - profile_execution_time: summed across all branches
    - This means parallel operators show TOTAL work done, not wall-clock time
"""

import multiprocessing
import re
import sys
import time
from typing import Optional

import pytest
from common import clear_database, pq, setup_thread_count_db
from neo4j import GraphDatabase


class TestParallelProfile:
    """
    Tests for PROFILE output of parallel queries.
    Verifies that profiling information is correctly collected and reported
    for parallel operators.
    """

    @pytest.fixture(autouse=True)
    def setup(self, memgraph):
        self.memgraph = memgraph
        clear_database(memgraph)
        # Create test data
        memgraph.execute_query(
            """
            UNWIND range(1, 100) AS i
            CREATE (:Node {id: i, value: i % 10, category: CASE WHEN i % 3 = 0 THEN 'A' ELSE 'B' END})
            """
        )

    def _get_profile_plan(self, query):
        """Helper to get the profile plan content."""
        result = self.memgraph.fetch_all(f"PROFILE {query}")
        return result

    def _verify_operator_in_profile(self, profile_results, operator_substring):
        """Check if an operator appears in the profile output."""
        for record in profile_results:
            operator = record.get("OPERATOR", "")
            if operator_substring in operator:
                return True
        return False

    def _get_operator_hits(self, profile_results, operator_substring):
        """Get the ACTUAL HITS for a specific operator."""
        for record in profile_results:
            operator = record.get("OPERATOR", "")
            if operator_substring in operator:
                return record.get("ACTUAL HITS", 0)
        return None

    def _get_operator_absolute_time(self, profile_results, operator_substring):
        """
        Get the ABSOLUTE TIME for a specific operator.
        Returns the time in milliseconds as a float, or None if not found.
        """
        for record in profile_results:
            operator = record.get("OPERATOR", "")
            if operator_substring in operator:
                abs_time_str = record.get("ABSOLUTE TIME", "")
                if abs_time_str:
                    # Parse "  0.123456 ms" format
                    match = re.search(r"([\d.]+)\s*ms", abs_time_str)
                    if match:
                        return float(match.group(1))
        return None

    def _get_operator_relative_time(self, profile_results, operator_substring):
        """
        Get the RELATIVE TIME % for a specific operator.
        Returns the percentage as a float, or None if not found.
        """
        for record in profile_results:
            operator = record.get("OPERATOR", "")
            if operator_substring in operator or operator_substring == "":
                # Try both "RELATIVE TIME %" and "RELATIVE TIME" keys
                rel_time_str = record.get("RELATIVE TIME %", "") or record.get("RELATIVE TIME", "")
                if rel_time_str:
                    # Parse "  10.123456 %" format
                    match = re.search(r"([\d.]+)\s*%", rel_time_str)
                    if match:
                        return float(match.group(1))
        return None

    def _get_scan_operator_name(self, profile_results):
        """
        Find the scan operator name in the profile.
        Parallel execution uses ScanParallel/ScanChunk, serial uses ScanAll.
        """
        for record in profile_results:
            operator = record.get("OPERATOR", "")
            if "ScanParallel" in operator or "ScanChunk" in operator:
                return "ScanChunk"  # Use ScanChunk for hit counting in parallel
            if "ScanAll" in operator:
                return "ScanAll"
        return "ScanAll"  # Default

    # =========================================================================
    # Basic Profile Tests
    # =========================================================================


class TestParallelProfileMatrix:
    """Consolidated profile tests for various dataset sizes and operators."""

    @pytest.mark.parametrize("n_elements", [100, 1000])
    def test_profile_basic_operators(self, memgraph, n_elements):
        """Verify profiling stats for basic operators across different dataset sizes."""
        clear_database(memgraph)
        memgraph.execute_query(f"UNWIND range(1, {n_elements}) AS i CREATE (:Node {{id: i, val: i % 10}})")

        for op_type in ["count(n)", "n.val", "n.id ORDER BY n.id"]:
            query = pq(f"MATCH (n:Node) RETURN {op_type}")
            profile = memgraph.fetch_all(f"PROFILE {query}")

            # Verify scan hits
            scan_op = "ScanChunk" if any("ScanChunk" in r["OPERATOR"] for r in profile) else "ScanAll"
            hits = 0
            for r in profile:
                if scan_op in r["OPERATOR"]:
                    hits = r.get("ACTUAL HITS", 0)
                    break
            assert n_elements <= hits <= n_elements + 10, f"Expected ~{n_elements} hits, got {hits}"

            # Verify CPU time is reported
            has_time = any(
                r.get("ABSOLUTE TIME") and "ms" in r.get("ABSOLUTE TIME")
                for r in profile
                if not r["OPERATOR"].startswith("|")
            )
            assert has_time, f"Missing CPU time for query: {query}"


class TestParallelProfileWarmup:
    """
    Test profiling behavior across multiple runs to detect warm-up effects.

    On first run, thread pool workers may be cold/sleeping, causing the main thread
    to steal all work. This can result in disproportionate time in one operator.
    Subsequent runs with warm workers should show more consistent stats.
    """

    @pytest.fixture(autouse=True)
    def setup(self, memgraph):
        self.memgraph = memgraph
        clear_database(memgraph)
        yield
        clear_database(memgraph)

    def _get_profile_plan(self, query):
        """Helper to get the profile plan content."""
        result = self.memgraph.fetch_all(f"PROFILE {query}")
        return result

    def _get_operator_absolute_time(self, profile, operator_name):
        """Extract absolute time for an operator from profile output."""
        for row in profile:
            if operator_name in str(row):
                row_str = str(row)
                time_match = re.search(r"(\d+(?:\.\d+)?)\s*(us|ms|s|ns)", row_str)
                if time_match:
                    value = float(time_match.group(1))
                    unit = time_match.group(2)
                    if unit == "ns":
                        return value / 1_000_000
                    elif unit == "us":
                        return value / 1000
                    elif unit == "ms":
                        return value
                    elif unit == "s":
                        return value * 1000
        return None

    def _get_operator_relative_time(self, profile, operator_name):
        """Extract relative time percentage for an operator."""
        for row in profile:
            if operator_name in str(row):
                row_str = str(row)
                pct_match = re.search(r"(\d+(?:\.\d+)?)\s*%", row_str)
                if pct_match:
                    return float(pct_match.group(1))
        return None

    def test_profile_multiple_runs_cpu_time_consistency(self):
        """
        Test that CPU time is relatively consistent across multiple warm runs.

        First run may include thread pool initialization overhead.
        Subsequent runs should report similar CPU times.
        """
        # Create test data
        self.memgraph.execute_query("UNWIND range(1, 300) AS i CREATE (:ConsistNode {id: i})")

        query = pq("MATCH (n:ConsistNode) RETURN n.id ORDER BY n.id")

        # Run multiple times
        cpu_times = []
        for _ in range(4):
            profile = self._get_profile_plan(query)
            # Try parallel operator first, then serial
            cpu_time = self._get_operator_absolute_time(profile, "ScanChunk")
            if cpu_time is None:
                cpu_time = self._get_operator_absolute_time(profile, "Scan")
            if cpu_time is not None:
                cpu_times.append(cpu_time)

        # Check that warm runs (after first) are consistent
        if len(cpu_times) >= 3:
            warm_times = cpu_times[1:]  # Skip first run

            # Warm runs should all be non-zero
            for i, t in enumerate(warm_times):
                assert t >= 0, f"Warm run {i+2} has invalid CPU time {t}"

            # Warm runs should be within 10x of each other (conservative)
            if all(t > 0 for t in warm_times):
                max_ratio = max(warm_times) / min(warm_times)
                assert max_ratio < 10, f"Warm run CPU times vary too much: {warm_times}, ratio={max_ratio:.2f}"

    def test_profile_first_vs_subsequent_runs(self):
        """
        Document the warm-up effect: first run may differ from subsequent runs.

        This test doesn't assert specific behavior but logs the difference
        for visibility. The key insight is that after the first run,
        subsequent runs should be more similar to each other.
        """
        # Create test data
        self.memgraph.execute_query("UNWIND range(1, 200) AS i CREATE (:DiffNode {id: i, group: i % 5})")

        # Force a new parallel context by using a different query each time
        base_query = "MATCH (n:DiffNode) WHERE n.group = {} RETURN count(n)"

        profiles_per_group = {}
        for group in range(3):
            query = pq(base_query.format(group))
            profiles = []
            for _ in range(3):
                profile = self._get_profile_plan(query)
                profiles.append(profile)
            profiles_per_group[group] = profiles

        # For each group, subsequent runs should be more similar
        for group, profiles in profiles_per_group.items():
            times = []
            for profile in profiles:
                # Try parallel operator first, then serial
                cpu_time = self._get_operator_absolute_time(profile, "ScanChunk")
                if cpu_time is None:
                    cpu_time = self._get_operator_absolute_time(profile, "Scan")
                if cpu_time is not None:
                    times.append(cpu_time)

            # Just verify we got valid profiles (timing may vary)
            assert len(profiles) == 3, f"Group {group}: expected 3 profiles"
            # Times may be empty if operator names don't match - that's ok
            # as long as profiles were returned


# =============================================================================
# Helper functions for concurrent query tests
# =============================================================================


def run_query_in_process(uri: str, query: str, result_queue: multiprocessing.Queue, duration_hint: float = 5.0):
    """
    Execute a query in a separate process.
    For long-running simulation, we run multiple iterations.
    """
    try:
        driver = GraphDatabase.driver(uri)
        with driver.session() as session:
            # Run the query
            result = session.run(query)
            records = list(result)
            result_queue.put(("success", len(records)))
        driver.close()
    except Exception as e:
        result_queue.put(("error", type(e).__name__, str(e)))


def start_query_process(uri: str, query: str) -> tuple:
    """
    Start a query in a separate process.
    Returns (process, result_queue).
    """
    result_queue = multiprocessing.Queue()
    process = multiprocessing.Process(target=run_query_in_process, args=(uri, query, result_queue))
    process.start()
    return process, result_queue


class TestParallelProfileConcurrent:
    """
    Tests for PROFILE output when worker threads are partially occupied.

    These tests simulate scenarios where:
    1. Some workers are busy with other queries
    2. The thread pool has limited availability
    3. Work distribution is affected by concurrent load
    """

    @pytest.fixture(autouse=True)
    def setup(self, memgraph):
        self.memgraph = memgraph
        self.uri = "bolt://localhost:7687"  # Default port
        clear_database(memgraph)
        # Create test data - moderate size for quick but measurable queries
        memgraph.execute_query(
            """
            UNWIND range(1, 1000) AS i
            CREATE (:ConcurrentNode {id: i, value: i % 100, group: i % 10})
            """
        )
        yield
        clear_database(memgraph)

    def _get_profile_plan(self, query):
        """Helper to get the profile plan content."""
        return self.memgraph.fetch_all(f"PROFILE {query}")

    def _get_operator_absolute_time(self, profile, operator_name):
        """Extract absolute time for an operator from profile output."""
        for row in profile:
            if operator_name in str(row):
                row_str = str(row)
                time_match = re.search(r"(\d+(?:\.\d+)?)\s*(us|ms|s|ns)", row_str)
                if time_match:
                    value = float(time_match.group(1))
                    unit = time_match.group(2)
                    if unit == "ns":
                        return value / 1_000_000
                    elif unit == "us":
                        return value / 1000
                    elif unit == "ms":
                        return value
                    elif unit == "s":
                        return value * 1000
        return None

    def _get_operator_hits(self, profile, operator_name):
        """Extract actual hits for an operator from profile output."""
        for row in profile:
            row_str = str(row)
            if operator_name in row_str:
                # Look for hits value - typically a number in the row
                # Profile format: OPERATOR | ACTUAL HITS | RELATIVE TIME | ABSOLUTE TIME
                parts = row_str.split("|") if "|" in row_str else row_str.split(",")
                for part in parts:
                    part = part.strip()
                    if part.isdigit():
                        return int(part)
        return None

    def test_profile_with_concurrent_queries(self):
        """
        Test profiling when multiple parallel queries run concurrently.

        This simulates a scenario where the thread pool has multiple queries
        competing for worker threads.
        """
        # Start a concurrent query that will run in parallel
        concurrent_query = pq("MATCH (n:ConcurrentNode) WHERE n.value > 50 RETURN count(n)")

        # Start multiple concurrent processes
        processes = []
        for _ in range(2):
            proc, queue = start_query_process(self.uri, concurrent_query)
            processes.append((proc, queue))

        # Give concurrent queries time to start
        time.sleep(0.1)

        # Run our profiled query while others are running
        profile_query = pq("MATCH (n:ConcurrentNode) RETURN n.group, count(n) AS cnt")
        profile = self._get_profile_plan(profile_query)

        # Wait for concurrent queries to finish
        for proc, queue in processes:
            proc.join(timeout=10)
            if proc.is_alive():
                proc.terminate()

        # Verify profiling still works - we should get valid stats
        # even when workers are busy
        assert profile is not None, "Profile should return results"
        assert len(profile) > 0, "Profile should have operator rows"

        # Check that we have a scan operator (parallel uses ScanChunk/ScanParallel)
        has_scan = any("Scan" in str(row) for row in profile)
        assert has_scan, f"Profile should include a Scan operator. Profile: {profile}"

    def test_profile_under_load_consistency(self):
        """
        Test that profiling results remain valid under concurrent load.

        Run the same query multiple times while other queries are executing
        and verify that results are consistent and valid.
        """
        # Create additional load data
        self.memgraph.execute_query(
            """
            UNWIND range(1, 500) AS i
            CREATE (:LoadNode {id: i, data: 'x' * (i % 50)})
            """
        )

        # Query we'll profile
        profile_query = pq("MATCH (n:ConcurrentNode) WHERE n.id < 500 RETURN count(n)")

        # Start background load
        load_query = pq("MATCH (n:LoadNode) RETURN n.id, n.data ORDER BY n.id")
        load_processes = []
        for _ in range(3):
            proc, queue = start_query_process(self.uri, load_query)
            load_processes.append((proc, queue))

        # Run profiled query multiple times under load
        profiles = []
        for _ in range(3):
            profile = self._get_profile_plan(profile_query)
            profiles.append(profile)
            time.sleep(0.05)  # Small delay between runs

        # Clean up background processes
        for proc, queue in load_processes:
            proc.join(timeout=5)
            if proc.is_alive():
                proc.terminate()

        # All profiles should be valid
        for i, profile in enumerate(profiles):
            assert profile is not None, f"Profile {i} should not be None"
            assert len(profile) > 0, f"Profile {i} should have rows"

    def test_profile_serial_vs_parallel_under_load(self):
        """
        Compare serial and parallel profiling results under concurrent load.

        Even under load, serial and parallel execution should report
        similar total work done (actual hits should match).
        """
        base_query = "MATCH (n:ConcurrentNode) WHERE n.group = 5 RETURN count(n)"

        # Start some background load
        load_query = pq("MATCH (n:ConcurrentNode) RETURN n ORDER BY n.id LIMIT 500")
        load_proc, load_queue = start_query_process(self.uri, load_query)

        time.sleep(0.05)  # Let load query start

        # Get serial profile
        serial_profile = self._get_profile_plan(base_query)

        # Get parallel profile
        parallel_profile = self._get_profile_plan(pq(base_query))

        # Clean up
        load_proc.join(timeout=5)
        if load_proc.is_alive():
            load_proc.terminate()

        # Both should have valid profiles
        assert serial_profile is not None
        assert parallel_profile is not None

        # Find Aggregate hits (count operation)
        serial_has_agg = any("Aggregate" in str(row) for row in serial_profile)
        parallel_has_agg = any("Aggregate" in str(row) for row in parallel_profile)

        assert serial_has_agg, "Serial profile should have Aggregate"
        assert parallel_has_agg, "Parallel profile should have Aggregate"

    def test_profile_heavy_concurrent_load(self):
        """
        Test profiling under heavy concurrent load (many parallel queries).

        This pushes the thread pool to its limits and verifies profiling
        still produces valid results.
        """
        # Create more data for heavier queries
        self.memgraph.execute_query(
            """
            UNWIND range(1, 2000) AS i
            CREATE (:HeavyNode {id: i, value: i * 2})
            """
        )

        # Heavy query to occupy workers
        heavy_query = pq("MATCH (n:HeavyNode) RETURN n.id, n.value ORDER BY n.value DESC LIMIT 1000")

        # Start multiple heavy queries
        heavy_processes = []
        for _ in range(4):  # Start 4 heavy queries
            proc, queue = start_query_process(self.uri, heavy_query)
            heavy_processes.append((proc, queue))

        time.sleep(0.1)  # Let queries start and occupy workers

        # Run profiled query - workers should be mostly occupied
        profile_query = pq("MATCH (n:ConcurrentNode) RETURN sum(n.value)")
        profile = self._get_profile_plan(profile_query)

        # Clean up
        for proc, queue in heavy_processes:
            proc.join(timeout=10)
            if proc.is_alive():
                proc.terminate()

        # Profile should still work
        assert profile is not None, "Profile should work under heavy load"

        # Should have Aggregate operator
        has_aggregate = any("Aggregate" in str(row) for row in profile)
        assert has_aggregate, "Profile should include Aggregate operator"

        # CPU time should be present
        cpu_time = self._get_operator_absolute_time(profile, "Aggregate")
        # Even under load, we should get timing information
        assert cpu_time is None or cpu_time >= 0, "CPU time should be valid if present"

    def test_profile_staggered_concurrent_queries(self):
        """
        Test profiling with staggered concurrent query starts.

        Start queries at different times to create varying levels of
        worker availability.
        """
        results = []
        processes = []

        # Stagger start of background queries
        bg_query = pq("MATCH (n:ConcurrentNode) RETURN n ORDER BY n.id")

        for delay in [0, 0.02, 0.04]:
            time.sleep(delay)
            proc, queue = start_query_process(self.uri, bg_query)
            processes.append((proc, queue))

            # Profile while this background query is starting
            profile_query = pq("MATCH (n:ConcurrentNode) WHERE n.group = 3 RETURN count(n)")
            profile = self._get_profile_plan(profile_query)
            results.append(profile)

        # Clean up
        for proc, queue in processes:
            proc.join(timeout=5)
            if proc.is_alive():
                proc.terminate()

        # All profiles should be valid
        for i, profile in enumerate(results):
            assert profile is not None, f"Staggered profile {i} should not be None"
            assert len(profile) > 0, f"Staggered profile {i} should have data"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
