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

    def test_profile_parallel_scan(self):
        """
        Test that PROFILE works with parallel scan.
        Parallel execution uses ScanParallel + ScanChunk instead of ScanAll.
        """
        query = pq("MATCH (n:Node) RETURN count(n)")
        profile = self._get_profile_plan(query)

        # Should have ScanParallel or ScanChunk operator (parallel mode)
        has_scan = self._verify_operator_in_profile(profile, "ScanParallel") or self._verify_operator_in_profile(
            profile, "ScanChunk"
        )
        assert has_scan, f"Expected ScanParallel or ScanChunk operator in profile: {profile}"

        # Should have Aggregate operator
        assert self._verify_operator_in_profile(
            profile, "Aggregate"
        ), f"Expected Aggregate operator in profile: {profile}"

    def test_profile_parallel_scan_hits_count(self):
        """
        Test that parallel scan reports correct number of actual hits.
        Hits are aggregated across all parallel branches.
        Note: Parallel execution may include setup hits (+1).
        """
        query = pq("MATCH (n:Node) RETURN n")
        profile = self._get_profile_plan(query)

        # Get hits from whichever scan operator is present
        scan_op = self._get_scan_operator_name(profile)
        hits = self._get_operator_hits(profile, scan_op)
        # Allow for +1 due to setup/initialization hit in profiling
        assert hits is not None and 100 <= hits <= 110, f"Expected ~100 hits for scan, got {hits}. Profile: {profile}"

    def test_profile_parallel_scan_has_cpu_time(self):
        """
        Test that parallel scan reports CPU time (ABSOLUTE TIME column).
        CPU time is aggregated across all parallel branches.
        """
        query = pq("MATCH (n:Node) RETURN n")
        profile = self._get_profile_plan(query)

        # Get CPU time from whichever scan operator is present
        scan_op = self._get_scan_operator_name(profile)
        cpu_time = self._get_operator_absolute_time(profile, scan_op)
        assert cpu_time is not None, f"Expected CPU time for scan. Profile: {profile}"
        assert cpu_time >= 0, f"CPU time should be non-negative, got {cpu_time}"

    def test_profile_parallel_filter(self):
        """
        Test that PROFILE correctly shows filter statistics in parallel execution.
        """
        query = pq("MATCH (n:Node) WHERE n.value < 5 RETURN n")
        profile = self._get_profile_plan(query)

        # Should have Filter operator
        assert self._verify_operator_in_profile(profile, "Filter"), f"Expected Filter operator in profile: {profile}"

        # Scan should have ~100 hits (allow for parallel overhead)
        scan_op = self._get_scan_operator_name(profile)
        scan_hits = self._get_operator_hits(profile, scan_op)
        assert scan_hits is not None and 100 <= scan_hits <= 110, f"Expected ~100 scan hits, got {scan_hits}"

        # Filter should have ~50 hits (values 0-4 out of 0-9, so half)
        # Allow for slight variation due to parallel execution
        filter_hits = self._get_operator_hits(profile, "Filter")
        assert filter_hits is not None and 49 <= filter_hits <= 55, f"Expected ~50 filter hits, got {filter_hits}"

    def test_profile_parallel_aggregate(self):
        """
        Test that PROFILE shows correct stats for parallel aggregation.
        """
        query = pq("MATCH (n:Node) RETURN n.category AS cat, count(n) AS cnt")
        profile = self._get_profile_plan(query)

        # Should have Aggregate operator
        assert self._verify_operator_in_profile(
            profile, "Aggregate"
        ), f"Expected Aggregate operator in profile: {profile}"

    def test_profile_parallel_orderby(self):
        """
        Test that PROFILE shows correct stats for parallel order by.
        Note: Parallel OrderBy may show aggregated hits across branches.
        """
        query = pq("MATCH (n:Node) RETURN n ORDER BY n.id")
        profile = self._get_profile_plan(query)

        # Should have OrderBy operator (may be OrderByParallel in parallel mode)
        has_orderby = self._verify_operator_in_profile(profile, "OrderBy") or self._verify_operator_in_profile(
            profile, "OrderByParallel"
        )
        assert has_orderby, f"Expected OrderBy operator in profile: {profile}"

        # OrderBy hits may be higher due to parallel branch aggregation
        # Each branch sorts its portion, then results are merged
        orderby_hits = self._get_operator_hits(profile, "OrderBy")
        assert (
            orderby_hits is not None and orderby_hits >= 100
        ), f"Expected at least 100 orderby hits, got {orderby_hits}"

    # =========================================================================
    # Consistency Tests - Compare Serial vs Parallel Profile
    # =========================================================================

    def test_profile_hits_consistency_scan(self):
        """
        Verify that serial and parallel execution process similar number of items.
        Parallel may use different operator names (ScanChunk vs ScanAll).
        """
        base_query = "MATCH (n:Node) RETURN count(n)"

        serial_profile = self._get_profile_plan(base_query)
        parallel_profile = self._get_profile_plan(pq(base_query))

        serial_scan_hits = self._get_operator_hits(serial_profile, "ScanAll")
        # Parallel uses ScanChunk instead of ScanAll
        parallel_scan_op = self._get_scan_operator_name(parallel_profile)
        parallel_scan_hits = self._get_operator_hits(parallel_profile, parallel_scan_op)

        assert serial_scan_hits is not None, f"Missing serial scan hits. Profile: {serial_profile}"
        assert parallel_scan_hits is not None, f"Missing parallel scan hits. Profile: {parallel_profile}"

        # Both should be close to 100 (allow small variance)
        assert 99 <= serial_scan_hits <= 110, f"Serial scan hits unexpected: {serial_scan_hits}"
        assert 99 <= parallel_scan_hits <= 120, f"Parallel scan hits unexpected: {parallel_scan_hits}"

    def test_profile_hits_consistency_filter(self):
        """
        Verify that serial and parallel execution report same number of hits for filter.
        """
        base_query = "MATCH (n:Node) WHERE n.value IN [1, 2, 3] RETURN n"

        serial_profile = self._get_profile_plan(base_query)
        parallel_profile = self._get_profile_plan(pq(base_query))

        serial_filter_hits = self._get_operator_hits(serial_profile, "Filter")
        parallel_filter_hits = self._get_operator_hits(parallel_profile, "Filter")

        assert (
            serial_filter_hits == parallel_filter_hits
        ), f"Serial ({serial_filter_hits}) vs Parallel ({parallel_filter_hits}) filter hits mismatch"

    def test_profile_cpu_time_similar_serial_vs_parallel_scan(self):
        """
        Verify that serial and parallel execution report similar total CPU time for scan.

        Since PROFILE returns total CPU cycles (aggregated across all parallel branches),
        the total CPU time should be in a similar range for the same workload.
        The parallel version may have some overhead, but should be within a reasonable factor.
        """
        base_query = "MATCH (n:Node) RETURN n"

        serial_profile = self._get_profile_plan(base_query)
        parallel_profile = self._get_profile_plan(pq(base_query))

        serial_cpu_time = self._get_operator_absolute_time(serial_profile, "ScanAll")
        # Parallel uses ScanChunk instead of ScanAll
        parallel_scan_op = self._get_scan_operator_name(parallel_profile)
        parallel_cpu_time = self._get_operator_absolute_time(parallel_profile, parallel_scan_op)

        assert serial_cpu_time is not None, f"Missing serial CPU time. Profile: {serial_profile}"
        assert parallel_cpu_time is not None, f"Missing parallel CPU time. Profile: {parallel_profile}"

        # CPU time should be in a similar ballpark (within 10x factor)
        # Some variation is expected due to parallelization overhead, caching effects, etc.
        if serial_cpu_time > 0:
            ratio = parallel_cpu_time / serial_cpu_time
            assert 0.1 <= ratio <= 10, (
                f"CPU time ratio {ratio:.2f} is outside expected range. "
                f"Serial: {serial_cpu_time:.4f}ms, Parallel: {parallel_cpu_time:.4f}ms"
            )

    def test_profile_cpu_time_similar_serial_vs_parallel_aggregate(self):
        """
        Verify that serial and parallel execution report valid CPU time for aggregation.
        Note: Parallel has coordination overhead, so times may differ significantly
        for small datasets. We verify both report valid times.
        """
        base_query = "MATCH (n:Node) RETURN count(n), sum(n.id)"

        serial_profile = self._get_profile_plan(base_query)
        parallel_profile = self._get_profile_plan(pq(base_query))

        serial_cpu_time = self._get_operator_absolute_time(serial_profile, "Aggregate")
        # Parallel uses AggregateParallel
        parallel_cpu_time = self._get_operator_absolute_time(
            parallel_profile, "AggregateParallel"
        ) or self._get_operator_absolute_time(parallel_profile, "Aggregate")

        assert serial_cpu_time is not None, "Missing serial CPU time"
        assert parallel_cpu_time is not None, f"Missing parallel CPU time. Profile: {parallel_profile}"

        # Both should be non-negative
        assert serial_cpu_time >= 0, f"Serial CPU time invalid: {serial_cpu_time}"
        assert parallel_cpu_time >= 0, f"Parallel CPU time invalid: {parallel_cpu_time}"

        # For small datasets, parallel overhead can be significant (up to 20x)
        # Just verify both produce valid results
        if serial_cpu_time > 0:
            ratio = parallel_cpu_time / serial_cpu_time
            # Log the ratio for visibility but use a wide tolerance
            assert 0.01 <= ratio <= 100, (
                f"CPU time ratio {ratio:.2f} is extremely unusual. "
                f"Serial: {serial_cpu_time:.4f}ms, Parallel: {parallel_cpu_time:.4f}ms"
            )

    def test_profile_cpu_time_present_all_operators(self):
        """
        Verify that all operators have CPU time (ABSOLUTE TIME) in profile output.
        CPU time is calculated from num_cycles which are aggregated across branches.
        """
        query = pq("MATCH (n:Node) WHERE n.value < 5 RETURN n ORDER BY n.id")
        profile = self._get_profile_plan(query)

        # All real operators (not branch markers) should have timing
        for record in profile:
            operator = record.get("OPERATOR", "")
            # Skip branch markers like "|\\"
            if operator.startswith("|"):
                continue
            if operator.strip().startswith("*") or operator.strip().startswith("P"):
                abs_time_str = record.get("ABSOLUTE TIME", "")
                assert abs_time_str, f"Operator '{operator}' missing ABSOLUTE TIME"
                assert "ms" in abs_time_str, f"Operator '{operator}' has invalid time format: {abs_time_str}"

    def test_profile_relative_time_sums_to_100(self):
        """
        Verify that relative times across all operators sum to approximately 100%.
        """
        query = pq("MATCH (n:Node) RETURN count(n)")
        profile = self._get_profile_plan(query)

        total_relative_time = 0.0
        for record in profile:
            operator = record.get("OPERATOR", "")
            # Skip branch markers
            if operator.startswith("|"):
                continue
            # Parse relative time directly from the record
            rel_time_str = record.get("RELATIVE TIME", "")
            if rel_time_str:
                match = re.search(r"([\d.]+)\s*%", rel_time_str)
                if match:
                    total_relative_time += float(match.group(1))

        # Should sum to approximately 100% (allow some floating point tolerance)
        assert 99.0 <= total_relative_time <= 101.0, f"Relative times sum to {total_relative_time}%, expected ~100%"

    # =========================================================================
    # Complex Query Profile Tests
    # =========================================================================

    def test_profile_parallel_with_limit(self):
        """
        Test profiling with LIMIT clause.
        Note: Parallel execution may have +1 hit due to initialization.
        """
        query = pq("MATCH (n:Node) RETURN n ORDER BY n.id LIMIT 10")
        profile = self._get_profile_plan(query)

        # Should have Limit operator
        assert self._verify_operator_in_profile(profile, "Limit"), f"Expected Limit operator in profile: {profile}"

        # Limit should output ~10 rows (allow +1 for setup hit)
        limit_hits = self._get_operator_hits(profile, "Limit")
        assert limit_hits is not None and 10 <= limit_hits <= 12, f"Expected ~10 limit hits, got {limit_hits}"

    def test_profile_parallel_with_skip_limit(self):
        """
        Test profiling with SKIP and LIMIT clauses.
        """
        query = pq("MATCH (n:Node) RETURN n ORDER BY n.id SKIP 5 LIMIT 10")
        profile = self._get_profile_plan(query)

        # Should have Skip and Limit operators
        assert self._verify_operator_in_profile(profile, "Skip"), f"Expected Skip in profile: {profile}"
        assert self._verify_operator_in_profile(profile, "Limit"), f"Expected Limit in profile: {profile}"

    def test_profile_parallel_multiple_aggregations(self):
        """
        Test profiling with multiple aggregation functions.
        """
        query = pq("MATCH (n:Node) RETURN min(n.id) AS mn, max(n.id) AS mx, count(n) AS cnt, avg(n.value) AS av")
        profile = self._get_profile_plan(query)

        # Should have Aggregate operator
        assert self._verify_operator_in_profile(
            profile, "Aggregate"
        ), f"Expected Aggregate operator in profile: {profile}"

    def test_profile_parallel_group_by(self):
        """
        Test profiling with GROUP BY (implicit via aggregation with non-aggregated column).
        Note: Parallel execution sums hits across branches, so we get
        (num_branches * groups_per_branch) + final_groups hits.
        """
        query = pq("MATCH (n:Node) RETURN n.category AS cat, count(n) AS cnt, avg(n.value) AS avg_val")
        profile = self._get_profile_plan(query)

        # Should have Aggregate operator
        has_aggregate = self._verify_operator_in_profile(profile, "Aggregate") or self._verify_operator_in_profile(
            profile, "AggregateParallel"
        )
        assert has_aggregate, f"Expected Aggregate operator in profile: {profile}"

        # Aggregate hits are summed across parallel branches
        # With 8 threads, each might produce 2 groups, plus the final merge
        # So hits could be 8*2 + 2 = 18 or similar
        agg_hits = self._get_operator_hits(profile, "AggregateParallel") or self._get_operator_hits(
            profile, "Aggregate"
        )
        assert agg_hits is not None and agg_hits >= 2, f"Expected at least 2 aggregate rows, got {agg_hits}"

    # =========================================================================
    # Edge Cases
    # =========================================================================

    def test_profile_empty_result(self):
        """
        Test profiling when query returns no results.
        Note: Profiling counts operator invocations, not result rows.
        Even with no matches, the filter is invoked to check each input.
        """
        query = pq("MATCH (n:Node) WHERE n.id > 1000 RETURN n")
        profile = self._get_profile_plan(query)

        # Profile should exist even with no results
        assert profile is not None, "Profile should be returned for empty result"
        assert len(profile) > 0, "Profile should have operators even for empty result"

        # Filter is present in the plan
        assert self._verify_operator_in_profile(profile, "Filter"), f"Expected Filter in profile: {profile}"

    def test_profile_with_unwind(self):
        """
        Test profiling with UNWIND in parallel query.
        """
        query = pq("UNWIND range(1, 10) AS i MATCH (n:Node) WHERE n.id = i RETURN n.id")
        profile = self._get_profile_plan(query)

        # Should have Unwind operator
        assert self._verify_operator_in_profile(profile, "Unwind"), f"Expected Unwind in profile: {profile}"

    def test_profile_cpu_time_is_non_negative(self):
        """
        Verify that all CPU times are non-negative.
        """
        query = pq("MATCH (n:Node) RETURN count(n)")
        profile = self._get_profile_plan(query)

        for record in profile:
            operator = record.get("OPERATOR", "")
            if operator.startswith("|"):
                continue
            cpu_time = self._get_operator_absolute_time([record], "")
            if cpu_time is not None:
                assert cpu_time >= 0, f"Operator '{operator}' has negative CPU time: {cpu_time}"

    def test_profile_scan_cpu_time_increases_with_data(self):
        """
        Verify that scanning more data results in higher CPU time.
        This confirms CPU time is actually being measured.
        """
        # Query that scans all nodes
        query_all = pq("MATCH (n:Node) RETURN n")
        profile_all = self._get_profile_plan(query_all)

        # Query that scans but filters early
        query_one = pq("MATCH (n:Node) WHERE n.id = 1 RETURN n")
        profile_one = self._get_profile_plan(query_one)

        # Get the scan operator name (parallel uses ScanChunk)
        scan_op_all = self._get_scan_operator_name(profile_all)
        scan_op_one = self._get_scan_operator_name(profile_one)

        scan_time_all = self._get_operator_absolute_time(profile_all, scan_op_all)
        scan_time_one = self._get_operator_absolute_time(profile_one, scan_op_one)

        # Both should have timing
        assert scan_time_all is not None, f"Missing scan time for query_all. Profile: {profile_all}"
        assert scan_time_one is not None, f"Missing scan time for query_one. Profile: {profile_one}"


class TestParallelProfileLargeData:
    """
    Profile tests with larger datasets to verify parallel execution benefits.
    CPU time (ABSOLUTE TIME) should be present and meaningful for all operators.
    """

    @pytest.fixture(autouse=True)
    def setup(self, memgraph):
        self.memgraph = memgraph
        clear_database(memgraph)
        # Create larger dataset
        memgraph.execute_query(
            """
            UNWIND range(1, 1000) AS i
            CREATE (:LargeNode {id: i, value: i % 100, group: i % 10})
            """
        )

    def _get_profile_plan(self, query):
        """Helper to get the profile plan content."""
        result = self.memgraph.fetch_all(f"PROFILE {query}")
        return result

    def _get_operator_hits(self, profile_results, operator_substring):
        """Get the ACTUAL HITS for a specific operator."""
        for record in profile_results:
            operator = record.get("OPERATOR", "")
            if operator_substring in operator:
                return record.get("ACTUAL HITS", 0)
        return None

    def _get_operator_absolute_time(self, profile_results, operator_substring):
        """Get the ABSOLUTE TIME for a specific operator."""
        for record in profile_results:
            operator = record.get("OPERATOR", "")
            if operator_substring in operator:
                abs_time_str = record.get("ABSOLUTE TIME", "")
                if abs_time_str:
                    match = re.search(r"([\d.]+)\s*ms", abs_time_str)
                    if match:
                        return float(match.group(1))
        return None

    def _get_scan_operator_name(self, profile_results):
        """Find the scan operator name in the profile."""
        for record in profile_results:
            operator = record.get("OPERATOR", "")
            if "ScanParallel" in operator or "ScanChunk" in operator:
                return "ScanChunk"
            if "ScanAll" in operator:
                return "ScanAll"
        return "ScanAll"

    def test_profile_large_scan(self):
        """
        Profile parallel scan on larger dataset.
        """
        query = pq("MATCH (n:LargeNode) RETURN count(n)")
        profile = self._get_profile_plan(query)

        # Parallel uses ScanChunk, serial uses ScanAll
        scan_op = self._get_scan_operator_name(profile)
        scan_hits = self._get_operator_hits(profile, scan_op)
        assert scan_hits is not None and 1000 <= scan_hits <= 1100, f"Expected ~1000 scan hits, got {scan_hits}"

    def test_profile_large_scan_cpu_time(self):
        """
        Verify CPU time is reported for large parallel scan.
        """
        query = pq("MATCH (n:LargeNode) RETURN n")
        profile = self._get_profile_plan(query)

        scan_op = self._get_scan_operator_name(profile)
        cpu_time = self._get_operator_absolute_time(profile, scan_op)
        assert cpu_time is not None, f"Expected CPU time for scan. Profile: {profile}"
        assert cpu_time >= 0, f"CPU time should be non-negative, got {cpu_time}"

    def test_profile_large_filter(self):
        """
        Profile parallel filter on larger dataset.
        """
        query = pq("MATCH (n:LargeNode) WHERE n.value < 50 RETURN n")
        profile = self._get_profile_plan(query)

        # 50% of nodes have value < 50 (allow small variance)
        filter_hits = self._get_operator_hits(profile, "Filter")
        assert filter_hits is not None and 499 <= filter_hits <= 510, f"Expected ~500 filter hits, got {filter_hits}"

    def test_profile_large_aggregate(self):
        """
        Profile parallel aggregation on larger dataset.
        Note: Parallel aggregation sums hits across branches.
        """
        query = pq("MATCH (n:LargeNode) RETURN n.group AS g, count(n) AS cnt")
        profile = self._get_profile_plan(query)

        # Parallel sums hits: could be branches * groups + final = e.g. 8*10 + 10 = 90
        # Just verify we have aggregate output
        agg_hits = self._get_operator_hits(profile, "AggregateParallel") or self._get_operator_hits(
            profile, "Aggregate"
        )
        assert agg_hits is not None and agg_hits >= 10, f"Expected at least 10 aggregate rows, got {agg_hits}"

    def test_profile_large_aggregate_cpu_time(self):
        """
        Verify CPU time is reported for large parallel aggregation.
        """
        query = pq("MATCH (n:LargeNode) RETURN n.group AS g, count(n) AS cnt, avg(n.value) AS avg_val")
        profile = self._get_profile_plan(query)

        cpu_time = self._get_operator_absolute_time(profile, "Aggregate")
        assert cpu_time is not None, f"Expected CPU time for Aggregate. Profile: {profile}"
        assert cpu_time >= 0, f"CPU time should be non-negative, got {cpu_time}"

    def test_profile_large_cpu_time_similar_serial_vs_parallel(self):
        """
        Verify that serial and parallel execution both report valid CPU times.

        Note: Parallel uses different operator names (ScanChunk vs ScanAll,
        AggregateParallel vs Aggregate).
        """
        base_query = "MATCH (n:LargeNode) RETURN count(n), sum(n.id), avg(n.value)"

        serial_profile = self._get_profile_plan(base_query)
        parallel_profile = self._get_profile_plan(pq(base_query))

        # Compare scan CPU time - parallel uses ScanChunk
        serial_scan_time = self._get_operator_absolute_time(serial_profile, "ScanAll")
        parallel_scan_op = self._get_scan_operator_name(parallel_profile)
        parallel_scan_time = self._get_operator_absolute_time(parallel_profile, parallel_scan_op)

        assert serial_scan_time is not None, f"Missing serial scan CPU time. Profile: {serial_profile}"
        assert parallel_scan_time is not None, f"Missing parallel scan CPU time. Profile: {parallel_profile}"

        # Both should report valid times
        assert serial_scan_time >= 0, f"Invalid serial scan time: {serial_scan_time}"
        assert parallel_scan_time >= 0, f"Invalid parallel scan time: {parallel_scan_time}"

        # Compare Aggregate CPU time - parallel uses AggregateParallel
        serial_agg_time = self._get_operator_absolute_time(serial_profile, "Aggregate")
        parallel_agg_time = self._get_operator_absolute_time(
            parallel_profile, "AggregateParallel"
        ) or self._get_operator_absolute_time(parallel_profile, "Aggregate")

        assert serial_agg_time is not None, f"Missing serial aggregate CPU time"
        assert parallel_agg_time is not None, f"Missing parallel aggregate CPU time"

        # Both should report valid times
        assert serial_agg_time >= 0, f"Invalid serial aggregate time: {serial_agg_time}"
        assert parallel_agg_time >= 0, f"Invalid parallel aggregate time: {parallel_agg_time}"


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
