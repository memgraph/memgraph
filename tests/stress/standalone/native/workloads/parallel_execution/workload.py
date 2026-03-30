#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
Parallel Execution Stress Test

This stress test validates the USING PARALLEL EXECUTION feature under load:
1. Correctness: Parallel results must match serial results
2. Concurrency: Multiple workers running parallel queries simultaneously
3. Stability: No crashes or hangs under sustained load
4. Mixed workloads: Readers and writers operating concurrently
"""

import argparse
import math
import multiprocessing
import random
import sys
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from neo4j import GraphDatabase

# =============================================================================
# Configuration
# =============================================================================

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 7687
DEFAULT_WORKER_COUNT = 8
DEFAULT_ITERATIONS = 100
DEFAULT_DURATION_SECONDS = 300  # 5 minutes


class QueryType(Enum):
    """Types of queries to test."""

    SCAN_ALL = "scan_all"
    SCAN_FILTERED = "scan_filtered"
    AGGREGATION_COUNT = "aggregation_count"
    AGGREGATION_SUM = "aggregation_sum"
    AGGREGATION_GROUPED = "aggregation_grouped"
    DISTINCT = "distinct"
    ORDER_BY = "order_by"
    SKIP = "skip"
    LIMIT = "limit"
    SKIP_LIMIT = "skip_limit"
    PATTERN_MATCH = "pattern_match"
    OPTIONAL_MATCH = "optional_match"
    UNWIND = "unwind"
    COLLECT = "collect"
    CARTESIAN_JOIN = "cartesian_join"
    CARTESIAN_JOIN_FILTERED = "cartesian_join_filtered"
    CARTESIAN_JOIN_AGGREGATION = "cartesian_join_aggregation"


@dataclass
class QueryResult:
    """Result of a query execution."""

    query_type: QueryType
    serial_result: Any
    parallel_result: Any
    serial_time_ms: float
    parallel_time_ms: float
    match: bool
    error: Optional[str] = None


@dataclass
class WorkerStats:
    """Statistics for a single worker."""

    worker_id: int
    queries_executed: int
    queries_passed: int
    queries_failed: int
    total_serial_time_ms: float
    total_parallel_time_ms: float
    errors: List[str]


# =============================================================================
# Query Definitions
# =============================================================================

# Queries that can be run with USING PARALLEL EXECUTION
# NOTE: All queries must be DETERMINISTIC - ordering must be fully specified
# to avoid false mismatches between serial and parallel execution.
PARALLEL_QUERIES = {
    QueryType.SCAN_ALL: "MATCH (n:Node) RETURN n.id, n.value",
    QueryType.SCAN_FILTERED: "MATCH (n:Node) WHERE n.value > 50 RETURN n.id, n.value",
    QueryType.AGGREGATION_COUNT: "MATCH (n:Node) RETURN count(n) AS cnt",
    QueryType.AGGREGATION_SUM: "MATCH (n:Node) RETURN sum(n.value) AS total, avg(n.value) AS average",
    QueryType.AGGREGATION_GROUPED: "MATCH (n:Node) RETURN n.category, count(*) AS cnt, sum(n.value) AS total",
    QueryType.DISTINCT: "MATCH (n:Node) RETURN DISTINCT n.category",
    # ORDER BY: Use n.id as tiebreaker to ensure deterministic ordering
    QueryType.ORDER_BY: "MATCH (n:Node) WHERE n.id < 1000 RETURN n.id, n.value ORDER BY n.value DESC, n.id ASC",
    QueryType.SKIP: "MATCH (n:Node) WHERE n.id < 1000 RETURN n.id ORDER BY n.id SKIP 50",
    QueryType.LIMIT: "MATCH (n:Node) RETURN n.id ORDER BY n.id LIMIT 100",
    QueryType.SKIP_LIMIT: "MATCH (n:Node) WHERE n.id < 1000 RETURN n.id ORDER BY n.id SKIP 100 LIMIT 50",
    QueryType.PATTERN_MATCH: "MATCH (a:Node)-[r:NEXT]->(b:Node) RETURN a.id, b.id, r.weight",
    QueryType.OPTIONAL_MATCH: "MATCH (n:Node) WHERE n.id < 100 OPTIONAL MATCH (n)-[r]->(m) RETURN n.id, type(r), m.id",
    QueryType.UNWIND: "UNWIND range(1, 1000) AS x MATCH (n:Node {id: x}) RETURN n.id, n.value",
    # COLLECT: Order nodes before collecting to ensure deterministic list order
    QueryType.COLLECT: "MATCH (n:Node) WHERE n.id < 100 WITH n ORDER BY n.id RETURN n.category, collect(n.id) AS ids",
    # Cartesian product / join queries
    QueryType.CARTESIAN_JOIN: "MATCH (n:Node), (m:Node) WHERE n.id < 100 AND m.id < 100 AND n.category = m.category AND n.id < m.id RETURN n.id, m.id",
    QueryType.CARTESIAN_JOIN_FILTERED: "MATCH (n:Node), (m:Node) WHERE n.id < 50 AND m.id < 50 AND n.value = m.value RETURN n.id, m.id, n.value",
    QueryType.CARTESIAN_JOIN_AGGREGATION: "MATCH (n:Node), (m:Node) WHERE n.id < 100 AND m.id < 100 AND n.category = m.category RETURN n.category, count(*) AS pair_count",
}


def parallel_query(query: str) -> str:
    """Wrap a query with USING PARALLEL EXECUTION hint."""
    return f"USING PARALLEL EXECUTION {query}"


# =============================================================================
# Result Normalization
# =============================================================================


def make_hashable(value: Any) -> Any:
    """Recursively convert dicts/lists to hashable types for comparison."""
    if isinstance(value, dict):
        return tuple(sorted((k, make_hashable(v)) for k, v in value.items()))
    if isinstance(value, list):
        return tuple(make_hashable(v) for v in value)
    return value


def normalize_results(results: List[Dict[str, Any]], sort: bool = True) -> List[Tuple]:
    """Normalize query results for comparison."""
    normalized = []
    for record in results:
        item_list = []
        for k in sorted(record.keys()):
            item_list.append((k, make_hashable(record[k])))
        normalized.append(tuple(item_list))

    if sort:
        # Sort by string representation to handle mixed types
        normalized.sort(key=lambda x: str(x))

    return normalized


# Query types that return floating-point aggregates; parallel execution can sum
# in a different order, so we use tolerant comparison to avoid flaky failures.
_FLOAT_TOLERANT_QUERY_TYPES = frozenset(
    {
        QueryType.AGGREGATION_SUM,
        QueryType.AGGREGATION_GROUPED,
        QueryType.CARTESIAN_JOIN_AGGREGATION,
    }
)


def _values_equal_with_tolerance(a: Any, b: Any, rel_tol: float = 1e-6, abs_tol: float = 1e-6) -> bool:
    """Compare two values; use math.isclose for numeric types to handle FP ordering."""
    if type(a) is not type(b):
        return False
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        if isinstance(a, int) and isinstance(b, int):
            return a == b
        return math.isclose(float(a), float(b), rel_tol=rel_tol, abs_tol=abs_tol)
    if isinstance(a, dict):
        return len(a) == len(b) and all(
            k in b and _values_equal_with_tolerance(v, b[k], rel_tol, abs_tol) for k, v in a.items()
        )
    if isinstance(a, list):
        return len(a) == len(b) and all(_values_equal_with_tolerance(x, y, rel_tol, abs_tol) for x, y in zip(a, b))
    return a == b


def results_match(
    serial: List[Dict],
    parallel: List[Dict],
    order_matters: bool = False,
    float_tolerant: bool = False,
) -> bool:
    """Compare serial and parallel results. If float_tolerant, use isclose for numerics."""
    if len(serial) != len(parallel):
        return False
    norm_serial = normalize_results(serial, sort=not order_matters)
    norm_parallel = normalize_results(parallel, sort=not order_matters)
    if not float_tolerant:
        return norm_serial == norm_parallel
    # Compare row by row with tolerance for numeric fields
    for rs, rp in zip(norm_serial, norm_parallel):
        if len(rs) != len(rp):
            return False
        for (ks, vs), (kp, vp) in zip(rs, rp):
            if ks != kp or not _values_equal_with_tolerance(vs, vp):
                return False
    return True


# =============================================================================
# Query Execution
# =============================================================================


def execute_query(driver, query: str) -> Tuple[List[Dict], float]:
    """Execute a query and return results with timing."""
    start = time.time()
    with driver.session() as session:
        result = session.run(query)
        records = [record.data() for record in result]
    elapsed_ms = (time.time() - start) * 1000
    return records, elapsed_ms


def execute_query_in_tx(tx, query: str) -> Tuple[List[Dict], float]:
    """Execute a query within an existing transaction and return results with timing."""
    start = time.time()
    result = tx.run(query)
    records = [record.data() for record in result]
    elapsed_ms = (time.time() - start) * 1000
    return records, elapsed_ms


def verify_parallel_correctness(driver, query_type: QueryType) -> QueryResult:
    """
    Execute query both serially and in parallel, verify results match.

    IMPORTANT: Both queries run within the SAME TRANSACTION to ensure they
    see the same snapshot of the data. This prevents false mismatches caused
    by concurrent writes between the serial and parallel execution.
    """
    query = PARALLEL_QUERIES[query_type]

    try:
        with driver.session() as session:
            # Start an explicit transaction so both queries see the same snapshot
            with session.begin_transaction() as tx:
                # Serial execution (no parallel hint)
                serial_start = time.time()
                serial_result_raw = tx.run(query)
                serial_result = [record.data() for record in serial_result_raw]
                serial_time = (time.time() - serial_start) * 1000

                # Parallel execution (with USING PARALLEL EXECUTION hint)
                parallel_start = time.time()
                parallel_result_raw = tx.run(parallel_query(query))
                parallel_result = [record.data() for record in parallel_result_raw]
                parallel_time = (time.time() - parallel_start) * 1000

                # Transaction will be automatically rolled back (read-only)

        # Check if results match
        # ORDER BY, SKIP, LIMIT queries need order preserved
        order_matters = query_type in [QueryType.ORDER_BY, QueryType.SKIP, QueryType.LIMIT, QueryType.SKIP_LIMIT]
        float_tolerant = query_type in _FLOAT_TOLERANT_QUERY_TYPES
        match = results_match(
            serial_result, parallel_result, order_matters=order_matters, float_tolerant=float_tolerant
        )

        if match:
            error_msg = None
        elif len(serial_result) != len(parallel_result):
            error_msg = f"Results mismatch: serial={len(serial_result)} rows, parallel={len(parallel_result)} rows"
        else:
            error_msg = (
                f"Result values differ: serial={len(serial_result)} rows, parallel={len(parallel_result)} rows "
                f"(serial={serial_result!r}, parallel={parallel_result!r})"
            )

        return QueryResult(
            query_type=query_type,
            serial_result=serial_result,
            parallel_result=parallel_result,
            serial_time_ms=serial_time,
            parallel_time_ms=parallel_time,
            match=match,
            error=error_msg,
        )
    except Exception as e:
        return QueryResult(
            query_type=query_type,
            serial_result=None,
            parallel_result=None,
            serial_time_ms=0,
            parallel_time_ms=0,
            match=False,
            error=str(e),
        )


# =============================================================================
# Worker Types
# =============================================================================


class WorkerType(Enum):
    """Types of workers for mixed workload testing."""

    PARALLEL_READER = "parallel_reader"  # Runs USING PARALLEL EXECUTION queries
    SERIAL_READER = "serial_reader"  # Runs standard single-threaded read queries
    VERIFICATION_READER = "verification"  # Verifies parallel == serial results
    WRITER = "writer"  # Runs write queries


# =============================================================================
# Worker Functions
# =============================================================================


def parallel_reader_worker(
    worker_id: int, host: str, port: int, iterations: int, duration_seconds: int, result_queue: multiprocessing.Queue
) -> None:
    """Worker that executes ONLY parallel read queries (no verification)."""
    print(f"[Worker {worker_id}] Starting PARALLEL reader worker...")

    driver = GraphDatabase.driver(f"bolt://{host}:{port}", auth=("", ""))

    stats = WorkerStats(
        worker_id=worker_id,
        queries_executed=0,
        queries_passed=0,
        queries_failed=0,
        total_serial_time_ms=0,
        total_parallel_time_ms=0,
        errors=[],
    )

    query_types = list(QueryType)
    start_time = time.time()
    iteration = 0

    try:
        while iteration < iterations and (time.time() - start_time) < duration_seconds:
            query_type = random.choice(query_types)
            query = parallel_query(PARALLEL_QUERIES[query_type])

            try:
                result, elapsed = execute_query(driver, query)
                stats.queries_executed += 1
                stats.queries_passed += 1
                stats.total_parallel_time_ms += elapsed
            except Exception as e:
                stats.queries_executed += 1
                stats.queries_failed += 1
                stats.errors.append(f"[PARALLEL {query_type.value}] {str(e)}")

            iteration += 1
            time.sleep(0.005)  # Small delay

        print(f"[Worker {worker_id}] PARALLEL reader completed {stats.queries_executed} queries")

    except Exception as e:
        stats.errors.append(f"Worker exception: {str(e)}")
        print(f"[Worker {worker_id}] Exception: {e}")

    finally:
        driver.close()
        result_queue.put(stats)


def serial_reader_worker(
    worker_id: int, host: str, port: int, iterations: int, duration_seconds: int, result_queue: multiprocessing.Queue
) -> None:
    """Worker that executes ONLY serial (single-threaded) read queries."""
    print(f"[Worker {worker_id}] Starting SERIAL reader worker...")

    driver = GraphDatabase.driver(f"bolt://{host}:{port}", auth=("", ""))

    stats = WorkerStats(
        worker_id=worker_id,
        queries_executed=0,
        queries_passed=0,
        queries_failed=0,
        total_serial_time_ms=0,
        total_parallel_time_ms=0,
        errors=[],
    )

    query_types = list(QueryType)
    start_time = time.time()
    iteration = 0

    try:
        while iteration < iterations and (time.time() - start_time) < duration_seconds:
            query_type = random.choice(query_types)
            query = PARALLEL_QUERIES[query_type]  # No parallel hint

            try:
                result, elapsed = execute_query(driver, query)
                stats.queries_executed += 1
                stats.queries_passed += 1
                stats.total_serial_time_ms += elapsed
            except Exception as e:
                stats.queries_executed += 1
                stats.queries_failed += 1
                stats.errors.append(f"[SERIAL {query_type.value}] {str(e)}")

            iteration += 1
            time.sleep(0.005)  # Small delay

        print(f"[Worker {worker_id}] SERIAL reader completed {stats.queries_executed} queries")

    except Exception as e:
        stats.errors.append(f"Worker exception: {str(e)}")
        print(f"[Worker {worker_id}] Exception: {e}")

    finally:
        driver.close()
        result_queue.put(stats)


def verification_reader_worker(
    worker_id: int, host: str, port: int, iterations: int, duration_seconds: int, result_queue: multiprocessing.Queue
) -> None:
    """Worker that verifies parallel results match serial results."""
    print(f"[Worker {worker_id}] Starting VERIFICATION reader worker...")

    driver = GraphDatabase.driver(f"bolt://{host}:{port}", auth=("", ""))

    stats = WorkerStats(
        worker_id=worker_id,
        queries_executed=0,
        queries_passed=0,
        queries_failed=0,
        total_serial_time_ms=0,
        total_parallel_time_ms=0,
        errors=[],
    )

    query_types = list(QueryType)
    start_time = time.time()
    iteration = 0

    try:
        while iteration < iterations and (time.time() - start_time) < duration_seconds:
            # Pick a random query type
            query_type = random.choice(query_types)

            # Execute and verify
            result = verify_parallel_correctness(driver, query_type)
            stats.queries_executed += 1
            stats.total_serial_time_ms += result.serial_time_ms
            stats.total_parallel_time_ms += result.parallel_time_ms

            if result.match:
                stats.queries_passed += 1
            else:
                stats.queries_failed += 1
                error_msg = f"[VERIFY {query_type.value}] {result.error}"
                stats.errors.append(error_msg)
                print(f"[Worker {worker_id}] FAILED: {error_msg}")

            iteration += 1

            # Small delay to prevent overwhelming the server
            time.sleep(0.01)

        print(
            f"[Worker {worker_id}] VERIFICATION completed {stats.queries_executed} queries "
            f"({stats.queries_passed} passed, {stats.queries_failed} failed)"
        )

    except Exception as e:
        stats.errors.append(f"Worker exception: {str(e)}")
        print(f"[Worker {worker_id}] Exception: {e}")

    finally:
        driver.close()
        result_queue.put(stats)


def writer_worker(
    worker_id: int, host: str, port: int, iterations: int, duration_seconds: int, result_queue: multiprocessing.Queue
) -> None:
    """Worker that performs write operations concurrent with reads."""
    print(f"[Worker {worker_id}] Starting WRITER worker...")

    driver = GraphDatabase.driver(f"bolt://{host}:{port}", auth=("", ""))

    stats = WorkerStats(
        worker_id=worker_id,
        queries_executed=0,
        queries_passed=0,
        queries_failed=0,
        total_serial_time_ms=0,
        total_parallel_time_ms=0,
        errors=[],
    )

    write_queries = [
        # Create new nodes
        "CREATE (:TempNode {id: $id, ts: timestamp()})",
        # Update existing nodes
        "MATCH (n:Node {id: $id}) SET n.updated = timestamp()",
        # Create relationships
        "MATCH (a:Node {id: $id}), (b:Node {id: $id + 1}) CREATE (a)-[:TEMP]->(b)",
    ]

    start_time = time.time()
    iteration = 0

    try:
        while iteration < iterations and (time.time() - start_time) < duration_seconds:
            # Pick a random write query
            query = random.choice(write_queries)
            node_id = random.randint(1, 10000)

            try:
                start = time.time()
                with driver.session() as session:
                    session.run(query, {"id": node_id})
                elapsed = (time.time() - start) * 1000
                stats.queries_executed += 1
                stats.queries_passed += 1
                stats.total_serial_time_ms += elapsed
            except Exception as e:
                stats.queries_executed += 1
                stats.queries_failed += 1
                # Some failures are expected (e.g., concurrent modifications)
                if "Serialization" not in str(e):
                    stats.errors.append(f"[WRITE] {str(e)}")

            iteration += 1

            # Writers are slower to not overwhelm the system
            time.sleep(0.05)

        print(f"[Worker {worker_id}] WRITER completed {stats.queries_executed} operations")

    except Exception as e:
        stats.errors.append(f"Writer exception: {str(e)}")
        print(f"[Worker {worker_id}] Writer exception: {e}")

    finally:
        driver.close()
        result_queue.put(stats)


def cleanup_temp_data(driver) -> None:
    """Clean up temporary data created by writers."""
    with driver.session() as session:
        session.run("MATCH (n:TempNode) DETACH DELETE n")
        session.run("MATCH ()-[r:TEMP]->() DELETE r")


# =============================================================================
# Main Stress Test
# =============================================================================


def run_stress_test(
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    worker_count: int = DEFAULT_WORKER_COUNT,
    iterations: int = DEFAULT_ITERATIONS,
    duration_seconds: int = DEFAULT_DURATION_SECONDS,
    include_writers: bool = True,
) -> bool:
    """
    Run the parallel execution stress test with MIXED workloads.

    Worker distribution (for 8 workers):
    - 2 PARALLEL readers: Only run USING PARALLEL EXECUTION queries
    - 2 SERIAL readers: Only run single-threaded queries
    - 2 VERIFICATION readers: Verify parallel == serial results
    - 2 WRITER workers: Run write queries (if include_writers=True)

    This creates a truly mixed workload where parallel and serial queries
    run simultaneously alongside write operations.

    Returns:
        True if all tests passed, False otherwise
    """
    print("=" * 70)
    print("PARALLEL EXECUTION STRESS TEST (MIXED WORKLOAD)")
    print("=" * 70)
    print(f"Host: {host}:{port}")
    print(f"Worker count: {worker_count}")
    print(f"Iterations per worker: {iterations}")
    print(f"Max duration: {duration_seconds} seconds")
    print(f"Include writers: {include_writers}")
    print("-" * 70)

    # Distribute workers across types for truly mixed workload
    # With 8 workers: 2 parallel, 2 serial, 2 verification, 2 writers
    if include_writers:
        num_parallel = max(1, worker_count // 4)
        num_serial = max(1, worker_count // 4)
        num_verification = max(1, worker_count // 4)
        num_writers = max(1, worker_count - num_parallel - num_serial - num_verification)
    else:
        num_parallel = max(1, worker_count // 3)
        num_serial = max(1, worker_count // 3)
        num_verification = worker_count - num_parallel - num_serial
        num_writers = 0

    print(f"Worker distribution:")
    print(f"  - PARALLEL readers: {num_parallel}")
    print(f"  - SERIAL readers: {num_serial}")
    print(f"  - VERIFICATION readers: {num_verification}")
    print(f"  - WRITER workers: {num_writers}")
    print("=" * 70)

    # Create result queue
    result_queue = multiprocessing.Queue()

    # Start workers
    processes = []
    worker_id = 0

    # Parallel reader workers (run only USING PARALLEL EXECUTION queries)
    for i in range(num_parallel):
        p = multiprocessing.Process(
            target=parallel_reader_worker, args=(worker_id, host, port, iterations, duration_seconds, result_queue)
        )
        processes.append((p, "PARALLEL"))
        p.start()
        worker_id += 1

    # Serial reader workers (run only single-threaded queries)
    for i in range(num_serial):
        p = multiprocessing.Process(
            target=serial_reader_worker, args=(worker_id, host, port, iterations, duration_seconds, result_queue)
        )
        processes.append((p, "SERIAL"))
        p.start()
        worker_id += 1

    # Verification workers (verify parallel == serial)
    for i in range(num_verification):
        p = multiprocessing.Process(
            target=verification_reader_worker, args=(worker_id, host, port, iterations, duration_seconds, result_queue)
        )
        processes.append((p, "VERIFICATION"))
        p.start()
        worker_id += 1

    # Writer workers
    for i in range(num_writers):
        p = multiprocessing.Process(
            target=writer_worker, args=(worker_id, host, port, iterations // 5, duration_seconds, result_queue)
        )
        processes.append((p, "WRITER"))
        p.start()
        worker_id += 1

    # Wait for all workers to complete
    for p, worker_type in processes:
        p.join()

    # Collect results
    all_stats: List[WorkerStats] = []
    while not result_queue.empty():
        all_stats.append(result_queue.get())

    # Cleanup temporary data
    try:
        driver = GraphDatabase.driver(f"bolt://{host}:{port}", auth=("", ""))
        cleanup_temp_data(driver)
        driver.close()
    except Exception as e:
        print(f"Warning: Cleanup failed: {e}")

    # Print summary
    print("\n" + "=" * 70)
    print("RESULTS SUMMARY")
    print("=" * 70)

    total_executed = sum(s.queries_executed for s in all_stats)
    total_passed = sum(s.queries_passed for s in all_stats)
    total_failed = sum(s.queries_failed for s in all_stats)
    total_serial_time = sum(s.total_serial_time_ms for s in all_stats)
    total_parallel_time = sum(s.total_parallel_time_ms for s in all_stats)
    all_errors = []

    for stats in all_stats:
        print(f"\nWorker {stats.worker_id}:")
        print(f"  Queries executed: {stats.queries_executed}")
        print(f"  Queries passed: {stats.queries_passed}")
        print(f"  Queries failed: {stats.queries_failed}")
        if stats.total_serial_time_ms > 0:
            print(f"  Total serial time: {stats.total_serial_time_ms:.2f} ms")
        if stats.total_parallel_time_ms > 0:
            print(f"  Total parallel time: {stats.total_parallel_time_ms:.2f} ms")
        all_errors.extend(stats.errors)

    print("\n" + "-" * 70)
    print("TOTALS:")
    print(f"  Total queries: {total_executed}")
    print(f"  Total passed: {total_passed}")
    print(f"  Total failed: {total_failed}")
    print(f"  Pass rate: {100 * total_passed / max(1, total_executed):.2f}%")

    if total_serial_time > 0 and total_parallel_time > 0:
        print(f"  Total serial time: {total_serial_time:.2f} ms")
        print(f"  Total parallel time: {total_parallel_time:.2f} ms")

    if all_errors:
        print("\nERRORS:")
        for i, error in enumerate(all_errors[:10]):  # Show first 10 errors
            print(f"  {i+1}. {error}")
        if len(all_errors) > 10:
            print(f"  ... and {len(all_errors) - 10} more errors")

    print("=" * 70)

    # Success if no failures (verification workers are the key - they check correctness)
    success = total_failed == 0
    if success:
        print("STRESS TEST PASSED!")
    else:
        print("STRESS TEST FAILED!")

    return success


# =============================================================================
# Entry Point
# =============================================================================


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Parallel Execution Stress Test")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Memgraph host")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Memgraph port")
    parser.add_argument("--worker-count", type=int, default=DEFAULT_WORKER_COUNT, help="Number of worker processes")
    parser.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS, help="Iterations per worker")
    parser.add_argument("--duration", type=int, default=DEFAULT_DURATION_SECONDS, help="Max duration in seconds")
    parser.add_argument("--no-writers", action="store_true", help="Disable writer workers")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    success = run_stress_test(
        host=args.host,
        port=args.port,
        worker_count=args.worker_count,
        iterations=args.iterations,
        duration_seconds=args.duration,
        include_writers=not args.no_writers,
    )

    sys.exit(0 if success else 1)
