#!/usr/bin/env python3
"""
Benchmark: single-store vector edge index vs master (old) implementation.

Measures:
  1. Bulk import (CREATE edges with vector properties)
  2. Search (vector_search.search_edges)
  3. Delete (DETACH DELETE edges)
  4. Memory (SHOW STORAGE INFO — graph + vector_index tracked)

Usage:
  # Start memgraph on the branch you want to benchmark, then:
  python3 vector_edge_index_bench.py [--port 7687] [--dimension 128] [--count 10000]

  # To compare, run once per branch and diff the output.
"""

import argparse
import random
import statistics
import time

import mgclient


def connect(port):
    conn = mgclient.connect(host="127.0.0.1", port=port)
    conn.autocommit = True
    return conn


def execute(cursor, query, params=None):
    cursor.execute(query, params or {})
    return cursor.fetchall()


def parse_mib(value):
    value = value.strip()
    if value.endswith("GiB"):
        return float(value[:-3]) * 1024
    if value.endswith("MiB"):
        return float(value[:-3])
    if value.endswith("KiB"):
        return float(value[:-3]) / 1024
    if value.endswith("B"):
        return float(value[:-1]) / (1024 * 1024)
    return 0.0


def get_memory(cursor):
    rows = execute(cursor, "SHOW STORAGE INFO")
    info = {row[0]: row[1] for row in rows}
    return {
        "memory_tracked_mib": parse_mib(info.get("memory_tracked", "0B")),
        "graph_memory_mib": parse_mib(info.get("graph_memory_tracked", "0B")),
        "vector_index_memory_mib": parse_mib(info.get("vector_index_memory_tracked", "0B")),
        "memory_res_mib": parse_mib(info.get("memory_res", "0B")),
    }


def random_vector(dim):
    return [random.random() for _ in range(dim)]


def setup(cursor, dimension, capacity):
    execute(cursor, "MATCH (n) DETACH DELETE n")
    execute(cursor, "FREE MEMORY")

    # Drop existing vector edge indices
    try:
        execute(cursor, "DROP VECTOR EDGE INDEX IF EXISTS bench_edge_idx")
    except Exception:
        pass

    execute(
        cursor,
        f"CREATE VECTOR EDGE INDEX bench_edge_idx ON :CONNECTS(embedding) "
        f'WITH CONFIG {{"dimension": {dimension}, "capacity": {capacity}}}',
    )

    # Create source and target nodes
    execute(cursor, "UNWIND range(0, 99) AS i CREATE (:Src {id: i})")
    execute(cursor, "UNWIND range(0, 99) AS i CREATE (:Dst {id: i})")


def bench_bulk_import(cursor, count, dimension):
    """Insert edges with vector properties in batches."""
    batch_size = 500
    total_time = 0.0

    for batch_start in range(0, count, batch_size):
        batch_end = min(batch_start + batch_size, count)
        actual_batch = batch_end - batch_start

        vectors = [random_vector(dimension) for _ in range(actual_batch)]
        src_ids = [random.randint(0, 99) for _ in range(actual_batch)]
        dst_ids = [random.randint(0, 99) for _ in range(actual_batch)]

        start = time.perf_counter()
        for i in range(actual_batch):
            execute(
                cursor,
                "MATCH (s:Src {id: $src}), (d:Dst {id: $dst}) " "CREATE (s)-[:CONNECTS {embedding: $vec}]->(d)",
                {"src": src_ids[i], "dst": dst_ids[i], "vec": vectors[i]},
            )
        elapsed = time.perf_counter() - start
        total_time += elapsed

    return {
        "total_sec": total_time,
        "edges_per_sec": count / total_time,
    }


def bench_search(cursor, dimension, iterations=100):
    """Run vector search queries and measure latency."""
    latencies = []
    for _ in range(iterations):
        query_vec = random_vector(dimension)
        start = time.perf_counter()
        execute(
            cursor,
            'CALL vector_search.search_edges("bench_edge_idx", 10, $query) '
            "YIELD edge, distance RETURN edge, distance",
            {"query": query_vec},
        )
        elapsed = time.perf_counter() - start
        latencies.append(elapsed * 1000)  # ms

    return {
        "iterations": iterations,
        "avg_ms": statistics.mean(latencies),
        "p50_ms": statistics.median(latencies),
        "p99_ms": sorted(latencies)[int(len(latencies) * 0.99)],
        "min_ms": min(latencies),
        "max_ms": max(latencies),
    }


def bench_delete(cursor, count):
    """Delete all edges and measure time."""
    start = time.perf_counter()
    execute(cursor, "MATCH ()-[r:CONNECTS]->() DELETE r")
    elapsed = time.perf_counter() - start

    # Force GC
    execute(cursor, "FREE MEMORY")

    return {
        "total_sec": elapsed,
        "edges_per_sec": count / elapsed if elapsed > 0 else 0,
    }


def main():
    parser = argparse.ArgumentParser(description="Vector edge index benchmark")
    parser.add_argument("--port", type=int, default=7687)
    parser.add_argument("--dimension", type=int, default=128)
    parser.add_argument("--count", type=int, default=10000, help="Number of edges to insert")
    parser.add_argument("--search-iterations", type=int, default=200)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)
    capacity = args.count * 2

    conn = connect(args.port)
    cursor = conn.cursor()

    print(f"=== Vector Edge Index Benchmark ===")
    print(f"  dimension={args.dimension}, count={args.count}, capacity={capacity}")
    print()

    # Baseline memory
    mem_baseline = get_memory(cursor)

    # Setup
    setup(cursor, args.dimension, capacity)
    mem_after_setup = get_memory(cursor)

    # 1. Bulk import
    print("--- Bulk Import ---")
    import_result = bench_bulk_import(cursor, args.count, args.dimension)
    print(f"  Total: {import_result['total_sec']:.2f}s")
    print(f"  Throughput: {import_result['edges_per_sec']:.0f} edges/sec")
    print()

    # Free deltas to get clean memory measurement
    execute(cursor, "FREE MEMORY")

    # 2. Memory after import
    mem_after_import = get_memory(cursor)
    print("--- Memory After Import (post FREE MEMORY) ---")
    print(f"  Total tracked:        {mem_after_import['memory_tracked_mib']:.2f} MiB")
    print(f"  Graph memory:         {mem_after_import['graph_memory_mib']:.2f} MiB")
    print(f"  Vector index memory:  {mem_after_import['vector_index_memory_mib']:.2f} MiB")
    print(f"  RSS:                  {mem_after_import['memory_res_mib']:.2f} MiB")
    print(
        f"  Delta from baseline:  {mem_after_import['memory_tracked_mib'] - mem_baseline['memory_tracked_mib']:.2f} MiB"
    )
    print()

    # 3. Search
    print("--- Search ---")
    search_result = bench_search(cursor, args.dimension, args.search_iterations)
    print(f"  Iterations: {search_result['iterations']}")
    print(f"  Avg:  {search_result['avg_ms']:.2f} ms")
    print(f"  P50:  {search_result['p50_ms']:.2f} ms")
    print(f"  P99:  {search_result['p99_ms']:.2f} ms")
    print(f"  Min:  {search_result['min_ms']:.2f} ms")
    print(f"  Max:  {search_result['max_ms']:.2f} ms")
    print()

    # 4. Delete
    print("--- Delete ---")
    delete_result = bench_delete(cursor, args.count)
    mem_after_delete = get_memory(cursor)
    print(f"  Total: {delete_result['total_sec']:.2f}s")
    print(f"  Throughput: {delete_result['edges_per_sec']:.0f} edges/sec")
    print()

    # 5. Memory after delete
    print("--- Memory After Delete + GC ---")
    print(f"  Total tracked:        {mem_after_delete['memory_tracked_mib']:.2f} MiB")
    print(f"  Graph memory:         {mem_after_delete['graph_memory_mib']:.2f} MiB")
    print(f"  Vector index memory:  {mem_after_delete['vector_index_memory_mib']:.2f} MiB")
    print(f"  RSS:                  {mem_after_delete['memory_res_mib']:.2f} MiB")
    print()

    # Summary
    print("=== Summary ===")
    print(f"  Import:  {import_result['edges_per_sec']:.0f} edges/sec")
    print(f"  Search:  {search_result['avg_ms']:.2f} ms avg, {search_result['p99_ms']:.2f} ms p99")
    print(f"  Delete:  {delete_result['edges_per_sec']:.0f} edges/sec")
    print(
        f"  Memory:  {mem_after_import['memory_tracked_mib']:.2f} MiB total "
        f"(graph={mem_after_import['graph_memory_mib']:.2f}, "
        f"vector_idx={mem_after_import['vector_index_memory_mib']:.2f})"
    )

    conn.close()


if __name__ == "__main__":
    main()
