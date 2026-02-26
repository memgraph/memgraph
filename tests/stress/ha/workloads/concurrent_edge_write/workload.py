#!/usr/bin/env python3
"""
High-concurrency edge write workload.
Creates 1000 nodes via UNWIND+CREATE, then 5 concurrent workers repeatedly
MERGE RELATED_TO edges between cartesian product pairs of those nodes.
"""
import sys
import time

from cluster_monitor import ClusterMonitor
from ha_common import Protocol, QueryType, execute_and_fetch, execute_query, run_parallel

COORDINATOR = "coord_1"

NUM_NODES = 1000
NUM_WORKERS = 5
NUM_ITERATIONS = 50


def create_indices_and_constraints() -> None:
    print("Creating indices and constraints...")
    for query in [
        "CREATE INDEX ON :Node;",
        "CREATE INDEX ON :Node(uid);",
        "CREATE CONSTRAINT ON (n:Node) ASSERT n.uid IS UNIQUE;",
    ]:
        print(f"  {query}")
        execute_query(COORDINATOR, query, protocol=Protocol.BOLT_ROUTING, query_type=QueryType.WRITE)
    print("Indices and constraints created.")


def create_nodes() -> None:
    print(f"\nCreating {NUM_NODES} nodes...")
    query = f"""
UNWIND range(0, {NUM_NODES - 1}) AS i
CREATE (:Node {{uid: i, name: 'node_' + toString(i)}});"""

    start_time = time.time()
    execute_query(COORDINATOR, query, protocol=Protocol.BOLT_ROUTING, query_type=QueryType.WRITE)
    elapsed = time.time() - start_time
    print(f"  Created {NUM_NODES} nodes in {elapsed:.1f}s")


def edge_write_worker(worker_id: int) -> dict:
    query = f"""
MATCH (a:Node), (b:Node)
WHERE a.uid < b.uid
  AND (a.uid + b.uid) % {NUM_WORKERS} = {worker_id}
MERGE (a)-[:RELATED_TO]->(b);"""

    t0 = time.time()
    for iteration in range(NUM_ITERATIONS):
        execute_query(COORDINATOR, query, protocol=Protocol.BOLT_ROUTING, query_type=QueryType.WRITE)
        if (iteration + 1) % 10 == 0:
            print(f"  [Worker {worker_id}] {iteration + 1}/{NUM_ITERATIONS} iterations")
    elapsed = time.time() - t0
    print(f"  [Worker {worker_id}] Completed {NUM_ITERATIONS} iterations in {elapsed:.1f}s")
    return {"worker_id": worker_id, "elapsed": elapsed}


def run_concurrent_edge_writes() -> None:
    print(f"\nRunning {NUM_WORKERS} concurrent edge write workers...")
    tasks = [(i,) for i in range(NUM_WORKERS)]

    t0 = time.time()
    run_parallel(edge_write_worker, tasks, num_workers=NUM_WORKERS)
    elapsed = time.time() - t0

    print(f"  All {NUM_WORKERS} workers completed in {elapsed:.1f}s")


def main():
    print("=" * 60)
    print("High-Concurrency Edge Write Workload")
    print("=" * 60)
    print(f"Nodes: {NUM_NODES}, Workers: {NUM_WORKERS}, Iterations: {NUM_ITERATIONS}")
    print("-" * 60)

    monitor = ClusterMonitor(
        coordinator=COORDINATOR,
        show_replicas=True,
        verify_up=True,
        storage_info=["vertex_count", "edge_count", "memory_res"],
        interval=5,
    )

    total_start = time.time()

    with monitor:
        create_indices_and_constraints()
        create_nodes()
        run_concurrent_edge_writes()

    total_elapsed = time.time() - total_start

    print("-" * 60)
    print(f"Total time: {total_elapsed:.1f}s ({total_elapsed/60:.1f} minutes)")

    result = execute_and_fetch(COORDINATOR, "MATCH (n:Node) RETURN count(n) as cnt", protocol=Protocol.BOLT_ROUTING)
    node_count = result[0]["cnt"] if result else 0

    result = execute_and_fetch(
        COORDINATOR, "MATCH ()-[r:RELATED_TO]->() RETURN count(r) as cnt", protocol=Protocol.BOLT_ROUTING
    )
    edge_count = result[0]["cnt"] if result else 0

    print(f"\nFinal counts: {node_count:,} nodes, {edge_count:,} edges")

    print("\nWaiting 30 seconds before final verification...")
    time.sleep(30)

    print("\nFinal replica status:")
    monitor.show_replicas()

    ok = monitor.verify_all_ready() and monitor.verify_instances_up()
    if not ok:
        sys.exit(1)
    print("Workload completed successfully!")


if __name__ == "__main__":
    main()
