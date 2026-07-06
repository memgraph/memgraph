#!/usr/bin/env python3
"""
Concurrent write workload for stress testing edge creation on a supernode
against a standalone (single-instance) Memgraph.

Creates a supernode and many regular nodes, then concurrently creates edges
between the supernode and each regular node using parallel worker processes.
This hammers MVCC and garbage collection with heavy contention on a single
vertex's edge list, so the monitor also tracks GC latency metrics.
"""
import multiprocessing
import sys
import time

from neo4j import GraphDatabase
from standalone_monitor import StandaloneMonitor

HOST = "127.0.0.1"
PORT = 7687
URI = f"bolt://{HOST}:{PORT}"
AUTH = ("", "")

NUM_NODES = 300000
NUM_WORKERS = 20
BATCH_SIZE = 100  # edges created per worker task (one transaction)

STORAGE_FIELDS = ["vertex_count", "edge_count", "memory_res", "memory_tracked"]
METRICS_FIELDS = [
    "GCLatency_us_50p",
    "GCLatency_us_90p",
    "GCLatency_us_99p",
    "GCSkiplistCleanupLatency_us_50p",
    "GCSkiplistCleanupLatency_us_90p",
    "GCSkiplistCleanupLatency_us_99p",
]

# Per-process driver, created lazily after fork. neo4j drivers are not
# fork-safe, so each worker process builds its own on first use.
_worker_driver = None


def _get_worker_driver():
    global _worker_driver
    if _worker_driver is None:
        _worker_driver = GraphDatabase.driver(URI, auth=AUTH)
    return _worker_driver


def run_parallel(worker_fn, tasks, num_workers: int) -> None:
    """Run tasks in parallel. Terminates all workers immediately if any one fails."""
    task_list = list(tasks)
    with multiprocessing.Pool(processes=num_workers) as pool:
        async_results = [pool.apply_async(worker_fn, t) for t in task_list]
        while True:
            for ar in async_results:
                if ar.ready() and not ar.successful():
                    pool.terminate()
                    pool.join()
                    ar.get()  # re-raise the worker's exception
            if all(ar.ready() for ar in async_results):
                break
            time.sleep(0.5)


def create_edges(node_ids: list[int]) -> None:
    driver = _get_worker_driver()
    # Managed write transaction: neo4j retries automatically on the
    # serialization (TransientError) conflicts expected under supernode contention.
    # Each task creates a whole batch of edges in a single transaction.
    with driver.session() as session:
        session.execute_write(
            lambda tx: tx.run(
                """
                MATCH (s:SuperNode {id: 0})
                UNWIND $node_ids AS nid
                MATCH (n:Node {id: nid})
                CREATE (s)-[:CONNECTED_TO {created_at: timestamp()}]->(n)
                """,
                node_ids=node_ids,
            ).consume()
        )


def setup_graph(driver) -> None:
    with driver.session() as session:
        print("Creating supernode...")
        session.run("CREATE (:SuperNode {id: 0})").consume()

        print(f"Creating {NUM_NODES} regular nodes...")
        session.run(
            "UNWIND range(1, $num_nodes) AS i CREATE (:Node {id: i})",
            num_nodes=NUM_NODES,
        ).consume()

        print("Creating indexes...")
        session.run("CREATE INDEX ON :SuperNode(id);").consume()
        session.run("CREATE INDEX ON :Node(id);").consume()
    print("Setup complete.")


def run_workload() -> None:
    print(f"\nCreating {NUM_NODES} edges concurrently with {NUM_WORKERS} workers " f"({BATCH_SIZE} edges/task)...")
    node_ids = range(1, NUM_NODES + 1)
    tasks = [(list(node_ids[i : i + BATCH_SIZE]),) for i in range(0, NUM_NODES, BATCH_SIZE)]
    run_parallel(create_edges, tasks, num_workers=NUM_WORKERS)
    print("Edge creation complete.")


def get_edge_count(driver) -> int:
    with driver.session() as session:
        result = session.run("MATCH (:SuperNode)-[r:CONNECTED_TO]->(:Node) RETURN count(r) AS edge_count")
        return result.single()["edge_count"]


def verify_results(driver) -> None:
    print("\nVerifying results...")
    edge_count = get_edge_count(driver)
    if edge_count != NUM_NODES:
        raise RuntimeError(f"Edge count mismatch: expected {NUM_NODES}, got {edge_count}")
    print(f"Edge count: {edge_count} (OK)")


def main():
    print("=" * 60)
    print("Concurrent Edge Creation on Supernode Workload (standalone)")
    print("=" * 60)
    print(f"Supernode         : 1")
    print(f"Regular nodes     : {NUM_NODES:,}")
    print(f"Workers           : {NUM_WORKERS}")
    print("-" * 60)

    driver = GraphDatabase.driver(URI, auth=AUTH)
    monitor = StandaloneMonitor(
        host=HOST,
        port=PORT,
        storage_info=STORAGE_FIELDS,
        metrics_info=METRICS_FIELDS,
        interval=2,
    )

    total_start = time.time()
    try:
        with monitor:
            setup_graph(driver)
            run_workload()
            verify_results(driver)

        total_elapsed = time.time() - total_start
        print("=" * 60)
        print(f"Completed in {total_elapsed:.1f}s ({total_elapsed / 60:.1f} min)")
        print("Workload completed successfully!")
    except Exception as e:
        print(f"\nFATAL: {e}", file=sys.stderr)
        raise
    finally:
        driver.close()


if __name__ == "__main__":
    main()
