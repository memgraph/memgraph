#!/usr/bin/env python3
"""
Concurrent write workload with mixed commit/abort transactions.

Creates a supernode and many regular nodes, then concurrently creates edges
in batches, with ~20% of transactions being explicitly rolled back.
"""
import multiprocessing
import random
import sys
import time

from ha_common import (
    Protocol,
    QueryType,
    cleanup,
    create_routing_driver_for,
    execute_and_fetch,
    execute_query,
    get_instance_names,
)

COORDINATOR = "coord_1"
DATA_INSTANCES = sorted(instance for instance in get_instance_names() if instance.startswith("data_"))

NUM_NODES = 200000
NUM_WORKERS = 5
BATCH_SIZE = 10000
ABORT_PROBABILITY = 0.2
REPLICA_SYNC_TIMEOUT_SEC = 120


def setup_graph() -> None:
    print("Creating supernode...")
    execute_query(
        COORDINATOR,
        "CREATE (:SuperNode {id: 0})",
        protocol=Protocol.BOLT_ROUTING,
        query_type=QueryType.WRITE,
    )

    print(f"Creating {NUM_NODES} regular nodes...")
    execute_query(
        COORDINATOR,
        "UNWIND range(1, $num_nodes) AS i CREATE (:Node {id: i})",
        params={"num_nodes": NUM_NODES},
        protocol=Protocol.BOLT_ROUTING,
        query_type=QueryType.WRITE,
    )

    print("Creating indexes...")
    execute_query(
        COORDINATOR,
        "CREATE INDEX ON :SuperNode(id);",
        protocol=Protocol.BOLT_ROUTING,
        query_type=QueryType.WRITE,
    )
    execute_query(
        COORDINATOR,
        "CREATE INDEX ON :Node(id);",
        protocol=Protocol.BOLT_ROUTING,
        query_type=QueryType.WRITE,
    )
    print("Setup complete.")


_worker_driver = None


def _get_worker_driver():
    """Get or create a driver for the current worker process."""
    global _worker_driver
    if _worker_driver is None:
        _worker_driver = create_routing_driver_for(COORDINATOR)
    return _worker_driver


def create_edges_batch_with_commit_or_abort(args: tuple) -> int:
    """
    Create a batch of edges in an explicit transaction, then commit or rollback.
    Returns the number of edges committed (batch size if committed, 0 if aborted).
    """
    node_ids, should_abort = args
    max_retries = 3
    retry_delay = 2

    for attempt in range(1, max_retries + 1):
        try:
            driver = _get_worker_driver()
            with driver.session() as session:
                tx = session.begin_transaction()
                try:
                    tx.run(
                        """
                        UNWIND $node_ids AS node_id
                        MATCH (s:SuperNode {id: 0}), (n:Node {id: node_id})
                        CREATE (s)-[:CONNECTED_TO {created_at: timestamp()}]->(n)
                        """,
                        node_ids=node_ids,
                    )
                    if should_abort:
                        tx.rollback()
                        return 0
                    else:
                        tx.commit()
                        return len(node_ids)
                except Exception:
                    tx.rollback()
                    raise
        except Exception as e:
            global _worker_driver
            if _worker_driver is not None:
                try:
                    _worker_driver.close()
                except Exception:
                    pass
                _worker_driver = None

            if attempt < max_retries:
                print(f"WARN: Batch failed (attempt {attempt}/{max_retries}), retrying in {retry_delay}s: {e}")
                time.sleep(retry_delay)
                continue
            raise


def run_workload(committed_count: multiprocessing.Value) -> None:
    num_batches = (NUM_NODES + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"\nCreating edges in {num_batches} batches (size {BATCH_SIZE}) with {NUM_WORKERS} workers...")
    print(f"Abort probability: {ABORT_PROBABILITY * 100:.0f}%")

    all_node_ids = list(range(1, NUM_NODES + 1))
    batches = [all_node_ids[i : i + BATCH_SIZE] for i in range(0, NUM_NODES, BATCH_SIZE)]
    tasks = [(batch, random.random() < ABORT_PROBABILITY) for batch in batches]

    aborted_batches = sum(1 for _, should_abort in tasks if should_abort)
    committed_batches = len(tasks) - aborted_batches
    print(f"Planned: {committed_batches} batch commits, {aborted_batches} batch aborts")

    with multiprocessing.Pool(processes=NUM_WORKERS) as pool:
        results = pool.map(create_edges_batch_with_commit_or_abort, tasks)

    actual_commits = sum(results)
    committed_count.value = actual_commits
    print(f"Edge creation complete. Committed edges: {actual_commits}, Aborted edges: {NUM_NODES - actual_commits}")


def get_edge_count(instance: str, protocol: Protocol = Protocol.BOLT) -> int:
    results = execute_and_fetch(
        instance,
        "MATCH (:SuperNode)-[r:CONNECTED_TO]->(:Node) RETURN count(r) AS edge_count",
        protocol=protocol,
        query_type=QueryType.READ,
    )
    return results[0]["edge_count"]


def wait_for_replica_sync(expected_count: int, timeout_sec: int = REPLICA_SYNC_TIMEOUT_SEC) -> None:
    print(f"\nWaiting for replicas to sync (timeout: {timeout_sec}s)...")
    start_time = time.time()

    while time.time() - start_time < timeout_sec:
        all_synced = True
        for instance in DATA_INSTANCES:
            try:
                count = get_edge_count(instance)
                if count != expected_count:
                    print(f"  {instance}: {count}/{expected_count} edges")
                    all_synced = False
            except Exception as e:
                print(f"  {instance}: error - {e}")
                all_synced = False

        if all_synced:
            print("All replicas synced.")
            return

        time.sleep(2)

    raise RuntimeError(f"Replicas did not sync within {timeout_sec} seconds")


def verify_results(expected_count: int) -> None:
    print(f"\nVerifying results on coordinator (expected: {expected_count} edges)...")
    edge_count = get_edge_count(COORDINATOR, Protocol.BOLT_ROUTING)
    if edge_count != expected_count:
        raise RuntimeError(f"Edge count mismatch: expected {expected_count}, got {edge_count}")
    print(f"Coordinator edge count: {edge_count}")

    wait_for_replica_sync(expected_count)

    print("\nVerifying edge count on all data instances...")
    for instance in DATA_INSTANCES:
        count = get_edge_count(instance)
        if count != expected_count:
            raise RuntimeError(f"{instance} edge count mismatch: expected {expected_count}, got {count}")
        print(f"  {instance}: {count} edges (OK)")


def main():
    print("Concurrent Write Workload with Mixed Commit/Abort")
    print(f"Supernode: 1, Regular nodes: {NUM_NODES}, Workers: {NUM_WORKERS}")
    print(f"Batch size: {BATCH_SIZE}, Abort probability: {ABORT_PROBABILITY * 100:.0f}%")
    print("-" * 60)

    committed_count = multiprocessing.Value("i", 0)

    try:
        setup_graph()
        run_workload(committed_count)
        verify_results(committed_count.value)

        print("-" * 60)
        print("Workload completed successfully!")
    finally:
        pass
    # TODO(colinbarry): Inspect why cleanup is failing to replicate on
    # large graphs. Commented out for now, as this is also the case with
    # `concurrent_edge_creation_on_supernodes` and `publications` stress tests.
    #     cleanup()


if __name__ == "__main__":
    main()
