#!/usr/bin/env python3
"""
Concurrent write workload for stress testing edge creation on a supernode.

Creates a supernode and many regular nodes, then concurrently creates edges
between the supernode and each regular node using parallel workers.
"""
import sys
import time

from cluster_monitor import ClusterMonitor
from ha_common import Protocol, QueryType, cleanup, execute_and_fetch, execute_query, get_instance_names, run_parallel

COORDINATOR = "coord_1"
COORDINATORS = ["coord_1", "coord_2", "coord_3"]
DATA_INSTANCES = sorted(instance for instance in get_instance_names() if instance.startswith("data_"))

NUM_NODES = 300000
NUM_WORKERS = 20
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


def create_edge(node_id: int) -> None:
    execute_query(
        COORDINATOR,
        """
        MATCH (s:SuperNode {id: 0}), (n:Node {id: $node_id})
        CREATE (s)-[:CONNECTED_TO {created_at: timestamp()}]->(n)
        """,
        params={"node_id": node_id},
        protocol=Protocol.BOLT_ROUTING,
        query_type=QueryType.WRITE,
        apply_retry_mechanism=True,
    )


def run_workload() -> None:
    print(f"\nCreating {NUM_NODES} edges concurrently with {NUM_WORKERS} workers...")
    tasks = [(node_id,) for node_id in range(1, NUM_NODES + 1)]
    run_parallel(create_edge, tasks, num_workers=NUM_WORKERS)
    print("Edge creation complete.")


def get_edge_count(instance: str, protocol: Protocol = Protocol.BOLT) -> int:
    """Get edge count from a specific instance."""
    results = execute_and_fetch(
        instance,
        "MATCH (:SuperNode)-[r:CONNECTED_TO]->(:Node) RETURN count(r) AS edge_count",
        protocol=protocol,
        query_type=QueryType.READ,
    )
    return results[0]["edge_count"]


def wait_for_replica_sync(timeout_sec: int = REPLICA_SYNC_TIMEOUT_SEC) -> None:
    """Wait for all replicas to sync with the expected edge count."""
    print(f"\nWaiting for replicas to sync (timeout: {timeout_sec}s)...")
    start_time = time.time()

    while time.time() - start_time < timeout_sec:
        all_synced = True
        for instance in DATA_INSTANCES:
            try:
                count = get_edge_count(instance)
                if count != NUM_NODES:
                    print(f"  {instance}: {count}/{NUM_NODES} edges")
                    all_synced = False
            except Exception as e:
                print(f"  {instance}: error - {e}")
                all_synced = False

        if all_synced:
            print("All replicas synced.")
            return

        time.sleep(2)

    raise RuntimeError(f"Replicas did not sync within {timeout_sec} seconds")


def verify_results(monitor: ClusterMonitor) -> None:
    print("\nVerifying results on coordinator...")
    edge_count = get_edge_count(COORDINATOR, Protocol.BOLT_ROUTING)
    if edge_count != NUM_NODES:
        raise RuntimeError(f"Edge count mismatch: expected {NUM_NODES}, got {edge_count}")
    print(f"Coordinator edge count: {edge_count}")

    wait_for_replica_sync()

    print("\nVerifying edge count on all data instances...")
    for instance in DATA_INSTANCES:
        count = get_edge_count(instance)
        if count != NUM_NODES:
            raise RuntimeError(f"{instance} edge count mismatch: expected {NUM_NODES}, got {count}")
        print(f"  {instance}: {count} edges (OK)")

    monitor.show_replicas()

    ok = monitor.verify_all_ready() and monitor.verify_instances_up()
    if not ok:
        print("ERROR: Cluster is not healthy!")
        sys.exit(1)


def main():
    print("=" * 60)
    print("Concurrent Edge Creation on Supernode Workload")
    print("=" * 60)
    print(f"Supernode         : 1")
    print(f"Regular nodes     : {NUM_NODES:,}")
    print(f"Workers           : {NUM_WORKERS}")
    print("-" * 60)

    monitor = ClusterMonitor(
        coordinators=COORDINATORS,
        show_replicas=True,
        verify_up=True,
        storage_info=["vertex_count", "edge_count", "memory_res", "allocation_limit"],
        metrics_info=["FailedQuery", "TransientErrors"],
        interval=2,
    )

    total_start = time.time()
    try:
        with monitor:
            setup_graph()
            run_workload()
            verify_results(monitor)

        total_elapsed = time.time() - total_start
        print("=" * 60)
        print(f"Completed in {total_elapsed:.1f}s ({total_elapsed / 60:.1f} min)")
        print("Workload completed successfully!")
    except SystemExit:
        raise
    finally:
        cleanup()


if __name__ == "__main__":
    main()
