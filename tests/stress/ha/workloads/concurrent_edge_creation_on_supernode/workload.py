#!/usr/bin/env python3
"""
Concurrent write workload for stress testing edge creation on a supernode.

Creates a supernode and many regular nodes, then concurrently creates edges
between the supernode and each regular node using parallel workers.

Pinned to a single data instance over direct BOLT (no coordinator routing).
"""

import time

from cluster_monitor import ClusterMonitor
from ha_common import Protocol, QueryType, execute_and_fetch, execute_query, run_parallel

DATA_INSTANCE = "data_1"

NUM_NODES = 300000
NUM_WORKERS = 20


def setup_graph() -> None:
    print("Creating supernode...")
    execute_query(
        DATA_INSTANCE,
        "CREATE (:SuperNode {id: 0})",
        protocol=Protocol.BOLT,
        query_type=QueryType.WRITE,
    )

    print(f"Creating {NUM_NODES} regular nodes...")
    execute_query(
        DATA_INSTANCE,
        "UNWIND range(1, $num_nodes) AS i CREATE (:Node {id: i})",
        params={"num_nodes": NUM_NODES},
        protocol=Protocol.BOLT,
        query_type=QueryType.WRITE,
    )

    print("Creating indexes...")
    execute_query(
        DATA_INSTANCE,
        "CREATE INDEX ON :SuperNode(id);",
        protocol=Protocol.BOLT,
        query_type=QueryType.WRITE,
    )
    execute_query(
        DATA_INSTANCE,
        "CREATE INDEX ON :Node(id);",
        protocol=Protocol.BOLT,
        query_type=QueryType.WRITE,
    )
    print("Setup complete.")


def create_edge(node_id: int) -> None:
    execute_query(
        DATA_INSTANCE,
        """
        MATCH (s:SuperNode {id: 0}), (n:Node {id: $node_id})
        CREATE (s)-[:CONNECTED_TO {created_at: timestamp()}]->(n)
        """,
        params={"node_id": node_id},
        protocol=Protocol.BOLT,
        query_type=QueryType.WRITE,
        apply_retry_mechanism=True,
    )


def run_workload() -> None:
    print(f"\nCreating {NUM_NODES} edges concurrently with {NUM_WORKERS} workers...")
    tasks = [(node_id,) for node_id in range(1, NUM_NODES + 1)]
    run_parallel(create_edge, tasks, num_workers=NUM_WORKERS)
    print("Edge creation complete.")


def get_edge_count() -> int:
    results = execute_and_fetch(
        DATA_INSTANCE,
        "MATCH (:SuperNode)-[r:CONNECTED_TO]->(:Node) RETURN count(r) AS edge_count",
        protocol=Protocol.BOLT,
        query_type=QueryType.READ,
    )
    return results[0]["edge_count"]


def verify_results() -> None:
    print("\nVerifying results...")
    edge_count = get_edge_count()
    if edge_count != NUM_NODES:
        raise RuntimeError(f"Edge count mismatch: expected {NUM_NODES}, got {edge_count}")
    print(f"Edge count: {edge_count} (OK)")


def main():
    print("=" * 60)
    print("Concurrent Edge Creation on Supernode Workload (standalone)")
    print("=" * 60)
    print(f"Target instance   : {DATA_INSTANCE}")
    print(f"Supernode         : 1")
    print(f"Regular nodes     : {NUM_NODES:,}")
    print(f"Workers           : {NUM_WORKERS}")
    print("-" * 60)

    monitor = ClusterMonitor(
        coordinators=[DATA_INSTANCE],
        protocol=Protocol.BOLT,
        storage_info=["vertex_count", "edge_count"],
        metrics_info=[
            "FailedQuery",
            "TransientErrors",
            "GCLatency_us_50p",
            "GCLatency_us_90p",
            "GCLatency_us_99p",
        ],
        interval=2,
    )

    total_start = time.time()
    with monitor:
        setup_graph()
        run_workload()
        verify_results()

    total_elapsed = time.time() - total_start
    print("=" * 60)
    print(f"Completed in {total_elapsed:.1f}s ({total_elapsed / 60:.1f} min)")
    print("Workload completed successfully!")


if __name__ == "__main__":
    main()
