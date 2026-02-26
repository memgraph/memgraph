#!/usr/bin/env python3
"""
Vector workload script for RAG stress testing.
Creates nodes with 1024-dimension vectors in batches using multiprocessing.

Workers write to the MAIN instance using bolt+routing protocol.
Periodically restarts the REPLICA instance to test replication resilience.

The restart function is auto-detected from the deployment's common module.
"""
import random
import sys
import time
from typing import Callable

from cluster_monitor import ClusterMonitor
from ha_common import Protocol, QueryType, execute_and_fetch, execute_query, get_restart_fn, run_parallel

MAIN = "data_1"
COORDINATOR = "coord_1"
REPLICA = "data_2"

SNAPSHOT_RECOVERY_LATENCY_METRIC = "SnapshotRecoveryLatency_us_99p"

BATCH_SIZE = 1000
NUM_BATCHES = 1000
VECTOR_DIMENSIONS = 1024
RESTART_PROBABILITY = 0.05
NUM_WORKERS = 8


def generate_vector(dimensions: int) -> list[float]:
    return [random.uniform(-1.0, 1.0) for _ in range(dimensions)]


def create_indexes() -> None:
    print("Creating indexes...")
    execute_query(
        COORDINATOR, "CREATE INDEX ON :VectorNode;", protocol=Protocol.BOLT_ROUTING, query_type=QueryType.WRITE
    )
    execute_query(
        COORDINATOR, "CREATE INDEX ON :VectorNode(id);", protocol=Protocol.BOLT_ROUTING, query_type=QueryType.WRITE
    )
    print("Indexes created.")


def build_batch_query_tasks(batch_numbers: list[int]) -> list[tuple]:
    query = """
        UNWIND $nodes AS node
        CREATE (:VectorNode {id: node.id, embedding: node.vector})
    """

    tasks = []
    for batch_num in batch_numbers:
        nodes_data = [
            {"id": batch_num * BATCH_SIZE + i, "vector": generate_vector(VECTOR_DIMENSIONS)} for i in range(BATCH_SIZE)
        ]
        tasks.append((COORDINATOR, query, {"nodes": nodes_data}, Protocol.BOLT_ROUTING, QueryType.WRITE, True))

    return tasks


def run_batches_parallel(batch_numbers: list[int]) -> None:
    tasks = build_batch_query_tasks(batch_numbers)
    run_parallel(execute_query, tasks, num_workers=NUM_WORKERS)


def get_metric(instance: str, name: str):
    results = execute_and_fetch(instance, "SHOW METRICS;", protocol=Protocol.BOLT)
    for metric in results:
        if metric.get("name") == name:
            return metric.get("value")
    return None


def show_replica_snapshot_metric() -> None:
    try:
        value_us = get_metric(REPLICA, SNAPSHOT_RECOVERY_LATENCY_METRIC)
        if value_us is not None:
            value_s = value_us / 1_000_000
            print(f"Replica SnapshotRecoveryLatency_s_99p: {value_s:.2f}s")
        else:
            print("SnapshotRecoveryLatency_s_99p: not found")
    except Exception:
        print("SnapshotRecoveryLatency_s_99p: replica not reachable for metrics")


def run_workload(monitor: ClusterMonitor, restart_fn: Callable[[str], None] | None = None) -> None:
    restart_count = 0

    for batch_num in range(NUM_BATCHES):
        print(f"\nBatch {batch_num + 1}/{NUM_BATCHES}")

        run_batches_parallel([batch_num])
        monitor.show_replicas()
        show_replica_snapshot_metric()

        if restart_fn and batch_num < NUM_BATCHES - 1 and random.random() < RESTART_PROBABILITY:
            restart_count += 1
            print(f"\n--- Restarting {REPLICA} (REPLICA) [restart #{restart_count}] ---")
            restart_fn(REPLICA)

    print(f"\nTotal restarts: {restart_count}")


def main():
    restart_fn = get_restart_fn()
    print(f"Workload: {NUM_BATCHES * BATCH_SIZE:,} nodes with {VECTOR_DIMENSIONS}-dimension vectors")
    print(f"Batch size: {BATCH_SIZE:,}, Batches: {NUM_BATCHES}")
    print(f"Workers: {NUM_WORKERS}")
    print(f"Restart probability: {RESTART_PROBABILITY * 100:.1f}%")
    print(f"Restart function: {'provided' if restart_fn else 'none (restarts disabled)'}")
    print("-" * 60)

    monitor = ClusterMonitor(
        coordinator=COORDINATOR,
        show_replicas=True,
        verify_up=False,
        storage_info=["vertex_count", "edge_count", "memory_res"],
        interval=10,
    )

    with monitor:
        create_indexes()
        run_workload(monitor, restart_fn=restart_fn)

    print("-" * 60)
    print("Waiting 30 seconds before final verification...")
    time.sleep(30)

    print("\nFinal replica status:")
    monitor.show_replicas()

    ok = monitor.verify_all_ready() and monitor.verify_instances_up()
    if not ok:
        sys.exit(1)
    print("Workload completed successfully!")


if __name__ == "__main__":
    main()
