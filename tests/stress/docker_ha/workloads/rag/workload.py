#!/usr/bin/env python3
"""
Vector workload script for RAG stress testing on Docker.
Creates nodes with 1500-dimension vectors in batches using multiprocessing.

Workers write to the MAIN instance using bolt+routing protocol.
Periodically restarts the REPLICA instance to test replication resilience.
"""
import os
import random
import sys
import time

# Add docker_ha directory to path for common imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from common import Protocol, QueryType, execute_and_fetch, execute_query, restart_container, run_parallel

# Instance names
MAIN = "data_1"
COORDINATOR = "coord_1"
REPLICA = "data_2"

# Queries
SHOW_REPLICAS_QUERY = "SHOW REPLICAS;"
SHOW_METRICS_QUERY = "SHOW METRICS;"

# Metrics
SNAPSHOT_RECOVERY_LATENCY_METRIC = "SnapshotRecoveryLatency_us_99p"

# Workload configuration
BATCH_SIZE = 1000
NUM_BATCHES = 1000  # Number of nodes is NUM_BATCHES * BATCH_SIZE
VECTOR_DIMENSIONS = 1024
RESTART_PROBABILITY = 0.05
NUM_WORKERS = 8


def generate_vector(dimensions: int) -> list[float]:
    """Generate a random vector with the specified dimensions."""
    return [random.uniform(-1.0, 1.0) for _ in range(dimensions)]


def create_indexes() -> None:
    """Create necessary indexes."""
    print("Creating indexes...")
    execute_query(
        COORDINATOR, "CREATE INDEX ON :VectorNode;", protocol=Protocol.BOLT_ROUTING, query_type=QueryType.WRITE
    )
    execute_query(
        COORDINATOR, "CREATE INDEX ON :VectorNode(id);", protocol=Protocol.BOLT_ROUTING, query_type=QueryType.WRITE
    )
    print("Indexes created.")


def build_batch_query_tasks(batch_numbers: list[int]) -> list[tuple]:
    """
    Build a list of query tasks for parallel execution.

    Args:
        batch_numbers: List of batch numbers to process.

    Returns:
        List of (instance_name, query, params, protocol, query_type, retries) tuples.
    """
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
    """
    Run multiple batches in parallel using a process pool.

    Args:
        batch_numbers: List of batch numbers to process.
    """
    tasks = build_batch_query_tasks(batch_numbers)
    run_parallel(execute_query, tasks, num_workers=NUM_WORKERS)


def show_replicas() -> None:
    """Print the current replica status in CSV format."""
    results = execute_and_fetch(COORDINATOR, SHOW_REPLICAS_QUERY, protocol=Protocol.BOLT_ROUTING)
    if results:
        headers = list(results[0].keys())
        print(",".join(headers))
        for replica in results:
            values = [str(replica[h]) for h in headers]
            print(",".join(values))


def get_metric(instance: str, name: str):
    """
    Get a metric value by name from an instance.

    Args:
        instance: Instance name to query.
        name: Metric name to find.

    Returns:
        Metric value if found, None otherwise.

    Raises:
        Exception if instance is not reachable.
    """
    results = execute_and_fetch(instance, SHOW_METRICS_QUERY, protocol=Protocol.BOLT)
    for metric in results:
        if metric.get("name") == name:
            return metric.get("value")
    return None


def show_replica_snapshot_metric() -> None:
    """Print SnapshotRecoveryLatency_us_99p from replica metrics (converted to seconds)."""
    try:
        value_us = get_metric(REPLICA, SNAPSHOT_RECOVERY_LATENCY_METRIC)
        if value_us is not None:
            value_s = value_us / 1_000_000
            print(f"Replica SnapshotRecoveryLatency_s_99p: {value_s:.2f}s")
        else:
            print("SnapshotRecoveryLatency_s_99p: not found")
    except Exception:
        print("SnapshotRecoveryLatency_s_99p: replica not reachable for metrics")


def verify_replicas_ready() -> bool:
    """
    Verify that all databases in all replicas have 'ready' status.

    Returns:
        True if all databases are ready, False otherwise.
    """
    results = execute_and_fetch(COORDINATOR, SHOW_REPLICAS_QUERY, protocol=Protocol.BOLT_ROUTING)
    all_ready = True

    for replica in results:
        name = replica.get("name", "unknown")
        data_info = replica.get("data_info", {})

        for db_name, db_status in data_info.items():
            status = db_status.get("status", "unknown")
            if status != "ready":
                print(f"WARN: Replica {name}, database {db_name} status is '{status}', expected 'ready'")
                all_ready = False

    return all_ready


def run_workload() -> None:
    """
    Run the vector workload, randomly restarting REPLICA based on probability.
    """
    restart_count = 0

    for batch_num in range(NUM_BATCHES):
        print(f"\nBatch {batch_num + 1}/{NUM_BATCHES}")

        run_batches_parallel([batch_num])
        show_replicas()
        show_replica_snapshot_metric()

        # Randomly restart REPLICA based on probability (except after last batch)
        if batch_num < NUM_BATCHES - 1 and random.random() < RESTART_PROBABILITY:
            restart_count += 1
            print(f"\n--- Restarting {REPLICA} (REPLICA) [restart #{restart_count}] ---")
            restart_container(REPLICA)

    print(f"\nTotal restarts: {restart_count}")


def main():
    print(f"Workload: {NUM_BATCHES * BATCH_SIZE:,} nodes with {VECTOR_DIMENSIONS}-dimension vectors")
    print(f"Batch size: {BATCH_SIZE:,}, Batches: {NUM_BATCHES}")
    print(f"Workers: {NUM_WORKERS}")
    print(f"Restart probability: {RESTART_PROBABILITY * 100:.1f}%")
    print("-" * 60)

    create_indexes()
    run_workload()

    print("-" * 60)
    print("Waiting 30 seconds before final verification...")
    time.sleep(30)

    print("\nFinal replica status:")
    show_replicas()

    if verify_replicas_ready():
        print("\nAll replicas are in sync!")
        print("Workload completed successfully!")
    else:
        print("\nERROR: Not all replicas are in sync!")
        sys.exit(1)


if __name__ == "__main__":
    main()
