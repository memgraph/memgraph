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

# Add docker_ha directory to path for common imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from common import Protocol, QueryType, execute_query, restart_container, run_parallel

# Instance names
MAIN = "data_1"
COORDINATOR = "coord_1"
REPLICA = "data_2"

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
        tasks.append((COORDINATOR, query, {"nodes": nodes_data}, Protocol.BOLT_ROUTING, QueryType.WRITE, False))

    return tasks


def run_batches_parallel(batch_numbers: list[int]) -> None:
    """
    Run multiple batches in parallel using a process pool.

    Args:
        batch_numbers: List of batch numbers to process.
    """
    tasks = build_batch_query_tasks(batch_numbers)
    run_parallel(execute_query, tasks, num_workers=NUM_WORKERS)


def run_workload() -> None:
    """
    Run the vector workload, randomly restarting REPLICA based on probability.
    """
    restart_count = 0

    for batch_num in range(NUM_BATCHES):
        print(f"\nBatch {batch_num + 1}/{NUM_BATCHES}")

        run_batches_parallel([batch_num])

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
    print("Workload completed successfully!")


if __name__ == "__main__":
    main()
