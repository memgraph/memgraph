#!/usr/bin/env python3
"""
Vector workload script for RAG stress testing.
Creates nodes with 1500-dimension vectors in batches using multiprocessing.

Workers write to the MAIN instance (data_1, port 7687).
Periodically restarts the REPLICA instance (data_2, port 7688) to test replication resilience.
"""
import multiprocessing
import os
import random
import subprocess

from neo4j import GraphDatabase

# Connection settings (MAIN instance)
HOST = "127.0.0.1"
PORT = 7687  # data_1 (MAIN)
URI = f"bolt://{HOST}:{PORT}"

# Workload configuration
BATCH_SIZE = 10000
NUM_BATCHES = 100
VECTOR_DIMENSIONS = 1500
RESTART_EVERY_N_BATCHES = 10
NUM_WORKERS = 4

# Instance to restart (REPLICA)
RESTART_INSTANCE = "data_2"  # port 7688

# Deployment script path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEPLOYMENT_SCRIPT = os.path.join(SCRIPT_DIR, "..", "..", "configurations", "deployments", "docker_ha.sh")


def restart_container(instance_name: str) -> None:
    """
    Restart a specific container using the deployment script.

    Args:
        instance_name: Name of the instance to restart (e.g., "data_1", "coord_2")
    """
    print(f"Restarting instance: {instance_name}")
    result = subprocess.run(
        [DEPLOYMENT_SCRIPT, "restart", instance_name],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise Exception(f"Failed to restart {instance_name}: {result.stderr}")
    print(f"Instance {instance_name} restart triggered")


def create_driver():
    """Create a new database driver."""
    return GraphDatabase.driver(URI, auth=("", ""))


def generate_vector(dimensions: int) -> list[float]:
    """Generate a random vector with the specified dimensions."""
    return [random.uniform(-1.0, 1.0) for _ in range(dimensions)]


def create_indexes() -> None:
    """Create necessary indexes."""
    print("Creating indexes...")
    driver = create_driver()
    try:
        with driver.session() as session:
            session.run("CREATE INDEX ON :VectorNode;")
            session.run("CREATE INDEX ON :VectorNode(id);")
        print("Indexes created.")
    finally:
        driver.close()


def process_batch(batch_num: int) -> tuple[int, int]:
    """
    Process a single batch - called by worker processes.

    Args:
        batch_num: The batch number to process.

    Returns:
        Tuple of (batch_num, nodes_created).
    """
    driver = create_driver()
    try:
        nodes_data = [
            {"id": batch_num * BATCH_SIZE + i, "vector": generate_vector(VECTOR_DIMENSIONS)} for i in range(BATCH_SIZE)
        ]

        with driver.session() as session:
            session.run(
                """
                UNWIND $nodes AS node
                CREATE (:VectorNode {id: node.id, embedding: node.vector})
                """,
                nodes=nodes_data,
            )

        return (batch_num, BATCH_SIZE)
    finally:
        driver.close()


def run_batches_parallel(batch_numbers: list[int]) -> int:
    """
    Run multiple batches in parallel using a process pool.

    Args:
        batch_numbers: List of batch numbers to process.

    Returns:
        Total nodes created.
    """
    with multiprocessing.Pool(processes=NUM_WORKERS) as pool:
        results = pool.map(process_batch, batch_numbers)

    total = sum(count for _, count in results)
    return total


def main():
    num_chunks = NUM_BATCHES // RESTART_EVERY_N_BATCHES  # 10 chunks of 10 batches

    print(f"Connecting to Memgraph MAIN at {URI}")
    print(f"Workload: {NUM_BATCHES * BATCH_SIZE:,} nodes with {VECTOR_DIMENSIONS}-dimension vectors")
    print(f"Batch size: {BATCH_SIZE:,}, Batches per chunk: {RESTART_EVERY_N_BATCHES}, Chunks: {num_chunks}")
    print(f"Workers: {NUM_WORKERS}")
    print(f"Restart {RESTART_INSTANCE} (REPLICA) after each chunk")
    print("-" * 60)

    create_indexes()

    total_created = 0

    for chunk in range(num_chunks):
        start_batch = chunk * RESTART_EVERY_N_BATCHES
        batch_numbers = list(range(start_batch, start_batch + RESTART_EVERY_N_BATCHES))

        print(f"\nChunk {chunk + 1}/{num_chunks}: batches {start_batch + 1}-{start_batch + RESTART_EVERY_N_BATCHES}")

        nodes_created = run_batches_parallel(batch_numbers)
        total_created += nodes_created

        print(f"Chunk {chunk + 1} completed. Total nodes: {total_created:,}")

        # Restart REPLICA after each chunk (except the last)
        if chunk < num_chunks - 1:
            print(f"\n--- Restarting {RESTART_INSTANCE} (REPLICA) ---")
            restart_container(RESTART_INSTANCE)

    print("-" * 60)
    print(f"Successfully created {total_created:,} nodes with vectors!")


if __name__ == "__main__":
    main()
