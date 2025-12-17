#!/usr/bin/env python3
"""
Vector workload script for RAG stress testing.
Creates nodes with 1500-dimension vectors in batches.
"""
import random

from neo4j import GraphDatabase

# Connection to data instance (MAIN)
HOST = "127.0.0.1"
PORT = 7687

# Workload configuration
BATCH_SIZE = 1000
NUM_BATCHES = 100
VECTOR_DIMENSIONS = 1500


def generate_vector(dimensions: int) -> list[float]:
    """Generate a random vector with the specified dimensions."""
    return [random.uniform(-1.0, 1.0) for _ in range(dimensions)]


def main():
    uri = f"bolt://{HOST}:{PORT}"
    print(f"Connecting to Memgraph at {uri}")

    driver = GraphDatabase.driver(uri, auth=("", ""))

    try:
        with driver.session() as session:
            # Create indexes
            print("Creating indexes...")
            session.run("CREATE INDEX ON :VectorNode;")
            session.run("CREATE INDEX ON :VectorNode(id);")
            print("Indexes created.")

            print(f"Creating {NUM_BATCHES * BATCH_SIZE} nodes with {VECTOR_DIMENSIONS}-dimension vectors...")
            print(f"Batch size: {BATCH_SIZE}, Number of batches: {NUM_BATCHES}")

            total_created = 0
            for batch_num in range(NUM_BATCHES):
                # Generate batch of nodes with vectors
                nodes_data = []
                for i in range(BATCH_SIZE):
                    node_id = batch_num * BATCH_SIZE + i
                    vector = generate_vector(VECTOR_DIMENSIONS)
                    nodes_data.append({"id": node_id, "vector": vector})

                # Create nodes in batch using UNWIND
                session.run(
                    """
                    UNWIND $nodes AS node
                    CREATE (:VectorNode {id: node.id, embedding: node.vector})
                    """,
                    nodes=nodes_data,
                )

                total_created += BATCH_SIZE
                print(f"Batch {batch_num + 1}/{NUM_BATCHES} completed. Total nodes: {total_created}")

            print(f"Successfully created {total_created} nodes with vectors!")

    finally:
        driver.close()


if __name__ == "__main__":
    main()
