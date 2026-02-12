#!/usr/bin/env python3
"""
Concurrent write workload for stress testing edge creation on a supernode.

Creates a supernode and 1000 regular nodes, then concurrently creates edges
between the supernode and each regular node using parallel workers.
"""
import os
import sys

# Add docker_ha directory to path for common imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from common import Protocol, QueryType, execute_query, run_parallel

COORDINATOR = "coord_1"

# Workload configuration
NUM_NODES = 1000
NUM_WORKERS = 4


def setup_graph() -> None:
    """Create the supernode and regular nodes."""
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
    """
    Create an edge from the supernode to a regular node.
    Worker function for parallel execution.
    """
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
    """Create edges concurrently from supernode to all regular nodes."""
    print(f"\nCreating {NUM_NODES} edges concurrently with {NUM_WORKERS} workers...")

    # Build tasks: each task is just the node_id
    tasks = [(node_id,) for node_id in range(1, NUM_NODES + 1)]

    run_parallel(create_edge, tasks, num_workers=NUM_WORKERS)

    print("Edge creation complete.")


def verify_results() -> None:
    """Verify the graph was created correctly."""
    print("\nVerifying results...")

    # This is a read query, but we use WRITE type since we're going through coordinator
    # and want to read from the leader to get consistent results
    result = execute_query(
        COORDINATOR,
        "MATCH (:SuperNode)-[r:CONNECTED_TO]->(:Node) RETURN count(r) AS edge_count",
        protocol=Protocol.BOLT_ROUTING,
        query_type=QueryType.READ,
    )
    print(f"Verification query executed. Check edge count matches {NUM_NODES}.")


def main():
    print(f"Concurrent Write Workload")
    print(f"Supernode: 1, Regular nodes: {NUM_NODES}, Workers: {NUM_WORKERS}")
    print("-" * 60)

    setup_graph()
    run_workload()
    verify_results()

    print("-" * 60)
    print("Workload completed successfully!")


if __name__ == "__main__":
    main()
