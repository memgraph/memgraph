#!/usr/bin/env python3
"""
Dropping databases workload.

Repeats the following cycle 10 times:
1) Creates 30 tenant databases, each with a vector index.
2) Ingests ~1,000,000 nodes per database in parallel (4 workers):
   - One supernode connected to all others via edges.
   - Every node has a vector embedding.
3) Drops all tenant databases via cleanup().

All writes go through a coordinator with bolt+routing.
No instance restarts.
"""
import random
import sys

from cluster_monitor import ClusterMonitor
from ha_common import Protocol, QueryType, cleanup, execute_and_fetch, execute_query, run_parallel

COORDINATOR = "coord_1"
COORDINATORS = ["coord_1", "coord_2", "coord_3"]

NUM_TENANTS = 30
DATABASES = [f"tenant_{i:02d}" for i in range(NUM_TENANTS)]

NUM_NODES = 1_000_000
VECTOR_DIMENSIONS = 128
BATCH_SIZE = 1000
NUM_WORKERS = 4

NUM_ROUNDS = 10


def generate_vector(dimensions: int) -> list[float]:
    return [random.uniform(-1.0, 1.0) for _ in range(dimensions)]


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------


def create_databases() -> None:
    print("Creating tenant databases...")
    for db_name in DATABASES:
        execute_query(
            COORDINATOR, f"CREATE DATABASE {db_name};", protocol=Protocol.BOLT_ROUTING, query_type=QueryType.WRITE
        )
    print(f"  {NUM_TENANTS} databases created.")


def create_vector_indices() -> None:
    print("Creating vector indices...")
    for db_name in DATABASES:
        execute_query(
            COORDINATOR,
            f"CREATE VECTOR INDEX vec_idx ON :Node(embedding) "
            f'WITH CONFIG {{"dimension": {VECTOR_DIMENSIONS}, "capacity": {NUM_NODES + 1}}};',
            protocol=Protocol.BOLT_ROUTING,
            query_type=QueryType.WRITE,
            database=db_name,
        )
    print(f"  Vector indices created in {NUM_TENANTS} databases.")


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------


def create_supernode(db_name: str) -> None:
    """Create the single supernode (id=0) that all other nodes connect to."""
    execute_query(
        COORDINATOR,
        "CREATE (:Node:Supernode {id: 0, embedding: $embedding})",
        params={"embedding": generate_vector(VECTOR_DIMENSIONS)},
        protocol=Protocol.BOLT_ROUTING,
        query_type=QueryType.WRITE,
        apply_retry_mechanism=True,
        database=db_name,
    )


def ingest_batch(batch_start: int, batch_size: int, db_name: str) -> None:
    """Create a batch of nodes and connect each to the supernode."""
    nodes_data = [
        {
            "id": batch_start + i,
            "embedding": generate_vector(VECTOR_DIMENSIONS),
        }
        for i in range(batch_size)
    ]
    execute_query(
        COORDINATOR,
        """
        UNWIND $nodes AS n
        CREATE (node:Node {id: n.id, embedding: n.embedding})
        WITH node
        MATCH (super:Supernode {id: 0})
        CREATE (node)-[:CONNECTED_TO]->(super)
        """,
        params={"nodes": nodes_data},
        protocol=Protocol.BOLT_ROUTING,
        query_type=QueryType.WRITE,
        apply_retry_mechanism=True,
        database=db_name,
    )


def ingest_for_database(db_name: str) -> None:
    """Ingest all nodes for a single database using parallel workers."""
    create_supernode(db_name)

    # Nodes 1..NUM_NODES (supernode is 0)
    num_batches = NUM_NODES // BATCH_SIZE
    tasks = [(batch_num * BATCH_SIZE + 1, BATCH_SIZE, db_name) for batch_num in range(num_batches)]

    print(f"  [{db_name}] Ingesting {NUM_NODES:,} nodes in {num_batches} batches ({NUM_WORKERS} workers)...")
    run_parallel(ingest_batch, tasks, num_workers=NUM_WORKERS)
    print(f"  [{db_name}] Done.")


def run_ingestion() -> None:
    total = NUM_NODES * NUM_TENANTS
    print(f"Ingesting {total:,} nodes total ({NUM_NODES:,} per database x {NUM_TENANTS} databases)...")
    for db_name in DATABASES:
        ingest_for_database(db_name)
    print(f"All {total:,} nodes ingested.")


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


def verify_node_counts() -> bool:
    print("Verifying node counts...")
    ok = True
    for db_name in DATABASES:
        rows = execute_and_fetch(
            COORDINATOR,
            "MATCH (n:Node) RETURN count(n) AS cnt",
            protocol=Protocol.BOLT_ROUTING,
            database=db_name,
        )
        count = rows[0]["cnt"] if rows else 0
        expected = NUM_NODES + 1  # +1 for supernode
        if count != expected:
            print(f"  ERROR [{db_name}]: expected {expected:,}, got {count:,}")
            ok = False
        else:
            print(f"  OK [{db_name}]: {count:,} nodes")
    return ok


def verify_databases_dropped() -> bool:
    print("Verifying all tenant databases were dropped...")
    rows = execute_and_fetch(COORDINATOR, "SHOW DATABASES;", protocol=Protocol.BOLT_ROUTING)
    existing = {next(iter(row.values())) for row in rows}
    remaining = set(DATABASES) & existing
    if remaining:
        print(f"  ERROR: databases still exist: {remaining}")
        return False
    print("  OK: all tenant databases dropped.")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    print("=" * 60)
    print("Dropping Databases Workload")
    print("=" * 60)
    print(f"Tenants: {NUM_TENANTS}")
    print(f"Nodes per database: {NUM_NODES:,} + 1 supernode")
    print(f"Vector dimensions: {VECTOR_DIMENSIONS}")
    print(f"Rounds: {NUM_ROUNDS}")
    print("-" * 60)

    monitor = ClusterMonitor(
        coordinators=COORDINATORS,
        show_replicas=True,
        verify_up=True,
        interval=5,
    )

    for round_num in range(1, NUM_ROUNDS + 1):
        print(f"\n{'=' * 60}")
        print(f"ROUND {round_num}/{NUM_ROUNDS}")
        print(f"{'=' * 60}")

        # Phase 1: Create and ingest
        print(f"\n[Round {round_num}] Phase 1: Create databases, indices, and ingest nodes")
        with monitor:
            create_databases()
            create_vector_indices()
            run_ingestion()

        # Phase 2: Verify
        print(f"\n[Round {round_num}] Phase 2: Verify node counts")
        if not verify_node_counts():
            print("ERROR: Node count verification failed!")
            sys.exit(1)

        # Phase 3: Drop everything
        print(f"\n[Round {round_num}] Phase 3: Drop all databases")
        cleanup()

        if not verify_databases_dropped():
            print("ERROR: Database drop verification failed!")
            sys.exit(1)

        print(f"\n[Round {round_num}] Complete.")

    print("\nFinal cluster status:")
    monitor.show_replicas()

    if not monitor.verify_all_ready() or not monitor.verify_instances_up():
        print("ERROR: Cluster is not healthy!")
        sys.exit(1)

    print("\nWorkload completed successfully!")


if __name__ == "__main__":
    try:
        main()
    finally:
        cleanup()
