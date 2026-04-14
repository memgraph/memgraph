#!/usr/bin/env python3
"""
High-concurrency random edge ingestion workload.

Stress tests driver lifecycle management by deliberately creating a brand-new
Neo4j driver for every single edge insertion — the driver is never reused
across tasks.  The goal is to observe cluster behaviour (routing table refresh,
connection churn, bolt-level backpressure) under sustained driver open/close
cycles from many concurrent workers.

  Phase 1 – Ingest 100 :Node nodes (id 0–99) via bolt+routing on coord_1.
  Phase 2 – 20 worker processes share 1 000 000 tasks.
             Each task: open driver (pool_size=5) → session.run CREATE edge
             between two random nodes → close driver.

All driver/session management lives entirely in this file; ha_common is only
used by ClusterMonitor for background health reporting.
"""

import logging
import multiprocessing
import os
import random
import sys
import time

from neo4j import GraphDatabase

# Suppress neo4j driver noise so it doesn't drown out workload output.
logging.getLogger("neo4j").setLevel(logging.CRITICAL)

# ── Configuration ─────────────────────────────────────────────────────────────

# coord_1 on the standard docker deployment listens on port 7691.
# Override via STRESS_COORD_URI for other deployments.
COORD_URI: str = os.environ.get("STRESS_COORD_URI", "neo4j://127.0.0.1:7691")
AUTH: tuple[str, str] = ("", "")

# Each ephemeral driver is allowed up to 5 pooled connections.
CONNECTION_POOL_SIZE: int = 5

NUM_NODES: int = 100
NUM_WORKERS: int = 20
TOTAL_EDGES: int = 1_000_000

# Used only by ClusterMonitor — no queries go through ha_common.
COORDINATORS: list[str] = ["coord_1", "coord_2", "coord_3"]

# ── Worker ────────────────────────────────────────────────────────────────────


def add_edge_worker(node_a: int, node_b: int) -> None:
    """
    Open a fresh neo4j:// driver, insert one edge, then close the driver.

    The driver is intentionally never reused across invocations so that the
    test exercises the full driver lifecycle under concurrent load.
    """
    driver = GraphDatabase.driver(
        COORD_URI,
        auth=AUTH,
        max_connection_pool_size=CONNECTION_POOL_SIZE,
    )
    try:
        with driver.session() as session:
            session.run(
                "MATCH (a:Node {id: $a}), (b:Node {id: $b}) " "CREATE (a)-[:CONNECTED]->(b)",
                {"a": node_a, "b": node_b},
            )
    finally:
        driver.close()


# ── Setup ─────────────────────────────────────────────────────────────────────


def create_index() -> None:
    print("Creating index on :Node(id)...")
    driver = GraphDatabase.driver(COORD_URI, auth=AUTH)
    try:
        with driver.session() as session:
            session.run("CREATE INDEX ON :Node(id);")
    finally:
        driver.close()
    print("  Index created.")


def ingest_nodes() -> None:
    """Phase 1: create NUM_NODES nodes with sequential ids."""
    print(f"\nPhase 1: Ingesting {NUM_NODES} nodes...")
    t0 = time.time()
    driver = GraphDatabase.driver(COORD_URI, auth=AUTH)
    try:
        with driver.session() as session:
            session.run(
                "UNWIND range(0, $n - 1) AS i CREATE (:Node {id: i})",
                {"n": NUM_NODES},
            )
            count = session.run("MATCH (n:Node) RETURN count(n) AS cnt").single()["cnt"]
    finally:
        driver.close()
    print(f"  {count} nodes ingested in {time.time() - t0:.1f}s.")


# ── Task generation ───────────────────────────────────────────────────────────


def generate_tasks(rng: random.Random) -> list[tuple[int, int]]:
    """Return TOTAL_EDGES random (node_a, node_b) pairs."""
    print(f"\nGenerating {TOTAL_EDGES:,} random edge tasks (seed=42)...")
    t0 = time.time()
    tasks = [(rng.randint(0, NUM_NODES - 1), rng.randint(0, NUM_NODES - 1)) for _ in range(TOTAL_EDGES)]
    print(f"  Tasks ready in {time.time() - t0:.1f}s.")
    return tasks


# ── Edge ingestion ────────────────────────────────────────────────────────────


def run_edge_ingestion(tasks: list[tuple[int, int]]) -> None:
    """Phase 2: distribute tasks across NUM_WORKERS processes."""
    print(f"\nPhase 2: Ingesting {TOTAL_EDGES:,} edges with {NUM_WORKERS} workers...")
    print(f"  Driver pool size per task : {CONNECTION_POOL_SIZE}")
    print(f"  Driver reuse              : none (new driver per task)")
    print()

    t0 = time.time()
    with multiprocessing.Pool(processes=NUM_WORKERS) as pool:
        pool.starmap(add_edge_worker, tasks, chunksize=500)

    elapsed = time.time() - t0
    rate = TOTAL_EDGES / elapsed if elapsed > 0 else 0
    print(f"\n  Completed: {TOTAL_EDGES:,} edges in {elapsed:.1f}s ({rate:.0f} edges/s)")


# ── Verification ──────────────────────────────────────────────────────────────


def verify(expected_nodes: int, expected_edges: int) -> None:
    print("\nVerifying final counts...")
    driver = GraphDatabase.driver(COORD_URI, auth=AUTH)
    try:
        with driver.session() as session:
            node_count = session.run("MATCH (n:Node) RETURN count(n) AS cnt").single()["cnt"]
            edge_count = session.run("MATCH ()-[r:CONNECTED]->() RETURN count(r) AS cnt").single()["cnt"]
    finally:
        driver.close()

    print(f"  Nodes: {node_count:,} (expected {expected_nodes})")
    print(f"  Edges: {edge_count:,} (expected {expected_edges:,})")

    if node_count != expected_nodes:
        raise RuntimeError(f"Node count mismatch: {node_count} != {expected_nodes}")
    if edge_count != expected_edges:
        raise RuntimeError(f"Edge count mismatch: {edge_count} != {expected_edges}")
    print("  Counts verified OK.")


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    print("=" * 60)
    print("High-Concurrency Random Edge Ingestion Workload")
    print("=" * 60)
    print(f"Nodes:             {NUM_NODES}")
    print(f"Workers:           {NUM_WORKERS}")
    print(f"Total edges:       {TOTAL_EDGES:,}")
    print(f"Coordinator URI:   {COORD_URI}")
    print(f"Pool size/driver:  {CONNECTION_POOL_SIZE}")
    print("-" * 60)

    # ClusterMonitor uses ha_common internally for health queries only.
    # It is optional; if ha_common is not configured the workload still runs.
    monitor = None
    try:
        from cluster_monitor import ClusterMonitor

        monitor = ClusterMonitor(
            coordinators=COORDINATORS,
            show_replicas=True,
            verify_up=True,
            storage_info=["vertex_count", "edge_count", "memory_res"],
            interval=10,
        )
    except Exception as exc:
        print(f"[warn] ClusterMonitor unavailable ({exc}); continuing without monitoring.")

    # Pre-generate all tasks in the main process so workers only receive plain ints.
    rng = random.Random(42)
    tasks = generate_tasks(rng)

    total_start = time.time()

    def run_all() -> None:
        create_index()
        ingest_nodes()
        run_edge_ingestion(tasks)
        verify(NUM_NODES, TOTAL_EDGES)

    if monitor is not None:
        with monitor:
            run_all()
    else:
        run_all()

    total_elapsed = time.time() - total_start
    print("-" * 60)
    print(f"Total time: {total_elapsed:.1f}s ({total_elapsed / 60:.1f} min)")
    print("Workload completed successfully!")

    if monitor is not None:
        print("\nFinal replica status:")
        monitor.show_replicas()
        if not (monitor.verify_all_ready() and monitor.verify_instances_up()):
            sys.exit(1)


if __name__ == "__main__":
    main()
