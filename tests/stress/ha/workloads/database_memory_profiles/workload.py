#!/usr/bin/env python3
"""
Database memory profiles workload.

Each iteration:
  1. Creates a tenant memory profile (1 GB) and 10 tenant databases bound to it.
  2. Seeds each database with 100 nodes carrying properties.
  3. Runs 4 workers per database that concurrently add edges between
     arbitrarily picked seeded nodes.
  4. Verifies cluster health and that node/edge counts agree across all data
     instances for every database (replication consistency).
  5. Drops every database and the profile so the next iteration starts clean.

Workers swallow query failures: hitting the per-database memory limit or
transient errors is expected — the test verifies the cluster stays healthy
and replicas stay consistent under that pressure. Repeats NUM_ITERATIONS times.
"""
import random
import sys
import time

import ha_common
from cluster_monitor import ClusterMonitor
from ha_common import Protocol, QueryType, cleanup, execute_and_fetch, execute_query, run_parallel

COORDINATOR = "coord_1"
COORDINATORS = ["coord_1", "coord_2", "coord_3"]

PROFILE_NAME = "stress_mem_1gb"
PROFILE_MEMORY_MB = 1024

NUM_DATABASES = 10
DB_PREFIX = "stress_db_"
NODES_PER_DB = 100
WORKERS_PER_DB = 4
NUM_ITERATIONS = 10
EDGES_PER_WORKER = 200

POST_WORKLOAD_QUIESCE_SEC = 10


def db_name(idx: int) -> str:
    return f"{DB_PREFIX}{idx}"


def setup_iteration(iteration: int) -> list[str]:
    print(
        f"\nIteration {iteration + 1}/{NUM_ITERATIONS}: creating profile "
        f"{PROFILE_NAME!r} ({PROFILE_MEMORY_MB} MB) and {NUM_DATABASES} databases..."
    )
    execute_query(
        COORDINATOR,
        f"CREATE TENANT PROFILE {PROFILE_NAME} LIMIT memory_limit {PROFILE_MEMORY_MB} MB",
        protocol=Protocol.BOLT_ROUTING,
        query_type=QueryType.WRITE,
    )

    databases = [db_name(i) for i in range(NUM_DATABASES)]
    for db in databases:
        execute_query(
            COORDINATOR,
            f"CREATE DATABASE {db}",
            protocol=Protocol.BOLT_ROUTING,
            query_type=QueryType.WRITE,
        )
        execute_query(
            COORDINATOR,
            f"SET TENANT PROFILE ON DATABASE {db} TO {PROFILE_NAME}",
            protocol=Protocol.BOLT_ROUTING,
            query_type=QueryType.WRITE,
        )

    print(f"Seeding {NODES_PER_DB} nodes into each of {len(databases)} databases...")
    for db in databases:
        execute_query(
            COORDINATOR,
            """
UNWIND range(0, $count - 1) AS i
CREATE (:Node {id: i, name: 'node_' + toString(i), payload: 'p_' + toString(i)})
""",
            params={"count": NODES_PER_DB},
            protocol=Protocol.BOLT_ROUTING,
            query_type=QueryType.WRITE,
            database=db,
            apply_retry_mechanism=True,
        )
    return databases


def teardown_iteration(iteration: int) -> None:
    print(f"Iteration {iteration + 1}/{NUM_ITERATIONS}: wiping cluster " f"and dropping profile {PROFILE_NAME!r}...")
    cleanup(coordinator=COORDINATOR)
    execute_query(
        COORDINATOR,
        f"DROP TENANT PROFILE {PROFILE_NAME}",
        protocol=Protocol.BOLT_ROUTING,
        query_type=QueryType.WRITE,
    )


def edge_worker(worker_id: int, iteration: int, database: str) -> dict:
    """Add EDGES_PER_WORKER edges between arbitrarily picked seeded nodes."""
    rng = random.Random(hash((worker_id, iteration, database)) & 0xFFFFFFFF)
    succeeded = 0
    failed = 0
    t0 = time.time()

    for _ in range(EDGES_PER_WORKER):
        a = rng.randrange(NODES_PER_DB)
        b = rng.randrange(NODES_PER_DB)
        try:
            execute_query(
                COORDINATOR,
                "MATCH (a:Node {id: $a}), (b:Node {id: $b}) CREATE (a)-[:LINKS]->(b)",
                params={"a": a, "b": b},
                protocol=Protocol.BOLT_ROUTING,
                query_type=QueryType.WRITE,
                database=database,
                apply_retry_mechanism=False,
            )
            succeeded += 1
        except Exception:
            failed += 1

    elapsed = time.time() - t0
    return {
        "worker_id": worker_id,
        "database": database,
        "iteration": iteration,
        "succeeded": succeeded,
        "failed": failed,
        "elapsed": elapsed,
    }


def run_iteration(databases: list[str], iteration: int) -> None:
    tasks = [(w, iteration, db) for db in databases for w in range(WORKERS_PER_DB)]
    total_workers = len(tasks)
    print(
        f"\n--- Iteration {iteration + 1}/{NUM_ITERATIONS}: {total_workers} edge workers "
        f"({WORKERS_PER_DB}/db × {len(databases)} dbs) ---"
    )

    t0 = time.time()
    results = run_parallel(edge_worker, tasks, num_workers=total_workers)
    elapsed = time.time() - t0

    succeeded = sum(r["succeeded"] for r in results)
    failed = sum(r["failed"] for r in results)
    print(
        f"Iteration {iteration + 1} edge workers done in {elapsed:.1f}s — "
        f"{succeeded} ok, {failed} failed (failures expected under memory pressure)."
    )


def _data_instances() -> list[str]:
    return [n for n in ha_common.get_instance_names() if n.startswith("data_")]


def _read_count(instance: str, database: str, query: str) -> int | None:
    try:
        rows = execute_and_fetch(
            instance,
            query,
            protocol=Protocol.BOLT,
            database=database,
        )
        return rows[0]["c"] if rows else None
    except Exception as e:
        print(f"  WARN: failed to read counts from {instance}/{database}: {e}")
        return None


def verify_replication_consistency(databases: list[str], iteration: int) -> bool:
    instances = _data_instances()
    if len(instances) < 2:
        print(f"  Only {len(instances)} data instance(s) — skipping cross-replica check.")
        return True

    all_ok = True
    for db in databases:
        per_instance: dict[str, tuple[int | None, int | None]] = {}
        for inst in instances:
            nodes = _read_count(inst, db, "MATCH (n) RETURN count(n) AS c")
            edges = _read_count(inst, db, "MATCH ()-[r]->() RETURN count(r) AS c")
            per_instance[inst] = (nodes, edges)

        readable = {k: v for k, v in per_instance.items() if v[0] is not None and v[1] is not None}
        if not readable:
            print(f"  ERROR: could not read counts for {db} from any data instance.")
            all_ok = False
            continue

        unique = set(readable.values())
        if len(unique) > 1:
            print(f"  ERROR: count mismatch on {db} (iter {iteration + 1}): {readable}")
            all_ok = False
        else:
            (nc, ec) = next(iter(unique))
            print(f"  [{db}] OK — nodes={nc}, edges={ec} across {len(readable)}/{len(instances)} instances")

    return all_ok


def run_workload(monitor: ClusterMonitor) -> None:
    for iteration in range(NUM_ITERATIONS):
        databases = setup_iteration(iteration)
        run_iteration(databases, iteration)

        time.sleep(POST_WORKLOAD_QUIESCE_SEC)
        monitor.show_replicas()

        cluster_ok = monitor.verify_all_ready() and monitor.verify_instances_up()
        if not cluster_ok:
            print(f"ERROR: cluster unhealthy after iteration {iteration + 1}")
            sys.exit(1)

        if not verify_replication_consistency(databases, iteration):
            print(f"ERROR: replication consistency check failed at iteration {iteration + 1}")
            sys.exit(1)

        teardown_iteration(iteration)


def main():
    print("=" * 60)
    print("Database Memory Profiles Workload")
    print("=" * 60)
    print(f"Databases         : {NUM_DATABASES}  ({DB_PREFIX}0..{DB_PREFIX}{NUM_DATABASES - 1})")
    print(f"Profile           : {PROFILE_NAME}  (memory_limit {PROFILE_MEMORY_MB} MB)")
    print(f"Nodes per DB      : {NODES_PER_DB}")
    print(f"Workers per DB    : {WORKERS_PER_DB}")
    print(f"Edges per worker  : {EDGES_PER_WORKER}")
    print(f"Iterations        : {NUM_ITERATIONS}")
    print("-" * 60)

    monitor = ClusterMonitor(
        coordinators=COORDINATORS,
        show_replicas=True,
        verify_up=True,
        storage_info=["vertex_count", "edge_count", "memory_res", "tenant_memory_limit"],
        metrics_info=["FailedQuery", "TransientErrors"],
        interval=5,
    )

    total_start = time.time()
    try:
        with monitor:
            run_workload(monitor)

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
