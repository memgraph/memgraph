#!/usr/bin/env python3
"""
Database memory profiles workload.

Each iteration creates a tenant memory profile (1 GB) and 10 tenant databases
bound to that profile, then runs 4 workers per database doing randomized
write queries (bulk CREATE, MERGE edges, SET properties, DETACH DELETE).
Workers swallow query failures: hitting the per-database memory limit or
transient errors is expected — the test verifies the cluster stays healthy
and replication stays consistent under that pressure.

After workers finish, for every database, node and edge counts are read
directly from each data instance. All instances must agree (replication
consistency). The iteration then drops every database and the profile, so
each iteration starts from a clean slate. Repeats NUM_ITERATIONS times.
"""
import random
import sys
import time

import ha_common
from cluster_monitor import ClusterMonitor
from ha_common import (
    Protocol,
    QueryType,
    cleanup,
    execute_and_fetch,
    execute_query,
    execute_with_manual_retries,
    run_parallel,
)

COORDINATOR = "coord_1"
COORDINATORS = ["coord_1", "coord_2", "coord_3"]

PROFILE_NAME = "stress_mem_1gb"
PROFILE_MEMORY_MB = 1024

NUM_DATABASES = 10
DB_PREFIX = "stress_db_"
WORKERS_PER_DB = 4
NUM_ITERATIONS = 10
OPS_PER_WORKER = 30

POST_WORKLOAD_QUIESCE_SEC = 10


def db_name(idx: int) -> str:
    return f"{DB_PREFIX}{idx}"


def setup_iteration(iteration: int) -> list[str]:
    print(
        f"\nIteration {iteration + 1}/{NUM_ITERATIONS}: creating profile "
        f"{PROFILE_NAME!r} ({PROFILE_MEMORY_MB} MB) and {NUM_DATABASES} databases..."
    )
    execute_with_manual_retries(
        COORDINATOR,
        f"CREATE TENANT PROFILE {PROFILE_NAME} LIMIT memory_limit {PROFILE_MEMORY_MB} MB",
        protocol=Protocol.BOLT_ROUTING,
    )

    databases = [db_name(i) for i in range(NUM_DATABASES)]
    for db in databases:
        execute_with_manual_retries(
            COORDINATOR,
            f"CREATE DATABASE {db}",
            protocol=Protocol.BOLT_ROUTING,
        )
        execute_with_manual_retries(
            COORDINATOR,
            f"SET TENANT PROFILE ON DATABASE {db} TO {PROFILE_NAME}",
            protocol=Protocol.BOLT_ROUTING,
        )
    return databases


def teardown_iteration(databases: list[str], iteration: int) -> None:
    print(
        f"Iteration {iteration + 1}/{NUM_ITERATIONS}: dropping "
        f"{len(databases)} databases and profile {PROFILE_NAME!r}..."
    )
    for db in databases:
        execute_with_manual_retries(
            COORDINATOR,
            f"DROP DATABASE {db} FORCE",
            protocol=Protocol.BOLT_ROUTING,
        )
    execute_with_manual_retries(
        COORDINATOR,
        f"DROP TENANT PROFILE {PROFILE_NAME}",
        protocol=Protocol.BOLT_ROUTING,
    )


_OPS = ("bulk_create", "merge_edges", "set_props", "detach_delete")


def _bulk_create_query(rng: random.Random, worker_id: int, iteration: int) -> tuple[str, dict]:
    size = rng.randint(200, 2000)
    payload_len = rng.randint(8, 128)
    return (
        """
UNWIND range(1, $size) AS i
CREATE (:Stress {worker: $worker, iter: $iter, i: i,
                 v: 'x' + toString(i) + '-' + $pad})
""",
        {
            "size": size,
            "worker": worker_id,
            "iter": iteration,
            "pad": "p" * payload_len,
        },
    )


def _merge_edges_query(rng: random.Random, worker_id: int, iteration: int) -> tuple[str, dict]:
    sample = rng.randint(50, 300)
    return (
        """
MATCH (a:Stress), (b:Stress)
WHERE a.worker = $worker AND b.worker = $worker AND a.i < b.i
WITH a, b LIMIT $sample
MERGE (a)-[:LINKS {iter: $iter}]->(b)
""",
        {"worker": worker_id, "iter": iteration, "sample": sample},
    )


def _set_props_query(rng: random.Random, worker_id: int, iteration: int) -> tuple[str, dict]:
    sample = rng.randint(100, 1000)
    payload_len = rng.randint(16, 256)
    return (
        """
MATCH (n:Stress {worker: $worker})
WITH n LIMIT $sample
SET n.touched_iter = $iter,
    n.tag = 'tag_' + toString($iter) + '-' + $pad
""",
        {
            "worker": worker_id,
            "iter": iteration,
            "sample": sample,
            "pad": "y" * payload_len,
        },
    )


def _detach_delete_query(rng: random.Random, worker_id: int, iteration: int) -> tuple[str, dict]:
    sample = rng.randint(50, 500)
    return (
        """
MATCH (n:Stress {worker: $worker})
WITH n LIMIT $sample
DETACH DELETE n
""",
        {"worker": worker_id, "sample": sample},
    )


_OP_BUILDERS = {
    "bulk_create": _bulk_create_query,
    "merge_edges": _merge_edges_query,
    "set_props": _set_props_query,
    "detach_delete": _detach_delete_query,
}


def write_worker(worker_id: int, iteration: int, database: str) -> dict:
    """Run OPS_PER_WORKER randomized write queries against `database`.

    Query failures (memory-limit hits, transient errors) are counted but do
    not propagate; the goal is to apply pressure without crashing the cluster.
    """
    rng = random.Random(hash((worker_id, iteration, database)) & 0xFFFFFFFF)
    succeeded = 0
    failed = 0
    t0 = time.time()

    for _ in range(OPS_PER_WORKER):
        # Bias the mix toward writes that grow memory.
        op = rng.choices(
            _OPS,
            weights=(5, 3, 3, 1),
            k=1,
        )[0]
        query, params = _OP_BUILDERS[op](rng, worker_id, iteration)
        try:
            execute_query(
                COORDINATOR,
                query,
                params=params,
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
        f"\n--- Iteration {iteration + 1}/{NUM_ITERATIONS}: {total_workers} workers "
        f"({WORKERS_PER_DB}/db × {len(databases)} dbs) ---"
    )

    t0 = time.time()
    results = run_parallel(write_worker, tasks, num_workers=total_workers)
    elapsed = time.time() - t0

    succeeded = sum(r["succeeded"] for r in results)
    failed = sum(r["failed"] for r in results)
    print(
        f"Iteration {iteration + 1} writers done in {elapsed:.1f}s — "
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

        teardown_iteration(databases, iteration)


def main():
    print("=" * 60)
    print("Database Memory Profiles Workload")
    print("=" * 60)
    print(f"Databases         : {NUM_DATABASES}  ({DB_PREFIX}0..{DB_PREFIX}{NUM_DATABASES - 1})")
    print(f"Profile           : {PROFILE_NAME}  (memory_limit {PROFILE_MEMORY_MB} MB)")
    print(f"Workers per DB    : {WORKERS_PER_DB}")
    print(f"Iterations        : {NUM_ITERATIONS}")
    print(f"Ops per worker    : {OPS_PER_WORKER}")
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
