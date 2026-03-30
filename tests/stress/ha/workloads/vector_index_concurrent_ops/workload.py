#!/usr/bin/env python3
"""
Vector index concurrent operations stress test.

Exercises the vector index under concurrent pressure from four operation types
running in parallel:
  1. Vector search (read path)
  2. Vertex creation with vector properties (add to index)
  3. Vertex deletion (remove from index)
  4. FREE MEMORY (triggers GC which removes obsolete entries from usearch)

The test verifies that no crashes, deadlocks, or data corruption occur when
these operations interleave. At the end it asserts that node counts on MAIN
and REPLICA are consistent and that vector search still works.
"""
import random
import sys
import time
import traceback

from cluster_monitor import ClusterMonitor
from ha_common import Protocol, QueryType, execute_and_fetch, execute_query, run_parallel

COORDINATOR = "coord_1"
COORDINATORS = ["coord_1", "coord_2", "coord_3"]
MAIN = "data_1"
REPLICA = "data_2"

DATABASE = "memgraph"

NUM_SEED_NODES = 500
VECTOR_DIMENSIONS = 64
BATCH_SIZE = 50

# Duration-based instead of iteration-based so all worker types overlap.
STRESS_DURATION_SECONDS = 120
NUM_SEARCH_WORKERS = 2
NUM_CREATE_WORKERS = 2
NUM_DELETE_WORKERS = 2
NUM_FREE_MEMORY_WORKERS = 1

VECTOR_INDEX_NAME = "stress_vec_idx"
LABEL = "VecNode"
PROPERTY = "embedding"

HEALTH_CHECK_RETRIES = 30
HEALTH_CHECK_INTERVAL = 10


def generate_vector(dimensions: int) -> list[float]:
    return [random.uniform(-1.0, 1.0) for _ in range(dimensions)]


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------


def create_vector_index() -> None:
    print("Creating vector index...")
    query = (
        f"CREATE VECTOR INDEX {VECTOR_INDEX_NAME} ON :{LABEL}({PROPERTY}) "
        f'WITH CONFIG {{"dimension": {VECTOR_DIMENSIONS}, "capacity": {NUM_SEED_NODES * 4}}};'
    )
    execute_query(COORDINATOR, query, protocol=Protocol.BOLT_ROUTING, query_type=QueryType.WRITE)
    print("Vector index created.")


def seed_nodes_batch(batch_start: int, batch_size: int) -> None:
    nodes_data = [{"id": batch_start + i, "embedding": generate_vector(VECTOR_DIMENSIONS)} for i in range(batch_size)]
    execute_query(
        COORDINATOR,
        f"""
        UNWIND $nodes AS n
        CREATE (:{LABEL} {{id: n.id, {PROPERTY}: n.{PROPERTY}}})
        """,
        params={"nodes": nodes_data},
        protocol=Protocol.BOLT_ROUTING,
        query_type=QueryType.WRITE,
        apply_retry_mechanism=True,
    )


def seed_initial_nodes() -> None:
    num_batches = NUM_SEED_NODES // BATCH_SIZE
    tasks = [(batch_num * BATCH_SIZE, BATCH_SIZE) for batch_num in range(num_batches)]
    print(f"Seeding {NUM_SEED_NODES} nodes ({num_batches} batches)...")
    run_parallel(seed_nodes_batch, tasks, num_workers=4)
    print(f"Seeded {NUM_SEED_NODES} nodes.")


# ---------------------------------------------------------------------------
# Workers
# ---------------------------------------------------------------------------

_SEARCH_QUERY = (
    f"CALL vector_search.search('{VECTOR_INDEX_NAME}', 10, $query_vector) " "YIELD similarity RETURN similarity"
)

_CREATE_QUERY = f"""
    CREATE (:{LABEL} {{id: $id, {PROPERTY}: $embedding}})
"""

_FETCH_RANDOM_ID_QUERY = f"MATCH (n:{LABEL}) RETURN n.id AS node_id ORDER BY rand() LIMIT 1"
_DELETE_BY_ID_QUERY = f"MATCH (n:{LABEL} {{id: $node_id}}) DETACH DELETE n"

_FREE_MEMORY_QUERY = "FREE MEMORY"


def worker_search(worker_id: int) -> dict:
    """Continuously runs vector searches for the stress duration."""
    count = 0
    errors = 0
    deadline = time.monotonic() + STRESS_DURATION_SECONDS
    while time.monotonic() < deadline:
        try:
            query_vector = generate_vector(VECTOR_DIMENSIONS)
            execute_and_fetch(
                COORDINATOR,
                _SEARCH_QUERY,
                params={"query_vector": query_vector},
                protocol=Protocol.BOLT_ROUTING,
                database=DATABASE,
            )
            count += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"  [Search-{worker_id}] error: {e}")
    print(f"  [Search-{worker_id}] done: {count} searches, {errors} errors")
    return {"type": "search", "worker_id": worker_id, "count": count, "errors": errors}


def worker_create(worker_id: int) -> dict:
    """Continuously creates vertices with vector properties."""
    count = 0
    errors = 0
    deadline = time.monotonic() + STRESS_DURATION_SECONDS
    while time.monotonic() < deadline:
        try:
            execute_query(
                COORDINATOR,
                _CREATE_QUERY,
                params={
                    "id": random.randint(100_000, 10_000_000),
                    "embedding": generate_vector(VECTOR_DIMENSIONS),
                },
                protocol=Protocol.BOLT_ROUTING,
                query_type=QueryType.WRITE,
                apply_retry_mechanism=True,
                database=DATABASE,
            )
            count += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"  [Create-{worker_id}] error: {e}")
    print(f"  [Create-{worker_id}] done: {count} creates, {errors} errors")
    return {"type": "create", "worker_id": worker_id, "count": count, "errors": errors}


def worker_delete(worker_id: int) -> dict:
    """Continuously fetches a random existing node and deletes it."""
    count = 0
    errors = 0
    deadline = time.monotonic() + STRESS_DURATION_SECONDS
    while time.monotonic() < deadline:
        try:
            rows = execute_and_fetch(
                COORDINATOR, _FETCH_RANDOM_ID_QUERY, protocol=Protocol.BOLT_ROUTING, database=DATABASE
            )
            if not rows:
                time.sleep(0.01)
                continue
            node_id = rows[0]["node_id"]
            execute_query(
                COORDINATOR,
                _DELETE_BY_ID_QUERY,
                params={"node_id": node_id},
                protocol=Protocol.BOLT_ROUTING,
                query_type=QueryType.WRITE,
                apply_retry_mechanism=True,
                database=DATABASE,
            )
            count += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"  [Delete-{worker_id}] error: {e}")
    print(f"  [Delete-{worker_id}] done: {count} deletes, {errors} errors")
    return {"type": "delete", "worker_id": worker_id, "count": count, "errors": errors}


def worker_free_memory(worker_id: int) -> dict:
    """Continuously triggers FREE MEMORY to exercise GC + vector index cleanup."""
    count = 0
    errors = 0
    deadline = time.monotonic() + STRESS_DURATION_SECONDS
    while time.monotonic() < deadline:
        try:
            execute_query(
                COORDINATOR,
                _FREE_MEMORY_QUERY,
                protocol=Protocol.BOLT_ROUTING,
                query_type=QueryType.WRITE,
                apply_retry_mechanism=False,
                database=DATABASE,
            )
            count += 1
            # Small pause to avoid hammering GC too aggressively.
            time.sleep(0.5)
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"  [FreeMemory-{worker_id}] error: {e}")
    print(f"  [FreeMemory-{worker_id}] done: {count} FREE MEMORY calls, {errors} errors")
    return {"type": "free_memory", "worker_id": worker_id, "count": count, "errors": errors}


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


def verify_vector_index_exists() -> bool:
    rows = execute_and_fetch(COORDINATOR, "SHOW VECTOR INDEX INFO;", protocol=Protocol.BOLT_ROUTING, database=DATABASE)
    names = {row.get("index_name") or row.get("name") for row in rows}
    if VECTOR_INDEX_NAME not in names:
        print(f"  ERROR: Vector index '{VECTOR_INDEX_NAME}' not found. Existing: {names}")
        return False
    print(f"  OK: Vector index '{VECTOR_INDEX_NAME}' exists.")
    return True


def verify_search_works() -> bool:
    query_vector = generate_vector(VECTOR_DIMENSIONS)
    try:
        rows = execute_and_fetch(
            COORDINATOR,
            _SEARCH_QUERY,
            params={"query_vector": query_vector},
            protocol=Protocol.BOLT_ROUTING,
            database=DATABASE,
        )
        print(f"  OK: Vector search returned {len(rows)} results.")
        return True
    except Exception as e:
        print(f"  ERROR: Vector search failed: {e}")
        return False


def get_node_count(instance_name: str) -> int:
    rows = execute_and_fetch(
        instance_name,
        f"MATCH (n:{LABEL}) RETURN count(n) AS cnt",
        protocol=Protocol.BOLT,
        database=DATABASE,
    )
    return rows[0]["cnt"] if rows else 0


def verify_replication_consistency() -> bool:
    count_main = get_node_count(MAIN)
    count_replica = get_node_count(REPLICA)
    if count_main != count_replica:
        print(f"  ERROR: MAIN={count_main}, REPLICA={count_replica}")
        return False
    print(f"  OK: Both MAIN and REPLICA have {count_main} nodes.")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

# Each task tuple is (worker_id,) — single argument per worker.
_WORKER_DISPATCH = {
    "search": (worker_search, NUM_SEARCH_WORKERS),
    "create": (worker_create, NUM_CREATE_WORKERS),
    "delete": (worker_delete, NUM_DELETE_WORKERS),
    "free_memory": (worker_free_memory, NUM_FREE_MEMORY_WORKERS),
}


def main():
    print("=" * 60)
    print("Vector Index Concurrent Operations Stress Test")
    print("=" * 60)
    print(f"Seed nodes: {NUM_SEED_NODES}, Dimensions: {VECTOR_DIMENSIONS}")
    print(f"Stress duration: {STRESS_DURATION_SECONDS}s")
    print(
        f"Workers: {NUM_SEARCH_WORKERS} search, {NUM_CREATE_WORKERS} create, "
        f"{NUM_DELETE_WORKERS} delete, {NUM_FREE_MEMORY_WORKERS} free_memory"
    )
    print("-" * 60)

    monitor = ClusterMonitor(
        coordinators=COORDINATORS,
        show_replicas=True,
        verify_up=True,
        interval=10,
    )

    # Phase 1: Setup
    print("\n" + "=" * 60)
    print("PHASE 1: Setup - create index and seed data")
    print("=" * 60)

    with monitor:
        create_vector_index()
        seed_initial_nodes()

    # Phase 2: Concurrent stress
    print("\n" + "=" * 60)
    print("PHASE 2: Concurrent stress (search + create + delete + FREE MEMORY)")
    print("=" * 60)

    tasks = []
    for worker_type, (fn, num_workers) in _WORKER_DISPATCH.items():
        for wid in range(num_workers):
            tasks.append((fn, (wid,)))

    total_workers = len(tasks)
    print(f"Launching {total_workers} workers for {STRESS_DURATION_SECONDS}s...")

    # run_parallel expects (fn, args_tuple) — we need a wrapper since each
    # worker type has a different function.
    def _run_worker(fn_and_args: tuple) -> dict:
        fn, args = fn_and_args
        return fn(*args)

    # Use run_parallel with a shim: each task is (shim, (fn, (wid,)))
    # But run_parallel unpacks task tuples as positional args to worker_fn.
    # So we pass (fn, args) as a single tuple arg to a wrapper.
    all_tasks = [(fn, wid) for _, (fn, num_workers) in _WORKER_DISPATCH.items() for wid in range(num_workers)]

    with monitor:
        results = run_parallel(_dispatch_worker, [(fn, wid) for fn, wid in all_tasks], num_workers=total_workers)

    # Summarize results
    print("\n--- Stress Results ---")
    total_errors = 0
    for worker_type in _WORKER_DISPATCH:
        type_results = [r for r in results if r["type"] == worker_type]
        ops = sum(r["count"] for r in type_results)
        errs = sum(r["errors"] for r in type_results)
        total_errors += errs
        print(f"  {worker_type}: {ops:,} ops, {errs} errors")

    # Phase 3: Verification
    print("\n" + "=" * 60)
    print("PHASE 3: Verification")
    print("=" * 60)

    idx_ok = verify_vector_index_exists()
    search_ok = verify_search_works()
    repl_ok = verify_replication_consistency()

    print("\nFinal replica status:")
    monitor.show_replicas()

    cluster_ok = monitor.verify_all_ready() and monitor.verify_instances_up()
    if not idx_ok or not search_ok or not repl_ok or not cluster_ok:
        print("\nSTRESS TEST FAILED!")
        sys.exit(1)

    if total_errors > 0:
        print(f"\nWARN: {total_errors} total errors during stress phase (non-fatal).")

    print("\nStress test completed successfully!")


def _dispatch_worker(fn, worker_id: int) -> dict:
    """Wrapper that run_parallel can call with positional args."""
    return fn(worker_id)


if __name__ == "__main__":
    main()
