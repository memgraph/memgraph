#!/usr/bin/env python3
"""
Multitenant vector index recovery workload.

1) Creates 3 tenant databases, each with 3 vector indices.
2) Ingests 1000 nodes (each with 3 embeddings) into every database in parallel.
3) Runs 100,000 iterations across parallel workers: each iteration randomly
   creates a new node OR fetches a real existing node id and deletes it.
4) Restarts ALL instances (coordinators + data).
5) Waits for the cluster to become healthy again.
6) Queries data_1 (MAIN) and data_2 (REPLICA) directly and asserts that node
   counts match across all databases.

Database selection is done via the ha_common `database=` parameter which is
forwarded to driver.session(database=db_name). Workers use bolt+routing.
"""
import random
import sys
import time

from cluster_monitor import ClusterMonitor
from ha_common import (
    Protocol,
    QueryType,
    cleanup,
    execute_and_fetch,
    execute_and_fetch_with_manual_retries,
    execute_query,
    get_restart_all_fn,
    run_parallel,
)

COORDINATOR = "coord_1"
COORDINATORS = ["coord_1", "coord_2", "coord_3"]
MAIN = "data_1"
REPLICA = "data_2"

DATABASES = ["tenant_alpha", "tenant_beta", "tenant_gamma"]

NUM_NODES = 1000
VECTOR_DIMENSIONS = 128
BATCH_SIZE = 100
NUM_WORKERS = 4

ITERATIONS = 100_000
NUM_WORKERS_PER_DB = 4

HEALTH_CHECK_RETRIES = 30
HEALTH_CHECK_INTERVAL = 10

# 3 vector indices per database: each covers one label + property pair
VECTOR_INDICES = [
    {"name": "vec_idx_alpha", "label": "VectorNodeAlpha", "property": "embedding_alpha"},
    {"name": "vec_idx_beta", "label": "VectorNodeBeta", "property": "embedding_beta"},
    {"name": "vec_idx_gamma", "label": "VectorNodeGamma", "property": "embedding_gamma"},
]


def generate_vector(dimensions: int) -> list[float]:
    return [random.uniform(-1.0, 1.0) for _ in range(dimensions)]


# ---------------------------------------------------------------------------
# Phase 1: setup
# ---------------------------------------------------------------------------


def create_databases() -> None:
    print("Creating tenant databases...")
    for db_name in DATABASES:
        query = f"CREATE DATABASE {db_name};"
        print(f"  {query}")
        execute_query(COORDINATOR, query, protocol=Protocol.BOLT_ROUTING, query_type=QueryType.WRITE)
    print("Databases created.")


def create_vector_indices() -> None:
    print("Creating vector indices in all databases...")
    for db_name in DATABASES:
        print(f"  [{db_name}] Creating vector indices...")
        for idx in VECTOR_INDICES:
            query = (
                f"CREATE VECTOR INDEX {idx['name']} ON :{idx['label']}({idx['property']}) "
                f'WITH CONFIG {{"dimension": {VECTOR_DIMENSIONS}, "capacity": {NUM_NODES}}};'
            )
            execute_query(
                COORDINATOR, query, protocol=Protocol.BOLT_ROUTING, query_type=QueryType.WRITE, database=db_name
            )
        print(f"  [{db_name}] Vector indices created.")
    print("All vector indices created.")


def create_nodes_batch(batch_start: int, batch_size: int, db_name: str) -> None:
    nodes_data = [
        {
            "id": batch_start + i,
            "embedding_alpha": generate_vector(VECTOR_DIMENSIONS),
            "embedding_beta": generate_vector(VECTOR_DIMENSIONS),
            "embedding_gamma": generate_vector(VECTOR_DIMENSIONS),
        }
        for i in range(batch_size)
    ]
    execute_query(
        COORDINATOR,
        """
        UNWIND $nodes AS n
        CREATE (node:VectorNodeAlpha:VectorNodeBeta:VectorNodeGamma {
            id: n.id,
            embedding_alpha: n.embedding_alpha,
            embedding_beta: n.embedding_beta,
            embedding_gamma: n.embedding_gamma
        })
        """,
        params={"nodes": nodes_data},
        protocol=Protocol.BOLT_ROUTING,
        query_type=QueryType.WRITE,
        apply_retry_mechanism=True,
        database=db_name,
    )


def run_parallel_node_creation() -> None:
    num_batches = NUM_NODES // BATCH_SIZE
    tasks = [(batch_num * BATCH_SIZE, BATCH_SIZE, db_name) for db_name in DATABASES for batch_num in range(num_batches)]
    total = NUM_NODES * len(DATABASES)
    print(
        f"Creating {total:,} nodes total "
        f"({NUM_NODES} per database × {len(DATABASES)} databases, "
        f"{len(tasks)} batches, {NUM_WORKERS} workers)..."
    )
    run_parallel(create_nodes_batch, tasks, num_workers=NUM_WORKERS)
    print(f"All {total:,} nodes created.")


# ---------------------------------------------------------------------------
# Phase 2: random create/delete iterations
# ---------------------------------------------------------------------------

_CREATE_QUERY = """
    CREATE (:VectorNodeAlpha:VectorNodeBeta:VectorNodeGamma {
        id: $id,
        embedding_alpha: $embedding_alpha,
        embedding_beta: $embedding_beta,
        embedding_gamma: $embedding_gamma
    })
"""

_FETCH_RANDOM_ID_QUERY = "MATCH (n:VectorNodeAlpha) RETURN n.id AS node_id ORDER BY rand() LIMIT 1"
_DELETE_BY_ID_QUERY = "MATCH (n:VectorNodeAlpha {id: $node_id}) DETACH DELETE n"


def _create_random_node(db_name: str) -> None:
    """Create a new node with random embeddings."""
    execute_query(
        COORDINATOR,
        _CREATE_QUERY,
        params={
            "id": random.randint(0, 10_000_000),
            "embedding_alpha": generate_vector(VECTOR_DIMENSIONS),
            "embedding_beta": generate_vector(VECTOR_DIMENSIONS),
            "embedding_gamma": generate_vector(VECTOR_DIMENSIONS),
        },
        protocol=Protocol.BOLT_ROUTING,
        query_type=QueryType.WRITE,
        apply_retry_mechanism=True,
        database=db_name,
    )


def _delete_random_node(db_name: str) -> bool:
    """Fetch a real existing node id, then delete it. Returns True if a node was deleted."""
    rows = execute_and_fetch(COORDINATOR, _FETCH_RANDOM_ID_QUERY, protocol=Protocol.BOLT_ROUTING, database=db_name)
    if not rows:
        return False
    node_id = rows[0]["node_id"]
    execute_query(
        COORDINATOR,
        _DELETE_BY_ID_QUERY,
        params={"node_id": node_id},
        protocol=Protocol.BOLT_ROUTING,
        query_type=QueryType.WRITE,
        apply_retry_mechanism=True,
        database=db_name,
    )
    return True


def run_iterations(worker_id: int, num_iterations: int, db_name: str) -> dict:
    """Randomly create or delete one node per iteration in db_name."""
    created = 0
    deleted = 0
    for i in range(num_iterations):
        if i > 0 and i % 1000 == 0:
            print(f"  [Worker {worker_id}/{db_name}] {i:,} / {num_iterations:,} iterations")
        if random.random() < 0.5:
            _create_random_node(db_name)
            created += 1
        else:
            if _delete_random_node(db_name):
                deleted += 1

    print(f"  [Worker {worker_id}/{db_name}] done: {created} created, {deleted} deleted")
    return {"worker_id": worker_id, "db_name": db_name, "created": created, "deleted": deleted}


def run_worker_iterations() -> None:
    iters_per_worker = ITERATIONS // NUM_WORKERS_PER_DB
    total_workers = NUM_WORKERS_PER_DB * len(DATABASES)
    tasks = [(worker_id, iters_per_worker, db_name) for db_name in DATABASES for worker_id in range(NUM_WORKERS_PER_DB)]
    print(
        f"Running {ITERATIONS:,} iterations per database × {len(DATABASES)} databases "
        f"({NUM_WORKERS_PER_DB} workers each, {iters_per_worker:,} iterations/worker, "
        f"{total_workers} workers total)..."
    )
    results = run_parallel(run_iterations, tasks, num_workers=total_workers)
    for db_name in DATABASES:
        db_results = [r for r in results if r["db_name"] == db_name]
        created = sum(r["created"] for r in db_results)
        deleted = sum(r["deleted"] for r in db_results)
        print(f"  [{db_name}] {created:,} creates, {deleted:,} deletes")


# ---------------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------------


def wait_for_healthy_cluster(monitor: ClusterMonitor) -> None:
    for attempt in range(1, HEALTH_CHECK_RETRIES + 1):
        try:
            instances_ok = monitor.verify_instances_up()
            replicas_ok = monitor.verify_all_ready()
            if instances_ok and replicas_ok:
                return
        except Exception as e:
            print(f"  Health check attempt {attempt}/{HEALTH_CHECK_RETRIES} failed: {e}")
        print(f"  Waiting {HEALTH_CHECK_INTERVAL}s before next health check...")
        time.sleep(HEALTH_CHECK_INTERVAL)

    print("ERROR: Cluster did not become healthy after restart!")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


def get_node_count_from_instance(instance_name: str, db_name: str) -> int:
    rows = execute_and_fetch(
        instance_name,
        "MATCH (n:VectorNodeAlpha) RETURN count(n) AS cnt",
        protocol=Protocol.BOLT,
        database=db_name,
    )
    return rows[0]["cnt"] if rows else 0


def verify_counts_match() -> bool:
    print("\nComparing node counts between MAIN and REPLICA...")
    for db_name in DATABASES:
        count_main = get_node_count_from_instance(MAIN, db_name)
        count_replica = get_node_count_from_instance(REPLICA, db_name)
        if count_main != count_replica:
            print(f"  ERROR [{db_name}]: {MAIN}={count_main}, {REPLICA}={count_replica}")
            return False
        print(f"  OK [{db_name}]: both have {count_main} nodes")
    return True


def get_vector_index_names(db_name: str) -> set[str]:
    rows = execute_and_fetch_with_manual_retries(
        COORDINATOR,
        "SHOW VECTOR INDEX INFO;",
        protocol=Protocol.BOLT_ROUTING,
        database=db_name,
        max_retries=10,
    )
    return {row.get("index_name") or row.get("name") for row in rows}


def verify_vector_indices() -> bool:
    print("\nVerifying vector indices in all databases...")
    expected = {idx["name"] for idx in VECTOR_INDICES}
    for db_name in DATABASES:
        existing = get_vector_index_names(db_name)
        missing = expected - existing
        if missing:
            print(f"  ERROR [{db_name}]: missing indices {missing}")
            return False
        print(f"  OK [{db_name}]: {existing}")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    restart_all_fn = get_restart_all_fn()
    if restart_all_fn is None:
        print("ERROR: restart_all function not available for this deployment")
        sys.exit(1)

    print("=" * 60)
    print("Multitenant Vector Index Recovery Workload")
    print("=" * 60)
    print(f"Databases: {DATABASES}")
    print(f"Initial nodes per database: {NUM_NODES}, Vector dimensions: {VECTOR_DIMENSIONS}")
    print(f"Worker iterations: {ITERATIONS:,} per database, {NUM_WORKERS_PER_DB} workers per database")
    print(f"Vector indices per database: {[idx['name'] for idx in VECTOR_INDICES]}")
    print("-" * 60)

    monitor = ClusterMonitor(
        coordinators=COORDINATORS,
        show_replicas=True,
        verify_up=True,
        interval=5,
    )

    # Phase 1: Setup + initial ingest
    print("\n" + "=" * 60)
    print("PHASE 1: Create databases, vector indices, and nodes")
    print("=" * 60)

    with monitor:
        create_databases()
        create_vector_indices()
        run_parallel_node_creation()

    # Phase 2: Random create/delete iterations
    print("\n" + "=" * 60)
    print("PHASE 2: Random create/delete iterations")
    print("=" * 60)

    with monitor:
        run_worker_iterations()

    # Phase 3: Restart all instances
    print("\n" + "=" * 60)
    print("PHASE 3: Restart all instances")
    print("=" * 60)

    print("Triggering full cluster restart...")
    t0 = time.time()
    restart_all_fn()
    print(f"Restart triggered in {time.time() - t0:.1f}s")

    print("Waiting for cluster to become healthy...")
    wait_for_healthy_cluster(monitor)
    print("Cluster is healthy after restart.")

    # Phase 4: Verify recovery
    print("\n" + "=" * 60)
    print("PHASE 4: Verify recovery")
    print("=" * 60)

    indices_ok = verify_vector_indices()
    counts_ok = verify_counts_match()

    print("\nFinal replica status:")
    monitor.show_replicas()

    cluster_ok = monitor.verify_all_ready() and monitor.verify_instances_up()
    if not indices_ok:
        print("ERROR: Vector index verification failed after recovery!")
        sys.exit(1)
    if not counts_ok:
        print("ERROR: Node count mismatch between MAIN and REPLICA after recovery!")
        sys.exit(1)
    if not cluster_ok:
        print("ERROR: Cluster is not healthy after recovery!")
        sys.exit(1)

    print("\nWorkload completed successfully!")


if __name__ == "__main__":
    try:
        main()
    finally:
        cleanup()
