#!/usr/bin/env python3
"""
Multitenant vector edge index recovery workload.

1) Creates 3 tenant databases, each with 3 vector edge indices.
2) Creates anchor nodes, then ingests 1000 edges (each with 3 embeddings) into
   every database in parallel.
3) Runs 100,000 iterations across parallel workers: each iteration randomly
   creates a new edge OR fetches a real existing edge and deletes it.
4) Restarts ALL instances (coordinators + data).
5) Waits for the cluster to become healthy again.
6) Queries data_1 (MAIN) and data_2 (REPLICA) directly and asserts that edge
   counts match across all databases.
"""
import random
import sys
import time

from cluster_monitor import ClusterMonitor
from ha_common import Protocol, QueryType, execute_and_fetch, execute_query, get_restart_all_fn, run_parallel

COORDINATOR = "coord_1"
COORDINATORS = ["coord_1", "coord_2", "coord_3"]
MAIN = "data_1"
REPLICA = "data_2"

DATABASES = ["tenant_alpha", "tenant_beta", "tenant_gamma"]

NUM_EDGES = 1000
VECTOR_DIMENSIONS = 128
BATCH_SIZE = 100
NUM_WORKERS = 4

ITERATIONS = 200_000
NUM_WORKERS_PER_DB = 5

HEALTH_CHECK_RETRIES = 30
HEALTH_CHECK_INTERVAL = 10

# 3 vector edge indices per database: each covers one edge type + property pair
VECTOR_EDGE_INDICES = [
    {"name": "vec_edge_idx_alpha", "edge_type": "EDGE_ALPHA", "property": "embedding_alpha"},
    {"name": "vec_edge_idx_beta", "edge_type": "EDGE_BETA", "property": "embedding_beta"},
    {"name": "vec_edge_idx_gamma", "edge_type": "EDGE_GAMMA", "property": "embedding_gamma"},
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


def create_anchor_nodes() -> None:
    """Create two anchor nodes per database for edges to connect."""
    print("Creating anchor nodes in all databases...")
    for db_name in DATABASES:
        execute_query(
            COORDINATOR,
            "CREATE (:Anchor {id: 1}), (:Anchor {id: 2})",
            protocol=Protocol.BOLT_ROUTING,
            query_type=QueryType.WRITE,
            database=db_name,
        )
    print("Anchor nodes created.")


def create_vector_edge_indices() -> None:
    print("Creating vector edge indices in all databases...")
    for db_name in DATABASES:
        print(f"  [{db_name}] Creating vector edge indices...")
        for idx in VECTOR_EDGE_INDICES:
            query = (
                f"CREATE VECTOR EDGE INDEX {idx['name']} ON :{idx['edge_type']}({idx['property']}) "
                f'WITH CONFIG {{"dimension": {VECTOR_DIMENSIONS}, "capacity": {NUM_EDGES}}};'
            )
            execute_query(
                COORDINATOR, query, protocol=Protocol.BOLT_ROUTING, query_type=QueryType.WRITE, database=db_name
            )
        print(f"  [{db_name}] Vector edge indices created.")
    print("All vector edge indices created.")


def create_edges_batch(batch_start: int, batch_size: int, db_name: str) -> None:
    edges_data = [
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
        MATCH (a:Anchor {id: 1}), (b:Anchor {id: 2})
        UNWIND $edges AS e
        CREATE (a)-[:EDGE_ALPHA {id: e.id, embedding_alpha: e.embedding_alpha}]->(b)
        CREATE (a)-[:EDGE_BETA {id: e.id, embedding_beta: e.embedding_beta}]->(b)
        CREATE (a)-[:EDGE_GAMMA {id: e.id, embedding_gamma: e.embedding_gamma}]->(b)
        """,
        params={"edges": edges_data},
        protocol=Protocol.BOLT_ROUTING,
        query_type=QueryType.WRITE,
        apply_retry_mechanism=True,
        database=db_name,
    )


def run_parallel_edge_creation() -> None:
    num_batches = NUM_EDGES // BATCH_SIZE
    tasks = [(batch_num * BATCH_SIZE, BATCH_SIZE, db_name) for db_name in DATABASES for batch_num in range(num_batches)]
    total = NUM_EDGES * len(DATABASES)
    print(
        f"Creating {total:,} edges total "
        f"({NUM_EDGES} per database x {len(DATABASES)} databases, "
        f"{len(tasks)} batches, {NUM_WORKERS} workers)..."
    )
    run_parallel(create_edges_batch, tasks, num_workers=NUM_WORKERS)
    print(f"All {total:,} edges created.")


# ---------------------------------------------------------------------------
# Phase 2: random create/delete iterations
# ---------------------------------------------------------------------------

_CREATE_EDGE_QUERY = """
    MATCH (a:Anchor {id: 1}), (b:Anchor {id: 2})
    CREATE (a)-[:EDGE_ALPHA {id: $id, embedding_alpha: $embedding_alpha}]->(b)
    CREATE (a)-[:EDGE_BETA {id: $id, embedding_beta: $embedding_beta}]->(b)
    CREATE (a)-[:EDGE_GAMMA {id: $id, embedding_gamma: $embedding_gamma}]->(b)
"""

_FETCH_RANDOM_EDGE_ID_QUERY = "MATCH ()-[r:EDGE_ALPHA]->() RETURN r.id AS edge_id ORDER BY rand() LIMIT 1"

_DELETE_EDGES_BY_ID_QUERY = """
    MATCH ()-[r]->()
    WHERE r.id = $edge_id AND (type(r) = 'EDGE_ALPHA' OR type(r) = 'EDGE_BETA' OR type(r) = 'EDGE_GAMMA')
    DELETE r
"""


def _delete_random_edges(db_name: str) -> bool:
    """Fetch a real existing edge id, then delete all edges with that id."""
    rows = execute_and_fetch(COORDINATOR, _FETCH_RANDOM_EDGE_ID_QUERY, protocol=Protocol.BOLT_ROUTING, database=db_name)
    if not rows:
        return False
    edge_id = rows[0]["edge_id"]
    execute_query(
        COORDINATOR,
        _DELETE_EDGES_BY_ID_QUERY,
        params={"edge_id": edge_id},
        protocol=Protocol.BOLT_ROUTING,
        query_type=QueryType.WRITE,
        apply_retry_mechanism=True,
        database=db_name,
    )
    return True


def run_iterations(worker_id: int, num_iterations: int, db_name: str) -> dict:
    """Randomly create or delete edges per iteration in db_name."""
    created = 0
    deleted = 0
    for _ in range(num_iterations):
        if random.random() < 0.5:
            execute_query(
                COORDINATOR,
                _CREATE_EDGE_QUERY,
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
            created += 1
        else:
            if _delete_random_edges(db_name):
                deleted += 1

    print(f"  [Worker {worker_id}/{db_name}] done: {created} created, {deleted} deleted")
    return {"worker_id": worker_id, "db_name": db_name, "created": created, "deleted": deleted}


def run_worker_iterations() -> None:
    iters_per_worker = ITERATIONS // NUM_WORKERS_PER_DB
    total_workers = NUM_WORKERS_PER_DB * len(DATABASES)
    tasks = [(worker_id, iters_per_worker, db_name) for db_name in DATABASES for worker_id in range(NUM_WORKERS_PER_DB)]
    print(
        f"Running {ITERATIONS:,} iterations per database x {len(DATABASES)} databases "
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


def get_edge_count_from_instance(instance_name: str, db_name: str, edge_type: str) -> int:
    rows = execute_and_fetch(
        instance_name,
        f"MATCH ()-[r:{edge_type}]->() RETURN count(r) AS cnt",
        protocol=Protocol.BOLT,
        database=db_name,
    )
    return rows[0]["cnt"] if rows else 0


def verify_counts_match() -> bool:
    print("\nComparing edge counts between MAIN and REPLICA...")
    all_ok = True
    for db_name in DATABASES:
        for idx in VECTOR_EDGE_INDICES:
            edge_type = idx["edge_type"]
            count_main = get_edge_count_from_instance(MAIN, db_name, edge_type)
            count_replica = get_edge_count_from_instance(REPLICA, db_name, edge_type)
            if count_main != count_replica:
                print(f"  ERROR [{db_name}/{edge_type}]: {MAIN}={count_main}, {REPLICA}={count_replica}")
                all_ok = False
            else:
                print(f"  OK [{db_name}/{edge_type}]: both have {count_main} edges")
    return all_ok


def get_vector_index_names(db_name: str) -> set[str]:
    rows = execute_and_fetch(COORDINATOR, "SHOW VECTOR INDEX INFO;", protocol=Protocol.BOLT_ROUTING, database=db_name)
    return {row.get("index_name") or row.get("name") for row in rows}


def verify_vector_edge_indices() -> bool:
    print("\nVerifying vector edge indices in all databases...")
    expected = {idx["name"] for idx in VECTOR_EDGE_INDICES}
    all_ok = True
    for db_name in DATABASES:
        existing = get_vector_index_names(db_name)
        missing = expected - existing
        if missing:
            print(f"  ERROR [{db_name}]: missing indices {missing}")
            all_ok = False
        else:
            print(f"  OK [{db_name}]: {existing}")
    return all_ok


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    restart_all_fn = get_restart_all_fn()
    if restart_all_fn is None:
        print("ERROR: restart_all function not available for this deployment")
        sys.exit(1)

    print("=" * 60)
    print("Multitenant Vector Edge Index Recovery Workload")
    print("=" * 60)
    print(f"Databases: {DATABASES}")
    print(f"Initial edges per database: {NUM_EDGES}, Vector dimensions: {VECTOR_DIMENSIONS}")
    print(f"Worker iterations: {ITERATIONS:,} per database, {NUM_WORKERS_PER_DB} workers per database")
    print(f"Vector edge indices per database: {[idx['name'] for idx in VECTOR_EDGE_INDICES]}")
    print("-" * 60)

    monitor = ClusterMonitor(
        coordinators=COORDINATORS,
        show_replicas=True,
        verify_up=True,
        interval=5,
    )

    # Phase 1: Setup + initial ingest
    print("\n" + "=" * 60)
    print("PHASE 1: Create databases, vector edge indices, and edges")
    print("=" * 60)

    with monitor:
        create_databases()
        create_anchor_nodes()
        create_vector_edge_indices()
        run_parallel_edge_creation()

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

    indices_ok = verify_vector_edge_indices()
    counts_ok = verify_counts_match()

    print("\nFinal replica status:")
    monitor.show_replicas()

    cluster_ok = monitor.verify_all_ready() and monitor.verify_instances_up()
    if not indices_ok or not counts_ok or not cluster_ok:
        sys.exit(1)

    print("\nWorkload completed successfully!")


if __name__ == "__main__":
    main()
