#!/usr/bin/env python3
"""
High-concurrency edge write workload.
Creates 1000 nodes via UNWIND+CREATE, then 5 concurrent workers repeatedly
MERGE RELATED_TO edges between cartesian product pairs of those nodes.

NUM_ITERATIONS can be overridden via the WORKLOAD_NUM_ITERATIONS env var.
"""
import os
import sys
import threading
import time

from ha_common import Protocol, QueryType, execute_and_fetch, execute_query, run_parallel

COORDINATOR = "coord_1"

SHOW_REPLICAS_QUERY = "SHOW REPLICAS;"
SHOW_STORAGE_INFO_QUERY = "SHOW STORAGE INFO;"

NUM_NODES = 1000
NUM_WORKERS = 5
NUM_ITERATIONS = int(os.environ.get("WORKLOAD_NUM_ITERATIONS", "50"))

STORAGE_INFO_INTERVAL_SECONDS = 5
SHOW_REPLICAS_INTERVAL_SECONDS = 5


def create_indices_and_constraints() -> None:
    print("Creating indices and constraints...")

    queries = [
        "CREATE INDEX ON :Node;",
        "CREATE INDEX ON :Node(uid);",
        "CREATE CONSTRAINT ON (n:Node) ASSERT n.uid IS UNIQUE;",
    ]

    for query in queries:
        print(f"  {query}")
        execute_query(COORDINATOR, query, protocol=Protocol.BOLT_ROUTING, query_type=QueryType.WRITE)

    print("Indices and constraints created.")


def create_nodes() -> None:
    print(f"\nCreating {NUM_NODES} nodes...")
    query = f"""
UNWIND range(0, {NUM_NODES - 1}) AS i
CREATE (:Node {{uid: i, name: 'node_' + toString(i)}});"""

    start_time = time.time()
    execute_query(COORDINATOR, query, protocol=Protocol.BOLT_ROUTING, query_type=QueryType.WRITE)
    elapsed = time.time() - start_time
    print(f"  Created {NUM_NODES} nodes in {elapsed:.1f}s")


def edge_write_worker(worker_id: int) -> dict:
    """
    Each worker MATCHes the cartesian product of Node pairs and MERGEs
    RELATED_TO edges. Partitioning via (a.uid + b.uid) % NUM_WORKERS
    ensures each worker handles a distinct, non-overlapping slice.
    """
    query = f"""
MATCH (a:Node), (b:Node)
WHERE a.uid < b.uid
  AND (a.uid + b.uid) % {NUM_WORKERS} = {worker_id}
MERGE (a)-[:RELATED_TO]->(b);"""

    t0 = time.time()
    for iteration in range(NUM_ITERATIONS):
        execute_query(
            COORDINATOR,
            query,
            protocol=Protocol.BOLT_ROUTING,
            query_type=QueryType.WRITE,
        )
        if (iteration + 1) % 10 == 0:
            print(f"  [Worker {worker_id}] {iteration + 1}/{NUM_ITERATIONS} iterations")
    elapsed = time.time() - t0
    print(f"  [Worker {worker_id}] Completed {NUM_ITERATIONS} iterations in {elapsed:.1f}s")
    return {"worker_id": worker_id, "elapsed": elapsed}


def run_concurrent_edge_writes() -> None:
    print(f"\nRunning {NUM_WORKERS} concurrent edge write workers...")
    tasks = [(i,) for i in range(NUM_WORKERS)]

    t0 = time.time()
    results = run_parallel(edge_write_worker, tasks, num_workers=NUM_WORKERS)
    elapsed = time.time() - t0

    print(f"  All {NUM_WORKERS} workers completed in {elapsed:.1f}s")


def storage_info_worker(stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        try:
            results = execute_and_fetch(
                COORDINATOR, SHOW_STORAGE_INFO_QUERY, protocol=Protocol.BOLT_ROUTING, query_type=QueryType.READ
            )
            info = {row["storage info"]: row["value"] for row in results if "storage info" in row}
            vertices = info.get("vertex_count", "?")
            edges = info.get("edge_count", "?")
            memory = info.get("memory_res", "?")
            print(
                f"\n[STORAGE INFO @ {time.strftime('%H:%M:%S')}] vertices={vertices}  edges={edges}  memory_res={memory}"
            )
        except Exception as e:
            print(f"\n[STORAGE INFO ERROR] {e}")

        stop_event.wait(STORAGE_INFO_INTERVAL_SECONDS)


def replicas_worker(stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        try:
            results = execute_and_fetch(COORDINATOR, SHOW_REPLICAS_QUERY, protocol=Protocol.BOLT_ROUTING)
            if results:
                headers = list(results[0].keys())
                print(f"\n[SHOW REPLICAS @ {time.strftime('%H:%M:%S')}]")
                print("  " + ",".join(headers))
                for replica in results:
                    print("  " + ",".join(str(replica[h]) for h in headers))
                    data_info = replica.get("data_info", {})
                    for db_name, db_status in data_info.items():
                        if isinstance(db_status, dict) and db_status.get("status") == "invalid":
                            name = replica.get("name", "unknown")
                            print(f"\n  FATAL: Replica '{name}' has invalid status for database '{db_name}'!")
                            print(f"  Replica crashed — aborting workload.")
                            os._exit(1)
        except Exception as e:
            print(f"\n[SHOW REPLICAS ERROR] {e}")

        stop_event.wait(SHOW_REPLICAS_INTERVAL_SECONDS)


def show_replicas() -> None:
    results = execute_and_fetch(COORDINATOR, SHOW_REPLICAS_QUERY, protocol=Protocol.BOLT_ROUTING)
    if results:
        headers = list(results[0].keys())
        print(",".join(headers))
        for replica in results:
            values = [str(replica[h]) for h in headers]
            print(",".join(values))


def verify_replicas_ready() -> bool:
    results = execute_and_fetch(COORDINATOR, SHOW_REPLICAS_QUERY, protocol=Protocol.BOLT_ROUTING)
    all_ready = True

    for replica in results:
        name = replica.get("name", "unknown")
        data_info = replica.get("data_info", {})

        for db_name, db_status in data_info.items():
            status = db_status.get("status", "unknown")
            if status != "ready":
                print(f"WARN: Replica {name}, database {db_name} status is '{status}', expected 'ready'")
                all_ready = False

    return all_ready


def main():
    print("=" * 60)
    print("High-Concurrency Edge Write Workload")
    print("=" * 60)
    print(f"Nodes: {NUM_NODES}")
    print(f"Workers: {NUM_WORKERS}")
    print(f"Iterations per worker: {NUM_ITERATIONS}")
    print("-" * 60)

    stop_event = threading.Event()
    storage_thread = threading.Thread(target=storage_info_worker, args=(stop_event,), daemon=True)
    storage_thread.start()
    print(f"Started background STORAGE INFO worker (interval: {STORAGE_INFO_INTERVAL_SECONDS}s)")

    replicas_thread = threading.Thread(target=replicas_worker, args=(stop_event,), daemon=True)
    replicas_thread.start()
    print(f"Started background SHOW REPLICAS worker (interval: {SHOW_REPLICAS_INTERVAL_SECONDS}s)")

    total_start = time.time()

    try:
        create_indices_and_constraints()
        create_nodes()
        run_concurrent_edge_writes()
    finally:
        stop_event.set()
        storage_thread.join(timeout=5)
        replicas_thread.join(timeout=5)
        print("\nStopped background workers")

    total_elapsed = time.time() - total_start

    print("-" * 60)
    print(f"Total time: {total_elapsed:.1f}s ({total_elapsed/60:.1f} minutes)")

    result = execute_and_fetch(COORDINATOR, "MATCH (n:Node) RETURN count(n) as cnt", protocol=Protocol.BOLT_ROUTING)
    node_count = result[0]["cnt"] if result else 0

    result = execute_and_fetch(
        COORDINATOR, "MATCH ()-[r:RELATED_TO]->() RETURN count(r) as cnt", protocol=Protocol.BOLT_ROUTING
    )
    edge_count = result[0]["cnt"] if result else 0

    print(f"\nFinal counts: {node_count:,} nodes, {edge_count:,} edges")

    print("\nWaiting 30 seconds before final verification...")
    time.sleep(30)

    print("\nFinal replica status:")
    show_replicas()

    if verify_replicas_ready():
        print("\nAll replicas are in sync!")
        print("Workload completed successfully!")
    else:
        print("\nERROR: Not all replicas are in sync!")
        sys.exit(1)


if __name__ == "__main__":
    main()
