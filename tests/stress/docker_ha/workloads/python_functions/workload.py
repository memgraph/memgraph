#!/usr/bin/env python3
"""
MAGE function stress workload.
Creates 1000 nodes, then 8 workers each create an edge between every pair
of nodes in their partition AND call nxalg.ancestors / date.format on each iteration.
"""
import os
import sys
import threading
import time

import neo4j

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from common import (
    COORDINATOR_INSTANCES,
    INSTANCE_PORTS,
    Protocol,
    QueryType,
    create_bolt_driver_for,
    execute_and_fetch,
    execute_query,
    run_parallel,
)

COORDINATOR = "coord_1"

NUM_NODES = 1000
NUM_ANCESTORS_WORKERS = 4
NUM_DATE_FORMAT_WORKERS = 4
NUM_ITERATIONS = 100

STORAGE_INFO_INTERVAL_SECONDS = 5
SHOW_REPLICAS_INTERVAL_SECONDS = 5


def create_nodes() -> None:
    print(f"Creating {NUM_NODES} nodes...")

    query = f"""
UNWIND range(0, {NUM_NODES - 1}) AS i
CREATE (:Node {{uid: i, name: 'node_' + toString(i)}});"""

    start_time = time.time()
    execute_query(COORDINATOR, query, protocol=Protocol.BOLT_ROUTING, query_type=QueryType.WRITE)
    elapsed = time.time() - start_time
    print(f"  Created {NUM_NODES} nodes in {elapsed:.1f}s")

    print("Creating index on :Node(uid)...")
    execute_query(
        COORDINATOR, "CREATE INDEX ON :Node(uid);", protocol=Protocol.BOLT_ROUTING, query_type=QueryType.WRITE
    )


def ancestors_worker(worker_id: int) -> dict:
    total_workers = NUM_ANCESTORS_WORKERS + NUM_DATE_FORMAT_WORKERS

    query = f"""
MATCH (a:Node), (b:Node)
WHERE a.uid < b.uid
  AND (a.uid + b.uid) % {total_workers} = {worker_id}
MERGE (a)-[:RELATED_TO]->(b)
WITH a LIMIT 10
CALL nxalg.ancestors(a)
YIELD ancestors
RETURN ancestors;"""

    t0 = time.time()
    for iteration in range(NUM_ITERATIONS):
        execute_and_fetch(COORDINATOR, query, protocol=Protocol.BOLT_ROUTING, query_type=QueryType.WRITE)
        if (iteration + 1) % 10 == 0:
            print(f"  [ancestors-{worker_id}] {iteration + 1}/{NUM_ITERATIONS}")
    elapsed = time.time() - t0
    print(f"  [ancestors-{worker_id}] Completed {NUM_ITERATIONS} iterations in {elapsed:.1f}s")
    return {"worker_id": worker_id, "type": "ancestors", "elapsed": elapsed}


def date_format_worker(worker_id: int) -> dict:
    total_workers = NUM_ANCESTORS_WORKERS + NUM_DATE_FORMAT_WORKERS
    global_id = NUM_ANCESTORS_WORKERS + worker_id

    query = f"""
MATCH (a:Node), (b:Node)
WHERE a.uid < b.uid
  AND (a.uid + b.uid) % {total_workers} = {global_id}
MERGE (a)-[:RELATED_TO]->(b)
WITH a LIMIT 1
CALL date.format(74976, "h", "%Y/%m/%d %H:%M:%S %Z", "Mexico/BajaNorte")
YIELD formatted
RETURN formatted;"""

    t0 = time.time()
    for iteration in range(NUM_ITERATIONS):
        execute_and_fetch(COORDINATOR, query, protocol=Protocol.BOLT_ROUTING, query_type=QueryType.WRITE)
        if (iteration + 1) % 10 == 0:
            print(f"  [date_format-{worker_id}] {iteration + 1}/{NUM_ITERATIONS}")
    elapsed = time.time() - t0
    print(f"  [date_format-{worker_id}] Completed {NUM_ITERATIONS} iterations in {elapsed:.1f}s")
    return {"worker_id": worker_id, "type": "date_format", "elapsed": elapsed}


def dispatch_worker(fn_type: str, worker_id: int) -> dict:
    if fn_type == "ancestors":
        return ancestors_worker(worker_id)
    return date_format_worker(worker_id)


def run_all_workers() -> None:
    total_workers = NUM_ANCESTORS_WORKERS + NUM_DATE_FORMAT_WORKERS
    print(
        f"\nRunning {total_workers} workers ({NUM_ANCESTORS_WORKERS} ancestors + {NUM_DATE_FORMAT_WORKERS} date_format)..."
    )
    print(f"Each worker: MERGE edges + call MAGE function, {NUM_ITERATIONS} iterations")

    tasks = [("ancestors", i) for i in range(NUM_ANCESTORS_WORKERS)] + [
        ("date_format", i) for i in range(NUM_DATE_FORMAT_WORKERS)
    ]

    t0 = time.time()
    results = run_parallel(dispatch_worker, tasks, num_workers=total_workers)
    elapsed = time.time() - t0

    print(f"  All {total_workers} workers completed in {elapsed:.1f}s")


def storage_info_worker(stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        try:
            results = execute_and_fetch(
                COORDINATOR, "SHOW STORAGE INFO;", protocol=Protocol.BOLT_ROUTING, query_type=QueryType.READ
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
            instances = execute_and_fetch(COORDINATOR, "SHOW INSTANCES;")
            if instances:
                down = [inst["name"] for inst in instances if inst.get("health", "").lower() != "up"]
                if down:
                    print(f"\nFATAL: Instance(s) DOWN: {', '.join(down)}")
                    os._exit(1)
                print(f"\n[INSTANCES @ {time.strftime('%H:%M:%S')}] All {len(instances)} instances up")

            results = execute_and_fetch(COORDINATOR, "SHOW REPLICAS;", protocol=Protocol.BOLT_ROUTING)
            if results:
                headers = list(results[0].keys())
                print(f"[SHOW REPLICAS @ {time.strftime('%H:%M:%S')}]")
                print("  " + ",".join(headers))
                for replica in results:
                    print("  " + ",".join(str(replica[h]) for h in headers))
        except Exception as e:
            print(f"\n[SHOW REPLICAS/INSTANCES ERROR] {e}")

        stop_event.wait(SHOW_REPLICAS_INTERVAL_SECONDS)


def show_replicas() -> None:
    results = execute_and_fetch(COORDINATOR, "SHOW REPLICAS;", protocol=Protocol.BOLT_ROUTING)
    if results:
        headers = list(results[0].keys())
        print(",".join(headers))
        for replica in results:
            print(",".join(str(replica[h]) for h in headers))


def verify_replicas_ready() -> bool:
    results = execute_and_fetch(COORDINATOR, "SHOW REPLICAS;", protocol=Protocol.BOLT_ROUTING)
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
    print("MAGE Function Stress Workload")
    print("=" * 60)
    print(f"Neo4j driver version: {neo4j.__version__}")
    print(f"Nodes: {NUM_NODES}")
    print(f"Ancestors workers: {NUM_ANCESTORS_WORKERS}")
    print(f"Date format workers: {NUM_DATE_FORMAT_WORKERS}")
    print(f"Iterations per worker: {NUM_ITERATIONS}")
    print("-" * 60)

    stop_event = threading.Event()
    storage_thread = threading.Thread(target=storage_info_worker, args=(stop_event,), daemon=True)
    storage_thread.start()

    replicas_thread = threading.Thread(target=replicas_worker, args=(stop_event,), daemon=True)
    replicas_thread.start()

    total_start = time.time()

    try:
        create_nodes()
        run_all_workers()
    finally:
        stop_event.set()
        storage_thread.join(timeout=5)
        replicas_thread.join(timeout=5)
        print("\nStopped background workers")

    total_elapsed = time.time() - total_start

    print("-" * 60)
    print(f"Total time: {total_elapsed:.1f}s ({total_elapsed/60:.1f} minutes)")

    print("\nWaiting 10 seconds before final verification...")
    time.sleep(10)

    print("\nFinal replica status:")
    show_replicas()

    print("\nInstance reachability check:")
    all_reachable = True
    for name, port in INSTANCE_PORTS.items():
        try:
            driver = create_bolt_driver_for(name)
            with driver.session() as session:
                if name in COORDINATOR_INSTANCES:
                    results = session.run("SHOW INSTANCES;")
                    records = [r.data() for r in results]
                    print(f"  {name} (port {port}): OK ({len(records)} instances)")
                else:
                    session.run("RETURN 1;").consume()
                    print(f"  {name} (port {port}): OK")
            driver.close()
        except Exception as e:
            print(f"  {name} (port {port}): UNREACHABLE - {e}")
            all_reachable = False

    if not all_reachable:
        print("\nERROR: Not all instances are reachable!")
        sys.exit(1)

    if verify_replicas_ready():
        print("\nAll replicas are in sync and all instances reachable!")
        print("Workload completed successfully!")
    else:
        print("\nERROR: Not all replicas are in sync!")
        sys.exit(1)


if __name__ == "__main__":
    main()
