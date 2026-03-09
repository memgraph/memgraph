#!/usr/bin/env python3
"""
Responsive reads workload.

Creates 10M nodes in chunks while a read worker continuously runs
SHOW INDEXES and SHOW METRICS in the main thread. Each read query
must complete in under 1 second or the test fails immediately.
"""
import sys
import threading
import time

from cluster_monitor import ClusterMonitor
from ha_common import Protocol, QueryType, cleanup, execute_and_fetch, execute_query

COORDINATOR = "coord_1"
COORDINATORS = ["coord_1", "coord_2", "coord_3"]

TOTAL_NODES = 10_000_000
LABELS = [f"L{i}" for i in range(10)]
PROPERTIES = [f"p{i}" for i in range(10)]

SETUP_CHUNK = 1_000_000  # nodes per CREATE batch during setup

MAX_READ_LATENCY_S = 1.0


def _create_nodes(done: threading.Event, error: list) -> None:
    """Create TOTAL_NODES nodes with labels and id property, in chunks."""
    try:
        execute_query(
            COORDINATOR,
            "CREATE INDEX ON :L0(id);",
            protocol=Protocol.BOLT_ROUTING,
            query_type=QueryType.WRITE,
        )
        t0 = time.time()
        for start in range(0, TOTAL_NODES, SETUP_CHUNK):
            end = min(start + SETUP_CHUNK, TOTAL_NODES)
            execute_query(
                COORDINATOR,
                "UNWIND range($start, $end - 1) AS id CREATE (:L0:L1:L2:L3:L4:L5:L6:L7:L8:L9 {id: id})",
                params={"start": start, "end": end},
                protocol=Protocol.BOLT_ROUTING,
                query_type=QueryType.WRITE,
            )
            elapsed = time.time() - t0
            print(f"  [Ingestion] {end:>10,}/{TOTAL_NODES:,} nodes  ({elapsed:.0f}s)")
        elapsed = time.time() - t0
        print(f"  [Ingestion] Done: {TOTAL_NODES:,} nodes in {elapsed:.1f}s")
    except Exception as e:
        error[0] = e
    finally:
        done.set()


def _timed_fetch(query: str) -> float:
    """Run a read query and return the elapsed time in seconds."""
    t0 = time.time()
    execute_and_fetch(
        COORDINATOR,
        query,
        protocol=Protocol.BOLT_ROUTING,
    )
    return time.time() - t0


def run_workload() -> None:
    """Create nodes in a background thread while the main thread polls read queries."""
    ingestion_done = threading.Event()
    ingestion_error = [None]

    print(f"\nCreating {TOTAL_NODES:,} nodes + read worker in parallel...")

    t0 = time.time()
    ingestion_thread = threading.Thread(target=_create_nodes, args=(ingestion_done, ingestion_error), daemon=True)
    ingestion_thread.start()

    # Read worker loop — runs in main thread until ingestion finishes
    queries = ["SHOW INDEXES;", "SHOW METRICS;"]
    iterations = 0
    max_latency = 0.0

    while not ingestion_done.is_set():
        for query in queries:
            latency = _timed_fetch(query)
            max_latency = max(max_latency, latency)
            iterations += 1

            if latency > MAX_READ_LATENCY_S:
                print(
                    f"  [Read Worker] FAIL: '{query}' took {latency:.3f}s "
                    f"(limit: {MAX_READ_LATENCY_S}s) on iteration {iterations}"
                )
                sys.exit(1)

            if iterations % 5 == 0:
                print(f"  [Read Worker] {iterations} queries OK (max latency so far: {max_latency:.3f}s)")

            time.sleep(0.5)

            if ingestion_done.is_set():
                break

    ingestion_thread.join()
    elapsed = time.time() - t0

    if ingestion_error[0] is not None:
        print(f"ERROR: Ingestion failed: {ingestion_error[0]}")
        sys.exit(1)

    print(f"  [Read Worker] Finished: {iterations} queries in {elapsed:.1f}s (max latency: {max_latency:.3f}s)")
    print(f"\nAll done in {elapsed:.1f}s ({elapsed / 60:.1f} min)")


def main():
    print("=" * 60)
    print("Responsive Reads Workload")
    print("=" * 60)
    print(f"Total nodes       : {TOTAL_NODES:,}")
    print(f"Chunk size        : {SETUP_CHUNK:,}")
    print(f"Labels per node   : {len(LABELS)}  ({', '.join(LABELS)})")
    print(f"Props per node    : {len(PROPERTIES)}  ({', '.join(PROPERTIES)})")
    print(f"Max read latency  : {MAX_READ_LATENCY_S}s")
    print("-" * 60)

    monitor = ClusterMonitor(
        coordinators=COORDINATORS,
        show_replicas=True,
        verify_up=True,
        storage_info=["vertex_count", "memory_res", "allocation_limit"],
        interval=2,
    )

    total_start = time.time()
    try:
        with monitor:
            run_workload()

            result = execute_and_fetch(
                COORDINATOR,
                "MATCH (n:L0) RETURN count(n) AS cnt",
                protocol=Protocol.BOLT_ROUTING,
            )
            node_count = result[0]["cnt"] if result else 0
            print(f"Node count (via :L0): {node_count:,}  (expected: {TOTAL_NODES:,})")

            monitor.show_replicas()

            ok = monitor.verify_all_ready() and monitor.verify_instances_up()
            if not ok:
                print("ERROR: Cluster is not healthy!")
                sys.exit(1)

            if node_count != TOTAL_NODES:
                print(f"ERROR: Node count mismatch — got {node_count:,}, expected {TOTAL_NODES:,}")
                sys.exit(1)

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
