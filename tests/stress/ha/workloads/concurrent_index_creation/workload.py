#!/usr/bin/env python3
"""
Concurrent index creation workload.

20 ingestion workers each insert 1,000,000 nodes (in batches of 10) while
1 index worker concurrently creates 100 label×property index combinations.
Each node carries 10 labels (L0..L9) and 10 integer properties (p0..p9).

The full workload is repeated 10 times with a cleanup between each run.
"""
import sys
import time

from cluster_monitor import ClusterMonitor
from ha_common import Protocol, QueryType, cleanup, execute_and_fetch, execute_query, run_parallel

COORDINATOR = "coord_1"
COORDINATORS = ["coord_1", "coord_2", "coord_3"]

NUM_RUNS = 10
NUM_INGESTION_WORKERS = 20
NODES_PER_WORKER = 500_000 // NUM_INGESTION_WORKERS  # 25_000
BATCH_SIZE = 10
LABELS = [f"L{i}" for i in range(10)]
PROPERTIES = [f"p{i}" for i in range(10)]

NUM_BATCHES_PER_WORKER = NODES_PER_WORKER // BATCH_SIZE  # 100,000

# Sentinel value identifying the index creation worker inside worker_dispatcher
INDEX_WORKER_ID = -1

# Cypher query for batch node insertion — labels and properties are hardcoded for performance
_INSERT_QUERY = """
UNWIND $nodes AS n
CREATE (:L0:L1:L2:L3:L4:L5:L6:L7:L8:L9 {
    p0: n.p0, p1: n.p1, p2: n.p2, p3: n.p3, p4: n.p4,
    p5: n.p5, p6: n.p6, p7: n.p7, p8: n.p8, p9: n.p9
})
"""


def ingest_worker(worker_id: int) -> dict:
    """Insert NODES_PER_WORKER nodes in BATCH_SIZE-sized batches."""
    base_uid = worker_id * NODES_PER_WORKER
    t0 = time.time()

    for batch in range(NUM_BATCHES_PER_WORKER):
        batch_base = base_uid + batch * BATCH_SIZE
        nodes = [
            {
                "p0": batch_base + i,
                "p1": (batch_base + i) % 1_000_000,
                "p2": (batch_base + i) % 10_000,
                "p3": worker_id,
                "p4": i,
                "p5": f"node_{batch_base + i}",
                "p6": f"worker_{worker_id}_batch_{batch % 1_000}",
                "p7": f"label_{(batch_base + i) % 10}",
                "p8": f"prop_{(batch_base + i) % 100}",
                "p9": f"val_{(batch_base + i) % 1_000_000}",
            }
            for i in range(BATCH_SIZE)
        ]
        execute_query(
            COORDINATOR,
            _INSERT_QUERY,
            params={"nodes": nodes},
            protocol=Protocol.BOLT_ROUTING,
            query_type=QueryType.WRITE,
        )
        if (batch + 1) % 10_000 == 0:
            elapsed = time.time() - t0
            print(f"  [Worker {worker_id:>2}] {batch + 1:>7,}/{NUM_BATCHES_PER_WORKER:,} batches  ({elapsed:.0f}s)")

    elapsed = time.time() - t0
    print(f"  [Worker {worker_id:>2}] Done — {NODES_PER_WORKER:,} nodes in {elapsed:.1f}s")
    return {"worker_id": worker_id, "elapsed": elapsed}


def index_creation_worker(worker_id: int) -> dict:
    """Create 100 label×property index combinations while ingestion is running."""
    created = 0
    failed = 0
    t0 = time.time()

    for label in LABELS:
        for prop in PROPERTIES:
            query = f"CREATE INDEX ON :{label}({prop});"
            try:
                execute_query(
                    COORDINATOR,
                    query,
                    protocol=Protocol.BOLT_ROUTING,
                    query_type=QueryType.WRITE,
                )
                created += 1
                print(f"  [Index Worker] Created index ON :{label}({prop})")
                time.sleep(0.5)
            except Exception as e:
                failed += 1
                print(f"  [Index Worker] Failed index ON :{label}({prop}): {e}")

    elapsed = time.time() - t0
    print(f"  [Index Worker] Finished: {created} created, {failed} failed in {elapsed:.1f}s")
    return {"worker_id": worker_id, "elapsed": elapsed, "indices_created": created, "indices_failed": failed}


def worker_dispatcher(worker_id: int) -> dict:
    """Route to index_creation_worker or ingest_worker based on worker_id."""
    if worker_id == INDEX_WORKER_ID:
        return index_creation_worker(worker_id)
    return ingest_worker(worker_id)


def run_concurrent_workload() -> None:
    total_workers = NUM_INGESTION_WORKERS + 1
    print(f"\nStarting {NUM_INGESTION_WORKERS} ingestion workers + 1 index worker ({total_workers} total)...")
    tasks = [(INDEX_WORKER_ID,)] + [(i,) for i in range(NUM_INGESTION_WORKERS)]

    t0 = time.time()
    run_parallel(worker_dispatcher, tasks, num_workers=total_workers)
    elapsed = time.time() - t0

    print(f"\nAll workers completed in {elapsed:.1f}s ({elapsed / 60:.1f} min)")


def verify_indices(run_index: int) -> None:
    rows = execute_and_fetch(
        COORDINATOR,
        "SHOW INDEX INFO;",
        protocol=Protocol.BOLT_ROUTING,
    )
    present = {(row["label"], row["property"]) for row in rows if row.get("property") is not None}
    expected = {(label, prop) for label in LABELS for prop in PROPERTIES}
    missing = expected - present
    extra = present - expected

    if missing:
        print(f"ERROR: {len(missing)} indices missing after run {run_index}:")
        for label, prop in sorted(missing):
            print(f"  :{label}({prop})")
        sys.exit(1)

    if extra:
        print(f"WARN: {len(extra)} unexpected indices found after run {run_index}:")
        for label, prop in sorted(extra):
            print(f"  :{label}({prop})")

    print(f"Index check OK — all {len(expected)} expected indices present.")


def run_one(monitor: ClusterMonitor, run_index: int) -> None:
    total_nodes = NUM_INGESTION_WORKERS * NODES_PER_WORKER
    print(f"\n{'=' * 60}")
    print(f"Run {run_index}/{NUM_RUNS}")
    print(f"{'=' * 60}")

    run_start = time.time()
    run_concurrent_workload()
    run_elapsed = time.time() - run_start

    print(f"\nRun {run_index} wall time: {run_elapsed:.1f}s ({run_elapsed / 60:.1f} min)")

    result = execute_and_fetch(
        COORDINATOR,
        "MATCH (n:L0) RETURN count(n) AS cnt",
        protocol=Protocol.BOLT_ROUTING,
    )
    node_count = result[0]["cnt"] if result else 0
    print(f"Node count (via :L0): {node_count:,}  (expected: {total_nodes:,})")

    monitor.show_replicas()

    ok = monitor.verify_all_ready() and monitor.verify_instances_up()
    if not ok:
        print(f"ERROR: Cluster is not healthy after run {run_index}!")
        sys.exit(1)

    if node_count != total_nodes:
        print(f"ERROR: Node count mismatch after run {run_index} — got {node_count:,}, expected {total_nodes:,}")
        sys.exit(1)

    verify_indices(run_index)

    print(f"Run {run_index} OK — cleaning up...")
    cleanup()


def main():
    total_nodes = NUM_INGESTION_WORKERS * NODES_PER_WORKER
    print("=" * 60)
    print("Concurrent Index Creation Workload")
    print("=" * 60)
    print(f"Runs              : {NUM_RUNS}")
    print(f"Ingestion workers : {NUM_INGESTION_WORKERS}")
    print(f"Nodes per worker  : {NODES_PER_WORKER:,}  (total per run: {total_nodes:,})")
    print(f"Batch size        : {BATCH_SIZE}")
    print(f"Labels per node   : {len(LABELS)}  ({', '.join(LABELS)})")
    print(f"Props per node    : {len(PROPERTIES)}  ({', '.join(PROPERTIES)})")
    print(f"Index combos      : {len(LABELS) * len(PROPERTIES)}")
    print("-" * 60)

    monitor = ClusterMonitor(
        coordinators=COORDINATORS,
        show_replicas=True,
        verify_up=True,
        storage_info=["vertex_count", "memory_res", "allocation_limit"],
        metrics_info=["FailedQuery"],
        interval=2,
    )

    total_start = time.time()
    try:
        with monitor:
            for run_index in range(1, NUM_RUNS + 1):
                run_one(monitor, run_index)

        total_elapsed = time.time() - total_start
        print("=" * 60)
        print(f"All {NUM_RUNS} runs completed in {total_elapsed:.1f}s ({total_elapsed / 60:.1f} min)")
        print("Workload completed successfully!")
    except SystemExit:
        raise
    finally:
        cleanup()


if __name__ == "__main__":
    main()
