#!/usr/bin/env python3
"""
Concurrent index creation workload.

Setup: 10M nodes (L0..L9, id property only) are pre-created before each run.
Concurrent phase: 20 workers each SET properties on 100 batches of 10 nodes
from their modulus partition (id % NUM_INGESTION_WORKERS == worker_id), while
1 index worker concurrently creates 100 label×property index combinations.

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
TOTAL_NODES = 10_000_000
BATCH_SIZE = 10
NUM_BATCHES_PER_WORKER = 100  # 100 batches × 10 nodes = 1000 nodes per worker
LABELS = [f"L{i}" for i in range(10)]
PROPERTIES = [f"p{i}" for i in range(10)]

SETUP_CHUNK = 1_000_000  # nodes per CREATE batch during setup

# Sentinel value identifying the index creation worker inside worker_dispatcher
INDEX_WORKER_ID = -1

_SET_QUERY = """
UNWIND $ids AS node_id
MATCH (n:L0 {id: node_id})
SET n.p0 = node_id,
    n.p1 = node_id % 1_000_000,
    n.p2 = node_id % 10_000,
    n.p3 = node_id % 1_000,
    n.p4 = node_id % 10,
    n.p5 = 'node_' + toString(node_id),
    n.p6 = 'mod_' + toString(node_id % 1_000_000),
    n.p7 = 'label_' + toString(node_id % 10),
    n.p8 = 'prop_' + toString(node_id % 100),
    n.p9 = 'val_' + toString(node_id % 1_000_000)
"""


def setup_nodes() -> None:
    """Create TOTAL_NODES nodes with labels and id property only, in chunks."""
    print(f"Creating {TOTAL_NODES:,} nodes (chunk size: {SETUP_CHUNK:,})...")
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
        if (start // SETUP_CHUNK + 1) % 100 == 0:
            elapsed = time.time() - t0
            print(f"  Setup: {end:>10,}/{TOTAL_NODES:,} nodes  ({elapsed:.0f}s)")
    elapsed = time.time() - t0
    print(f"  Setup done: {TOTAL_NODES:,} nodes in {elapsed:.1f}s")


def ingest_worker(worker_id: int) -> dict:
    """SET properties on 100 batches of 10 nodes from this worker's modulus partition."""
    t0 = time.time()

    for batch in range(NUM_BATCHES_PER_WORKER):
        # Worker w owns nodes where id % NUM_INGESTION_WORKERS == worker_id.
        # Batch b covers the b-th group of BATCH_SIZE consecutive ids in that partition.
        ids = [worker_id + (batch * BATCH_SIZE + i) * NUM_INGESTION_WORKERS for i in range(BATCH_SIZE)]
        execute_query(
            COORDINATOR,
            _SET_QUERY,
            params={"ids": ids},
            protocol=Protocol.BOLT_ROUTING,
            query_type=QueryType.WRITE,
            apply_retry_mechanism=True,
        )

    elapsed = time.time() - t0
    print(f"  [Worker {worker_id:>2}] Done — {NUM_BATCHES_PER_WORKER * BATCH_SIZE} nodes updated in {elapsed:.1f}s")
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
                    apply_retry_mechanism=True,
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
    print(f"\n{'=' * 60}")
    print(f"Run {run_index}/{NUM_RUNS}")
    print(f"{'=' * 60}")

    setup_nodes()

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
    print(f"Node count (via :L0): {node_count:,}  (expected: {TOTAL_NODES:,})")

    monitor.show_replicas()

    ok = monitor.verify_all_ready() and monitor.verify_instances_up()
    if not ok:
        print(f"ERROR: Cluster is not healthy after run {run_index}!")
        sys.exit(1)

    if node_count != TOTAL_NODES:
        print(f"ERROR: Node count mismatch after run {run_index} — got {node_count:,}, expected {TOTAL_NODES:,}")
        sys.exit(1)

    verify_indices(run_index)

    print(f"Run {run_index} OK — cleaning up...")
    cleanup()


def main():
    print("=" * 60)
    print("Concurrent Index Creation Workload")
    print("=" * 60)
    print(f"Runs              : {NUM_RUNS}")
    print(f"Total nodes       : {TOTAL_NODES:,}")
    print(f"Ingestion workers : {NUM_INGESTION_WORKERS}")
    print(f"Batches per worker: {NUM_BATCHES_PER_WORKER}  ({NUM_BATCHES_PER_WORKER * BATCH_SIZE} nodes per worker)")
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
        metrics_info=["FailedQuery", "TransientErrors"],
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
