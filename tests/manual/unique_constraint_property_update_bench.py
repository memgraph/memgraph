#!/usr/bin/env python3

"""
Measures the cost of updating properties that take part in no unique
constraint, on vertices whose label does carry a unique constraint.

Every non-null property write used to put its vertex into the commit-time
unique constraint verification set, so a batch that only touched unrelated
properties still paid one constraint update and one validation per vertex
inside the storage engine lock. This script reproduces that workload:

    (:Node {id})<-[:VALUE_OF]-(:Value {id, v, ts, ...})

with unique constraints on Node(id) and Value(id), then runs batches of
1000 UPDATE-style writes that change only Value.v, Value.ts, and friends.
Batches never share a vertex, so the concurrent runs measure commit-path
serialization rather than write-write conflicts.

Run against a fresh, empty instance, one binary at a time:

    ./unique_constraint_property_update_bench.py --port 7687 --pid $(pgrep -x memgraph)

The --pid argument is optional and only used to report the CPU time the
server process consumed during the timed trials (read from /proc).
"""

import argparse
import multiprocessing
import os
import random
import statistics
import time

import mgclient

VERTICES = 100_000
BATCH_SIZE = 1_000
BATCHES = 48
TRIALS = 3
WRITER_COUNTS = (1, 12)
SEED = 20260907

SCHEMA = [
    "CREATE INDEX ON :Node(id)",
    "CREATE INDEX ON :Value(id)",
    "CREATE CONSTRAINT ON (n:Node) ASSERT n.id IS UNIQUE",
    "CREATE CONSTRAINT ON (v:Value) ASSERT v.id IS UNIQUE",
]

LOAD = (
    "UNWIND $ids AS id "
    "CREATE (n:Node {id: id, name: 'x'})<-[:VALUE_OF]-(:Value {id: id, v: 1.0, dv: '1.0', ts: 't', q: null})"
)

UPDATE = (
    "UNWIND $rows AS row "
    "MATCH (n:Node {id: row[0]})<-[:VALUE_OF]-(v:Value) "
    "SET v.prev_v = v.v, v.prev_dv = v.dv, v.prev_ts = v.ts, v.prev_q = v.q, "
    "    v.v = row[1], v.dv = row[2], v.ts = row[3], v.q = row[4] "
    "RETURN count(*)"
)


def connect(args):
    connection = mgclient.connect(host=args.host, port=args.port)
    connection.autocommit = True
    return connection


def execute(connection, query, params=None):
    cursor = connection.cursor()
    cursor.execute(query, params or {})
    return cursor.fetchall()


def process_cpu_seconds(pid):
    if pid is None:
        return 0.0
    with open(f"/proc/{pid}/stat") as stat:
        fields = stat.read().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def load(args, ids):
    connection = connect(args)
    for statement in SCHEMA:
        execute(connection, statement)
    for start in range(0, len(ids), 5_000):
        execute(connection, LOAD, {"ids": ids[start : start + 5_000]})
    constraints = {(row[1], tuple(row[2])) for row in execute(connection, "SHOW CONSTRAINT INFO") if row[0] == "unique"}
    assert {("Node", ("id",)), ("Value", ("id",))} <= constraints, constraints


def make_batches(ids, rng, batches):
    chosen = rng.sample(ids, batches * BATCH_SIZE)
    return [
        [
            [vertex_id, round(rng.random() * 100, 3), "x", "2026-01-01T00:00:00+00:00", None]
            for vertex_id in chosen[start : start + BATCH_SIZE]
        ]
        for start in range(0, len(chosen), BATCH_SIZE)
    ]


def writer(args, batches, results):
    """Runs its share of `batches` on its own connection, reports each latency or the first error, then signals completion."""
    try:
        connection = connect(args)
        for batch in batches:
            started = time.perf_counter()
            try:
                execute(connection, UPDATE, {"rows": batch})
            except Exception as error:  # noqa: BLE001 - report every failure, whatever its type
                results.put(("error", str(error)))
                return
            results.put(("latency", time.perf_counter() - started))
    finally:
        results.put(("done", None))


def trial(args, ids, rng, writers):
    # Separate processes, not threads: a client library that holds the GIL while it waits on the
    # socket would otherwise serialize the writers and hide the commit-path contention being measured.
    batches = make_batches(ids, rng, args.batches)
    shares = [batches[index::writers] for index in range(writers)]
    results = multiprocessing.Queue()
    processes = [multiprocessing.Process(target=writer, args=(args, share, results)) for share in shares]

    time.sleep(0.5)
    cpu_before = process_cpu_seconds(args.pid)
    wall_started = time.perf_counter()
    for process in processes:
        process.start()

    # Every writer ends with a completion message, so blocking reads until all of them have arrived
    # drain the queue deterministically; Queue.empty() is not reliable for that.
    latencies, errors = [], []
    remaining = writers
    while remaining:
        kind, value = results.get()
        if kind == "done":
            remaining -= 1
        elif kind == "latency":
            latencies.append(value)
        else:
            errors.append(value)
    wall = time.perf_counter() - wall_started
    for process in processes:
        process.join()
    cpu = process_cpu_seconds(args.pid) - cpu_before

    assert not errors and len(latencies) == args.batches, (errors, len(latencies))
    latencies.sort()
    p99 = latencies[min(len(latencies) - 1, int(round(0.99 * (len(latencies) - 1))))]
    return wall, cpu, statistics.median(latencies) * 1000, p99 * 1000


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7687)
    parser.add_argument("--pid", type=int, default=None, help="server pid, for CPU accounting via /proc")
    parser.add_argument(
        "--writers",
        default=",".join(str(count) for count in WRITER_COUNTS),
        help="comma-separated writer counts to sweep (default: %(default)s)",
    )
    parser.add_argument("--batches", type=int, default=BATCHES, help="batches per trial (default: %(default)s)")
    args = parser.parse_args()
    writer_counts = [int(count) for count in args.writers.split(",") if count]

    rng = random.Random(SEED)
    ids = [f"bench-{i:06d}" for i in range(VERTICES)]
    load(args, ids)
    print(f"loaded {VERTICES} Node/Value pairs; {args.batches} disjoint batches of {BATCH_SIZE} rows per trial")
    print(
        f"{'writers':>7}  {'wall_s':>7}  {'tx_per_s':>8}  {'rows_per_s':>10}  {'server_cpu_s':>12}  "
        f"{'cpu_ms_per_tx':>13}  {'tx_p50_ms':>9}  {'tx_p99_ms':>9}   (medians of {TRIALS} trials)"
    )
    for writers in writer_counts:
        trial(args, ids, rng, writers)  # warm-up, not reported
        results = [trial(args, ids, rng, writers) for _ in range(TRIALS)]
        wall = statistics.median(r[0] for r in results)
        cpu = statistics.median(r[1] for r in results)
        p50 = statistics.median(r[2] for r in results)
        p99 = statistics.median(r[3] for r in results)
        tx_per_s = args.batches / wall
        print(
            f"{writers:>7}  {wall:>7.2f}  {tx_per_s:>8.1f}  {tx_per_s * BATCH_SIZE:>10.0f}  {cpu:>12.2f}  "
            f"{cpu * 1000 / args.batches:>13.2f}  {p50:>9.1f}  {p99:>9.1f}"
        )


if __name__ == "__main__":
    main()
