#!/usr/bin/env python3

"""
Worker liveness under a saturated pool with the pipelined commit enabled.

Starts a server with --experimental-enabled=lockfree-read-snapshot,pipelined-commit
and a small, explicitly configured Bolt worker count, then drives real storage
commits through the pool from 16 connections while the head committer is parked
in its encode stage by an out-of-band controller: the server reads the
MG_TEST_PIPELINED_S2_PARK_FIFO environment variable and the first eligible
committer after MG_TEST_PIPELINED_S2_PARK_SKIP others polls, with a 10 ms
sleep, until that file exists. Nothing goes
through the saturated worker pool to release it. A 17th connection issues a
read-only query while the writers are queued behind the head; the script
records how long that read waited for admission, releases the head, and asserts
that every writer and the reader finish within a bound. PERIODIC COMMIT and a
before-commit trigger are exercised through the interpreter afterwards.

    ./pipelined_commit_worker_liveness.py --binary build/memgraph --workers 4

The read has no admission guarantee while every worker is blocked in a
synchronous commit; the delay is reported, not asserted.
"""

import argparse
import multiprocessing
import os
import queue
import shutil
import subprocess
import sys
import tempfile
import time

import mgclient

WRITERS = 16
PORT = 7699


def connect(port):
    connection = mgclient.connect(host="127.0.0.1", port=port)
    connection.autocommit = True
    return connection


def execute(connection, query, params=None):
    cursor = connection.cursor()
    cursor.execute(query, params or {})
    return cursor.fetchall()


def wait_for_port(port, timeout=60):
    import socket

    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket() as sock:
            sock.settimeout(0.5)
            try:
                sock.connect(("127.0.0.1", port))
                return True
            except OSError:
                time.sleep(0.2)
    return False


def writer(port, index, started, results):
    try:
        connection = connect(port)
        execute(connection, "MATCH (n:Node {id: $id}) SET n.v = n.v + 1 RETURN n.v", {"id": index})
        results.put(("writer", index, time.perf_counter() - started))
    except Exception as error:  # noqa: BLE001
        results.put(("error", f"writer {index}", str(error)))


def reader(port, results):
    try:
        connection = connect(port)
        read_started = time.perf_counter()
        execute(connection, "MATCH (n:Node) RETURN count(n)")
        results.put(("reader", 0, time.perf_counter() - read_started))
    except Exception as error:  # noqa: BLE001
        results.put(("error", "reader", str(error)))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--workers", type=int, default=4, help="Bolt worker count for the server")
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument("--bound", type=float, default=30.0, help="seconds every writer and the reader must finish in")
    args = parser.parse_args()

    workdir = tempfile.mkdtemp(prefix="mg_liveness_")
    park_file = os.path.join(workdir, "release-head")
    # The seeding query below is the first eligible commit; the parked head is the one after it.
    env = dict(os.environ, MG_TEST_PIPELINED_S2_PARK_FIFO=park_file, MG_TEST_PIPELINED_S2_PARK_SKIP="1")
    server = subprocess.Popen(
        [
            args.binary,
            f"--data-directory={workdir}/data",
            f"--bolt-port={args.port}",
            f"--bolt-num-workers={args.workers}",
            "--log-level=WARNING",
            f"--log-file={workdir}/memgraph.log",
            "--also-log-to-stderr=false",
            "--telemetry-enabled=false",
            "--storage-snapshot-on-exit=false",
            "--storage-snapshot-interval-sec=86400",
            "--storage-wal-enabled=true",
            "--query-modules-directory=",
            "--experimental-enabled=lockfree-read-snapshot,pipelined-commit",
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        assert wait_for_port(args.port), "server did not start"
        setup = connect(args.port)
        execute(setup, "CREATE INDEX ON :Node(id)")
        execute(setup, "UNWIND range(0, 99) AS i CREATE (:Node {id: i, v: 0})")
        print(f"server pid {server.pid}, {args.workers} bolt workers, {WRITERS} writers + 1 reader")

        # Separate processes, not threads: the client library holds the GIL while it waits on the socket, so
        # threads would never let this controller run while the writers are blocked.
        results = multiprocessing.Queue()
        started = time.perf_counter()
        writers = [
            multiprocessing.Process(target=writer, args=(args.port, index, started, results))
            for index in range(WRITERS)
        ]
        for process in writers:
            process.start()
        # The head is parked in its encode stage; the other writers queue behind it (gate or pool).
        time.sleep(1.0)
        reader_process = multiprocessing.Process(target=reader, args=(args.port, results))
        reader_process.start()
        time.sleep(1.0)
        with open(park_file, "w") as release:
            release.write("go\n")
        release_at = time.perf_counter() - started
        finished, errors, read_result = {}, [], {}
        deadline = time.time() + args.bound
        while len(finished) + len(errors) < WRITERS or ("admission_s" not in read_result and not errors):
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            try:
                kind, key, value = results.get(timeout=remaining)
            except queue.Empty:
                break
            if kind == "writer":
                finished[key] = value
            elif kind == "reader":
                read_result["admission_s"] = value
            else:
                errors.append(f"{kind} {key}: {value}")
        for process in writers:
            process.join(timeout=5)
        reader_process.join(timeout=5)
        assert not errors, errors
        assert len(finished) == WRITERS, f"only {len(finished)} writers finished within {args.bound}s"
        assert "admission_s" in read_result, f"the reader did not finish within {args.bound}s"
        last = max(finished.values())
        print(f"head released at {release_at * 1000:.0f} ms; last writer finished at {last * 1000:.0f} ms")
        print(f"reader admission delay during saturation: {read_result['admission_s'] * 1000:.0f} ms")
        assert last - release_at < args.bound, "writers did not complete in bound after the release"

        # Interpreter paths that commit through the same storage path.
        rows = execute(setup, "USING PERIODIC COMMIT 100 UNWIND range(0, 499) AS i CREATE (:Periodic {i: i})")
        assert execute(setup, "MATCH (p:Periodic) RETURN count(p)")[0][0] == 500, rows
        execute(
            setup,
            "CREATE TRIGGER guard ON () CREATE BEFORE COMMIT EXECUTE " "UNWIND createdVertices AS v SET v.seen = true",
        )
        execute(setup, "CREATE (:Guarded {i: 1})")
        assert execute(setup, "MATCH (g:Guarded) RETURN g.seen")[0][0] is True
        execute(setup, "DROP TRIGGER guard")
        counters = {row[0]: row[1] for row in execute(setup, "SHOW STORAGE INFO ON CURRENT DATABASE")}
        print(
            "pipelined_commit_s2_encodes",
            counters.get("pipelined_commit_s2_encodes"),
            "budget_fallbacks",
            counters.get("pipelined_commit_budget_fallbacks"),
        )
        assert counters.get("pipelined_commit_s2_encodes", 0) >= WRITERS
        print("OK")
    finally:
        server.terminate()
        try:
            server.wait(timeout=30)
        except subprocess.TimeoutExpired:
            server.kill()
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
