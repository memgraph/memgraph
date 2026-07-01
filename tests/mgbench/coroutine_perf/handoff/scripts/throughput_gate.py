#!/usr/bin/env python3
"""c3.4 §6.2 — throughput-under-concurrency A/B: PARK vs BLOCK.

Measures whether parking the coordinator (freeing its worker) improves aggregate throughput when
many parallel queries run concurrently, WITHOUT regressing single-query latency.

Same binary, two modes via the split-point knob:
  PARK  (this branch, default): --query-coroutine-yield-ops="Aggregate,OrderBy,Accumulate,Distinct,HashJoin"
  BLOCK (baseline)            : --query-coroutine-yield-ops=""   (Sync => WaitOrSteal blocks)

This script launches memgraph itself (once per mode), loads a dataset, then:
  1. single-query latency (p50/p95) for a few query shapes;
  2. throughput (completed parallel queries/sec) at each client-concurrency level.

Usage:
  throughput_gate.py --port 7712 --workers 8 --clients 1,2,4,8,16 --dataset 400000 --repeats 5
Requires: MEMGRAPH_ORGANIZATION_NAME + MEMGRAPH_ENTERPRISE_LICENSE in the environment (enterprise).
          A Release memgraph at ${MG_BIN:-./build/memgraph}. Python venv tests/ve3 (neo4j driver).

For PMU counters (instructions/query, IPC) wrap the load window with `perf stat` on bare metal
(see ../perf_pmu.sh). This script reports wall-clock latency/throughput only.
"""
import argparse
import os
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time

from neo4j import GraphDatabase

PARK_KNOB = "Aggregate,OrderBy,Accumulate,Distinct,HashJoin"
BLOCK_KNOB = ""  # empty => every cursor Sync => coordinator blocks in WaitOrSteal

MG_BIN = os.environ.get("MG_BIN", "./build/memgraph")


def wait_port(port, timeout_s=120):
    for _ in range(timeout_s):
        try:
            s = socket.socket()
            s.settimeout(1)
            s.connect(("127.0.0.1", port))
            s.close()
            return True
        except OSError:
            time.sleep(1)
    return False


def launch(port, knob, data_dir):
    env = dict(os.environ)
    for k in ("MEMGRAPH_ORGANIZATION_NAME", "MEMGRAPH_ENTERPRISE_LICENSE"):
        if not env.get(k):
            sys.exit(f"ERROR: export {k} first (enterprise feature)")
    args = [
        MG_BIN,
        f"--data-directory={data_dir}",
        f"--bolt-port={port}",
        f"--monitoring-port={port + 100}",
        f"--metrics-port={port + 400}",
        "--log-level=WARNING",
        f"--query-coroutine-yield-ops={knob}",
    ]
    # new process group so we can kill the whole tree cleanly
    p = subprocess.Popen(args, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    if not wait_port(port):
        p.send_signal(signal.SIGKILL)
        sys.exit(f"server did not come up on {port} (knob={knob!r})")
    return p


def kill(p):
    try:
        os.killpg(os.getpgid(p.pid), signal.SIGKILL)
    except ProcessLookupError:
        pass


def measure_mode(port, par, dataset, clients, repeats):
    d = GraphDatabase.driver(f"bolt://127.0.0.1:{port}", auth=("", ""), max_connection_pool_size=64)

    def run(q):
        with d.session() as s:
            return [r.values() for r in s.run(q)]

    run("MATCH (n) DETACH DELETE n")
    made = 0
    while made < dataset:
        b = min(50000, dataset - made)
        run(f"UNWIND range({made + 1},{made + b}) AS i CREATE (:N {{p:i, g:i%7}})")
        made += b

    shapes = {
        "agg": f"USING PARALLEL EXECUTION {par} MATCH (n:N) RETURN count(n), sum(n.p)",
        "grp": f"USING PARALLEL EXECUTION {par} MATCH (n:N) RETURN n.g AS g, count(*) AS c ORDER BY g",
        "ord": f"USING PARALLEL EXECUTION {par} MATCH (n:N) RETURN n.p ORDER BY n.p DESC LIMIT 10",
    }

    # 1. single-query latency (warm)
    lat = {}
    for name, q in shapes.items():
        for _ in range(3):
            run(q)  # warm
        samples = []
        for _ in range(max(20, repeats * 4)):
            t0 = time.perf_counter()
            run(q)
            samples.append((time.perf_counter() - t0) * 1000.0)
        samples.sort()
        lat[name] = (statistics.median(samples), samples[int(len(samples) * 0.95)])

    # 2. throughput under concurrency (use the "agg" shape)
    q = shapes["agg"]
    tput = {}
    for c in clients:
        best = 0.0
        for _ in range(repeats):
            done = [0]
            stop = threading.Event()

            def worker():
                dd = GraphDatabase.driver(f"bolt://127.0.0.1:{port}", auth=("", ""))
                cnt = 0
                while not stop.is_set():
                    with dd.session() as s:
                        list(s.run(q))
                    cnt += 1
                dd.close()
                done[0] += cnt

            threads = [threading.Thread(target=worker) for _ in range(c)]
            for t in threads:
                t.start()
            window = 5.0
            time.sleep(window)
            stop.set()
            for t in threads:
                t.join()
            best = max(best, done[0] / window)
        tput[c] = best
    d.close()
    return lat, tput


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=7712)
    ap.add_argument(
        "--workers",
        type=int,
        default=8,
        help="(informational) intended pool size; set via memgraph flags if applicable",
    )
    ap.add_argument("--clients", default="1,2,4,8,16")
    ap.add_argument("--dataset", type=int, default=400000)
    ap.add_argument("--par", type=int, default=4)
    ap.add_argument("--repeats", type=int, default=5)
    args = ap.parse_args()
    clients = [int(x) for x in args.clients.split(",")]

    results = {}
    for mode, knob in (("BLOCK", BLOCK_KNOB), ("PARK", PARK_KNOB)):
        data_dir = f"/tmp/mg_handoff_tput_{mode.lower()}"
        subprocess.run(["rm", "-rf", data_dir])
        os.makedirs(data_dir, exist_ok=True)
        print(f"\n=== launching {mode} (knob={knob!r}) ===", flush=True)
        p = launch(args.port, knob, data_dir)
        try:
            results[mode] = measure_mode(args.port, args.par, args.dataset, clients, args.repeats)
        finally:
            kill(p)
            time.sleep(3)

    # report
    print("\n================ A/B RESULT ================")
    print("Single-query latency (ms, p50 / p95) — PARK must NOT regress vs BLOCK:")
    for name in ("agg", "grp", "ord"):
        b50, b95 = results["BLOCK"][0][name]
        p50, p95 = results["PARK"][0][name]
        print(
            f"  {name:>4}: BLOCK {b50:6.1f}/{b95:6.1f}   PARK {p50:6.1f}/{p95:6.1f}   "
            f"Δp50={100*(p50-b50)/b50:+.1f}%"
        )
    print("\nThroughput (parallel queries/sec) — PARK should WIN or TIE, esp. at high concurrency:")
    for c in clients:
        b = results["BLOCK"][1][c]
        p = results["PARK"][1][c]
        print(f"  clients={c:>3}: BLOCK {b:8.1f}   PARK {p:8.1f}   Δ={100*(p-b)/b:+.1f}%")
    print("\nDecision: ship if PARK holds single-query latency (within noise) AND ties/wins throughput.")
    print("If PARK regresses, revisit split-point placement (see APPROACH_B.md).")


if __name__ == "__main__":
    main()
