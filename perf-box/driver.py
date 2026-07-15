#!/usr/bin/env python3
"""Stand up a versioned branch (realistic churn) and hammer the shortest-path BFS
in a tight loop on either the MAIN or the BRANCH session, so an external `perf record`
can attribute the branch overhead to real symbols. Prints the memgraph PID to --pidfile."""
import argparse
import os
import random
import subprocess
import sys
import time

import bench as vb  # renamed copy of versioning_bench.py

BFS = (
    "MATCH (n:User {id:$from}), (m:User {id:$to}) WITH n, m "
    "MATCH p=(n)-[*BFS..15]->(m) RETURN [x IN nodes(p) | x.id] AS path"
)
# Traversal-DOMINATED query: 2-hop fan-out from a start, visits thousands of vertices per
# query so per-query fixed cost (planning/bolt) is amortized -> clean per-vertex profile.
DEEP = "MATCH (s:User {id:$from})-->()-->(n:User) RETURN count(n) AS c"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mg", required=True)
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--index", required=True)
    ap.add_argument("--data", required=True)
    ap.add_argument("--port", type=int, default=7720)
    ap.add_argument("--mode", choices=["main", "branch"], required=True)
    ap.add_argument("--pidfile", required=True)
    ap.add_argument("--duration", type=float, default=60.0)
    ap.add_argument("--query", choices=["bfs", "deep"], default="bfs")
    args = ap.parse_args()
    QUERY = BFS if args.query == "bfs" else DEEP

    vb.select_churn("realistic")
    from neo4j import GraphDatabase

    subprocess.run(["rm", "-rf", args.data_dir], check=False)
    os.makedirs(args.data_dir, exist_ok=True)
    logf = open(os.path.join(args.data_dir, "mg.log"), "w")
    proc = subprocess.Popen(
        [
            args.mg,
            f"--data-directory={args.data_dir}",
            "--versioning-enabled=true",
            "--storage-wal-enabled=true",
            "--telemetry-enabled=false",
            "--log-level=ERROR",
            f"--bolt-port={args.port}",
            "--storage-snapshot-on-exit=false",
            "--storage-properties-on-edges=false",
        ],
        stdout=logf,
        stderr=logf,
    )
    with open(args.pidfile, "w") as pf:
        pf.write(str(proc.pid))
    try:
        if not vb.wait_bolt(args.port, 60):
            raise RuntimeError("bolt did not come up")
        uri = f"bolt://127.0.0.1:{args.port}"
        drv = GraphDatabase.driver(uri, auth=("", ""), encrypted=False, max_connection_pool_size=1)
        vb.load_dataset(drv, args.index, args.data)
        with drv.session() as s0:
            s0.run("CREATE BRANCH bench FROM main").consume()
        s = drv.session()
        if args.mode == "branch":
            s.run("CHECKOUT BRANCH bench").consume()
            vb.apply_churn(s)
            vb.log("[profile] BRANCH mode: churn applied, branch 'bench' live")
        else:
            vb.log("[profile] MAIN mode: no checkout")
        vb.log(f"[profile] memgraph pid={proc.pid} — looping BFS for {args.duration}s (mode={args.mode})")
        rng = random.Random(1234)
        n = 0
        t0 = time.time()
        while time.time() - t0 < args.duration:
            frm = rng.randint(1, vb.NUM_VERTICES)
            to = rng.randint(1, vb.NUM_VERTICES)
            try:
                s.run(QUERY, {"from": frm, "to": to}).consume()
            except Exception:
                pass
            n += 1
        vb.log(f"[profile] done, ran {n} BFS queries")
        s.close()
        drv.close()
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except Exception:
            proc.kill()


if __name__ == "__main__":
    main()
