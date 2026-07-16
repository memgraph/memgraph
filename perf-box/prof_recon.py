#!/usr/bin/env python3
"""Profiling driver for the READ-PATH RECONCILIATION cost (worst-churn branch).

Stands up the real pokec_medium, optionally checks out a worst-churn branch (STAYS checked
out), then hammers ONE chosen query family in a tight loop so an external `perf record` can
attribute branch reconciliation overhead (UnionVertices / ResolveEdges / ResolveVertex /
tombstone-skip / branched()) to real symbols. Prints the memgraph PID to --pidfile.

Usage: prof_recon.py --mode {main,branch} --query {aggregate,expansion4,neighbours2} [--churn worst]
"""
import argparse
import os
import random
import socket
import subprocess
import sys
import time
from pathlib import Path

os.environ.setdefault("NUM_VERTICES", "100000")
import bench as vb
from neo4j import GraphDatabase

UNWIND = "/home/andreja/workspace/memgraph/perf-box/pokec/pokec_medium_real.unwind.cypher"
NV = int(os.environ["NUM_VERTICES"])

QUERIES = {
    "aggregate": ("none", "MATCH (n:User) RETURN n.age, COUNT(*)"),
    "expansion4": ("vid", "MATCH (s:User {id: $id})-->()-->()-->()-->(n:User) RETURN DISTINCT n.id"),
    "neighbours2": ("vid", "MATCH (s:User {id: $id})-[*1..2]->(n:User) RETURN DISTINCT n.id"),
    "expansion2": ("vid", "MATCH (s:User {id: $id})-->()-->(n:User) RETURN DISTINCT n.id"),
}


def wait_bolt(port, t=60):
    t0 = time.time()
    while time.time() - t0 < t:
        try:
            socket.create_connection(("127.0.0.1", port), 1).close()
            return True
        except OSError:
            time.sleep(0.3)
    return False


def load(drv):
    vb.log("[load] index + data ...")
    with drv.session() as s:
        s.run("CREATE INDEX ON :User(id)").consume()
        s.run("CREATE INDEX ON :User(gender)").consume()
        batch = []

        def flush(b):
            if not b:
                return
            with drv.session() as bs:
                tx = bs.begin_transaction()
                for st in b:
                    tx.run(st)
                tx.commit()

        for line in Path(UNWIND).read_text().splitlines():
            line = line.strip()
            if line:
                batch.append(line)
                if len(batch) >= 40:
                    flush(batch)
                    batch = []
        flush(batch)
    vb.log("[load] done")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mg", required=True)
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--port", type=int, default=7770)
    ap.add_argument("--mode", choices=["main", "branch"], required=True)
    ap.add_argument("--query", choices=list(QUERIES.keys()), required=True)
    ap.add_argument("--churn", choices=["realistic", "worst"], default="worst")
    ap.add_argument("--pidfile", required=True)
    ap.add_argument("--duration", type=float, default=70.0)
    args = ap.parse_args()
    kind, QUERY = QUERIES[args.query]

    subprocess.run(["rm", "-rf", args.data_dir], check=False)
    os.makedirs(args.data_dir, exist_ok=True)
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
        ],
        stdout=open(os.path.join(args.data_dir, "mg.log"), "w"),
        stderr=subprocess.STDOUT,
    )
    with open(args.pidfile, "w") as pf:
        pf.write(str(proc.pid))
    try:
        if not wait_bolt(args.port):
            vb.log("!! bolt down")
            return 2
        drv = GraphDatabase.driver(f"bolt://127.0.0.1:{args.port}", auth=("", ""))
        load(drv)
        rng = random.Random(2026)
        vids = [{"id": rng.randint(1, NV)} for _ in range(500)]
        fts = [{} for _ in range(500)]
        params = vids if kind == "vid" else fts
        s = drv.session()
        if args.mode == "branch":
            vb.select_churn(args.churn)
            s.run("CREATE BRANCH bench FROM main").consume()
            s.run("CHECKOUT BRANCH bench").consume()
            vb.apply_churn(s)
            vb.log(f"[profile] BRANCH ({args.churn}) churn applied")
        vb.log(f"[profile] memgraph pid={proc.pid} — looping {args.query} for {args.duration}s (mode={args.mode})")
        t0 = time.time()
        i = 0
        while time.time() - t0 < args.duration:
            for _ in range(20):
                s.run(QUERY, params[i % len(params)]).consume()
                i += 1
        s.close()
        drv.close()
    finally:
        proc.terminate()
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            proc.kill()
    return 0


if __name__ == "__main__":
    sys.exit(main())
