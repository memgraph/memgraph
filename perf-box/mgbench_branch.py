#!/usr/bin/env python3
"""Run the REAL mgbench pokec query families single-session, MAIN vs a checked-out Graph
Versioning BRANCH, on the REAL cached pokec_medium dataset (tests/mgbench/.cache).

Why a bespoke runner instead of `benchmark.py`: mgbench's own client fans the workload across N
concurrent, stateless bolt connections, none of which issues CHECKOUT BRANCH; and versioning uses
EXCLUSIVE single-session checkout (one session per branch). The two models are incompatible, so the
faithful way to benchmark "on a branch" is a single persistent session that CHECKOUTs, then runs the
same mgbench queries. We measure steady-state median latency (post-warmup, i.e. plan-cache hot) which
is what the branch-local plan cache fix delivers; cold (first-call) latency is also reported to show
the planning tax the fix removes.

Loads once; runs MAIN, then BRANCH under {realistic, worst} churn (reusing the single load).
"""
import os
import random
import re
import socket
import statistics
import subprocess
import sys
import time
from pathlib import Path

from neo4j import GraphDatabase

os.environ.setdefault("NUM_VERTICES", "100000")
import bench as vb  # for select_churn / apply_churn / NUM_VERTICES

MG = os.environ.get("MG", "/home/andreja/workspace/memgraph/build/memgraph")
PORT = int(os.environ.get("PORT", "7766"))
NV = int(os.environ["NUM_VERTICES"])
MGBENCH_CACHE = "/home/andreja/workspace/memgraph/tests/mgbench/.cache/datasets/pokec/medium"
RAW = f"{MGBENCH_CACHE}/dataset.cypher"
UNWIND = "/home/andreja/workspace/memgraph/perf-box/pokec/pokec_medium_real.unwind.cypher"
DATA = f"/tmp/lima/tmp/claude-1000/mgbench-branch-{PORT}"

# mgbench pokec READ query families (verbatim from tests/mgbench/workloads/pokec.py).
# kind: 'vid' -> one random vertex id param $id; 'fromto' -> $from/$to; 'none' -> no param.
FAMILIES = [
    ("single_vertex_read", "vid", "MATCH (n:User {id: $id}) RETURN n", 300),
    ("expansion_1", "vid", "MATCH (s:User {id: $id})-->(n:User) RETURN n.id", 200),
    ("expansion_2", "vid", "MATCH (s:User {id: $id})-->()-->(n:User) RETURN DISTINCT n.id", 120),
    ("expansion_3", "vid", "MATCH (s:User {id: $id})-->()-->()-->(n:User) RETURN DISTINCT n.id", 60),
    ("expansion_4", "vid", "MATCH (s:User {id: $id})-->()-->()-->()-->(n:User) RETURN DISTINCT n.id", 30),
    ("neighbours_2", "vid", "MATCH (s:User {id: $id})-[*1..2]->(n:User) RETURN DISTINCT n.id", 120),
    ("aggregate", "none", "MATCH (n:User) RETURN n.age, COUNT(*)", 30),
    ("aggregate_with_filter", "none", "MATCH (n:User) WHERE n.age >= 18 RETURN n.age, COUNT(*)", 30),
    (
        "shortest_path_bfs",
        "fromto",
        "MATCH (n:User {id: $from}), (m:User {id: $to}) WITH n, m "
        "MATCH p=(n)-[*bfs..15]->(m) RETURN [x IN nodes(p) | x.id] AS path",
        80,
    ),
]
WARMUP = 8  # per-family warmup iterations before measuring (warms the plan cache)


def log(m):
    print(m, flush=True)


def wait_bolt(port, timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            socket.create_connection(("127.0.0.1", port), 1).close()
            return True
        except OSError:
            time.sleep(0.3)
    return False


def build_unwind_once():
    """Convert the real dataset.cypher (1.87M single statements) into UNWIND batches once,
    so the (single) load is seconds not tens of minutes. Cached on disk."""
    if Path(UNWIND).exists():
        log(f"[prep] using cached UNWIND dataset {UNWIND}")
        return
    log(f"[prep] converting real dataset -> UNWIND (one-time) ...")
    vre = re.compile(r"CREATE \(:User \{id: (\d+), completion_percentage: (\d+), gender: \"(\w+)\", age: (\d+)\}\);")
    ere = re.compile(r"MATCH \(n:User \{id: (\d+)\}\), \(m:User \{id: (\d+)\}\) CREATE \(n\)-\[e: Friend\]->\(m\);")
    verts, edges = [], []
    with open(RAW) as f:
        for line in f:
            m = vre.match(line)
            if m:
                verts.append((int(m[1]), int(m[2]), m[3], int(m[4])))
                continue
            m = ere.match(line)
            if m:
                edges.append((int(m[1]), int(m[2])))
    B = 500
    with open(UNWIND, "w") as f:
        for s in range(0, len(verts), B):
            rows = ",".join(f"{{id:{i},cp:{c},g:'{g}',a:{a}}}" for i, c, g, a in verts[s : s + B])
            f.write(
                f"UNWIND [{rows}] AS r CREATE (:User {{id:r.id, completion_percentage:r.cp, gender:r.g, age:r.a}});\n"
            )
        for s in range(0, len(edges), B):
            lst = ",".join(f"[{a},{b}]" for a, b in edges[s : s + B])
            f.write(f"UNWIND [{lst}] AS p MATCH (a:User {{id:p[0]}}),(b:User {{id:p[1]}}) CREATE (a)-[:Friend]->(b);\n")
    log(f"[prep] wrote {len(verts)} vertices + {len(edges)} edges as UNWIND batches")


def load(drv):
    log("[load] index + data ...")
    t0 = time.time()
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
    with drv.session() as s:
        nv = s.run("MATCH (n:User) RETURN count(n) AS c").single()["c"]
        ne = s.run("MATCH ()-[e]->() RETURN count(e) AS c").single()["c"]
    log(f"[load] done in {time.time()-t0:.1f}s: vertices={nv} edges={ne}")


def measure_phase(session, params_by_kind):
    """Run each family: WARMUP iters (discarded), then the measured iters. Returns per-family
    {cold_ms, median_ms, p90_ms}. Uses pre-generated params so MAIN and BRANCH see identical ids."""
    out = {}
    for name, kind, query, iters in FAMILIES:
        params = params_by_kind[kind]
        # cold: first execution (plan build on a fresh cache)
        t = time.perf_counter()
        session.run(query, params[0]).consume()
        cold = (time.perf_counter() - t) * 1000
        for i in range(1, WARMUP):
            session.run(query, params[i % len(params)]).consume()
        lat = []
        for i in range(iters):
            p = params[i % len(params)]
            t = time.perf_counter()
            session.run(query, p).consume()
            lat.append((time.perf_counter() - t) * 1000)
        lat.sort()
        out[name] = dict(cold_ms=cold, median_ms=statistics.median(lat), p90_ms=lat[int(len(lat) * 0.9)], n=iters)
        log(
            f"    {name:24s} cold={cold:7.2f}ms  median={out[name]['median_ms']:7.3f}ms  p90={out[name]['p90_ms']:7.3f}ms"
        )
    return out


def main():
    build_unwind_once()
    subprocess.run(["rm", "-rf", DATA], check=False)
    os.makedirs(DATA, exist_ok=True)
    proc = subprocess.Popen(
        [
            MG,
            f"--data-directory={DATA}",
            "--versioning-enabled=true",
            "--storage-wal-enabled=true",
            "--telemetry-enabled=false",
            "--log-level=ERROR",
            f"--bolt-port={PORT}",
            "--storage-snapshot-on-exit=false",
        ],
        stdout=open(DATA + "/mg.log", "w"),
        stderr=subprocess.STDOUT,
    )
    results = {}
    try:
        if not wait_bolt(PORT):
            log("!! bolt down")
            return 2
        drv = GraphDatabase.driver(f"bolt://127.0.0.1:{PORT}", auth=("", ""))
        load(drv)

        # pre-generate params (seeded) reused across ALL phases for a fair comparison
        rng = random.Random(2026)
        N = max(WARMUP, max(f[3] for f in FAMILIES)) + 5
        vids = [{"id": rng.randint(1, NV)} for _ in range(N)]
        fts = [{"from": rng.randint(1, NV), "to": rng.randint(1, NV)} for _ in range(N)]
        params_by_kind = {"vid": vids, "fromto": fts, "none": [{}] * N}

        log("\n==================  MAIN  ==================")
        with drv.session() as s:
            results["main"] = measure_phase(s, params_by_kind)

        for mode in ["realistic", "worst"]:
            vb.select_churn(mode)
            bname = f"bench_{mode}"
            log(f"\n==================  BRANCH ({mode} churn)  ==================")
            with drv.session() as s:
                s.run(f"CREATE BRANCH {bname} FROM main").consume()
                s.run(f"CHECKOUT BRANCH {bname}").consume()
                vb.apply_churn(s)  # mutates the branch's diff engine; session STAYS checked out
                results[f"branch_{mode}"] = measure_phase(s, params_by_kind)
                s.run("CHECKOUT BRANCH main").consume()
        drv.close()
    finally:
        proc.terminate()
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            proc.kill()

    # ---- report ----
    log("\n\n################  RESULTS: branch/main median-latency ratio  ################")
    hdr = f"{'query family':24s} {'main ms':>9s} {'br-real ms':>11s} {'ratio':>6s} {'br-worst ms':>12s} {'ratio':>6s}"
    log(hdr)
    log("-" * len(hdr))
    for name, *_ in FAMILIES:
        m = results["main"][name]["median_ms"]
        r = results["branch_realistic"][name]["median_ms"]
        w = results["branch_worst"][name]["median_ms"]
        log(f"{name:24s} {m:9.3f} {r:11.3f} {r/m:6.2f} {w:12.3f} {w/m:6.2f}")
    log("\n(cold/first-call latency, shows the planning tax the branch-local cache removes)")
    log(f"{'query family':24s} {'main cold':>10s} {'br-real cold':>13s} {'br-worst cold':>14s}")
    for name, *_ in FAMILIES:
        log(
            f"{name:24s} {results['main'][name]['cold_ms']:10.2f} "
            f"{results['branch_realistic'][name]['cold_ms']:13.2f} {results['branch_worst'][name]['cold_ms']:14.2f}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
