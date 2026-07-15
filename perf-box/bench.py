#!/usr/bin/env python3
"""Graph Versioning v1 — branch-vs-main performance A/B harness (pokec_small).

Design goals:
  * ONE memgraph instance. Session M stays on main; Session B checks out the
    branch ONCE (so BuildFromFork runs once, not per query).
  * Worst-case branch churn crafted so the impactful queries exercise the
    diff-vs-historical merge paths (esp. expansions crossing split adjacency).
  * Unstable machine => interleave M and B per parameter within each run, and
    report the PAIRED ratio branch/main per run. The distribution of that ratio
    across N runs cancels machine-wide transients.

Outputs a JSON blob (per-query: per-run main/branch ms + ratio, plus summary
stats) to --out. A separate step renders the HTML report from it.
"""
import argparse
import json
import os
import random
import socket
import statistics
import subprocess
import sys
import time
from pathlib import Path


def log(*a):
    print(*a, file=sys.stderr, flush=True)


def wait_bolt(port, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            time.sleep(0.5)
    return False


# ---------------------------------------------------------------------------
# Query workload — curated most-impactful pokec queries. Each entry:
#   name, cypher, kind (param generator), n (executions per run per mode)
# param kinds:
#   'hot'   -> start vertex id drawn from HOT set (split adjacency, COW'd)
#   'cow'   -> id drawn from COW'd set (id % 5 == 0), point/index read
#   'none'  -> no params (full-scan aggregate)
# ---------------------------------------------------------------------------
QUERIES = [
    # --- point / index reads ---
    dict(
        name="vertex_read_clean", kind="clean", n=60, cypher="MATCH (n:User {id:$id}) RETURN n"
    ),  # untouched: pure overhead
    dict(name="vertex_read_cow", kind="cow", n=60, cypher="MATCH (n:User {id:$id}) RETURN n"),  # COW'd: diff-side hit
    dict(
        name="index_scan_gender", kind="none", n=25, cypher="MATCH (n:User {gender:'man'}) RETURN count(n)"
    ),  # index range merge
    # --- expansions: clean (same edges as main => isolates overhead) vs split (worst case) ---
    dict(name="expansion_1_clean", kind="clean", n=60, cypher="MATCH (s:User {id:$id})-->(n:User) RETURN n.id"),
    dict(name="expansion_1_split", kind="hot", n=60, cypher="MATCH (s:User {id:$id})-->(n:User) RETURN n.id"),
    dict(
        name="expansion_2_clean",
        kind="clean",
        n=50,
        cypher="MATCH (s:User {id:$id})-->()-->(n:User) RETURN DISTINCT n.id",
    ),
    dict(
        name="expansion_2_split",
        kind="hot",
        n=50,
        cypher="MATCH (s:User {id:$id})-->()-->(n:User) RETURN DISTINCT n.id",
    ),
    dict(
        name="expansion_4_split",
        kind="hot",
        n=25,
        cypher="MATCH (s:User {id:$id})-->()-->()-->()-->(n:User) RETURN DISTINCT n.id",
    ),
    dict(
        name="neighbours_var_1_2_split",
        kind="hot",
        n=40,
        cypher="MATCH (s:User {id:$id})-[*1..2]->(n:User) RETURN DISTINCT n.id",
    ),
    # --- shortest path (BFS, deep-path traversal; per-hop ResolveEdges union on a branch) ---
    dict(
        name="shortest_path_bfs",
        kind="fromto",
        n=20,
        cypher="MATCH (n:User {id:$from}), (m:User {id:$to}) WITH n, m "
        "MATCH p=(n)-[*BFS..15]->(m) RETURN [x IN nodes(p) | x.id] AS path",
    ),
    # --- full-scan aggregates (Vertices() streaming union + tombstone skip) ---
    dict(name="aggregate_count", kind="none", n=25, cypher="MATCH (n) RETURN count(n), count(n.age)"),
    dict(name="aggregate_min_max_avg", kind="none", n=25, cypher="MATCH (n) RETURN min(n.age), max(n.age), avg(n.age)"),
]

NUM_VERTICES = 10000

# Churn profiles: 'worst' ≈ 25% of main touched (near worst case); 'realistic' ≈ 1-1.5%.
CHURN_PROFILES = {
    "worst": dict(cow_mod=5, tomb_mod=47, hot=list(range(5, 1505, 5)), native=500, edges_per_hot=5),
    "realistic": dict(cow_mod=100, tomb_mod=500, hot=list(range(100, 3100, 100)), native=50, edges_per_hot=5),
}
CHURN_MODE = "worst"  # overridden by --churn in main()
CHURN = CHURN_PROFILES[CHURN_MODE]
HOT = CHURN["hot"]
COW_IDS = []
CLEAN_IDS = []


def is_tombstoned(i):
    return i % CHURN["tomb_mod"] == 0


def select_churn(mode):
    """Set the module-level churn config + id pools for the chosen profile."""
    global CHURN_MODE, CHURN, HOT, COW_IDS, CLEAN_IDS
    CHURN_MODE = mode
    CHURN = CHURN_PROFILES[mode]
    cm, tm = CHURN["cow_mod"], CHURN["tomb_mod"]
    HOT = CHURN["hot"]
    hotset = set(HOT)
    COW_IDS = [i for i in range(cm, NUM_VERTICES + 1, cm) if i % tm != 0]
    # untouched: not COW'd, not tombstoned, not in the split-adjacency hot set
    CLEAN_IDS = [i for i in range(1, NUM_VERTICES + 1) if i % cm != 0 and i % tm != 0 and i not in hotset]


def gen_param(kind, rng):
    if kind == "none":
        return {}
    if kind == "fromto":  # shortest path: two distinct non-tombstoned endpoints
        a = rng.randint(1, NUM_VERTICES)
        while is_tombstoned(a):
            a = rng.randint(1, NUM_VERTICES)
        b = rng.randint(1, NUM_VERTICES)
        while b == a or is_tombstoned(b):
            b = rng.randint(1, NUM_VERTICES)
        return {"from": a, "to": b}
    if kind == "hot":
        vid = rng.choice(HOT)
    elif kind == "cow":
        vid = rng.choice(COW_IDS)
    else:  # clean (untouched)
        vid = rng.choice(CLEAN_IDS)
    return {"id": vid}


def run_query(session, cypher, params, force_main):
    q = cypher
    # USING VERSION 'main' escape hatch when we want the branch session to hit main?
    # No: main is a separate session. force_main unused here (kept for clarity).
    t0 = time.perf_counter()
    res = session.run(q, params)
    _ = res.consume()  # force full server-side execution
    return (time.perf_counter() - t0) * 1000.0  # ms


def load_dataset(driver, index_path, data_path):
    log("[load] indexes + data ...")
    with driver.session() as s:
        for line in Path(index_path).read_text().splitlines():
            line = line.strip()
            if line:
                s.run(line).consume()
        # batch data statements
        batch, cnt = [], 0

        def flush(b):
            if not b:
                return
            with driver.session() as bs:
                tx = bs.begin_transaction()
                for st in b:
                    tx.run(st)
                tx.commit()

        for line in Path(data_path).read_text().splitlines():
            line = line.strip().rstrip("\r").strip()
            if not line or line == ";":
                continue  # skip blanks + the lone section-separator ';'
            batch.append(line)
            if len(batch) >= 2000:
                flush(batch)
                cnt += len(batch)
                batch = []
        flush(batch)
        cnt += len(batch)
        log(f"[load] executed ~{cnt} statements")


def apply_churn(s):
    """Apply worst-case churn on branch `bench` via a session that STAYS checked out.
    (Deliberately NOT via checkout-away/re-checkout: replaying a COW'd-fork SetProperty
    on re-checkout is a known server-abort bug — branch_engine.cpp:208 — so we keep the
    diff engine live in this session, which also measures steady-state read overhead.)"""
    cm, tm = CHURN["cow_mod"], CHURN["tomb_mod"]
    log(f"[branch] applying '{CHURN_MODE}' churn (session stays checked out) ...")
    # 1) COW a fraction of vertices (property update)
    s.run(f"MATCH (n:User) WHERE n.id % {cm} = 0 SET n.age = n.age + 1").consume()
    # 2) tombstone a fraction (detach delete)
    s.run(f"MATCH (n:User) WHERE n.id % {tm} = 0 DETACH DELETE n").consume()
    # 3) split adjacency: branch-native edges on the HOT set
    rng = random.Random(42)
    for vid in HOT:
        for _ in range(CHURN["edges_per_hot"]):
            tgt = rng.randint(1, NUM_VERTICES)
            s.run("MATCH (a:User {id:$a}), (b:User {id:$b}) CREATE (a)-[:Friend]->(b)", {"a": vid, "b": tgt}).consume()
    # 4) branch-native vertices
    for nid in range(100001, 100001 + CHURN["native"]):
        s.run("CREATE (:User {id:$id, age:30, gender:'man', completion_percentage:50})", {"id": nid}).consume()
    log(f"[branch] churn done: COW≈{len(COW_IDS)} tomb≈{NUM_VERTICES//tm} hot={len(HOT)} native={CHURN['native']}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mg", required=True, help="path to memgraph binary")
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--index", required=True)
    ap.add_argument("--data", required=True)
    ap.add_argument("--port", type=int, default=7699)
    ap.add_argument("--runs", type=int, default=10)
    ap.add_argument("--churn", choices=list(CHURN_PROFILES.keys()), default="worst")
    ap.add_argument("--props-on-edges", choices=["true", "false"], default="true")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    select_churn(args.churn)
    log(f"[cfg] churn profile = {args.churn}  props_on_edges = {args.props_on_edges}")

    from neo4j import GraphDatabase

    # fresh data dir
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
            f"--storage-properties-on-edges={args.props_on_edges}",
        ],
        stdout=logf,
        stderr=logf,
    )
    try:
        if not wait_bolt(args.port, 60):
            raise RuntimeError("bolt did not come up")
        uri = f"bolt://127.0.0.1:{args.port}"
        # SEPARATE drivers, each pinned to a SINGLE connection: branch checkout is
        # connection-scoped, so a shared pool would let session B's checkout bleed into
        # session M (both multiplex one socket) -> main would see branch data. Two 1-conn
        # drivers guarantee M's connection never carries B's checkout.
        driverM = GraphDatabase.driver(uri, auth=("", ""), encrypted=False, max_connection_pool_size=1)
        driverB = GraphDatabase.driver(uri, auth=("", ""), encrypted=False, max_connection_pool_size=1)

        load_dataset(driverM, args.index, args.data)
        with driverM.session() as s0:
            s0.run("CREATE BRANCH bench FROM main").consume()

        # Session M = main (never checks out). Session B = branch: checks out ONCE and
        # applies churn in-place, then STAYS checked out (live diff engine, no replay).
        sM = driverM.session()
        sB = driverB.session()
        sB.run("CHECKOUT BRANCH bench").consume()
        apply_churn(sB)
        log("[bench] Session B holds branch 'bench' with churn live (one checkout, no re-checkout)")

        # sanity: confirm B sees churn (branch-native vertex) and M does not
        b_new = sB.run("MATCH (n:User {id:100001}) RETURN count(n) AS c").single()["c"]
        m_new = sM.run("MATCH (n:User {id:100001}) RETURN count(n) AS c").single()["c"]
        log(f"[sanity] branch sees id100001={b_new} (expect 1), main sees={m_new} (expect 0)")
        if not (b_new == 1 and m_new == 0):
            raise RuntimeError(f"branch/main isolation sanity FAILED: b={b_new} m={m_new}")

        results = {q["name"]: {"main_ms": [], "branch_ms": [], "ratio": []} for q in QUERIES}
        rng = random.Random(1234)

        # warmup
        for q in QUERIES:
            for _ in range(3):
                p = gen_param(q["kind"], rng)
                run_query(sM, q["cypher"], p, True)
                run_query(sB, q["cypher"], p, False)

        for r in range(1, args.runs + 1):
            for q in QUERIES:
                # fixed param sequence for this run (identical for M and B)
                params = [gen_param(q["kind"], rng) for _ in range(q["n"])]
                tM = tB = 0.0
                # interleave per-param, alternate order to avoid systematic bias
                for i, p in enumerate(params):
                    if i % 2 == 0:
                        tM += run_query(sM, q["cypher"], p, True)
                        tB += run_query(sB, q["cypher"], p, False)
                    else:
                        tB += run_query(sB, q["cypher"], p, False)
                        tM += run_query(sM, q["cypher"], p, True)
                mM = tM / q["n"]
                mB = tB / q["n"]
                results[q["name"]]["main_ms"].append(mM)
                results[q["name"]]["branch_ms"].append(mB)
                results[q["name"]]["ratio"].append(mB / mM if mM > 0 else float("nan"))
            log(
                f"[run {r}/{args.runs}] "
                + "  ".join(f'{q["name"][:14]}={results[q["name"]]["ratio"][-1]:.2f}x' for q in QUERIES)
            )

        # summary
        summary = {}
        for name, d in results.items():
            ratios = [x for x in d["ratio"] if x == x]
            summary[name] = dict(
                main_ms_median=statistics.median(d["main_ms"]),
                branch_ms_median=statistics.median(d["branch_ms"]),
                ratio_median=statistics.median(ratios),
                ratio_min=min(ratios),
                ratio_max=max(ratios),
                ratio_mean=statistics.mean(ratios),
                ratio_stdev=statistics.pstdev(ratios) if len(ratios) > 1 else 0.0,
            )
        out = dict(
            meta=dict(
                runs=args.runs,
                dataset="pokec_small",
                vertices=NUM_VERTICES,
                hot=len(HOT),
                queries=[q["name"] for q in QUERIES],
            ),
            results=results,
            summary=summary,
        )
        Path(args.out).write_text(json.dumps(out, indent=2))
        log(f"[done] wrote {args.out}")
        # human summary
        log("\n==== SUMMARY (branch/main latency ratio, median of %d runs) ====" % args.runs)
        for name, sdict in summary.items():
            log(
                f"  {name:24s}  main={sdict['main_ms_median']:.3f}ms  "
                f"branch={sdict['branch_ms_median']:.3f}ms  "
                f"ratio={sdict['ratio_median']:.2f}x  "
                f"[{sdict['ratio_min']:.2f}–{sdict['ratio_max']:.2f}]"
            )
        sM.close()
        sB.close()
        driverM.close()
        driverB.close()
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
        logf.close()


if __name__ == "__main__":
    main()
