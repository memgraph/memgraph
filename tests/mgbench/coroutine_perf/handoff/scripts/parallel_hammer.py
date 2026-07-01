#!/usr/bin/env python3
"""Parallel-query correctness hammer for the uniform-branch scheduler redesign.

Drives `USING PARALLEL EXECUTION` aggregate / grouped-aggregate / order-by queries, sequentially
and concurrently, and checks every result against a serial ground truth. Used to gate the
double-execution fix (c3.2) under ASan/TSan.

Usage:  parallel_hammer.py <bolt-port> [--nodes 4000] [--threads 8] [--iters 10] [--par 4]

No secrets here; the enterprise license is read by the *server* from its environment.
"""
import argparse
import sys
import threading

from neo4j import GraphDatabase


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("port", type=int)
    ap.add_argument("--nodes", type=int, default=4000)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--iters", type=int, default=10)
    ap.add_argument("--par", type=int, default=4, help="USING PARALLEL EXECUTION degree")
    ap.add_argument("--seq", type=int, default=30, help="sequential iterations")
    args = ap.parse_args()

    d = GraphDatabase.driver(f"bolt://127.0.0.1:{args.port}", auth=("", ""))

    def run(q):
        with d.session() as s:
            return [r.values() for r in s.run(q)]

    # ---- setup ----
    run("MATCH (n) DETACH DELETE n")
    # create in batches to keep individual transactions modest
    made = 0
    while made < args.nodes:
        batch = min(50000, args.nodes - made)
        run(f"UNWIND range({made + 1},{made + batch}) AS i CREATE (:N {{p:i, g:i%7}})")
        made += batch
    print("COUNT:", run("MATCH (n:N) RETURN count(n)"), flush=True)

    P = args.par
    # serial ground truth
    gt_agg = run("MATCH (n:N) RETURN count(n), sum(n.p)")
    gt_grp = sorted(run("MATCH (n:N) RETURN n.g AS g, count(*) AS c, sum(n.p) AS s ORDER BY g"))
    gt_ord = run("MATCH (n:N) RETURN n.p ORDER BY n.p DESC LIMIT 10")

    queries = [
        ("agg", f"USING PARALLEL EXECUTION {P} MATCH (n:N) RETURN count(n), sum(n.p)", gt_agg),
        (
            "grp",
            f"USING PARALLEL EXECUTION {P} MATCH (n:N) RETURN n.g AS g, count(*) AS c, sum(n.p) AS s ORDER BY g",
            gt_grp,
        ),
        ("ord", f"USING PARALLEL EXECUTION {P} MATCH (n:N) RETURN n.p ORDER BY n.p DESC LIMIT 10", gt_ord),
    ]

    def check(name, q, gt):
        try:
            r = run(q)
            if name == "grp":
                r = sorted(r)
            return "OK" if r == gt else f"MISMATCH got={r}"
        except Exception as e:  # noqa: BLE001
            return f"EXC {type(e).__name__}: {str(e)[:100]}"

    # ---- sequential ----
    seq_bad = 0
    for i in range(args.seq):
        for name, q, gt in queries:
            res = check(name, q, gt)
            if res != "OK":
                seq_bad += 1
                print(f"[SEQ {i} {name}] {res}", flush=True)
    print(f"SEQ done: seq_bad={seq_bad}/{args.seq * len(queries)}", flush=True)

    # ---- concurrent ----
    con_bad = [0]
    lock = threading.Lock()

    def worker(wid):
        for i in range(args.iters):
            for name, q, gt in queries:
                res = check(name, q, gt)
                if res != "OK":
                    with lock:
                        con_bad[0] += 1
                    print(f"[CON w{wid} it{i} {name}] {res}", flush=True)

    ts = [threading.Thread(target=worker, args=(w,)) for w in range(args.threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    print(f"CON done: con_bad={con_bad[0]}/{args.threads * args.iters * len(queries)}", flush=True)
    print("SERVER ALIVE (final probe):", run("RETURN 1"), flush=True)
    d.close()
    print("=== HAMMER COMPLETE ===", flush=True)
    sys.exit(1 if (seq_bad or con_bad[0]) else 0)


if __name__ == "__main__":
    main()
