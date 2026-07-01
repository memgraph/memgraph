#!/usr/bin/env python3
"""c3.3 verification: TERMINATE a parallel query while its coordinator is PARKED, assert clean unwind.

A parallel coordinator parks for the whole query window, so a TERMINATE issued from a second
connection deterministically hits the parked state. Verifies: the running query aborts (no hang),
the server stays healthy, and a subsequent parallel query still works.

Usage:  abort_mid_park_test.py <bolt-port> [--nodes 400000] [--trials 5] [--par 4]
Requires the server to already be running (enterprise license set in the server's env).
"""
import argparse
import sys
import threading
import time

from neo4j import GraphDatabase


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("port", type=int)
    ap.add_argument("--nodes", type=int, default=400000, help="enough that the scan runs long enough to catch")
    ap.add_argument("--trials", type=int, default=5)
    ap.add_argument("--par", type=int, default=4)
    args = ap.parse_args()

    d = GraphDatabase.driver(f"bolt://127.0.0.1:{args.port}", auth=("", ""))

    def run(q):
        with d.session() as s:
            return [r.values() for r in s.run(q)]

    run("MATCH (n) DETACH DELETE n")
    made = 0
    while made < args.nodes:
        batch = min(50000, args.nodes - made)
        run(f"UNWIND range({made + 1},{made + batch}) AS i CREATE (:N {{p:i, g:i%7}})")
        made += batch
    print("nodes:", run("MATCH (n:N) RETURN count(n)"), flush=True)

    Q = (
        f"USING PARALLEL EXECUTION {args.par} MATCH (n:N) "
        "RETURN count(n), sum(n.p), avg(toFloat(n.p)), min(n.p), max(n.p)"
    )
    aborted = completed = hang = 0

    for trial in range(args.trials):
        res = {}

        def worker():
            try:
                res["r"] = run(Q)
            except Exception as e:  # noqa: BLE001
                res["exc"] = type(e).__name__

        th = threading.Thread(target=worker)
        th.start()
        killed = False
        for _ in range(400):
            time.sleep(0.003)
            try:
                txns = run("SHOW TRANSACTIONS")
            except Exception:  # noqa: BLE001
                continue
            for row in txns:
                if "PARALLEL EXECUTION" in str(row[2]):
                    try:
                        run(f"TERMINATE TRANSACTIONS '{row[1]}'")
                        killed = True
                    except Exception:  # noqa: BLE001
                        pass
                    break
            if killed:
                break
        th.join(timeout=30)
        if th.is_alive():
            hang += 1
            print(f"trial{trial}: HANG (worker stuck)", flush=True)
            break
        if "exc" in res:
            aborted += 1
        elif "r" in res:
            completed += 1
        # health check
        assert run("RETURN 1") == [[1]], "server unhealthy after abort"

    print(f"RESULT: aborted={aborted} completed={completed} hang={hang}", flush=True)
    print("post-abort parallel:", run(f"USING PARALLEL EXECUTION {args.par} MATCH (n:N) RETURN count(n)"), flush=True)
    d.close()
    # pass = no hang; at least some trials caught+aborted the query mid-park
    sys.exit(0 if (hang == 0 and aborted > 0) else 1)


if __name__ == "__main__":
    main()
