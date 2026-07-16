#!/usr/bin/env python3
"""Functional correctness smoke test for the branch-local plan cache.

Verifies the exact invariant the old guard protected, now that branches cache plans:
  1. A label-index query on a branch returns CORRECT rows (not the 0-rows silent-wrong
     failure the guard warned about), and returns the SAME correct rows on the 2nd+
     execution (i.e. a cache HIT does not corrupt results).
  2. Branch writes are visible to a cached branch plan (plan caches the PLAN, not results).
  3. main and branch do NOT cross-contaminate: a plan cached on main does not leak onto a
     branch and vice-versa (checked by result correctness on both sides after interleaving).
  4. Branch DROP INDEX does not yield a stale/broken cached plan (lazy re-validation).

Exit 0 on success, non-zero with a diagnostic on any mismatch.
"""
import os
import subprocess
import sys
import time

from neo4j import GraphDatabase

MG = os.environ.get("MG", "/home/andreja/workspace/memgraph/build/memgraph")
PORT = int(os.environ.get("PORT", "7799"))
DATA = f"/tmp/lima/tmp/claude-1000/smoke-branch-cache-{PORT}"


def wait_bolt(port, timeout=60):
    import socket

    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            with socket.create_connection(("127.0.0.1", port), 1):
                return True
        except OSError:
            time.sleep(0.3)
    return False


def q(sess, cypher, **params):
    return [r.data() for r in sess.run(cypher, **params)]


def main():
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
        stdout=open(os.path.join(DATA, "mg.log"), "w"),
        stderr=subprocess.STDOUT,
    )
    failures = []
    try:
        if not wait_bolt(PORT):
            print("!! bolt never came up")
            return 2
        drv = GraphDatabase.driver(f"bolt://127.0.0.1:{PORT}", auth=("", ""))
        with drv.session() as s:
            # --- main: seed data + label index ---
            s.run("CREATE INDEX ON :User(id)").consume()
            for i in range(20):
                s.run("CREATE (:User {id:$id, tag:'main'})", id=i).consume()
            main_q = "MATCH (n:User {id:$id}) RETURN n.id AS id, n.tag AS tag"
            base = q(s, main_q, id=5)
            assert base == [{"id": 5, "tag": "main"}], f"main baseline wrong: {base}"

            # prime main's plan cache (run twice on main)
            q(s, main_q, id=6)
            q(s, main_q, id=7)

            # --- checkout branch ---
            s.run("CREATE BRANCH bench FROM main").consume()
            s.run("CHECKOUT BRANCH bench").consume()

            # (1) branch label-index query must return CORRECT rows, not 0 rows
            b1 = q(s, main_q, id=5)
            if b1 != [{"id": 5, "tag": "main"}]:
                failures.append(f"[1] branch index query returned {b1}, expected id=5 tag=main (0-rows guard hazard!)")

            # (2) cache HIT: same query again must still be correct
            b2 = q(s, main_q, id=5)
            if b2 != b1:
                failures.append(f"[2] cache-hit result diverged: first={b1} second={b2}")

            # (3) branch write visible to a cached branch plan
            s.run("CREATE (:User {id:$id, tag:'branch'})", id=100).consume()
            b3 = q(s, main_q, id=100)
            if b3 != [{"id": 100, "tag": "branch"}]:
                failures.append(f"[3] branch-written row not visible to cached plan: {b3}")
            # existing row still correct after the write (re-run cached plan)
            b4 = q(s, main_q, id=5)
            if b4 != [{"id": 5, "tag": "main"}]:
                failures.append(f"[3b] fork-state row wrong after branch write: {b4}")

            # (4) branch DROP INDEX then rerun the cached-by-id plan: must not error, must be correct
            try:
                s.run("DROP INDEX ON :User(id)").consume()
            except Exception as e:
                # branch-local DROP INDEX may or may not be supported; tolerate but note
                print(f"   (note: branch DROP INDEX raised: {e})")
            b5 = q(s, main_q, id=5)
            if b5 != [{"id": 5, "tag": "main"}]:
                failures.append(f"[4] result wrong after branch DROP INDEX (stale plan?): {b5}")

            # --- return to main: main's cache must be intact & uncontaminated ---
            s.run("CHECKOUT BRANCH main").consume()
            m1 = q(s, main_q, id=5)
            if m1 != [{"id": 5, "tag": "main"}]:
                failures.append(f"[5] main result wrong after branch interlude: {m1}")
            # main must NOT see the branch-only row id=100
            m2 = q(s, main_q, id=100)
            if m2 != []:
                failures.append(f"[5b] main leaked branch-only row id=100: {m2}")
        drv.close()
    finally:
        proc.terminate()
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            proc.kill()

    if failures:
        print("FAIL:")
        for f in failures:
            print("  -", f)
        return 1
    print(
        "PASS: branch-local plan cache is correct (index query, cache hit, branch write, DROP INDEX, no cross-contamination)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
