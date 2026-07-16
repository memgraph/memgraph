#!/usr/bin/env python3
"""Functional test for the per-query ResolveVertex memo — focused on invalidation.

Targets the exact cases the memo must get right:
  1. Same-query COW-then-read: MATCH n SET n.x WITH n ... RETURN n.x must see the NEW value
     (CowVertex must have invalidated the memo entry the initial MATCH cached).
  2. Same-query read-then-SET-then-read across WITH boundaries.
  3. Repeated reads of the same vertex in one query (memo hit) stay correct.
  4. Same-query COW-then-DELETE: deleted vertex must not resurrect.
  5. Read-only repeated queries: results identical (memo is per-query, no cross-query staleness).
"""
import os
import socket
import subprocess
import sys
import time

from neo4j import GraphDatabase

MG = os.environ.get("MG", "/home/andreja/workspace/memgraph/build/memgraph")
PORT = int(os.environ.get("PORT", "7794"))
DATA = f"/tmp/lima/tmp/claude-1000/smoke-memo-{PORT}"


def wb(p, t=60):
    t0 = time.time()
    while time.time() - t0 < t:
        try:
            socket.create_connection(("127.0.0.1", p), 1).close()
            return True
        except OSError:
            time.sleep(0.3)
    return False


def q(s, c, **p):
    return [r.data() for r in s.run(c, **p)]


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
        stdout=open(DATA + "/mg.log", "w"),
        stderr=subprocess.STDOUT,
    )
    fail = []
    try:
        if not wb(PORT):
            print("!! bolt down")
            return 2
        drv = GraphDatabase.driver(f"bolt://127.0.0.1:{PORT}", auth=("", ""))
        with drv.session() as s:
            s.run("CREATE INDEX ON :User(id)").consume()
            for i in range(1, 21):
                s.run("CREATE (:User {id:$id, age:$a, name:$n})", id=i, a=10 + i, n=f"u{i}").consume()
            s.run("CREATE (:User {id:1})-[:F]->(:User {id:2})").consume()  # give some edges for revisits
            s.run("CREATE BRANCH b FROM main").consume()
            s.run("CHECKOUT BRANCH b").consume()

            # (1) same-query read (caches historical) then COW then read -> must see NEW value
            r = q(s, "MATCH (n:User {id:3}) WITH n, n.age AS before SET n.age=999 RETURN before, n.age AS after")
            if not (r and r[0]["before"] == 13 and r[0]["after"] == 999):
                fail.append(f"[1] same-query COW-then-read wrong: {r} (expect before=13 after=999)")

            # (2) read same vertex twice around a SET in one query
            r = q(s, "MATCH (n:User {id:4}) WITH n, n.name AS n1 SET n.name='X' WITH n, n1 RETURN n1, n.name AS n2")
            if not (r and r[0]["n1"] == "u4" and r[0]["n2"] == "X"):
                fail.append(f"[2] read/SET/read wrong: {r} (expect n1=u4 n2=X)")

            # (3) repeated read of same vertex (memo hit) correct
            r = q(s, "MATCH (n:User {id:5}) RETURN n.age AS a1, n.age AS a2, n.name AS nm")
            if not (r and r[0]["a1"] == 15 and r[0]["a2"] == 15 and r[0]["nm"] == "u5"):
                fail.append(f"[3] repeated-read memo hit wrong: {r}")

            # (4) same-query COW then DELETE -> gone
            s.run("MATCH (n:User {id:7}) SET n.age=77 WITH n DETACH DELETE n").consume()
            if q(s, "MATCH (n:User {id:7}) RETURN n") != []:
                fail.append("[4] COW+delete same-query left vertex alive")

            # (5) read-only query run twice -> identical (per-query memo, no cross-query staleness)
            a = q(s, "MATCH (n:User) WHERE n.age > 900 RETURN n.id AS id ORDER BY id")
            b = q(s, "MATCH (n:User) WHERE n.age > 900 RETURN n.id AS id ORDER BY id")
            if a != b or {row["id"] for row in a} != {
                3
            }:  # only id3 has age 999 (id4 name changed, age 14; id7 deleted)
                fail.append(f"[5] cross-query result mismatch/wrong: a={a} b={b} (expect both [{{id:3}}])")

            # verify persisted branch state is coherent after all the above
            r = q(s, "MATCH (n:User {id:3}) RETURN n.age AS age")
            if not (r and r[0]["age"] == 999):
                fail.append(f"[6] id3 age not persisted on branch: {r}")

            s.run("CHECKOUT BRANCH main").consume()
            rm = q(s, "MATCH (n:User {id:3}) RETURN n.age AS age")
            if not (rm and rm[0]["age"] == 13):
                fail.append(f"[7] main leaked branch COW on id3: {rm}")
        drv.close()
    finally:
        proc.terminate()
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            proc.kill()
    if fail:
        print("FAIL:")
        [print("  -", f) for f in fail]
        return 1
    print(
        "PASS: resolve-memo invalidation correct (same-query COW-then-read, read/SET/read, memo-hit, COW+delete, cross-query, main-isolation)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
