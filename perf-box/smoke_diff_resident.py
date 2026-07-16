#!/usr/bin/env python3
"""Functional test for the diff-resident read fast path (props/labels read from impl_ directly).

Targets exactly the paths the optimization changed:
  1. COW'd fork vertex (diff-resident): multi-property + label reads must be correct & consistent.
  2. Branch-native vertex (diff-resident): reads correct.
  3. COW'd-then-DELETEd vertex: must NOT resurrect (read-after-delete-in-command must be gone) --
     this is the tombstone-bypass case the fast path must still get right via diff-engine MVCC.
  4. Un-branched fork vertex: still reads fork-state correctly (regression).
  5. Label/property updates on a branch are visible; main is unaffected.
"""
import os
import socket
import subprocess
import sys
import time

from neo4j import GraphDatabase

MG = os.environ.get("MG", "/home/andreja/workspace/memgraph/build/memgraph")
PORT = int(os.environ.get("PORT", "7796"))
DATA = f"/tmp/lima/tmp/claude-1000/smoke-diffres-{PORT}"


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
            for i in range(1, 11):
                s.run("CREATE (:User {id:$id, name:$n, age:$a})", id=i, n=f"u{i}", a=20 + i).consume()
            s.run("CREATE BRANCH b FROM main").consume()
            s.run("CHECKOUT BRANCH b").consume()

            # (1) COW a fork vertex via SET -> diff-resident; multi-prop + label reads must be consistent
            s.run("MATCH (n:User {id:3}) SET n.age=999, n.extra='x', n:Tagged").consume()
            r = q(s, "MATCH (n:User {id:3}) RETURN n.age AS age, n.name AS name, n.extra AS extra, labels(n) AS lbls")
            if not (
                r
                and r[0]["age"] == 999
                and r[0]["name"] == "u3"
                and r[0]["extra"] == "x"
                and set(r[0]["lbls"]) == {"User", "Tagged"}
            ):
                fail.append(f"[1] COW'd diff-resident read wrong: {r}")
            # read again (stability / repeated fast path)
            r2 = q(s, "MATCH (n:User {id:3}) RETURN n.age AS age")
            if not (r2 and r2[0]["age"] == 999):
                fail.append(f"[1b] repeated read diverged: {r2}")

            # (2) branch-native vertex (diff-resident): reads correct
            s.run("CREATE (:User {id:1001, name:'native', age:7})").consume()
            r = q(s, "MATCH (n:User {id:1001}) RETURN n.name AS name, n.age AS age")
            if not (r and r[0]["name"] == "native" and r[0]["age"] == 7):
                fail.append(f"[2] native read wrong: {r}")

            # (3) COW then DELETE (diff-resident tombstone): must not resurrect
            s.run("MATCH (n:User {id:5}) SET n.age=555").consume()  # COW -> diff-resident
            s.run("MATCH (n:User {id:5}) DETACH DELETE n").consume()  # delete the diff-resident copy
            r = q(s, "MATCH (n:User {id:5}) RETURN n.age AS age")
            if r != []:
                fail.append(f"[3] deleted diff-resident vertex resurrected: {r}")
            # COW-in-same-command then delete-in-same-command (no read of the deleted obj)
            s.run("MATCH (n:User {id:6}) SET n.age=666 WITH n DETACH DELETE n").consume()
            r6 = q(s, "MATCH (n:User {id:6}) RETURN n")
            if r6 != []:
                fail.append(f"[3b] same-command COW+delete left id6 alive: {r6}")

            # (4) un-branched fork vertex still reads fork-state
            r = q(s, "MATCH (n:User {id:8}) RETURN n.age AS age, n.name AS name")
            if not (r and r[0]["age"] == 28 and r[0]["name"] == "u8"):
                fail.append(f"[4] un-branched fork read wrong: {r}")

            # (5) main unaffected
            s.run("CHECKOUT BRANCH main").consume()
            rm = q(s, "MATCH (n:User {id:3}) RETURN n.age AS age, labels(n) AS lbls")
            if not (rm and rm[0]["age"] == 23 and rm[0]["lbls"] == ["User"]):
                fail.append(f"[5] main leaked branch COW: {rm}")
            r5 = q(s, "MATCH (n:User {id:5}) RETURN n.age AS age")
            if not (r5 and r5[0]["age"] == 25):
                fail.append(f"[5b] main lost id5 (branch delete leaked): {r5}")
            rn = q(s, "MATCH (n:User {id:1001}) RETURN n")
            if rn != []:
                fail.append(f"[5c] main sees branch-native id1001: {rn}")
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
        "PASS: diff-resident read fast path correct (COW multi-prop, native, COW+delete no-resurrect, un-branched, main-isolation)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
