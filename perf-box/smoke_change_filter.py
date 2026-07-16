#!/usr/bin/env python3
"""Functional test for the branch-side change filters (property/label/edge).

Attacks the invariants the filters must uphold — every case where a CLEAR filter bit makes the read
path skip the resolve and read the fork copy (impl_) directly, and every case where a SET bit must
still resolve:

  1. label-clear / prop-set: on a branch, SET n.age (prop filter set, label filter CLEAR). Reading
     :User must be fork-correct via the direct read; reading n.age must resolve to the NEW value.
  2. prop-clear / label-set: ADD/REMOVE a label (label set, prop CLEAR). Reading a property must be
     fork-correct via the direct read; the label read must resolve.
  3. cross-branch isolation: branch b1 changes V; branch b2 (never touched V) reads V's label AND
     property -> must see FORK values (b2's filters are clear for V).
  4. C1 lock: main mutates V.age AFTER b2 forked; b2 (never touched V) reads V.age -> must return the
     FORK value, not main's post-fork value (the direct-read path must be fork_ts-pinned).
  5. same-query COW-then-read: SET n.age then read n.age in one query -> NEW value (filter set
     mid-query before the read).
  6. edge topology: branch adds edge V->W; expansion from V (and into W) must see the new edge even
     though V/W were branched only-for-a-property earlier.
  7. edge on a property-churned vertex: SET n.age on V (edge filter CLEAR) -> V's existing adjacency
     still reads correctly via the direct-from-main fast path.
  8. delete still hidden: DETACH DELETE a vertex -> gone at View::NEW despite any filter state.
"""
import os
import socket
import subprocess
import sys
import time

from neo4j import GraphDatabase

MG = os.environ.get("MG", "/home/andreja/workspace/memgraph/build/memgraph")
PORT = int(os.environ.get("PORT", "7796"))
DATA = os.environ.get("DATA", f"/home/andreja/.claude/jobs/c8905448/tmp/smoke-cf-{PORT}")


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
            for i in range(1, 31):
                s.run("CREATE (:User {id:$id, age:$a, name:$n})", id=i, a=10 + i, n=f"u{i}").consume()
            # fork edge: id1 -[:FOLLOWS]-> id2 (used by case 7 to prove edge-filter-clear reads fork adjacency)
            s.run("MATCH (a:User {id:1}),(b:User {id:2}) CREATE (a)-[:FOLLOWS]->(b)").consume()

            # ---- branch b1 ----
            s.run("CREATE BRANCH b1 FROM main").consume()
            s.run("CHECKOUT BRANCH b1").consume()

            # (1) label-clear / prop-set: SET age on id=3, read :User (fork-correct) + age (resolves NEW)
            s.run("MATCH (n:User {id:3}) SET n.age=999").consume()
            r = q(s, "MATCH (n:User {id:3}) RETURN n.age AS age, labels(n) AS lbls")
            if not (r and r[0]["age"] == 999 and r[0]["lbls"] == ["User"]):
                fail.append(f"[1] label-clear/prop-set wrong: {r} (expect age=999, lbls=[User])")

            # (2) prop-clear / label-set: add :VIP on id=4, read age (fork-correct) + label (resolves)
            s.run("MATCH (n:User {id:4}) SET n:VIP").consume()
            r = q(s, "MATCH (n:User {id:4}) RETURN n.age AS age, ('VIP' IN labels(n)) AS vip")
            if not (r and r[0]["age"] == 14 and r[0]["vip"] is True):
                fail.append(f"[2] prop-clear/label-set wrong: {r} (expect age=14, vip=True)")

            # (5) same-query COW-then-read on a fresh vertex (id=5)
            r = q(s, "MATCH (n:User {id:5}) SET n.age=555 RETURN n.age AS age")
            if not (r and r[0]["age"] == 555):
                fail.append(f"[5] same-query COW-then-read wrong: {r} (expect 555)")

            # (6) edge topology: add id5->id6 edge, expand from id5 must see it
            s.run("MATCH (a:User {id:5}),(b:User {id:6}) CREATE (a)-[:KNOWS]->(b)").consume()
            r = q(s, "MATCH (a:User {id:5})-[:KNOWS]->(b) RETURN b.id AS bid ORDER BY bid")
            if [row["bid"] for row in r] != [6]:
                fail.append(f"[6] edge topology (out) wrong: {r} (expect [6])")
            r = q(s, "MATCH (a)-[:KNOWS]->(b:User {id:6}) RETURN a.id AS aid ORDER BY aid")
            if [row["aid"] for row in r] != [5]:
                fail.append(f"[6b] edge topology (in) wrong: {r} (expect [5])")

            # (7) edge-filter clear on a prop-churned vertex: SET age on id1 (prop change -> id1's
            #     EDGE filter stays clear), then expand id1's fork adjacency -> must still return id2
            #     via the direct-from-main fast path (main adjacency == fork adjacency, unchanged).
            s.run("MATCH (n:User {id:1}) SET n.age=111").consume()
            r = q(s, "MATCH (a:User {id:1})-[:FOLLOWS]->(b) RETURN b.id AS bid ORDER BY bid")
            if [row["bid"] for row in r] != [2]:
                fail.append(f"[7] edge-filter-clear fork adjacency wrong: {r} (expect [2])")
            r = q(s, "MATCH (a:User {id:1}) OPTIONAL MATCH (a)-[e]->() RETURN count(e) AS deg")
            if not (r and r[0]["deg"] == 1):
                fail.append(f"[7b] edge-filter-clear out-degree wrong: {r} (expect 1)")

            # (8) delete still hidden
            s.run("MATCH (n:User {id:7}) DETACH DELETE n").consume()
            if q(s, "MATCH (n:User {id:7}) RETURN n") != []:
                fail.append("[8] deleted vertex still visible on branch")

            # ---- cross-branch isolation + C1 lock ----
            s.run("CHECKOUT BRANCH main").consume()
            s.run("CREATE BRANCH b2 FROM main").consume()
            s.run("CHECKOUT BRANCH b2").consume()
            # b2 never touches id=8/id=9. b1 already changed id=3 (age) and id=4 (:VIP).
            # (3) cross-branch: on b2, id=3 age must be FORK (13), id=4 must NOT have :VIP.
            r = q(s, "MATCH (n:User {id:3}) RETURN n.age AS age")
            if not (r and r[0]["age"] == 13):
                fail.append(f"[3] cross-branch prop leak: id3 age={r} on b2 (expect fork 13)")
            r = q(s, "MATCH (n:User {id:4}) RETURN ('VIP' IN labels(n)) AS vip")
            if not (r and r[0]["vip"] is False):
                fail.append(f"[3b] cross-branch label leak: id4 vip={r} on b2 (expect fork False)")

        # (4) C1 lock: main mutates id=8 age AFTER b2 forked; b2 must still read fork value.
        with drv.session() as s:
            s.run("CHECKOUT BRANCH main").consume()
            s.run("MATCH (n:User {id:8}) SET n.age=88888").consume()  # main post-fork mutation
            s.run("CHECKOUT BRANCH b2").consume()
            r = q(s, "MATCH (n:User {id:8}) RETURN n.age AS age")
            if not (r and r[0]["age"] == 18):
                fail.append(f"[4] C1 lock FAILED: b2 sees id8 age={r} (expect fork 18, NOT main's 88888)")
            # sanity: main itself sees the new value
            s.run("CHECKOUT BRANCH main").consume()
            r = q(s, "MATCH (n:User {id:8}) RETURN n.age AS age")
            if not (r and r[0]["age"] == 88888):
                fail.append(f"[4b] main lost its own post-fork write: id8 age={r} (expect 88888)")
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
    print("PASS: branch-side change filters correct (label/prop/edge kind isolation, cross-branch, C1 lock, delete)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
