#!/usr/bin/env python3
"""Red-green repro for the USING VERSION 'main' plan-cache-pollution bug.

Scenario (verifier HIGH-1): checked out on branch b, a `USING VERSION 'main'` query
routes at main, builds a main-shaped plan, and (with the bug) stores it in the BRANCH
plan cache keyed by stripped text. A later ordinary branch query with the same stripped
text hits that main-shaped plan -> silently wrong results on the branch.

Prints whether the bug reproduces (exit 1 = bug present, exit 0 = correct).
"""
import os
import socket
import subprocess
import sys
import time

from neo4j import GraphDatabase

MG = os.environ.get("MG", "/home/andreja/workspace/memgraph/build/memgraph")
PORT = int(os.environ.get("PORT", "7798"))
DATA = f"/tmp/lima/tmp/claude-1000/repro-uv-{PORT}"


def wait_bolt(port, timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            with socket.create_connection(("127.0.0.1", port), 1):
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
        stdout=open(os.path.join(DATA, "mg.log"), "w"),
        stderr=subprocess.STDOUT,
    )
    bug = False
    try:
        if not wait_bolt(PORT):
            print("!! bolt down")
            return 2
        drv = GraphDatabase.driver(f"bolt://127.0.0.1:{PORT}", auth=("", ""))
        with drv.session() as s:
            s.run("CREATE INDEX ON :User(id)").consume()
            for i in range(20):
                s.run("CREATE (:User {id:$id, tag:'main'})", id=i).consume()
            s.run("CREATE BRANCH b FROM main").consume()
            s.run("CHECKOUT BRANCH b").consume()
            # branch-only row that main does NOT have:
            s.run("CREATE (:User {id:100, tag:'branch'})").consume()

            Q = "MATCH (n:User {id:100}) RETURN n.tag AS tag"
            # 1) prime: USING VERSION 'main' routes at main -> main has no id=100 -> []
            uv = q(s, "USING VERSION 'main' " + Q)
            print(f"   USING VERSION 'main' result (expect []): {uv}")
            # 2) ordinary branch query, SAME base text -> must see the branch row
            br = q(s, Q)
            print(f"   ordinary branch result (expect tag=branch): {br}")

            if br != [{"tag": "branch"}]:
                bug = True
                print(
                    f"!! BUG REPRODUCED: ordinary branch query returned {br}, "
                    f"expected [{{'tag':'branch'}}] -- main-shaped plan leaked into branch cache"
                )
            else:
                print("   OK: ordinary branch query correct (no pollution / keys differ)")

            # reverse direction: does a branch-shaped plan pollute a later USING VERSION 'main'?
            uv2 = q(s, "USING VERSION 'main' " + Q)
            print(f"   USING VERSION 'main' again (expect []): {uv2}")
            if uv2 != []:
                bug = True
                print(f"!! BUG (reverse): USING VERSION 'main' returned {uv2}, expected []")
        drv.close()
    finally:
        proc.terminate()
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            proc.kill()
    return 1 if bug else 0


if __name__ == "__main__":
    rc = main()
    print("RESULT:", "BUG PRESENT" if rc == 1 else ("ERROR" if rc > 1 else "CORRECT"))
    sys.exit(rc)
