import statistics
import sys
import time

import mgclient

PORT = int(sys.argv[1])
TAG = sys.argv[2]
c = mgclient.connect(host="127.0.0.1", port=PORT)
c.autocommit = True


def run(q):
    cur = c.cursor()
    cur.execute(q)
    return cur.fetchall()


# high fan-out: 1000 :S x 100 :T = 100k edges; each :S has 100 out-edges => InitEdges ~once/source
if run("MATCH (n:S) RETURN count(*)")[0][0] < 1000:
    run("UNWIND range(1,1000) AS i CREATE (:S {id:i})")
    run("UNWIND range(1,100)  AS i CREATE (:T {id:i})")
    run("MATCH (s:S),(t:T) CREATE (s)-[:R]->(t)")
print(
    f"[{TAG}] S={run('MATCH (n:S) RETURN count(*)')[0][0]} T={run('MATCH (n:T) RETURN count(*)')[0][0]} edges={run('MATCH ()-[r:R]->() RETURN count(*)')[0][0]}",
    flush=True,
)
Q = "MATCH (s:S)-[:R]->(t) RETURN count(*)"  # 100k edges, ~1000 InitEdges calls
K = 40
for _ in range(5):
    run(Q)  # warm
s = []
for _ in range(K):
    t = time.perf_counter()
    run(Q)
    s.append((time.perf_counter() - t) * 1000)
s.sort()
import json

json.dump({"expand_fanout": statistics.median(s)}, open(f"/tmp/fan_{TAG}.json", "w"))
print(f"[{TAG}] expand_fanout median={statistics.median(s):.3f}ms min={s[0]:.3f}ms", flush=True)
