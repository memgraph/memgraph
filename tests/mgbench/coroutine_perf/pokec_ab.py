import json
import statistics
import sys
import time

import mgclient

PORT = int(sys.argv[1])
TAG = sys.argv[2]

c = mgclient.connect(host="127.0.0.1", port=PORT)
c.autocommit = True
cur = c.cursor()


def run(q, p=None):
    cur.execute(q, p or {})
    return cur.fetchall()


nodes = run("MATCH (n:User) RETURN count(*)")[0][0]
assert nodes == 100000, f"snapshot not recovered: nodes={nodes}"

# Fixed pool of real vertex ids (deterministic ORDER BY id) so every arm hits the SAME vertices.
POOL = [r[0] for r in run("MATCH (n:User) RETURN n.id AS id ORDER BY id LIMIT 300")]

# Representative read slice of the standard mgbench pokec queries.
# (name, query, param_mode)  param_mode: "scan" = no params (run once x K), "vertex" = $id from POOL
QUERIES = [
    ("aggregate", "MATCH (n:User) RETURN n.age, COUNT(*)", "scan"),
    ("aggregate_filter", "MATCH (n:User) WHERE n.age >= 18 RETURN n.age, COUNT(*)", "scan"),
    ("agg_count", "MATCH (n:User) RETURN count(n)", "scan"),
    ("point_lookup", "MATCH (n:User {id: $id}) RETURN n", "vertex"),
    ("expansion_1", "MATCH (s:User {id: $id})-->(n:User) RETURN n.id", "vertex"),
    ("expansion_2", "MATCH (s:User {id: $id})-->()-->(n:User) RETURN DISTINCT n.id", "vertex"),
    ("expansion_2_filter", "MATCH (s:User {id: $id})-->()-->(n:User) WHERE n.age >= 18 RETURN DISTINCT n.id", "vertex"),
]

# warm
for _, q, mode in QUERIES:
    for i in range(5):
        run(q, {"id": POOL[i]} if mode == "vertex" else None)

results = {}
for name, q, mode in QUERIES:
    samples = []
    if mode == "scan":
        K = 25
        for _ in range(K):
            t = time.perf_counter()
            run(q)
            samples.append((time.perf_counter() - t) * 1000.0)
    else:
        # one run per vertex in the pool (same pool across arms)
        for vid in POOL:
            t = time.perf_counter()
            run(q, {"id": vid})
            samples.append((time.perf_counter() - t) * 1000.0)
    samples.sort()
    results[name] = statistics.median(samples)
    print(f"[{TAG}] {name:20s} median={statistics.median(samples):8.3f}ms  n={len(samples)}", flush=True)

json.dump(results, open(f"/tmp/pokec_{TAG}.json", "w"))
