import json
import statistics
import sys
import time

import mgclient

PORT = int(sys.argv[1])
TAG = sys.argv[2]


def conn():
    c = mgclient.connect(host="127.0.0.1", port=PORT)
    c.autocommit = True
    return c


c = conn()


def run(q):
    cur = c.cursor()
    cur.execute(q)
    return cur.fetchall()


# --- load :Node graph if empty (NOT timed) ---
n = run("MATCH (n:Node) RETURN count(*)")[0][0]
if n < 100000:
    for b in range(0, 100000, 10000):
        run(f"UNWIND range({b+1},{b+10000}) AS i CREATE (:Node {{id:i, value:i%100, category:i%10}})")
nodes = run("MATCH (n:Node) RETURN count(*)")[0][0]
assert nodes == 100000, f"graph not loaded: {nodes}"

# non-vacuity: confirm the parallel plan actually parallelizes
expl = [r[0] for r in run("EXPLAIN USING PARALLEL EXECUTION MATCH (n:Node) RETURN count(n)")]
threads = any("threads" in str(r).lower() for r in expl)
print(f"[{TAG}] nodes={nodes} parallel_plan_has_threads={threads}", flush=True)

QUERIES = {
    "p_count": "USING PARALLEL EXECUTION MATCH (n:Node) RETURN count(n) AS c",
    "p_sum": "USING PARALLEL EXECUTION MATCH (n:Node) RETURN sum(n.value) AS s",
    "p_grouped": "USING PARALLEL EXECUTION MATCH (n:Node) RETURN n.category AS g, count(*) AS c ORDER BY g",
    "p_orderby": "USING PARALLEL EXECUTION MATCH (n:Node) RETURN n.id AS id ORDER BY n.value DESC, n.id ASC LIMIT 1000",
}
K = 30
for q in QUERIES.values():  # warm
    for _ in range(5):
        run(q)

results = {}
for name, q in QUERIES.items():
    samples = []
    for _ in range(K):
        t = time.perf_counter()
        run(q)
        samples.append((time.perf_counter() - t) * 1000.0)
    samples.sort()
    results[name] = statistics.median(samples)
    print(f"[{TAG}] {name:12s} median={statistics.median(samples):8.3f}ms  min={samples[0]:8.3f}ms", flush=True)

json.dump(results, open(f"/tmp/par_{TAG}.json", "w"))
