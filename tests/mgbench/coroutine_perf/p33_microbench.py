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


# --- load synthetic pull-heavy graph if empty (NOT timed) ---
n = run("MATCH (n:N) RETURN count(*)")[0][0]
if n < 100000:
    run("CREATE INDEX ON :N(id)")
    for b in range(0, 100000, 10000):
        run(f"UNWIND range({b+1},{b+10000}) AS i CREATE (:N {{id:i, x:i%100, g:i%10}})")
    # chain edges i -> i+1
    for b in range(0, 100000, 10000):
        run(f"UNWIND range({b+1},{b+10000-1}) AS i MATCH (a:N{{id:i}}),(b:N{{id:i+1}}) CREATE (a)-[:R]->(b)")
print(
    f"[{TAG}] nodes={run('MATCH (n:N) RETURN count(*)')[0][0]} edges={run('MATCH ()-[r:R]->() RETURN count(*)')[0][0]}",
    flush=True,
)

QUERIES = {
    "scan_count": "MATCH (n:N) RETURN count(*)",
    "filter_count": "MATCH (n:N) WHERE n.x >= 50 RETURN count(*)",
    "sum_agg": "MATCH (n:N) RETURN sum(n.x)",
    "group_agg": "MATCH (n:N) RETURN n.g AS g, count(*) AS c ORDER BY g",
    "expand_count": "MATCH (n:N)-[:R]->(m) RETURN count(*)",
    "expand2_count": "MATCH (n:N)-[:R]->()-[:R]->(m) RETURN count(*)",
    "project_10k": "MATCH (n:N) RETURN n.id AS id LIMIT 10000",
    "orderby_1k": "MATCH (n:N) RETURN n.id AS id ORDER BY id DESC LIMIT 1000",
}
K = 40  # iterations per query
# warm (5 untimed)
for q in QUERIES.values():
    for _ in range(5):
        run(q)

results = {}
for name, q in QUERIES.items():
    samples = []
    for _ in range(K):
        t = time.perf_counter()
        run(q)
        samples.append((time.perf_counter() - t) * 1000.0)  # ms
    samples.sort()
    results[name] = statistics.median(samples)
    print(f"[{TAG}] {name:14s} median={statistics.median(samples):8.3f}ms  min={samples[0]:8.3f}ms", flush=True)
import json

json.dump(results, open(f"/tmp/p33_{TAG}.json", "w"))
