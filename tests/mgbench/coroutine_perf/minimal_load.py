import sys

import mgclient

port = int(sys.argv[1])
c = mgclient.connect(host="127.0.0.1", port=port)
c.autocommit = True
cur = c.cursor()


def run(q):
    cur.execute(q)
    return cur.fetchall()


n = run("MATCH (n:N) RETURN count(*)")[0][0]
if n < 100000:
    run("CREATE INDEX ON :N(id)")
    for b in range(0, 100000, 10000):
        run(f"UNWIND range({b+1},{b+10000}) AS i CREATE (:N {{id:i, x:i%100, g:i%10}})")
    for b in range(0, 100000, 10000):
        run(f"UNWIND range({b+1},{b+10000-1}) AS i MATCH (a:N{{id:i}}),(b:N{{id:i+1}}) CREATE (a)-[:R]->(b)")
nodes = run("MATCH (n:N) RETURN count(*)")[0][0]
edges = run("MATCH ()-[r:R]->() RETURN count(*)")[0][0]
print(f"loaded nodes={nodes} edges={edges}", file=sys.stderr)
assert nodes == 100000, f"GRAPH NOT LOADED: nodes={nodes}"
