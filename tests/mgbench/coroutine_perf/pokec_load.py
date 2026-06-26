import sys
import time

import mgclient

port = int(sys.argv[1])
data_file = sys.argv[2]
index_file = sys.argv[3]

c = mgclient.connect(host="127.0.0.1", port=port)


def exec_commit(stmts):
    for s in stmts:
        cur.execute(s)
    c.commit()


# indexes first (so edge MATCHes are fast) — MUST be in autocommit (implicit tx)
c.autocommit = True
cur = c.cursor()
with open(index_file) as f:
    idx = [ln.strip().rstrip(";").strip() for ln in f if ln.strip() and ln.strip() != ";"]
for s in idx:
    cur.execute(s)
    cur.fetchall()
print(f"created {len(idx)} indexes", flush=True)

# data in batched explicit transactions
c.autocommit = False
cur = c.cursor()

BATCH = 5000
batch = []
total = 0
t0 = time.time()
with open(data_file) as f:
    for ln in f:
        s = ln.strip()
        if not s or s == ";":
            continue
        batch.append(s.rstrip(";").strip())
        if len(batch) >= BATCH:
            exec_commit(batch)
            total += len(batch)
            batch = []
            if total % 100000 == 0:
                print(f"  loaded {total} stmts ({total/(time.time()-t0):.0f}/s)", flush=True)
if batch:
    exec_commit(batch)
    total += len(batch)
print(f"loaded {total} statements in {time.time()-t0:.1f}s", flush=True)

c.autocommit = True
cur = c.cursor()
cur.execute("MATCH (n:User) RETURN count(*)")
v = cur.fetchall()[0][0]
cur.execute("MATCH ()-[r]->() RETURN count(*)")
e = cur.fetchall()[0][0]
print(f"FINAL nodes={v} edges={e}", flush=True)
assert v == 100000, f"expected 100000 vertices, got {v}"
assert e == 1768515, f"expected 1768515 edges, got {e}"
