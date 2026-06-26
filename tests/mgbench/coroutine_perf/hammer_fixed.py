import sys

import mgclient

port = int(sys.argv[1])
q = sys.argv[2]
n = int(sys.argv[3])
c = mgclient.connect(host="127.0.0.1", port=port)
c.autocommit = True
cur = c.cursor()
# warm-up (not measured by the outer perf window: outer perf attaches before this runs,
# but warm-up cost is amortized across n; keep n large)
for _ in range(n):
    cur.execute(q)
    cur.fetchall()
print(f"completed {n} queries", file=sys.stderr)
