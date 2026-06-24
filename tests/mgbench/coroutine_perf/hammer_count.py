"""Tight single-connection query loop that COUNTS completed queries (prints QUERIES=N to stderr).

Used by kernel_counter_ab.sh: run under `perf stat -p <memgraph-pid>` for the loop's exact
duration so instructions/query is measured directly (deterministic; the primary P3.3 signal).
Companion to hammer.py (which does not count). See INVESTIGATION.md (EXP-1/2/3/11)."""
import sys
import time

import mgclient

port = int(sys.argv[1])
q = sys.argv[2]
secs = float(sys.argv[3])
c = mgclient.connect(host="127.0.0.1", port=port)
c.autocommit = True
cur = c.cursor()
n = 0
end = time.time() + secs
while time.time() < end:
    cur.execute(q)
    cur.fetchall()
    n += 1
sys.stderr.write(f"QUERIES={n}\n")
sys.stderr.flush()
