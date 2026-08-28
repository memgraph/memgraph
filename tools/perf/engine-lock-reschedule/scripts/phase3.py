#!/usr/bin/env python3
# Phase-3 driver v2 — full workload spectrum. READERS + WRITERS against the main, concurrently.
# Parameterized by env so one script covers fast/long reads, short/long writes, explicit/implicit txns:
#   RMODE  = explicit | auto     reader txn mode (explicit: BEGIN + NQ queries + COMMIT; auto: per-query)
#   RQROWS = read query cost      (fast ~130k, long ~1M rows in UNWIND..RETURN sum)
#   NQ     = queries per explicit read txn (RMODE=explicit only; the in-flight non-BEGIN/COMMIT work)
#   WMODE  = explicit | auto     writer txn mode
#   WQROWS = 0 -> short write (single CREATE); >0 -> long write (UNWIND range(1,WQROWS) CREATE, heavy exec)
# Writers' COMMIT blocks on the (netem-delayed) SYNC/STRICT_SYNC replica ack -> the "slow commit".
# Reports read QUERY throughput + latency percentiles, read txn rate, and write throughput.
#
# Usage: phase3.py <uri> <n_readers> <n_writers> <dur_s>
import sys, os, time, multiprocessing as mp
from neo4j import GraphDatabase

URI = sys.argv[1]
NR = int(sys.argv[2]); NW = int(sys.argv[3]); DUR = float(sys.argv[4])
RMODE = os.environ.get("RMODE", "explicit")
RQROWS = int(os.environ.get("RQROWS", "1000000"))
NQ = int(os.environ.get("NQ", "8"))
WMODE = os.environ.get("WMODE", "explicit")
WQROWS = int(os.environ.get("WQROWS", "0"))
READ_Q = f"UNWIND range(1,{RQROWS}) AS x RETURN sum(x)"
WRITE_Q = (f"UNWIND range(1,{WQROWS}) AS i CREATE (n:W {{w:$w, i:i}})" if WQROWS > 0
           else "CREATE (n:W {w:$w, i:$i})")

def reader(idx, q, stop_at):
    d = GraphDatabase.driver(URI, auth=None); ntx = 0; nq = 0; qlats = []
    try:
        with d.session() as s:
            while time.monotonic() < stop_at:
                if RMODE == "auto":
                    t0 = time.monotonic()
                    s.run(READ_Q).consume()                 # implicit txn: BEGIN+exec+COMMIT per query
                    qlats.append((time.monotonic() - t0) * 1000.0); nq += 1; ntx += 1
                else:
                    tx = s.begin_transaction()               # explicit BEGIN (needs engine_lock)
                    for _ in range(NQ):
                        if time.monotonic() >= stop_at: break
                        t0 = time.monotonic()
                        tx.run(READ_Q).consume()             # engine-lock-free PULL = "other work"
                        qlats.append((time.monotonic() - t0) * 1000.0); nq += 1
                    tx.commit(); ntx += 1
    except Exception as e:
        q.put(("rerr", str(e)[:120])); d.close(); return
    d.close(); q.put(("r", ntx, nq, qlats))

def writer(idx, q, stop_at):
    d = GraphDatabase.driver(URI, auth=None); n = 0; lats = []
    try:
        with d.session() as s:
            i = 0
            while time.monotonic() < stop_at:
                t0 = time.monotonic()
                if WMODE == "auto":
                    s.run(WRITE_Q, w=idx, i=i).consume()     # implicit write txn
                else:
                    tx = s.begin_transaction()
                    tx.run(WRITE_Q, w=idx, i=i).consume()
                    tx.commit()                              # blocks on replica ack -> slow commit
                lats.append((time.monotonic() - t0) * 1000.0); n += 1; i += 1
    except Exception as e:
        q.put(("werr", str(e)[:120])); d.close(); return
    d.close(); q.put(("w", n, lats))

def pct(xs, p):
    if not xs: return 0.0
    xs = sorted(xs); k = min(len(xs) - 1, int(p / 100.0 * len(xs)))
    return xs[k]

def main():
    q = mp.Queue(); stop_at = time.monotonic() + DUR + 0.5
    procs = []
    for i in range(NR): procs.append(mp.Process(target=reader, args=(i, q, stop_at)))
    for i in range(NW): procs.append(mp.Process(target=writer, args=(i, q, stop_at)))
    t0 = time.monotonic()
    for p in procs: p.start()
    got = []; need = len(procs)                              # drain concurrently (avoid join-before-drain deadlock)
    while len(got) < need:
        try: got.append(q.get(timeout=DUR + 120))
        except Exception: break
    for p in procs: p.join(timeout=10)
    elapsed = time.monotonic() - t0
    rtx = sum(m[1] for m in got if m[0] == "r")
    rq = sum(m[2] for m in got if m[0] == "r")
    rql = [x for m in got if m[0] == "r" for x in m[3]]
    wn = sum(m[1] for m in got if m[0] == "w"); wl = [x for m in got if m[0] == "w" for x in m[2]]
    errs = [m for m in got if m[0] in ("rerr", "werr")]
    print(f"R={NR} W={NW} dur={elapsed:.1f}s | "
          f"READ q/s={rq/elapsed:7.1f} txn/s={rtx/elapsed:6.1f} q_p50={pct(rql,50):6.1f} q_p99={pct(rql,99):7.1f} | "
          f"WRITE op/s={wn/elapsed:6.1f} p50={pct(wl,50):7.1f}ms"
          + (f" | ERR={len(errs)}:{errs[0][1]}" if errs else ""))

if __name__ == "__main__":
    main()
