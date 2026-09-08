#!/usr/bin/env python3
"""A/B benchmark for the experimental `lockfree-read-snapshot` commit flag.

The flag makes a write committer release its internal engine lock right after
minting the commit timestamp (running WAL + SYNC replication under a separate
mutex). With the flag OFF the engine lock is held across the whole commit,
including the SYNC-replica ACK wait, so every new BEGIN stalls behind another
transaction's slow commit. The win is therefore *concurrent reads not stalling
behind slow commits*.

Each scenario is run twice against a freshly launched `memgraph` with a fresh
temp data-directory:

  * OFF -- plain launch.
  * ON  -- same launch plus ``--experimental-enabled=lockfree-read-snapshot``.

Only the MAIN's flag is toggled; in the GOOD scenario the replica is always
launched plain.

Scenarios
---------
GOOD    reads under slow SYNC commits -- writers commit against a genuinely
        slow SYNC replica while readers do fast point reads. Expect ON to raise
        reader throughput / lower p99 (readers no longer stall behind the write
        commits' replica-wait). Needs an enterprise license for REGISTER
        REPLICA; skipped gracefully if unavailable.
BAD     single-threaded write overhead -- one writer, no replica, no read
        contention to relieve. Expect ON slightly LOWER (commit-serializer
        mutex + snapshot-ring writes are pure overhead here).
NEUTRAL read-only, no writers -- expect ON ~EQUAL to OFF (one extra atomic
        load at BEGIN, nothing to unblock).

Python only, uses the `neo4j` driver over Bolt against a local `build/memgraph`
process. No Docker, no netem/netns/iptables -- only localhost processes, flags
and Cypher.
"""

from __future__ import annotations

import argparse
import multiprocessing
import os
import queue
import random
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from array import array
from contextlib import contextmanager
from dataclasses import dataclass, field

try:
    from neo4j import GraphDatabase
    from neo4j.exceptions import Neo4jError, ServiceUnavailable, TransientError
except ImportError:  # pragma: no cover
    sys.exit("error: the 'neo4j' driver is required (pip install neo4j==5.28.3)")

# Ports. MAIN speaks Bolt on MAIN_BOLT; the GOOD scenario also launches a
# replica on REPLICA_BOLT that listens for replication on REPLICA_REPL_PORT.
MAIN_BOLT = 7687
REPLICA_BOLT = 7688
REPLICA_REPL_PORT = 10000

EXPERIMENTAL_FLAG_DEFAULT = "lockfree-read-snapshot"


# --------------------------------------------------------------------------- #
# memgraph process management
# --------------------------------------------------------------------------- #
class MemgraphInstance:
    """A single launched `memgraph` subprocess with its own temp data-dir."""

    def __init__(self, binary: str, bolt_port: int, name: str, extra_args: list[str], env: dict[str, str]):
        self.binary = binary
        self.bolt_port = bolt_port
        self.name = name
        self.extra_args = extra_args
        self.env = env
        self.data_dir = tempfile.mkdtemp(prefix=f"mg_{name}_")
        self.log_path = os.path.join(self.data_dir, f"{name}.log")
        self.proc: subprocess.Popen | None = None
        self._log_fh = None

    def start(self) -> None:
        args = [
            self.binary,
            f"--bolt-port={self.bolt_port}",
            f"--data-directory={self.data_dir}/data",
            "--telemetry-enabled=false",
            "--log-level=WARNING",
            "--data-recovery-on-startup=false",
            *self.extra_args,
        ]
        self._log_fh = open(self.log_path, "w")
        self.proc = subprocess.Popen(
            args,
            stdout=self._log_fh,
            stderr=subprocess.STDOUT,
            env=self.env,
        )

    def wait_until_ready(self, timeout: float = 30.0) -> None:
        deadline = time.monotonic() + timeout
        uri = f"bolt://127.0.0.1:{self.bolt_port}"
        last_err: Exception | None = None
        while time.monotonic() < deadline:
            if self.proc is not None and self.proc.poll() is not None:
                raise RuntimeError(
                    f"{self.name} exited early (code {self.proc.returncode}). " f"Log tail:\n{self._log_tail()}"
                )
            try:
                drv = GraphDatabase.driver(uri, auth=("", ""))
                with drv.session() as s:
                    s.run("RETURN 1").consume()
                drv.close()
                return
            except (ServiceUnavailable, Neo4jError, OSError) as exc:
                last_err = exc
                time.sleep(0.1)
        raise RuntimeError(
            f"{self.name} not ready after {timeout}s (last error: {last_err}). " f"Log tail:\n{self._log_tail()}"
        )

    def _log_tail(self, n: int = 15) -> str:
        try:
            with open(self.log_path) as fh:
                return "".join(fh.readlines()[-n:])
        except OSError:
            return "(no log)"

    def stop(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
        if self._log_fh is not None:
            self._log_fh.close()
            self._log_fh = None

    def cleanup(self) -> None:
        self.stop()
        shutil.rmtree(self.data_dir, ignore_errors=True)


@contextmanager
def launched(binary, bolt_port, name, extra_args, env, ready_timeout):
    inst = MemgraphInstance(binary, bolt_port, name, extra_args, env)
    try:
        inst.start()
        inst.wait_until_ready(ready_timeout)
        yield inst
    finally:
        inst.cleanup()


# --------------------------------------------------------------------------- #
# Cypher helpers
# --------------------------------------------------------------------------- #
def seed(driver, vertices: int) -> None:
    with driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n").consume()
        s.run("CREATE INDEX ON :Node(id)").consume()
        # One UNWIND commit; batched so seeding stays cheap even with a replica.
        batch = 10000
        for lo in range(0, vertices, batch):
            hi = min(lo + batch, vertices)
            s.run(
                "UNWIND range($lo, $hi - 1) AS i CREATE (:Node {id: i, v: 0})",
                lo=lo,
                hi=hi,
            ).consume()


READ_Q = "MATCH (n:Node {id: $id}) RETURN n.v"
WRITE_Q = "MATCH (n:Node {id: $id}) SET n.v = $v"
# GOOD writer: one large CREATE-only transaction. Fresh nodes => no shared ids
# => no write-write conflicts, and a big WAL write (plus the replica applying
# every delta) holds engine_lock for real time in the OFF path.
CHURN_Q = "UNWIND range(0, $batch - 1) AS i CREATE (:Churn {w: $w, i: i})"

MAX_RETRIES = 4  # transient-conflict retries before an op is counted as an error


# --------------------------------------------------------------------------- #
# Worker loops
# --------------------------------------------------------------------------- #
@dataclass
class WorkerResult:
    starts: array = field(default_factory=lambda: array("d"))
    lats: array = field(default_factory=lambda: array("d"))
    errors: int = 0


def read_op_factory(vertices):
    def op(session, rng):
        session.run(READ_Q, id=rng.randrange(vertices)).consume()

    return op


def set_op_factory(vertices):
    def op(session, rng):
        session.run(WRITE_Q, id=rng.randrange(vertices), v=rng.randrange(1_000_000)).consume()

    return op


def churn_op_factory(write_batch, w):
    def op(session, rng):
        session.run(CHURN_Q, batch=write_batch, w=w).consume()

    return op


def _worker_loop(driver, op, seed_val, barrier, stop_event, out):
    """Loop `op` until stopped. Never propagates an exception: a failed op is
    counted (out.errors) and the loop continues, refreshing the session so one
    bad op cannot wedge the thread and corrupt the measurement."""
    rng = random.Random(seed_val)
    session = driver.session()
    try:
        # Prime the connection/plan cache before the barrier so warmup timing
        # is not polluted by first-query cost.
        try:
            op(session, rng)
        except Exception:  # noqa: BLE001
            pass
        barrier.wait()
        while not stop_event.is_set():
            t0 = time.monotonic()
            ok = False
            for attempt in range(MAX_RETRIES):
                try:
                    op(session, rng)
                    ok = True
                    break
                except TransientError:
                    time.sleep(0.001 * (attempt + 1))  # brief backoff, then retry
                    continue
                except Exception:  # noqa: BLE001 - any query error: count + move on
                    break
            t1 = time.monotonic()
            if ok:
                out.starts.append(t0)
                out.lats.append(t1 - t0)
            else:
                out.errors += 1
                try:  # a failed op may leave the session unusable; refresh it
                    session.close()
                except Exception:  # noqa: BLE001
                    pass
                session = driver.session()
    finally:
        try:
            session.close()
        except Exception:  # noqa: BLE001
            pass


def run_workload(driver, readers, reader_factory, writers, writer_factory, duration, warmup, rng):
    """Run `readers` read threads and `writers` write threads concurrently.

    `reader_factory(k)` / `writer_factory(k)` return the per-thread op callable.
    Ops are counted only inside the [warmup, warmup+duration] window so ramp-up
    is excluded. Returns (read_result, write_result, window_seconds) where each
    result is (latencies, error_count).
    """
    n = readers + writers
    barrier = threading.Barrier(n + 1)
    stop_event = threading.Event()
    read_res = [WorkerResult() for _ in range(readers)]
    write_res = [WorkerResult() for _ in range(writers)]
    threads: list[threading.Thread] = []

    for k in range(readers):
        t = threading.Thread(
            target=_worker_loop,
            args=(driver, reader_factory(k), rng.randrange(1 << 30), barrier, stop_event, read_res[k]),
            daemon=True,
        )
        threads.append(t)
    for k in range(writers):
        t = threading.Thread(
            target=_worker_loop,
            args=(driver, writer_factory(k), rng.randrange(1 << 30), barrier, stop_event, write_res[k]),
            daemon=True,
        )
        threads.append(t)

    for t in threads:
        t.start()
    barrier.wait()
    t_start = time.monotonic()
    try:
        time.sleep(warmup + duration)
    finally:
        stop_event.set()
    for t in threads:
        t.join(timeout=30)

    win_lo = t_start + warmup
    win_hi = t_start + warmup + duration
    return (_collapse(read_res, win_lo, win_hi), _collapse(write_res, win_lo, win_hi), duration)


def _collapse(results: list[WorkerResult], win_lo, win_hi):
    """Merge per-thread ops, keeping only those started inside the window.

    Returns (latencies, total_error_count)."""
    lats: list[float] = []
    errors = 0
    for r in results:
        errors += r.errors
        for s0, lat in zip(r.starts, r.lats):
            if win_lo <= s0 < win_hi:
                lats.append(lat)
    return lats, errors


# --------------------------------------------------------------------------- #
# Process-mode readers (bypass the GIL so the *server* is the bottleneck)
# --------------------------------------------------------------------------- #
# On Linux CLOCK_MONOTONIC is system-wide (boot-relative), so a time.monotonic()
# value minted in the parent is directly comparable in a child process; we use
# an absolute `t0` to align every reader's start and window instead of a
# cross-process barrier.
def _reader_proc(bolt_port, vertices, seed_val, t0, warmup, duration, out_q):
    """Reader run in its OWN process/driver; returns (window_latencies, errors)."""
    rng = random.Random(seed_val)
    driver = GraphDatabase.driver(f"bolt://127.0.0.1:{bolt_port}", auth=("", ""))
    starts: list[float] = []
    lats: list[float] = []
    errors = 0
    session = None
    try:
        session = driver.session()
        try:
            session.run(READ_Q, id=rng.randrange(vertices)).consume()  # prime
        except Exception:  # noqa: BLE001
            pass
        while time.monotonic() < t0:  # align start across processes
            time.sleep(0.0005)
        end = t0 + warmup + duration
        while time.monotonic() < end:
            s0 = time.monotonic()
            ok = False
            for attempt in range(MAX_RETRIES):
                try:
                    session.run(READ_Q, id=rng.randrange(vertices)).consume()
                    ok = True
                    break
                except TransientError:
                    time.sleep(0.001 * (attempt + 1))
                except Exception:  # noqa: BLE001
                    break
            s1 = time.monotonic()
            if ok:
                starts.append(s0)
                lats.append(s1 - s0)
            else:
                errors += 1
                try:
                    session.close()
                except Exception:  # noqa: BLE001
                    pass
                session = driver.session()
    finally:
        if session is not None:
            try:
                session.close()
            except Exception:  # noqa: BLE001
                pass
        driver.close()
    win_lo = t0 + warmup
    win_hi = win_lo + duration
    out_q.put(([lat for s0, lat in zip(starts, lats) if win_lo <= s0 < win_hi], errors))


def _thread_worker_timed(driver, op, seed_val, t0, warmup, duration, out):
    """Thread worker aligned to absolute `t0` (matches the process readers)."""
    rng = random.Random(seed_val)
    session = driver.session()
    try:
        try:
            op(session, rng)
        except Exception:  # noqa: BLE001
            pass
        while time.monotonic() < t0:
            time.sleep(0.0005)
        end = t0 + warmup + duration
        while time.monotonic() < end:
            s0 = time.monotonic()
            ok = False
            for attempt in range(MAX_RETRIES):
                try:
                    op(session, rng)
                    ok = True
                    break
                except TransientError:
                    time.sleep(0.001 * (attempt + 1))
                except Exception:  # noqa: BLE001
                    break
            s1 = time.monotonic()
            if ok:
                out.starts.append(s0)
                out.lats.append(s1 - s0)
            else:
                out.errors += 1
                try:
                    session.close()
                except Exception:  # noqa: BLE001
                    pass
                session = driver.session()
    finally:
        try:
            session.close()
        except Exception:  # noqa: BLE001
            pass


def run_workload_proc_readers(
    bolt_port, driver, readers, vertices, writers, writer_factory, duration, warmup, rng, launch_slack=2.0
):
    """Readers as separate processes (GIL-free); writers as threads in-process.

    Returns (read_collapsed, write_collapsed, window_seconds), each collapsed as
    (latencies, error_count) -- same shape as run_workload.
    """
    ctx = multiprocessing.get_context("spawn")  # fresh interpreter: no fork-with-threads hazard
    out_q = ctx.Queue()
    t0 = time.monotonic() + launch_slack
    procs = []
    for _ in range(readers):
        p = ctx.Process(
            target=_reader_proc, args=(bolt_port, vertices, rng.randrange(1 << 30), t0, warmup, duration, out_q)
        )
        p.start()
        procs.append(p)

    write_res = [WorkerResult() for _ in range(writers)]
    wthreads = []
    for k in range(writers):
        t = threading.Thread(
            target=_thread_worker_timed,
            args=(driver, writer_factory(k), rng.randrange(1 << 30), t0, warmup, duration, write_res[k]),
            daemon=True,
        )
        t.start()
        wthreads.append(t)

    # Drain the queue BEFORE joining so a child blocked on a full pipe can exit.
    reader_lats: list[float] = []
    reader_errors = 0
    for _ in range(readers):
        try:
            wl, er = out_q.get(timeout=launch_slack + warmup + duration + 60)
        except queue.Empty:
            wl, er = [], 0  # a reader that never reported (crash) contributes nothing
        reader_lats.extend(wl)
        reader_errors += er
    for p in procs:
        p.join(timeout=30)
    for t in wthreads:
        t.join(timeout=30)

    win_lo = t0 + warmup
    win_hi = win_lo + duration
    return (reader_lats, reader_errors), _collapse(write_res, win_lo, win_hi), duration


def percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return float("nan")
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    idx = p / 100.0 * (len(sorted_vals) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def summarize(collapsed, window_s: float) -> dict:
    lats, errors = collapsed
    tput = len(lats) / window_s if window_s > 0 else 0.0
    s = sorted(lats)
    return {
        "ops": len(lats),
        "throughput": tput,
        "p50_ms": percentile(s, 50) * 1000.0,
        "p99_ms": percentile(s, 99) * 1000.0,
        "errors": errors,
    }


# --------------------------------------------------------------------------- #
# Scenarios
# --------------------------------------------------------------------------- #
def _driver(bolt_port):
    return GraphDatabase.driver(f"bolt://127.0.0.1:{bolt_port}", auth=("", ""))


def scenario_good(cfg, extra_flags) -> dict | None:
    """MAIN + slow SYNC replica; measure reader throughput under slow commits.

    Returns metrics dict, or None if replication could not be set up (e.g. no
    enterprise license), signalling the caller to skip GOOD.
    """
    replica_args = [
        "--storage-wal-enabled=true",
        "--storage-wal-file-flush-every-n-tx=1",  # fsync every replicated commit
    ]
    main_args = [
        "--storage-wal-enabled=true",
        *extra_flags,
    ]
    with launched(cfg.binary, REPLICA_BOLT, "replica", replica_args, cfg.env, cfg.ready_timeout) as replica, launched(
        cfg.binary, MAIN_BOLT, "main", main_args, cfg.env, cfg.ready_timeout
    ) as main:
        # Configure replication exactly like tests/e2e/replication/workloads.yaml.
        rdrv = _driver(replica.bolt_port)
        try:
            with rdrv.session() as s:
                s.run(f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICA_REPL_PORT}").consume()
        finally:
            rdrv.close()

        mdrv = _driver(main.bolt_port)
        try:
            with mdrv.session() as s:
                try:
                    s.run(f"REGISTER REPLICA r1 SYNC TO '127.0.0.1:{REPLICA_REPL_PORT}'").consume()
                except Neo4jError as exc:
                    msg = str(exc).lower()
                    if "license" in msg or "enterprise" in msg:
                        return None
                    raise
            seed(mdrv, cfg.vertices)
            rng = random.Random(cfg.base_seed)
            if cfg.reader_mode == "process":
                reads, _writes, win = run_workload_proc_readers(
                    main.bolt_port,
                    mdrv,
                    cfg.readers,
                    cfg.vertices,
                    cfg.writers,
                    lambda k: churn_op_factory(cfg.write_batch, k),
                    cfg.duration,
                    cfg.warmup,
                    rng,
                )
            else:
                reads, _writes, win = run_workload(
                    mdrv,
                    cfg.readers,
                    lambda k: read_op_factory(cfg.vertices),
                    cfg.writers,
                    lambda k: churn_op_factory(cfg.write_batch, k),
                    cfg.duration,
                    cfg.warmup,
                    rng,
                )
        finally:
            mdrv.close()
    return summarize(reads, win)


def scenario_bad(cfg, extra_flags) -> dict:
    """Single MAIN, one writer, no read contention -- measure write throughput."""
    main_args = [
        "--storage-snapshot-interval-sec=0",
        *extra_flags,
    ]
    with launched(cfg.binary, MAIN_BOLT, "main", main_args, cfg.env, cfg.ready_timeout) as main:
        mdrv = _driver(main.bolt_port)
        try:
            seed(mdrv, cfg.vertices)
            rng = random.Random(cfg.base_seed)
            _reads, writes, win = run_workload(
                mdrv,
                0,
                lambda k: read_op_factory(cfg.vertices),
                1,
                lambda k: set_op_factory(cfg.vertices),
                cfg.duration,
                cfg.warmup,
                rng,
            )
        finally:
            mdrv.close()
    return summarize(writes, win)


def scenario_neutral(cfg, extra_flags) -> dict:
    """Single MAIN, read-only -- measure read throughput (expect ON ~= OFF)."""
    main_args = [
        "--storage-snapshot-interval-sec=0",
        *extra_flags,
    ]
    with launched(cfg.binary, MAIN_BOLT, "main", main_args, cfg.env, cfg.ready_timeout) as main:
        mdrv = _driver(main.bolt_port)
        try:
            seed(mdrv, cfg.vertices)
            rng = random.Random(cfg.base_seed)
            if cfg.reader_mode == "process":
                reads, _writes, win = run_workload_proc_readers(
                    main.bolt_port,
                    mdrv,
                    cfg.readers,
                    cfg.vertices,
                    0,
                    lambda k: set_op_factory(cfg.vertices),
                    cfg.duration,
                    cfg.warmup,
                    rng,
                )
            else:
                reads, _writes, win = run_workload(
                    mdrv,
                    cfg.readers,
                    lambda k: read_op_factory(cfg.vertices),
                    0,
                    lambda k: set_op_factory(cfg.vertices),
                    cfg.duration,
                    cfg.warmup,
                    rng,
                )
        finally:
            mdrv.close()
    return summarize(reads, win)


SCENARIOS = {
    "good": scenario_good,
    "bad": scenario_bad,
    "neutral": scenario_neutral,
}


# --------------------------------------------------------------------------- #
# A/B driver + reporting
# --------------------------------------------------------------------------- #
def median_runs(fn, cfg, extra_flags, reps):
    """Run a scenario `reps` times; median each metric field. None => skip."""
    runs = []
    for _ in range(reps):
        r = fn(cfg, extra_flags)
        if r is None:
            return None
        runs.append(r)
    keys = runs[0].keys()
    return {k: statistics.median(run[k] for run in runs) for k in keys}


def _delta_pct(off, on):
    if off == 0:
        return float("nan")
    return (on - off) / off * 100.0


def _verdict(scenario, metric, off, on):
    d = _delta_pct(off, on)
    if scenario == "good":
        if metric == "throughput":
            return "PASS (ON higher)" if d > 0 else "FAIL (ON not higher)"
        if metric == "p99_ms":
            return "better" if d < 0 else "worse"
    if scenario == "bad" and metric == "throughput":
        return "OK (within 10%)" if d >= -10.0 else "REGRESSION"
    if scenario == "neutral" and metric == "throughput":
        return "PASS (within 5%)" if abs(d) <= 5.0 else "CHECK (>5%)"
    return ""


PRIMARY_METRIC = {"good": "throughput", "bad": "throughput", "neutral": "throughput"}

# (label, metric-key, unit, show?) rows per scenario.
ROWS = {
    "good": [
        ("reader throughput", "throughput", "reads/s"),
        ("read p50", "p50_ms", "ms"),
        ("read p99", "p99_ms", "ms"),
    ],
    "bad": [("write throughput", "throughput", "commits/s")],
    "neutral": [
        ("read throughput", "throughput", "reads/s"),
        ("read p50", "p50_ms", "ms"),
        ("read p99", "p99_ms", "ms"),
    ],
}


def print_table(scenario, off, on):
    print(f"\n=== {scenario.upper()} ===")
    hdr = f"{'metric':<20} {'OFF':>14} {'ON':>14} {'delta%':>9}  verdict"
    print(hdr)
    print("-" * len(hdr))
    for label, key, unit in ROWS[scenario]:
        o, n = off[key], on[key]
        d = _delta_pct(o, n)
        v = _verdict(scenario, key, o, n)
        print(f"{label:<20} {o:>14.1f} {n:>14.1f} {d:>8.1f}%  {v}   [{unit}]")
    print(f"{'errors':<20} {off['errors']:>14} {on['errors']:>14}" f" {'':>9}  (query errors during window)")


def print_summary_line(scenario, off, on):
    key = PRIMARY_METRIC[scenario]
    o, n = off[key], on[key]
    d = _delta_pct(o, n)
    v = _verdict(scenario, key, o, n)
    unit = dict((k, u) for _, k, u in ROWS[scenario])[key]
    print(f"SUMMARY [{scenario}]: primary={key} OFF={o:.1f} ON={n:.1f} " f"({d:+.1f}%) {unit} -> {v}")


@dataclass
class Config:
    binary: str
    vertices: int
    readers: int
    writers: int
    duration: float
    warmup: float
    reps: int
    ready_timeout: float
    base_seed: int
    env: dict
    experimental_flag: str
    write_batch: int
    reader_mode: str


@dataclass
class ScenarioOutcome:
    """What a scenario did, for the CI gate. `ran` False + no exception => SKIP.
    `errors` sums worker errors across the OFF and ON runs. A raised exception
    (a CRASH) is caught by main(), which records it separately."""

    ran: bool = False
    skipped: bool = False
    errors: int = 0


def run_scenario_ab(name, cfg) -> ScenarioOutcome:
    fn = SCENARIOS[name]
    off_flags: list[str] = []
    on_flags = [f"--experimental-enabled={cfg.experimental_flag}"]

    print(f"\n>>> {name}: running OFF ...", flush=True)
    off = median_runs(fn, cfg, off_flags, cfg.reps)
    if off is None:
        print(
            f"[skip] {name}: could not set up replication -- the GOOD scenario "
            f"needs an enterprise license. Provide MEMGRAPH_ORGANIZATION_NAME + "
            f"MEMGRAPH_ENTERPRISE_LICENSE in the environment, or pass "
            f"--organization-name/--license-key. Skipping {name}."
        )
        return ScenarioOutcome(ran=False, skipped=True)

    print(f">>> {name}: running ON (--experimental-enabled={cfg.experimental_flag}) ...", flush=True)
    on = median_runs(fn, cfg, on_flags, cfg.reps)
    if on is None:
        print(f"[skip] {name}: replication setup failed with the flag ON. Skipping.")
        return ScenarioOutcome(ran=False, skipped=True)

    print_table(name, off, on)
    print_summary_line(name, off, on)
    # Operational health only: worker errors across both runs. The perf verdict
    # direction (ON slower than OFF) is expected on a single machine and MUST
    # NOT enter the exit code.
    return ScenarioOutcome(ran=True, skipped=False, errors=off["errors"] + on["errors"])


def _kill_stragglers():
    """Best-effort: nothing to do -- every instance is torn down in `finally`.

    Kept as a hook; MemgraphInstance.cleanup already terminates + kills.
    """


def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--memgraph-binary", default="./build/memgraph")
    p.add_argument("--scenario", choices=["good", "bad", "neutral", "all"], default="all")
    p.add_argument("--duration", type=float, default=15.0, help="measurement window seconds (default 15)")
    p.add_argument("--warmup", type=float, default=2.0, help="warmup seconds excluded from measurement (default 2)")
    p.add_argument("--readers", type=int, default=8)
    p.add_argument("--writers", type=int, default=2)
    p.add_argument("--vertices", type=int, default=5000)
    p.add_argument("--reps", type=int, default=1, help="repeat each A/B and take the median (default 1)")
    p.add_argument("--ready-timeout", type=float, default=30.0)
    p.add_argument("--seed", type=int, default=1234, help="fixed RNG seed for reproducibility")
    p.add_argument("--organization-name", default=None, help="enterprise org name (else $MEMGRAPH_ORGANIZATION_NAME)")
    p.add_argument("--license-key", default=None, help="enterprise license key (else $MEMGRAPH_ENTERPRISE_LICENSE)")
    p.add_argument(
        "--experimental-flag",
        default=EXPERIMENTAL_FLAG_DEFAULT,
        help="experimental value toggled ON (default lockfree-read-snapshot)",
    )
    p.add_argument(
        "--write-batch",
        type=int,
        default=2000,
        help="GOOD: vertices CREATEd per writer commit (default 2000). " "Bigger => longer engine_lock hold in OFF.",
    )
    p.add_argument(
        "--reader-mode",
        choices=["thread", "process"],
        default="process",
        help="readers as threads (GIL-bound) or separate processes "
        "(GIL-free, saturates the server). Default process. "
        "Applies to GOOD/NEUTRAL; BAD has no readers.",
    )
    p.add_argument(
        "--fail-on-error",
        action="store_true",
        help="exit non-zero on operational failure (crash or worker errors); "
        "perf-direction verdicts never affect exit code",
    )
    return p.parse_args(argv)


def build_env(args) -> dict:
    env = dict(os.environ)
    org = args.organization_name or os.environ.get("MEMGRAPH_ORGANIZATION_NAME")
    lic = args.license_key or os.environ.get("MEMGRAPH_ENTERPRISE_LICENSE")
    if org:
        env["MEMGRAPH_ORGANIZATION_NAME"] = org
    if lic:
        env["MEMGRAPH_ENTERPRISE_LICENSE"] = lic
    return env


def main(argv=None):
    args = parse_args(argv)
    binary = os.path.abspath(args.memgraph_binary)
    if not os.path.exists(binary):
        sys.exit(f"error: memgraph binary not found: {binary}")

    cfg = Config(
        binary=binary,
        vertices=args.vertices,
        readers=args.readers,
        writers=args.writers,
        duration=args.duration,
        warmup=args.warmup,
        reps=args.reps,
        ready_timeout=args.ready_timeout,
        base_seed=args.seed,
        env=build_env(args),
        experimental_flag=args.experimental_flag,
        write_batch=args.write_batch,
        reader_mode=args.reader_mode,
    )

    scenarios = ["good", "bad", "neutral"] if args.scenario == "all" else [args.scenario]
    have_license = bool(cfg.env.get("MEMGRAPH_ENTERPRISE_LICENSE"))
    print(f"binary: {binary}")
    print(
        f"config: vertices={cfg.vertices} readers={cfg.readers} writers={cfg.writers} "
        f"write-batch={cfg.write_batch} reader-mode={cfg.reader_mode} "
        f"duration={cfg.duration}s warmup={cfg.warmup}s "
        f"reps={cfg.reps} experimental-flag={cfg.experimental_flag}"
    )
    print(f"enterprise license present: {have_license}")

    ran = skipped = crashed = total_errors = 0
    interrupted = False
    try:
        for name in scenarios:
            try:
                outcome = run_scenario_ab(name, cfg)
                if outcome.skipped:
                    skipped += 1
                elif outcome.ran:
                    ran += 1
                    total_errors += outcome.errors
            except Exception as exc:  # noqa: BLE001 - one bad scenario must not sink the rest
                print(f"[error] scenario {name} failed: {exc}")
                crashed += 1
    except KeyboardInterrupt:
        print("\ninterrupted -- tearing down.")
        interrupted = True
    finally:
        _kill_stragglers()
    print("\ndone.")

    # Operational failure only: a crash, an interrupt, or any worker errors.
    # A gracefully-skipped GOOD (no license) and an ON-slower perf verdict are
    # NOT failures.
    failed = crashed > 0 or total_errors > 0 or interrupted
    reasons = []
    if crashed:
        reasons.append(f"{crashed} scenario(s) crashed")
    if total_errors:
        reasons.append(f"{total_errors} worker error(s)")
    if interrupted:
        reasons.append("interrupted")
    status = "FAIL" if failed else "PASS"
    detail = (" (" + "; ".join(reasons) + ")") if reasons else ""
    print(
        f"CI RESULT: {status}{detail} -- scenarios ran={ran} skipped={skipped} "
        f"crashed={crashed} total_errors={total_errors}"
    )

    if args.fail_on_error and failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
