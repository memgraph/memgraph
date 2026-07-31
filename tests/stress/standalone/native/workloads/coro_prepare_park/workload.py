#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright 2026 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

"""
Parkable-Prepare (--experimental-coro-prepare-accessor-yield) contention soak.

WHY THIS EXISTS, and it is not "more coverage for its own sake". The feature's two
characteristic failure modes are BOTH ACCUMULATION bugs, and both were real defects on the
branch that introduced it -- each found by adversarial review, neither by any test:

  1. A park that is published but never delivered. The coroutine frame stays suspended
     forever, its `shared_ptr<Session>` never dies, that Session holds a DatabaseAccess, and
     Gatekeeper<Database>::count_ never returns to zero.
  2. A resumed park left registered behind itself. `waiters_pending_` stays non-zero, which
     permanently defeats the "nobody is parked" fast path, and every later admitting
     transition then bumps the wake epoch -- a bumped epoch fails concurrent RegisterWaiter
     calls, so REAL parkers are sent back around the acquire loop with no backoff. One stale
     entry converts parkers into spinners.

Neither is visible in the functional e2e (3 contenders, ~6 seconds, one deterministic
scenario). One leaked entry per timed-out park is invisible at that scale and obvious at
100k. The e2e proves parking WORKS; this proves it does not ROT.

---------------------------------------------------------------------------------------------
CONTENTION DESIGN -- and a mistake worth not repeating

The first version of this file saturated the pool with EIGHT WRITER THREADS doing small
committed transactions, then asserted the probe stayed fast. It failed, at p50 ~0.96s, and the
failure was the test's fault, not the server's. Measured, not guessed:

    writers + READ_ONLY churn : p50 0.9620s
    writers + UNIQUE churn    : p50 1.9772s
    writers + both            : p50 1.0657s

Busy writers are not BLOCKED writers. A pool worker executing a real transaction is not a
worker being wasted, and parking neither can nor should help there -- that latency was
ordinary queueing behind productive work. Worse, the continuously-PENDING UNIQUE in that
design gates every new acquire of every kind (can_acquire<READ> demands
unique_pending_count == 0), so the probe was not waiting for a worker at all; it was
inadmissible. No amount of parking fixes inadmissible.

What parking actually recovers is workers pinned in `try_lock_for` waiting on an accessor
somebody else holds. So Phase A below builds exactly that: ONE long-held WRITE accessor, and
more READ_ONLY contenders than there are Bolt workers. Same shape, measured on this branch:

    parking ON  : 365 probes, p50 0.0016s
    parking OFF :  22 probes, p50 0.9500s

~600x, and that is the property worth asserting. Phase B then runs the harsher mixed churn
(including UNIQUE) purely to MANUFACTURE PARKS for the leak gate, with NO latency assertion --
because under a pending UNIQUE, slow is correct.

---------------------------------------------------------------------------------------------
WHAT IT ASSERTS

  A. LEAK GATE (the load-bearing one). After the contended window every workload driver is
     closed, then the run waits for `ActiveBoltSessions` to return to the pre-run baseline. A
     leaked park is exactly a Session that outlives its connection, and SessionHL holds a
     ScopedGauge on that metric for the session's whole lifetime -- so this is a direct probe
     of failure mode 1, needing no enterprise license and no multi-tenancy (unlike the
     DROP DATABASE formulation, which also works but only with a license).
  B. RESPONSIVENESS, Phase A, flag-on only. An unrelated `RETURN 1` must stay fast while every
     Bolt worker has a contender blocked on a held accessor.
  C. NO STRANDED WORK. The server answers after the run, and every client error matched a
     known marker -- an unrecognised error fails the run rather than being swallowed.
  D. NON-VACUITY. Contention must have actually happened: the run fails if the contenders
     completed nothing, if Phase B produced no accessor timeouts (i.e. nothing ever rode a
     deadline, so no park was manufactured), or if the probe never ran.

WHAT IT DOES **NOT** ASSERT -- do not read more into a green run than this

  * It does not prove a park ever occurred. There is no server-side signal for parked-ness:
    PrepareCoro defers SetupInterpreterTransaction, so a parked query is deliberately
    invisible to SHOW TRANSACTIONS. The on/off latency contrast is the only evidence, and it
    is statistical, not structural.
  * It does not localise a leak. A failing gate says "a Session outlived its connection", not
    which of the two registries kept it alive.
  * It does not exercise shutdown-with-parks-in-flight. The stress runner owns the process
    lifetime; that path is covered by the park_shutdown e2e instead.

Access types come from interpreter.cpp's TransactionRequirements visitor; only UNIQUE and
READ_ONLY can park. `CREATE INDEX` is READ_ONLY, `CREATE POINT INDEX` is UNIQUE, and an
explicit transaction doing a CREATE holds WRITE. Index DDL is idempotent in Memgraph -- CREATE
on an existing index and DROP on a missing one both return OK -- which is what lets the
"unrecognised error fails the run" rule be strict.

Run directly (smoke test):

  python3 workload.py --endpoint 127.0.0.1:7687 --responsive-sec 20 --churn-sec 40

The stress runner invokes it via workload.yaml's script_args. Designed to stay correct under
ASan/TSan slowdown: every wait is a bounded poll, never a fixed sleep sized to a guess.
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

try:
    from neo4j import GraphDatabase
except ImportError as exc:  # pragma: no cover - environment problem, not a test failure
    sys.exit(f"FATAL: neo4j Python driver not installed: {exc}")


DEFAULT_ENDPOINT = "127.0.0.1:7687"
DEFAULT_USERNAME = "neo4j"
DEFAULT_PASSWORD = "1234"

# Errors this workload EXPECTS under contention. Anything else fails the run. Kept short on
# purpose: index DDL is idempotent, so the only legitimate failures are an accessor that could
# not be acquired within --storage-access-timeout-sec, and a write/write conflict.
EXPECTED_MARKERS = (
    "cannot get shared access to the storage",
    "cannot get unique access to the storage",
    "cannot get read-only access to the storage",
    "cannot resolve conflicting transactions",
)

# How long the leak gate waits for ActiveBoltSessions to fall back to baseline. Generous
# because TCP teardown and the driver's pool close are asynchronous; a real leak never
# converges, so a high ceiling costs nothing on a passing run and does not weaken the gate.
LEAK_SETTLE_TIMEOUT_SEC = 60.0
LEAK_POLL_SEC = 0.5


class ErrorCollector:
    """Thread-safe collector for client errors that do NOT match a known marker."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._errors: list[str] = []

    def record(self, context: str, exc: Exception) -> None:
        msg = str(exc).lower()
        if not any(marker in msg for marker in EXPECTED_MARKERS):
            with self._lock:
                self._errors.append(f"[{context}] {type(exc).__name__}: {exc}")

    def errors(self) -> list[str]:
        with self._lock:
            return list(self._errors)


class Counters:
    """Thread-safe tallies shared by the workers."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.accessor_timeouts = 0
        self.latencies: list[float] = []

    def add_timeout(self) -> None:
        with self._lock:
            self.accessor_timeouts += 1

    def add_latency(self, seconds: float) -> None:
        with self._lock:
            self.latencies.append(seconds)

    def snapshot_latencies(self) -> list[float]:
        with self._lock:
            return list(self.latencies)

    def reset_latencies(self) -> None:
        with self._lock:
            self.latencies = []


def make_driver(endpoint: str, username: str, password: str):
    return GraphDatabase.driver(f"bolt://{endpoint}", auth=(username, password), encrypted=False)


def run_query(session, query: str, **params) -> list[dict]:
    result = session.run(query, **params)
    data = result.data()
    result.consume()
    return data


def classify(exc: Exception, context: str, errors: ErrorCollector, counters: Counters) -> None:
    if "access to the storage" in str(exc).lower():
        counters.add_timeout()
    errors.record(context, exc)


def wait_for_server(endpoint: str, username: str, password: str, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            drv = make_driver(endpoint, username, password)
            with drv.session() as sess:
                run_query(sess, "RETURN 1 AS ok")
            drv.close()
            return
        except Exception:
            time.sleep(0.2)
    sys.exit(f"FATAL: server at {endpoint} did not become ready within {timeout}s")


def read_active_bolt_sessions(session) -> int:
    """
    Current value of the ActiveBoltSessions gauge, via SHOW METRICS INFO.

    Rows are (name, type, metric_type, value). The gauge is held by SessionHL for the whole
    lifetime of a Bolt session, which is exactly why it detects a park that outlived its
    connection.
    """
    for row in run_query(session, "SHOW METRICS INFO"):
        values = list(row.values())
        if values and values[0] == "ActiveBoltSessions":
            return int(values[-1])
    sys.exit("FATAL: SHOW METRICS INFO did not report ActiveBoltSessions -- the leak gate cannot run")


# ---------------------------------------------------------------------------
# Phase A workers -- the discriminating shape: one long-held accessor, more
# blocked contenders than there are Bolt workers.
# ---------------------------------------------------------------------------


def holder_worker(
    endpoint: str,
    username: str,
    password: str,
    hold_sec: float,
    stop_flag: list[bool],
    errors: ErrorCollector,
    counters: Counters,
) -> int:
    """
    Holds a WRITE accessor open for `hold_sec` at a time. WRITE blocks READ_ONLY
    (can_acquire<READ_ONLY> demands w_count == 0), which is what makes the contenders park
    rather than sail through. Released and retaken in a loop so the run also exercises the
    release/wake path repeatedly, not just once.
    """
    holds = 0
    driver = make_driver(endpoint, username, password)
    try:
        while not stop_flag[0]:
            try:
                with driver.session() as sess:
                    tx = sess.begin_transaction()
                    tx.run("CREATE (:ParkHolder {p: 1})").consume()
                    # Bounded poll rather than a flat sleep so a stop request is honoured
                    # promptly even with a long hold configured.
                    hold_deadline = time.monotonic() + hold_sec
                    while time.monotonic() < hold_deadline and not stop_flag[0]:
                        time.sleep(0.05)
                    tx.commit()
                holds += 1
            except Exception as exc:  # noqa: BLE001 - classified below
                classify(exc, "holder", errors, counters)
    finally:
        driver.close()
    return holds


def contender_worker(
    cid: int,
    endpoint: str,
    username: str,
    password: str,
    stop_flag: list[bool],
    errors: ErrorCollector,
    counters: Counters,
) -> int:
    """
    Wants READ_ONLY while the holder has WRITE, so it blocks -- and under the flag it PARKS,
    releasing its Bolt worker. These are the workers the probe below depends on being free.
    Its own label so contenders never conflict with each other, only with the holder.
    """
    ops = 0
    driver = make_driver(endpoint, username, password)
    try:
        while not stop_flag[0]:
            for query in (f"CREATE INDEX ON :C{cid}(p)", f"DROP INDEX ON :C{cid}(p)"):
                if stop_flag[0]:
                    break
                try:
                    with driver.session() as sess:
                        run_query(sess, query)
                    ops += 1
                except Exception as exc:  # noqa: BLE001
                    classify(exc, f"contender-{cid}", errors, counters)
    finally:
        driver.close()
    return ops


def probe_worker(
    endpoint: str,
    username: str,
    password: str,
    stop_flag: list[bool],
    errors: ErrorCollector,
    counters: Counters,
) -> int:
    """
    The responsiveness probe. `RETURN 1` is unrelated to the contended label, so its latency
    measures whether a pool worker was AVAILABLE -- the property parking provides. Under the
    blocking path the workers sit in try_lock_for and this goes slow.

    Its own connection deliberately: a probe sharing a contending session would serialise
    behind that session's in-flight query and measure the wrong thing.
    """
    ran = 0
    driver = make_driver(endpoint, username, password)
    try:
        while not stop_flag[0]:
            started = time.monotonic()
            try:
                with driver.session() as sess:
                    run_query(sess, "RETURN 1 AS ok")
                counters.add_latency(time.monotonic() - started)
                ran += 1
            except Exception as exc:  # noqa: BLE001
                classify(exc, "probe", errors, counters)
            time.sleep(0.05)
    finally:
        driver.close()
    return ran


# ---------------------------------------------------------------------------
# Phase B workers -- harsher mixed churn, run to MANUFACTURE PARKS for the leak
# gate. No latency claim is made about this phase.
# ---------------------------------------------------------------------------


def writer_worker(
    wid: int,
    endpoint: str,
    username: str,
    password: str,
    num_labels: int,
    nodes_per_tx: int,
    stop_flag: list[bool],
    errors: ErrorCollector,
    counters: Counters,
) -> int:
    committed = 0
    driver = make_driver(endpoint, username, password)
    try:
        while not stop_flag[0]:
            label = f"L{random.randrange(num_labels)}"
            try:
                with driver.session() as sess:
                    tx = sess.begin_transaction()
                    for i in range(nodes_per_tx):
                        tx.run(f"CREATE (:{label} {{p: $v}})", v=committed * nodes_per_tx + i)
                    tx.commit()
                committed += 1
            except Exception as exc:  # noqa: BLE001
                classify(exc, f"writer-{wid}", errors, counters)
    finally:
        driver.close()
    return committed


def ddl_churn_worker(
    endpoint: str,
    username: str,
    password: str,
    num_labels: int,
    unique_mode: bool,
    stop_flag: list[bool],
    errors: ErrorCollector,
    counters: Counters,
) -> int:
    """
    READ_ONLY churn (`CREATE INDEX`) or UNIQUE churn (`CREATE POINT INDEX`). UNIQUE needs
    state == UNLOCKED so it parks behind every other holder, and while merely pending it gates
    every new acquire of every kind -- the harshest shape the lock has, and the reason Phase B
    makes no latency claim.
    """
    ops = 0
    driver = make_driver(endpoint, username, password)
    kind = "unique-ddl" if unique_mode else "ro-ddl"
    try:
        while not stop_flag[0]:
            label = f"L{random.randrange(num_labels)}"
            if unique_mode:
                queries = (f"CREATE POINT INDEX ON :{label}(pt)", f"DROP POINT INDEX ON :{label}(pt)")
            else:
                queries = (f"CREATE INDEX ON :{label}(p)", f"DROP INDEX ON :{label}(p)")
            for query in queries:
                if stop_flag[0]:
                    break
                try:
                    with driver.session() as sess:
                        run_query(sess, query)
                    ops += 1
                except Exception as exc:  # noqa: BLE001
                    classify(exc, kind, errors, counters)
    finally:
        driver.close()
    return ops


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, int(round((pct / 100.0) * (len(ordered) - 1))))
    return ordered[idx]


def drain(futures: list[tuple[str, object]], phase: str) -> dict[str, int]:
    tallies: dict[str, int] = {}
    failures: list[tuple[str, Exception]] = []
    for role, fut in futures:
        try:
            tallies[role] = tallies.get(role, 0) + int(fut.result(timeout=180.0) or 0)
        except Exception as exc:  # noqa: BLE001
            failures.append((role, exc))
    if failures:
        for role, exc in failures:
            print(f"  WORKER FAILURE [{phase}/{role}]: {exc}", file=sys.stderr, flush=True)
        sys.exit(f"FAIL: one or more {phase} workers raised an unexpected exception")
    return tallies


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--endpoint", default=os.environ.get("ENDPOINT", DEFAULT_ENDPOINT))
    parser.add_argument("--username", default=DEFAULT_USERNAME)
    parser.add_argument("--password", default=DEFAULT_PASSWORD)
    parser.add_argument(
        "--contenders",
        type=int,
        default=6,
        help=(
            "Phase A: threads blocked on the held accessor. MUST exceed --bolt-num-workers or the "
            "probe finds a free worker regardless of parking and the assertion stops discriminating."
        ),
    )
    parser.add_argument("--hold-sec", type=float, default=3.0, help="Phase A: how long each WRITE hold lasts")
    parser.add_argument("--responsive-sec", type=float, default=20.0, help="Phase A duration")
    parser.add_argument("--churn-sec", type=float, default=40.0, help="Phase B duration (park manufacturing)")
    parser.add_argument("--parallelism", type=int, default=8, help="Phase B writer threads")
    parser.add_argument("--num-labels", type=int, default=6)
    parser.add_argument("--nodes-per-tx", type=int, default=10)
    parser.add_argument(
        "--probe-ceiling-ms",
        type=float,
        default=250.0,
        help=(
            "Phase A, flag-on only: probe p99 ceiling. Measured p50 on this branch is ~1.6ms parked "
            "vs ~950ms blocking, so this sits far from both and is not a tuning knob."
        ),
    )
    parser.add_argument(
        "--expect-parking",
        choices=("true", "false"),
        default="true",
        help=(
            "true: assert Phase A responsiveness. false: the control arm -- run the identical "
            "contention with the flag off and assert ONLY the leak/liveness gates, since a blocking "
            "build is expected to be slow. The leak gate applies to BOTH arms on purpose: the pool "
            "changes this feature made (work_must_run_, the teardown tail) are NOT flag-gated."
        ),
    )
    # Stress-runner boilerplate, accepted and ignored.
    parser.add_argument("--worker-count", type=int, default=0)
    parser.add_argument("--logging", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    expect_parking = args.expect_parking == "true"

    print(
        f"[coro-prepare-park] endpoint={args.endpoint} contenders={args.contenders} hold={args.hold_sec}s "
        f"phaseA={args.responsive_sec}s phaseB={args.churn_sec}s writers={args.parallelism} "
        f"expect_parking={expect_parking}",
        flush=True,
    )

    print("Phase 1: waiting for the server...", flush=True)
    wait_for_server(args.endpoint, args.username, args.password)

    # Baseline BEFORE any worker connects, read through the observer's own connection so that
    # session is included in the number the leak gate later compares against.
    observer = make_driver(args.endpoint, args.username, args.password)
    with observer.session() as sess:
        baseline_sessions = read_active_bolt_sessions(sess)
    print(f"  baseline ActiveBoltSessions (observer included) = {baseline_sessions}", flush=True)

    errors = ErrorCollector()
    counters = Counters()

    # ---- Phase A: responsiveness under blocked contenders. ----
    print(
        f"Phase 2A ({args.responsive_sec}s): 1 WRITE holder + {args.contenders} blocked READ_ONLY "
        f"contenders + probe...",
        flush=True,
    )
    stop_a = [False]
    with ThreadPoolExecutor(max_workers=args.contenders + 2) as pool:
        futures_a: list[tuple[str, object]] = [
            (
                "holder",
                pool.submit(
                    holder_worker,
                    args.endpoint,
                    args.username,
                    args.password,
                    args.hold_sec,
                    stop_a,
                    errors,
                    counters,
                ),
            ),
            (
                "probe",
                pool.submit(probe_worker, args.endpoint, args.username, args.password, stop_a, errors, counters),
            ),
        ]
        for cid in range(args.contenders):
            futures_a.append(
                (
                    "contender",
                    pool.submit(
                        contender_worker,
                        cid,
                        args.endpoint,
                        args.username,
                        args.password,
                        stop_a,
                        errors,
                        counters,
                    ),
                )
            )
        time.sleep(args.responsive_sec)
        stop_a[0] = True
        print("  Phase 2A stop signal sent, draining...", flush=True)
        tallies_a = drain(futures_a, "phaseA")

    phase_a_latencies = counters.snapshot_latencies()
    counters.reset_latencies()
    print(
        f"  holds={tallies_a.get('holder', 0)} contender_ops={tallies_a.get('contender', 0)} "
        f"probes={tallies_a.get('probe', 0)}",
        flush=True,
    )

    # ---- Phase B: manufacture parks for the leak gate. ----
    print(
        f"Phase 2B ({args.churn_sec}s): {args.parallelism} writers + READ_ONLY churn + UNIQUE churn "
        f"(no latency claim -- a pending UNIQUE legitimately gates everything)...",
        flush=True,
    )
    stop_b = [False]
    with ThreadPoolExecutor(max_workers=args.parallelism + 2) as pool:
        futures_b: list[tuple[str, object]] = []
        for wid in range(args.parallelism):
            futures_b.append(
                (
                    "writer",
                    pool.submit(
                        writer_worker,
                        wid,
                        args.endpoint,
                        args.username,
                        args.password,
                        args.num_labels,
                        args.nodes_per_tx,
                        stop_b,
                        errors,
                        counters,
                    ),
                )
            )
        for unique_mode, role in ((False, "ro_ddl"), (True, "unique_ddl")):
            futures_b.append(
                (
                    role,
                    pool.submit(
                        ddl_churn_worker,
                        args.endpoint,
                        args.username,
                        args.password,
                        args.num_labels,
                        unique_mode,
                        stop_b,
                        errors,
                        counters,
                    ),
                )
            )
        time.sleep(args.churn_sec)
        stop_b[0] = True
        print("  Phase 2B stop signal sent, draining...", flush=True)
        tallies_b = drain(futures_b, "phaseB")

    print(
        f"  committed={tallies_b.get('writer', 0)} ro_ddl_ops={tallies_b.get('ro_ddl', 0)} "
        f"unique_ddl_ops={tallies_b.get('unique_ddl', 0)} accessor_timeouts={counters.accessor_timeouts}",
        flush=True,
    )

    # ---- Phase 3: non-vacuity. A green run must have actually contended. ----
    print("Phase 3: non-vacuity checks...", flush=True)
    if tallies_a.get("contender", 0) == 0:
        sys.exit(
            "FAIL (vacuous): no Phase A contender completed a single READ_ONLY operation, so nothing "
            "ever blocked on the held accessor and the responsiveness measurement means nothing."
        )
    if tallies_a.get("holder", 0) == 0:
        sys.exit(
            "FAIL (vacuous): the Phase A holder never completed a WRITE hold, so the contenders had "
            "nothing to block on."
        )
    if expect_parking and tallies_a.get("probe", 0) == 0:
        sys.exit("FAIL (vacuous): the responsiveness probe never completed a query, so its latency is unmeasured.")
    if tallies_b.get("ro_ddl", 0) == 0 and tallies_b.get("unique_ddl", 0) == 0:
        sys.exit(
            "FAIL (vacuous): neither Phase B DDL thread completed an operation, so no READ_ONLY or "
            "UNIQUE accessor was ever taken and no park was manufactured for the leak gate."
        )
    if counters.accessor_timeouts == 0:
        sys.exit(
            "FAIL (vacuous): not a single accessor timeout was observed across either phase. Under the "
            "shipped 1s --storage-access-timeout-sec these phases are expected to produce them in "
            "quantity, and they are the evidence that acquires genuinely rode their deadline -- i.e. "
            "that parks were created and resolved by the deadline sweep, the path that used to leak a "
            "waiter on every occurrence. Without them the leak gate has nothing to detect."
        )
    print(
        f"  contention was real: contender_ops={tallies_a.get('contender', 0)}, "
        f"accessor_timeouts={counters.accessor_timeouts}",
        flush=True,
    )

    # ---- Phase 4: the leak gate. ----
    print("Phase 4: leak gate -- waiting for ActiveBoltSessions to return to baseline...", flush=True)
    deadline = time.monotonic() + LEAK_SETTLE_TIMEOUT_SEC
    observed = None
    while time.monotonic() < deadline:
        with observer.session() as sess:
            observed = read_active_bolt_sessions(sess)
        if observed <= baseline_sessions:
            break
        time.sleep(LEAK_POLL_SEC)

    if observed is None or observed > baseline_sessions:
        observer.close()
        sys.exit(
            f"FAIL (session leak): ActiveBoltSessions settled at {observed}, above the pre-run baseline "
            f"of {baseline_sessions}, {LEAK_SETTLE_TIMEOUT_SEC}s after every workload driver was closed. "
            f"A Bolt session outliving its connection is the signature of a park that was published but "
            f"never delivered: the suspended coroutine frame still holds its shared_ptr<Session>, that "
            f"Session holds a DatabaseAccess, and Gatekeeper<Database>::count_ will not reach zero -- "
            f"which stalls DROP DATABASE and, at shutdown, ~Gatekeeper."
        )
    print(f"  ActiveBoltSessions returned to {observed} (baseline {baseline_sessions}): OK", flush=True)

    # ---- Phase 5: responsiveness verdict (Phase A data, flag-on only). ----
    p50 = percentile(phase_a_latencies, 50.0)
    p99 = percentile(phase_a_latencies, 99.0)
    worst = max(phase_a_latencies) if phase_a_latencies else 0.0
    print(
        f"Phase 5: Phase A probe latency p50={p50:.4f}s p99={p99:.4f}s max={worst:.4f}s "
        f"over {len(phase_a_latencies)} samples",
        flush=True,
    )
    if expect_parking:
        ceiling = args.probe_ceiling_ms / 1000.0
        if p99 > ceiling:
            observer.close()
            sys.exit(
                f"FAIL (responsiveness): the RETURN 1 probe's p99 was {p99:.3f}s, above the "
                f"{ceiling:.3f}s ceiling, while {args.contenders} contenders were blocked on a held "
                f"WRITE accessor and should have been PARKED with their Bolt workers freed. Reference "
                f"numbers for this exact shape: ~1.6ms parked, ~950ms blocking. A result near the "
                f"blocking figure means parking stopped engaging (is the flag on for this arm?); an "
                f"intermediate result means resumes are being delayed behind unrelated work."
            )
        print("  probe stayed responsive while every worker had a blocked contender: OK", flush=True)
    else:
        print(
            "  responsiveness NOT asserted (control arm -- the blocking path is expected to be slow; "
            "this figure is for contrast with the flag-on arm only)",
            flush=True,
        )

    # ---- Phase 6: liveness + error classification. ----
    print("Phase 6: liveness and error classification...", flush=True)
    try:
        with observer.session() as sess:
            rows = run_query(sess, "RETURN 1 AS alive")
        assert rows and list(rows[0].values())[0] == 1
    except Exception as exc:  # noqa: BLE001
        observer.close()
        sys.exit(f"FAIL: server liveness check failed -- possible crash: {exc}")
    print("  server answered after the run: OK", flush=True)

    unexpected = errors.errors()
    observer.close()
    if unexpected:
        print("  UNEXPECTED ERRORS detected:", file=sys.stderr, flush=True)
        for err in unexpected[:40]:
            print(f"    {err}", file=sys.stderr, flush=True)
        sys.exit(
            f"FAIL: {len(unexpected)} unexpected error(s) during the run. Only accessor timeouts and "
            f"write conflicts are expected -- index DDL is idempotent, so anything else is a real defect "
            f"or a new error path that needs classifying."
        )
    print("  all client errors matched a known marker: OK", flush=True)

    print("[coro-prepare-park] PASSED", flush=True)


if __name__ == "__main__":
    main()
