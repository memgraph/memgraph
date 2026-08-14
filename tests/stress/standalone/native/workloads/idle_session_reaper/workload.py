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
Idle-Session Reaper Concurrency Stress Workload.

Exercises the generic *idle-session accessor reaper* (server launched with
``--experimental-enabled=idle-session-reaper
--session-idle-accessor-release-sec=N``). When a Bolt session sits idle longer
than N seconds a background sweep releases that session's DB accessor WITHOUT
closing the socket. The next query on the session transparently re-acquires the
accessor; if the session's current DB was dropped/suspended meanwhile it falls
back to db-less and a db-requiring query then errors cleanly (NOT a crash, NOT a
hang). The reaper only reaps NON-default tenants.

Four concurrent worker groups race against the reaper for --duration-sec:

  1. idle_holders     — USE a stable keep_db (seeded with a known count, never
                        dropped), run one query, then sit IDLE. Every ~(2*N +
                        margin) seconds re-query and ASSERT the count is still
                        the seeded value. This proves TRANSPARENT RE-ACQUIRE
                        after the reaper released the accessor. keep_db is never
                        dropped/suspended, so ANY error here is a hard failure.
  2. query_churn      — pick a random churn tenant (create-on-miss), USE it, do a
                        few reads/writes, sometimes go idle > N then re-query.
                        Tolerate server-side errors from a concurrent
                        drop/suspend/recreate by rebinding to keep_db.
  3. tenant_lifecycle — CREATE / SUSPEND+RESUME / DROP DATABASE churn_x FORCE on
                        random churn tenants from a default-DB admin connection,
                        racing the churn/holders.
  4. reconnect_churn  — connect -> USE random tenant -> 1 query -> close, tight
                        loop (exercises connection setup/teardown vs. the sweep).

ERROR CLASSIFICATION is TYPE-BASED, not substring-based: for the
churn/lifecycle/reconnect workers a server-side query error (neo4j ``Neo4jError``
and its subclasses ``ClientError``/``DatabaseError``/``TransientError`` — the
server was alive and rejected the query because a tenant was concurrently
dropped/suspended/recreated, or the session fell back to db-less) is EXPECTED. A
connection-level failure (``ServiceUnavailable``/``SessionExpired``/broken
socket) or any crash is UNEXPECTED and fails the run.

REAPER-FIRED NON-VACUITY: after the stress groups stop, a dedicated prober_db
holder connects, USE prober_db, runs a single query, then stays idle with its
socket OPEN. After waiting > (N + sweep + margin) an admin connection issues
SUSPEND DATABASE prober_db, which MUST succeed — if the reaper had NOT released
the idle holder's accessor, prober_db would still be pinned and SUSPEND would
fail with an active-connections error. A failing SUSPEND here means the reaper
never fired and the whole test is vacuous, so we exit nonzero.

Run directly (smoke test):

  python3 workload.py --endpoint 127.0.0.1:7687 \\
      --parallelism 8 --num-tenants 6 --duration-sec 20 --idle-timeout-sec 2

The stress runner invokes it via the workload.yaml script_args.

NOTE: Designed to run against ASan / TSan builds.  The idle windows are the
feature under test (the reaper only fires on genuinely-idle sessions), so the
deliberate sleeps here are load-bearing, not flakiness; all waits are sized
generously above the reaper timeout so a sweep is guaranteed to fire even under
sanitizer slowdown.
"""

from __future__ import annotations

import argparse
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import NamedTuple

# ---------------------------------------------------------------------------
# Driver import: mirror hot_cold/workload.py — use the neo4j driver via the
# shared hot_cold_common helpers. Allow running from the source tree (the stress
# runner sets cwd/PYTHONPATH); climb to tests/stress/ where hot_cold_common.py
# lives (workloads/<name>/workload.py -> workloads -> native -> standalone ->
# stress, four levels up — identical depth to the hot_cold workloads).
# ---------------------------------------------------------------------------
_STRESS_ROOT = str(Path(__file__).resolve().parents[4])
if _STRESS_ROOT not in sys.path:
    sys.path.insert(0, _STRESS_ROOT)

from hot_cold_common import (  # noqa: E402  (import after sys.path bootstrap)
    assert_server_alive,
    build_base_arg_parser,
    count_nodes_on_tenant,
    create_tenants,
    make_driver,
    run_query,
    wait_for_server,
)

try:
    # Server-side vs. connection-level split drives the whole error classifier.
    # hot_cold_common only re-exports ClientError/TransientError, so pull the
    # base classes (Neo4jError, DriverError, SessionExpired) directly.
    from neo4j.exceptions import DriverError, Neo4jError, ServiceUnavailable, SessionExpired
except ImportError as exc:  # pragma: no cover - environment guard
    sys.exit(f"FATAL: neo4j Python driver not installed: {exc}")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Stable tenant that must survive the whole run intact (idle_holders re-check it).
KEEP_DB = "keep_db"
KEEP_COUNT = 500

# Dedicated tenant used ONLY by the reaper-fired non-vacuity assertion. Kept
# disjoint from the churn pool so nothing else ever pins/drops it.
PROBER_DB = "prober_db"

# Pause between reconnect/lifecycle loop iterations (avoid a tight spin).
LOOP_SLEEP = 0.05

# Upper bound (seconds) the prober assertion keeps re-trying SUSPEND after the
# deterministic idle wait. The wait alone should suffice; this only absorbs
# sanitizer/CI timing jitter. If SUSPEND never lands in this window the accessor
# was never released -> the reaper is broken (vacuous test).
PROBER_SUSPEND_POLL_SEC = 30.0


# ---------------------------------------------------------------------------
# Per-worker result record (aggregated by main after the pool drains)
# ---------------------------------------------------------------------------


class WorkerResult(NamedTuple):
    """Tally returned by every worker; main sums these across the pool."""

    ok: int  # successful query cycles
    expected: int  # server-side rejections tolerated as churn races
    unexpected: list[str]  # non-transient / connection-level errors (bugs)
    holder_failures: list[str]  # transparent-re-acquire failures on keep_db


# ---------------------------------------------------------------------------
# Error classification (TYPE-based — see module docstring)
# ---------------------------------------------------------------------------


def is_expected_server_error(exc: BaseException) -> bool:
    """
    Classify *exc* raised in the churn/lifecycle/reconnect workers, which
    DELIBERATELY race DROP/SUSPEND/CREATE against USE/query.

    EXPECTED (True): a server-side rejection — the server was alive and answered
    with an error because the target tenant was concurrently dropped, suspended,
    or recreated, or the session fell back to db-less. In the neo4j driver every
    such error is a ``Neo4jError`` (``ClientError``/``DatabaseError``/
    ``TransientError``), so we classify by TYPE rather than chasing individual
    message wordings.

    UNEXPECTED (False): anything that is NOT a clean server-side rejection — a
    broken/refused connection (``ServiceUnavailable``/``SessionExpired``, or any
    other ``DriverError``) or a crash. Those mean the server or the reaper
    actually misbehaved. Idle-holders on keep_db (never dropped) use a stricter
    check: ANY error there is a failure.
    """
    # Connection-level failures are fatal even though they are not Neo4jError
    # (explicit for intent; ServiceUnavailable/SessionExpired are DriverError).
    if isinstance(exc, (ServiceUnavailable, SessionExpired, DriverError)):
        return False
    return isinstance(exc, Neo4jError)


def _ensure_db(admin_sess, name: str) -> None:
    """Create *name* if missing (create-on-miss); swallow the already-exists race."""
    try:
        run_query(admin_sess, f"CREATE DATABASE {name}")
    except Neo4jError as exc:
        if "already exists" in str(exc).lower():
            return
        raise


# ---------------------------------------------------------------------------
# Worker groups (run in the ThreadPoolExecutor)
# ---------------------------------------------------------------------------


def idle_holder_worker(
    worker_id: int,
    endpoint: str,
    username: str,
    password: str,
    check_interval: float,
    stop: threading.Event,
) -> WorkerResult:
    """
    Parked/pooled connection: USE keep_db, run one query, then sit IDLE for the
    whole run on ONE long-lived session (the socket stays open so the reaper
    releases the accessor between checks). Every *check_interval* seconds
    (> reaper timeout) re-query and assert the count is still KEEP_COUNT — the
    transparent-re-acquire proof. keep_db is never dropped/suspended, so ANY
    error here is a holder failure.
    """
    holder_failures: list[str] = []
    drv = make_driver(endpoint, username, password)
    sess = drv.session()
    try:
        run_query(sess, f"USE DATABASE {KEEP_DB}")
        rows = run_query(sess, "MATCH (n) RETURN count(n) AS c")
        cnt = rows[0]["c"] if rows else None
        if cnt != KEEP_COUNT:
            holder_failures.append(f"holder{worker_id}: initial count {cnt} != {KEEP_COUNT}")

        last_check = time.monotonic()
        while not stop.is_set():
            # Stay genuinely idle between checks so the reaper releases us.
            stop.wait(0.5)
            if time.monotonic() - last_check < check_interval:
                continue
            last_check = time.monotonic()
            try:
                rows = run_query(sess, "MATCH (n) RETURN count(n) AS c")
                cnt = rows[0]["c"] if rows else None
                if cnt != KEEP_COUNT:
                    holder_failures.append(f"holder{worker_id}: re-acquire count {cnt} != {KEEP_COUNT}")
            except Exception as exc:  # noqa: BLE001 - keep_db never churns -> any error is the bug we hunt
                holder_failures.append(f"holder{worker_id}: transparent re-acquire FAILED: {exc!r}")
    except (ServiceUnavailable, SessionExpired) as exc:
        # Connection-level death on a stable tenant is always a failure.
        holder_failures.append(f"holder{worker_id}: fatal connection error: {exc!r}")
    finally:
        try:
            sess.close()
        except Exception:  # noqa: BLE001 - best-effort teardown
            pass
        drv.close()
    return WorkerResult(ok=0, expected=0, unexpected=[], holder_failures=holder_failures)


def query_churn_worker(
    worker_id: int,
    endpoint: str,
    username: str,
    password: str,
    churn_names: list[str],
    idle_timeout: float,
    stop: threading.Event,
    rng_seed: int,
) -> WorkerResult:
    """
    Loop: pick a random churn tenant (create-on-miss), USE it, do a few
    reads/writes, and sometimes go idle > N then query again (transparent
    re-acquire, OR a clean server-side error if the tenant was dropped/suspended
    meanwhile). Tolerate expected churn errors by rebinding to keep_db.
    """
    rng = random.Random(rng_seed)
    ok = 0
    expected = 0
    unexpected: list[str] = []
    reap_min = idle_timeout + 1.0
    reap_max = idle_timeout + 3.0

    drv = make_driver(endpoint, username, password)
    admin_drv = make_driver(endpoint, username, password)
    sess = drv.session()
    admin_sess = admin_drv.session()
    try:
        run_query(sess, f"USE DATABASE {KEEP_DB}")
        while not stop.is_set():
            name = rng.choice(churn_names)
            try:
                _ensure_db(admin_sess, name)
                run_query(sess, f"USE DATABASE {name}")
                for _ in range(rng.randint(1, 3)):
                    run_query(sess, "CREATE (:Churn)")
                run_query(sess, "MATCH (n) RETURN count(n) AS c")
                ok += 1

                if rng.random() < 0.15:
                    # Deliberately go idle past the reaper timeout, then query.
                    if stop.wait(rng.uniform(reap_min, reap_max)):
                        break
                    run_query(sess, "MATCH (n) RETURN count(n) AS c")
                    ok += 1
            except (ServiceUnavailable, SessionExpired):
                raise  # connection-level -> fatal, propagate to the collector
            except Neo4jError as exc:
                if is_expected_server_error(exc):
                    expected += 1
                    # Recover: rebind to the stable tenant and continue.
                    try:
                        run_query(sess, f"USE DATABASE {KEEP_DB}")
                    except (ServiceUnavailable, SessionExpired):
                        raise
                    except Neo4jError as exc2:
                        if not is_expected_server_error(exc2):
                            unexpected.append(f"churn{worker_id} recover: {exc2!r}")
                else:
                    unexpected.append(f"churn{worker_id}: {exc!r}")
            except Exception as exc:  # noqa: BLE001 - unknown type is not a clean rejection -> unexpected
                unexpected.append(f"churn{worker_id} unknown: {exc!r}")
    finally:
        for closeable in (sess, admin_sess):
            try:
                closeable.close()
            except Exception:  # noqa: BLE001 - best-effort teardown
                pass
        drv.close()
        admin_drv.close()
    return WorkerResult(ok=ok, expected=expected, unexpected=unexpected, holder_failures=[])


def tenant_lifecycle_worker(
    worker_id: int,
    endpoint: str,
    username: str,
    password: str,
    churn_names: list[str],
    stop: threading.Event,
    rng_seed: int,
) -> WorkerResult:
    """
    Loop CREATE DATABASE churn_x; optionally SUSPEND then RESUME; then
    DROP DATABASE churn_x FORCE — all from a default-DB admin connection while
    other workers use those tenants. SUSPEND may legitimately fail with
    ACTIVE_CONNECTIONS (a live session pins the tenant until the reaper releases
    it) — an expected server-side error, not a failure.
    """
    rng = random.Random(rng_seed)
    ok = 0
    expected = 0
    unexpected: list[str] = []

    admin_drv = make_driver(endpoint, username, password)
    admin_sess = admin_drv.session()
    try:
        while not stop.is_set():
            name = rng.choice(churn_names)
            roll = rng.random()
            try:
                _ensure_db(admin_sess, name)
                if stop.wait(rng.uniform(0.2, 1.0)):
                    break

                if roll < 0.4:
                    # Suspend/resume round-trip.
                    run_query(admin_sess, f"SUSPEND DATABASE {name}")
                    if stop.wait(rng.uniform(0.5, 2.0)):
                        break
                    run_query(admin_sess, f"RESUME DATABASE {name}")
                    ok += 1
                else:
                    run_query(admin_sess, f"DROP DATABASE {name} FORCE")
                    ok += 1
            except (ServiceUnavailable, SessionExpired):
                raise  # connection-level -> fatal, propagate to the collector
            except Neo4jError as exc:
                if is_expected_server_error(exc):
                    expected += 1
                else:
                    unexpected.append(f"lifecycle{worker_id}: {exc!r}")
            except Exception as exc:  # noqa: BLE001 - unknown type is not a clean rejection -> unexpected
                unexpected.append(f"lifecycle{worker_id} unknown: {exc!r}")
    finally:
        try:
            admin_sess.close()
        except Exception:  # noqa: BLE001 - best-effort teardown
            pass
        admin_drv.close()
    return WorkerResult(ok=ok, expected=expected, unexpected=unexpected, holder_failures=[])


def reconnect_churn_worker(
    worker_id: int,
    endpoint: str,
    username: str,
    password: str,
    churn_names: list[str],
    stop: threading.Event,
    rng_seed: int,
) -> WorkerResult:
    """Tight loop: connect -> USE random tenant -> 1 query -> close."""
    rng = random.Random(rng_seed)
    ok = 0
    expected = 0
    unexpected: list[str] = []
    targets = churn_names + [KEEP_DB]
    try:
        while not stop.is_set():
            drv = make_driver(endpoint, username, password)
            try:
                with drv.session() as sess:
                    name = rng.choice(targets)
                    run_query(sess, f"USE DATABASE {name}")
                    run_query(sess, "MATCH (n) RETURN count(n) AS c")
                ok += 1
            except (ServiceUnavailable, SessionExpired):
                raise  # connection-level -> fatal, propagate to the collector
            except Neo4jError as exc:
                if is_expected_server_error(exc):
                    expected += 1
                else:
                    unexpected.append(f"reconnect{worker_id}: {exc!r}")
            except Exception as exc:  # noqa: BLE001 - unknown type is not a clean rejection -> unexpected
                unexpected.append(f"reconnect{worker_id} unknown: {exc!r}")
            finally:
                drv.close()
            stop.wait(LOOP_SLEEP)
    finally:
        pass
    return WorkerResult(ok=ok, expected=expected, unexpected=unexpected, holder_failures=[])


# ---------------------------------------------------------------------------
# Setup / assertions
# ---------------------------------------------------------------------------


def seed_tenant(endpoint: str, username: str, password: str, name: str, count: int) -> None:
    """USE *name* and create exactly *count* labelled nodes; verify the count."""
    drv = make_driver(endpoint, username, password)
    try:
        with drv.session() as sess:
            run_query(sess, f"USE DATABASE {name}")
            run_query(sess, "MATCH (n) DETACH DELETE n")
            run_query(sess, f"UNWIND range(1, {count}) AS i CREATE (:Keep {{i: i}})")
            rows = run_query(sess, "MATCH (n) RETURN count(n) AS c")
            seeded = rows[0]["c"] if rows else None
        if seeded != count:
            sys.exit(f"FATAL: seed of {name} failed — has {seeded} nodes, expected {count}")
    finally:
        drv.close()


def prober_reaper_assertion(
    endpoint: str,
    username: str,
    password: str,
    idle_timeout: float,
) -> None:
    """
    Reaper-fired NON-VACUITY assertion.

    ONE connection connects, USE prober_db, runs a single query, then stays IDLE
    with its socket OPEN (the holder session/driver are kept alive across the
    wait so the pin is real). After waiting > (N + sweep + margin) a SEPARATE
    admin connection issues SUSPEND DATABASE prober_db, which MUST succeed: if the
    reaper had NOT released the idle holder's accessor, prober_db would still be
    pinned and SUSPEND would fail with an active-connections error. A SUSPEND that
    never lands means the reaper never fired -> the whole test is vacuous -> exit
    nonzero.
    """
    sweep = max(1.0, idle_timeout / 2.0)
    margin = max(5.0, 3.0 * idle_timeout)
    wait_sec = idle_timeout + sweep + margin

    hold_drv = make_driver(endpoint, username, password)
    hold_sess = hold_drv.session()
    try:
        # Acquire prober_db's accessor on the holder session, then go idle.
        run_query(hold_sess, f"USE DATABASE {PROBER_DB}")
        run_query(hold_sess, "MATCH (n) RETURN count(n) AS c")
        print(
            f"  prober: holder pinned {PROBER_DB}; staying idle for {wait_sec:.1f}s "
            f"(N={idle_timeout} + sweep={sweep} + margin={margin})...",
            flush=True,
        )
        # Keep hold_sess / hold_drv ALIVE and OPEN across this wait — do not run
        # any query on them — so the pin is genuine and the reaper must release it.
        time.sleep(wait_sec)

        admin_drv = make_driver(endpoint, username, password)
        suspended = False
        last_exc: Exception | None = None
        deadline = time.monotonic() + PROBER_SUSPEND_POLL_SEC
        try:
            while time.monotonic() < deadline:
                try:
                    with admin_drv.session() as asess:
                        run_query(asess, f"SUSPEND DATABASE {PROBER_DB}")
                    suspended = True
                    break
                except (ServiceUnavailable, SessionExpired):
                    raise  # connection-level failure — server likely crashed
                except Neo4jError as exc:
                    # Still pinned (active connections) or a transient system-tx
                    # collision — keep polling until the deadline.
                    last_exc = exc
                    time.sleep(0.25)
        finally:
            admin_drv.close()

        if not suspended:
            sys.exit(
                "FAIL: reaper did not release the idle pin (vacuous) — "
                f"SUSPEND DATABASE {PROBER_DB} never succeeded within "
                f"{PROBER_SUSPEND_POLL_SEC:.0f}s after a {wait_sec:.1f}s idle wait; "
                f"last error: {last_exc!r}"
            )
        print(f"  prober: SUSPEND {PROBER_DB} succeeded — reaper released the idle accessor: OK", flush=True)
    finally:
        try:
            hold_sess.close()
        except Exception:  # noqa: BLE001 - best-effort teardown
            pass
        hold_drv.close()


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = build_base_arg_parser(
        description=__doc__,
        default_parallelism=8,
        default_num_tenants=6,
        default_duration_sec=20,
    )
    parser.add_argument(
        "--idle-timeout-sec",
        type=float,
        default=2.0,
        help="Reaper idle timeout N (MUST match --session-idle-accessor-release-sec on the server)",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()

    endpoint: str = args.endpoint
    username: str = args.username
    password: str = args.password
    parallelism: int = max(1, args.parallelism)
    num_tenants: int = max(1, args.num_tenants)
    duration_sec: float = args.duration_sec
    idle_timeout: float = args.idle_timeout_sec

    churn_names = [f"churn_{i}" for i in range(num_tenants)]
    # Holders re-check on a cadence comfortably above the reaper timeout so the
    # accessor is definitely reaped-and-re-acquired between checks.
    holder_check_interval = 2.0 * idle_timeout + 4.0

    n_holders = parallelism
    n_churn = parallelism
    n_lifecycle = max(1, parallelism // 4)
    n_reconnect = max(1, parallelism // 2)

    print("==> Idle-session reaper concurrency stress", flush=True)
    print(f"    endpoint        : {endpoint}", flush=True)
    print(f"    idle timeout N  : {idle_timeout}s (server --session-idle-accessor-release-sec)", flush=True)
    print(f"    churn tenants   : {churn_names}", flush=True)
    print(f"    stable tenants  : {KEEP_DB} (seeded {KEEP_COUNT}), {PROBER_DB} (reaper prober)", flush=True)
    print(
        f"    worker groups   : {n_holders} idle_holders + {n_churn} query_churn + "
        f"{n_lifecycle} tenant_lifecycle + {n_reconnect} reconnect_churn",
        flush=True,
    )
    print(f"    duration        : {duration_sec}s", flush=True)

    # Phase 1: server readiness + stable-tenant setup. Churn tenants are created
    # on-miss by the workers, so the CREATE path is exercised from t=0.
    print("\n==> Phase 1: server readiness + tenant setup", flush=True)
    wait_for_server(endpoint, username, password)
    create_tenants(endpoint, username, password, [KEEP_DB, PROBER_DB])
    seed_tenant(endpoint, username, password, KEEP_DB, KEEP_COUNT)
    print(f"  seeded {KEEP_DB} with {KEEP_COUNT} nodes", flush=True)

    # Phase 2: concurrent stress run.
    print(f"\n==> Phase 2: concurrent stress for {duration_sec}s", flush=True)
    stop = threading.Event()
    base_seed = int(time.time())
    futures: list[tuple[str, int, object]] = []

    total_workers = n_holders + n_churn + n_lifecycle + n_reconnect
    with ThreadPoolExecutor(max_workers=total_workers) as pool:
        for i in range(n_holders):
            futures.append(
                (
                    "holder",
                    i,
                    pool.submit(idle_holder_worker, i, endpoint, username, password, holder_check_interval, stop),
                )
            )
        for i in range(n_churn):
            futures.append(
                (
                    "churn",
                    i,
                    pool.submit(
                        query_churn_worker,
                        i,
                        endpoint,
                        username,
                        password,
                        churn_names,
                        idle_timeout,
                        stop,
                        base_seed + 100 + i,
                    ),
                )
            )
        for i in range(n_lifecycle):
            futures.append(
                (
                    "lifecycle",
                    i,
                    pool.submit(
                        tenant_lifecycle_worker,
                        i,
                        endpoint,
                        username,
                        password,
                        churn_names,
                        stop,
                        base_seed + 200 + i,
                    ),
                )
            )
        for i in range(n_reconnect):
            futures.append(
                (
                    "reconnect",
                    i,
                    pool.submit(
                        reconnect_churn_worker,
                        i,
                        endpoint,
                        username,
                        password,
                        churn_names,
                        stop,
                        base_seed + 300 + i,
                    ),
                )
            )

        time.sleep(duration_sec)
        stop.set()
        print("  stop signal sent, waiting for workers...", flush=True)

        # Collect results; a propagated connection-level exception = real bug.
        total_ok = 0
        total_expected = 0
        all_unexpected: list[str] = []
        all_holder_failures: list[str] = []
        worker_exceptions: list[tuple[str, int, Exception]] = []
        for role, wid, f in futures:
            try:
                res: WorkerResult = f.result(timeout=120.0)
                total_ok += res.ok
                total_expected += res.expected
                all_unexpected.extend(res.unexpected)
                all_holder_failures.extend(res.holder_failures)
            except Exception as exc:  # noqa: BLE001 - surface any worker crash as a run failure
                worker_exceptions.append((role, wid, exc))

    if worker_exceptions:
        for role, wid, exc in worker_exceptions:
            print(f"  WORKER FAILURE [{role}-{wid}]: {exc!r}", file=sys.stderr, flush=True)
        sys.exit("FAIL: one or more workers raised a connection-level / non-transient exception")

    print(
        f"  workers done: ok={total_ok} expected_err={total_expected} "
        f"unexpected={len(all_unexpected)} holder_fail={len(all_holder_failures)}",
        flush=True,
    )

    # Phase 3: reaper-fired non-vacuity assertion (exits nonzero if reaper never fired).
    print("\n==> Phase 3: reaper-fired non-vacuity assertion", flush=True)
    prober_reaper_assertion(endpoint, username, password, idle_timeout)

    # Phase 4: keep_db integrity — the stable tenant must be untouched.
    print("\n==> Phase 4: keep_db integrity verification", flush=True)
    keep_actual = count_nodes_on_tenant(endpoint, username, password, KEEP_DB)
    keep_ok = keep_actual == KEEP_COUNT
    if keep_ok:
        print(f"  OK   {KEEP_DB}: {keep_actual} nodes", flush=True)
    else:
        print(f"  FAIL {KEEP_DB}: actual={keep_actual} expected={KEEP_COUNT}", flush=True)

    # Phase 5: server liveness check.
    print("\n==> Phase 5: server liveness check", flush=True)
    assert_server_alive(endpoint, username, password)

    # Phase 6: non-vacuity summary + final verdict.
    print("\n==> Phase 6: non-vacuity summary", flush=True)
    print("  reaps proven    : YES (prober SUSPEND succeeded — an idle pin was released)", flush=True)
    print(f"  ok ops          : {total_ok}", flush=True)
    print(f"  expected races  : {total_expected}", flush=True)
    print(f"  keep_db intact  : {keep_ok} ({keep_actual}/{KEEP_COUNT})", flush=True)

    failures: list[str] = []
    if all_holder_failures:
        failures.append(
            f"{len(all_holder_failures)} idle-holder re-acquire failure(s); first: {all_holder_failures[0]}"
        )
    if all_unexpected:
        distinct = sorted({str(e) for e in all_unexpected})
        print(f"  {len(all_unexpected)} UNEXPECTED error(s), {len(distinct)} distinct:", flush=True)
        for d in distinct[:20]:
            print(f"    - {d}", flush=True)
        failures.append(f"{len(all_unexpected)} unexpected worker error(s); first: {all_unexpected[0]}")
    if not keep_ok:
        failures.append(f"keep_db integrity: actual={keep_actual} expected={KEEP_COUNT}")

    print("\n" + "=" * 60, flush=True)
    if failures:
        print("RESULT: FAIL — idle-session reaper stress detected an error", flush=True)
        for reason in failures:
            print(f"  {reason}", flush=True)
        print("=" * 60, flush=True)
        sys.exit(1)
    print("RESULT: PASS — reaper proven live, keep_db intact, no unexpected errors", flush=True)
    print(f"  {total_ok} ok ops, {total_expected} expected races tolerated", flush=True)
    print("=" * 60, flush=True)


if __name__ == "__main__":
    main()
