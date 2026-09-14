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
Stress workload: concurrent DROP DATABASE ... FORCE under live-session pins.

Validates PR #4573 (non-blocking FORCE drop):

  Control assertion
    A dedicated thread continuously issues RETURN 1 against the default
    ``memgraph`` database.  Latency must stay below CONTROL_MAX_LATENCY_S
    throughout the entire worker storm.  The pre-fix behaviour froze the
    whole instance for the duration of the pin; the control heartbeat
    catches any regression.

  Non-blocking-DROP assertion
    Worker threads create ephemeral tenant databases, pin each one with a
    live Bolt session (USE DATABASE + a query, session kept open), and then
    issue DROP DATABASE <name> FORCE while the pin is still held.  The
    command must return within DROP_FORCE_MAX_LATENCY_S even though the
    accessor has not been released yet.

  Convergence assertion
    After all workers finish and their pinner sessions are closed, the
    workload polls SHOW DATABASES until no tenant_* rows remain, confirming
    that the deferred teardown worker eventually reclaims every husk.

Usage::

    python drop_force.py [--endpoint 127.0.0.1:7687]
                         [--username neo4j] [--password 1234]
                         [--worker-count 4] [--repetition-count 10]
                         [--logging INFO]
"""

from __future__ import annotations

import logging
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from common import OutputData, connection_argument_parser

try:
    from neo4j import GraphDatabase
    from neo4j.exceptions import ClientError, ServiceUnavailable, TransientError
except ImportError as exc:
    sys.exit(f"FATAL: neo4j Python driver not installed: {exc}")

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

# Generous upper bound on a control-path RETURN 1 query.  Pre-fix, the
# instance could freeze for seconds to minutes; 10 s catches any real freeze
# while being immune to normal transient network hiccups.
CONTROL_MAX_LATENCY_S: float = 10.0

# Generous upper bound on DROP DATABASE ... FORCE returning.  Non-blocking
# DROP must return before the pinner session is released.
DROP_FORCE_MAX_LATENCY_S: float = 10.0

# How long the pinner session is kept alive AFTER DROP FORCE returns.
# This confirms the drop truly did not wait for the accessor to release.
PIN_HOLD_AFTER_DROP_S: float = 0.15

# Convergence window: how long to wait for all tenant_* husks to drain
# from SHOW DATABASES after every pinner session has been closed.
CONVERGENCE_TIMEOUT_S: float = 60.0

# Poll cadence during the convergence wait.
CONVERGENCE_POLL_S: float = 1.0

# Interval between successive control-thread heartbeat queries.
CONTROL_POLL_S: float = 0.2

# Backoff between retries when a concurrent system query is rejected.
SYSTEM_QUERY_RETRY_S: float = 0.05

# Overall deadline for retrying a system query.  Prevents an infinite retry
# loop if the system-query lock is genuinely wedged; the test still fails fast.
SYSTEM_QUERY_RETRY_TIMEOUT_S: float = 30.0


# ---------------------------------------------------------------------------
# Driver / session helpers
# ---------------------------------------------------------------------------


def _make_driver(endpoint: str, username: str, password: str, ssl: bool = False):
    """Create a neo4j Bolt driver.

    Accepts the removal of the ``trust`` parameter in newer driver releases.
    """
    try:
        from neo4j import TRUST_ALL_CERTIFICATES

        return GraphDatabase.driver(
            f"bolt://{endpoint}",
            auth=(username, password),
            encrypted=ssl,
            trust=TRUST_ALL_CERTIFICATES,
        )
    except TypeError:
        # Newer driver versions removed the trust kwarg.
        return GraphDatabase.driver(
            f"bolt://{endpoint}",
            auth=(username, password),
            encrypted=ssl,
        )


def _run(session, query: str) -> list[dict]:
    """Execute *query*, consume the result, and return the data rows."""
    result = session.run(query)
    data = result.data()
    result.consume()
    return data


def _run_system_query(driver, query: str) -> float:
    """Execute a system query, retrying only on concurrent-system-query rejection.

    Memgraph serializes system queries (CREATE/DROP DATABASE) globally.  When
    two workers collide, one receives ClientError "Multiple concurrent system
    queries are not supported."  This helper treats that specific error as
    retriable and backs off for SYSTEM_QUERY_RETRY_S between attempts.

    Returns the wall-clock latency (seconds) of the single ACCEPTED attempt —
    backoff sleep time is excluded so callers can assert on response time.

    Any other ClientError or unexpected exception is re-raised immediately.
    Raises RuntimeError if SYSTEM_QUERY_RETRY_TIMEOUT_S elapses without the
    query being accepted.
    """
    deadline = time.monotonic() + SYSTEM_QUERY_RETRY_TIMEOUT_S
    while True:
        t0 = time.monotonic()
        try:
            with driver.session() as sess:
                _run(sess, query)
            return time.monotonic() - t0
        except ClientError as exc:
            if "multiple concurrent system queries" in str(exc).lower():
                if time.monotonic() >= deadline:
                    raise RuntimeError(
                        f"System query could not be accepted within "
                        f"{SYSTEM_QUERY_RETRY_TIMEOUT_S}s (concurrent-query lock held): {query}"
                    ) from exc
                log.debug(
                    "System query deferred (concurrent lock); retrying in %.2fs: %s",
                    SYSTEM_QUERY_RETRY_S,
                    query,
                )
                time.sleep(SYSTEM_QUERY_RETRY_S)
            else:
                raise


# ---------------------------------------------------------------------------
# Control thread
# ---------------------------------------------------------------------------


class _ControlThread:
    """Continuously pings the instance with RETURN 1 and records latency.

    ``last_heartbeat_ts`` is updated under a lock after every successful
    round-trip.  The main thread polls staleness via ``check_staleness()``
    so that a frozen ``session.run()`` — which would block the control
    thread indefinitely — is still detectable from outside.
    """

    def __init__(self, endpoint: str, username: str, password: str, ssl: bool) -> None:
        self._endpoint = endpoint
        self._username = username
        self._password = password
        self._ssl = ssl
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self.last_heartbeat_ts: float = time.monotonic()
        self.latencies: list[float] = []
        self.failures: list[str] = []
        self._thread = threading.Thread(target=self._loop, daemon=True, name="control")

    def start(self) -> None:
        self._thread.start()

    def stop(self, join_timeout: float = CONTROL_MAX_LATENCY_S * 2) -> None:
        self._stop.set()
        self._thread.join(timeout=join_timeout)

    def check_staleness(self) -> Optional[str]:
        """Return a failure string if the heartbeat is stale, else None."""
        with self._lock:
            age = time.monotonic() - self.last_heartbeat_ts
        if age > CONTROL_MAX_LATENCY_S:
            return f"Control-thread heartbeat stale for {age:.1f}s — instance may be frozen"
        return None

    def _loop(self) -> None:
        drv = _make_driver(self._endpoint, self._username, self._password, self._ssl)
        try:
            while not self._stop.is_set():
                t0 = time.monotonic()
                try:
                    with drv.session() as sess:
                        rows = _run(sess, "RETURN 1 AS alive")
                    if not rows or rows[0].get("alive") != 1:
                        self._fail("control query returned unexpected result")
                        continue
                    latency = time.monotonic() - t0
                    with self._lock:
                        self.latencies.append(latency)
                        self.last_heartbeat_ts = time.monotonic()
                    if latency > CONTROL_MAX_LATENCY_S:
                        self._fail(f"control query latency {latency:.3f}s > {CONTROL_MAX_LATENCY_S}s")
                except (ServiceUnavailable, TransientError, ClientError) as exc:
                    latency = time.monotonic() - t0
                    self._fail(f"control query error after {latency:.3f}s: {exc}")
                except Exception as exc:
                    latency = time.monotonic() - t0
                    self._fail(f"control query unexpected error after {latency:.3f}s: {exc}")
                time.sleep(CONTROL_POLL_S)
        finally:
            drv.close()

    def _fail(self, message: str) -> None:
        log.error("CONTROL: %s", message)
        with self._lock:
            self.failures.append(message)


# ---------------------------------------------------------------------------
# Worker: one iteration = CREATE / pin / DROP FORCE / release
# ---------------------------------------------------------------------------


def _worker_iteration(
    worker_id: int,
    rep: int,
    admin_drv,
    endpoint: str,
    username: str,
    password: str,
    ssl: bool,
) -> float:
    """Perform one CREATE / pin / DROP-FORCE / release cycle.

    Returns the wall-clock latency (seconds) of the DROP DATABASE … FORCE
    call.  Raises AssertionError if the latency exceeds the bound.
    """
    name = f"tenant_{worker_id}_{rep}"

    # Step 1: Create the tenant database.  _run_system_query retries the
    # concurrent-system-query rejection for us; "already exists" / "duplicate"
    # (left over from a previous interrupted run) are still tolerated here.
    try:
        _run_system_query(admin_drv, f"CREATE DATABASE {name}")
    except (ClientError, Exception) as exc:
        msg = str(exc).lower()
        if "already exists" in msg or "duplicate" in msg:
            log.warning("[worker-%d rep-%d] %s already exists, continuing", worker_id, rep, name)
        else:
            raise
    log.debug("[worker-%d rep-%d] created %s", worker_id, rep, name)

    # Step 2: Open a pinner session and keep it open (NOT inside a `with`
    # block) so the Bolt connection — and therefore the Memgraph DbAccessor
    # — remains live while we issue the DROP below.
    pin_drv = _make_driver(endpoint, username, password, ssl)
    pin_sess = pin_drv.session()
    drop_latency: float = 0.0
    try:
        _run(pin_sess, f"USE DATABASE {name}")
        _run(pin_sess, "RETURN 1 AS pinned")
        log.debug("[worker-%d rep-%d] accessor pinned on %s", worker_id, rep, name)

        # Step 3: Issue DROP FORCE while the pin is alive.  The command must
        # return promptly; blocking here would indicate a regression.
        # _run_system_query retries concurrent-system-query rejections and
        # returns only the latency of the single ACCEPTED attempt — backoff
        # sleep time is excluded so the assertion below stays meaningful.
        drop_latency = _run_system_query(admin_drv, f"DROP DATABASE {name} FORCE")
        log.debug(
            "[worker-%d rep-%d] DROP FORCE returned in %.3fs (pin still held)",
            worker_id,
            rep,
            drop_latency,
        )

        if drop_latency > DROP_FORCE_MAX_LATENCY_S:
            raise AssertionError(
                f"DROP DATABASE {name} FORCE took {drop_latency:.3f}s "
                f"> {DROP_FORCE_MAX_LATENCY_S}s; instance blocked on the pin"
            )

        # Step 4: Hold the pinner session briefly after the DROP returns to
        # confirm the drop truly did not wait for the accessor to release.
        time.sleep(PIN_HOLD_AFTER_DROP_S)

    finally:
        # Always release the pin regardless of outcome.
        try:
            pin_sess.close()
        except Exception:
            pass
        try:
            pin_drv.close()
        except Exception:
            pass

    return drop_latency


def _worker(
    worker_id: int,
    repetition_count: int,
    endpoint: str,
    username: str,
    password: str,
    ssl: bool,
) -> list[float]:
    """Run *repetition_count* CREATE/pin/DROP-FORCE cycles.

    Returns the per-iteration DROP FORCE latencies.
    """
    admin_drv = _make_driver(endpoint, username, password, ssl)
    latencies: list[float] = []
    try:
        for rep in range(repetition_count):
            log.info("[worker-%d] iteration %d / %d", worker_id, rep + 1, repetition_count)
            drop_lat = _worker_iteration(worker_id, rep, admin_drv, endpoint, username, password, ssl)
            latencies.append(drop_lat)
    finally:
        admin_drv.close()
    return latencies


# ---------------------------------------------------------------------------
# Convergence poll
# ---------------------------------------------------------------------------


def _wait_for_convergence(endpoint: str, username: str, password: str, ssl: bool) -> None:
    """Poll SHOW DATABASES until no tenant_* rows remain.

    Calls sys.exit() if husks do not drain within CONVERGENCE_TIMEOUT_S.
    """
    deadline = time.monotonic() + CONVERGENCE_TIMEOUT_S
    drv = _make_driver(endpoint, username, password, ssl)
    remaining: list[str] = []
    try:
        while time.monotonic() < deadline:
            with drv.session() as sess:
                rows = _run(sess, "SHOW DATABASES")
            # Memgraph returns a "name" column; guard against driver variants.
            remaining = [
                r.get("name", r.get("Database Name", ""))
                for r in rows
                if r.get("name", r.get("Database Name", "")).startswith("tenant_")
            ]
            if not remaining:
                log.info("Convergence: all tenant_* husks drained. OK")
                return
            log.debug("Convergence: %d husk(s) still present: %s", len(remaining), remaining)
            time.sleep(CONVERGENCE_POLL_S)
    finally:
        drv.close()

    sys.exit(f"FAIL: tenant_* husks still present after {CONVERGENCE_TIMEOUT_S}s: {remaining}")


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def _parse_args():
    """Build the CLI argument parser.

    Flags
    -----
    --endpoint          host:port of the Bolt listener   (default: 127.0.0.1:7687)
    --username                                           (default: neo4j)
    --password                                           (default: 1234)
    --use-ssl           Enable TLS on the Bolt connection
    --worker-count      Concurrent worker threads        (default: 4)
    --repetition-count  CREATE/pin/DROP-FORCE cycles per worker (default: 10)
    --logging           Log verbosity                    (default: INFO)
    """
    parser = connection_argument_parser()
    parser.add_argument(
        "--worker-count",
        type=int,
        default=4,
        help="Number of concurrent worker threads (default: 4)",
    )
    parser.add_argument(
        "--repetition-count",
        type=int,
        default=10,
        help="CREATE/pin/DROP-FORCE cycles per worker thread (default: 10)",
    )
    parser.add_argument(
        "--logging",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO)",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = _parse_args()
    logging.basicConfig(
        level=args.logging,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    output_data = OutputData()
    endpoint: str = args.endpoint
    username: str = args.username
    password: str = args.password
    ssl: bool = args.use_ssl

    log.info(
        "drop_force stress: worker_count=%d repetition_count=%d endpoint=%s",
        args.worker_count,
        args.repetition_count,
        endpoint,
    )

    # Phase 0: verify the server is reachable before spending threads.
    log.info("Phase 0: checking server liveness...")
    try:
        drv = _make_driver(endpoint, username, password, ssl)
        with drv.session() as sess:
            rows = _run(sess, "RETURN 1 AS alive")
            assert rows and rows[0].get("alive") == 1
        drv.close()
    except Exception as exc:
        sys.exit(f"FAIL: server at {endpoint} is not reachable: {exc}")
    log.info("Phase 0: server alive. OK")

    # Phase 1: start the control thread.
    log.info("Phase 1: starting control thread...")
    ctrl = _ControlThread(endpoint, username, password, ssl)
    ctrl.start()
    log.info("Phase 1: control thread running.")

    # Phase 2: run worker threads concurrently.
    log.info(
        "Phase 2: launching %d worker thread(s) (%d repetitions each)...",
        args.worker_count,
        args.repetition_count,
    )
    t_start = time.monotonic()
    all_drop_latencies: list[float] = []
    first_worker_error: Optional[str] = None

    with ThreadPoolExecutor(max_workers=args.worker_count, thread_name_prefix="df-worker") as pool:
        futures = {
            pool.submit(_worker, wid, args.repetition_count, endpoint, username, password, ssl): wid
            for wid in range(args.worker_count)
        }
        for future in as_completed(futures):
            wid = futures[future]
            # Check control-thread health while draining completed futures.
            stale = ctrl.check_staleness()
            if stale and first_worker_error is None:
                first_worker_error = stale
            try:
                latencies = future.result()
                all_drop_latencies.extend(latencies)
                log.info(
                    "[worker-%d] finished: %d drop(s), max_lat=%.3fs",
                    wid,
                    len(latencies),
                    max(latencies, default=0.0),
                )
            except AssertionError as exc:
                log.error("[worker-%d] ASSERTION FAILURE: %s", wid, exc)
                if first_worker_error is None:
                    first_worker_error = str(exc)
            except Exception as exc:
                log.error("[worker-%d] UNEXPECTED EXCEPTION: %s", wid, exc)
                if first_worker_error is None:
                    first_worker_error = f"worker-{wid} raised: {exc}"

    total_time = time.monotonic() - t_start
    log.info("Phase 2: all workers finished in %.2fs.", total_time)

    # Phase 3: stop the control thread and collect its findings.
    log.info("Phase 3: stopping control thread...")
    ctrl.stop()
    if ctrl.failures:
        for msg in ctrl.failures:
            log.error("Control failure: %s", msg)
        if first_worker_error is None:
            first_worker_error = f"control thread recorded {len(ctrl.failures)} failure(s): {ctrl.failures[0]}"

    # Final staleness check (after workers complete; thread is joined).
    stale = ctrl.check_staleness()
    if stale and first_worker_error is None:
        first_worker_error = stale
    log.info("Phase 3: control thread stopped.")

    # Phase 4: convergence — wait for every tenant_* husk to drain.
    log.info(
        "Phase 4: polling SHOW DATABASES for convergence (timeout=%ds)...",
        int(CONVERGENCE_TIMEOUT_S),
    )
    _wait_for_convergence(endpoint, username, password, ssl)  # sys.exit on timeout

    # Record summary metrics.
    output_data.add_measurement("total_time", round(total_time, 3))
    if all_drop_latencies:
        output_data.add_measurement("drop_force_max_latency_s", round(max(all_drop_latencies), 3))
        output_data.add_measurement(
            "drop_force_avg_latency_s",
            round(sum(all_drop_latencies) / len(all_drop_latencies), 3),
        )
    output_data.add_status("drop_force_ops", len(all_drop_latencies))
    output_data.add_status("worker_count", args.worker_count)
    output_data.add_status("repetition_count", args.repetition_count)
    output_data.add_status(
        "control_max_latency_s",
        round(max(ctrl.latencies, default=0.0), 3),
    )

    output_data.dump(print)

    if first_worker_error:
        sys.exit(f"FAIL: {first_worker_error}")

    print("PASS: all assertions satisfied.", flush=True)


if __name__ == "__main__":
    main()
