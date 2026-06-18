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
Hot/Cold Trigger Durability Stress Workload.

Exercises Memgraph's TRIGGERS feature under hot/cold tenant suspend/resume
churn.  Triggers are durable (stored in the per-DB KVStore under triggers/)
and are restored on resume via the hot/cold on_resume_ / RestoreTriggers path.

The workload proves three properties simultaneously:

  1. TRIGGER DURABILITY under churn: a BEFORE COMMIT trigger created on a
     tenant survives SUSPEND → COLD → RESUME and is correctly restored each
     time the tenant is brought HOT.

  2. TRIGGER FIRES AFTER RESUME: the restored trigger actually fires — not just
     a stale catalog entry.  Every :Data node written after a resume MUST have
     .stamped != null.

  3. GLOBAL INTEGRITY: across the full churn window, every :Data node committed
     to any tenant (including commits interleaved with suspend/resume cycles)
     MUST have .stamped set.  A single unstamped node is a failure.

Four concurrent thread roles during the churn phase:

  1. DATA-WRITER threads (one per writer_id, round-robins tenants): commit
     batches of (:Data{seq:..., worker_id:...}) nodes.  The BEFORE COMMIT
     trigger fires synchronously in the committing transaction and sets
     n.stamped = timestamp() on each created vertex.  Committed batch count is
     tracked per tenant for the final node-count cross-check.

  2. SUSPENDER thread: repeatedly picks a random tenant and issues SUSPEND
     DATABASE.  All expected-under-contention errors are silently tolerated.
     Returns suspends_ok for non-vacuity gating.

  3. RESUMER thread: repeatedly picks a random tenant and issues RESUME
     DATABASE.  Returns resumes_ok (informational).

  4. READER thread: issues USE DATABASE + MATCH (n) RETURN count(n) on random
     tenants to exercise the read-path resume.  No count assertion during the
     run (counts change concurrently with writers).

After the timed churn phase, the workload runs a final one-at-a-time
verification pass (all tenants COLD first, then each is resumed, checked, and
re-suspended before moving to the next) to ensure that:

  - stamp_trigger still appears in SHOW TRIGGERS (trigger-persistence teeth).
  - A fresh probe node gains .stamped IS NOT NULL (trigger-fires-after-resume teeth).
  - MATCH (n:Data) RETURN count(n) == committed_batches * nodes_per_tx + 1
    (the +1 is the probe node).
  - MATCH (n:Data) WHERE n.stamped IS NULL RETURN count(n) == 0 (global
    integrity — no node escaped the trigger across the full churn window).

Non-vacuity hard gates: suspends_ok > 0, triggers_restored > 0,
triggers_fired_after_resume > 0.

Run directly (smoke test):

  python3 workload.py --endpoint 127.0.0.1:7687 \\
      --parallelism 4 --duration-sec 30 --num-tenants 3

The stress runner invokes it via workload.yaml script_args.

NOTE: Designed to run against ASan / TSan builds.  All waits use bounded
retries rather than fixed sleep() so the test stays correct under sanitiser
slowdown.
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

# ---------------------------------------------------------------------------
# Driver import: mirror hot_cold/workload.py — fall back to direct neo4j import.
# ---------------------------------------------------------------------------
_STRESS_ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", ".."))
if _STRESS_ROOT not in sys.path:
    sys.path.insert(0, _STRESS_ROOT)

try:
    from neo4j import GraphDatabase
    from neo4j.exceptions import ClientError, ServiceUnavailable, TransientError
except ImportError as exc:
    sys.exit(f"FATAL: neo4j Python driver not installed: {exc}")

# ---------------------------------------------------------------------------
# Constants / tunables
# ---------------------------------------------------------------------------
_DEFAULT_ENDPOINT = "127.0.0.1:7687"
_DEFAULT_USERNAME = "neo4j"
_DEFAULT_PASSWORD = "1234"

# Nodes per data-writer transaction.  Kept small so one write never trips a
# half-commit and makes the committed-count accounting inexact.
_NODES_PER_TX = 10

# Trigger name and DDL used on every tenant.  BEFORE COMMIT fires synchronously
# in the committing transaction — deterministic, no async races with the count.
# Setting a property (n.stamped = ...) is an UPDATE, not a CREATE, so the
# ON CREATE trigger does not re-fire → no infinite loop.
_TRIGGER_NAME = "stamp_trigger"
_TRIGGER_DDL = (
    "CREATE TRIGGER stamp_trigger "
    "ON CREATE BEFORE COMMIT EXECUTE "
    "UNWIND createdVertices AS n SET n.stamped = timestamp()"
)

# v2 error-marker tuple (verbatim from hot_cold/workload.py + e2e convergence test).
_TRANSITIONAL_MARKERS = (
    "is suspended (cold)",  # writer touched a tenant that was just taken COLD
    "active connections",  # SUSPEND lost the race to an active writer
    "does not exist or is already cold",  # SUSPEND raced another suspender / already COLD
    "does not exist or is not suspended",  # RESUME raced a writer / already HOT
    "failed to recover while resuming",  # RESUME hit OOM mid-rebuild; COLD, retriable
    "memory limit exceeded",  # allocation tripped the hard memory ceiling
    "multiple concurrent system queries are not supported",  # SUSPEND+RESUME collided
    "unknown database",  # USE DATABASE arrived mid-suspend (in-memory repr torn down); retriable
)

_MAX_RETRIES = 120
_RETRY_SLEEP = 0.05  # seconds


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--endpoint",
        default=os.environ.get("ENDPOINT", _DEFAULT_ENDPOINT),
        help="host:port of the Memgraph Bolt listener",
    )
    parser.add_argument("--username", default=_DEFAULT_USERNAME)
    parser.add_argument("--password", default=_DEFAULT_PASSWORD)
    parser.add_argument(
        "--parallelism",
        type=int,
        default=int(os.environ.get("PARALLELISM", 4)),
        help="Number of concurrent data-writer threads",
    )
    parser.add_argument(
        "--num-tenants",
        type=int,
        default=3,
        help="Number of tenant databases to create (tenant_0 .. tenant_N-1)",
    )
    parser.add_argument(
        "--duration-sec",
        type=float,
        default=60.0,
        help="How long to run the concurrent phase (seconds)",
    )
    parser.add_argument(
        "--nodes-per-tx",
        type=int,
        default=_NODES_PER_TX,
        help="Nodes to create per data-writer transaction",
    )
    # Accept and ignore stress-runner boilerplate args.
    parser.add_argument("--worker-count", type=int, default=None)
    parser.add_argument("--logging", default=None)
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Driver helpers  (reused verbatim-style from hot_cold/workload.py)
# ---------------------------------------------------------------------------


def _make_driver(endpoint: str, username: str, password: str):
    """Create a neo4j Bolt driver.  Uses TRUST_ALL_CERTIFICATES for localhost."""
    try:
        from neo4j import TRUST_ALL_CERTIFICATES

        return GraphDatabase.driver(
            f"bolt://{endpoint}",
            auth=(username, password),
            encrypted=False,
            trust=TRUST_ALL_CERTIFICATES,
        )
    except TypeError:
        # Newer driver versions removed the trust parameter.
        return GraphDatabase.driver(
            f"bolt://{endpoint}",
            auth=(username, password),
            encrypted=False,
        )


def _run_query(session, query: str, **params) -> list[dict]:
    """Run a query and consume the result, returning data rows."""
    result = session.run(query, **params)
    data = result.data()
    result.consume()
    return data


def _is_transient(exc: Exception) -> bool:
    """Return True if the exception matches an expected hot/cold transient error."""
    msg = str(exc).lower()
    return any(marker.lower() in msg for marker in _TRANSITIONAL_MARKERS)


def _run_with_retry(session, query: str, max_retries: int = _MAX_RETRIES, **params) -> Optional[list[dict]]:
    """
    Execute a query on an already-open session, retrying on transient hot/cold
    errors.  Returns the data rows on success, or None if we exhausted retries
    on a skippable transient.  Raises on any non-transient error.
    """
    for _attempt in range(max_retries):
        try:
            return _run_query(session, query, **params)
        except (ClientError, TransientError) as exc:
            if _is_transient(exc):
                time.sleep(_RETRY_SLEEP)
                continue
            raise
        except Exception as exc:
            if _is_transient(exc):
                time.sleep(_RETRY_SLEEP)
                continue
            raise
    print(
        f"  [warn] query exhausted {max_retries} retries on transient error, skipping: {query[:80]}",
        flush=True,
    )
    return None


# ---------------------------------------------------------------------------
# Tenant lifecycle helpers  (reused from hot_cold/workload.py)
# ---------------------------------------------------------------------------


def _wait_for_server(endpoint: str, username: str, password: str, timeout: float = 30.0) -> None:
    """Poll until the server responds to a simple query."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            drv = _make_driver(endpoint, username, password)
            with drv.session() as sess:
                _run_query(sess, "RETURN 1 AS ok")
            drv.close()
            return
        except Exception:
            time.sleep(0.2)
    sys.exit(f"FATAL: server at {endpoint} did not become ready within {timeout}s")


def _create_tenants(endpoint: str, username: str, password: str, names: list[str]) -> None:
    """Create all tenant databases.  Ignore already-exists errors."""
    drv = _make_driver(endpoint, username, password)
    try:
        with drv.session() as sess:
            for name in names:
                try:
                    _run_query(sess, f"CREATE DATABASE {name}")
                    print(f"  created tenant {name}", flush=True)
                except Exception as exc:
                    msg = str(exc).lower()
                    if "already exists" in msg or "duplicate" in msg:
                        print(f"  tenant {name} already exists, continuing", flush=True)
                    else:
                        raise
    finally:
        drv.close()


def _resume_tenant_blocking(endpoint: str, username: str, password: str, name: str, timeout: float = 90.0) -> None:
    """
    Issue RESUME DATABASE <name> and wait until USE DATABASE <name> succeeds
    (i.e. tenant is fully HOT).  Used in the final verification phase.
    """
    deadline = time.monotonic() + timeout
    drv = _make_driver(endpoint, username, password)
    try:
        with drv.session() as sess:
            try:
                _run_query(sess, f"RESUME DATABASE {name}")
            except Exception as exc:
                msg = str(exc).lower()
                if "does not exist or is not suspended" not in msg and "does not exist" not in msg:
                    pass  # some other error; ignore, we will detect below

        while time.monotonic() < deadline:
            try:
                with drv.session() as sess:
                    _run_query(sess, f"USE DATABASE {name}")
                    _run_query(sess, "RETURN 1 AS ping")
                return
            except Exception as exc:
                if _is_transient(exc):
                    time.sleep(0.2)
                    continue
                raise
        raise RuntimeError(f"Tenant {name} did not come HOT within {timeout}s")
    finally:
        drv.close()


def _count_nodes_on_tenant(endpoint: str, username: str, password: str, name: str) -> int:
    """USE DATABASE <name> then MATCH (n) RETURN count(n).  Retries on transient errors."""
    drv = _make_driver(endpoint, username, password)
    try:
        for _attempt in range(_MAX_RETRIES):
            try:
                with drv.session() as sess:
                    _run_query(sess, f"USE DATABASE {name}")
                    rows = _run_query(sess, "MATCH (n) RETURN count(n) AS cnt")
                    return int(rows[0]["cnt"])
            except Exception as exc:
                if _is_transient(exc):
                    time.sleep(_RETRY_SLEEP)
                    continue
                raise
        raise RuntimeError(f"count_nodes on {name} failed after {_MAX_RETRIES} retries")
    finally:
        drv.close()


def _suspend_tenant(endpoint: str, username: str, password: str, name: str) -> None:
    """
    Best-effort SUSPEND DATABASE <name>.

    Tolerates: already-cold / transient / active-connection / durability /
    replica / default-database errors.  Retries on transient errors up to
    _MAX_RETRIES.  Never raises — suspend here is housekeeping to free memory
    before the next resume, not a correctness assertion.
    """
    _SUSPEND_SWALLOW = (
        "not in memory",
        "already",
        "cold",
        "suspended",
        "active",
        "durability",
        "replica",
        "default database",
    )
    drv = _make_driver(endpoint, username, password)
    try:
        for _attempt in range(_MAX_RETRIES):
            try:
                with drv.session() as sess:
                    _run_query(sess, f"SUSPEND DATABASE {name}")
                return
            except Exception as exc:
                msg = str(exc).lower()
                if _is_transient(exc):
                    time.sleep(_RETRY_SLEEP)
                    continue
                if any(t in msg for t in _SUSPEND_SWALLOW):
                    return
                # Unexpected error: swallow silently (best-effort helper).
                return
    finally:
        drv.close()


# ---------------------------------------------------------------------------
# Shared error collector  (reused verbatim from hot_cold_oom/workload.py)
# ---------------------------------------------------------------------------


class _ErrorCollector:
    """Thread-safe list of unexpected (non-marker) client errors."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._errors: list[str] = []

    def record(self, context: str, exc: Exception) -> None:
        msg = str(exc).lower()
        if not any(marker.lower() in msg for marker in _TRANSITIONAL_MARKERS):
            with self._lock:
                self._errors.append(f"[{context}] {type(exc).__name__}: {exc}")

    def errors(self) -> list[str]:
        with self._lock:
            return list(self._errors)


# ---------------------------------------------------------------------------
# Trigger setup helper
# ---------------------------------------------------------------------------


def _setup_trigger(endpoint: str, username: str, password: str, tenant_name: str) -> None:
    """
    USE DATABASE <tenant_name> then CREATE TRIGGER stamp_trigger if it does not
    already exist.  Idempotent: "already exists" errors are silently swallowed
    so the setup is safe to call after a server restart (tenant already had the
    trigger restored by on_resume_).
    """
    drv = _make_driver(endpoint, username, password)
    try:
        for _attempt in range(_MAX_RETRIES):
            try:
                with drv.session() as sess:
                    _run_query(sess, f"USE DATABASE {tenant_name}")
                    try:
                        _run_query(sess, _TRIGGER_DDL)
                        print(f"  created trigger '{_TRIGGER_NAME}' on {tenant_name}", flush=True)
                    except Exception as exc:
                        msg = str(exc).lower()
                        if "already exists" in msg or "duplicate" in msg:
                            print(f"  trigger '{_TRIGGER_NAME}' already exists on {tenant_name}", flush=True)
                        else:
                            raise
                return
            except Exception as exc:
                if _is_transient(exc):
                    time.sleep(_RETRY_SLEEP)
                    continue
                raise
        raise RuntimeError(f"_setup_trigger on {tenant_name} failed after {_MAX_RETRIES} retries")
    finally:
        drv.close()


# ---------------------------------------------------------------------------
# Worker 1: DATA-WRITER  (one per writer_id — round-robins across all tenants)
# ---------------------------------------------------------------------------


def _data_writer_worker(
    worker_id: int,
    endpoint: str,
    username: str,
    password: str,
    tenant_names: list[str],
    nodes_per_tx: int,
    stop_flag: list[bool],
    committed_counts: dict[str, int],
    counts_lock: threading.Lock,
    error_collector: _ErrorCollector,
    rng_seed: int,
) -> None:
    """
    Repeatedly pick a random tenant, open a session, USE DATABASE tenant_k,
    then BEGIN / CREATE nodes / COMMIT.

    Because the stamp_trigger fires BEFORE COMMIT (synchronously in the same
    transaction), every successfully committed :Data node MUST have .stamped set
    by the time the session returns from commit.

    On a clean commit, increments committed_counts[tenant_name] by 1 (= one
    batch = nodes_per_tx nodes).  On transient hot/cold error, retries up to
    _MAX_RETRIES.  On non-transient client error, logs and skips the op.
    On ServiceUnavailable, raises (server gone).
    """
    rng = random.Random(rng_seed)
    local_counts: dict[str, int] = defaultdict(int)
    drv = _make_driver(endpoint, username, password)

    try:
        while not stop_flag[0]:
            tenant = rng.choice(tenant_names)
            committed = False
            for _attempt in range(_MAX_RETRIES):
                if stop_flag[0]:
                    break
                try:
                    with drv.session() as sess:
                        _run_query(sess, f"USE DATABASE {tenant}")
                        tx = sess.begin_transaction()
                        try:
                            seq = local_counts[tenant]
                            tx.run(
                                "UNWIND range(1, $n) AS i " "CREATE (:Data {worker_id: $wid, seq: $seq + i})",
                                n=nodes_per_tx,
                                wid=worker_id,
                                seq=seq * nodes_per_tx,
                            ).consume()
                            tx.commit()
                            committed = True
                        except Exception:
                            try:
                                tx.rollback()
                            except Exception:
                                pass
                            raise
                    break
                except (ClientError, TransientError) as exc:
                    if _is_transient(exc):
                        time.sleep(_RETRY_SLEEP)
                        continue
                    # Non-transient client error — record and skip this op.
                    error_collector.record(f"data_writer:{worker_id}:{tenant}", exc)
                    break
                except ServiceUnavailable:
                    raise
                except Exception as exc:
                    if _is_transient(exc):
                        time.sleep(_RETRY_SLEEP)
                        continue
                    error_collector.record(f"data_writer:{worker_id}:{tenant}", exc)
                    break

            if committed:
                local_counts[tenant] += 1

            # Idle gap so the suspender can reach sole-accessor (the
            # connection-scoped v2 accessor is held only while a session
            # is open; sleeping here with the session already closed gives
            # the SUSPEND thread a clear window).
            time.sleep(random.uniform(0.15, 0.30))
    finally:
        drv.close()

    with counts_lock:
        for tname, cnt in local_counts.items():
            committed_counts[tname] = committed_counts.get(tname, 0) + cnt

    total_nodes = sum(local_counts[t] * nodes_per_tx for t in tenant_names)
    print(
        f"  [data_writer:{worker_id}] committed batches per tenant: "
        f"{dict(local_counts)}  ({total_nodes} nodes total)",
        flush=True,
    )


# ---------------------------------------------------------------------------
# Worker 2: SUSPENDER  (targets all tenants)
# ---------------------------------------------------------------------------


def _suspender_worker(
    endpoint: str,
    username: str,
    password: str,
    tenant_names: list[str],
    stop_flag: list[bool],
    error_collector: _ErrorCollector,
    rng_seed: int,
) -> int:
    """
    Repeatedly pick a random tenant and issue SUSPEND DATABASE.  Returns the
    count of successful SUSPEND operations for the non-vacuity assertion.
    """
    rng = random.Random(rng_seed)
    drv = _make_driver(endpoint, username, password)
    ops = 0
    try:
        while not stop_flag[0]:
            tenant = rng.choice(tenant_names)
            try:
                with drv.session() as sess:
                    _run_query(sess, f"SUSPEND DATABASE {tenant}")
                    ops += 1
            except (ClientError, TransientError) as exc:
                if _is_transient(exc):
                    pass
                else:
                    msg = str(exc).lower()
                    if any(m in msg for m in ("default database", "not in memory", "durability", "replica")):
                        pass
                    else:
                        error_collector.record(f"suspender:{tenant}", exc)
            except Exception as exc:
                if _is_transient(exc):
                    pass
                else:
                    error_collector.record(f"suspender:{tenant}", exc)
            time.sleep(_RETRY_SLEEP)
    finally:
        drv.close()
    print(f"  [suspender] issued {ops} successful suspends", flush=True)
    return ops


# ---------------------------------------------------------------------------
# Worker 3: RESUMER  (targets all tenants)
# ---------------------------------------------------------------------------


def _resumer_worker(
    endpoint: str,
    username: str,
    password: str,
    tenant_names: list[str],
    stop_flag: list[bool],
    error_collector: _ErrorCollector,
    rng_seed: int,
) -> int:
    """
    Repeatedly pick a random tenant and issue RESUME DATABASE.  Returns the
    count of successful RESUME operations (informational).
    """
    rng = random.Random(rng_seed)
    drv = _make_driver(endpoint, username, password)
    ops = 0
    try:
        while not stop_flag[0]:
            tenant = rng.choice(tenant_names)
            try:
                with drv.session() as sess:
                    _run_query(sess, f"RESUME DATABASE {tenant}")
                    ops += 1
            except (ClientError, TransientError) as exc:
                if _is_transient(exc):
                    pass
                else:
                    error_collector.record(f"resumer:{tenant}", exc)
            except Exception as exc:
                if _is_transient(exc):
                    pass
                else:
                    error_collector.record(f"resumer:{tenant}", exc)
            time.sleep(_RETRY_SLEEP)
    finally:
        drv.close()
    print(f"  [resumer] issued {ops} successful resumes", flush=True)
    return ops


# ---------------------------------------------------------------------------
# Worker 4: READER  (exercises read-path resume — no count assertion during run)
# ---------------------------------------------------------------------------


def _reader_worker(
    worker_id: int,
    endpoint: str,
    username: str,
    password: str,
    tenant_names: list[str],
    stop_flag: list[bool],
    rng_seed: int,
) -> None:
    """
    Exercises the read-path resume: USE DATABASE tenant_k then count nodes.
    No assertion during the run (counts change concurrently with writers).
    """
    rng = random.Random(rng_seed)
    drv = _make_driver(endpoint, username, password)
    ops = 0
    try:
        while not stop_flag[0]:
            tenant = rng.choice(tenant_names)
            for _attempt in range(_MAX_RETRIES):
                if stop_flag[0]:
                    break
                try:
                    with drv.session() as sess:
                        _run_query(sess, f"USE DATABASE {tenant}")
                        _run_query(sess, "MATCH (n) RETURN count(n) AS cnt")
                    ops += 1
                    break
                except Exception as exc:
                    if _is_transient(exc):
                        time.sleep(_RETRY_SLEEP)
                        continue
                    break
    finally:
        drv.close()
    print(f"  [reader-{worker_id}] performed {ops} read ops", flush=True)


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
    nodes_per_tx: int = max(1, args.nodes_per_tx)

    tenant_names = [f"tenant_{i}" for i in range(num_tenants)]

    print("==> Hot/Cold Trigger Durability stress (v2: manual suspend/resume + triggers)", flush=True)
    print(f"    endpoint       : {endpoint}", flush=True)
    print(f"    tenants        : {tenant_names}", flush=True)
    print(f"    parallelism    : {parallelism} writers + 1 suspender + 1 resumer + 1 reader", flush=True)
    print(f"    duration       : {duration_sec}s", flush=True)
    print(f"    nodes_per_tx   : {nodes_per_tx}", flush=True)
    print(f"    trigger_name   : {_TRIGGER_NAME}", flush=True)
    print(f"    trigger_ddl    : {_TRIGGER_DDL}", flush=True)

    # -----------------------------------------------------------------------
    # Phase 1: server readiness + tenant setup + trigger creation.
    # -----------------------------------------------------------------------
    print("\n==> Phase 1: server readiness + tenant setup + trigger creation", flush=True)
    _wait_for_server(endpoint, username, password)
    _create_tenants(endpoint, username, password, tenant_names)

    # Install the stamp_trigger on every tenant before the churn phase so that
    # every committed :Data node (including the very first batch) is stamped.
    for tname in tenant_names:
        _setup_trigger(endpoint, username, password, tname)

    # -----------------------------------------------------------------------
    # Phase 2: concurrent stress run.
    # -----------------------------------------------------------------------
    print(f"\n==> Phase 2: concurrent stress for {duration_sec}s", flush=True)

    stop_flag: list[bool] = [False]
    committed_counts: dict[str, int] = {}
    counts_lock = threading.Lock()
    error_collector = _ErrorCollector()

    base_seed = int(time.time())
    futures = []

    # Total workers: parallelism data-writers + 1 suspender + 1 resumer + 1 reader.
    total_workers = parallelism + 3

    with ThreadPoolExecutor(max_workers=total_workers) as pool:
        # DATA-WRITER threads.
        for wid in range(parallelism):
            f = pool.submit(
                _data_writer_worker,
                wid,
                endpoint,
                username,
                password,
                tenant_names,
                nodes_per_tx,
                stop_flag,
                committed_counts,
                counts_lock,
                error_collector,
                base_seed + wid,
            )
            futures.append(("data_writer", wid, f))

        # SUSPENDER thread.
        f = pool.submit(
            _suspender_worker,
            endpoint,
            username,
            password,
            tenant_names,
            stop_flag,
            error_collector,
            base_seed + parallelism,
        )
        futures.append(("suspender", 0, f))

        # RESUMER thread.
        f = pool.submit(
            _resumer_worker,
            endpoint,
            username,
            password,
            tenant_names,
            stop_flag,
            error_collector,
            base_seed + parallelism + 1,
        )
        futures.append(("resumer", 0, f))

        # READER thread.
        f = pool.submit(
            _reader_worker,
            0,
            endpoint,
            username,
            password,
            tenant_names,
            stop_flag,
            base_seed + parallelism + 2,
        )
        futures.append(("reader", 0, f))

        # Run for duration_sec then signal all workers to stop.
        time.sleep(duration_sec)
        stop_flag[0] = True
        print("  stop signal sent, waiting for workers...", flush=True)

        # Collect results / exceptions.
        suspends_ok: int = 0
        resumes_ok: int = 0
        worker_failures = []
        for role, wid, f in futures:
            try:
                result = f.result(timeout=120.0)
                if role == "suspender":
                    suspends_ok = result if result is not None else 0
                elif role == "resumer":
                    resumes_ok = result if result is not None else 0
            except Exception as exc:
                worker_failures.append((role, wid, exc))

    if worker_failures:
        for role, wid, exc in worker_failures:
            print(f"  WORKER FAILURE [{role}-{wid}]: {exc}", file=sys.stderr, flush=True)
        sys.exit("FAIL: one or more workers raised a non-transient exception")

    # -----------------------------------------------------------------------
    # Phase 3: unexpected-error check.
    # -----------------------------------------------------------------------
    print("\n==> Phase 3: unexpected-error check", flush=True)
    unexpected_errors = error_collector.errors()
    if unexpected_errors:
        print("  UNEXPECTED ERRORS detected:", file=sys.stderr, flush=True)
        for e in unexpected_errors:
            print(f"    {e}", file=sys.stderr, flush=True)
        sys.exit(f"FAIL: {len(unexpected_errors)} unexpected error(s) observed during the stress run.")
    print("  all client errors matched a known v2 marker: OK", flush=True)

    # -----------------------------------------------------------------------
    # Phase 4: compute expected node counts.
    # -----------------------------------------------------------------------
    print("\n==> Phase 4: computing expected node counts", flush=True)
    expected: dict[str, int] = {}
    for tname in tenant_names:
        batches = committed_counts.get(tname, 0)
        # +1 for the probe node added during per-tenant verification below.
        expected[tname] = batches * nodes_per_tx
        print(
            f"    expected[{tname}] = {expected[tname]} nodes ({batches} batches, "
            f"probe node counted separately in verification)",
            flush=True,
        )

    total_expected = sum(expected.values())
    print(f"    total expected (pre-probe): {total_expected} nodes across all tenants", flush=True)

    # -----------------------------------------------------------------------
    # Phase 5: one-at-a-time trigger verification.
    #
    # Pattern (from hot_cold_oom): suspend ALL tenants first, then for each
    # tenant: resume → verify trigger catalog + trigger fires + data integrity
    # → re-suspend before moving to the next.
    # -----------------------------------------------------------------------
    print("\n==> Phase 5: suspending all tenants before one-at-a-time verification", flush=True)
    for tname in tenant_names:
        _suspend_tenant(endpoint, username, password, tname)

    mismatches: list[tuple] = []
    triggers_restored: int = 0
    triggers_fired_after_resume: int = 0

    for tname in tenant_names:
        print(f"\n  verifying {tname}...", flush=True)
        resumed = False

        # Resume with bounded retry (tolerates transient errors).
        for _attempt in range(_MAX_RETRIES):
            try:
                _resume_tenant_blocking(endpoint, username, password, tname, timeout=90.0)
                resumed = True
                break
            except Exception as exc:
                if _is_transient(exc):
                    time.sleep(_RETRY_SLEEP)
                    continue
                mismatches.append((tname, -1, expected[tname], f"resume failed: {exc}"))
                break

        if not resumed:
            # Re-suspend best-effort before next iteration.
            _suspend_tenant(endpoint, username, password, tname)
            continue

        drv_verify = _make_driver(endpoint, username, password)
        try:
            # ---- Tooth 1: trigger-persistence ----
            # SHOW TRIGGERS must list stamp_trigger.
            try:
                with drv_verify.session() as sess:
                    _run_query(sess, f"USE DATABASE {tname}")
                    rows = _run_query(sess, "SHOW TRIGGERS")
                trigger_names = [r.get("trigger name", r.get("name", "")) for r in rows]
                if _TRIGGER_NAME in trigger_names:
                    triggers_restored += 1
                    print(f"    trigger-persistence: OK ('{_TRIGGER_NAME}' present)", flush=True)
                else:
                    mismatches.append(
                        (
                            tname,
                            -1,
                            -1,
                            f"trigger-persistence FAIL: '{_TRIGGER_NAME}' not in SHOW TRIGGERS: {trigger_names}",
                        )
                    )
                    print(
                        f"    FAIL trigger-persistence: '{_TRIGGER_NAME}' missing from SHOW TRIGGERS — got {trigger_names}",
                        flush=True,
                    )
            except Exception as exc:
                mismatches.append((tname, -1, -1, f"SHOW TRIGGERS failed: {exc}"))
                print(f"    FAIL SHOW TRIGGERS: {exc}", flush=True)

            # ---- Tooth 2: trigger fires after resume ----
            # Insert a probe node and verify .stamped IS NOT NULL.
            probe_stamped: Optional[int] = None
            try:
                with drv_verify.session() as sess:
                    _run_query(sess, f"USE DATABASE {tname}")
                    tx = sess.begin_transaction()
                    try:
                        tx.run("CREATE (:Data {probe: true})").consume()
                        tx.commit()
                    except Exception:
                        try:
                            tx.rollback()
                        except Exception:
                            pass
                        raise

                with drv_verify.session() as sess:
                    _run_query(sess, f"USE DATABASE {tname}")
                    rows = _run_query(sess, "MATCH (n:Data {probe: true}) RETURN n.stamped AS stamped LIMIT 1")
                    if rows:
                        probe_stamped = rows[0]["stamped"]

                if probe_stamped is not None:
                    triggers_fired_after_resume += 1
                    print(f"    trigger-fires-after-resume: OK (stamped={probe_stamped})", flush=True)
                else:
                    mismatches.append(
                        (
                            tname,
                            -1,
                            -1,
                            "trigger-fires-after-resume FAIL: probe node has stamped=None",
                        )
                    )
                    print(
                        "    FAIL trigger-fires-after-resume: probe node has stamped=None",
                        flush=True,
                    )
            except Exception as exc:
                mismatches.append((tname, -1, -1, f"probe write / stamped check failed: {exc}"))
                print(f"    FAIL probe write: {exc}", flush=True)

            # ---- Tooth 3: node count integrity ----
            # committed batches * nodes_per_tx + 1 probe node.
            expected_with_probe = expected[tname] + 1
            try:
                actual = _count_nodes_on_tenant(endpoint, username, password, tname)
                if actual != expected_with_probe:
                    mismatches.append((tname, actual, expected_with_probe, "COUNT MISMATCH (including probe node)"))
                    print(
                        f"    FAIL count: actual={actual} expected={expected_with_probe}",
                        flush=True,
                    )
                else:
                    print(f"    count: OK ({actual} nodes = {expected[tname]} data + 1 probe)", flush=True)
            except Exception as exc:
                mismatches.append((tname, -1, expected_with_probe, f"count query failed: {exc}"))
                print(f"    FAIL count query: {exc}", flush=True)

            # ---- Tooth 4: global integrity — zero unstamped nodes ----
            # Every :Data node (written during churn AND the probe) must have
            # .stamped set.  A single unstamped node means the trigger was
            # absent during a commit window — a data-integrity failure.
            try:
                with drv_verify.session() as sess:
                    _run_query(sess, f"USE DATABASE {tname}")
                    rows = _run_query(
                        sess,
                        "MATCH (n:Data) WHERE n.stamped IS NULL RETURN count(n) AS cnt",
                    )
                    unstamped = int(rows[0]["cnt"])
                if unstamped == 0:
                    print(f"    global-integrity: OK (0 unstamped nodes)", flush=True)
                else:
                    mismatches.append(
                        (
                            tname,
                            unstamped,
                            0,
                            f"global-integrity FAIL: {unstamped} unstamped :Data node(s) — "
                            "trigger did not fire for every commit",
                        )
                    )
                    print(
                        f"    FAIL global-integrity: {unstamped} unstamped :Data node(s)",
                        flush=True,
                    )
            except Exception as exc:
                mismatches.append((tname, -1, 0, f"unstamped-count query failed: {exc}"))
                print(f"    FAIL unstamped query: {exc}", flush=True)

        finally:
            drv_verify.close()
            # Re-suspend this tenant so the next tenant has the full memory
            # budget available when it resumes.  Best-effort: never raises.
            _suspend_tenant(endpoint, username, password, tname)

    # -----------------------------------------------------------------------
    # Phase 6: server liveness check.
    # -----------------------------------------------------------------------
    print("\n==> Phase 6: server liveness check", flush=True)
    try:
        drv_live = _make_driver(endpoint, username, password)
        with drv_live.session() as sess:
            rows = _run_query(sess, "RETURN 1 AS alive")
            assert rows[0]["alive"] == 1
        drv_live.close()
        print("  server alive: OK", flush=True)
    except Exception as exc:
        sys.exit(f"FAIL: server liveness check failed — possible crash: {exc}")

    # -----------------------------------------------------------------------
    # Phase 7: non-vacuity check.
    # -----------------------------------------------------------------------
    print("\n==> Phase 7: non-vacuity check", flush=True)
    print(f"    suspends_ok                  : {suspends_ok}", flush=True)
    print(f"    resumes_ok                   : {resumes_ok}  (informational)", flush=True)
    print(f"    triggers_restored            : {triggers_restored}", flush=True)
    print(f"    triggers_fired_after_resume  : {triggers_fired_after_resume}", flush=True)
    print(f"    total_committed (pre-probe)  : {total_expected} nodes", flush=True)

    vacuity_failures = []
    if suspends_ok == 0:
        vacuity_failures.append(
            "suspends_ok=0: the suspender never landed a successful SUSPEND. "
            "Increase --duration-sec or --num-tenants and re-run."
        )
    if triggers_restored == 0:
        vacuity_failures.append(
            "triggers_restored=0: no tenant's trigger survived a suspend→resume cycle. "
            "Either no SUSPEND succeeded, or the on_resume_ / RestoreTriggers path is broken."
        )
    if triggers_fired_after_resume == 0:
        vacuity_failures.append(
            "triggers_fired_after_resume=0: the restored trigger never fired on any tenant. "
            "The trigger may be present in the catalog but not wired into the commit path after resume."
        )

    if vacuity_failures:
        for msg in vacuity_failures:
            print(f"  FAIL: {msg}", file=sys.stderr, flush=True)
        sys.exit("FAIL: non-vacuity check failed — churn window did not exercise required conditions.")

    print(
        "  non-vacuity: OK (SUSPEND fired, trigger survived at least one resume, "
        "trigger fired after at least one resume)",
        flush=True,
    )

    # -----------------------------------------------------------------------
    # Final verdict.
    # -----------------------------------------------------------------------
    print("\n" + "=" * 70, flush=True)
    if mismatches:
        print("RESULT: FAIL — trigger durability or data-integrity error detected", flush=True)
        for tname, actual, exp, reason in mismatches:
            print(f"  {tname}: {reason}  actual={actual} expected={exp}", flush=True)
        sys.exit(1)
    else:
        print("RESULT: PASS — all tenants verified, triggers durable and firing, no data loss", flush=True)
        print(
            f"  {len(tenant_names)} tenants verified, {total_expected} data nodes + {len(tenant_names)} probe nodes, "
            f"suspends_ok={suspends_ok}, triggers_restored={triggers_restored}, "
            f"triggers_fired_after_resume={triggers_fired_after_resume}",
            flush=True,
        )
    print("=" * 70, flush=True)


if __name__ == "__main__":
    main()
