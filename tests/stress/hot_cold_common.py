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

"""Shared scaffolding for the hot/cold stress workloads."""

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
# Driver import: fall back to direct neo4j import.
# ---------------------------------------------------------------------------
try:
    from neo4j import GraphDatabase
    from neo4j.exceptions import ClientError, ServiceUnavailable, TransientError
except ImportError as exc:
    sys.exit(f"FATAL: neo4j Python driver not installed: {exc}")

# ---------------------------------------------------------------------------
# Constants / tunables
# ---------------------------------------------------------------------------
DEFAULT_ENDPOINT = "127.0.0.1:7687"
DEFAULT_USERNAME = "neo4j"
DEFAULT_PASSWORD = "1234"

MAX_RETRIES = 120  # bounded retry ceiling for transient errors
RETRY_SLEEP = 0.05  # seconds between retries (small; avoid tight-spin)

# Quiescent tail (seconds) the suspender runs AFTER the data writers / resumer have
# stopped. SUSPEND fails fast on an in-use database (ACTIVE_CONNECTIONS, by design), so
# under continuous churn the suspender can finish the contended window with zero
# successful suspends and trip the suspends_ok>0 non-vacuity gate on a busy/slow CI host
# (a flaky false failure, not a product bug). Draining the antagonists first gives the
# suspender a contention-free tail in which its SUSPENDs land deterministically.
SUSPENDER_TAIL_SEC = 5.0  # seconds

# Error substrings we tolerate under normal hot/cold contention (v2 marker set).
# 8-marker SUPERSET: the 7 in the base workload PLUS "unknown database" (which
# idx/trg/ttl also tolerate — USE DATABASE can race a mid-suspend teardown).
# Any client error whose lowercased text matches NONE of these is a real failure.
TRANSITIONAL_MARKERS = (
    "is suspended (cold)",  # a writer touched a tenant a suspender just took COLD
    "active connections",  # SUSPEND lost the race to a writer still holding the tenant
    "does not exist or is already cold",  # SUSPEND raced another suspender / tenant already COLD
    "does not exist or is not suspended",  # RESUME raced a writer / another resumer; tenant already HOT
    "failed to recover while resuming",  # RESUME hit OOM mid-rebuild; left COLD, retriable
    "memory limit exceeded",  # an allocation tripped the hard memory ceiling
    "multiple concurrent system queries are not supported",  # SUSPEND and RESUME collided on the system tx
    "unknown database",  # USE DATABASE arrived mid-suspend (in-memory repr torn down); retriable
)


# ---------------------------------------------------------------------------
# Driver helpers
# ---------------------------------------------------------------------------


def make_driver(endpoint: str, username: str, password: str):
    """Create a neo4j Bolt driver. Uses TRUST_ALL_CERTIFICATES for localhost."""
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


def run_query(session, query: str, **params) -> list[dict]:
    """Run a query and consume the result, returning data rows."""
    result = session.run(query, **params)
    data = result.data()
    result.consume()
    return data


def is_transient(exc: Exception) -> bool:
    """Return True if the exception is an expected hot/cold transient error."""
    msg = str(exc).lower()
    return any(marker.lower() in msg for marker in TRANSITIONAL_MARKERS)


# ---------------------------------------------------------------------------
# Tenant lifecycle helpers
# ---------------------------------------------------------------------------


def wait_for_server(endpoint: str, username: str, password: str, timeout: float = 30.0) -> None:
    """Poll until the server responds to a simple query."""
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


def create_tenants(endpoint: str, username: str, password: str, names: list[str]) -> None:
    """Create all tenant databases. Ignore already-exists errors."""
    drv = make_driver(endpoint, username, password)
    try:
        with drv.session() as sess:
            for name in names:
                try:
                    run_query(sess, f"CREATE DATABASE {name}")
                    print(f"  created tenant {name}", flush=True)
                except Exception as exc:
                    msg = str(exc).lower()
                    if "already exists" in msg or "duplicate" in msg:
                        print(f"  tenant {name} already exists, continuing", flush=True)
                    else:
                        raise
    finally:
        drv.close()


def resume_tenant_blocking(endpoint: str, username: str, password: str, name: str, timeout: float = 90.0) -> None:
    """
    Issue RESUME DATABASE <name> and wait until USE DATABASE <name> succeeds
    (i.e. tenant is fully HOT).  Used in the final verification phase.
    """
    deadline = time.monotonic() + timeout
    drv = make_driver(endpoint, username, password)
    try:
        while time.monotonic() < deadline:
            try:
                with drv.session() as sess:
                    run_query(sess, f"RESUME DATABASE {name}")
            except Exception:
                pass  # already HOT, or transient failure — detect below

            try:
                with drv.session() as sess:
                    run_query(sess, f"USE DATABASE {name}")
                    run_query(sess, "RETURN 1 AS ping")
                return
            except Exception as exc:
                if is_transient(exc):
                    time.sleep(0.2)
                    continue
                raise
        raise RuntimeError(f"Tenant {name} did not come HOT within {timeout}s")
    finally:
        drv.close()


def count_nodes_on_tenant(endpoint: str, username: str, password: str, name: str) -> int:
    """
    USE DATABASE <name> then MATCH (n) RETURN count(n).
    Retries on transient resuming errors.
    """
    drv = make_driver(endpoint, username, password)
    try:
        for _attempt in range(MAX_RETRIES):
            try:
                with drv.session() as sess:
                    run_query(sess, f"USE DATABASE {name}")
                    rows = run_query(sess, "MATCH (n) RETURN count(n) AS cnt")
                    return int(rows[0]["cnt"])
            except Exception as exc:
                if is_transient(exc):
                    time.sleep(RETRY_SLEEP)
                    continue
                raise
        raise RuntimeError(f"count_nodes on {name} failed after {MAX_RETRIES} retries")
    finally:
        drv.close()


def suspend_tenant(endpoint: str, username: str, password: str, name: str) -> None:
    """
    Best-effort SUSPEND DATABASE <name>.

    Tolerates: already-cold / transient / active-connection / durability /
    replica / default-database errors.  Retries on transient errors up to
    MAX_RETRIES.  Never raises — suspend here is housekeeping to free memory
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
    drv = make_driver(endpoint, username, password)
    try:
        for _attempt in range(MAX_RETRIES):
            try:
                with drv.session() as sess:
                    run_query(sess, f"SUSPEND DATABASE {name}")
                return
            except Exception as exc:
                msg = str(exc).lower()
                if is_transient(exc):
                    time.sleep(RETRY_SLEEP)
                    continue
                if any(t in msg for t in _SUSPEND_SWALLOW):
                    return
                # Unexpected error: swallow silently (best-effort helper).
                return
    finally:
        drv.close()


# ---------------------------------------------------------------------------
# Shared error collector
# ---------------------------------------------------------------------------


class ErrorCollector:
    """Thread-safe list of unexpected (non-marker) client errors."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._errors: list[str] = []

    def record(self, context: str, exc: Exception) -> None:
        msg = str(exc).lower()
        if not any(marker.lower() in msg for marker in TRANSITIONAL_MARKERS):
            with self._lock:
                self._errors.append(f"[{context}] {type(exc).__name__}: {exc}")

    def errors(self) -> list[str]:
        with self._lock:
            return list(self._errors)


# ---------------------------------------------------------------------------
# Shared worker functions (run in threads)
# ---------------------------------------------------------------------------


def reader_worker(
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
    drv = make_driver(endpoint, username, password)
    ops = 0
    try:
        while not stop_flag[0]:
            tenant = rng.choice(tenant_names)
            for _attempt in range(MAX_RETRIES):
                if stop_flag[0]:
                    break
                try:
                    with drv.session() as sess:
                        run_query(sess, f"USE DATABASE {tenant}")
                        run_query(sess, "MATCH (n) RETURN count(n) AS cnt")
                    ops += 1
                    break
                except Exception as exc:
                    if is_transient(exc):
                        time.sleep(RETRY_SLEEP)
                        continue
                    break
    finally:
        drv.close()
    print(f"  [reader-{worker_id}] performed {ops} read ops", flush=True)


def suspender_worker(
    endpoint: str,
    username: str,
    password: str,
    tenant_names: list[str],
    stop_flag: list[bool],
    error_collector: ErrorCollector,
    rng_seed: int,
) -> int:
    """
    Repeatedly pick a random tenant and issue SUSPEND DATABASE.  Returns the
    count of successful SUSPEND operations for the non-vacuity assertion.
    """
    rng = random.Random(rng_seed)
    drv = make_driver(endpoint, username, password)
    ops = 0
    try:
        while not stop_flag[0]:
            tenant = rng.choice(tenant_names)
            try:
                with drv.session() as sess:
                    run_query(sess, f"SUSPEND DATABASE {tenant}")
                    ops += 1
            except (ClientError, TransientError) as exc:
                if is_transient(exc):
                    pass
                else:
                    msg = str(exc).lower()
                    if any(m in msg for m in ("default database", "not in memory", "durability", "replica")):
                        pass
                    else:
                        error_collector.record(f"suspender:{tenant}", exc)
            except Exception as exc:
                if is_transient(exc):
                    pass
                else:
                    error_collector.record(f"suspender:{tenant}", exc)
            time.sleep(RETRY_SLEEP)
    finally:
        drv.close()
    print(f"  [suspender] issued {ops} successful suspends", flush=True)
    return ops


def resumer_worker(
    endpoint: str,
    username: str,
    password: str,
    tenant_names: list[str],
    stop_flag: list[bool],
    error_collector: ErrorCollector,
    rng_seed: int,
) -> int:
    """
    Repeatedly pick a random tenant and issue RESUME DATABASE.  Returns the
    count of successful RESUME operations (informational only).
    """
    rng = random.Random(rng_seed)
    drv = make_driver(endpoint, username, password)
    ops = 0
    try:
        while not stop_flag[0]:
            tenant = rng.choice(tenant_names)
            try:
                with drv.session() as sess:
                    run_query(sess, f"RESUME DATABASE {tenant}")
                    ops += 1
            except (ClientError, TransientError) as exc:
                if is_transient(exc):
                    pass
                else:
                    error_collector.record(f"resumer:{tenant}", exc)
            except Exception as exc:
                if is_transient(exc):
                    pass
                else:
                    error_collector.record(f"resumer:{tenant}", exc)
            time.sleep(RETRY_SLEEP)
    finally:
        drv.close()
    print(f"  [resumer] issued {ops} successful resumes", flush=True)
    return ops


# ---------------------------------------------------------------------------
# Phase orchestration helpers (shared across all five hot/cold workloads)
# ---------------------------------------------------------------------------


def staggered_stop(
    stop_flag: list[bool],
    suspender_stop_flag: list[bool],
    duration_sec: float,
    tail_sec: float = SUSPENDER_TAIL_SEC,
) -> None:
    """
    Sleep for *duration_sec*, set stop_flag[0]=True (draining antagonists),
    print the quiescent-tail message, sleep for *tail_sec*, then set
    suspender_stop_flag[0]=True and print the final "waiting for workers" line.

    Must be called from INSIDE the ThreadPoolExecutor ``with`` block so that
    the pool is still alive when the flags are read by the worker threads.

    The *tail_sec* parameter defaults to SUSPENDER_TAIL_SEC so callers that
    rely on the module constant do not need to pass it explicitly.
    """
    time.sleep(duration_sec)
    stop_flag[0] = True
    print(
        f"  antagonists stopped; giving the suspender a {tail_sec}s quiescent tail window...",
        flush=True,
    )
    time.sleep(tail_sec)
    suspender_stop_flag[0] = True
    print("  stop signal sent, waiting for workers...", flush=True)


def collect_worker_results(
    futures: list[tuple[str, object, object]],
    timeout: float,
) -> tuple[int, int]:
    """
    Drain *futures* (a list of ``(role, wid, future)`` triples), collect
    ``suspends_ok`` and ``resumes_ok`` tallies, and call ``sys.exit`` with a
    descriptive message on any worker exception.

    Returns ``(suspends_ok, resumes_ok)``.

    The ``sys.exit`` message is:
      "FAIL: one or more workers raised a non-transient exception"
    matching the message used across all five workloads.
    """
    suspends_ok: int = 0
    resumes_ok: int = 0
    worker_failures: list[tuple[str, object, Exception]] = []

    for role, wid, f in futures:
        try:
            result = f.result(timeout=timeout)
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

    return suspends_ok, resumes_ok


def assert_server_alive(endpoint: str, username: str, password: str) -> None:
    """
    Open a fresh driver, run ``RETURN 1 AS alive``, assert the result, then
    close the driver.  On any exception calls ``sys.exit`` with the message:
      "FAIL: server liveness check failed — possible crash: <exc>"

    Mirrors the Phase N "server liveness check" block present in every workload.
    """
    try:
        drv = make_driver(endpoint, username, password)
        with drv.session() as sess:
            rows = run_query(sess, "RETURN 1 AS alive")
            assert rows[0]["alive"] == 1
        drv.close()
        print("  server alive: OK", flush=True)
    except Exception as exc:
        sys.exit(f"FAIL: server liveness check failed — possible crash: {exc}")


def fail_on_unexpected_errors(error_collector: "ErrorCollector", suffix: str = "") -> None:
    """
    Call ``error_collector.errors()``.  If any errors are present, print each
    one to stderr and call ``sys.exit`` with:
      "FAIL: <N> unexpected error(s) observed during the stress run. <suffix>"

    The *suffix* is workload-specific (e.g. "All client errors must match a
    known v2 marker.").  Pass an empty string (the default) when the workload
    uses no suffix.

    On success (no errors) prints:
      "  all client errors matched a known v2 marker: OK"
    """
    unexpected_errors = error_collector.errors()
    if unexpected_errors:
        print("  UNEXPECTED ERRORS detected:", file=sys.stderr, flush=True)
        for e in unexpected_errors:
            print(f"    {e}", file=sys.stderr, flush=True)
        msg = f"FAIL: {len(unexpected_errors)} unexpected error(s) observed during the stress run."
        if suffix:
            msg = f"{msg} {suffix}"
        sys.exit(msg)
    print("  all client errors matched a known v2 marker: OK", flush=True)


# ---------------------------------------------------------------------------
# Argument parser factory
# ---------------------------------------------------------------------------


def build_base_arg_parser(
    *,
    description: str,
    default_parallelism: int,
    default_num_tenants: int,
    default_duration_sec: float,
    add_nodes_per_tx: bool = False,
    default_nodes_per_tx: int = 10,
) -> argparse.ArgumentParser:
    """
    Return an ArgumentParser pre-populated with the args common to ALL five
    hot/cold stress workloads.  The caller adds feature-specific args afterward
    and calls parser.parse_args() itself.

    Common args (present in every workload's parse_args):
      --endpoint         host:port of the Memgraph Bolt listener
      --username
      --password
      --parallelism      Number of concurrent writer/data-writer threads
      --num-tenants      Number of tenant databases to create
      --duration-sec     How long to run the concurrent phase (seconds)
      --worker-count     Accepted and ignored (stress-runner boilerplate)
      --logging          Accepted and ignored (stress-runner boilerplate)

    Conditional arg (present in base/idx/trg but NOT in ttl/oom):
      --nodes-per-tx     Only added when add_nodes_per_tx=True
    """
    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--endpoint",
        default=os.environ.get("ENDPOINT", DEFAULT_ENDPOINT),
        help="host:port of the Memgraph Bolt listener",
    )
    parser.add_argument("--username", default=DEFAULT_USERNAME)
    parser.add_argument("--password", default=DEFAULT_PASSWORD)
    parser.add_argument(
        "--parallelism",
        type=int,
        default=int(os.environ.get("PARALLELISM", default_parallelism)),
        help="Number of concurrent writer threads",
    )
    parser.add_argument(
        "--num-tenants",
        type=int,
        default=default_num_tenants,
        help="Number of tenant databases to create (tenant_0 .. tenant_N-1)",
    )
    parser.add_argument(
        "--duration-sec",
        type=float,
        default=default_duration_sec,
        help="How long to run the concurrent phase (seconds)",
    )
    if add_nodes_per_tx:
        parser.add_argument(
            "--nodes-per-tx",
            type=int,
            default=default_nodes_per_tx,
            help="Nodes to create per writer transaction",
        )
    # The stress runner passes these; accept and ignore.
    parser.add_argument("--worker-count", type=int, default=None)
    parser.add_argument("--logging", default=None)
    return parser
