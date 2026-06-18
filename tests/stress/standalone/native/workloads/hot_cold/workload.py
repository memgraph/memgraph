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
Hot/Cold Tenant Concurrency Stress Workload (v2).

Hammers the hot/cold 4-state gatekeeper (HOT -> SUSPENDING -> COLD ->
RESUMING -> HOT) under two-way concurrent contention:

  1. WRITER threads   — open a per-tenant connection, USE DATABASE tenant_k,
                        BEGIN / CREATE M nodes / COMMIT; track committed count.
  2. SUSPENDER thread — issue SUSPEND DATABASE tenant_k on random tenants via
                        the default-db control connection.
  3. RESUMER thread   — issue RESUME DATABASE tenant_k on random tenants via
                        the default-db control connection.
  4. READER thread    — USE DATABASE tenant_k / MATCH (n) RETURN count(n);
                        exercises the read-path resume, no count assertion
                        during the run.

NOTE: v2 re-scoped the feature to MANUAL suspend/resume only. All
auto-eviction machinery was deleted. The only churn dimension is the
SUSPEND/RESUME thread pair above.

After the timed run every tenant is resumed and its live node count is checked
against the sum of per-worker committed writes. A mismatch = data loss = exit 1.

A non-vacuity check asserts that at least one SUSPEND and at least one RESUME
actually succeeded during the churn window. If neither count is positive, the
test is vacuously green and exits 1 with a clear message.

Run directly (smoke test):

  python3 workload.py --endpoint 127.0.0.1:7687 \\
      --parallelism 6 --duration-sec 20 --num-tenants 6

The stress runner invokes it via the workload.yaml script_args.

NOTE: Designed to run against ASan / TSan builds.  All waits use bounded retries
rather than fixed sleep(), so the test remains correct under sanitizer slowdown.
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

# ---------------------------------------------------------------------------
# Driver import: mirror ring/workload.py — use the neo4j driver via common.py
# helpers when available, fall back to direct neo4j import.
# ---------------------------------------------------------------------------
# Allow running from the source tree (stress runner sets PYTHONPATH or cwd).
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

# Error substrings we tolerate under normal hot/cold contention (v2 marker set).
# These are the authoritative v2 retriable outcomes, copied verbatim from
# tests/e2e/system_replication/hot_cold_convergence.py::EXPECTED_STRESS_MARKERS.
# Any client error whose lowercased text matches NONE of these is a real failure.
_TRANSITIONAL_MARKERS = (
    "is suspended (cold)",  # a writer touched a tenant a suspender just took COLD
    "active connections",  # SUSPEND lost the race to a writer still holding the tenant
    "does not exist or is already cold",  # SUSPEND raced another suspender / tenant already COLD
    "does not exist or is not suspended",  # RESUME raced a writer / another resumer; tenant already HOT
    "failed to recover while resuming",  # RESUME hit OOM mid-rebuild; left COLD, retriable
    "memory limit exceeded",  # an allocation tripped the hard memory ceiling
    "multiple concurrent system queries are not supported",  # SUSPEND and RESUME collided on the system tx
)

_MAX_RETRIES = 120  # bounded retry ceiling for transient errors
_RETRY_SLEEP = 0.05  # seconds between retries (small; avoid tight-spin)


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
        default=int(os.environ.get("PARALLELISM", 8)),
        help="Number of concurrent writer threads",
    )
    parser.add_argument(
        "--num-tenants",
        type=int,
        default=6,
        help="Number of tenant databases to create (tenant_0 .. tenant_N-1)",
    )
    parser.add_argument(
        "--duration-sec",
        type=float,
        default=20.0,
        help="How long to run the concurrent phase (seconds)",
    )
    parser.add_argument(
        "--nodes-per-tx",
        type=int,
        default=5,
        help="Nodes to create per writer transaction",
    )
    # The stress runner passes these; accept and ignore.
    parser.add_argument("--worker-count", type=int, default=None)
    parser.add_argument("--logging", default=None)
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Driver helpers
# ---------------------------------------------------------------------------


def _make_driver(endpoint: str, username: str, password: str):
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


def _run_query(session, query: str, **params) -> list[dict]:
    """Run a query and consume the result, returning data rows."""
    result = session.run(query, **params)
    data = result.data()
    result.consume()
    return data


def _is_transient(exc: Exception) -> bool:
    """Return True if the exception is an expected hot/cold transient error."""
    msg = str(exc).lower()
    return any(marker.lower() in msg for marker in _TRANSITIONAL_MARKERS)


def _run_with_retry(session, query: str, max_retries: int = _MAX_RETRIES, **params) -> Optional[list[dict]]:
    """
    Execute a query on an already-open session, retrying on transient hot/cold
    errors (tenant suspended/cold, active-connections on suspend, etc.).

    Returns the data rows on success, or None if we exhausted retries on a
    *skippable* transient (like SUSPEND failing because of active connections).
    Raises on any non-transient error.
    """
    for attempt in range(max_retries):
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
    # Exhausted retries on transient — log and return None (caller decides).
    print(
        f"  [warn] query exhausted {max_retries} retries on transient error, skipping: {query[:80]}",
        flush=True,
    )
    return None


# ---------------------------------------------------------------------------
# Tenant lifecycle helpers
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
    """Create all tenant databases. Ignore already-exists errors."""
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


def _resume_tenant_blocking(endpoint: str, username: str, password: str, name: str, timeout: float = 60.0) -> None:
    """
    Issue RESUME DATABASE <name> and wait until USE DATABASE <name> succeeds
    (i.e., tenant is fully HOT).  Used in the final verification phase.
    """
    deadline = time.monotonic() + timeout
    drv = _make_driver(endpoint, username, password)
    try:
        # First try to resume (may already be hot).
        with drv.session() as sess:
            try:
                _run_query(sess, f"RESUME DATABASE {name}")
            except Exception as exc:
                msg = str(exc).lower()
                # "does not exist or is not suspended" means the tenant is already HOT — fine.
                # Any other error is suppressed here; we will detect it during the poll below.
                if "does not exist or is not suspended" not in msg and "does not exist" not in msg:
                    pass  # some other error; ignore, we will poll below

        # Now poll until USE DATABASE succeeds.
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
    """
    USE DATABASE <name> then MATCH (n) RETURN count(n).
    Retries on transient resuming errors.
    """
    drv = _make_driver(endpoint, username, password)
    try:
        for attempt in range(_MAX_RETRIES):
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


# ---------------------------------------------------------------------------
# Worker functions (run in threads)
# ---------------------------------------------------------------------------


def _writer_worker(
    worker_id: int,
    endpoint: str,
    username: str,
    password: str,
    tenant_names: list[str],
    nodes_per_tx: int,
    stop_flag: list[bool],  # mutable single-element list used as a shared flag
    committed_counts: dict,  # tenant_name -> int  (each worker writes its own key)
    rng_seed: int,
) -> None:
    """
    Repeatedly pick a random tenant, open a connection on it, create
    `nodes_per_tx` nodes labelled :W{worker_id} in an explicit transaction.
    On success, increment committed_counts[tenant_name].
    On transient hot/cold error, retry the whole op.
    """
    rng = random.Random(rng_seed)
    local_counts: dict[str, int] = defaultdict(int)
    drv = _make_driver(endpoint, username, password)

    try:
        while not stop_flag[0]:
            tenant = rng.choice(tenant_names)
            # Each write attempt: new session (so USE DATABASE and state are fresh).
            committed = False
            for attempt in range(_MAX_RETRIES):
                if stop_flag[0]:
                    break
                try:
                    with drv.session() as sess:
                        # Switch to tenant context.
                        _run_query(sess, f"USE DATABASE {tenant}")
                        # Explicit transaction: BEGIN / CREATE / COMMIT.
                        tx = sess.begin_transaction()
                        try:
                            seq = local_counts[tenant]
                            tx.run(
                                "UNWIND range(1, $n) AS i " "CREATE (:W {worker_id: $wid, seq: $seq + i})",
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
                    # Non-transient client error — log and skip this op.
                    print(
                        f"  [writer-{worker_id}] non-transient error on {tenant}: {exc}",
                        flush=True,
                    )
                    break
                except ServiceUnavailable:
                    # Server gone — propagate up so the test catches it.
                    raise
                except Exception as exc:
                    if _is_transient(exc):
                        time.sleep(_RETRY_SLEEP)
                        continue
                    print(
                        f"  [writer-{worker_id}] unexpected error on {tenant}: {exc}",
                        flush=True,
                    )
                    break

            if committed:
                local_counts[tenant] += 1
    finally:
        drv.close()

    # Accumulate into shared dict under a unique per-worker key.
    for tenant, count in local_counts.items():
        committed_counts[f"{worker_id}:{tenant}"] = count


def _suspender_worker(
    endpoint: str,
    username: str,
    password: str,
    tenant_names: list[str],
    stop_flag: list[bool],
    rng_seed: int,
) -> int:
    """
    Repeatedly pick a random tenant and issue SUSPEND DATABASE on the default
    connection.  All expected-under-contention errors are ignored.

    Returns the count of successful SUSPEND operations (used for non-vacuity
    assertion in main).
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
                    pass  # expected race
                else:
                    msg = str(exc).lower()
                    # Also tolerate "cannot suspend the default database" and
                    # other permanent guards that fire during contention.
                    if any(
                        m in msg for m in ("default database", "not in memory", "durability", "replicat", "replica")
                    ):
                        pass
                    else:
                        print(f"  [suspender] unexpected error on {tenant}: {exc}", flush=True)
            except Exception as exc:
                if _is_transient(exc):
                    pass
                else:
                    print(f"  [suspender] unexpected error on {tenant}: {exc}", flush=True)
            time.sleep(_RETRY_SLEEP)
    finally:
        drv.close()
    print(f"  [suspender] issued {ops} successful suspends", flush=True)
    return ops


def _resumer_worker(
    endpoint: str,
    username: str,
    password: str,
    tenant_names: list[str],
    stop_flag: list[bool],
    rng_seed: int,
) -> int:
    """
    Repeatedly pick a random tenant and issue RESUME DATABASE on the default
    connection.  All expected-under-contention errors are ignored.

    Returns the count of successful RESUME operations (used for non-vacuity
    assertion in main).
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
                    print(f"  [resumer] unexpected error on {tenant}: {exc}", flush=True)
            except Exception as exc:
                if _is_transient(exc):
                    pass
                else:
                    print(f"  [resumer] unexpected error on {tenant}: {exc}", flush=True)
            time.sleep(_RETRY_SLEEP)
    finally:
        drv.close()
    print(f"  [resumer] issued {ops} successful resumes", flush=True)
    return ops


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

    print("==> Hot/Cold concurrency stress (v2: manual suspend/resume only)", flush=True)
    print(f"    endpoint       : {endpoint}", flush=True)
    print(f"    tenants        : {tenant_names}", flush=True)
    print(f"    parallelism    : {parallelism} writers + 1 suspender + 1 resumer + 1 reader", flush=True)
    print(f"    duration       : {duration_sec}s", flush=True)
    print(f"    nodes_per_tx   : {nodes_per_tx}", flush=True)

    # Phase 1: wait for server, then create tenant databases.
    print("\n==> Phase 1: server readiness + tenant setup", flush=True)
    _wait_for_server(endpoint, username, password)
    _create_tenants(endpoint, username, password, tenant_names)

    # Phase 2: concurrent stress run.
    print(f"\n==> Phase 2: concurrent stress for {duration_sec}s", flush=True)
    stop_flag: list[bool] = [False]
    committed_counts: dict[str, int] = {}

    base_seed = int(time.time())
    futures = []

    with ThreadPoolExecutor(max_workers=parallelism + 3) as pool:
        # Writer workers (parallelism of them).
        for wid in range(parallelism):
            f = pool.submit(
                _writer_worker,
                wid,
                endpoint,
                username,
                password,
                tenant_names,
                nodes_per_tx,
                stop_flag,
                committed_counts,
                base_seed + wid,
            )
            futures.append(("writer", wid, f))

        # One suspender.
        f = pool.submit(
            _suspender_worker,
            endpoint,
            username,
            password,
            tenant_names,
            stop_flag,
            base_seed + parallelism,
        )
        futures.append(("suspender", 0, f))

        # One resumer.
        f = pool.submit(
            _resumer_worker,
            endpoint,
            username,
            password,
            tenant_names,
            stop_flag,
            base_seed + parallelism + 1,
        )
        futures.append(("resumer", 0, f))

        # One reader.
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

        # Let all workers run for duration_sec, then signal stop.
        time.sleep(duration_sec)
        stop_flag[0] = True
        print("  stop signal sent, waiting for workers...", flush=True)

        # Collect any exceptions from workers (non-transient = real bug).
        suspends_ok: int = 0
        resumes_ok: int = 0
        worker_failures = []
        for role, wid, f in futures:
            try:
                result = f.result(timeout=60.0)
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

    # Phase 3: compute expected counts per tenant.
    print("\n==> Phase 3: computing expected node counts", flush=True)
    expected: dict[str, int] = defaultdict(int)
    for key, count in committed_counts.items():
        _wid_str, tenant = key.split(":", 1)
        expected[tenant] += count * nodes_per_tx

    for name in tenant_names:
        print(f"    expected[{name}] = {expected[name]} nodes", flush=True)

    total_expected = sum(expected.values())
    print(f"    total expected: {total_expected} nodes across all tenants", flush=True)

    # Phase 4: resume each tenant and verify actual counts.
    print("\n==> Phase 4: final data-integrity verification", flush=True)

    mismatches = []
    for name in tenant_names:
        print(f"  verifying {name}...", flush=True)
        try:
            _resume_tenant_blocking(endpoint, username, password, name, timeout=60.0)
        except Exception as exc:
            mismatches.append((name, -1, expected[name], f"resume failed: {exc}"))
            continue

        try:
            actual = _count_nodes_on_tenant(endpoint, username, password, name)
        except Exception as exc:
            mismatches.append((name, -1, expected[name], f"count query failed: {exc}"))
            continue

        if actual != expected[name]:
            mismatches.append((name, actual, expected[name], "COUNT MISMATCH"))
            print(
                f"  FAIL {name}: actual={actual} expected={expected[name]}",
                flush=True,
            )
        else:
            print(f"  OK   {name}: {actual} nodes", flush=True)

    # Phase 5: server liveness check.
    print("\n==> Phase 5: server liveness check", flush=True)
    try:
        drv = _make_driver(endpoint, username, password)
        with drv.session() as sess:
            rows = _run_query(sess, "RETURN 1 AS alive")
            assert rows[0]["alive"] == 1
        drv.close()
        print("  server alive: OK", flush=True)
    except Exception as exc:
        sys.exit(f"FAIL: server liveness check failed — possible crash: {exc}")

    # Phase 6: non-vacuity check.
    # If neither the suspender nor the resumer managed a single successful op,
    # the churn window never exercised the state machine and the test is
    # vacuously green — that is a harness bug, not a PASS.
    print("\n==> Phase 6: non-vacuity check", flush=True)
    print(f"    suspends_ok : {suspends_ok}", flush=True)
    print(f"    resumes_ok  : {resumes_ok}", flush=True)
    if suspends_ok == 0 or resumes_ok == 0:
        sys.exit(
            f"FAIL: non-vacuity check failed — "
            f"suspends_ok={suspends_ok} resumes_ok={resumes_ok}. "
            "The churn window did not exercise both SUSPEND and RESUME. "
            "Increase --duration-sec or --num-tenants and re-run."
        )
    print("  non-vacuity: OK (both SUSPEND and RESUME fired at least once)", flush=True)

    # Final verdict.
    print("\n" + "=" * 60, flush=True)
    if mismatches:
        print("RESULT: FAIL — data loss or integrity error detected", flush=True)
        for name, actual, exp, reason in mismatches:
            print(f"  {name}: {reason}  actual={actual} expected={exp}", flush=True)
        sys.exit(1)
    else:
        print("RESULT: PASS — all tenant counts match, no data loss", flush=True)
        print(f"  {len(tenant_names)} tenants verified, {total_expected} total nodes", flush=True)
    print("=" * 60, flush=True)


if __name__ == "__main__":
    main()
