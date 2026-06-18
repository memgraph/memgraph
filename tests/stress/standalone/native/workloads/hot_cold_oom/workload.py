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
Hot/Cold OOM Stress Workload.

Hammers the hot/cold suspend/resume engine under a HARD MEMORY CEILING to prove:

  1. OOM is always a clean retriable error — never a crash, a terminate, or a
     half-state.
  2. Hot/cold SUSPEND + RESUME is crash-free under concurrent memory pressure.
  3. Per-tenant node counts exactly match committed writes (no data loss) even
     when the memory-hog thread forced OOM repeatedly.
  4. Runtime tenant-profile changes (resource limits) while tenants cycle
     COLD/HOT do not corrupt state or cause unexpected errors.

Tenants are split into two DISJOINT ROLES so the suspend/resume-under-pressure
path is reliably exercised:

  BALLAST tenants (first half of t0..tN-1):
    Grown large by dedicated GROWER threads that run continuously (no
    duty-cycle sleep) and are NEVER suspended.  They provide the persistent
    memory footprint that keeps the system under pressure.  Counted for
    data-loss (committed_counts).

  CHURN tenants (second half):
    Written lightly by a LIGHT-WRITER thread that writes one small batch then
    sleeps 150–300 ms, so each churn tenant is idle most of the time and
    reliably suspendable.  Subjected to repeated SUSPEND / RESUME by the
    SUSPENDER + RESUMER threads.  Because the churn tenants are NOT
    continuously pinned, SUSPEND reliably reaches sole-accessor.
    The RESUME happens UNDER the ballast's memory pressure, exercising the
    "resume-under-pressure / resume-cannot-fit" safe-fail path.
    Counted for data-loss (committed_counts).

Five concurrent thread roles:

  1. GROWER threads (one per BALLAST tenant): USE DATABASE tk then commit small
     batches of 25 nodes with a distinct ~payload_bytes string payload so the
     tenant accrues real resident memory.  No inter-batch sleep — the ballast
     stays continuously busy / HOT.  Committed batches counted for integrity.
     Past _GROWER_CAP the grower keeps cycling without incrementing the counter.

  2. LIGHT-WRITER thread: picks a random CHURN tenant, writes one small batch
     (25 nodes), then sleeps 150–300 ms.  A "is suspended (cold)" error means
     the tenant is mid-churn — tolerated, NOT counted (committed stays exact).
     Counted into committed_counts only on a clean commit.

  3. MEMORY-HOG thread: on the default `memgraph` DB repeatedly runs
       WITH range(1, 8000000) AS r RETURN size(r)
     which is purely transient.  Under the ceiling + grown ballast tenants this
     fires OOM repeatedly.  Each OOM increments oom_observed.

  4. SUSPENDER + RESUMER threads: target ONLY churn tenants (rng.choice from
     churn list).  suspends_ok now climbs reliably because churn tenants are
     idle most of the time.  RESUME that fails with "failed to recover while
     resuming" or "memory limit exceeded" is the RESUME-CANNOT-FIT safe-fail
     path — counted as resume_failed_safe and treated as EXPECTED/retriable.

  5. PROFILE-CHANGER thread: periodically CREATE / ALTER / SET / REMOVE tenant
     profiles on CHURN tenants to stress the scenario where a tenant's resource
     limit changes while it is COLD and is then resumed under the new limit.

After the timed run:
  - Every tenant is resumed HOT (profiles removed so nothing blocks recovery).
  - Per-tenant on-disk node count is checked against the committed sum for
    BOTH ballast AND churn tenants.
  - Non-vacuity: oom_observed > 0 REQUIRED, suspends_ok > 0 REQUIRED.
    resumes_ok is trivially true (RESUME on a HOT tenant is an idempotent
    no-op that still counts) — it is logged but NOT a hard gate.

Run directly (smoke test):

  python3 workload.py --endpoint 127.0.0.1:7687 \\
      --parallelism 6 --duration-sec 30 --num-tenants 4 --payload-bytes 16384

The stress runner invokes it via the workload.yaml script_args.

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

# Nodes per GROWER / LIGHT-WRITER transaction (keep small so one write never
# trips OOM mid-commit — that would make the committed-count accounting inexact).
_NODES_PER_TX = 25

# Per-tenant maximum COUNTED batches for BALLAST growers.  Past this cap the
# grower keeps cycling without incrementing the counter so the all-HOT
# footprint stays bounded.
_GROWER_CAP = 4000

# Per-tenant maximum COUNTED batches for LIGHT-WRITER churn tenants.  Kept
# lower than _GROWER_CAP because churn tenants are written slowly.
_LIGHT_WRITER_CAP = 400

# HOG query: large transient allocator — produces ~64 MiB of range list data.
# Commits nothing; designed to reliably trip the 1024 MiB ceiling once tenants
# have grown.
_HOG_QUERY = "WITH range(1, 8000000) AS r RETURN size(r)"

# v2 error-marker tuple (verbatim from hot_cold/workload.py + e2e convergence test).
_TRANSITIONAL_MARKERS = (
    "is suspended (cold)",  # writer touched a tenant that was just taken COLD
    "active connections",  # SUSPEND lost the race to an active writer
    "does not exist or is already cold",  # SUSPEND raced another suspender / already COLD
    "does not exist or is not suspended",  # RESUME raced a writer / already HOT
    "failed to recover while resuming",  # RESUME hit OOM mid-rebuild; COLD, retriable
    "memory limit exceeded",  # allocation tripped the hard memory ceiling
    "multiple concurrent system queries are not supported",  # SUSPEND+RESUME collided on system tx
)

# Additional substrings tolerated by the PROFILE-CHANGER thread (profile/limit
# races that are not bugs).
_PROFILE_EXTRA_TOLERATIONS = (
    "profile",
    "does not exist",
    "memory",
    "already exists",
    "limit",
)

_MAX_RETRIES = 120
_RETRY_SLEEP = 0.05  # seconds

# Light-writer inter-batch sleep range (seconds).  Wide enough that a churn
# tenant is idle for much longer than the SUSPEND sole-accessor freeze window
# (~100ms), so a concurrent SUSPEND almost always wins.
_LIGHT_WRITER_SLEEP_LO = 0.15
_LIGHT_WRITER_SLEEP_HI = 0.30


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
        default=int(os.environ.get("PARALLELISM", 6)),
        help="Hint for ThreadPoolExecutor size; actual worker count is determined by tenant split",
    )
    parser.add_argument(
        "--num-tenants",
        type=int,
        default=4,
        help="Number of tenant databases to create (t0 .. tN-1); must be >= 2",
    )
    parser.add_argument(
        "--duration-sec",
        type=float,
        default=120.0,
        help="How long to run the concurrent phase (seconds)",
    )
    parser.add_argument(
        "--payload-bytes",
        type=int,
        default=16384,
        help="Approximate size of the string payload stored in each ballast node",
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
# Shared error collector
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
# Worker 1: GROWER  (one per BALLAST tenant — runs continuously, no sleep)
# ---------------------------------------------------------------------------


def _grower_worker(
    tenant_name: str,
    endpoint: str,
    username: str,
    password: str,
    payload_bytes: int,
    stop_flag: list[bool],
    committed_counts: dict[str, int],
    counts_lock: threading.Lock,
    error_collector: _ErrorCollector,
    rng_seed: int,
) -> None:
    """
    Continuously commit small batches of _NODES_PER_TX nodes to a BALLAST
    tenant.  No inter-batch sleep — the ballast tenant stays continuously busy
    and HOT, providing persistent memory pressure.

    committed_counts[tenant_name] is incremented on every successful commit
    (each unit = _NODES_PER_TX nodes).  Past _GROWER_CAP the grower keeps
    looping (to maintain memory pressure / contention) but stops incrementing
    the counter.
    """
    rng = random.Random(rng_seed)
    # Pre-build a base payload string; we vary it per-batch with the sequence
    # number so payloads are distinct and cannot be de-duped.
    base_payload = "X" * max(1, payload_bytes - 20)
    local_committed: int = 0
    drv = _make_driver(endpoint, username, password)

    try:
        while not stop_flag[0]:
            past_cap = local_committed >= _GROWER_CAP
            seq = local_committed
            payload = f"{seq:020d}{base_payload}"[: payload_bytes + 20]

            committed = False
            for _attempt in range(_MAX_RETRIES):
                if stop_flag[0]:
                    break
                try:
                    with drv.session() as sess:
                        _run_query(sess, f"USE DATABASE {tenant_name}")
                        tx = sess.begin_transaction()
                        try:
                            tx.run(
                                "UNWIND range(1, $n) AS i " "CREATE (:N {seq: $seq + i, payload: $payload})",
                                n=_NODES_PER_TX,
                                seq=seq * _NODES_PER_TX,
                                payload=payload,
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
                    error_collector.record(f"grower:{tenant_name}", exc)
                    break
                except ServiceUnavailable:
                    raise
                except Exception as exc:
                    if _is_transient(exc):
                        time.sleep(_RETRY_SLEEP)
                        continue
                    error_collector.record(f"grower:{tenant_name}", exc)
                    break

            if committed and not past_cap:
                local_committed += 1
            # No sleep: ballast growers run at full speed to stay HOT and
            # maintain memory pressure.
    finally:
        drv.close()

    with counts_lock:
        committed_counts[tenant_name] = local_committed
    print(
        f"  [grower:{tenant_name}] committed {local_committed} batches " f"({local_committed * _NODES_PER_TX} nodes)",
        flush=True,
    )


# ---------------------------------------------------------------------------
# Worker 2: LIGHT-WRITER  (one shared thread for ALL churn tenants)
# ---------------------------------------------------------------------------


def _light_writer_worker(
    churn_tenants: list[str],
    endpoint: str,
    username: str,
    password: str,
    stop_flag: list[bool],
    committed_counts: dict[str, int],
    counts_lock: threading.Lock,
    error_collector: _ErrorCollector,
    rng_seed: int,
) -> None:
    """
    Picks a random CHURN tenant, writes one small batch (25 nodes), then
    sleeps 150–300 ms so the churn tenant is idle most of the time and
    reliably suspendable by the SUSPENDER thread.

    A "is suspended (cold)" error means the tenant is mid-suspend/resume cycle —
    tolerated and NOT counted (committed_counts stays exact).  Only a clean
    commit increments the counter.
    """
    rng = random.Random(rng_seed)
    local_counts: dict[str, int] = {t: 0 for t in churn_tenants}
    drv = _make_driver(endpoint, username, password)

    try:
        while not stop_flag[0]:
            tenant = rng.choice(churn_tenants)
            past_cap = local_counts[tenant] >= _LIGHT_WRITER_CAP
            seq = local_counts[tenant]
            # Small payload for churn tenants — just enough to be a real write.
            payload = f"churn-{seq:010d}-{'Y' * 64}"

            committed = False
            try:
                with drv.session() as sess:
                    _run_query(sess, f"USE DATABASE {tenant}")
                    tx = sess.begin_transaction()
                    try:
                        tx.run(
                            "UNWIND range(1, $n) AS i " "CREATE (:N {seq: $seq + i, payload: $payload})",
                            n=_NODES_PER_TX,
                            seq=seq * _NODES_PER_TX,
                            payload=payload,
                        ).consume()
                        tx.commit()
                        committed = True
                    except Exception:
                        try:
                            tx.rollback()
                        except Exception:
                            pass
                        raise
            except (ClientError, TransientError) as exc:
                msg = str(exc).lower()
                if "is suspended (cold)" in msg:
                    # Tenant is mid-suspend: tolerated, do NOT count.
                    pass
                elif _is_transient(exc):
                    # Other expected transient: tolerated, do NOT count.
                    pass
                else:
                    error_collector.record(f"light_writer:{tenant}", exc)
            except ServiceUnavailable:
                raise
            except Exception as exc:
                if _is_transient(exc):
                    pass
                else:
                    error_collector.record(f"light_writer:{tenant}", exc)

            if committed and not past_cap:
                local_counts[tenant] += 1

            # Wide idle gap — churn tenant must be idle long enough for a
            # concurrent SUSPEND to reach sole-accessor.
            time.sleep(rng.uniform(_LIGHT_WRITER_SLEEP_LO, _LIGHT_WRITER_SLEEP_HI))
    finally:
        drv.close()

    with counts_lock:
        for tname, cnt in local_counts.items():
            committed_counts[tname] = committed_counts.get(tname, 0) + cnt

    total_nodes = sum(local_counts[t] * _NODES_PER_TX for t in churn_tenants)
    print(
        f"  [light_writer] committed batches per churn tenant: " f"{dict(local_counts)}  ({total_nodes} nodes total)",
        flush=True,
    )


# ---------------------------------------------------------------------------
# Worker 3: MEMORY-HOG
# ---------------------------------------------------------------------------


def _memory_hog_worker(
    endpoint: str,
    username: str,
    password: str,
    stop_flag: list[bool],
    oom_counter: list[int],
    oom_lock: threading.Lock,
    error_collector: _ErrorCollector,
) -> None:
    """
    Repeatedly run a large transient-allocator query on the default `memgraph`
    DB.  Commits nothing.  Each "memory limit exceeded" OOM increments
    oom_counter[0].  Any other non-marker error is recorded.
    """
    drv = _make_driver(endpoint, username, password)
    runs = 0
    try:
        while not stop_flag[0]:
            try:
                with drv.session() as sess:
                    _run_query(sess, _HOG_QUERY)
                runs += 1
            except (ClientError, TransientError) as exc:
                msg = str(exc).lower()
                if "memory limit exceeded" in msg:
                    with oom_lock:
                        oom_counter[0] += 1
                elif _is_transient(exc):
                    pass  # other expected transients
                else:
                    error_collector.record("memory_hog", exc)
            except ServiceUnavailable:
                raise
            except Exception as exc:
                if _is_transient(exc):
                    pass
                else:
                    error_collector.record("memory_hog", exc)
            time.sleep(_RETRY_SLEEP)
    finally:
        drv.close()
    print(f"  [memory_hog] ran {runs} queries, OOM hits will be in oom_counter", flush=True)


# ---------------------------------------------------------------------------
# Worker 4a: SUSPENDER  (targets CHURN tenants only)
# ---------------------------------------------------------------------------


def _suspender_worker(
    endpoint: str,
    username: str,
    password: str,
    churn_tenants: list[str],
    stop_flag: list[bool],
    error_collector: _ErrorCollector,
    rng_seed: int,
) -> int:
    """
    Repeatedly pick a random CHURN tenant and issue SUSPEND DATABASE.  Returns
    the count of successful SUSPEND operations for the non-vacuity assertion.

    Because churn tenants are idle most of the time (light-writer sleeps
    150–300 ms between batches), SUSPEND reliably reaches sole-accessor and
    suspends_ok climbs consistently.
    """
    rng = random.Random(rng_seed)
    drv = _make_driver(endpoint, username, password)
    ops = 0
    try:
        while not stop_flag[0]:
            tenant = rng.choice(churn_tenants)
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
# Worker 4b: RESUMER  (targets CHURN tenants only)
# ---------------------------------------------------------------------------


def _resumer_worker(
    endpoint: str,
    username: str,
    password: str,
    churn_tenants: list[str],
    stop_flag: list[bool],
    resume_failed_safe_counter: list[int],
    resume_lock: threading.Lock,
    error_collector: _ErrorCollector,
    rng_seed: int,
) -> int:
    """
    Repeatedly pick a random CHURN tenant and issue RESUME DATABASE.  Returns
    the count of successful RESUME operations (informational only — not a hard
    non-vacuity gate because RESUME on an already-HOT tenant is an idempotent
    no-op that counts too).

    A RESUME that fails with "failed to recover while resuming" or
    "memory limit exceeded" is the RESUME-CANNOT-FIT safe-fail path —
    increments resume_failed_safe_counter and is NOT treated as a failure.
    """
    rng = random.Random(rng_seed)
    drv = _make_driver(endpoint, username, password)
    ops = 0
    try:
        while not stop_flag[0]:
            tenant = rng.choice(churn_tenants)
            try:
                with drv.session() as sess:
                    _run_query(sess, f"RESUME DATABASE {tenant}")
                    ops += 1
            except (ClientError, TransientError) as exc:
                msg = str(exc).lower()
                if "failed to recover while resuming" in msg or "memory limit exceeded" in msg:
                    with resume_lock:
                        resume_failed_safe_counter[0] += 1
                elif _is_transient(exc):
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
# Worker 5: PROFILE-CHANGER  (targets CHURN tenants only)
# ---------------------------------------------------------------------------


def _profile_changer_worker(
    endpoint: str,
    username: str,
    password: str,
    churn_tenants: list[str],
    stop_flag: list[bool],
    error_collector: _ErrorCollector,
    rng_seed: int,
) -> None:
    """
    Periodically create, alter, set, and remove tenant profiles on CHURN
    tenants to stress the scenario where a tenant's resource limit changes
    while it is COLD and is then resumed under the new limit.

    Tolerated errors: any exception whose text matches a v2 marker OR contains
    "profile", "does not exist", "memory", "already exists", or "limit".
    Anything else is recorded as an unexpected error.
    """
    rng = random.Random(rng_seed)
    drv = _make_driver(endpoint, username, password)
    profile_seq = 0

    def _is_profile_tolerated(exc: Exception) -> bool:
        msg = str(exc).lower()
        if _is_transient(exc):
            return True
        return any(t in msg for t in _PROFILE_EXTRA_TOLERATIONS)

    try:
        while not stop_flag[0]:
            tenant = rng.choice(churn_tenants)
            limit_mb = rng.randint(50, 400)
            profile_name = f"p_{profile_seq}"
            profile_seq += 1

            # Step 1: CREATE TENANT PROFILE
            try:
                with drv.session() as sess:
                    _run_query(
                        sess,
                        f"CREATE TENANT PROFILE {profile_name} LIMIT memory_limit {limit_mb} MB",
                    )
            except Exception as exc:
                if not _is_profile_tolerated(exc):
                    error_collector.record(f"profile_changer:create:{profile_name}", exc)
                time.sleep(_RETRY_SLEEP)
                continue

            # Step 2: SET TENANT PROFILE ON DATABASE tenant TO profile_name
            try:
                with drv.session() as sess:
                    _run_query(
                        sess,
                        f"SET TENANT PROFILE ON DATABASE {tenant} TO {profile_name}",
                    )
            except Exception as exc:
                if not _is_profile_tolerated(exc):
                    error_collector.record(f"profile_changer:set:{tenant}:{profile_name}", exc)
                # Continue to the remove step even if set failed.

            # Step 3: occasionally ALTER the profile limit
            if rng.random() < 0.5:
                new_limit_mb = rng.randint(50, 400)
                try:
                    with drv.session() as sess:
                        _run_query(
                            sess,
                            f"ALTER TENANT PROFILE {profile_name} SET memory_limit {new_limit_mb} MB",
                        )
                except Exception as exc:
                    if not _is_profile_tolerated(exc):
                        error_collector.record(f"profile_changer:alter:{profile_name}", exc)

            # Step 4: REMOVE TENANT PROFILE FROM DATABASE tenant
            try:
                with drv.session() as sess:
                    _run_query(sess, f"REMOVE TENANT PROFILE FROM DATABASE {tenant}")
            except Exception as exc:
                if not _is_profile_tolerated(exc):
                    error_collector.record(f"profile_changer:remove:{tenant}", exc)

            time.sleep(rng.uniform(0.05, 0.3))
    finally:
        drv.close()
    print(f"  [profile_changer] cycled {profile_seq} profiles", flush=True)


# ---------------------------------------------------------------------------
# Tenant role split helper
# ---------------------------------------------------------------------------


def _split_tenants(tenant_names: list[str]) -> tuple[list[str], list[str]]:
    """
    Split `tenant_names` into (ballast, churn) halves.

    ballast = first half  (at least 1)
    churn   = second half (at least 1)

    For odd counts the churn half gets the extra tenant.
    Requires len(tenant_names) >= 2.
    """
    n = len(tenant_names)
    if n < 2:
        sys.exit("FATAL: --num-tenants must be >= 2 (need at least 1 ballast and 1 churn tenant)")
    split_idx = max(1, n // 2)
    ballast = tenant_names[:split_idx]
    churn = tenant_names[split_idx:]
    if not churn:
        # Safety net (split_idx == n would be caught by the n < 2 guard, but be explicit).
        sys.exit("FATAL: tenant split produced an empty churn set — increase --num-tenants")
    return ballast, churn


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()

    endpoint: str = args.endpoint
    username: str = args.username
    password: str = args.password
    num_tenants: int = args.num_tenants
    duration_sec: float = args.duration_sec
    payload_bytes: int = max(1, args.payload_bytes)

    tenant_names = [f"t{i}" for i in range(num_tenants)]
    ballast_tenants, churn_tenants = _split_tenants(tenant_names)

    # Total thread count:
    #   len(ballast_tenants) GROWER threads
    #   1 LIGHT-WRITER thread (serves all churn tenants via rng.choice)
    #   1 MEMORY-HOG thread
    #   1 SUSPENDER thread
    #   1 RESUMER thread
    #   1 PROFILE-CHANGER thread
    total_workers = len(ballast_tenants) + 5

    print("==> Hot/Cold OOM stress (memory ceiling + suspend/resume + profile changes)", flush=True)
    print(f"    endpoint         : {endpoint}", flush=True)
    print(f"    all tenants      : {tenant_names}", flush=True)
    print(f"    ballast tenants  : {ballast_tenants}  (GROWER × {len(ballast_tenants)}, never suspended)", flush=True)
    print(f"    churn tenants    : {churn_tenants}  (LIGHT-WRITER + SUSPEND/RESUME churn)", flush=True)
    print(f"    duration         : {duration_sec}s", flush=True)
    print(f"    payload_bytes    : {payload_bytes} (ballast nodes)", flush=True)
    print(f"    nodes_per_tx     : {_NODES_PER_TX}", flush=True)
    print(f"    grower_cap       : {_GROWER_CAP} batches/ballast-tenant", flush=True)
    print(f"    light_writer_cap : {_LIGHT_WRITER_CAP} batches/churn-tenant", flush=True)
    print(f"    total_workers    : {total_workers}", flush=True)
    print(f"    parallelism_hint : {args.parallelism}", flush=True)

    # -----------------------------------------------------------------------
    # Phase 1: server readiness + tenant setup.
    # -----------------------------------------------------------------------
    print("\n==> Phase 1: server readiness + tenant setup", flush=True)
    _wait_for_server(endpoint, username, password)
    _create_tenants(endpoint, username, password, tenant_names)

    # -----------------------------------------------------------------------
    # Phase 2: concurrent stress run.
    # -----------------------------------------------------------------------
    print(f"\n==> Phase 2: concurrent stress for {duration_sec}s", flush=True)

    stop_flag: list[bool] = [False]
    committed_counts: dict[str, int] = {}
    counts_lock = threading.Lock()
    oom_counter: list[int] = [0]
    oom_lock = threading.Lock()
    resume_failed_safe_counter: list[int] = [0]
    resume_lock = threading.Lock()
    error_collector = _ErrorCollector()

    base_seed = int(time.time())
    futures = []

    with ThreadPoolExecutor(max_workers=total_workers) as pool:
        # GROWER threads: one per BALLAST tenant (continuous, no sleep).
        for i, tname in enumerate(ballast_tenants):
            f = pool.submit(
                _grower_worker,
                tname,
                endpoint,
                username,
                password,
                payload_bytes,
                stop_flag,
                committed_counts,
                counts_lock,
                error_collector,
                base_seed + i,
            )
            futures.append(("grower", tname, f))

        # LIGHT-WRITER thread: serves all churn tenants.
        f = pool.submit(
            _light_writer_worker,
            churn_tenants,
            endpoint,
            username,
            password,
            stop_flag,
            committed_counts,
            counts_lock,
            error_collector,
            base_seed + len(ballast_tenants),
        )
        futures.append(("light_writer", "churn", f))

        # MEMORY-HOG thread.
        f = pool.submit(
            _memory_hog_worker,
            endpoint,
            username,
            password,
            stop_flag,
            oom_counter,
            oom_lock,
            error_collector,
        )
        futures.append(("memory_hog", "default", f))

        # SUSPENDER thread (churn tenants only).
        f = pool.submit(
            _suspender_worker,
            endpoint,
            username,
            password,
            churn_tenants,
            stop_flag,
            error_collector,
            base_seed + len(ballast_tenants) + 1,
        )
        futures.append(("suspender", "churn", f))

        # RESUMER thread (churn tenants only).
        f = pool.submit(
            _resumer_worker,
            endpoint,
            username,
            password,
            churn_tenants,
            stop_flag,
            resume_failed_safe_counter,
            resume_lock,
            error_collector,
            base_seed + len(ballast_tenants) + 2,
        )
        futures.append(("resumer", "churn", f))

        # PROFILE-CHANGER thread (churn tenants only).
        f = pool.submit(
            _profile_changer_worker,
            endpoint,
            username,
            password,
            churn_tenants,
            stop_flag,
            error_collector,
            base_seed + len(ballast_tenants) + 3,
        )
        futures.append(("profile_changer", "churn", f))

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
        sys.exit(
            f"FAIL: {len(unexpected_errors)} unexpected error(s) observed during the stress run. "
            "OOM must always surface as a retriable marker, never an unrecognised exception."
        )
    print("  all client errors matched a known v2 marker: OK", flush=True)

    # -----------------------------------------------------------------------
    # Phase 4: compute expected node counts.
    # -----------------------------------------------------------------------
    print("\n==> Phase 4: computing expected node counts", flush=True)
    expected: dict[str, int] = {}
    for tname in tenant_names:
        batches = committed_counts.get(tname, 0)
        expected[tname] = batches * _NODES_PER_TX
        role_tag = "ballast" if tname in ballast_tenants else "churn"
        print(
            f"    expected[{tname}] ({role_tag}) = {expected[tname]} nodes ({batches} batches)",
            flush=True,
        )

    total_expected = sum(expected.values())
    print(f"    total expected: {total_expected} nodes across all tenants", flush=True)

    # -----------------------------------------------------------------------
    # Phase 5: remove all profiles (so no tiny-limit profile blocks resume),
    #          then resume each tenant HOT and verify actual counts.
    # -----------------------------------------------------------------------
    print("\n==> Phase 5: profile cleanup + final data-integrity verification", flush=True)

    # Remove any lingering tenant profiles before resuming.
    # Ballast tenants never had profiles set; this is a no-op for them.
    drv_cleanup = _make_driver(endpoint, username, password)
    try:
        with drv_cleanup.session() as sess:
            for tname in tenant_names:
                try:
                    _run_query(sess, f"REMOVE TENANT PROFILE FROM DATABASE {tname}")
                    print(f"  removed profile from {tname}", flush=True)
                except Exception as exc:
                    msg = str(exc).lower()
                    # "does not exist" / "profile" errors are fine — tenant had no profile.
                    if not any(t in msg for t in _PROFILE_EXTRA_TOLERATIONS):
                        print(f"  [warn] could not remove profile from {tname}: {exc}", flush=True)
    finally:
        drv_cleanup.close()

    print("  freeing memory: suspended all tenants before one-at-a-time verification", flush=True)
    for tname in tenant_names:
        _suspend_tenant(endpoint, username, password, tname)

    mismatches = []
    for tname in tenant_names:
        role_tag = "ballast" if tname in ballast_tenants else "churn"
        print(f"  verifying {tname} ({role_tag})...", flush=True)
        try:
            # Resume with bounded retry (tolerates transient / OOM by retrying).
            # Each tenant gets nearly the full memory budget because all others
            # were suspended above (and will be re-suspended in the finally block
            # after counting so the next iteration also has the full budget).
            resumed = False
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
                continue

            try:
                actual = _count_nodes_on_tenant(endpoint, username, password, tname)
            except Exception as exc:
                mismatches.append((tname, -1, expected[tname], f"count query failed: {exc}"))
                continue

            if actual != expected[tname]:
                mismatches.append((tname, actual, expected[tname], "COUNT MISMATCH"))
                print(f"  FAIL {tname}: actual={actual} expected={expected[tname]}", flush=True)
            else:
                print(f"  OK   {tname}: {actual} nodes", flush=True)
        finally:
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
    oom_observed = oom_counter[0]
    resume_failed_safe = resume_failed_safe_counter[0]

    print(f"    oom_observed        : {oom_observed}", flush=True)
    print(f"    suspends_ok         : {suspends_ok}", flush=True)
    print(
        f"    resumes_ok          : {resumes_ok}  "
        "(informational: RESUME on HOT tenant is idempotent no-op — not a hard gate)",
        flush=True,
    )
    print(f"    resume_failed_safe  : {resume_failed_safe}", flush=True)
    print(f"    total_committed     : {total_expected} nodes", flush=True)

    if resume_failed_safe == 0:
        print(
            "  [info] resume_failed_safe=0 on this run (no RESUME-under-OOM collision — that is OK)",
            flush=True,
        )

    vacuity_failures = []
    if oom_observed == 0:
        vacuity_failures.append(
            "oom_observed=0: the memory hog never tripped the ceiling. "
            "The ceiling (--memory-limit=1024) or the payload sizes are mis-tuned — the test is vacuous."
        )
    if suspends_ok == 0:
        vacuity_failures.append(
            "suspends_ok=0: the suspender never landed a successful SUSPEND on a churn tenant. "
            "Increase --duration-sec and re-run.  "
            "(With the ballast/churn split this should be reliable; "
            "if it still flaps, check that --num-tenants >= 2.)"
        )
    # resumes_ok is intentionally NOT a hard gate — see docstring above.

    if vacuity_failures:
        for msg in vacuity_failures:
            print(f"  FAIL: {msg}", file=sys.stderr, flush=True)
        sys.exit("FAIL: non-vacuity check failed — churn window did not exercise required conditions.")

    print("  non-vacuity: OK (OOM fired, SUSPEND fired)", flush=True)

    # -----------------------------------------------------------------------
    # Final verdict.
    # -----------------------------------------------------------------------
    print("\n" + "=" * 70, flush=True)
    if mismatches:
        print("RESULT: FAIL — data loss or integrity error detected", flush=True)
        for tname, actual, exp, reason in mismatches:
            print(f"  {tname}: {reason}  actual={actual} expected={exp}", flush=True)
        sys.exit(1)
    else:
        print("RESULT: PASS — all tenant counts match, no data loss", flush=True)
        print(
            f"  {len(tenant_names)} tenants verified ({len(ballast_tenants)} ballast + "
            f"{len(churn_tenants)} churn), {total_expected} nodes total, "
            f"oom_observed={oom_observed}, suspends_ok={suspends_ok}, "
            f"resumes_ok={resumes_ok} (info only), resume_failed_safe={resume_failed_safe}",
            flush=True,
        )
    print("=" * 70, flush=True)


if __name__ == "__main__":
    main()
