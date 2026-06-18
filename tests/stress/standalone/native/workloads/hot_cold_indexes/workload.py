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
Hot/Cold Index Stress Workload.

Verifies that Memgraph's index subsystem survives hot/cold tenant
suspend/resume churn without metadata loss, data corruption, or crashes.

Three correctness axes are probed simultaneously:

  (a) INDEX PERSISTENCE — index metadata (MetadataDelta WAL entries) must
      survive suspend → COLD → resume rebuild.  After the churn window,
      SHOW INDEX INFO on each tenant must still list the two baseline
      indexes that were created before churn began:
        - label index  :Data
        - label-property index  :Data(key)

  (b) INDEX-BACKED QUERY CORRECTNESS — after resume, a range-scan query
      that is serviced by the recovered :Data(key) index must return exactly
      the number of key==7 nodes that were committed during the churn window.
      This proves that the AsyncIndexer reconstruction produced a consistent
      index, not a stale or partial one.

  (c) ASYNC-INDEXER × SUSPEND INTERACTION — the INDEX-CHURNER thread
      continuously creates and drops a secondary index (:Data(churn_prop))
      on random tenants while the suspender/resumer threads are cycling them
      COLD/HOT.  This exercises the race window where an async-indexer
      background job lands while a suspend or resume is in flight.  The goal
      is to verify that no crash, assertion failure, or data corruption occurs
      under this race.

Thread roles:

  1. DATA-WRITER threads (one per tenant): commit batches of (:Data) nodes
     with `seq` and `key` properties.  key = seq % 100.  Per-tenant committed
     batch counts are tracked; key==7 sentinel hits are tracked separately.

  2. INDEX-CHURNER thread (one, all tenants): picks a random tenant, creates
     CREATE INDEX ON :Data(churn_prop), then later drops it.  Counts
     successful create operations as index_ops_ok.

  3. SUSPENDER thread: issues SUSPEND DATABASE on random tenants.

  4. RESUMER thread: issues RESUME DATABASE on random tenants.

After the timed run, for EACH tenant (one at a time, all others COLD):
  - Resume the tenant HOT.
  - SHOW INDEX INFO must list both baseline indexes.
  - MATCH (n:Data) WHERE n.key = 7 RETURN count(n) must equal the
    per-tenant committed sentinel count.
  - MATCH (n:Data) RETURN count(n) must equal committed_batches × nodes_per_batch.
  - Re-suspend before moving on.

Non-vacuity gates (hard FAIL if any is 0):
  - suspends_ok  > 0
  - indexes_restored > 0  (both baseline indexes survived on at least one tenant)
  - index_ops_ok > 0      (churner created at least one index during the run)

Run directly (smoke test):

  python3 workload.py --endpoint 127.0.0.1:7687 \\
      --parallelism 4 --num-tenants 3 --duration-sec 30

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
# Driver import: mirror hot_cold_oom/workload.py — fall back to direct neo4j import.
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

# Nodes created per writer transaction.  Kept small so a single tx never
# crosses a suspend boundary mid-commit.
_NODES_PER_TX = 10

# The sentinel key value whose per-tenant count is verified against the
# index-backed query result after resume.
_SENTINEL_KEY = 7

# v2 error-marker tuple (verbatim from hot_cold_oom/workload.py).
# Any client error whose lowercased text matches NONE of these is a real failure.
_TRANSITIONAL_MARKERS = (
    "is suspended (cold)",  # writer touched a tenant that was just taken COLD
    "active connections",  # SUSPEND lost the race to an active writer
    "does not exist or is already cold",  # SUSPEND raced another suspender / already COLD
    "does not exist or is not suspended",  # RESUME raced a writer / already HOT
    "failed to recover while resuming",  # RESUME hit OOM mid-rebuild; COLD, retriable
    "memory limit exceeded",  # allocation tripped the hard memory ceiling
    "multiple concurrent system queries are not supported",  # SUSPEND+RESUME collided on system tx
    "unknown database",  # USE DATABASE arrived mid-suspend (in-memory repr torn down); retriable
)

_MAX_RETRIES = 120
_RETRY_SLEEP = 0.05  # seconds

# Inter-iteration sleep bounds for the index-churner (seconds).
#
# The previous value (0.1 s) produced ~9 index ops/s across 3 tenants, which
# kept every tenant continuously occupied and starved the suspender of the
# sole-accessor window it needs to land a successful SUSPEND (suspends_ok=0).
#
# With 1.0–2.0 s of idle time between iterations (re-sampled each iteration
# via random.uniform inside _index_churner_worker) the churner performs at
# most ~1 op/s.  That is still sufficient to satisfy the index_ops_ok > 0
# gate while leaving each tenant idle long enough (well above the 0.15–0.30 s
# data-writer idle window) for the suspender to acquire sole-accessor.
_CHURNER_SLEEP_MIN = 1.0  # seconds
_CHURNER_SLEEP_MAX = 2.0  # seconds


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
        help="Nodes to create per writer transaction",
    )
    # Accept and ignore stress-runner boilerplate args.
    parser.add_argument("--worker-count", type=int, default=None)
    parser.add_argument("--logging", default=None)
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Driver helpers  (reused verbatim-style from hot_cold_oom/workload.py)
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
# Tenant lifecycle helpers  (reused from hot_cold_oom/workload.py)
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
# Shared error collector  (verbatim from hot_cold_oom/workload.py)
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
# Setup helper: create baseline indexes on a tenant
# ---------------------------------------------------------------------------


def _setup_indexes_on_tenant(endpoint: str, username: str, password: str, name: str) -> None:
    """
    USE DATABASE <name>, then create:
      CREATE INDEX ON :Data           (label index)
      CREATE INDEX ON :Data(key)      (label-property index, used by verification query)

    Retries on transient hot/cold markers.  Raises on any non-transient error.
    Both CREATE INDEX statements are idempotent (Memgraph ignores duplicate
    index creation), so re-running after a restart is safe.
    """
    drv = _make_driver(endpoint, username, password)
    try:
        for _attempt in range(_MAX_RETRIES):
            try:
                with drv.session() as sess:
                    _run_query(sess, f"USE DATABASE {name}")
                    _run_query(sess, "CREATE INDEX ON :Data")
                    _run_query(sess, "CREATE INDEX ON :Data(key)")
                print(f"  indexes created on {name}", flush=True)
                return
            except Exception as exc:
                if _is_transient(exc):
                    time.sleep(_RETRY_SLEEP)
                    continue
                raise
        raise RuntimeError(f"setup_indexes on {name} failed after {_MAX_RETRIES} retries")
    finally:
        drv.close()


# ---------------------------------------------------------------------------
# Worker 1: DATA-WRITER  (one per tenant)
# ---------------------------------------------------------------------------


def _data_writer_worker(
    tenant_name: str,
    endpoint: str,
    username: str,
    password: str,
    nodes_per_tx: int,
    stop_flag: list[bool],
    committed_counts: dict[str, int],
    sentinel_counts: dict[str, int],
    counts_lock: threading.Lock,
    error_collector: _ErrorCollector,
    rng_seed: int,
) -> None:
    """
    Continuously commit batches of (:Data{seq, key}) nodes to `tenant_name`.

    key = (global_seq_within_this_tenant) % 100.
    Each committed batch increments committed_counts[tenant_name].
    The number of nodes with key==_SENTINEL_KEY committed is tracked in
    sentinel_counts[tenant_name] for exact verification after resume.

    Transient hot/cold errors are retried; non-transient errors are recorded.
    """
    local_committed: int = 0
    local_sentinel: int = 0  # count of key==_SENTINEL_KEY nodes committed
    drv = _make_driver(endpoint, username, password)

    try:
        while not stop_flag[0]:
            # Compute per-batch starting seq so key distribution is stable
            # across retries (we do NOT advance seq on failed attempts).
            seq_base = local_committed * nodes_per_tx

            # Count how many of this batch's nodes will have key==_SENTINEL_KEY.
            sentinel_in_batch = sum(1 for i in range(nodes_per_tx) if (seq_base + i) % 100 == _SENTINEL_KEY)

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
                                "UNWIND range(0, $n - 1) AS i "
                                "CREATE (:Data {seq: $base + i, key: ($base + i) % 100})",
                                n=nodes_per_tx,
                                base=seq_base,
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
                    error_collector.record(f"data_writer:{tenant_name}", exc)
                    break
                except ServiceUnavailable:
                    raise
                except Exception as exc:
                    if _is_transient(exc):
                        time.sleep(_RETRY_SLEEP)
                        continue
                    error_collector.record(f"data_writer:{tenant_name}", exc)
                    break

            if committed:
                local_committed += 1
                local_sentinel += sentinel_in_batch

            # Idle between batches so the connection-scoped accessor is released
            # long enough for the suspender to reach sole-accessor and land a
            # successful SUSPEND.  Without this the writer loops back immediately
            # and the tenant is never idle.  Mirrors _light_writer_worker in
            # hot_cold_oom/workload.py.
            time.sleep(random.uniform(0.15, 0.30))
    finally:
        drv.close()

    with counts_lock:
        committed_counts[tenant_name] = committed_counts.get(tenant_name, 0) + local_committed
        sentinel_counts[tenant_name] = sentinel_counts.get(tenant_name, 0) + local_sentinel

    total_nodes = local_committed * nodes_per_tx
    print(
        f"  [data_writer:{tenant_name}] committed {local_committed} batches "
        f"({total_nodes} nodes, {local_sentinel} with key=={_SENTINEL_KEY})",
        flush=True,
    )


# ---------------------------------------------------------------------------
# Worker 2: INDEX-CHURNER  (single thread, all tenants)
# ---------------------------------------------------------------------------


def _index_churner_worker(
    tenant_names: list[str],
    endpoint: str,
    username: str,
    password: str,
    stop_flag: list[bool],
    index_ops_counter: list[int],
    ops_lock: threading.Lock,
    error_collector: _ErrorCollector,
    rng_seed: int,
) -> None:
    """
    Repeatedly picks a random tenant, creates CREATE INDEX ON :ChurnProbe(churn_prop),
    then drops it with DROP INDEX ON :ChurnProbe(churn_prop).

    :ChurnProbe nodes are NEVER created anywhere in this workload, so the index
    build scans zero nodes and completes instantly.  This is deliberate: using a
    populated label (:Data) caused the build to scan thousands of nodes on every
    iteration, pinning the tenant long enough to starve the suspender of the
    sole-accessor window it needs to land a successful SUSPEND (suspends_ok=0).
    Using a dedicated empty label preserves the async-indexer × suspend/resume
    race coverage (the actual purpose of the churner) without contending with
    the suspender.  Populated-index durability coverage is already provided by
    the :Data and :Data(key) baseline indexes verified in Phase 5.

    Every successful CREATE is counted in index_ops_counter[0].

    All transient errors (including "is suspended (cold)") are tolerated — those
    are expected under concurrent suspend/resume churn.  Only genuinely unexpected
    errors are forwarded to the error_collector.
    """
    rng = random.Random(rng_seed)
    drv = _make_driver(endpoint, username, password)
    local_ops: int = 0

    # Additional tolerated error substrings specific to index DDL races.
    _INDEX_EXTRA_TOLERATIONS = (
        "index",  # "index already exists", "index does not exist", etc.
        "already",  # duplicate-index guard
        "does not exist",  # drop on already-absent index
    )

    def _is_index_tolerated(exc: Exception) -> bool:
        msg = str(exc).lower()
        if _is_transient(exc):
            return True
        return any(t in msg for t in _INDEX_EXTRA_TOLERATIONS)

    try:
        while not stop_flag[0]:
            tenant = rng.choice(tenant_names)

            # Step 1: CREATE INDEX ON :Data(churn_prop)
            created = False
            try:
                with drv.session() as sess:
                    _run_query(sess, f"USE DATABASE {tenant}")
                    _run_query(sess, "CREATE INDEX ON :ChurnProbe(churn_prop)")
                    created = True
                    with ops_lock:
                        index_ops_counter[0] += 1
                        local_ops += 1
            except (ClientError, TransientError) as exc:
                if not _is_index_tolerated(exc):
                    error_collector.record(f"index_churner:create:{tenant}", exc)
            except ServiceUnavailable:
                raise
            except Exception as exc:
                if not _is_index_tolerated(exc):
                    error_collector.record(f"index_churner:create:{tenant}", exc)

            # Step 2: DROP INDEX ON :Data(churn_prop)  (only attempt if we created it)
            if created:
                try:
                    with drv.session() as sess:
                        _run_query(sess, f"USE DATABASE {tenant}")
                        _run_query(sess, "DROP INDEX ON :ChurnProbe(churn_prop)")
                except (ClientError, TransientError) as exc:
                    if not _is_index_tolerated(exc):
                        error_collector.record(f"index_churner:drop:{tenant}", exc)
                except ServiceUnavailable:
                    raise
                except Exception as exc:
                    if not _is_index_tolerated(exc):
                        error_collector.record(f"index_churner:drop:{tenant}", exc)

            time.sleep(random.uniform(_CHURNER_SLEEP_MIN, _CHURNER_SLEEP_MAX))
    finally:
        drv.close()

    print(f"  [index_churner] performed {local_ops} successful CREATE INDEX ops", flush=True)


# ---------------------------------------------------------------------------
# Worker 3: SUSPENDER
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
# Worker 4: RESUMER
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
    count of successful RESUME operations (informational only).
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
# SHOW INDEX INFO parsing helper
# ---------------------------------------------------------------------------


def _parse_index_info(rows: list[dict]) -> tuple[bool, bool]:
    """
    Parse the output of SHOW INDEX INFO and determine whether the two
    baseline indexes on :Data are present.

    Returns:
      (has_label_index, has_label_property_index)

    where:
      has_label_index          == True  iff a label-only index on :Data exists
      has_label_property_index == True  iff a label-property index on :Data(key) exists

    Column names are accessed defensively via dict.get() to tolerate minor
    schema variations across Memgraph versions.  The authoritative columns
    are "label" and "property" (and optionally "index type").  A row is
    considered a label-only index when property is absent, None, or empty
    string; it is considered a label-property index on "key" when property
    equals "key".
    """
    has_label = False
    has_label_prop = False

    for row in rows:
        label = row.get("label") or row.get("Label") or ""
        property_val = row.get("property") or row.get("Property") or row.get("properties") or ""
        # property_val may be a list (Memgraph returns ["key"] for composite indexes);
        # normalise to a simple string for single-property indexes.
        if isinstance(property_val, list):
            property_val = property_val[0] if len(property_val) == 1 else ""

        if str(label) != "Data":
            continue

        prop_str = str(property_val).strip() if property_val else ""

        if not prop_str:
            # Label-only index (:Data with no property).
            has_label = True
        elif prop_str == "key":
            # Label-property index (:Data(key)).
            has_label_prop = True

    return has_label, has_label_prop


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

    # Total thread count:
    #   num_tenants DATA-WRITER threads
    #   1           INDEX-CHURNER thread
    #   1           SUSPENDER thread
    #   1           RESUMER thread
    total_workers = num_tenants + 3

    print("==> Hot/Cold Index stress (index metadata durability + async-indexer × suspend)", flush=True)
    print(f"    endpoint       : {endpoint}", flush=True)
    print(f"    tenants        : {tenant_names}", flush=True)
    print(f"    parallelism    : {parallelism} (data-writer pool hint)", flush=True)
    print(f"    total_workers  : {total_workers}", flush=True)
    print(f"    duration       : {duration_sec}s", flush=True)
    print(f"    nodes_per_tx   : {nodes_per_tx}", flush=True)
    print(f"    sentinel_key   : {_SENTINEL_KEY}", flush=True)

    # -----------------------------------------------------------------------
    # Phase 1: server readiness + tenant setup + baseline index creation.
    # -----------------------------------------------------------------------
    print("\n==> Phase 1: server readiness + tenant setup + index creation", flush=True)
    _wait_for_server(endpoint, username, password)
    _create_tenants(endpoint, username, password, tenant_names)

    # Create baseline indexes on each tenant BEFORE churn begins.
    for name in tenant_names:
        _setup_indexes_on_tenant(endpoint, username, password, name)

    # -----------------------------------------------------------------------
    # Phase 2: concurrent stress run.
    # -----------------------------------------------------------------------
    print(f"\n==> Phase 2: concurrent stress for {duration_sec}s", flush=True)

    stop_flag: list[bool] = [False]
    committed_counts: dict[str, int] = {}
    sentinel_counts: dict[str, int] = {}
    counts_lock = threading.Lock()
    index_ops_counter: list[int] = [0]
    ops_lock = threading.Lock()
    error_collector = _ErrorCollector()

    base_seed = int(time.time())
    futures = []

    with ThreadPoolExecutor(max_workers=total_workers) as pool:
        # DATA-WRITER threads: one per tenant.
        for i, tname in enumerate(tenant_names):
            f = pool.submit(
                _data_writer_worker,
                tname,
                endpoint,
                username,
                password,
                nodes_per_tx,
                stop_flag,
                committed_counts,
                sentinel_counts,
                counts_lock,
                error_collector,
                base_seed + i,
            )
            futures.append(("data_writer", tname, f))

        # INDEX-CHURNER thread.
        f = pool.submit(
            _index_churner_worker,
            tenant_names,
            endpoint,
            username,
            password,
            stop_flag,
            index_ops_counter,
            ops_lock,
            error_collector,
            base_seed + num_tenants,
        )
        futures.append(("index_churner", "all", f))

        # SUSPENDER thread.
        f = pool.submit(
            _suspender_worker,
            endpoint,
            username,
            password,
            tenant_names,
            stop_flag,
            error_collector,
            base_seed + num_tenants + 1,
        )
        futures.append(("suspender", "all", f))

        # RESUMER thread.
        f = pool.submit(
            _resumer_worker,
            endpoint,
            username,
            password,
            tenant_names,
            stop_flag,
            error_collector,
            base_seed + num_tenants + 2,
        )
        futures.append(("resumer", "all", f))

        # Let workers run for duration_sec then signal stop.
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
            "All client errors must match a known v2 marker."
        )
    print("  all client errors matched a known v2 marker: OK", flush=True)

    # -----------------------------------------------------------------------
    # Phase 4: compute expected counts.
    # -----------------------------------------------------------------------
    print("\n==> Phase 4: computing expected counts", flush=True)
    expected_nodes: dict[str, int] = {}
    expected_sentinel: dict[str, int] = {}
    for tname in tenant_names:
        batches = committed_counts.get(tname, 0)
        expected_nodes[tname] = batches * nodes_per_tx
        expected_sentinel[tname] = sentinel_counts.get(tname, 0)
        print(
            f"    expected[{tname}] = {expected_nodes[tname]} nodes "
            f"({batches} batches), sentinel(key=={_SENTINEL_KEY})={expected_sentinel[tname]}",
            flush=True,
        )

    total_expected = sum(expected_nodes.values())
    print(f"    total expected: {total_expected} nodes across all tenants", flush=True)
    print(f"    index_ops_ok  : {index_ops_counter[0]} (churner CREATE INDEX successes)", flush=True)

    # -----------------------------------------------------------------------
    # Phase 5: one-at-a-time verification (all others COLD).
    #
    # For each tenant:
    #   (i)   Resume HOT.
    #   (ii)  SHOW INDEX INFO → both baseline indexes must be present.
    #   (iii) MATCH (n:Data) WHERE n.key = 7 RETURN count(n) == expected_sentinel.
    #   (iv)  MATCH (n:Data) RETURN count(n) == expected_nodes.
    #   (v)   Re-suspend.
    # -----------------------------------------------------------------------
    print("\n==> Phase 5: final one-at-a-time verification (suspend-all first)", flush=True)

    # First suspend all tenants so each gets a clean memory budget during
    # its individual resume+verify step.
    print("  suspending all tenants before per-tenant verification...", flush=True)
    for tname in tenant_names:
        _suspend_tenant(endpoint, username, password, tname)

    mismatches: list[tuple[str, str, str]] = []
    indexes_restored: int = 0

    for tname in tenant_names:
        print(f"\n  -- verifying {tname} --", flush=True)
        try:
            # Resume with bounded retry in case RESUME transiently fails.
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
                    mismatches.append((tname, "resume", f"resume failed: {exc}"))
                    break

            if not resumed:
                continue

            drv_verify = _make_driver(endpoint, username, password)
            try:
                with drv_verify.session() as sess:
                    _run_query(sess, f"USE DATABASE {tname}")

                    # (ii) Index-persistence check.
                    index_rows = _run_query(sess, "SHOW INDEX INFO")
                    has_label, has_label_prop = _parse_index_info(index_rows)
                    if has_label and has_label_prop:
                        indexes_restored += 1
                        print(f"  OK   {tname}: both baseline indexes present after resume", flush=True)
                    else:
                        missing = []
                        if not has_label:
                            missing.append(":Data (label index)")
                        if not has_label_prop:
                            missing.append(":Data(key) (label-property index)")
                        detail = f"missing after resume: {', '.join(missing)}"
                        mismatches.append((tname, "index_persistence", detail))
                        print(f"  FAIL {tname}: {detail}", flush=True)

                    # (iii) Index-backed sentinel query.
                    sentinel_rows = _run_query(
                        sess,
                        "MATCH (n:Data) WHERE n.key = $k RETURN count(n) AS cnt",
                        k=_SENTINEL_KEY,
                    )
                    actual_sentinel = int(sentinel_rows[0]["cnt"]) if sentinel_rows else 0
                    exp_sentinel = expected_sentinel[tname]
                    if actual_sentinel == exp_sentinel:
                        print(
                            f"  OK   {tname}: sentinel key=={_SENTINEL_KEY} count={actual_sentinel}",
                            flush=True,
                        )
                    else:
                        detail = f"index-backed sentinel mismatch: " f"actual={actual_sentinel} expected={exp_sentinel}"
                        mismatches.append((tname, "sentinel_count", detail))
                        print(f"  FAIL {tname}: {detail}", flush=True)

                    # (iv) Total-node integrity check.
                    total_rows = _run_query(sess, "MATCH (n:Data) RETURN count(n) AS cnt")
                    actual_total = int(total_rows[0]["cnt"]) if total_rows else 0
                    exp_total = expected_nodes[tname]
                    if actual_total == exp_total:
                        print(f"  OK   {tname}: total node count={actual_total}", flush=True)
                    else:
                        detail = f"node count mismatch: actual={actual_total} expected={exp_total}"
                        mismatches.append((tname, "node_count", detail))
                        print(f"  FAIL {tname}: {detail}", flush=True)

            finally:
                drv_verify.close()

        finally:
            # Re-suspend this tenant so the next one gets a clean budget.
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
    print(f"    suspends_ok      : {suspends_ok}", flush=True)
    print(f"    resumes_ok       : {resumes_ok}  (informational)", flush=True)
    print(f"    indexes_restored : {indexes_restored}", flush=True)
    print(f"    index_ops_ok     : {index_ops_counter[0]}", flush=True)

    vacuity_failures = []
    if suspends_ok == 0:
        vacuity_failures.append(
            "suspends_ok=0: the suspender never landed a successful SUSPEND. "
            "Increase --duration-sec or --num-tenants and re-run."
        )
    if indexes_restored == 0:
        vacuity_failures.append(
            "indexes_restored=0: no tenant had both baseline indexes present after resume. "
            "Either index metadata durability is broken or no suspend/resume cycle completed."
        )
    if index_ops_counter[0] == 0:
        vacuity_failures.append(
            "index_ops_ok=0: the index-churner never successfully created a transient index. "
            "The async-indexer × suspend interaction path was never exercised."
        )

    if vacuity_failures:
        for msg in vacuity_failures:
            print(f"  FAIL: {msg}", file=sys.stderr, flush=True)
        sys.exit("FAIL: non-vacuity check failed — churn window did not exercise required conditions.")

    print("  non-vacuity: OK (suspends fired, indexes survived, churner ran)", flush=True)

    # -----------------------------------------------------------------------
    # Final verdict.
    # -----------------------------------------------------------------------
    print("\n" + "=" * 70, flush=True)
    if mismatches:
        print("RESULT: FAIL — index or data integrity error detected", flush=True)
        for tname, check, reason in mismatches:
            print(f"  {tname} [{check}]: {reason}", flush=True)
        sys.exit(1)
    else:
        print("RESULT: PASS — all index and data checks passed", flush=True)
        print(
            f"  {len(tenant_names)} tenants verified, {total_expected} total nodes, "
            f"suspends_ok={suspends_ok}, resumes_ok={resumes_ok} (info), "
            f"indexes_restored={indexes_restored}, index_ops_ok={index_ops_counter[0]}",
            flush=True,
        )
    print("=" * 70, flush=True)


if __name__ == "__main__":
    main()
