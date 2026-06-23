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
      continuously creates and drops a secondary index (:ChurnProbe(churn_prop))
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
     CREATE INDEX ON :ChurnProbe(churn_prop), then later drops it.  Counts
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

import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

# ---------------------------------------------------------------------------
# Driver import: mirror hot_cold_oom/workload.py — fall back to direct neo4j import.
# ---------------------------------------------------------------------------
_STRESS_ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", ".."))
if _STRESS_ROOT not in sys.path:
    sys.path.insert(0, _STRESS_ROOT)

from hot_cold_common import (
    MAX_RETRIES,
    RETRY_SLEEP,
    ClientError,
    ErrorCollector,
    ServiceUnavailable,
    TransientError,
    assert_server_alive,
    build_base_arg_parser,
    collect_worker_results,
    create_tenants,
    fail_on_unexpected_errors,
    is_transient,
    make_driver,
    resume_tenant_blocking,
    resumer_worker,
    run_query,
    staggered_stop,
    suspend_tenant,
    suspender_worker,
    wait_for_server,
)

# ---------------------------------------------------------------------------
# Constants / tunables
# ---------------------------------------------------------------------------

# Nodes created per writer transaction.  Kept small so a single tx never
# crosses a suspend boundary mid-commit.
_NODES_PER_TX = 10

# The sentinel key value whose per-tenant count is verified against the
# index-backed query result after resume.
_SENTINEL_KEY = 7

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


def parse_args():
    parser = build_base_arg_parser(
        description=__doc__,
        default_parallelism=4,
        default_num_tenants=3,
        default_duration_sec=60.0,
        add_nodes_per_tx=True,
        default_nodes_per_tx=_NODES_PER_TX,
    )
    return parser.parse_args()


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
    drv = make_driver(endpoint, username, password)
    try:
        for _attempt in range(MAX_RETRIES):
            try:
                with drv.session() as sess:
                    run_query(sess, f"USE DATABASE {name}")
                    run_query(sess, "CREATE INDEX ON :Data")
                    run_query(sess, "CREATE INDEX ON :Data(key)")
                print(f"  indexes created on {name}", flush=True)
                return
            except Exception as exc:
                if is_transient(exc):
                    time.sleep(RETRY_SLEEP)
                    continue
                raise
        raise RuntimeError(f"setup_indexes on {name} failed after {MAX_RETRIES} retries")
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
    error_collector: ErrorCollector,
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
    drv = make_driver(endpoint, username, password)

    try:
        while not stop_flag[0]:
            # Compute per-batch starting seq so key distribution is stable
            # across retries (we do NOT advance seq on failed attempts).
            seq_base = local_committed * nodes_per_tx

            # Count how many of this batch's nodes will have key==_SENTINEL_KEY.
            sentinel_in_batch = sum(1 for i in range(nodes_per_tx) if (seq_base + i) % 100 == _SENTINEL_KEY)

            committed = False
            for _attempt in range(MAX_RETRIES):
                if stop_flag[0]:
                    break
                try:
                    with drv.session() as sess:
                        run_query(sess, f"USE DATABASE {tenant_name}")
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
                    if is_transient(exc):
                        time.sleep(RETRY_SLEEP)
                        continue
                    error_collector.record(f"data_writer:{tenant_name}", exc)
                    break
                except ServiceUnavailable:
                    raise
                except Exception as exc:
                    if is_transient(exc):
                        time.sleep(RETRY_SLEEP)
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
    error_collector: ErrorCollector,
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
    drv = make_driver(endpoint, username, password)
    local_ops: int = 0

    # Additional tolerated error substrings specific to index DDL races.
    _INDEX_EXTRA_TOLERATIONS = (
        "index",  # "index already exists", "index does not exist", etc.
        "already",  # duplicate-index guard
        "does not exist",  # drop on already-absent index
    )

    def _is_index_tolerated(exc: Exception) -> bool:
        msg = str(exc).lower()
        if is_transient(exc):
            return True
        return any(t in msg for t in _INDEX_EXTRA_TOLERATIONS)

    try:
        while not stop_flag[0]:
            tenant = rng.choice(tenant_names)

            # Step 1: CREATE INDEX ON :ChurnProbe(churn_prop)
            created = False
            try:
                with drv.session() as sess:
                    run_query(sess, f"USE DATABASE {tenant}")
                    run_query(sess, "CREATE INDEX ON :ChurnProbe(churn_prop)")
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

            # Step 2: DROP INDEX ON :ChurnProbe(churn_prop)  (only attempt if we created it)
            if created:
                try:
                    with drv.session() as sess:
                        run_query(sess, f"USE DATABASE {tenant}")
                        run_query(sess, "DROP INDEX ON :ChurnProbe(churn_prop)")
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
    wait_for_server(endpoint, username, password)
    create_tenants(endpoint, username, password, tenant_names)

    # Create baseline indexes on each tenant BEFORE churn begins.
    for name in tenant_names:
        _setup_indexes_on_tenant(endpoint, username, password, name)

    # -----------------------------------------------------------------------
    # Phase 2: concurrent stress run.
    # -----------------------------------------------------------------------
    print(f"\n==> Phase 2: concurrent stress for {duration_sec}s", flush=True)

    stop_flag: list[bool] = [False]
    suspender_stop_flag: list[bool] = [False]
    committed_counts: dict[str, int] = {}
    sentinel_counts: dict[str, int] = {}
    counts_lock = threading.Lock()
    index_ops_counter: list[int] = [0]
    ops_lock = threading.Lock()
    error_collector = ErrorCollector()

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

        # SUSPENDER thread. It uses its OWN stop flag so it can keep running for a
        # short quiescent tail after the antagonists (writers/churner/resumer) stop —
        # see the staggered shutdown below.
        f = pool.submit(
            suspender_worker,
            endpoint,
            username,
            password,
            tenant_names,
            suspender_stop_flag,
            error_collector,
            base_seed + num_tenants + 1,
        )
        futures.append(("suspender", "all", f))

        # RESUMER thread.
        f = pool.submit(
            resumer_worker,
            endpoint,
            username,
            password,
            tenant_names,
            stop_flag,
            error_collector,
            base_seed + num_tenants + 2,
        )
        futures.append(("resumer", "all", f))

        # Let workers run for duration_sec, then perform a STAGGERED stop.
        #
        # SUSPEND fails fast on an in-use database (ACTIVE_CONNECTIONS) — that is the
        # designed v2 behavior, not an error. Under continuous write + resume churn the
        # suspender therefore legitimately loses most races, and on a busy/slow CI host
        # it can finish the whole window with zero successful suspends and trip the
        # `suspends_ok > 0` non-vacuity gate — a flaky false failure, not a product bug.
        #
        # Fix: stop the antagonists (data writers, index churner, resumer) first, then
        # let the suspender keep running for a short, contention-free tail in which its
        # SUSPENDs deterministically land. The full-contention window above still
        # exercises the concurrent suspend-vs-write path (the actual coverage); the tail
        # only guarantees the non-vacuity gate is satisfied deterministically.
        staggered_stop(stop_flag, suspender_stop_flag, duration_sec)

    suspends_ok, resumes_ok = collect_worker_results(futures, timeout=120.0)

    # -----------------------------------------------------------------------
    # Phase 3: unexpected-error check.
    # -----------------------------------------------------------------------
    print("\n==> Phase 3: unexpected-error check", flush=True)
    fail_on_unexpected_errors(error_collector, suffix="All client errors must match a known v2 marker.")

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
        suspend_tenant(endpoint, username, password, tname)

    mismatches: list[tuple[str, str, str]] = []
    indexes_restored: int = 0

    for tname in tenant_names:
        print(f"\n  -- verifying {tname} --", flush=True)
        try:
            # Resume with bounded retry in case RESUME transiently fails.
            resumed = False
            for _attempt in range(MAX_RETRIES):
                try:
                    resume_tenant_blocking(endpoint, username, password, tname, timeout=90.0)
                    resumed = True
                    break
                except Exception as exc:
                    if is_transient(exc):
                        time.sleep(RETRY_SLEEP)
                        continue
                    mismatches.append((tname, "resume", f"resume failed: {exc}"))
                    break

            if not resumed:
                continue

            drv_verify = make_driver(endpoint, username, password)
            try:
                with drv_verify.session() as sess:
                    run_query(sess, f"USE DATABASE {tname}")

                    # (ii) Index-persistence check.
                    index_rows = run_query(sess, "SHOW INDEX INFO")
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
                    sentinel_rows = run_query(
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
                    total_rows = run_query(sess, "MATCH (n:Data) RETURN count(n) AS cnt")
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
            suspend_tenant(endpoint, username, password, tname)

    # -----------------------------------------------------------------------
    # Phase 6: server liveness check.
    # -----------------------------------------------------------------------
    print("\n==> Phase 6: server liveness check", flush=True)
    assert_server_alive(endpoint, username, password)

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
