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

from hot_cold_common import (
    DEFAULT_ENDPOINT,
    DEFAULT_PASSWORD,
    DEFAULT_USERNAME,
    MAX_RETRIES,
    RETRY_SLEEP,
    SUSPENDER_TAIL_SEC,
    TRANSITIONAL_MARKERS,
    ClientError,
    ErrorCollector,
    GraphDatabase,
    ServiceUnavailable,
    TransientError,
    build_base_arg_parser,
    count_nodes_on_tenant,
    create_tenants,
    is_transient,
    make_driver,
    reader_worker,
    resume_tenant_blocking,
    resumer_worker,
    run_query,
    run_with_retry,
    suspend_tenant,
    suspender_worker,
    wait_for_server,
)

# ---------------------------------------------------------------------------
# Trigger-specific constants
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def parse_args():
    parser = build_base_arg_parser(
        description=__doc__,
        default_parallelism=4,
        default_num_tenants=3,
        default_duration_sec=60,
        add_nodes_per_tx=True,
        default_nodes_per_tx=_NODES_PER_TX,
    )
    return parser.parse_args()


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
    drv = make_driver(endpoint, username, password)
    try:
        for _attempt in range(MAX_RETRIES):
            try:
                with drv.session() as sess:
                    run_query(sess, f"USE DATABASE {tenant_name}")
                    try:
                        run_query(sess, _TRIGGER_DDL)
                        print(f"  created trigger '{_TRIGGER_NAME}' on {tenant_name}", flush=True)
                    except Exception as exc:
                        msg = str(exc).lower()
                        if "already exists" in msg or "duplicate" in msg:
                            print(f"  trigger '{_TRIGGER_NAME}' already exists on {tenant_name}", flush=True)
                        else:
                            raise
                return
            except Exception as exc:
                if is_transient(exc):
                    time.sleep(RETRY_SLEEP)
                    continue
                raise
        raise RuntimeError(f"_setup_trigger on {tenant_name} failed after {MAX_RETRIES} retries")
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
    error_collector: ErrorCollector,
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
    MAX_RETRIES.  On non-transient client error, logs and skips the op.
    On ServiceUnavailable, raises (server gone).
    """
    rng = random.Random(rng_seed)
    local_counts: dict[str, int] = defaultdict(int)
    drv = make_driver(endpoint, username, password)

    try:
        while not stop_flag[0]:
            tenant = rng.choice(tenant_names)
            committed = False
            for _attempt in range(MAX_RETRIES):
                if stop_flag[0]:
                    break
                try:
                    with drv.session() as sess:
                        run_query(sess, f"USE DATABASE {tenant}")
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
                    if is_transient(exc):
                        time.sleep(RETRY_SLEEP)
                        continue
                    # Non-transient client error — record and skip this op.
                    error_collector.record(f"data_writer:{worker_id}:{tenant}", exc)
                    break
                except ServiceUnavailable:
                    raise
                except Exception as exc:
                    if is_transient(exc):
                        time.sleep(RETRY_SLEEP)
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
    wait_for_server(endpoint, username, password)
    create_tenants(endpoint, username, password, tenant_names)

    # Install the stamp_trigger on every tenant before the churn phase so that
    # every committed :Data node (including the very first batch) is stamped.
    for tname in tenant_names:
        _setup_trigger(endpoint, username, password, tname)

    # -----------------------------------------------------------------------
    # Phase 2: concurrent stress run.
    # -----------------------------------------------------------------------
    print(f"\n==> Phase 2: concurrent stress for {duration_sec}s", flush=True)

    stop_flag: list[bool] = [False]
    suspender_stop_flag: list[bool] = [False]
    committed_counts: dict[str, int] = {}
    counts_lock = threading.Lock()
    error_collector = ErrorCollector()

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

        # SUSPENDER thread. Uses its OWN stop flag for the staggered shutdown below.
        f = pool.submit(
            suspender_worker,
            endpoint,
            username,
            password,
            tenant_names,
            suspender_stop_flag,
            error_collector,
            base_seed + parallelism,
        )
        futures.append(("suspender", 0, f))

        # RESUMER thread.
        f = pool.submit(
            resumer_worker,
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
            reader_worker,
            0,
            endpoint,
            username,
            password,
            tenant_names,
            stop_flag,
            base_seed + parallelism + 2,
        )
        futures.append(("reader", 0, f))

        # Run for duration_sec, then STAGGER the stop: drain the antagonists (writers /
        # resumer) first, then give the suspender a short contention-free tail so its
        # SUSPENDs land deterministically (see SUSPENDER_TAIL_SEC). The contended window
        # above still exercises the concurrent suspend-vs-trigger path; the tail only
        # guarantees the suspends_ok>0 non-vacuity gate.
        time.sleep(duration_sec)
        stop_flag[0] = True
        print(
            f"  antagonists stopped; giving the suspender a {SUSPENDER_TAIL_SEC}s " "quiescent tail window...",
            flush=True,
        )
        time.sleep(SUSPENDER_TAIL_SEC)
        suspender_stop_flag[0] = True
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
        suspend_tenant(endpoint, username, password, tname)

    mismatches: list[tuple] = []
    triggers_restored: int = 0
    triggers_fired_after_resume: int = 0

    for tname in tenant_names:
        print(f"\n  verifying {tname}...", flush=True)
        resumed = False

        # Resume with bounded retry (tolerates transient errors).
        for _attempt in range(MAX_RETRIES):
            try:
                resume_tenant_blocking(endpoint, username, password, tname, timeout=90.0)
                resumed = True
                break
            except Exception as exc:
                if is_transient(exc):
                    time.sleep(RETRY_SLEEP)
                    continue
                mismatches.append((tname, -1, expected[tname], f"resume failed: {exc}"))
                break

        if not resumed:
            # Re-suspend best-effort before next iteration.
            suspend_tenant(endpoint, username, password, tname)
            continue

        drv_verify = make_driver(endpoint, username, password)
        try:
            # ---- Tooth 1: trigger-persistence ----
            # SHOW TRIGGERS must list stamp_trigger.
            try:
                with drv_verify.session() as sess:
                    run_query(sess, f"USE DATABASE {tname}")
                    rows = run_query(sess, "SHOW TRIGGERS")
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
                    run_query(sess, f"USE DATABASE {tname}")
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
                    run_query(sess, f"USE DATABASE {tname}")
                    rows = run_query(sess, "MATCH (n:Data {probe: true}) RETURN n.stamped AS stamped LIMIT 1")
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
                actual = count_nodes_on_tenant(endpoint, username, password, tname)
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
                    run_query(sess, f"USE DATABASE {tname}")
                    rows = run_query(
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
            suspend_tenant(endpoint, username, password, tname)

    # -----------------------------------------------------------------------
    # Phase 6: server liveness check.
    # -----------------------------------------------------------------------
    print("\n==> Phase 6: server liveness check", flush=True)
    try:
        drv_live = make_driver(endpoint, username, password)
        with drv_live.session() as sess:
            rows = run_query(sess, "RETURN 1 AS alive")
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
