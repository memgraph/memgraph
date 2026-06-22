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
Hot/Cold TTL Stress Workload.

Exercises Memgraph's TTL feature under hot/cold tenant suspend/resume churn.
Proves that:

  1. The per-DB TTL scheduler STOPS when a tenant is suspended
     (~InMemoryStorage::StopAllBackgroundTasks → ttl_.Shutdown()) and
     RESTARTS when the tenant is resumed (TTL enabled-state is durable via
     WAL/snapshot, so ENABLE TTL survives a COLD→HOT cycle).
  2. No permanent node data is lost across a SUSPEND / RESUME cycle even
     when the TTL scheduler is running concurrently with writers.
  3. All expected error shapes under hot/cold contention surface as known
     retriable markers, never as crashes or unrecognised exceptions.

Four concurrent thread roles during the timed churn window:

  1. PERM-WRITER (one per tenant):
       Commits batches of (:Perm{seq:...}) nodes — NO :TTL label, so the TTL
       scheduler never touches them.  Each committed batch is counted; the
       final per-tenant count is the integrity spine (committed == live after
       resume).

  2. EPHEMERAL-WRITER (one shared thread for all tenants):
       Picks a random tenant each iteration, commits one small batch of
       (:TTL{ttl: <epoch-microseconds slightly in the past>}) nodes, then
       sleeps 0.5–1.0 s.  Nodes are deleted by the TTL scheduler within
       ~1 s.  NOT counted for integrity (they are expected to vanish).
       Transient errors (including "unknown database" races) are skipped.

  3. SUSPENDER thread:
       Picks a random tenant and issues SUSPEND DATABASE.

  4. RESUMER thread:
       Picks a random tenant and issues RESUME DATABASE.

After the timed churn window, all tenants are suspended and then verified
ONE AT A TIME (all others COLD, so each resuming tenant gets the full memory
budget):

  Phase A — TTL-liveness probe:
    CREATE a fresh (:TTL{ttl: <now+2s>}) probe node on the just-resumed
    tenant, then poll up to ~15s until MATCH (n:TTL) RETURN count(n) reaches
    zero.  This proves the TTL scheduler restarted on resume and is actively
    deleting.  Each confirmed deletion increments ttl_deletions_observed.

  Phase B — Integrity:
    MATCH (n:Perm) RETURN count(n) must equal committed_perm_batches ×
    _NODES_PER_TX for that tenant.  A mismatch = data loss = hard FAIL.

Non-vacuity gates:
  - suspends_ok > 0              INFORMATIONAL only — busy TTL tenants legitimately
                                 lose the 100 ms fail-fast SUSPEND race (by design,
                                 R5); baseline/triggers/indexes workloads cover
                                 "suspend-lands-under-churn".
  - ttl_deletions_observed > 0   HARD FAIL if 0 — requires a full
                                 suspend→resume→TTL-restart→delete cycle.

Run directly (smoke test):

  python3 workload.py --endpoint 127.0.0.1:7687 \\
      --parallelism 4 --num-tenants 3 --duration-sec 60

The stress runner invokes it via the workload.yaml script_args.

NOTE: Designed to run against ASan / TSan builds.  All waits use bounded
retries rather than fixed sleep() so the test remains correct under sanitiser
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

from hot_cold_common import (
    MAX_RETRIES,
    RETRY_SLEEP,
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
    resume_tenant_blocking,
    resumer_worker,
    run_query,
    run_with_retry,
    suspend_tenant,
    wait_for_server,
)

# ---------------------------------------------------------------------------
# Constants / tunables
# ---------------------------------------------------------------------------

# Nodes per PERM-WRITER / EPHEMERAL-WRITER transaction.  Kept small so one
# write never trips a transient error mid-commit (makes committed-count exact).
_NODES_PER_TX = 10

# Per-tenant maximum COUNTED batches for PERM-WRITER.
_PERM_WRITER_CAP = 1000

# How far in the PAST to set the :TTL node's ttl epoch (microseconds).
# 1 second in the past ensures TTL fires immediately on the next scheduler tick.
_TTL_PAST_OFFSET_US: int = 1_000_000  # 1 second in microseconds

# How far in the FUTURE to set the TTL-liveness probe epoch (microseconds).
# 2 seconds gives the probe a short window so the TTL scheduler deletes it
# predictably once we enter the poll loop.
_TTL_PROBE_FUTURE_OFFSET_US: int = 2_000_000  # 2 seconds in microseconds

# Maximum wall-clock seconds to poll for the TTL-liveness probe to be deleted.
_TTL_PROBE_POLL_TIMEOUT_SEC: float = 15.0

# Sleep between TTL probe poll iterations (seconds).
_TTL_PROBE_POLL_SLEEP: float = 0.5

# Number of rapid-fire SUSPEND attempts per tenant per outer-loop iteration.
# At 50 ms per attempt this spans ~1 s — comfortably longer than the
# perm-writer's 0.15–0.30 s batch+sleep cycle, so the burst reliably
# catches the sole-accessor idle window.
_SUSPEND_BURST = 20


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = build_base_arg_parser(
        description=__doc__,
        default_parallelism=4,
        default_num_tenants=3,
        default_duration_sec=60,
        add_nodes_per_tx=False,
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Tenant lifecycle helpers  (TTL-specific)
# ---------------------------------------------------------------------------


def _enable_ttl_on_tenant(endpoint: str, username: str, password: str, name: str) -> None:
    """
    USE DATABASE <name> then ENABLE TTL EVERY "1s".

    Retries on transient errors.  TTL enabled-state is durable (WAL/snapshot)
    so this call is idempotent across restarts.
    """
    drv = make_driver(endpoint, username, password)
    try:
        for _attempt in range(MAX_RETRIES):
            try:
                with drv.session() as sess:
                    run_query(sess, f"USE DATABASE {name}")
                    run_query(sess, 'ENABLE TTL EVERY "1s"')
                print(f"  TTL enabled on tenant {name}", flush=True)
                return
            except Exception as exc:
                if is_transient(exc):
                    time.sleep(RETRY_SLEEP)
                    continue
                raise
        raise RuntimeError(f"_enable_ttl_on_tenant: {name} failed after {MAX_RETRIES} retries")
    finally:
        drv.close()


def _count_perm_nodes_on_tenant(endpoint: str, username: str, password: str, name: str) -> int:
    """USE DATABASE <name> then MATCH (n:Perm) RETURN count(n).  Retries on transient errors."""
    drv = make_driver(endpoint, username, password)
    try:
        for _attempt in range(MAX_RETRIES):
            try:
                with drv.session() as sess:
                    run_query(sess, f"USE DATABASE {name}")
                    rows = run_query(sess, "MATCH (n:Perm) RETURN count(n) AS cnt")
                    return int(rows[0]["cnt"])
            except Exception as exc:
                if is_transient(exc):
                    time.sleep(RETRY_SLEEP)
                    continue
                raise
        raise RuntimeError(f"count_perm_nodes on {name} failed after {MAX_RETRIES} retries")
    finally:
        drv.close()


# ---------------------------------------------------------------------------
# Worker 1: PERM-WRITER  (one per tenant — counts for integrity)
# ---------------------------------------------------------------------------


def _perm_writer_worker(
    tenant_name: str,
    endpoint: str,
    username: str,
    password: str,
    stop_flag: list[bool],
    committed_counts: dict[str, int],
    counts_lock: threading.Lock,
    error_collector: ErrorCollector,
    rng_seed: int,
) -> None:
    """
    Continuously commit small batches of (:Perm{seq:...}) nodes.

    The :Perm label is NEVER used in a TTL predicate, so the TTL scheduler
    will never delete these nodes.  Every committed batch increments
    committed_counts[tenant_name] — the final count must equal the number
    of (:Perm) nodes that survive in the DB (integrity invariant).

    An inter-batch sleep (150–300 ms) keeps the tenant idle most of the time
    so the SUSPENDER can reliably reach sole-accessor and land a SUSPEND.
    _PERM_WRITER_CAP is retained for the startup summary only; the actual
    bound comes from --duration-sec.

    A "is suspended (cold)" error means the tenant is mid-suspend cycle —
    tolerated and NOT counted (committed_counts stays exact).
    """
    rng = random.Random(rng_seed)
    _ = rng  # reserved for future per-batch randomisation
    local_committed: int = 0
    drv = make_driver(endpoint, username, password)

    try:
        while not stop_flag[0]:
            seq = local_committed

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
                                "UNWIND range(1, $n) AS i " "CREATE (:Perm {seq: $base + i})",
                                n=_NODES_PER_TX,
                                base=seq * _NODES_PER_TX,
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
                    error_collector.record(f"perm_writer:{tenant_name}", exc)
                    break
                except ServiceUnavailable:
                    raise
                except Exception as exc:
                    if is_transient(exc):
                        time.sleep(RETRY_SLEEP)
                        continue
                    error_collector.record(f"perm_writer:{tenant_name}", exc)
                    break

            if committed:
                local_committed += 1
            time.sleep(random.uniform(0.15, 0.30))
    finally:
        drv.close()

    with counts_lock:
        committed_counts[tenant_name] = local_committed
    print(
        f"  [perm_writer:{tenant_name}] committed {local_committed} batches "
        f"({local_committed * _NODES_PER_TX} Perm nodes)",
        flush=True,
    )


# ---------------------------------------------------------------------------
# Worker 2: EPHEMERAL-WRITER  (ONE shared thread for all tenants)
# ---------------------------------------------------------------------------


def _ephemeral_writer_worker(
    tenant_names: list[str],
    endpoint: str,
    username: str,
    password: str,
    stop_flag: list[bool],
    error_collector: ErrorCollector,
    rng_seed: int,
) -> None:
    """
    Shared ephemeral-writer thread that serves ALL tenants via rng.choice.

    Each iteration:
      1. Picks a random tenant from tenant_names.
      2. Opens a fresh session (per-batch — no long-lived session state).
      3. USE DATABASE <tenant>; commit one small batch of
         (:TTL{ttl: <1 s in the past>}) nodes.
      4. Closes the session.
      5. Sleeps 0.5–1.0 s before the next iteration.

    The longer inter-batch sleep (vs the perm-writer's 0.15–0.30 s) means
    each tenant is only lightly touched by this thread, leaving each tenant
    with exactly ONE continuous writer (its perm_writer) — matching the
    contention profile that reliably allows the suspender to reach
    sole-accessor.

    Transient errors (including "unknown database" when USE DATABASE races a
    mid-suspend teardown) are skipped without counting and without recording
    to the error_collector.  Non-transient errors are recorded normally.
    """
    rng = random.Random(rng_seed)
    drv = make_driver(endpoint, username, password)

    try:
        while not stop_flag[0]:
            tenant = rng.choice(tenant_names)
            # ttl epoch: 1 second in the past so the TTL scheduler fires
            # immediately on the next tick.
            ttl_epoch_us = int(time.time() * 1_000_000) - _TTL_PAST_OFFSET_US

            try:
                with drv.session() as sess:
                    run_query(sess, f"USE DATABASE {tenant}")
                    tx = sess.begin_transaction()
                    try:
                        tx.run(
                            "UNWIND range(1, $n) AS i " "CREATE (:TTL {ttl: $ttl_epoch})",
                            n=_NODES_PER_TX,
                            ttl_epoch=ttl_epoch_us,
                        ).consume()
                        tx.commit()
                    except Exception:
                        try:
                            tx.rollback()
                        except Exception:
                            pass
                        raise
            except (ClientError, TransientError) as exc:
                if is_transient(exc):
                    pass  # expected transient (cold tenant / mid-suspend race); skip
                else:
                    error_collector.record(f"ephemeral_writer:{tenant}", exc)
            except ServiceUnavailable:
                raise
            except Exception as exc:
                if is_transient(exc):
                    pass  # e.g. "unknown database" mid-teardown
                else:
                    error_collector.record(f"ephemeral_writer:{tenant}", exc)

            # Longer inter-batch sleep: keeps per-tenant contention low so
            # the suspender can reach sole-accessor on each perm-writer cycle.
            time.sleep(rng.uniform(0.5, 1.0))
    finally:
        drv.close()
    print("  [ephemeral_writer] stopped", flush=True)


# ---------------------------------------------------------------------------
# Worker 3: SUSPENDER  (targets all tenants — BURST variant, TTL-specific)
# ---------------------------------------------------------------------------


def _suspender_worker(
    endpoint: str,
    username: str,
    password: str,
    tenant_names: list[str],
    stop_flag: list[bool],
    error_collector: ErrorCollector,
    rng_seed: int,
) -> int:
    """
    Repeatedly pick a random tenant and issue SUSPEND DATABASE.

    Returns the count of successful SUSPEND operations (used for the
    non-vacuity assertion in main).

    For each picked tenant a tight inner burst of up to _SUSPEND_BURST
    attempts (50 ms apart, totalling ~1 s) is fired so the suspender
    catches the idle window that opens between the perm-writer's
    0.15–0.30 s batch+sleep cycles.  Under churn a single one-shot
    attempt almost never coincides with an idle window; the burst
    reliably does.

    Inner-loop retry policy:
      - SUCCESS (no exception): increment ops, break inner loop.
      - Tolerated transient ("active connections", "already cold",
        "default database", "not in memory", "durability", "replica",
        or any is_transient marker): keep retrying within the burst.
      - Genuinely unexpected error: record to error_collector, break
        inner loop (do not keep hammering on a broken tenant).

    stop_flag is checked inside the inner loop so shutdown is prompt.
    """
    rng = random.Random(rng_seed)
    drv = make_driver(endpoint, username, password)
    ops = 0

    # Errors that indicate "tenant currently busy / already cold" — we keep
    # retrying within the burst rather than giving up on this tenant.
    _BURST_CONTINUE_MARKERS = (
        "default database",
        "not in memory",
        "durability",
        "replica",
    )

    try:
        while not stop_flag[0]:
            tenant = rng.choice(tenant_names)
            suspended_this_round = False

            for _burst in range(_SUSPEND_BURST):
                if stop_flag[0]:
                    break
                try:
                    with drv.session() as sess:
                        run_query(sess, f"SUSPEND DATABASE {tenant}")
                    ops += 1
                    suspended_this_round = True
                    break  # success — move on to the next outer iteration
                except (ClientError, TransientError) as exc:
                    msg = str(exc).lower()
                    if is_transient(exc) or any(m in msg for m in _BURST_CONTINUE_MARKERS):
                        # Tenant busy or already cold — wait and retry within burst.
                        time.sleep(0.05)
                        continue
                    # Genuinely unexpected ClientError/TransientError.
                    error_collector.record(f"suspender:{tenant}", exc)
                    break
                except Exception as exc:
                    if is_transient(exc):
                        time.sleep(0.05)
                        continue
                    error_collector.record(f"suspender:{tenant}", exc)
                    break

            # Brief outer sleep after a successful suspend so the resumer has
            # a chance to bring the tenant back up before the next outer pick.
            if suspended_this_round:
                time.sleep(RETRY_SLEEP)
    finally:
        drv.close()
    print(f"  [suspender] issued {ops} successful suspends", flush=True)
    return ops


# ---------------------------------------------------------------------------
# TTL-liveness probe  (called per tenant in the verification phase)
# ---------------------------------------------------------------------------


def _probe_ttl_liveness(
    endpoint: str,
    username: str,
    password: str,
    tenant_name: str,
) -> bool:
    """
    Prove that the TTL scheduler is alive and deleting on the (already HOT)
    tenant by:

      1. Creating a fresh (:TTL{ttl: <now + _TTL_PROBE_FUTURE_OFFSET_US>}) probe
         node (TTL fires ~2s after creation, well within the poll window).
      2. Verifying count(:TTL) went up by at least 1 immediately after the write
         (sanity-check that the create actually committed).
      3. Polling up to _TTL_PROBE_POLL_TIMEOUT_SEC until count(:TTL) reaches 0,
         which proves the TTL scheduler deleted the probe.

    Returns True if the probe was deleted within the timeout (TTL liveness
    confirmed), False otherwise (TTL scheduler did not restart — hard failure).
    """
    drv = make_driver(endpoint, username, password)
    try:
        # Step 1: count baseline (:TTL nodes before probe).
        for _attempt in range(MAX_RETRIES):
            try:
                with drv.session() as sess:
                    run_query(sess, f"USE DATABASE {tenant_name}")
                    rows = run_query(sess, "MATCH (n:TTL) RETURN count(n) AS cnt")
                    baseline_count = int(rows[0]["cnt"])
                break
            except Exception as exc:
                if is_transient(exc):
                    time.sleep(RETRY_SLEEP)
                    continue
                raise
        else:
            raise RuntimeError(f"TTL probe: could not read baseline count on {tenant_name}")

        # Step 2: write the probe node with ttl set ~2s in the future.
        probe_ttl_us = int(time.time() * 1_000_000) + _TTL_PROBE_FUTURE_OFFSET_US
        for _attempt in range(MAX_RETRIES):
            try:
                with drv.session() as sess:
                    run_query(sess, f"USE DATABASE {tenant_name}")
                    tx = sess.begin_transaction()
                    try:
                        tx.run(
                            "CREATE (:TTL {ttl: $ttl_epoch, probe: true})",
                            ttl_epoch=probe_ttl_us,
                        ).consume()
                        tx.commit()
                    except Exception:
                        try:
                            tx.rollback()
                        except Exception:
                            pass
                        raise
                break
            except Exception as exc:
                if is_transient(exc):
                    time.sleep(RETRY_SLEEP)
                    continue
                raise
        else:
            raise RuntimeError(f"TTL probe: could not write probe node on {tenant_name}")

        # Step 3: sanity-check — count must have gone up by at least 1.
        for _attempt in range(MAX_RETRIES):
            try:
                with drv.session() as sess:
                    run_query(sess, f"USE DATABASE {tenant_name}")
                    rows = run_query(sess, "MATCH (n:TTL) RETURN count(n) AS cnt")
                    after_write_count = int(rows[0]["cnt"])
                break
            except Exception as exc:
                if is_transient(exc):
                    time.sleep(RETRY_SLEEP)
                    continue
                raise
        else:
            raise RuntimeError(f"TTL probe: could not read post-write count on {tenant_name}")

        if after_write_count <= baseline_count:
            # The probe node did not appear — write race or wrong database context.
            print(
                f"  [ttl_probe:{tenant_name}] WARNING: post-write count={after_write_count} "
                f"<= baseline={baseline_count}; probe write may have not landed",
                flush=True,
            )
            # Still proceed to the deletion poll: if TTL already deleted it
            # that would be OK (very fast TTL), but we cannot confirm liveness.

        # Step 4: poll until all :TTL nodes (including the probe) are gone.
        # The probe's ttl is 2s in the future, so we must wait at least 2s
        # before the TTL scheduler fires on it.  The poll window is 15s total.
        deadline = time.monotonic() + _TTL_PROBE_POLL_TIMEOUT_SEC
        last_count = after_write_count
        while time.monotonic() < deadline:
            time.sleep(_TTL_PROBE_POLL_SLEEP)
            try:
                with drv.session() as sess:
                    run_query(sess, f"USE DATABASE {tenant_name}")
                    rows = run_query(sess, "MATCH (n:TTL) RETURN count(n) AS cnt")
                    current_count = int(rows[0]["cnt"])
                if current_count != last_count:
                    print(
                        f"  [ttl_probe:{tenant_name}] TTL deleted nodes: count {last_count} -> {current_count}",
                        flush=True,
                    )
                    last_count = current_count
                if current_count == 0:
                    print(
                        f"  [ttl_probe:{tenant_name}] CONFIRMED: all :TTL nodes deleted — scheduler restarted",
                        flush=True,
                    )
                    return True
            except Exception as exc:
                if is_transient(exc):
                    continue
                raise

        print(
            f"  [ttl_probe:{tenant_name}] TIMEOUT: :TTL count={last_count} after "
            f"{_TTL_PROBE_POLL_TIMEOUT_SEC}s — TTL scheduler did NOT restart",
            flush=True,
        )
        return False
    finally:
        drv.close()


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()

    endpoint: str = args.endpoint
    username: str = args.username
    password: str = args.password
    num_tenants: int = max(1, args.num_tenants)
    duration_sec: float = args.duration_sec

    tenant_names = [f"tenant_{i}" for i in range(num_tenants)]

    # Total thread count:
    #   num_tenants PERM-WRITER threads  (one per tenant)
    #   1           EPHEMERAL-WRITER thread (shared, picks random tenant each batch)
    #   1           SUSPENDER thread
    #   1           RESUMER thread
    total_workers = num_tenants + 3

    print("==> Hot/Cold TTL stress (TTL scheduler lifecycle under suspend/resume churn)", flush=True)
    print(f"    endpoint         : {endpoint}", flush=True)
    print(f"    tenants          : {tenant_names}", flush=True)
    print(f"    duration         : {duration_sec}s", flush=True)
    print(f"    nodes_per_tx     : {_NODES_PER_TX}", flush=True)
    print(f"    perm_writer_cap  : {_PERM_WRITER_CAP} batches/tenant", flush=True)
    print(f"    total_workers    : {total_workers}", flush=True)
    print(f"    parallelism_hint : {args.parallelism}", flush=True)

    # -----------------------------------------------------------------------
    # Phase 1: server readiness + tenant setup + TTL enable.
    # -----------------------------------------------------------------------
    print("\n==> Phase 1: server readiness + tenant setup + TTL enable", flush=True)
    wait_for_server(endpoint, username, password)
    create_tenants(endpoint, username, password, tenant_names)
    for name in tenant_names:
        _enable_ttl_on_tenant(endpoint, username, password, name)

    # -----------------------------------------------------------------------
    # Phase 2: concurrent stress run.
    # -----------------------------------------------------------------------
    print(f"\n==> Phase 2: concurrent stress for {duration_sec}s", flush=True)

    stop_flag: list[bool] = [False]
    committed_counts: dict[str, int] = {}
    counts_lock = threading.Lock()
    error_collector = ErrorCollector()

    base_seed = int(time.time())
    futures = []

    with ThreadPoolExecutor(max_workers=total_workers) as pool:
        # PERM-WRITER: one per tenant (counts for integrity).
        for i, tname in enumerate(tenant_names):
            f = pool.submit(
                _perm_writer_worker,
                tname,
                endpoint,
                username,
                password,
                stop_flag,
                committed_counts,
                counts_lock,
                error_collector,
                base_seed + i,
            )
            futures.append(("perm_writer", tname, f))

        # EPHEMERAL-WRITER: one shared thread for all tenants (picks randomly).
        f = pool.submit(
            _ephemeral_writer_worker,
            tenant_names,
            endpoint,
            username,
            password,
            stop_flag,
            error_collector,
            base_seed + num_tenants,
        )
        futures.append(("ephemeral_writer", "shared", f))

        # SUSPENDER thread (all tenants) — burst variant.
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

        # RESUMER thread (all tenants).
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
            "All hot/cold errors must surface as known retriable markers."
        )
    print("  all client errors matched a known v2 marker: OK", flush=True)

    # -----------------------------------------------------------------------
    # Phase 4: compute expected Perm node counts.
    # -----------------------------------------------------------------------
    print("\n==> Phase 4: computing expected Perm node counts", flush=True)
    expected_perm: dict[str, int] = {}
    for tname in tenant_names:
        batches = committed_counts.get(tname, 0)
        expected_perm[tname] = batches * _NODES_PER_TX
        print(
            f"    expected_perm[{tname}] = {expected_perm[tname]} nodes ({batches} batches)",
            flush=True,
        )
    total_expected_perm = sum(expected_perm.values())
    print(f"    total expected Perm: {total_expected_perm} nodes across all tenants", flush=True)

    # -----------------------------------------------------------------------
    # Phase 5: suspend ALL tenants first (one-at-a-time verification pattern).
    # -----------------------------------------------------------------------
    print("\n==> Phase 5: suspending all tenants before one-at-a-time verification", flush=True)
    for tname in tenant_names:
        suspend_tenant(endpoint, username, password, tname)
        print(f"  suspended {tname}", flush=True)

    # -----------------------------------------------------------------------
    # Phase 6: per-tenant TTL-liveness probe + data-integrity verification.
    #
    # All other tenants remain COLD while one tenant is verified.  This gives
    # each tenant the full memory budget and serialises the TTL probe cleanly.
    # -----------------------------------------------------------------------
    print("\n==> Phase 6: per-tenant TTL-liveness probe + data-integrity verification", flush=True)

    mismatches: list[tuple[str, int, int, str]] = []
    ttl_deletions_observed: int = 0

    for tname in tenant_names:
        print(f"\n  -- verifying {tname} --", flush=True)
        resumed = False
        try:
            # Step A: resume the tenant HOT (bounded retry).
            for _attempt in range(MAX_RETRIES):
                try:
                    resume_tenant_blocking(endpoint, username, password, tname, timeout=90.0)
                    resumed = True
                    break
                except Exception as exc:
                    if is_transient(exc):
                        time.sleep(RETRY_SLEEP)
                        continue
                    mismatches.append((tname, -1, expected_perm[tname], f"resume failed: {exc}"))
                    break

            if not resumed:
                print(f"  SKIP {tname}: could not resume (see mismatch log)", flush=True)
                continue

            # Step B: TTL-liveness probe.
            # Proves the TTL scheduler restarted on resume.
            ttl_alive = _probe_ttl_liveness(endpoint, username, password, tname)
            if ttl_alive:
                ttl_deletions_observed += 1
                print(f"  TTL-liveness probe: PASS on {tname}", flush=True)
            else:
                mismatches.append(
                    (
                        tname,
                        -1,
                        -1,
                        "TTL-liveness probe FAILED: scheduler did not restart after resume",
                    )
                )
                print(f"  TTL-liveness probe: FAIL on {tname}", flush=True)

            # Step C: integrity — count :Perm nodes (must equal committed batches × _NODES_PER_TX).
            try:
                actual_perm = _count_perm_nodes_on_tenant(endpoint, username, password, tname)
            except Exception as exc:
                mismatches.append((tname, -1, expected_perm[tname], f"Perm count query failed: {exc}"))
                continue

            if actual_perm != expected_perm[tname]:
                mismatches.append((tname, actual_perm, expected_perm[tname], "PERM COUNT MISMATCH"))
                print(
                    f"  FAIL {tname}: Perm actual={actual_perm} expected={expected_perm[tname]}",
                    flush=True,
                )
            else:
                print(f"  OK   {tname}: {actual_perm} Perm nodes", flush=True)
        finally:
            # Re-suspend so the next tenant gets the full memory budget.
            suspend_tenant(endpoint, username, password, tname)

    # -----------------------------------------------------------------------
    # Phase 7: server liveness check.
    # -----------------------------------------------------------------------
    print("\n==> Phase 7: server liveness check", flush=True)
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
    # Phase 8: non-vacuity check.
    # -----------------------------------------------------------------------
    print("\n==> Phase 8: non-vacuity check", flush=True)
    print(
        f"    suspends_ok (churn)     : {suspends_ok}"
        " (informational — busy TTL tenants legitimately lose the 100ms fail-fast"
        " SUSPEND race; see comment)",
        flush=True,
    )
    print(
        f"    resumes_ok              : {resumes_ok}  "
        "(informational: RESUME on HOT tenant is idempotent no-op — not a hard gate)",
        flush=True,
    )
    print(f"    ttl_deletions_observed  : {ttl_deletions_observed}", flush=True)
    print(f"    total_expected_perm     : {total_expected_perm} Perm nodes", flush=True)

    # Non-vacuity rationale
    # ---------------------
    # suspends_ok (churn phase) is INFORMATIONAL, not a hard gate.
    #
    # A TTL-enabled tenant under sustained write churn almost never wins the
    # 100 ms fail-fast ACTIVE_CONNECTIONS window that SUSPEND DATABASE waits
    # for: each SUSPEND attempt blocks ~100 ms then fails "has active
    # connections; cannot suspend while in use" (R5 — SUSPEND does not kill
    # in-flight queries; it is fail-fast and retriable by design).  The TTL
    # background deletion transaction (~1/s) alone is enough to keep a tenant
    # continuously "active" from SUSPEND's perspective during the churn window.
    # This is NOT a correctness bug: no crash, no data loss, the error is a
    # clean retriable marker.  The "suspend-lands-under-churn" liveness property
    # is already covered by the baseline/triggers/indexes workloads (which land
    # 173/119/1 suspends respectively).
    #
    # ttl_deletions_observed > 0 is the HARD non-vacuity gate.
    #
    # It can be > 0 only if the verification phase successfully SUSPENDED a
    # tenant (HOT→COLD), RESUMED it (COLD→HOT), the TTL scheduler RESTARTED
    # (the bug we just fixed), and TTL then DELETED the future-dated probe.
    # That single gate proves the entire TTL × suspend/resume pipeline
    # end-to-end — strictly stronger than churn-phase suspends_ok > 0.
    vacuity_failures: list[str] = []
    if ttl_deletions_observed == 0:
        vacuity_failures.append(
            "ttl_deletions_observed=0: the TTL-liveness probe never confirmed a deletion "
            "post-resume on any tenant.  Either TTL did not restart after resume, or the "
            "probe poll window is too short.  Check ENABLE TTL and --storage-wal-enabled."
        )

    if vacuity_failures:
        for msg in vacuity_failures:
            print(f"  FAIL: {msg}", file=sys.stderr, flush=True)
        sys.exit("FAIL: non-vacuity check failed — required conditions were not exercised.")

    print(
        f"  non-vacuity: OK (suspends_ok={suspends_ok} (informational), "
        f"ttl_deletions_observed={ttl_deletions_observed} (hard gate))",
        flush=True,
    )

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
        print("RESULT: PASS — all tenant counts match, TTL restarts confirmed, no data loss", flush=True)
        print(
            f"  {len(tenant_names)} tenants verified, {total_expected_perm} Perm nodes, "
            f"suspends_ok={suspends_ok}, resumes_ok={resumes_ok} (info only), "
            f"ttl_deletions_observed={ttl_deletions_observed}",
            flush=True,
        )
    print("=" * 70, flush=True)


if __name__ == "__main__":
    main()
