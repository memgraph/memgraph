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

import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

# ---------------------------------------------------------------------------
# Shared scaffolding bootstrap: resolve the stress-tests root and pull in
# hot_cold_common which re-exports the neo4j driver and all shared helpers.
# ---------------------------------------------------------------------------
_STRESS_ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", ".."))
if _STRESS_ROOT not in sys.path:
    sys.path.insert(0, _STRESS_ROOT)

from hot_cold_common import (  # noqa: E402
    MAX_RETRIES,
    RETRY_SLEEP,
    ClientError,
    ErrorCollector,
    ServiceUnavailable,
    TransientError,
    assert_server_alive,
    build_base_arg_parser,
    collect_worker_results,
    count_nodes_on_tenant,
    create_tenants,
    fail_on_unexpected_errors,
    is_transient,
    make_driver,
    resume_tenant_blocking,
    run_query,
    staggered_stop,
    suspend_tenant,
    suspender_worker,
    wait_for_server,
)

# ---------------------------------------------------------------------------
# OOM-specific constants / tunables
# ---------------------------------------------------------------------------

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

# Additional substrings tolerated by the PROFILE-CHANGER thread (profile/limit
# races that are not bugs).
_PROFILE_EXTRA_TOLERATIONS = (
    "profile",
    "does not exist",
    "memory",
    "already exists",
    "limit",
)

# Light-writer inter-batch sleep range (seconds).  Wide enough that a churn
# tenant is idle for much longer than the SUSPEND sole-accessor freeze window
# (~100ms), so a concurrent SUSPEND almost always wins.
_LIGHT_WRITER_SLEEP_LO = 0.15
_LIGHT_WRITER_SLEEP_HI = 0.30


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def parse_args():
    parser = build_base_arg_parser(
        description=__doc__,
        default_parallelism=6,
        default_num_tenants=4,
        default_duration_sec=120.0,
        add_nodes_per_tx=False,
    )
    parser.add_argument(
        "--payload-bytes",
        type=int,
        default=16384,
        help="Approximate size of the string payload stored in each ballast node",
    )
    return parser.parse_args()


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
    error_collector: ErrorCollector,
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
    drv = make_driver(endpoint, username, password)

    try:
        while not stop_flag[0]:
            past_cap = local_committed >= _GROWER_CAP
            seq = local_committed
            payload = f"{seq:020d}{base_payload}"[: payload_bytes + 20]

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
                    if is_transient(exc):
                        time.sleep(RETRY_SLEEP)
                        continue
                    error_collector.record(f"grower:{tenant_name}", exc)
                    break
                except ServiceUnavailable:
                    raise
                except Exception as exc:
                    if is_transient(exc):
                        time.sleep(RETRY_SLEEP)
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
    error_collector: ErrorCollector,
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
    drv = make_driver(endpoint, username, password)

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
                    run_query(sess, f"USE DATABASE {tenant}")
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
                elif is_transient(exc):
                    # Other expected transient: tolerated, do NOT count.
                    pass
                else:
                    error_collector.record(f"light_writer:{tenant}", exc)
            except ServiceUnavailable:
                raise
            except Exception as exc:
                if is_transient(exc):
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
    error_collector: ErrorCollector,
) -> None:
    """
    Repeatedly run a large transient-allocator query on the default `memgraph`
    DB.  Commits nothing.  Each "memory limit exceeded" OOM increments
    oom_counter[0].  Any other non-marker error is recorded.
    """
    drv = make_driver(endpoint, username, password)
    runs = 0
    try:
        while not stop_flag[0]:
            try:
                with drv.session() as sess:
                    run_query(sess, _HOG_QUERY)
                runs += 1
            except (ClientError, TransientError) as exc:
                msg = str(exc).lower()
                if "memory limit exceeded" in msg:
                    with oom_lock:
                        oom_counter[0] += 1
                elif is_transient(exc):
                    pass  # other expected transients
                else:
                    error_collector.record("memory_hog", exc)
            except ServiceUnavailable:
                raise
            except Exception as exc:
                if is_transient(exc):
                    pass
                else:
                    error_collector.record("memory_hog", exc)
            time.sleep(RETRY_SLEEP)
    finally:
        drv.close()
    print(f"  [memory_hog] ran {runs} queries, OOM hits will be in oom_counter", flush=True)


# ---------------------------------------------------------------------------
# Worker 4b: RESUMER  (OOM safe-fail variant — targets CHURN tenants only)
#
# IMPORTANT: this is NOT the shared resumer_worker from hot_cold_common.
# It carries two extra parameters (resume_failed_safe_counter, resume_lock)
# and a "failed to recover / memory limit" safe-fail branch that tracks the
# RESUME-CANNOT-FIT path unique to the OOM workload.
# ---------------------------------------------------------------------------


def _resumer_worker(
    endpoint: str,
    username: str,
    password: str,
    churn_tenants: list[str],
    stop_flag: list[bool],
    resume_failed_safe_counter: list[int],
    resume_lock: threading.Lock,
    error_collector: ErrorCollector,
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
    drv = make_driver(endpoint, username, password)
    ops = 0
    try:
        while not stop_flag[0]:
            tenant = rng.choice(churn_tenants)
            try:
                with drv.session() as sess:
                    run_query(sess, f"RESUME DATABASE {tenant}")
                    ops += 1
            except (ClientError, TransientError) as exc:
                msg = str(exc).lower()
                if "failed to recover while resuming" in msg or "memory limit exceeded" in msg:
                    with resume_lock:
                        resume_failed_safe_counter[0] += 1
                elif is_transient(exc):
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
# Worker 5: PROFILE-CHANGER  (targets CHURN tenants only)
# ---------------------------------------------------------------------------


def _profile_changer_worker(
    endpoint: str,
    username: str,
    password: str,
    churn_tenants: list[str],
    stop_flag: list[bool],
    error_collector: ErrorCollector,
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
    drv = make_driver(endpoint, username, password)
    profile_seq = 0

    def _is_profile_tolerated(exc: Exception) -> bool:
        msg = str(exc).lower()
        if is_transient(exc):
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
                    run_query(
                        sess,
                        f"CREATE TENANT PROFILE {profile_name} LIMIT memory_limit {limit_mb} MB",
                    )
            except Exception as exc:
                if not _is_profile_tolerated(exc):
                    error_collector.record(f"profile_changer:create:{profile_name}", exc)
                time.sleep(RETRY_SLEEP)
                continue

            # Step 2: SET TENANT PROFILE ON DATABASE tenant TO profile_name
            try:
                with drv.session() as sess:
                    run_query(
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
                        run_query(
                            sess,
                            f"ALTER TENANT PROFILE {profile_name} SET memory_limit {new_limit_mb} MB",
                        )
                except Exception as exc:
                    if not _is_profile_tolerated(exc):
                        error_collector.record(f"profile_changer:alter:{profile_name}", exc)

            # Step 4: REMOVE TENANT PROFILE FROM DATABASE tenant
            try:
                with drv.session() as sess:
                    run_query(sess, f"REMOVE TENANT PROFILE FROM DATABASE {tenant}")
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
    wait_for_server(endpoint, username, password)
    create_tenants(endpoint, username, password, tenant_names)

    # -----------------------------------------------------------------------
    # Phase 2: concurrent stress run.
    # -----------------------------------------------------------------------
    print(f"\n==> Phase 2: concurrent stress for {duration_sec}s", flush=True)

    stop_flag: list[bool] = [False]
    suspender_stop_flag: list[bool] = [False]
    committed_counts: dict[str, int] = {}
    counts_lock = threading.Lock()
    oom_counter: list[int] = [0]
    oom_lock = threading.Lock()
    resume_failed_safe_counter: list[int] = [0]
    resume_lock = threading.Lock()
    error_collector = ErrorCollector()

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

        # SUSPENDER thread (churn tenants only). Uses its OWN stop flag for the
        # staggered shutdown below.
        f = pool.submit(
            suspender_worker,
            endpoint,
            username,
            password,
            churn_tenants,
            suspender_stop_flag,
            error_collector,
            base_seed + len(ballast_tenants) + 1,
        )
        futures.append(("suspender", "churn", f))

        # RESUMER thread (churn tenants only) — OOM safe-fail variant.
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

        # Run for duration_sec, then STAGGER the stop: drain the antagonists (writers /
        # memory hog / resumer) first, then give the suspender a short contention-free
        # tail so its SUSPENDs land deterministically (see SUSPENDER_TAIL_SEC). The
        # contended window above still exercises the concurrent suspend-under-OOM path;
        # the tail only guarantees the suspends_ok>0 non-vacuity gate.
        staggered_stop(stop_flag, suspender_stop_flag, duration_sec)

        # Collect results / exceptions.
        suspends_ok, resumes_ok = collect_worker_results(futures, timeout=120.0)

    # -----------------------------------------------------------------------
    # Phase 3: unexpected-error check.
    # -----------------------------------------------------------------------
    print("\n==> Phase 3: unexpected-error check", flush=True)
    fail_on_unexpected_errors(
        error_collector,
        suffix="OOM must always surface as a retriable marker, never an unrecognised exception.",
    )

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
    drv_cleanup = make_driver(endpoint, username, password)
    try:
        with drv_cleanup.session() as sess:
            for tname in tenant_names:
                try:
                    run_query(sess, f"REMOVE TENANT PROFILE FROM DATABASE {tname}")
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
        suspend_tenant(endpoint, username, password, tname)

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
                continue

            try:
                actual = count_nodes_on_tenant(endpoint, username, password, tname)
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
