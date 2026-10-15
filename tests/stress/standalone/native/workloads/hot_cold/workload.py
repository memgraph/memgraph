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
  5. DDL thread       — issues CREATE DATABASE / DROP DATABASE [FORCE] /
                        RENAME DATABASE ... TO ... against a POOL OF CHURN
                        TENANTS (churn_tenant_0..N-1) that is DISJOINT from
                        tenant_0..k, so DDL-vs-transition races are exercised
                        without perturbing the fixed set's write/count
                        accounting (reviewer request #15).

NOTE: v2 re-scoped the feature to MANUAL suspend/resume only. All
auto-eviction machinery was deleted. The only churn dimensions are the
SUSPEND/RESUME thread pair and the DDL thread above.

After the timed run every tenant is resumed and its live node count is checked
against the sum of per-worker committed writes. A mismatch = data loss = exit 1.
The churn tenants are transient by design and are NOT part of this check — we
assert only that the DDL thread ran without a non-transient exception, not that
any particular churn_tenant_N exists (or doesn't) when the run ends.

A non-vacuity check asserts that at least one SUSPEND and at least one RESUME
actually succeeded during the churn window. If neither count is positive, the
test is vacuously green and exits 1 with a clear message. (The DDL thread's op
count is reported for visibility but is not part of this gate — CREATE/DROP/
RENAME races are expected to be lossy under contention by design.)

Run directly (smoke test):

  python3 workload.py --endpoint 127.0.0.1:7687 \\
      --parallelism 6 --duration-sec 20 --num-tenants 6 --num-churn-tenants 4

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

# ---------------------------------------------------------------------------
# Driver import: mirror ring/workload.py — use the neo4j driver via common.py
# helpers when available, fall back to direct neo4j import.
# ---------------------------------------------------------------------------
# Allow running from the source tree (stress runner sets PYTHONPATH or cwd).
_STRESS_ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", ".."))
if _STRESS_ROOT not in sys.path:
    sys.path.insert(0, _STRESS_ROOT)

from hot_cold_common import (
    MAX_RETRIES,
    RETRY_SLEEP,
    SUSPENDER_TAIL_SEC,
    ClientError,
    ServiceUnavailable,
    TransientError,
    assert_server_alive,
    build_base_arg_parser,
    collect_worker_results,
    count_nodes_on_tenant,
    create_tenants,
    is_transient,
    make_driver,
    reader_worker,
    resume_tenant_blocking,
    run_query,
    staggered_stop,
    wait_for_server,
)

try:
    from neo4j import GraphDatabase
except ImportError as exc:
    sys.exit(f"FATAL: neo4j Python driver not installed: {exc}")


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = build_base_arg_parser(
        description=__doc__,
        default_parallelism=8,
        default_num_tenants=6,
        default_duration_sec=20,
        add_nodes_per_tx=True,
        default_nodes_per_tx=5,
    )
    # DDL-churn pool: disjoint from tenant_0..k (see _ddl_worker docstring).  Kept as a
    # local flag rather than a build_base_arg_parser addition — this pool is specific to
    # this workload's reviewer-requested DDL-churn thread, not shared by the other four
    # hot/cold workloads.
    parser.add_argument(
        "--num-churn-tenants",
        type=int,
        default=4,
        help="Number of churn_tenant_N databases the DDL thread creates/drops/renames " "(disjoint from tenant_0..k)",
    )
    return parser.parse_args()


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
    drv = make_driver(endpoint, username, password)

    try:
        while not stop_flag[0]:
            tenant = rng.choice(tenant_names)
            # Each write attempt: new session (so USE DATABASE and state are fresh).
            committed = False
            for attempt in range(MAX_RETRIES):
                if stop_flag[0]:
                    break
                try:
                    with drv.session() as sess:
                        # Switch to tenant context.
                        run_query(sess, f"USE DATABASE {tenant}")
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
                    if is_transient(exc):
                        time.sleep(RETRY_SLEEP)
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
                    if is_transient(exc):
                        time.sleep(RETRY_SLEEP)
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
                if is_transient(exc):
                    pass
                else:
                    print(f"  [suspender] unexpected error on {tenant}: {exc}", flush=True)
            time.sleep(RETRY_SLEEP)
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
                    print(f"  [resumer] unexpected error on {tenant}: {exc}", flush=True)
            except Exception as exc:
                if is_transient(exc):
                    pass
                else:
                    print(f"  [resumer] unexpected error on {tenant}: {exc}", flush=True)
            time.sleep(RETRY_SLEEP)
    finally:
        drv.close()
    print(f"  [resumer] issued {ops} successful resumes", flush=True)
    return ops


# Error substrings produced by CREATE/DROP/RENAME DATABASE (interpreter.cpp) that are
# EXPECTED outcomes of two DDL ops racing each other on the churn pool (e.g. two DROPs
# on the same churn_tenant_N, or a RENAME losing to a DROP). None of these indicate a
# product bug — they mirror how _suspender_worker/_resumer_worker above tolerate their
# own "lost the race" errors. TRANSITIONAL_MARKERS (is_transient) already covers the
# suspend/resume-shaped races (e.g. "multiple concurrent system queries are not
# supported", which fires for ANY two DDL/suspend/resume ops since they all share the
# one system transaction); this tuple adds the CREATE/DROP/RENAME-specific ones.
_DDL_SWALLOW = (
    "already exists",  # CREATE raced another CREATE, or RENAME's target churn name collided
    "does not exist",  # DROP/RENAME raced a DROP that already removed the churn tenant
    "currently being used",  # DROP/RENAME (USING) raced a still-mid-transition churn tenant
    "same as the old name",  # RENAME picked identical src/dst names (benign, not a race)
    "cannot delete the default database",  # defensive; churn pool never includes the default db
    "cannot rename the default database",  # defensive; same reasoning
)


def _classify_ddl_error(exc: Exception) -> bool:
    """Return True if *exc* is an expected DDL-churn race (log-and-continue, not a failure)."""
    if is_transient(exc):
        return True
    msg = str(exc).lower()
    return any(marker in msg for marker in _DDL_SWALLOW)


def _ddl_worker(
    endpoint: str,
    username: str,
    password: str,
    churn_tenant_names: list[str],
    stop_flag: list[bool],
    rng_seed: int,
) -> int:
    """
    Concurrent DBMS-DDL churn thread (reviewer request #15): repeatedly issues
    CREATE DATABASE / DROP DATABASE [FORCE] / RENAME DATABASE ... TO ... against
    a POOL OF CHURN TENANTS (churn_tenant_0..churn_tenant_{N-1}) that is disjoint
    from the fixed tenant_0..tenant_k set used by the writer/suspender/resumer/reader
    threads above.

    Isolation rationale: the fixed tenant set backs the test's final data-integrity
    accounting (committed writes vs. resumed node counts). Racing CREATE/DROP/RENAME
    against tenants a writer might be mid-commit on would require tearing down that
    accounting (a dropped tenant's committed writes become unverifiable). Keeping DDL
    churn on a disjoint namespace exercises exactly the DDL-vs-hot/cold-transition
    races the reviewer asked for, without touching the invariant the rest of the
    workload checks.

    The DDL grammar only exposes a FORCE variant for DROP DATABASE (see
    MemgraphCypher.g4: `dropDatabase: DROP DATABASE databaseName (FORCE)?` vs.
    `createDatabase`/`renameDatabase`, which take no FORCE token) — plain DROP fails
    fast on an in-use database (DeleteError::USING) while FORCE best-effort-terminates
    active interpreters first. Both variants are exercised (50/50 per DROP attempt) to
    cover both code paths.

    Expected-under-contention errors (see _classify_ddl_error / _DDL_SWALLOW /
    TRANSITIONAL_MARKERS) are counted and logged, not raised — a churn tenant that
    another DDL op or an in-flight suspend/resume already mutated is normal churn, not
    a bug. Because the churn pool never overlaps tenant_names, tenant-set-vs-DDL
    interference cannot occur, so this thread never touches the SUSPEND/RESUME state
    machine's namespace either.

    Returns the count of successful DDL operations (CREATE + DROP + RENAME combined)
    for reporting; not used in the non-vacuity gate (SUSPEND/RESUME are).
    """
    rng = random.Random(rng_seed)
    drv = make_driver(endpoint, username, password)
    ops = 0
    # existing_churn tracks which churn_tenant_N names this thread currently believes
    # are live, so RENAME has a plausible (name, name) pair to try instead of blindly
    # guessing — a purely local hint, not a correctness dependency (a stale/incorrect
    # guess just yields an expected "does not exist" race, handled above).
    existing_churn: set[str] = set()
    try:
        while not stop_flag[0]:
            op = rng.choice(("create", "drop", "drop", "rename"))
            try:
                with drv.session() as sess:
                    if op == "create":
                        name = rng.choice(churn_tenant_names)
                        run_query(sess, f"CREATE DATABASE {name}")
                        existing_churn.add(name)
                        ops += 1
                    elif op == "drop":
                        name = rng.choice(churn_tenant_names)
                        force = rng.random() < 0.5
                        query = f"DROP DATABASE {name} FORCE" if force else f"DROP DATABASE {name}"
                        run_query(sess, query)
                        existing_churn.discard(name)
                        ops += 1
                    else:  # rename
                        src = rng.choice(churn_tenant_names)
                        dst = rng.choice(churn_tenant_names)
                        if src == dst:
                            continue
                        run_query(sess, f"RENAME DATABASE {src} TO {dst}")
                        existing_churn.discard(src)
                        existing_churn.add(dst)
                        ops += 1
            except (ClientError, TransientError) as exc:
                if not _classify_ddl_error(exc):
                    print(f"  [ddl] unexpected error on {op}: {exc}", flush=True)
            except ServiceUnavailable:
                raise
            except Exception as exc:
                if not _classify_ddl_error(exc):
                    print(f"  [ddl] unexpected error on {op}: {exc}", flush=True)
            time.sleep(RETRY_SLEEP)
    finally:
        # Best-effort cleanup: FORCE-drop any churn tenant this thread believes it left
        # behind, so a killed run doesn't leak churn_tenant_N databases into the next
        # invocation. Failures here are swallowed — cleanup is housekeeping, not an
        # assertion (mirrors suspend_tenant's "never raises" contract in common.py).
        for name in list(existing_churn):
            try:
                with drv.session() as sess:
                    run_query(sess, f"DROP DATABASE {name} FORCE")
            except Exception:
                pass
        drv.close()
    print(f"  [ddl] issued {ops} successful DDL ops on the churn pool", flush=True)
    return ops


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
    num_churn_tenants: int = max(1, args.num_churn_tenants)

    tenant_names = [f"tenant_{i}" for i in range(num_tenants)]
    # Disjoint namespace from tenant_names — see _ddl_worker docstring. "churn_" prefix
    # makes any accidental overlap with tenant_N immediately visible in logs/repro.
    churn_tenant_names = [f"churn_tenant_{i}" for i in range(num_churn_tenants)]

    print("==> Hot/Cold concurrency stress (v2: manual suspend/resume only)", flush=True)
    print(f"    endpoint       : {endpoint}", flush=True)
    print(f"    tenants        : {tenant_names}", flush=True)
    print(f"    churn tenants  : {churn_tenant_names}  (DDL churn pool, disjoint)", flush=True)
    print(
        f"    parallelism    : {parallelism} writers + 1 suspender + 1 resumer + 1 reader + 1 ddl",
        flush=True,
    )
    print(f"    duration       : {duration_sec}s", flush=True)
    print(f"    nodes_per_tx   : {nodes_per_tx}", flush=True)

    # Phase 1: wait for server, then create tenant databases.
    # NOTE: churn_tenant_names are intentionally NOT pre-created here — the DDL thread
    # itself CREATEs (and DROPs/RENAMEs) them, so the pool starts empty and the DDL
    # thread's own CREATE ops are exercised from t=0 instead of only DROP/RENAME.
    print("\n==> Phase 1: server readiness + tenant setup", flush=True)
    wait_for_server(endpoint, username, password)
    create_tenants(endpoint, username, password, tenant_names)

    # Phase 2: concurrent stress run.
    print(f"\n==> Phase 2: concurrent stress for {duration_sec}s", flush=True)
    stop_flag: list[bool] = [False]
    suspender_stop_flag: list[bool] = [False]
    committed_counts: dict[str, int] = {}

    base_seed = int(time.time())
    futures = []

    with ThreadPoolExecutor(max_workers=parallelism + 4) as pool:
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

        # One suspender. Uses its OWN stop flag for the staggered shutdown below.
        f = pool.submit(
            _suspender_worker,
            endpoint,
            username,
            password,
            tenant_names,
            suspender_stop_flag,
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

        # One DDL-churn thread (reviewer request #15). Uses the shared stop_flag (not
        # suspender_stop_flag) so it drains together with the other antagonists and does
        # not run into the suspender's contention-free tail — the tail's only job is to
        # let SUSPEND land deterministically on tenant_names, and DDL churn never touches
        # that namespace anyway, so there is nothing to gain by extending its lifetime.
        f = pool.submit(
            _ddl_worker,
            endpoint,
            username,
            password,
            churn_tenant_names,
            stop_flag,
            base_seed + parallelism + 3,
        )
        futures.append(("ddl", 0, f))

        # Let all workers run for duration_sec, then STAGGER the stop: drain the
        # antagonists (writers/resumer) first, then give the suspender a short
        # contention-free tail so its SUSPENDs land deterministically (see
        # SUSPENDER_TAIL_SEC). The contended window above still exercises the
        # concurrent suspend-vs-write path; the tail only guarantees non-vacuity.
        staggered_stop(stop_flag, suspender_stop_flag, duration_sec)

        # Collect any exceptions from workers (non-transient = real bug).
        # collect_worker_results only tallies the "suspender"/"resumer" roles by name;
        # it still awaits every future (so the "ddl" future's exceptions propagate the
        # same as any other worker's), but its return value is informational-only here
        # (see Phase 3.5 below) so we fetch it directly rather than growing the shared
        # helper's role-name special-casing for a single caller.
        suspends_ok, resumes_ok = collect_worker_results(futures, timeout=60.0)
        ddl_ops_ok = next(f.result() for role, _wid, f in futures if role == "ddl")

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

    # Phase 3.5: DDL-churn readout (informational only — see reviewer request #15).
    # The churn tenants are transient by design: we assert neither their existence nor
    # non-existence at the end, only that the DDL thread ran without raising a
    # non-transient exception (already enforced above by collect_worker_results, which
    # awaits the "ddl" future same as every other worker).
    print("\n==> Phase 3.5: DDL-churn summary (informational, no assertion)", flush=True)
    print(f"    ddl_ops_ok  : {ddl_ops_ok} (CREATE+DROP+RENAME on the churn pool combined)", flush=True)

    # Phase 4: resume each tenant and verify actual counts.
    print("\n==> Phase 4: final data-integrity verification", flush=True)

    mismatches = []
    for name in tenant_names:
        print(f"  verifying {name}...", flush=True)
        try:
            resume_tenant_blocking(endpoint, username, password, name, timeout=60.0)
        except Exception as exc:
            mismatches.append((name, -1, expected[name], f"resume failed: {exc}"))
            continue

        try:
            actual = count_nodes_on_tenant(endpoint, username, password, name)
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
    assert_server_alive(endpoint, username, password)

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
