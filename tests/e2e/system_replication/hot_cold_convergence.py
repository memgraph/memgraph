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

# End-to-end coverage for hot/cold tenants over replication plus the node-local cross-restart
# durability path.
#
#   SUSPEND/RESUME DATABASE is system-replicated: a SUSPEND on MAIN streams a SuspendDatabaseRpc
#        so each connected replica tears its own copy down to a COLD shell; RESUME rebuilds it. This
#        is exercised by test_suspend_resume_convergence (replica online the whole time).
#
#   A reconnecting/lagging replica converges to MAIN's authoritative {HOT ∪ COLD} set via the
#        V3 SystemRecovery payload (which now carries the COLD set). This is exercised by
#        test_lagging_replica_convergence: the replica is DOWN across the SUSPEND, so the
#        SuspendDatabaseRpc never reaches it; on reconnect SystemRecovery must force-suspend the
#        tenant the replica still holds HOT (SR-1 exempt-from-delete + SR-1'(2) force-suspend).
#
#   Hot/cold is durable: a tenant suspended before a restart recovers COLD (metadata-only shell,
#        durable cold marker), a HOT tenant recovers HOT with its data, and the COLD tenant still
#        resumes with all data intact. This is a single-instance test (no replication) exercised by
#        test_cross_restart_hot_only_recovery.
#
#   Eager-epoch promotion: when a node holding a COLD tenant is promoted to MAIN, the new epoch
#        is written into the cold tenant's durable metadata (PromoteColdTenants, since ForEach skips
#        cold tenants) WITH the pre-promotion epoch appended to the epoch history. A later RESUME runs
#        the new epoch, and a replica that still holds the tenant at the OLD epoch finds that old epoch
#        in the new MAIN's continuous history -> it converges instead of diverging. Exercised by
#        test_promotion_eager_epoch_convergence (the negative control: without the history boundary the
#        old-epoch replica would DIVERGE and never converge).
#
#   Hot/cold is observable: SHOW DATABASES lists a COLD tenant with a HOT/COLD status column
#        (it would otherwise vanish, being excluded from All()), and SHOW STORAGE INFO ON <cold>
#        serves the durable as-of-suspend snapshot instead of erroring (previously this errored).
#        Exercised by test_cold_aware_show.
#
# Note: querying DATA on a COLD tenant (USE DATABASE + MATCH) still trips the query seam in
# DbmsHandler::Get_ ("... is suspended (cold); run RESUME ..."), which tenant_probe relies on — only
# the SHOW surfaces are cold-aware (per product point 1: cold access is an error, not a reheat).

import os
import random
import sys
import threading
import time

import interactive_mg_runner
import mgclient
import pytest
from common import execute_and_fetch_all, get_data_path, get_logs_path
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

BOLT_PORTS = {"main": 7687, "replica_1": 7688}
# replica_1 is the original replica; "main" gets a replication port too because the promotion test
# demotes the old MAIN to a REPLICA of the promoted node (manual failover).
REPLICATION_PORTS = {"replica_1": 10001, "main": 10002}
file = "hot_cold_convergence"


@pytest.fixture
def test_name(request):
    return request.node.name


@pytest.fixture(autouse=True)
def cleanup_after_test():
    interactive_mg_runner.kill_all(keep_directories=False)
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


def main_args(test_name, recovery: bool = False, restore_replication: bool = False):
    args = [
        "--bolt-port",
        f"{BOLT_PORTS['main']}",
        "--log-level=TRACE",
    ]
    if recovery:
        # Cross-restart durability test: on restart MAIN must recover its tenants from its own disk
        # (HOT tenants rebuilt, COLD tenants restored as metadata-only shells).
        args += ["--data-recovery-on-startup=true"]
    if restore_replication:
        # MAIN-crash-recovery test: on restart MAIN must restore its MAIN role + registered replicas and
        # auto-reconnect (driving SystemRecovery), as a real crashed MAIN does.
        args += ["--replication-restore-state-on-startup=true"]
    return {
        "args": args,
        "log_file": f"{get_logs_path(file, test_name)}/main.log",
        "data_directory": f"{get_data_path(file, test_name)}/main",
        "setup_queries": [],
    }


def replica_args(test_name, recovery: bool):
    args = [
        "--bolt-port",
        f"{BOLT_PORTS['replica_1']}",
        "--log-level=TRACE",
    ]
    if recovery:
        # Needed for the lagging-replica test: on restart the replica must recover its tenant from
        # its own disk (HOT) and restore the REPLICA role so MAIN reconnects and drives SystemRecovery.
        args += ["--replication-restore-state-on-startup=true", "--data-recovery-on-startup=true"]
    return {
        "args": args,
        "log_file": f"{get_logs_path(file, test_name)}/replica1.log",
        "data_directory": f"{get_data_path(file, test_name)}/replica1",
        "setup_queries": [],
    }


def register_replica(main_cursor, sync: bool):
    mode = "SYNC" if sync else "ASYNC"
    execute_and_fetch_all(
        main_cursor, f"REGISTER REPLICA replica_1 {mode} TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';"
    )


def set_replica_role(replica_cursor):
    execute_and_fetch_all(
        replica_cursor, f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};"
    )


def create_and_populate(main_cursor, db_name, n):
    execute_and_fetch_all(main_cursor, f"CREATE DATABASE {db_name};")
    execute_and_fetch_all(main_cursor, f"USE DATABASE {db_name};")
    for _ in range(n):
        execute_and_fetch_all(main_cursor, "CREATE ();")


def tenant_probe(cursor, db_name):
    """Returns the node count if the tenant is HOT, the string "COLD" if it is suspended, or
    "MISSING" if it is unknown. Used with mg_sleep_and_assert to wait for convergence.

    IMPORTANT: the probe resets the session back to the default database in a finally block. A bolt
    session keeps a live accessor on its current database for the whole session, which would pin the
    tenant HOT (an "active connection") and prevent SUSPEND from ever reaching the sole-accessor state
    — both the SuspendDatabaseRpc apply handler and the SystemRecovery reconcile would time out
    draining. Releasing A after every
    probe leaves the suspend a window to win."""

    def func():
        try:
            execute_and_fetch_all(cursor, f"USE DATABASE {db_name};")
            return execute_and_fetch_all(cursor, "MATCH (n) RETURN count(*);")[0][0]
        except mgclient.DatabaseError as e:
            msg = str(e)
            if "suspended (cold)" in msg:
                return "COLD"
            if "unknown database" in msg.lower() or "doesn't exist" in msg.lower():
                return "MISSING"
            raise
        finally:
            # Release the tenant: do not let the probe session pin it HOT across a suspend.
            try:
                execute_and_fetch_all(cursor, "USE DATABASE memgraph;")
            except mgclient.DatabaseError:
                pass

    return func


def test_suspend_resume_convergence(connection, test_name):
    # With the replica online, a SUSPEND on MAIN must drive the replica COLD, and a RESUME must
    # drive it HOT again with the tenant's data intact.
    instances = {
        "replica_1": replica_args(test_name, recovery=False),
        "main": main_args(test_name),
    }
    interactive_mg_runner.start_all(instances, keep_directories=False)

    replica_cursor = connection(BOLT_PORTS["replica_1"], "replica_1").cursor()
    set_replica_role(replica_cursor)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()
    register_replica(main_cursor, sync=True)

    # Create + populate tenant A on MAIN; it replicates to the (HOT) replica.
    create_and_populate(main_cursor, "A", 5)
    mg_sleep_and_assert(5, tenant_probe(replica_cursor, "A"))

    # SUSPEND on MAIN -> streams SuspendDatabaseRpc, replica copy torn down to COLD.
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "SUSPEND DATABASE A;")
    mg_sleep_and_assert("COLD", tenant_probe(replica_cursor, "A"))
    # MAIN's own copy is COLD too.
    assert tenant_probe(main_cursor, "A")() == "COLD"

    # RESUME on MAIN -> streams ResumeDatabaseRpc, replica copy rebuilt HOT with data intact.
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "RESUME DATABASE A;")
    mg_sleep_and_assert(5, tenant_probe(replica_cursor, "A"))
    assert tenant_probe(main_cursor, "A")() == 5


def test_lagging_replica_convergence(connection, test_name):
    # The replica is DOWN across the SUSPEND, so the SuspendDatabaseRpc never reaches it. On
    # reconnect, the V3 SystemRecovery payload carries A in the COLD set; the replica (which recovered
    # A HOT from its own disk) must force-suspend it to converge (SR-1 / SR-1'(2)) rather than serving
    # a stale HOT copy or dropping it.
    instances = {
        "replica_1": replica_args(test_name, recovery=True),
        "main": main_args(test_name),
    }
    interactive_mg_runner.start_all(instances, keep_directories=False)

    replica_cursor = connection(BOLT_PORTS["replica_1"], "replica_1").cursor()
    set_replica_role(replica_cursor)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()
    register_replica(main_cursor, sync=False)  # ASYNC: SUSPEND must not block on the down replica

    create_and_populate(main_cursor, "A", 7)
    mg_sleep_and_assert(7, tenant_probe(replica_cursor, "A"))

    # Take the replica down (keep its data dir so it recovers A HOT on restart).
    interactive_mg_runner.kill(instances, "replica_1", keep_directories=True)

    # SUSPEND A on MAIN while the replica is offline. The SuspendDatabaseRpc is lost; only the V3
    # SystemRecovery payload can convey the COLD state on reconnect.
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "SUSPEND DATABASE A;")
    assert tenant_probe(main_cursor, "A")() == "COLD"

    # Bring the replica back. It recovers A HOT from disk, reconnects, MAIN runs SystemRecovery (V3),
    # and the replica reconciles A to COLD.
    interactive_mg_runner.start(instances, "replica_1")
    replica_cursor = connection(BOLT_PORTS["replica_1"], "replica_1").cursor()
    mg_sleep_and_assert("COLD", tenant_probe(replica_cursor, "A"))

    # And a subsequent RESUME on MAIN still converges the replica back to HOT with data intact.
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "RESUME DATABASE A;")
    mg_sleep_and_assert(7, tenant_probe(replica_cursor, "A"))


def test_cross_restart_hot_only_recovery(connection, test_name):
    # Single MAIN, no replication. A tenant suspended before a restart must recover COLD (durable
    # cold marker), a HOT tenant must recover HOT with its data, and the COLD tenant must still resume
    # with all its data — proving the durable cold marker round-trips and the restore loop branches on
    # it (and preserves both data directories).
    instances = {"main": main_args(test_name, recovery=True)}
    interactive_mg_runner.start_all(instances, keep_directories=False)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()
    create_and_populate(main_cursor, "A", 7)
    create_and_populate(main_cursor, "B", 4)

    # Suspend A; B stays HOT.
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "SUSPEND DATABASE A;")
    assert tenant_probe(main_cursor, "A")() == "COLD"
    assert tenant_probe(main_cursor, "B")() == 4

    # Restart MAIN, keeping its data directory.
    interactive_mg_runner.kill(instances, "main", keep_directories=True)
    interactive_mg_runner.start(instances, "main")
    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # A recovers COLD (durable cold marker), B recovers HOT with its data intact.
    assert tenant_probe(main_cursor, "A")() == "COLD", "a tenant suspended before restart must recover COLD"
    assert tenant_probe(main_cursor, "B")() == 4, "a HOT tenant must recover HOT with its data"

    # A still resumes from its preserved data directory with all data intact.
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "RESUME DATABASE A;")
    assert tenant_probe(main_cursor, "A")() == 7, "the cold tenant must resume with all data after a restart"


def test_cold_aware_show(connection, test_name):
    # Single MAIN. After SUSPEND, the cold tenant must remain VISIBLE in SHOW DATABASES (tagged
    # COLD) and SHOW STORAGE INFO ON <cold> must serve its as-of-suspend snapshot instead of erroring
    # (previously this errored). RESUME flips it back to HOT.
    instances = {"main": main_args(test_name)}
    interactive_mg_runner.start_all(instances, keep_directories=False)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()
    create_and_populate(main_cursor, "A", 8)
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")

    # While HOT: SHOW DATABASES tags A as HOT.
    rows = dict((r[0], r[1]) for r in execute_and_fetch_all(main_cursor, "SHOW DATABASES;"))
    assert rows.get("A") == "HOT" and rows.get("memgraph") == "HOT", rows

    execute_and_fetch_all(main_cursor, "SUSPEND DATABASE A;")

    # COLD: A must still appear in SHOW DATABASES, now tagged COLD (it is excluded from All()).
    rows = dict((r[0], r[1]) for r in execute_and_fetch_all(main_cursor, "SHOW DATABASES;"))
    assert rows.get("A") == "COLD", f"a suspended tenant must remain visible as COLD: {rows}"
    assert rows.get("memgraph") == "HOT", rows

    # SHOW STORAGE INFO ON <cold> serves the as-of-suspend snapshot (does NOT raise the cold seam).
    info = dict((r[0], r[1]) for r in execute_and_fetch_all(main_cursor, "SHOW STORAGE INFO ON DATABASE A;"))
    assert info.get("vertex_count") == 8, f"cold SHOW STORAGE INFO must carry the as-of-suspend count: {info}"
    assert "COLD" in str(info.get("status", "")), f"cold storage info must be labelled a snapshot: {info}"
    assert info.get("name") == "A", info

    # Querying DATA on the cold tenant still errors (cold access is an error, not a reheat).
    assert tenant_probe(main_cursor, "A")() == "COLD"

    # RESUME -> HOT again in SHOW DATABASES.
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "RESUME DATABASE A;")
    rows = dict((r[0], r[1]) for r in execute_and_fetch_all(main_cursor, "SHOW DATABASES;"))
    assert rows.get("A") == "HOT", f"a resumed tenant must be HOT again: {rows}"
    assert tenant_probe(main_cursor, "A")() == 8


def test_promotion_eager_epoch_convergence(connection, test_name):
    # Promote a node that holds a COLD tenant, then prove a replica still at the pre-promotion
    # epoch converges (continuous history) instead of diverging.
    #
    #   1. main + replica_1, tenant A replicated, both HOT at epoch E1.
    #   2. SUSPEND A -> both COLD; A's data sits on disk at E1 on both.
    #   3. Kill main. replica_1 is the survivor, holding COLD A at E1.
    #   4. Promote replica_1 -> MAIN: DoToMainPromotion mints a new epoch E2 and PromoteColdTenants
    #      rewrites A's durable cold metadata to E2 WITH (E1, ldt) appended to its epoch history
    #      (ForEach alone would skip the cold tenant, so PromoteColdTenants handles it explicitly).
    #   5. RESUME A on the new MAIN -> HOT at E2, data intact, history carrying the E1 boundary.
    #   6. Restart the old main as a REPLICA of the new MAIN. It still holds A at E1. The new MAIN's
    #      continuous-history check finds E1 in A's history -> continuous -> the replica converges to
    #      the new MAIN's data. Without the eager-epoch boundary, the replica at E1 would be flagged a
    #      branching point (DIVERGED_FROM_MAIN) and never converge -> this assertion would time out
    #      (the negative control that gives the test teeth).
    instances = {
        "replica_1": replica_args(test_name, recovery=True),
        "main": main_args(test_name, recovery=True),
    }
    interactive_mg_runner.start_all(instances, keep_directories=False)

    replica_cursor = connection(BOLT_PORTS["replica_1"], "replica_1").cursor()
    set_replica_role(replica_cursor)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()
    register_replica(main_cursor, sync=True)

    # Tenant A: replicated to the HOT replica at epoch E1.
    create_and_populate(main_cursor, "A", 6)
    mg_sleep_and_assert(6, tenant_probe(replica_cursor, "A"))

    # SUSPEND A -> both copies COLD at E1.
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "SUSPEND DATABASE A;")
    mg_sleep_and_assert("COLD", tenant_probe(replica_cursor, "A"))

    # Kill the old MAIN (keep its data dir: it returns as a replica still holding A at E1).
    interactive_mg_runner.kill(instances, "main", keep_directories=True)

    # Promote the survivor (replica_1) to MAIN. DoToMainPromotion generates a new epoch and
    # PromoteColdTenants rewrites the COLD tenant's durable epoch + history boundary.
    execute_and_fetch_all(replica_cursor, "SET REPLICATION ROLE TO MAIN;")

    # RESUME A on the new MAIN: it runs the new epoch E2, data intact, history holds the E1 boundary.
    execute_and_fetch_all(replica_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(replica_cursor, "RESUME DATABASE A;")
    mg_sleep_and_assert(6, tenant_probe(replica_cursor, "A"))

    # Bring the old MAIN back and demote it to a REPLICA of the new MAIN. It recovers A (COLD) at E1
    # from its own disk; --replication-restore-state is OFF so it does not try to resurrect its old
    # MAIN role / stale replica registration.
    interactive_mg_runner.start(instances, "main")
    # Tag this connection "replica": the old main is demoted below, and the connection fixture's
    # teardown only write-cleans a "main" (replicas reject writes). kill_all wipes the dirs regardless.
    old_main_cursor = connection(BOLT_PORTS["main"], "replica").cursor()
    execute_and_fetch_all(old_main_cursor, f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['main']};")

    # The new MAIN registers the old main as a replica; SystemRecovery resumes A on it and streams the
    # data. The continuous-history check accepts E1 (it is in A's history on the new MAIN), so the
    # replica converges to 6 nodes rather than diverging.
    execute_and_fetch_all(replica_cursor, f"REGISTER REPLICA old_main SYNC TO '127.0.0.1:{REPLICATION_PORTS['main']}';")
    mg_sleep_and_assert(6, tenant_probe(old_main_cursor, "A"))


# The client-facing errors that are EXPECTED under the concurrent suspend/resume + memory-ceiling stress.
# Each is a benign, retriable outcome of the race, NOT a bug. An error whose text matches NONE of these is
# treated as a real failure — this is the core "OOM / contention surfaces a retriable error, never a crash
# or a terminate" assertion (product point 7). These are the v2 markers only: the v1 auto-machinery markers
# ("is resuming", "not been hot long enough") were deleted when the feature was rescoped.
EXPECTED_STRESS_MARKERS = (
    "is suspended (cold)",  # a writer touched a tenant a suspender just took COLD
    "active connections",  # SUSPEND lost the race to a writer still holding the tenant
    "does not exist or is already cold",  # SUSPEND raced another suspender / tenant already COLD
    "does not exist or is not suspended",  # RESUME raced a writer / another resumer; tenant already HOT
    "failed to recover while resuming",  # RESUME hit OOM mid-rebuild; left COLD, retriable
    "memory limit exceeded",  # an allocation tripped the hard memory ceiling
    "multiple concurrent system queries are not supported",  # SUSPEND and RESUME collided on the system tx
)


def _classify(exc, unexpected):
    """Record exc as a real failure unless its text matches a known retriable stress marker."""
    msg = str(exc).lower()
    if not any(marker in msg for marker in EXPECTED_STRESS_MARKERS):
        unexpected.append(f"{type(exc).__name__}: {exc}")


def _run_churn(stop, exercised, min_seconds=10, max_seconds=45):
    """Drive a stress window for at least min_seconds, then keep going until exercised() reports the
    non-vacuity conditions have been observed (a SUSPEND won, a RESUME won, an OOM fired, ...), or until
    max_seconds elapses — then signal the workers to stop.

    This replaces a fixed-duration window. A fixed slice flakes on a slow/loaded host that simply does not
    happen to land a winning SUSPEND/RESUME (or a memory/cold collision) inside that arbitrary slice, even
    though the feature is healthy. Adapting to the observed conditions removes that flake while keeping the
    teeth: if the conditions are STILL not met at max_seconds we stop anyway and the caller's
    `assert ... was exercised` fires loudly — a bounded, real signal that the churn could not make progress."""
    start = time.time()
    while True:
        elapsed = time.time() - start
        if elapsed >= max_seconds:
            break
        if elapsed >= min_seconds and exercised():
            break
        time.sleep(0.2)
    stop.set()


def _supervised(body, worker_errors):
    """Wrap a stress-worker body so ANY exception that escapes it is recorded instead of dying silently.

    A worker thread's body catches only mgclient.DatabaseError (the expected retriable surface). Anything
    else — a connection-level OperationalError/InterfaceError from a server crash/restart, an AssertionError,
    a KeyError — would otherwise just print a traceback to stderr and kill that one thread. The main thread's
    `th.is_alive()` check would still report a clean stop, so surviving workers could satisfy the counters and
    the test would go GREEN while masking a real crash. Recording the exception lets the caller fail loudly."""

    def run():
        try:
            body()
        except Exception as e:  # noqa: BLE001 - any escape is a real failure to surface, not to swallow
            worker_errors.append(f"{type(e).__name__}: {e}")

    return run


def test_concurrent_suspend_resume_under_memory_ceiling(connection, test_name):
    # Stress matrix (product point 7 — "audit + stress-test every bad_alloc/crash point"). A single
    # MAIN under a hard memory ceiling runs concurrent allocating writers against several tenants WHILE a
    # suspender and a resumer churn those tenants HOT <-> COLD. The feature must stay crash-free and
    # lossless: every error a client sees must be one of the known retriable outcomes (cold access,
    # lost-the-race SUSPEND/RESUME, OOM, concurrent-system-query) — never a terminate, a tenant stranded in
    # a half-SUSPENDING/half-RESUMING limbo, or lost committed data. After the churn, every tenant resumed
    # HOT must hold EXACTLY the nodes its writer knows it durably committed.
    #
    # The companion exception-safety audit proves the suspend/resume paths are OOM-safe by construction;
    # this test is the runtime backstop that the proof holds under real contention.
    instances = {
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
                # Hard ceiling (MiB): low enough that the concurrent transient allocators below trip it,
                # high enough that all tenants boot and stay HOT at their (small, counted) final size.
                "--memory-limit=512",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [],
        }
    }
    interactive_mg_runner.start_all(instances, keep_directories=False)

    tenants = ["A", "B", "C", "D"]
    base = 5  # baseline committed nodes per tenant before the churn starts
    setup_cursor = connection(BOLT_PORTS["main"], "main").cursor()
    for t in tenants:
        create_and_populate(setup_cursor, t, base)
    execute_and_fetch_all(setup_cursor, "USE DATABASE memgraph;")

    committed = {t: base for t in tenants}  # writers extend this on every durable commit
    committed_lock = threading.Lock()
    unexpected = []  # any non-allowlisted error => real failure
    suspends_ok = [0]
    resumes_ok = [0]
    stop = threading.Event()

    BATCH = 25  # nodes per committed tx
    BIG = "x" * 128  # property payload
    # Cap the COUNTED writes per tenant so the final all-HOT footprint is provably tiny and always fits the
    # ceiling: 4 tenants x CAP nodes x ~350 B (128 B payload + node/property overhead) ~= 2.8 MiB total,
    # three orders of magnitude under the 512 MiB limit. (The ceiling pressure comes from the transient
    # allocator in step (a), NOT from committed data; and that allocator is joined before the final RESUME
    # loop below, so the loop never competes with it for memory.) Past the cap the writer keeps cycling
    # (still pressuring memory + giving suspends a window) but stops growing the durable set.
    CAP = 2000

    def writer(t):
        cur = connection(BOLT_PORTS["main"], "main").cursor()
        while not stop.is_set():
            # (a) Memory pressure on the always-HOT default DB: build a large transient list server-side.
            #     ~96 MiB per call; with four writers concurrently this crosses the 512 MiB ceiling and
            #     some calls trip OOM ("memory limit exceeded") — which must surface as a retriable error,
            #     not a crash. It commits nothing, so it never affects the data-loss accounting.
            try:
                execute_and_fetch_all(cur, "WITH range(1, 6000000) AS r RETURN size(r);")
            except mgclient.DatabaseError as e:
                _classify(e, unexpected)
            # (b) A modest, counted write to the tenant — only when it is HOT and below the cap. On success
            #     (autocommit) the batch is durably committed, so we count it; any race/OOM error is
            #     classified and skipped. A 25-node CREATE is far too small to trip the ceiling, so there is
            #     effectively no post-commit-error window that could leave the server ahead of the count.
            try:
                with committed_lock:
                    grow = committed[t] < CAP
                execute_and_fetch_all(cur, f"USE DATABASE {t};")
                if grow:
                    execute_and_fetch_all(cur, f"UNWIND range(1, {BATCH}) AS i CREATE (n:N {{v: '{BIG}'}});")
                    with committed_lock:
                        committed[t] += BATCH
            except mgclient.DatabaseError as e:
                _classify(e, unexpected)
            finally:
                # Release the tenant so a suspender can reach sole-accessor (an open session pins it HOT).
                try:
                    execute_and_fetch_all(cur, "USE DATABASE memgraph;")
                except mgclient.DatabaseError:
                    pass

    def churner(action, counter):
        cur = connection(BOLT_PORTS["main"], "main").cursor()
        while not stop.is_set():
            t = random.choice(tenants)
            try:
                execute_and_fetch_all(cur, f"{action} DATABASE {t};")
                counter[0] += 1
            except mgclient.DatabaseError as e:
                _classify(e, unexpected)
            time.sleep(0.03)

    worker_errors = []  # any exception that escapes a worker body (crash disconnect, assert, ...) -> hard fail
    threads = [threading.Thread(target=_supervised(lambda t=t: writer(t), worker_errors)) for t in tenants]
    threads.append(threading.Thread(target=_supervised(lambda: churner("SUSPEND", suspends_ok), worker_errors)))
    threads.append(threading.Thread(target=_supervised(lambda: churner("RESUME", resumes_ok), worker_errors)))
    for th in threads:
        th.start()
    # Churn until both a SUSPEND and a RESUME have actually won (bounded), so the run is never vacuously
    # green just because an arbitrary fixed slice happened to land no winning system op on a loaded host.
    _run_churn(stop, lambda: suspends_ok[0] > 0 and resumes_ok[0] > 0)
    for th in threads:
        th.join(timeout=30)
    # Every worker must have actually stopped before we read the shared counters / committed map and start
    # the single-threaded data-loss phase — a thread still alive here would race that phase (and a worker
    # wedged in a 30s+ server call is itself a failure worth surfacing loudly).
    assert all(not th.is_alive() for th in threads), "a stress worker did not stop within the join timeout"
    # A worker that died on a non-DatabaseError (e.g. a connection drop from a server crash) is otherwise
    # invisible to is_alive(); surface it rather than letting surviving workers carry the test to green.
    assert not worker_errors, f"a stress worker died on an unhandled exception: {worker_errors[:10]}"

    # 1. NO CRASH: a fresh connection still works and the control DB is responsive.
    live = connection(BOLT_PORTS["main"], "main").cursor()
    assert execute_and_fetch_all(live, "RETURN 1;")[0][0] == 1, "MAIN must still be alive after the stress"

    # 2. NO UNEXPECTED ERROR: every client error during the run was a known retriable outcome (this is the
    #    "OOM/contention is never a terminate or a hard failure" assertion).
    assert not unexpected, f"non-retriable error(s) surfaced during the stress: {unexpected[:10]}"

    # 3. The contention was actually exercised (otherwise the test is vacuously green).
    assert suspends_ok[0] > 0, "no SUSPEND ever succeeded — the suspend/resume race was not exercised"
    assert resumes_ok[0] > 0, "no RESUME ever succeeded — the suspend/resume race was not exercised"

    # 4. NO DATA LOSS: resume every tenant HOT and assert it holds EXACTLY the nodes the writer committed.
    #    RESUME is retried (via mg_sleep_and_assert) because a stray concurrent-system-query tail or a
    #    transient recovery-OOM can make a single attempt fail; the steady state must converge.
    execute_and_fetch_all(live, "USE DATABASE memgraph;")
    for t in tenants:

        def resumed_count(tt=t):
            def f():
                try:
                    execute_and_fetch_all(live, f"RESUME DATABASE {tt};")
                except mgclient.DatabaseError as e:
                    low = str(e).lower()
                    # "not suspended" => already HOT (fine); "failed to recover" => retriable; else unexpected.
                    if "not suspended" not in low and "failed to recover" not in low:
                        raise
                finally:
                    try:
                        execute_and_fetch_all(live, "USE DATABASE memgraph;")
                    except mgclient.DatabaseError:
                        pass
                return tenant_probe(live, tt)()

            return f

        with committed_lock:
            expected = committed[t]
        mg_sleep_and_assert(expected, resumed_count(), max_duration=30)


def test_tenant_query_memory_pressure_with_churn(connection, test_name):
    # Gap A+C (added after the holistic test-coverage review): the stress test above pressures memory with
    # a transient query on the always-HOT DEFAULT database, while the churned tenants stay tiny. This test
    # closes the gap the review flagged: the memory-consuming query runs ON the tenant that is being
    # SUSPENDed/RESUMEd, so a ~96 MiB allocation is in flight in tenant T's query context at the instant a
    # churner tears T down (SUSPEND) or rebuilds it (RESUME). The feature must stay crash-free and lossless:
    # the query either wins (suspend waits on the active connection) or loses (it hits the cold seam / a
    # mid-flight teardown) — both are retriable, never a terminate or a half-state. RESUME here also rebuilds
    # while the instance is under real pressure, exercising the resume-rebuild-OOM leave-COLD path with live
    # contention (not the durability-corruption stand-in the unit test uses).
    instances = {
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
                "--memory-limit=512",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [],
        }
    }
    interactive_mg_runner.start_all(instances, keep_directories=False)

    tenants = ["A", "B", "C", "D"]
    base = 5
    setup_cursor = connection(BOLT_PORTS["main"], "main").cursor()
    for t in tenants:
        create_and_populate(setup_cursor, t, base)
    execute_and_fetch_all(setup_cursor, "USE DATABASE memgraph;")

    committed = {t: base for t in tenants}
    committed_lock = threading.Lock()
    unexpected = []
    suspends_ok = [0]
    resumes_ok = [0]
    cold_hits = [0]  # times the heavy query lost the race and hit the cold seam (proves the on-tenant race fired)
    oom_hits = [0]  # times an allocation tripped the ceiling (proves the memory dimension is non-vacuous)
    stop = threading.Event()

    BATCH = 25
    BIG = "x" * 128
    CAP = 2000

    def writer(t):
        cur = connection(BOLT_PORTS["main"], "main").cursor()
        while not stop.is_set():
            # Switch the session's CURRENT database to the tenant, then run the ~96 MiB transient allocator
            # IN THE TENANT'S CONTEXT. Four writers crossing the 512 MiB ceiling trip OOM (retriable); and
            # the allocation now races a SUSPEND/RESUME of this very tenant — the gap the default-DB version
            # could not reach. If the churner took the tenant COLD first, the USE/heavy query hits the cold
            # seam; if the query is mid-flight, SUSPEND must wait for it (active connection), never tear out
            # from under it.
            try:
                execute_and_fetch_all(cur, f"USE DATABASE {t};")
                execute_and_fetch_all(cur, "WITH range(1, 6000000) AS r RETURN size(r);")
            except mgclient.DatabaseError as e:
                msg = str(e).lower()
                if "is suspended (cold)" in msg:
                    cold_hits[0] += 1
                if "memory limit exceeded" in msg:
                    oom_hits[0] += 1
                _classify(e, unexpected)
            # A modest counted write to the tenant when HOT and below the cap (data-loss accounting).
            try:
                with committed_lock:
                    grow = committed[t] < CAP
                if grow:
                    execute_and_fetch_all(cur, f"USE DATABASE {t};")
                    execute_and_fetch_all(cur, f"UNWIND range(1, {BATCH}) AS i CREATE (n:N {{v: '{BIG}'}});")
                    with committed_lock:
                        committed[t] += BATCH
            except mgclient.DatabaseError as e:
                _classify(e, unexpected)
            finally:
                # Release the tenant so suspends can reach sole-accessor (otherwise the open session pins it
                # HOT and suspend would ALWAYS lose). Alternating hold/release is what makes both race
                # outcomes reachable.
                try:
                    execute_and_fetch_all(cur, "USE DATABASE memgraph;")
                except mgclient.DatabaseError:
                    pass

    def churner(action, counter):
        cur = connection(BOLT_PORTS["main"], "main").cursor()
        while not stop.is_set():
            t = random.choice(tenants)
            try:
                execute_and_fetch_all(cur, f"{action} DATABASE {t};")
                counter[0] += 1
            except mgclient.DatabaseError as e:
                _classify(e, unexpected)
            time.sleep(0.03)

    worker_errors = []  # any exception that escapes a worker body (crash disconnect, assert, ...) -> hard fail
    threads = [threading.Thread(target=_supervised(lambda t=t: writer(t), worker_errors)) for t in tenants]
    threads.append(threading.Thread(target=_supervised(lambda: churner("SUSPEND", suspends_ok), worker_errors)))
    threads.append(threading.Thread(target=_supervised(lambda: churner("RESUME", resumes_ok), worker_errors)))
    for th in threads:
        th.start()
    # Churn until every non-vacuity condition has actually fired (a SUSPEND/RESUME won, the heavy query both
    # OOMed and hit the cold seam), bounded — so the on-tenant race is provably exercised, not luck of a slice.
    _run_churn(
        stop,
        lambda: suspends_ok[0] > 0 and resumes_ok[0] > 0 and oom_hits[0] > 0 and cold_hits[0] > 0,
    )
    for th in threads:
        th.join(timeout=30)
    assert all(not th.is_alive() for th in threads), "a stress worker did not stop within the join timeout"
    # A worker that died on a non-DatabaseError (e.g. a connection drop from a server crash) would otherwise
    # be invisible; surface it as a failure rather than letting survivors carry the test to green.
    assert not worker_errors, f"a stress worker died on an unhandled exception: {worker_errors[:10]}"

    # 1. NO CRASH.
    live = connection(BOLT_PORTS["main"], "main").cursor()
    assert execute_and_fetch_all(live, "RETURN 1;")[0][0] == 1, "MAIN must still be alive after the stress"
    # 2. NO UNEXPECTED ERROR (every error was a known retriable outcome).
    assert not unexpected, f"non-retriable error(s) surfaced during the stress: {unexpected[:10]}"
    # 3. The race was actually exercised on the tenants (not vacuously green):
    #    - suspends AND resumes both succeeded (system-tx churn happened),
    #    - the memory ceiling was genuinely hit (the heavy on-tenant query OOM'd at least once),
    #    - and at least one heavy query lost the race to a suspend and hit the cold seam — i.e. a
    #      memory-consuming query and a SUSPEND of the same tenant actually collided.
    assert suspends_ok[0] > 0, "no SUSPEND ever succeeded — the race was not exercised"
    assert resumes_ok[0] > 0, "no RESUME ever succeeded — the race was not exercised"
    assert oom_hits[0] > 0, "the memory ceiling was never hit — the memory dimension is vacuous; tighten it"
    assert cold_hits[0] > 0, "no heavy query ever raced a SUSPEND of its tenant — the on-tenant race did not fire"
    # 4. NO DATA LOSS: resume every tenant HOT and assert exactly the committed count.
    execute_and_fetch_all(live, "USE DATABASE memgraph;")
    for t in tenants:

        def resumed_count(tt=t):
            def f():
                try:
                    execute_and_fetch_all(live, f"RESUME DATABASE {tt};")
                except mgclient.DatabaseError as e:
                    low = str(e).lower()
                    if "not suspended" not in low and "failed to recover" not in low:
                        raise
                finally:
                    try:
                        execute_and_fetch_all(live, "USE DATABASE memgraph;")
                    except mgclient.DatabaseError:
                        pass
                return tenant_probe(live, tt)()

            return f

        with committed_lock:
            expected = committed[t]
        mg_sleep_and_assert(expected, resumed_count(), max_duration=30)


def test_suspend_reclaims_memory_under_ceiling(connection, test_name):
    # Gap D (deterministic): the core value proposition of hot/cold under a memory ceiling is that
    # SUSPENDing a tenant RETURNS its resident memory to the tracker, so work that could not fit while the
    # tenant was HOT fits once it is COLD. Proven end to end and deterministically (no threads):
    #   1. A fixed transient allocator query Q fits on the default DB (baseline headroom exists).
    #   2. Grow a tenant T (HOT) until Q no longer fits — T's tracked resident memory + Q now exceeds the
    #      ceiling, so Q OOMs. (Auto-tuned: we grow until OOM rather than hard-coding a size, so the test
    #      is robust to allocator/tracker accounting instead of guessing exact byte budgets.)
    #   3. SUSPEND T. If teardown truly frees T's memory, Q fits again.
    #   4. Q must now succeed. (If suspend did NOT reclaim the memory, Q would keep OOMing and this fails —
    #      that is the assertion with teeth.)
    instances = {
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
                # 1024 MiB: memgraph's base resident is ~100-150 MiB. A wide ceiling gives a comfortable
                # window — base + Q sit well under it, while a few hundred MiB of tenant data tips Q over —
                # so the proof is margin-based, not a knife-edge that flakes on allocator jitter.
                "--memory-limit=1024",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [],
        }
    }
    interactive_mg_runner.start_all(instances, keep_directories=False)
    cur = connection(BOLT_PORTS["main"], "main").cursor()

    # A transient allocator query whose peak (one large vector materialised by size()) fits comfortably on a
    # near-empty instance but is big enough that a few hundred MiB of resident tenant data pushes its peak
    # over the 1024 MiB ceiling. Measured against this binary: range(1, 3.5M) fits at a ~10 MiB baseline and
    # OOMs once the global tracker carries a few hundred MiB of tenant data. The exact byte size is NOT
    # hard-coded — step 2 grows T until Q is observed to OOM, so the test self-calibrates to the allocator.
    Q = "WITH range(1, 3500000) AS r RETURN size(r);"

    def q_ooms():
        execute_and_fetch_all(cur, "USE DATABASE memgraph;")
        try:
            execute_and_fetch_all(cur, Q)
            return False
        except mgclient.DatabaseError as e:
            assert "memory limit exceeded" in str(e).lower(), f"unexpected error from Q: {e}"
            return True

    # 1. Baseline: Q fits on the empty default DB.
    execute_and_fetch_all(cur, "USE DATABASE memgraph;")
    assert not q_ooms(), "the transient query must fit on an empty instance (ceiling too low for the baseline)"

    # 2. Grow T (HOT) until Q no longer fits in STEADY STATE. With WAL enabled, a freshly-committed CREATE
    #    batch leaves transient delta/WAL buffers that briefly inflate the tracker and drain on the next DB
    #    switch, so a single q_ooms() right after a batch can be a false positive; settle, then require Q to
    #    OOM on a re-check before declaring the tip reached. Bounded so a mis-sized ceiling fails loudly.
    def q_ooms_steady():
        if not q_ooms():
            return False
        time.sleep(1.0)  # let the last batch's transient memory drain
        return q_ooms()

    execute_and_fetch_all(cur, "CREATE DATABASE T;")
    # Fat DISTINCT payload (~16 KiB, globally unique per node via the running offset) so the tenant accrues
    # full-size resident memory per node and climbs to a few hundred MiB in a handful of batches. Distinctness
    # matters: identical values dedup/accrue less per node, so a unique prefix keeps every value its full size.
    PAD = "x" * 16384
    BATCH = 2000  # small steps so the tip is approached finely (max SUSPEND headroom), not overshot.
    grew = 0
    MAX_NODES = 2_000_000  # safety bound

    def grow_one_batch():
        # Returns True if the batch committed, False if it tripped the ceiling (T is now near the limit).
        nonlocal grew
        execute_and_fetch_all(cur, "USE DATABASE T;")
        try:
            execute_and_fetch_all(
                cur, f"UNWIND range(1, {BATCH}) AS i CREATE (n:N {{v: toString({grew} + i) + '{PAD}'}});"
            )
            grew += BATCH
            return True
        except mgclient.DatabaseError as e:
            assert "memory limit exceeded" in str(e).lower(), f"unexpected error while growing T: {e}"
            return False
        finally:
            try:
                execute_and_fetch_all(cur, "USE DATABASE memgraph;")
            except mgclient.DatabaseError:
                pass

    # Grow until Q steadily OOMs with T HOT (the tip). We STOP at the tip — never overshoot toward the
    # ceiling. Overshooting would leave no headroom for SUSPEND's own (small) teardown allocations, so SUSPEND
    # itself would OOM; stopping at the first steady tip keeps a full Q-worth of headroom for the suspend.
    tipped = False
    while grew < MAX_NODES:
        if not grow_one_batch():
            tipped = True  # a batch tripped the ceiling -> T is at the limit; Q will surely OOM
            break
        time.sleep(0.4)  # settle WAL/delta transients so q_ooms() reads true resident pressure
        if q_ooms_steady():
            tipped = True
            break
    assert tipped, "T never grew enough to push Q over the ceiling within the node bound"

    # 3. SUSPEND T -> its storage is torn down and (the claim) its memory returned to the tracker. At the tip
    #    the instance is under pressure, so SUSPEND's own teardown allocations may momentarily lose the race
    #    for the last few MiB; retry across the asynchronous drain instead of demanding it win first try.
    suspended = False
    for _ in range(8):
        execute_and_fetch_all(cur, "USE DATABASE memgraph;")
        try:
            execute_and_fetch_all(cur, "SUSPEND DATABASE T;")
            suspended = True
            break
        except mgclient.DatabaseError as e:
            assert "memory limit exceeded" in str(e).lower(), f"unexpected error from SUSPEND: {e}"
            time.sleep(1.5)
    assert suspended, "SUSPEND never succeeded under pressure (teardown could not reclaim headroom)"
    assert tenant_probe(cur, "T")() == "COLD"

    # 4. FUNCTIONAL TEETH: the work that could not fit while T was HOT now fits. This is the proof with
    #    teeth — Q needs a large transient chunk, so its succeeding after SUSPEND (when it OOMed steadily
    #    before) means SUSPEND returned enough headroom to the tracker for previously-impossible work to run.
    #    If SUSPEND did NOT reclaim, Q would keep OOMing and this would fail. Retry: teardown frees memory
    #    asynchronously (jemalloc may not return pages to the tracker instantly, and SUSPEND's own snapshot
    #    flush leaves transient buffers that drain over a few seconds). A direct byte-count assertion on the
    #    tracker is deliberately avoided: jemalloc's page-return-to-tracker timing is nondeterministic, so the
    #    settled figure varies run to run — the functional "Q now fits" is the robust, allocator-agnostic proof.
    def q_fits_now():
        return not q_ooms()

    mg_sleep_and_assert(
        True,
        q_fits_now,
        max_duration=20,
    )


def test_tenant_churn_under_memory_pressure_replicated(connection, test_name):
    # Gap E: the replicated counterpart of the memory-pressure churn. An ASYNC replica must apply the
    # SUSPEND/RESUME RPCs (tenant teardown / rebuild on the replica side) and the replicated data while MAIN
    # is hammered by on-tenant memory-heavy queries + suspend/resume churn under a hard ceiling. The replica
    # must stay crash-free throughout, and after the stress a reconnect-driven SystemRecovery must fully
    # reconcile it to MAIN's per-tenant counts — i.e. the replica's apply-handler teardown/rebuild leaves it
    # in a state SystemRecovery can repair, never crashed or unrecoverably corrupted. (Exact steady-state
    # equality under ASYNC + churn is NOT guaranteed without a reconnect — see the convergence step below.)
    instances = {
        "replica_1": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_1']}",
                "--log-level=TRACE",
                "--memory-limit=512",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica1",
            "setup_queries": [],
        },
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
                "--memory-limit=512",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [],
        },
    }
    interactive_mg_runner.start_all(instances, keep_directories=False)

    replica_cursor = connection(BOLT_PORTS["replica_1"], "replica_1").cursor()
    set_replica_role(replica_cursor)
    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()
    # ASYNC: a struggling/pressured replica must not stall MAIN's SUSPEND/RESUME commits; convergence is
    # eventual and asserted after the churn window.
    register_replica(main_cursor, sync=False)

    tenants = ["A", "B", "C", "D"]
    base = 5
    for t in tenants:
        create_and_populate(main_cursor, t, base)
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    # Let the initial creates replicate before the churn starts.
    for t in tenants:
        mg_sleep_and_assert(base, tenant_probe(replica_cursor, t))

    committed = {t: base for t in tenants}
    committed_lock = threading.Lock()
    unexpected = []
    suspends_ok = [0]
    resumes_ok = [0]
    stop = threading.Event()

    BATCH = 25
    BIG = "x" * 128
    CAP = 2000

    def writer(t):
        cur = connection(BOLT_PORTS["main"], "main").cursor()
        while not stop.is_set():
            try:
                execute_and_fetch_all(cur, f"USE DATABASE {t};")
                execute_and_fetch_all(cur, "WITH range(1, 6000000) AS r RETURN size(r);")
            except mgclient.DatabaseError as e:
                _classify(e, unexpected)
            try:
                with committed_lock:
                    grow = committed[t] < CAP
                if grow:
                    execute_and_fetch_all(cur, f"USE DATABASE {t};")
                    execute_and_fetch_all(cur, f"UNWIND range(1, {BATCH}) AS i CREATE (n:N {{v: '{BIG}'}});")
                    with committed_lock:
                        committed[t] += BATCH
            except mgclient.DatabaseError as e:
                _classify(e, unexpected)
            finally:
                try:
                    execute_and_fetch_all(cur, "USE DATABASE memgraph;")
                except mgclient.DatabaseError:
                    pass

    def churner(action, counter):
        cur = connection(BOLT_PORTS["main"], "main").cursor()
        while not stop.is_set():
            t = random.choice(tenants)
            try:
                execute_and_fetch_all(cur, f"{action} DATABASE {t};")
                counter[0] += 1
            except mgclient.DatabaseError as e:
                _classify(e, unexpected)
            time.sleep(0.03)

    worker_errors = []  # any exception that escapes a worker body (crash disconnect, assert, ...) -> hard fail
    threads = [threading.Thread(target=_supervised(lambda t=t: writer(t), worker_errors)) for t in tenants]
    threads.append(threading.Thread(target=_supervised(lambda: churner("SUSPEND", suspends_ok), worker_errors)))
    threads.append(threading.Thread(target=_supervised(lambda: churner("RESUME", resumes_ok), worker_errors)))
    for th in threads:
        th.start()
    # Churn until both a SUSPEND and a RESUME have actually won (bounded), so the replicated teardown/rebuild
    # apply path is provably exercised rather than relying on an arbitrary fixed window landing a winner.
    _run_churn(stop, lambda: suspends_ok[0] > 0 and resumes_ok[0] > 0)
    for th in threads:
        th.join(timeout=30)
    assert all(not th.is_alive() for th in threads), "a stress worker did not stop within the join timeout"
    assert not worker_errors, f"a stress worker died on an unhandled exception: {worker_errors[:10]}"

    # NO CRASH on either node.
    main_live = connection(BOLT_PORTS["main"], "main").cursor()
    assert execute_and_fetch_all(main_live, "RETURN 1;")[0][0] == 1, "MAIN must still be alive"
    replica_live = connection(BOLT_PORTS["replica_1"], "replica_1").cursor()
    assert execute_and_fetch_all(replica_live, "RETURN 1;")[0][0] == 1, "REPLICA must still be alive"
    assert not unexpected, f"non-retriable error(s) surfaced during the stress: {unexpected[:10]}"
    assert suspends_ok[0] > 0 and resumes_ok[0] > 0, "the suspend/resume churn was not exercised"

    # Quiesce MAIN to its committed state: resume every tenant HOT and confirm MAIN's own counts first.
    execute_and_fetch_all(main_live, "USE DATABASE memgraph;")
    for t in tenants:

        def main_resumed(tt=t):
            def f():
                try:
                    execute_and_fetch_all(main_live, f"RESUME DATABASE {tt};")
                except mgclient.DatabaseError as e:
                    low = str(e).lower()
                    if "not suspended" not in low and "failed to recover" not in low:
                        raise
                finally:
                    try:
                        execute_and_fetch_all(main_live, "USE DATABASE memgraph;")
                    except mgclient.DatabaseError:
                        pass
                return tenant_probe(main_live, tt)()

            return f

        with committed_lock:
            expected = committed[t]
        mg_sleep_and_assert(expected, main_resumed(), max_duration=30)

    # CONVERGENCE: force a reconnect-driven SystemRecovery, then assert the replica reconciles to MAIN's
    # per-tenant counts. This is deliberate: under ASYNC, writes MAIN streamed for a tenant while the
    # replica's copy was momentarily COLD (a resume lagging the churn) cannot apply on the replica and plain
    # ASYNC catch-up never backfills them — only the SystemRecovery that a (re)registration triggers
    # re-streams MAIN's authoritative state. So we DROP + re-REGISTER the replica (the supported way to
    # repair a diverged ASYNC replica) and require it to converge. The assertion proves the replica's
    # apply-handler teardown/rebuild left it in a state SystemRecovery can fully reconcile after the stress.
    execute_and_fetch_all(main_live, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_live, "DROP REPLICA replica_1;")
    register_replica(main_live, sync=False)
    for t in tenants:
        with committed_lock:
            expected = committed[t]
        mg_sleep_and_assert(expected, tenant_probe(replica_cursor, t), max_duration=90)


def count_ttl_nodes(cursor, db_name):
    """Count :TTL nodes on db_name; resets the session to the default db afterwards so it
    does not pin the tenant HOT (which would block SUSPEND)."""

    def func():
        try:
            execute_and_fetch_all(cursor, f"USE DATABASE {db_name};")
            return execute_and_fetch_all(cursor, "MATCH (n:TTL) RETURN count(n);")[0][0]
        finally:
            try:
                execute_and_fetch_all(cursor, "USE DATABASE memgraph;")
            except mgclient.DatabaseError:
                pass

    return func


def test_ttl_restarts_after_resume(connection, test_name):
    """Regression: TTL scheduler must restart after RESUME; the on_resume_ arm re-installs the TTL
    user-check (IsMainWriteable). Pre-fix, TTL nodes created after a RESUME were never deleted because
    the scheduler's per-tenant user-check was not re-installed when the tenant came back HOT. Post-fix,
    the scheduler fires again within the configured interval and clears them.

    Note: the replica-side invariant "TTL must NOT delete on a REPLICA" is guaranteed by construction:
    the re-installed user-check is IsMainWriteable, which is false on a replica, so a replica that
    mirrors MAIN's already-deleted state via replication can never double-delete. That property is not
    separately asserted here because a single-MAIN test has no replica.
    """
    # Single MAIN, no cross-restart recovery needed for this regression path.
    instances = {"main": main_args(test_name)}
    interactive_mg_runner.start_all(instances, keep_directories=False)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # Create tenant A with 3 permanent (non-TTL) nodes that serve as a data-integrity witness:
    # TTL must never touch nodes without a ttl property, and the 3 must be present at every
    # HOT checkpoint below.
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    for _ in range(3):
        execute_and_fetch_all(main_cursor, "CREATE ();")
    # Enable TTL with a 1-second tick so the scheduler runs frequently enough for the test.
    execute_and_fetch_all(main_cursor, 'ENABLE TTL EVERY "1s";')
    # Leave the session on the default DB so the tenant is not pinned for SUSPEND later.
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")

    # --- PHASE 1: TTL works while the tenant is HOT ---
    # Write 10 expired :TTL nodes (ttl:0 = epoch 0 = already expired).
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "UNWIND RANGE(1,10) AS i CREATE (:TTL{ttl:0});")
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    # Poll until TTL has reaped all 10 expired nodes; the 3 permanent nodes are untouched (no ttl prop).
    mg_sleep_and_assert(0, count_ttl_nodes(main_cursor, "A"))

    # --- SUSPEND ---
    # Session is already on memgraph (count_ttl_nodes resets it in finally), so SUSPEND can proceed.
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "SUSPEND DATABASE A;")
    assert tenant_probe(main_cursor, "A")() == "COLD"

    # --- RESUME ---
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "RESUME DATABASE A;")
    # Wait for the tenant to come back HOT; the 3 permanent nodes must be intact.
    mg_sleep_and_assert(3, tenant_probe(main_cursor, "A"))

    # --- PHASE 2: THE REGRESSION TEETH ---
    # Write fresh expired :TTL nodes AFTER the RESUME and assert the TTL scheduler deletes them.
    # PRE-FIX: mg_sleep_and_assert times out here because the on_resume_ arm did not re-install the
    # user-check, so the TTL scheduler never fires on the resumed tenant and the count stays at 10.
    # POST-FIX: the scheduler re-starts within the 1s tick and clears all 10 nodes; this passes.
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "UNWIND RANGE(1,10) AS i CREATE (:TTL{ttl:0});")
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    mg_sleep_and_assert(0, count_ttl_nodes(main_cursor, "A"))

    # Final integrity check: only the 3 permanent nodes remain; TTL did not over-delete.
    assert tenant_probe(main_cursor, "A")() == 3, "permanent nodes must survive after TTL reaping post-resume"


def test_drop_cold_tenant_converges(connection, test_name):
    # H1 (replica DropDatabaseRpc → cold-aware Delete(uuid)):
    #   Before the fix, a replica that held tenant A COLD would never process a DropDatabaseRpc issued
    #   while A was suspended, because the delete path only walked the HOT set. The replica was left
    #   with a cold ghost that tenant_probe returned as "COLD" forever rather than "MISSING".
    #   After the fix, DropDatabaseRpc calls a cold-aware Delete(uuid) that also reaps the cold shell.
    #
    # H4 (SystemRecovery leftover-delete sees COLD):
    #   This path is exercised by the companion test below; see test_drop_cold_tenant_while_replica_down_converges.
    #
    # Phase 1 — online drop (H1 replica apply path):
    #   MAIN suspends A then drops it while the replica is online. The DropDatabaseRpc must cause the
    #   replica to remove the cold ghost; tenant_probe must transition from "COLD" to "MISSING".
    instances = {
        "replica_1": replica_args(test_name, recovery=False),
        "main": main_args(test_name),
    }
    interactive_mg_runner.start_all(instances, keep_directories=False)

    replica_cursor = connection(BOLT_PORTS["replica_1"], "replica_1").cursor()
    set_replica_role(replica_cursor)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()
    # SYNC: the DROP must complete on the replica before returning to MAIN, so the "MISSING" assertion
    # does not race the RPC in-flight.
    register_replica(main_cursor, sync=True)

    # Create + populate tenant A on MAIN; it replicates to the (HOT) replica.
    create_and_populate(main_cursor, "A", 5)
    mg_sleep_and_assert(5, tenant_probe(replica_cursor, "A"))

    # SUSPEND A on MAIN -> streams SuspendDatabaseRpc, replica copy torn down to COLD.
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "SUSPEND DATABASE A;")
    mg_sleep_and_assert("COLD", tenant_probe(replica_cursor, "A"))
    # MAIN's own copy is COLD too.
    assert tenant_probe(main_cursor, "A")() == "COLD"

    # DROP A on MAIN while it is COLD. This must succeed locally (H1 local cold-aware drop), and the
    # DropDatabaseRpc sent to the replica must remove the cold ghost there too (H1 replica apply path).
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "DROP DATABASE A;")

    # Replica must have reaped the cold ghost (pre-fix: stays "COLD" forever).
    mg_sleep_and_assert("MISSING", tenant_probe(replica_cursor, "A"))
    # MAIN's own view must also be "MISSING".
    assert tenant_probe(main_cursor, "A")() == "MISSING"


def test_drop_cold_tenant_while_replica_down_converges(connection, test_name):
    # H4 (SystemRecovery leftover-delete sees COLD):
    #   Before the fix, the V3 SystemRecovery leftover-delete loop only walked MAIN's HOT set when
    #   seeding the "tenants MAIN no longer has" set. A tenant dropped while it was COLD was absent from
    #   both MAIN's HOT set and MAIN's COLD set at reconnect time (it had already been destroyed), so the
    #   replica's cold ghost was never included in the leftover-delete and survived indefinitely.
    #   After the fix, the leftover-delete seeds from HOT∪COLD (including the cross-restart COLD recovery
    #   of B from the replica's own disk), so it reaps the ghost on reconnect.
    #
    # Sequence:
    #   1. Fresh instances; replica registered ASYNC.
    #   2. Create + populate tenant B; wait replica HOT.
    #   3. SUSPEND B on MAIN; wait replica COLD.
    #   4. Kill the replica (keep its data dir so it recovers B COLD on restart via --data-recovery-on-startup).
    #   5. DROP B on MAIN while the replica is offline (DropDatabaseRpc lost).
    #   6. Restart the replica; it recovers B as a COLD shell from its own disk.
    #   7. On reconnect MAIN runs SystemRecovery (V3); the H4 leftover-delete seeds from HOT∪COLD and
    #      includes B (which MAIN no longer holds at all) -> the replica reaps the cold ghost.
    #   8. tenant_probe must converge to "MISSING".
    instances = {
        "replica_1": replica_args(test_name, recovery=True),
        "main": main_args(test_name),
    }
    interactive_mg_runner.start_all(instances, keep_directories=False)

    replica_cursor = connection(BOLT_PORTS["replica_1"], "replica_1").cursor()
    set_replica_role(replica_cursor)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()
    # ASYNC: DROP must not block on the down replica.
    register_replica(main_cursor, sync=False)

    create_and_populate(main_cursor, "B", 6)
    mg_sleep_and_assert(6, tenant_probe(replica_cursor, "B"))

    # SUSPEND B on MAIN; wait for the replica copy to go COLD.
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "SUSPEND DATABASE B;")
    mg_sleep_and_assert("COLD", tenant_probe(replica_cursor, "B"))
    assert tenant_probe(main_cursor, "B")() == "COLD"

    # Take the replica down; keep its data directory so it recovers B COLD from disk on restart.
    interactive_mg_runner.kill(instances, "replica_1", keep_directories=True)

    # DROP B on MAIN while the replica is offline. The DropDatabaseRpc is lost; only the V3
    # SystemRecovery leftover-delete can reap the cold ghost on the next reconnect (H4).
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "DROP DATABASE B;")
    assert tenant_probe(main_cursor, "B")() == "MISSING"

    # Restart the replica with recovery flags. It recovers B as a COLD shell from its own disk
    # (--data-recovery-on-startup) and restores its REPLICA role (--replication-restore-state-on-startup)
    # so MAIN reconnects and runs SystemRecovery (V3).
    interactive_mg_runner.start(instances, "replica_1")
    replica_cursor = connection(BOLT_PORTS["replica_1"], "replica_1").cursor()

    # H4: the SystemRecovery leftover-delete now seeds from HOT∪COLD. MAIN holds neither a HOT nor a
    # COLD copy of B (it was dropped), so B is in the leftover set, and the replica reaps the cold ghost.
    # Pre-fix: the ghost survived as "COLD" forever because the leftover-delete only walked the HOT set.
    mg_sleep_and_assert("MISSING", tenant_probe(replica_cursor, "B"))


def test_main_crash_in_suspend_gap_converges(connection, test_name):
    # T4 / L4 (crash window between finish_suspend() and the SuspendDatabase replication action):
    #   Suspend_ flips the tenant durably COLD (finish_suspend writes the cold marker) BEFORE the
    #   SuspendDatabase action is appended to the replication stream. If MAIN crashes in that gap, the
    #   suspend is durable locally but was NEVER replicated as an action — neither delivered to the
    #   replica nor recorded in MAIN's stream. Convergence must then come SOLELY from the durable COLD
    #   marker surfacing through the V3 SystemRecovery payload on (re)connect (the BEHIND -> reconcile
    #   path), NOT from replaying the action. The synthesis adjudicated this self-healing; this is the
    #   explicit coverage e2e it asked for.
    #
    #   We cannot hook the exact in-process gap from e2e, but we reproduce its OBSERVABLE end-state
    #   precisely — MAIN durably COLD, replica never saw a suspend action, MAIN itself crashed+recovered
    #   mid-flight — by taking the replica down, suspending on MAIN, then crashing+restarting MAIN. The
    #   suspend therefore reaches the replica only via SystemRecovery, after a MAIN restart.
    #
    # Sequence:
    #   1. main + replica, tenant A replicated HOT; replica ASYNC (SUSPEND must not block on it). Both
    #      recover state on restart; MAIN also restores its replica registration + role and auto-reconnects.
    #   2. Kill the replica (keep its dir so it recovers A HOT on restart).
    #   3. SUSPEND A on MAIN -> A COLD durably on MAIN; the SuspendDatabaseRpc is lost (replica down).
    #   4. Kill MAIN (the crash) keeping its dir.
    #   5. Restart the replica FIRST, while MAIN is still down -> it recovers its STALE A HOT copy from its
    #      own disk and restores its REPLICA role. Asserted HOT here: with no MAIN up nothing can converge
    #      it yet, so this deterministically witnesses the divergent copy SystemRecovery must later fix.
    #   6. Restart MAIN -> it recovers A COLD from its durable marker and (restore-state) restores its
    #      registration of replica_1, auto-reconnecting to the already-up replica. The suspend now exists
    #      ONLY as durable state on MAIN — never as a streamed/recorded action.
    #   7. On reconnect MAIN runs SystemRecovery (V3); A is in MAIN's COLD set, so the replica force-
    #      suspends its HOT A to COLD (SR-1'(2)) driven purely by the recovery payload, not an action.
    #      tenant_probe must converge the replica to "COLD".
    #   8. A subsequent RESUME on MAIN still converges the replica back to HOT with data intact.
    instances = {
        "replica_1": replica_args(test_name, recovery=True),
        "main": main_args(test_name, recovery=True, restore_replication=True),
    }
    interactive_mg_runner.start_all(instances, keep_directories=False)

    replica_cursor = connection(BOLT_PORTS["replica_1"], "replica_1").cursor()
    set_replica_role(replica_cursor)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()
    register_replica(main_cursor, sync=False)  # ASYNC: SUSPEND must not block on the (soon) down replica

    create_and_populate(main_cursor, "A", 7)
    mg_sleep_and_assert(7, tenant_probe(replica_cursor, "A"))

    # Take the replica down BEFORE the suspend (keep its dir so it recovers A HOT on restart).
    interactive_mg_runner.kill(instances, "replica_1", keep_directories=True)

    # SUSPEND A on MAIN while the replica is offline -> A COLD durably on MAIN; action not delivered.
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "SUSPEND DATABASE A;")
    assert tenant_probe(main_cursor, "A")() == "COLD"

    # Crash MAIN (keep its dir).
    interactive_mg_runner.kill(instances, "main", keep_directories=True)

    # Bring the replica back FIRST, while MAIN is still down: it recovers its STALE HOT copy of A from
    # disk and restores its REPLICA role. With no MAIN up, nothing can converge it yet, so the HOT
    # assertion is stable and witnesses the divergence SystemRecovery must repair.
    interactive_mg_runner.start(instances, "replica_1")
    replica_cursor = connection(BOLT_PORTS["replica_1"], "replica_1").cursor()
    assert tenant_probe(replica_cursor, "A")() == 7, "the replica must recover its pre-crash HOT copy of A"

    # Restart MAIN: it recovers A COLD from its durable marker and restores its replica registration,
    # auto-reconnecting to the already-up replica (the real crash-recovery path). The suspend now exists
    # ONLY as durable state on MAIN, never as a streamed action.
    interactive_mg_runner.start(instances, "main")
    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()
    assert (
        tenant_probe(main_cursor, "A")() == "COLD"
    ), "MAIN must recover A COLD from its durable marker after the crash"

    # On reconnect, SystemRecovery (V3) reconciles A to COLD on the replica purely from the recovery
    # payload (no suspend action was ever streamed).
    mg_sleep_and_assert("COLD", tenant_probe(replica_cursor, "A"))

    # And a subsequent RESUME on MAIN still converges the replica back to HOT with data intact.
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "RESUME DATABASE A;")
    mg_sleep_and_assert(7, tenant_probe(replica_cursor, "A"))


def test_suspend_resume_requires_multi_database_edit(connection, test_name):
    # R2: SUSPEND/RESUME DATABASE are gated on the MULTI_DATABASE_EDIT privilege (same as
    # CREATE/DROP/RENAME DATABASE; required_privileges.cpp). This is the negative control the unit
    # suite lacks: a user WITHOUT MULTI_DATABASE_EDIT must be denied, and granting it must flip the
    # query to allowed (so the privilege is the actual gate, not some other check).
    instances = {"main": main_args(test_name)}
    interactive_mg_runner.start_all(instances, keep_directories=False)

    # Before any user exists the connection is the implicit admin: do the data + user setup here.
    admin_cursor = connection(BOLT_PORTS["main"], "main").cursor()
    create_and_populate(admin_cursor, "A", 5)
    execute_and_fetch_all(admin_cursor, "USE DATABASE memgraph;")
    # NB: "admin" is a built-in role name (ambiguous in GRANT), so the admin user is named dbadmin.
    execute_and_fetch_all(admin_cursor, "CREATE USER dbadmin IDENTIFIED BY 'dbadmin';")
    execute_and_fetch_all(admin_cursor, "GRANT ALL PRIVILEGES TO dbadmin;")
    execute_and_fetch_all(admin_cursor, "GRANT DATABASE * TO dbadmin;")

    # Creating the first user downgrades the anonymous connection; reconnect as the real admin.
    admin_cursor = connection(BOLT_PORTS["main"], "main", username="dbadmin", password="dbadmin").cursor()
    # `limited` gets database access to A but NOT the MULTI_DATABASE_EDIT privilege. Granting DATABASE *
    # rules out "no DB access" as the reason for denial, isolating MULTI_DATABASE_EDIT as the gate.
    execute_and_fetch_all(admin_cursor, "CREATE USER limited IDENTIFIED BY 'limited';")
    execute_and_fetch_all(admin_cursor, "GRANT DATABASE * TO limited;")

    limited_cursor = connection(BOLT_PORTS["main"], "main", username="limited", password="limited").cursor()
    for query in ("SUSPEND DATABASE A;", "RESUME DATABASE A;"):
        with pytest.raises(mgclient.DatabaseError) as exc:
            execute_and_fetch_all(limited_cursor, query)
        assert (
            "not authorized" in str(exc.value).lower()
        ), f"{query} must be denied without MULTI_DATABASE_EDIT: {exc.value}"

    # A is untouched by the denied attempts — still HOT.
    rows = dict((r[0], r[1]) for r in execute_and_fetch_all(admin_cursor, "SHOW DATABASES;"))
    assert rows.get("A") == "HOT", f"a denied SUSPEND must not change state: {rows}"

    # Grant the privilege; the same user can now suspend and resume.
    execute_and_fetch_all(admin_cursor, "GRANT MULTI_DATABASE_EDIT TO limited;")
    limited_cursor = connection(BOLT_PORTS["main"], "main", username="limited", password="limited").cursor()
    execute_and_fetch_all(limited_cursor, "SUSPEND DATABASE A;")
    rows = dict((r[0], r[1]) for r in execute_and_fetch_all(admin_cursor, "SHOW DATABASES;"))
    assert rows.get("A") == "COLD", f"SUSPEND must succeed once MULTI_DATABASE_EDIT is granted: {rows}"
    execute_and_fetch_all(limited_cursor, "RESUME DATABASE A;")

    # Verify (and make dbadmin the LAST connection, so the conftest teardown's DETACH DELETE runs as a
    # privileged user rather than `limited`).
    admin_cursor = connection(BOLT_PORTS["main"], "main", username="dbadmin", password="dbadmin").cursor()
    assert tenant_probe(admin_cursor, "A")() == 5, "RESUME must rebuild A with its data once privileged"


def test_default_database_cannot_be_suspended(connection, test_name):
    # The default database "memgraph" is a system database (it backs auth, multi-tenancy metadata, etc.,
    # not just a user graph) and must never be suspendable. The engine rejects it (SuspendError::DEFAULT_DB);
    # this asserts the query layer surfaces that as a clean error and leaves the database HOT.
    instances = {"main": main_args(test_name)}
    interactive_mg_runner.start_all(instances, keep_directories=False)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()
    # Create some data so we can prove the default DB is fully usable before and after the rejected SUSPEND.
    execute_and_fetch_all(main_cursor, "CREATE (), (), ();")

    with pytest.raises(mgclient.DatabaseError) as exc:
        execute_and_fetch_all(main_cursor, "SUSPEND DATABASE memgraph;")
    assert "default database" in str(exc.value).lower(), f"expected a default-db rejection: {exc.value}"

    # memgraph stays HOT and fully usable — the rejected SUSPEND was a no-op.
    rows = dict((r[0], r[1]) for r in execute_and_fetch_all(main_cursor, "SHOW DATABASES;"))
    assert rows.get("memgraph") == "HOT", f"the default database must remain HOT after a rejected SUSPEND: {rows}"
    assert execute_and_fetch_all(main_cursor, "MATCH (n) RETURN count(*);")[0][0] == 3


def test_suspend_is_isolated_from_other_databases(connection, test_name):
    # R7: suspending database A must not disturb a concurrently-active database B. A live writer hammers
    # B with committed transactions while A is suspended (and resumed) underneath it; B must see zero
    # disruption — every write commits, B stays HOT, and its final count equals the committed total.
    instances = {"main": main_args(test_name)}
    interactive_mg_runner.start_all(instances, keep_directories=False)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()
    create_and_populate(main_cursor, "A", 9)
    create_and_populate(main_cursor, "B", 4)  # B starts with 4 nodes
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")

    stop = threading.Event()
    committed = {"n": 0}
    errors = []

    def hammer_b():
        # Independent session pinned to B for the whole run. It must never observe A's suspend.
        conn = connection(BOLT_PORTS["main"], "main")
        cur = conn.cursor()
        try:
            execute_and_fetch_all(cur, "USE DATABASE B;")
            while not stop.is_set():
                try:
                    execute_and_fetch_all(cur, "CREATE ();")
                    committed["n"] += 1
                except Exception as e:  # noqa: BLE001 — any disruption to B is a failure
                    errors.append(str(e))
                    return
                time.sleep(0.005)
        except Exception as e:  # noqa: BLE001
            errors.append(str(e))

    writer = threading.Thread(target=hammer_b)
    writer.start()
    try:
        # Let B accumulate writes, then suspend + resume A repeatedly underneath the live B writer.
        time.sleep(0.5)
        for _ in range(3):
            execute_and_fetch_all(main_cursor, "SUSPEND DATABASE A;")
            assert dict((r[0], r[1]) for r in execute_and_fetch_all(main_cursor, "SHOW DATABASES;")).get("A") == "COLD"
            time.sleep(0.2)
            execute_and_fetch_all(main_cursor, "RESUME DATABASE A;")
            time.sleep(0.2)
    finally:
        stop.set()
        writer.join(timeout=30)

    assert not errors, f"B was disrupted by A's suspend/resume: {errors[:3]}"
    assert committed["n"] > 0, "writer made no progress — test is vacuous"

    # B is HOT and holds exactly its initial 4 + every committed write. A round-trips with its data.
    rows = dict((r[0], r[1]) for r in execute_and_fetch_all(main_cursor, "SHOW DATABASES;"))
    assert rows.get("B") == "HOT", rows
    assert tenant_probe(main_cursor, "B")() == 4 + committed["n"], "B lost or gained writes during A's churn"
    assert tenant_probe(main_cursor, "A")() == 9, "A must round-trip its data through suspend/resume"


def test_drop_recreate_cold_tenant_uuid_fix_converges(connection, test_name):
    # F1 (SystemRecovery COLD loop stale-UUID regression):
    #   Before the fix, the replica's SystemRecovery COLD loop refreshed a cold tenant's stats but never
    #   its UUID. When a tenant was dropped and recreated under the same name while the replica was down,
    #   the replica's COLD shell still carried the OLD uuid. On reconnect, SystemRecovery would match the
    #   name "tenant_x" in the COLD set and update its stats, but leave the UUID stale. Because all
    #   subsequent replication traffic (data + state) is routed by UUID, the replica remained permanently
    #   BEHIND — the new tenant's data never arrived and no self-heal was possible.
    #   After the fix, the COLD loop also refreshes the UUID, so the replica's COLD shell is re-keyed to
    #   the new UUID and the new tenant's data replicates normally.
    #
    # Sequence:
    #   1. main + replica_1, ASYNC (the replica goes down mid-scenario).
    #   2. Create tenant_x on MAIN with 3 :OldLabel nodes; wait replica HOT.
    #   3. SUSPEND tenant_x on MAIN; wait replica COLD.
    #   4. Kill the replica (keep its data dir so it recovers tenant_x COLD from disk on restart).
    #   5. While the replica is DOWN:
    #        DROP DATABASE tenant_x  (destroys the old UUID)
    #        CREATE DATABASE tenant_x (mints a brand-new UUID under the same name)
    #        write 7 :NewLabel nodes to the new tenant_x
    #        SUSPEND tenant_x again (so MAIN holds the NEW tenant_x COLD)
    #   6. Restart the replica; it recovers the OLD tenant_x as a COLD shell from its own disk and
    #      restores its REPLICA role. MAIN reconnects and runs SystemRecovery (V3).
    #   7. RESUME tenant_x on MAIN (NEW uuid, NEW data); write 1 :MarkerLabel node as fresh replication
    #      bait; let it replicate to the now-converged replica.
    #   8. ASSERT that the replica ends up with the NEW tenant's data (8 nodes total: 7 :NewLabel + 1
    #      :MarkerLabel), NOT the old data (3 :OldLabel nodes). Pre-fix: the replica is permanently
    #      BEHIND (stale UUID) and tenant_probe never reaches 8; post-fix it converges.
    instances = {
        "replica_1": replica_args(test_name, recovery=True),
        "main": main_args(test_name),
    }
    interactive_mg_runner.start_all(instances, keep_directories=False)

    replica_cursor = connection(BOLT_PORTS["replica_1"], "replica_1").cursor()
    set_replica_role(replica_cursor)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()
    # ASYNC: SUSPEND and DROP must not block on the (soon) down replica.
    register_replica(main_cursor, sync=False)

    # Create the initial tenant_x with 3 :OldLabel nodes (distinguishable from the new data).
    execute_and_fetch_all(main_cursor, "CREATE DATABASE tenant_x;")
    execute_and_fetch_all(main_cursor, "USE DATABASE tenant_x;")
    for _ in range(3):
        execute_and_fetch_all(main_cursor, "CREATE (:OldLabel);")
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    # Wait for the replica to receive the initial data HOT.
    mg_sleep_and_assert(3, tenant_probe(replica_cursor, "tenant_x"))

    # SUSPEND tenant_x on MAIN; wait for the replica copy to go COLD too.
    execute_and_fetch_all(main_cursor, "SUSPEND DATABASE tenant_x;")
    mg_sleep_and_assert("COLD", tenant_probe(replica_cursor, "tenant_x"))
    assert tenant_probe(main_cursor, "tenant_x")() == "COLD"

    # Take the replica DOWN; keep its data directory so it recovers tenant_x COLD from disk on restart.
    # From this point the replica misses every operation until it is restarted in step 6.
    interactive_mg_runner.kill(instances, "replica_1", keep_directories=True)

    # ---- While the replica is offline ----
    # DROP the old tenant_x (destroys the old UUID from MAIN's perspective).
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "DROP DATABASE tenant_x;")
    assert tenant_probe(main_cursor, "tenant_x")() == "MISSING"

    # CREATE a brand-new tenant_x (fresh UUID under the same name).
    execute_and_fetch_all(main_cursor, "CREATE DATABASE tenant_x;")
    execute_and_fetch_all(main_cursor, "USE DATABASE tenant_x;")
    for _ in range(7):
        execute_and_fetch_all(main_cursor, "CREATE (:NewLabel);")
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")

    # SUSPEND the new tenant_x so MAIN holds it COLD with the new UUID.
    # This is the exact state SystemRecovery will encounter on reconnect: MAIN's COLD set contains
    # tenant_x at a UUID the replica has NEVER seen.
    execute_and_fetch_all(main_cursor, "SUSPEND DATABASE tenant_x;")
    assert tenant_probe(main_cursor, "tenant_x")() == "COLD"

    # ---- Restart the replica ----
    # It recovers the OLD tenant_x COLD shell from its own disk (stale UUID) and restores its REPLICA
    # role (--replication-restore-state-on-startup) so MAIN reconnects and runs SystemRecovery (V3).
    interactive_mg_runner.start(instances, "replica_1")
    replica_cursor = connection(BOLT_PORTS["replica_1"], "replica_1").cursor()

    # ---- Resume on MAIN and write a replication marker ----
    # RESUME the new tenant_x on MAIN (new UUID, new data). Then write 1 :MarkerLabel node so there is
    # fresh replicated data in flight that the convergence assertion can wait on.
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "RESUME DATABASE tenant_x;")
    execute_and_fetch_all(main_cursor, "USE DATABASE tenant_x;")
    execute_and_fetch_all(main_cursor, "CREATE (:MarkerLabel);")
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")

    # ASSERT CONVERGENCE: the replica must end up with the NEW tenant's data — 8 nodes total
    # (7 :NewLabel from the recreated tenant + 1 :MarkerLabel written after resume). Pre-fix: the
    # replica is pinned to the stale UUID and never converges (mg_sleep_and_assert times out here).
    # Post-fix: SystemRecovery re-keys the COLD shell to the new UUID and the new data replicates.
    mg_sleep_and_assert(8, tenant_probe(replica_cursor, "tenant_x"))

    # STRONG assertion: no :OldLabel nodes must be present (they belonged to the dropped tenant; if the
    # replica mistakenly kept the old shell, at least 3 OldLabel nodes would be here instead).
    def old_label_count():
        def func():
            try:
                execute_and_fetch_all(replica_cursor, "USE DATABASE tenant_x;")
                return execute_and_fetch_all(replica_cursor, "MATCH (n:OldLabel) RETURN count(*);")[0][0]
            except mgclient.DatabaseError as e:
                msg = str(e)
                if "suspended (cold)" in msg or "unknown database" in msg.lower() or "doesn't exist" in msg.lower():
                    return -1  # not yet converged; keep polling
                raise
            finally:
                try:
                    execute_and_fetch_all(replica_cursor, "USE DATABASE memgraph;")
                except mgclient.DatabaseError:
                    pass

        return func

    mg_sleep_and_assert(0, old_label_count())

    # Sanity: MAIN's own view of the new tenant is also correct.
    assert tenant_probe(main_cursor, "tenant_x")() == 8, "MAIN must also hold 8 nodes after the marker write"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
