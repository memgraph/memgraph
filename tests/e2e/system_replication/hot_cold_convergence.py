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

# End-to-end coverage for hot/cold tenants over replication (feature v2, commits C7 + C8) plus the
# node-local cross-restart durability path (C9).
#
#   C7 — SUSPEND/RESUME DATABASE is system-replicated: a SUSPEND on MAIN streams a SuspendDatabaseRpc
#        so each connected replica tears its own copy down to a COLD shell; RESUME rebuilds it. This
#        is exercised by test_suspend_resume_convergence (replica online the whole time).
#
#   C8 — a reconnecting/lagging replica converges to MAIN's authoritative {HOT ∪ COLD} set via the
#        V3 SystemRecovery payload (which now carries the COLD set). This is exercised by
#        test_lagging_replica_convergence: the replica is DOWN across the SUSPEND, so the
#        SuspendDatabaseRpc never reaches it; on reconnect SystemRecovery must force-suspend the
#        tenant the replica still holds HOT (SR-1 exempt-from-delete + SR-1'(2) force-suspend).
#
#   C9 — hot/cold is durable: a tenant suspended before a restart recovers COLD (metadata-only shell,
#        durable cold marker), a HOT tenant recovers HOT with its data, and the COLD tenant still
#        resumes with all data intact. This is a single-instance test (no replication) exercised by
#        test_cross_restart_hot_only_recovery.
#
#   C10 — eager-epoch promotion: when a node holding a COLD tenant is promoted to MAIN, the new epoch
#        is written into the cold tenant's durable metadata (PromoteColdTenants, since ForEach skips
#        cold tenants) WITH the pre-promotion epoch appended to the epoch history. A later RESUME runs
#        the new epoch, and a replica that still holds the tenant at the OLD epoch finds that old epoch
#        in the new MAIN's continuous history -> it converges instead of diverging. Exercised by
#        test_promotion_eager_epoch_convergence (the negative control: without the history boundary the
#        old-epoch replica would DIVERGE and never converge).
#
#   C11 — hot/cold is observable: SHOW DATABASES lists a COLD tenant with a HOT/COLD status column
#        (it would otherwise vanish, being excluded from All()), and SHOW STORAGE INFO ON <cold>
#        serves the durable as-of-suspend snapshot instead of erroring (reverses HC-5). Exercised by
#        test_cold_aware_show.
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
# replica_1 is the original replica; "main" gets a replication port too because the C10 test demotes
# the old MAIN to a REPLICA of the promoted node (manual failover).
REPLICATION_PORTS = {"replica_1": 10001, "main": 10002}
file = "hot_cold_convergence"

HOT_COLD_FLAG = "--experimental-enabled=hot-cold-tenants"


@pytest.fixture
def test_name(request):
    return request.node.name


@pytest.fixture(autouse=True)
def cleanup_after_test():
    interactive_mg_runner.kill_all(keep_directories=False)
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


def main_args(test_name, recovery: bool = False):
    args = [
        "--bolt-port",
        f"{BOLT_PORTS['main']}",
        "--log-level=TRACE",
        HOT_COLD_FLAG,
    ]
    if recovery:
        # Cross-restart durability test: on restart MAIN must recover its tenants from its own disk
        # (HOT tenants rebuilt, COLD tenants restored as metadata-only shells).
        args += ["--data-recovery-on-startup=true"]
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
        HOT_COLD_FLAG,
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
    — both the C7 apply handler and the C8 reconcile would time out draining. Releasing A after every
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
    # C7: with the replica online, a SUSPEND on MAIN must drive the replica COLD, and a RESUME must
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

    # SUSPEND on MAIN -> replica copy torn down to COLD (C7 SuspendDatabaseRpc).
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "SUSPEND DATABASE A;")
    mg_sleep_and_assert("COLD", tenant_probe(replica_cursor, "A"))
    # MAIN's own copy is COLD too.
    assert tenant_probe(main_cursor, "A")() == "COLD"

    # RESUME on MAIN -> replica copy rebuilt HOT with data intact (C7 ResumeDatabaseRpc).
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "RESUME DATABASE A;")
    mg_sleep_and_assert(5, tenant_probe(replica_cursor, "A"))
    assert tenant_probe(main_cursor, "A")() == 5


def test_lagging_replica_convergence(connection, test_name):
    # C8: the replica is DOWN across the SUSPEND, so the SuspendDatabaseRpc never reaches it. On
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
    # C9: a single MAIN, no replication. A tenant suspended before a restart must recover COLD (durable
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
    # C11: a single MAIN. After SUSPEND, the cold tenant must remain VISIBLE in SHOW DATABASES (tagged
    # COLD) and SHOW STORAGE INFO ON <cold> must serve its as-of-suspend snapshot instead of erroring
    # (HC-5 reversed). RESUME flips it back to HOT.
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
    # C10: promote a node that holds a COLD tenant, then prove a replica still at the pre-promotion
    # epoch converges (continuous history) instead of diverging.
    #
    #   1. main + replica_1, tenant A replicated, both HOT at epoch E1.
    #   2. SUSPEND A -> both COLD; A's data sits on disk at E1 on both.
    #   3. Kill main. replica_1 is the survivor, holding COLD A at E1.
    #   4. Promote replica_1 -> MAIN: DoToMainPromotion mints a new epoch E2 and PromoteColdTenants
    #      rewrites A's durable cold metadata to E2 WITH (E1, ldt) appended to its epoch history (the
    #      C10 path; the ForEach epoch loop alone would skip the cold tenant).
    #   5. RESUME A on the new MAIN -> HOT at E2, data intact, history carrying the E1 boundary.
    #   6. Restart the old main as a REPLICA of the new MAIN. It still holds A at E1. The new MAIN's
    #      continuous-history check finds E1 in A's history -> continuous -> the replica converges to
    #      the new MAIN's data. WITHOUT the C10 boundary, the replica at E1 would be flagged a branching
    #      point (DIVERGED_FROM_MAIN) and never converge -> this assertion would time out (the negative
    #      control that gives the test teeth).
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

    # Promote the survivor (replica_1) to MAIN. This is the C10 trigger: DoToMainPromotion generates a
    # new epoch and PromoteColdTenants rewrites the COLD tenant's durable epoch + history boundary.
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


def test_concurrent_suspend_resume_under_memory_ceiling(connection, test_name):
    # C13: the stress matrix (product point 7 — "audit + stress-test every bad_alloc/crash point"). A single
    # MAIN under a hard memory ceiling runs concurrent allocating writers against several tenants WHILE a
    # suspender and a resumer churn those tenants HOT <-> COLD. The feature must stay crash-free and
    # lossless: every error a client sees must be one of the known retriable outcomes (cold access,
    # lost-the-race SUSPEND/RESUME, OOM, concurrent-system-query) — never a terminate, a tenant stranded in
    # a half-SUSPENDING/half-RESUMING limbo, or lost committed data. After the churn, every tenant resumed
    # HOT must hold EXACTLY the nodes its writer knows it durably committed.
    #
    # The companion exception-safety audit (commit message / design §19) proves the suspend/resume paths are
    # OOM-safe by construction; this test is the runtime backstop that the proof holds under real contention.
    instances = {
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
                HOT_COLD_FLAG,
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

    threads = [threading.Thread(target=writer, args=(t,)) for t in tenants]
    threads.append(threading.Thread(target=churner, args=("SUSPEND", suspends_ok)))
    threads.append(threading.Thread(target=churner, args=("RESUME", resumes_ok)))
    for th in threads:
        th.start()
    time.sleep(12)  # bounded churn window
    stop.set()
    for th in threads:
        th.join(timeout=30)
    # Every worker must have actually stopped before we read the shared counters / committed map and start
    # the single-threaded data-loss phase — a thread still alive here would race that phase (and a worker
    # wedged in a 30s+ server call is itself a failure worth surfacing loudly).
    assert all(not th.is_alive() for th in threads), "a stress worker did not stop within the join timeout"

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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
