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
# COLD is observed without relying on the (later, C11) cold-aware SHOW: accessing a COLD tenant
# trips the query seam in DbmsHandler::Get_, which raises "... is suspended (cold); run RESUME ...".

import os
import sys

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
REPLICATION_PORTS = {"replica_1": 10001}
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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
