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

# Defunct Recovery (HA): RESET DATABASE on the MAIN must reconcile replicas. When the main's tenant
# is defunct and the operator runs RESET DATABASE, the main resets that tenant to an empty state with
# a fresh epoch and then force-resyncs every replica. Each replica must therefore WIPE its stale tenant
# data and converge to the clean empty tenant, ready to accept the main's subsequent commits.
#
# This is the inverse of defunct_replica_self_heal.py (there the replica was defunct and self-healed
# from a healthy main). Here the MAIN is defunct, the replica is healthy and holds the old (now stale)
# tenant data; the proof of the fix is that the replica's old data is wiped after RESET and that fresh
# data written on the resetted main replicates to it.
#
# Placement note: like defunct_replica_self_heal.py, this needs the coordinator + data-instance harness
# from tests/e2e/high_availability/, so it lives next to the other coordinator tests and reuses
# high_availability/common.py.

import copy
import os
import shutil
import sys
import time
from functools import partial

import interactive_mg_runner
import mgclient
import pytest
from common import connect, corrupt_snapshots, execute_and_fetch_all, get_data_path, get_logs_path, show_instances
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

file = "defunct_reset_replica_clean"

TENANT = "broken_db"
NUM_NODES = 5000


@pytest.fixture
def test_name(request):
    return request.node.name


@pytest.fixture(autouse=True)
def cleanup_instances():
    interactive_mg_runner.kill_all()
    yield
    interactive_mg_runner.kill_all()


def find_tenant_dir(instance_full_data_directory):
    """Resolve the on-disk directory of the broken tenant for an instance. Tenants live under
    <data_directory>/databases/<uuid>/. The default `memgraph` tenant and the `.durability` system
    metadata are skipped."""
    databases_dir = os.path.join(instance_full_data_directory, "databases")
    assert os.path.isdir(databases_dir), f"Expected databases dir at {databases_dir}"
    tenant_dirs = [
        os.path.join(databases_dir, d)
        for d in os.listdir(databases_dir)
        if d not in ("memgraph", ".durability")
        and not d.startswith(".")
        and os.path.isdir(os.path.join(databases_dir, d))
        and os.path.isdir(os.path.join(databases_dir, d, "snapshots"))
    ]
    assert tenant_dirs, f"Expected a non-default tenant directory under {databases_dir}"
    return tenant_dirs[0]


def get_memgraph_instances_description(test_name: str):
    return {
        "instance_1": {
            "args": [
                "--bolt-port",
                "7688",
                "--log-level",
                "TRACE",
                "--management-port",
                "10011",
                "--replication-restore-state-on-startup=true",
                "--data-recovery-on-startup=true",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_1",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--bolt-port",
                "7689",
                "--log-level",
                "TRACE",
                "--management-port",
                "10012",
                "--replication-restore-state-on-startup=true",
                "--data-recovery-on-startup=true",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_2",
            "setup_queries": [],
        },
        "instance_3": {
            "args": [
                "--bolt-port",
                "7687",
                "--log-level",
                "TRACE",
                "--management-port",
                "10013",
                "--replication-restore-state-on-startup=true",
                "--data-recovery-on-startup=true",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_3",
            "setup_queries": [],
            # The main (instance_3) is the one we corrupt. Write a snapshot of every tenant on graceful
            # shutdown so the tenant has an on-disk snapshot to corrupt into a defunct boot.
            "storage_snapshot_on_exit": True,
        },
        "coordinator_1": {
            "args": [
                "--bolt-port",
                "7690",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10111",
                "--coordinator-hostname=localhost",
                "--management-port=10121",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_1",
            "setup_queries": [],
        },
        "coordinator_2": {
            "args": [
                "--bolt-port",
                "7691",
                "--log-level=TRACE",
                "--coordinator-id=2",
                "--coordinator-port=10112",
                "--coordinator-hostname=localhost",
                "--management-port=10122",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_2",
            "setup_queries": [],
        },
        "coordinator_3": {
            "args": [
                "--bolt-port",
                "7692",
                "--log-level=TRACE",
                "--coordinator-id=3",
                "--coordinator-port=10113",
                "--coordinator-hostname=localhost",
                "--management-port=10123",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_3",
            "setup_queries": [
                "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
                "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
                "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}",
                "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
                "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
                "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
                "SET INSTANCE instance_3 TO MAIN",
            ],
        },
    }


def tenant_status(cursor):
    """SHOW DATABASES -> {name: status} on whatever instance the cursor points to."""
    return {row[0]: row[1] for row in execute_and_fetch_all(cursor, "SHOW DATABASES")}


def tenant_vertex_count(cursor):
    execute_and_fetch_all(cursor, f"USE DATABASE {TENANT}")
    return execute_and_fetch_all(cursor, "MATCH (n) RETURN count(n)")[0][0]


def tenant_storage_status(cursor):
    """SHOW STORAGE INFO ON CURRENT DATABASE -> the `status` row value for the current database."""
    rows = {row[0]: row[1] for row in execute_and_fetch_all(cursor, "SHOW STORAGE INFO ON CURRENT DATABASE")}
    return rows.get("status")


INSTANCE_BOLT_PORT = {"instance_1": 7688, "instance_2": 7689, "instance_3": 7687}


def test_reset_database_on_main_cleans_replica_tenant(test_name):
    # 1. Bring up a coordinator-managed cluster: instance_3 (7687) MAIN, instance_1 (7688) and
    #    instance_2 (7689) REPLICAs.
    # 2. Create the broken_db tenant on the main and populate it. The data replicates to both replicas
    #    -> the replicas now hold NUM_NODES nodes (the stale data that RESET must later wipe).
    # 3. Gracefully stop ALL data instances (keeping the coordinators up), corrupt instance_3's
    #    broken_db snapshot, then restart the data instances keeping their directories. With every data
    #    instance down at once the coordinator has no failover target, so instance_3's MAIN role is
    #    preserved and restored on restart (no failover); instance_3's broken_db boots defunct
    #    (--storage-allow-recovery-failure), while the replicas restore their healthy broken_db data. The
    #    replicas therefore still hold the stale NUM_NODES nodes -- nothing has cleaned them.
    # 4. RESET DATABASE on the (defunct) main: resets broken_db to empty with a fresh epoch and
    #    force-resyncs every replica (the fix under test).
    # 5. Assert each replica's stale broken_db data is WIPED (0 nodes) and `ready` -- the whole point of
    #    the fix: RESET cleans the replicas, not just the main. Without the fix the replicas keep their
    #    stale NUM_NODES nodes.
    # 6. Import fresh data on the resetted main; assert it replicates to the replicas (they are clean
    #    empty tenants ready to accept the main's commits).

    instances = get_memgraph_instances_description(test_name=test_name)

    base_data_directory = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", get_data_path(file, test_name))
    shutil.rmtree(base_data_directory, ignore_errors=True)

    # 1
    interactive_mg_runner.start_all(instances, keep_directories=False)

    coord_cursor = connect(host="localhost", port=7692).cursor()
    expected_data_on_coord = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7688", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7689", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7687", "", "localhost:10013", "up", "main"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, partial(show_instances, coord_cursor))

    # 2: create + populate the tenant on the main; wait until both replicas hold the data.
    main_cursor = connect(host="localhost", port=7687).cursor()
    execute_and_fetch_all(main_cursor, f"CREATE DATABASE {TENANT}")
    execute_and_fetch_all(main_cursor, f"USE DATABASE {TENANT}")
    execute_and_fetch_all(main_cursor, f"UNWIND range(1, {NUM_NODES}) AS i CREATE (:Node {{id: i}})")

    instance_1_cursor = connect(host="localhost", port=7688).cursor()
    instance_2_cursor = connect(host="localhost", port=7689).cursor()
    mg_sleep_and_assert(NUM_NODES, partial(tenant_vertex_count, instance_1_cursor))
    mg_sleep_and_assert(NUM_NODES, partial(tenant_vertex_count, instance_2_cursor))

    # 3: gracefully stop all three data instances (SIGTERM -> instance_3 writes a snapshot of broken_db
    #    via storage_snapshot_on_exit), leaving the coordinators running. Drop instance_3's tenant WAL so
    #    the corrupted snapshot is the sole durability source on the next boot, then corrupt the
    #    snapshot's data region so broken_db boots defunct.
    for name in ("instance_1", "instance_2", "instance_3"):
        interactive_mg_runner.stop(instances, name, keep_directories=True)

    instance_3_full_data_directory = os.path.join(base_data_directory, "instance_3")
    tenant_dir = find_tenant_dir(instance_3_full_data_directory)
    wal_dir = os.path.join(tenant_dir, "wal")
    if os.path.isdir(wal_dir):
        shutil.rmtree(wal_dir)
        os.makedirs(wal_dir, exist_ok=True)
    corrupt_snapshots(tenant_dir)

    # Restart the data instances keeping directories. Clear their setup queries (none anyway) and add the
    # recovery-failure flag to instance_3 so its corrupted broken_db boots defunct. The coordinators
    # stayed up and restore instance_3 as MAIN; with no data instance alive during the gap there was no
    # failover target, so the MAIN role is preserved.
    restart_instances = copy.deepcopy(instances)
    for desc in restart_instances.values():
        desc["setup_queries"] = []
    restart_instances["instance_3"]["args"].append("--storage-allow-recovery-failure=true")
    for name in ("instance_3", "instance_1", "instance_2"):
        interactive_mg_runner.start(restart_instances, name)

    coord_cursor = connect(host="localhost", port=7692).cursor()

    # The coordinators restore the persisted roles: instance_3 stays MAIN, no failover.
    mg_sleep_and_assert(expected_data_on_coord, partial(show_instances, coord_cursor))

    # broken_db on the (still) main is defunct.
    instance_3_cursor = connect(host="localhost", port=7687).cursor()
    mg_sleep_and_assert("broken", lambda: tenant_status(instance_3_cursor).get(TENANT))

    # 3b: A broken database must reject starting an explicit transaction. An explicit transaction opens a
    # data accessor whose queries bypass the per-query broken gate, so BEGIN itself must fail -- otherwise
    # BEGIN; ...; CREATE ... would be a loophole for touching the broken tenant before it is recovered.
    broken_txn_cursor = connect(host="localhost", port=7687).cursor()
    execute_and_fetch_all(broken_txn_cursor, f"USE DATABASE {TENANT}")
    with pytest.raises(mgclient.DatabaseError, match="broken state"):
        execute_and_fetch_all(broken_txn_cursor, "BEGIN")

    # 4: RESET DATABASE on the (defunct) main. Resets broken_db to empty and force-resyncs every replica.
    main_cursor = connect(host="localhost", port=7687).cursor()
    execute_and_fetch_all(main_cursor, f"USE DATABASE {TENANT}")
    execute_and_fetch_all(main_cursor, "RESET DATABASE")
    assert tenant_status(main_cursor).get(TENANT) == "ready", "Main's tenant should be ready after RESET"

    # 5: RESET's force-recover drives every replica into RECOVERY and re-syncs it to the main's fresh,
    # empty epoch. Wait until the wipe has fully propagated -- 0 nodes on the main AND both replicas (the
    # stale NUM_NODES nodes are gone everywhere). Reconnect each poll (the instances were restarted) and
    # tolerate transient read errors while a replica is mid-resync.
    def count_on(port):
        try:
            return tenant_vertex_count(connect(host="localhost", port=port).cursor())
        except Exception:
            return None

    for port in (7687, INSTANCE_BOLT_PORT["instance_1"], INSTANCE_BOLT_PORT["instance_2"]):
        mg_sleep_and_assert(0, partial(count_on, port))

    # 5b: The wiped state must survive a replica restart. Restart both replicas (keeping their
    # directories); on recovery their broken_db must still be empty (0 nodes), not resurrected with the
    # old stale NUM_NODES data. The main records each replica's repair confirmation, so on reconnect the
    # reset is not re-advertised -- the replica must therefore already be durably clean on its own.
    for name in ("instance_1", "instance_2"):
        interactive_mg_runner.stop(restart_instances, name, keep_directories=True)
    for name in ("instance_1", "instance_2"):
        interactive_mg_runner.start(restart_instances, name)

    # Coordinator restores both as replicas (instance_3 remains MAIN).
    mg_sleep_and_assert(expected_data_on_coord, partial(show_instances, coord_cursor))

    for port in (INSTANCE_BOLT_PORT["instance_1"], INSTANCE_BOLT_PORT["instance_2"]):
        mg_sleep_and_assert(0, partial(count_on, port))

    # Before importing fresh data, wait until SHOW REPLICAS on the main reports BOTH replicas READY for
    # the tenant -- i.e. they have finished re-syncing the reset and are ready to accept the main's commits.
    def both_replicas_ready_for_tenant():
        rows = execute_and_fetch_all(main_cursor, "SHOW REPLICAS")
        # data_info (the last column) maps database name -> {"ts", "behind", "status"}.
        statuses = [row[-1].get(TENANT, {}).get("status") for row in rows]
        return len(statuses) == 2 and all(s == "ready" for s in statuses)

    mg_sleep_and_assert(True, both_replicas_ready_for_tenant)

    # 6: The clean tenant is ready to accept the main's commits. Import FRESH_NODES nodes on the main and
    # assert the main and both replicas converge to EXACTLY that count: not NUM_NODES (stale data was
    # wiped) and not NUM_NODES + FRESH_NODES (no leftover divergence), but exactly the fresh data the
    # clean tenant accepted. Retry the import only while the just-resetted main is briefly not writeable
    # ("Write queries currently forbidden"), for at most 5s. "Failed to replicate to SYNC replica" must
    # NOT happen -- the replicas are clean and ready.
    FRESH_NODES = 42
    deadline = time.time() + 5
    while True:
        try:
            execute_and_fetch_all(main_cursor, f"UNWIND range(1, {FRESH_NODES}) AS i CREATE (:Fresh)")
            break
        except Exception as e:
            if "Write queries currently forbidden" in str(e) and time.time() < deadline:
                time.sleep(0.2)
                continue
            break

    for port in (7687, INSTANCE_BOLT_PORT["instance_1"], INSTANCE_BOLT_PORT["instance_2"]):
        mg_sleep_and_assert(FRESH_NODES, partial(count_on, port))

    # The resetted main holds exactly the fresh data, and its tenant is ready.
    assert tenant_status(main_cursor).get(TENANT) == "ready"
    assert tenant_vertex_count(main_cursor) == FRESH_NODES

    # Each replica's tenant reports ready via SHOW STORAGE INFO over a fresh connection.
    for name in ("instance_1", "instance_2"):
        port = INSTANCE_BOLT_PORT[name]

        def replica_storage_ready(p=port):
            cur = connect(host="localhost", port=p).cursor()
            execute_and_fetch_all(cur, f"USE DATABASE {TENANT}")
            return tenant_storage_status(cur)

        mg_sleep_and_assert("ready", replica_storage_ready)
        assert tenant_status(connect(host="localhost", port=port).cursor()).get(TENANT) == "ready"

    # The default tenant was never affected on any instance.
    for port in (7687, 7688, 7689):
        assert tenant_status(connect(host="localhost", port=port).cursor()).get("memgraph") == "ready"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
