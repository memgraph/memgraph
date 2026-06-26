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

# Slice 7 (Defunct Recovery / HA): a replica that boots with a corrupted tenant snapshot and
# --storage-allow-recovery-failure=true comes up defunct for that tenant, then self-heals: the
# main detects the replica is behind (a defunct tenant reports commit-ts 0 with a fresh epoch),
# drives it into RECOVERY and sends recovery steps, and the matching replica-side handler clears
# the defunct flag. After self-heal the replica reports `ready` and serves the tenant's data.
#
# The recovery steps the main sends depend on what durability the main holds for the tenant
# (GetRecoverySteps), so this test is parametrized over both healing paths:
#   - "snapshot": the main took a CREATE SNAPSHOT, so it sends a full snapshot; SnapshotHandler
#     clears defunct.
#   - "wal": the main never snapshotted the tenant, so its WAL chain reaches back to seq 0 and it
#     sends WAL files / the current WAL only; WalFilesHandler / CurrentWalHandler clear defunct.
# The replica boots defunct identically in both cases (its own corrupted snapshot is the only
# durability source); only the main-side recovery steps differ.
#
# Placement note: the spec suggests tests/e2e/durability/, but this test needs the coordinator +
# data-instance harness (the Raft coordinators, SHOW INSTANCES, the instance descriptions) that
# lives in tests/e2e/high_availability/. Wiring the coordinator helpers from the durability
# directory is impractical, so the test lives next to the other coordinator tests and reuses
# high_availability/common.py.

import os
import shutil
import sys
from functools import partial

import interactive_mg_runner
import pytest
from common import connect, corrupt_snapshots, execute_and_fetch_all, get_data_path, get_logs_path, show_instances
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

file = "defunct_replica_self_heal"

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
            # Write a snapshot of every tenant on graceful shutdown so the replica has an on-disk
            # snapshot of broken_db to corrupt (under normal replication the tenant arrives via WAL
            # deltas only, never a snapshot file).
            "storage_snapshot_on_exit": True,
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


@pytest.mark.parametrize("recovery_path", ["snapshot", "wal"])
def test_defunct_replica_tenant_self_heals_from_main(test_name, recovery_path):
    # 1. Bring up a coordinator-managed cluster: instance_3 (7687) MAIN, instance_1 (7688) and
    #    instance_2 (7689) REPLICAs.
    # 2. Create the broken_db tenant and populate it. For the "snapshot" path the main also takes a
    #    CREATE SNAPSHOT; for the "wal" path it does not, so the main's only durability for the
    #    tenant is its WAL chain (which reaches back to seq 0).
    # 3. Kill replica instance_1 and corrupt its broken_db snapshot (written via storage_snapshot_on_exit).
    # 4. Restart instance_1 with --storage-allow-recovery-failure=true: broken_db boots defunct.
    # 5. Assert the main is unaffected, then assert instance_1's broken_db self-heals to `ready`
    #    (the main's recovery steps clear defunct: a snapshot for the "snapshot" path, WAL files /
    #    current WAL for the "wal" path) and serves the tenant's data.

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

    # 2
    main_cursor = connect(host="localhost", port=7687).cursor()
    execute_and_fetch_all(main_cursor, f"CREATE DATABASE {TENANT}")
    execute_and_fetch_all(main_cursor, f"USE DATABASE {TENANT}")
    execute_and_fetch_all(main_cursor, f"UNWIND range(1, {NUM_NODES}) AS i CREATE (:Node {{id: i}})")
    if recovery_path == "snapshot":
        # The main holds a snapshot of the tenant -> GetRecoverySteps heals the replica via a full
        # snapshot. For the "wal" path we deliberately skip this so the main heals via WAL only.
        execute_and_fetch_all(main_cursor, "CREATE SNAPSHOT")

    # The tenant and its data must be present on both replicas before we corrupt one of them.
    instance_1_cursor = connect(host="localhost", port=7688).cursor()
    instance_2_cursor = connect(host="localhost", port=7689).cursor()
    mg_sleep_and_assert(NUM_NODES, partial(tenant_vertex_count, instance_1_cursor))
    mg_sleep_and_assert(NUM_NODES, partial(tenant_vertex_count, instance_2_cursor))

    # 3: gracefully stop replica instance_1 (SIGTERM, keep its data directory). storage_snapshot_on_exit
    #    makes it write a snapshot of broken_db. Then drop the tenant's WAL so the corrupted snapshot is
    #    the sole durability source on the next boot, and corrupt the snapshot's data region.
    interactive_mg_runner.stop(instances, "instance_1", keep_directories=True)
    instance_1_full_data_directory = os.path.join(base_data_directory, "instance_1")
    tenant_dir = find_tenant_dir(instance_1_full_data_directory)
    wal_dir = os.path.join(tenant_dir, "wal")
    if os.path.isdir(wal_dir):
        shutil.rmtree(wal_dir)
        os.makedirs(wal_dir, exist_ok=True)
    corrupt_snapshots(tenant_dir)

    # 4: restart instance_1 with the recovery-failure flag so the corrupted tenant boots defunct
    #    instead of crashing the whole instance.
    instances["instance_1"]["args"].append("--storage-allow-recovery-failure=true")
    interactive_mg_runner.start(instances, "instance_1")

    # 5a: the main is unaffected.
    assert tenant_status(main_cursor).get(TENANT) == "ready"

    # Drive the main to replicate broken_db again. After instance_1 restarts, its broken_db is
    # defunct and reports commit-ts 0 with a fresh epoch; the next replicated transaction makes the
    # main see the replica as behind, transition it to RECOVERY and send recovery steps (a snapshot
    # for the "snapshot" path, WAL files / the current WAL for the "wal" path), which the matching
    # replica-side handler uses to clear the defunct flag. The exact moment the main re-checks the
    # restarted replica's state is timing dependent, so we keep nudging it with writes while polling
    # for the heal. Every write is on broken_db, so the main's broken_db count is the ground truth
    # the healed replica must converge to.
    execute_and_fetch_all(main_cursor, f"USE DATABASE {TENANT}")

    instance_1_cursor = connect(host="localhost", port=7688).cursor()

    def nudge_and_check_healed():
        try:
            execute_and_fetch_all(main_cursor, "CREATE (:Heal)")
        except Exception as e:
            # A SYNC replica that is mid-recovery can transiently make the commit fail; tolerate only
            # that specific error and let the next poll retry. Anything else is a real failure.
            assert "Failed to replicate to SYNC replica" in str(e), f"Unexpected error while nudging main: {e}"
        return tenant_status(instance_1_cursor).get(TENANT)

    # 5b: the replica self-heals to `ready`.
    mg_sleep_and_assert("ready", nudge_and_check_healed, max_duration=120, time_between_attempt=2)

    # 5c: the healed replica serves the tenant's data. It must converge to whatever the main holds
    #     for broken_db (the original NUM_NODES nodes plus however many :Heal nodes were written to
    #     trigger the heal).
    instance_1_cursor = connect(host="localhost", port=7688).cursor()
    main_broken_db_count = tenant_vertex_count(main_cursor)
    assert main_broken_db_count >= NUM_NODES
    mg_sleep_and_assert(main_broken_db_count, partial(tenant_vertex_count, instance_1_cursor))

    # The default tenant was never affected on the healed replica.
    assert tenant_status(connect(host="localhost", port=7688).cursor()).get("memgraph") == "ready"

    # The untouched replica instance_2 stayed healthy throughout (defunct tenant on one instance
    # does not disrupt the other healthy instances).
    assert tenant_status(instance_2_cursor).get(TENANT) == "ready"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
