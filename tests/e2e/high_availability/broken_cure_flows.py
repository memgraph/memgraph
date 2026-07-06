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

"""Coordinator-based e2e coverage for the broken-tenant cure flows.

These tests bring up a Raft coordinator cluster with two data instances, create a
multi-tenant database, corrupt that tenant's durability files on one or both data
instances, and restart them with --storage-allow-recovery-failure=true so the tenant
boots broken. They then verify the operator cure flows (RECOVER SNAPSHOT, RESET
DATABASE) and replica self-heal in the HA setting.

HA + multi-tenancy require an enterprise license; the whole module is skipped when no
license is configured in the environment.
"""

import os
import shutil
import sys
import time
from functools import partial

import interactive_mg_runner
import pytest
from common import (
    connect,
    execute_and_fetch_all,
    get_data_path,
    get_logs_path,
    get_vertex_count,
    show_instances,
    show_replicas,
    wait_until_main_writeable,
)
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

file = "broken_cure_flows"

TENANT = "clients"

# Scenario 4 uses two databases whose broken/ready split is mirrored across the two instances.
DEFAULT_DB = "memgraph"
DB1 = "db1"
DEFAULT_DB_COUNT = 3000
DB1_COUNT = 2000

# HA + multitenancy are enterprise-only; without a license the cluster cannot be formed
# and CREATE DATABASE is rejected, so skip the whole module rather than report a fake pass.
pytestmark = pytest.mark.skipif(
    not (os.environ.get("MEMGRAPH_ENTERPRISE_LICENSE") and os.environ.get("MEMGRAPH_ORGANIZATION_NAME")),
    reason="HA broken-cure e2e needs an enterprise license (MEMGRAPH_ENTERPRISE_LICENSE + MEMGRAPH_ORGANIZATION_NAME)",
)


@pytest.fixture
def test_name(request):
    return request.node.name


@pytest.fixture(autouse=True)
def cleanup_after_test():
    interactive_mg_runner.kill_all(keep_directories=False)
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


def data_dir_of(test_name, instance):
    return os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", get_data_path(file, test_name), instance)


def tenant_dir(instance_data_dir):
    """Resolve the on-disk durability directory of the TENANT database (databases/<uuid>/)."""
    databases_dir = os.path.join(instance_data_dir, "databases")
    candidates = [
        os.path.join(databases_dir, d)
        for d in os.listdir(databases_dir)
        if d not in ("memgraph", ".durability")
        and not d.startswith(".")
        and os.path.isdir(os.path.join(databases_dir, d))
    ]
    assert len(candidates) == 1, f"Expected exactly one tenant dir, got {candidates}"
    return candidates[0]


def _files_in(directory):
    if not os.path.isdir(directory):
        return []
    return [os.path.join(directory, f) for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]


def _corrupt_data_region(path, head_keep, tail_keep):
    """Overwrite the data region of a durability file with 0xFF, preserving the header and the
    trailing metadata so the file is still selected for recovery but fails to load."""
    size = os.path.getsize(path)
    start = min(head_keep, size)
    end = max(start, size - tail_keep)
    assert end > start, f"File {path} too small ({size} bytes) to corrupt safely"
    with open(path, "r+b") as fh:
        fh.seek(start)
        fh.write(b"\xff" * (end - start))


def corrupt_tenant_durability(instance_data_dir):
    """Corrupt the tenant's durability so recovery fails and it boots broken.

    The main has a snapshot: corrupt its data region and wipe the WAL so the snapshot is the only
    (now broken) source. A replica that received the tenant via replication has only a WAL and no
    snapshot: corrupt the WAL's data region instead. Either way recovery throws and the tenant goes
    broken (under --storage-allow-recovery-failure)."""
    tdir = tenant_dir(instance_data_dir)
    snapshot_dir = os.path.join(tdir, "snapshots")
    wal_dir = os.path.join(tdir, "wal")

    snapshot_files = _files_in(snapshot_dir)
    wal_files = _files_in(wal_dir)
    assert snapshot_files or wal_files, f"Expected snapshot or WAL durability to corrupt under {tdir}"

    if snapshot_files:
        for path in snapshot_files:
            _corrupt_data_region(path, head_keep=1024, tail_keep=4096)
        # Snapshot is the failure point: drop the WAL so there is no clean replay fallback.
        for path in wal_files:
            os.remove(path)
    else:
        # Replica with WAL-only durability: corrupt the delta region of every WAL file.
        for path in wal_files:
            _corrupt_data_region(path, head_keep=256, tail_keep=64)


def plant_corrupt_snapshot(instance_data_dir, source_snapshot):
    """Give a tenant a corruptible snapshot by planting a copy of a real snapshot into its snapshot
    directory, then corrupting it and wiping its WAL. Used for the replica, which received the
    tenant via replication and therefore has only WAL (no snapshot) on disk. This makes the replica
    fail recovery on the deterministic 'no usable snapshot' path and boot broken."""
    tdir = tenant_dir(instance_data_dir)
    snapshot_dir = os.path.join(tdir, "snapshots")
    wal_dir = os.path.join(tdir, "wal")
    os.makedirs(snapshot_dir, exist_ok=True)

    planted = os.path.join(snapshot_dir, os.path.basename(source_snapshot))
    shutil.copyfile(source_snapshot, planted)
    _corrupt_data_region(planted, head_keep=1024, tail_keep=4096)
    for path in _files_in(wal_dir):
        os.remove(path)


def corrupt_default_db_durability(instance_data_dir):
    """Corrupt the DEFAULT `memgraph` database so it boots broken. Unlike named tenants (which live under
    databases/<uuid>/), the default db's durability is the data directory root: <data_dir>/snapshots and
    <data_dir>/wal. Corrupt its snapshot data region and wipe its WAL so the snapshot is the only (now
    broken) source and there is no clean replay fallback."""
    snapshot_dir = os.path.join(instance_data_dir, "snapshots")
    wal_dir = os.path.join(instance_data_dir, "wal")
    snapshot_files = _files_in(snapshot_dir)
    assert snapshot_files, f"Expected a default-db snapshot to corrupt under {snapshot_dir}"
    for path in snapshot_files:
        _corrupt_data_region(path, head_keep=1024, tail_keep=4096)
    for path in _files_in(wal_dir):
        os.remove(path)


def stash_good_tenant_snapshot(instance_data_dir, dest):
    """Copy a known-good copy of the tenant snapshot aside before corruption."""
    tdir = tenant_dir(instance_data_dir)
    snapshot_dir = os.path.join(tdir, "snapshots")
    snapshot_files = [
        os.path.join(snapshot_dir, f) for f in os.listdir(snapshot_dir) if os.path.isfile(os.path.join(snapshot_dir, f))
    ]
    assert len(snapshot_files) == 1, f"Expected exactly one tenant snapshot, got {snapshot_files}"
    shutil.copyfile(snapshot_files[0], dest)
    return dest


def get_instances_description(test_name: str):
    """Three Raft coordinators + two data instances, no WAL/snapshot scheduling surprises."""

    def data_instance(idx, bolt, mgmt):
        return {
            "args": [
                "--bolt-port",
                f"{bolt}",
                "--log-level",
                "TRACE",
                "--management-port",
                f"{mgmt}",
                "--storage-snapshot-interval-sec",
                "100000",
                "--storage-snapshot-on-exit=false",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_{idx}.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_{idx}",
            "setup_queries": [],
        }

    def coordinator(idx, bolt, coord_port, mgmt):
        return {
            "args": [
                "--bolt-port",
                f"{bolt}",
                "--log-level=TRACE",
                f"--coordinator-id={idx}",
                f"--coordinator-port={coord_port}",
                f"--management-port={mgmt}",
                "--coordinator-hostname",
                "localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_{idx}.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_{idx}",
            "setup_queries": [],
        }

    return {
        "instance_1": data_instance(1, 7687, 10011),
        "instance_2": data_instance(2, 7688, 10012),
        "coordinator_1": coordinator(1, 7690, 10111, 10121),
        "coordinator_2": coordinator(2, 7691, 10112, 10122),
        "coordinator_3": coordinator(3, 7692, 10113, 10123),
    }


def get_setup_queries():
    return [
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
        "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}",
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
        "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
        "SET INSTANCE instance_1 TO MAIN",
    ]


def enable_recovery_failure(instances, instance):
    instances[instance]["args"].append("--storage-allow-recovery-failure=true")


def main_is(coord_cursor, instance_name):
    def checker():
        for row in show_instances(coord_cursor):
            if row[0] == instance_name:
                return row[-1]
        return None

    return checker


def storage_info(cursor):
    rows = execute_and_fetch_all(cursor, "SHOW STORAGE INFO ON CURRENT DATABASE")
    return {row[0]: row[1] for row in rows}


def databases_status(cursor):
    return {row[0]: row[1] for row in execute_and_fetch_all(cursor, "SHOW DATABASES")}


def use_tenant(cursor):
    execute_and_fetch_all(cursor, f"USE DATABASE {TENANT}")


def db_status_on(port, db):
    """SHOW STORAGE INFO status for `db` over a fresh connection to the instance on `port`."""
    c = connect(host="localhost", port=port).cursor()
    execute_and_fetch_all(c, f"USE DATABASE {db}")
    return storage_info(c).get("status")


def db_count_on(port, db):
    """Vertex count for `db` over a fresh connection to the instance on `port`."""
    c = connect(host="localhost", port=port).cursor()
    execute_and_fetch_all(c, f"USE DATABASE {db}")
    return get_vertex_count(c)


def wait_main(coord_cursor, instance_name):
    mg_sleep_and_assert("main", main_is(coord_cursor, instance_name))


def wait_replica(coord_cursor, instance_name):
    mg_sleep_and_assert("replica", main_is(coord_cursor, instance_name))


INSTANCE_BOLT_PORT = {"instance_1": 7687, "instance_2": 7688}


def current_main_name(coord_cursor):
    """Return the name of the single up main instance, or None if the cluster is mid-transition."""
    mains = [row[0] for row in show_instances(coord_cursor) if row[-1] == "main" and row[-2] == "up"]
    return mains[0] if len(mains) == 1 else None


def wait_stable_main(coord_cursor):
    """Wait until the coordinator reports exactly one up main for several consecutive checks, then
    return its name. HA can briefly show 'cluster without main' and fail over right after a restart,
    so we require the main to be stable before driving queries at it."""
    stable_name = None
    stable_count = 0
    for _ in range(120):
        name = current_main_name(coord_cursor)
        if name is not None and name == stable_name:
            stable_count += 1
            if stable_count >= 4:
                return name
        else:
            stable_name = name
            stable_count = 1 if name is not None else 0
        time.sleep(0.5)
    assert False, f"No stable main settled, last seen: {stable_name}"


def resolve_main_cursor(coord_cursor):
    name = wait_stable_main(coord_cursor)
    return connect(host="localhost", port=INSTANCE_BOLT_PORT[name]).cursor(), name


def nudge_main_write(cursor, query, timeout=30):
    """Commit a single write on the main to advance its timestamp and trigger replica recovery.

    Retries only while the main is transiently non-writeable right after a role change; treats a
    'SYNC replica not reachable / not in sync' error as success, because the write IS committed on
    the main (the broken replica will be recovered automatically). The write therefore lands at
    most once, so it cannot double-insert."""
    start = time.time()
    while True:
        try:
            execute_and_fetch_all(cursor, query)
            return
        except Exception as e:
            msg = str(e)
            if "Failed to replicate to SYNC replica" in msg:
                return
            if "Write queries currently forbidden on the main instance" in msg and time.time() - start < timeout:
                time.sleep(0.5)
                continue
            raise


def wait_replica_ready_on_main(main_cur, timeout=60):
    """Wait until the main reports a registered replica whose overall replication status is
    'ready', so a subsequent SYNC write replicates rather than failing with the replica unreachable
    (avoids retrying writes, which would otherwise double-insert)."""

    start = time.time()
    while time.time() - start < timeout:
        try:
            rows = show_replicas(main_cur)
        except Exception:
            rows = []
        for row in rows:
            status_field = row[3]
            if isinstance(status_field, dict) and status_field.get("status") == "ready":
                return
        time.sleep(0.5)
    assert False, "No replica reached 'ready' status on the main"


def setup_cluster_with_tenant(test_name, vertex_count=5000):
    """Bring up the cluster, create + populate + snapshot the TENANT on the main, and confirm
    it replicated to the replica. Returns (instances_description, coord_cursor)."""
    # Wipe any stale data left by a previous (possibly crashed) run so the healthy setup boot does
    # not trip over a corrupt tenant from an earlier iteration.
    test_data_root = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", get_data_path(file, test_name))
    shutil.rmtree(test_data_root, ignore_errors=True)

    instances = get_instances_description(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)

    coord_cursor = connect(host="localhost", port=7692).cursor()
    for query in get_setup_queries():
        execute_and_fetch_all(coord_cursor, query)

    wait_main(coord_cursor, "instance_1")
    wait_replica(coord_cursor, "instance_2")

    main_cursor = connect(host="localhost", port=7687).cursor()
    execute_and_fetch_all(main_cursor, f"CREATE DATABASE {TENANT}")
    use_tenant(main_cursor)
    execute_and_fetch_all(main_cursor, f"UNWIND range(1, {vertex_count}) AS i CREATE (:Node {{id: i}})")
    execute_and_fetch_all(main_cursor, "CREATE SNAPSHOT")

    # Confirm the tenant replicated to instance_2 before we take the cluster down.
    replica_cursor = connect(host="localhost", port=7688).cursor()
    use_tenant(replica_cursor)
    mg_sleep_and_assert(vertex_count, partial(get_vertex_count, replica_cursor))

    return instances, coord_cursor


# ---------------------------------------------------------------------------------------------
# Scenario 1: main boots with a corrupted tenant -> admin cures via RECOVER SNAPSHOT.
# ---------------------------------------------------------------------------------------------
def test_main_corrupt_cured_with_recover_snapshot(test_name):
    instances, coord_cursor = setup_cluster_with_tenant(test_name)

    good_snapshot = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", f"{file}_{test_name}_good_snapshot")
    stash_good_tenant_snapshot(data_dir_of(test_name, "instance_1"), good_snapshot)

    # Take the whole cluster down. The replica (instance_2) is kept down while the operator
    # cures the main so the coordinator has no healthy failover target: instance_1 stays main
    # and the cure is exercised deterministically on the corrupt main.
    interactive_mg_runner.kill(instances, "instance_1")
    interactive_mg_runner.kill(instances, "instance_2")
    corrupt_tenant_durability(data_dir_of(test_name, "instance_1"))
    enable_recovery_failure(instances, "instance_1")
    interactive_mg_runner.start(instances, "instance_1")
    wait_main(coord_cursor, "instance_1")

    main_cursor = connect(host="localhost", port=7687).cursor()
    use_tenant(main_cursor)
    assert storage_info(main_cursor)["status"] == "broken"

    # Operator cures the main in place with the known-good snapshot copy.
    execute_and_fetch_all(main_cursor, f"RECOVER SNAPSHOT '{good_snapshot}'")
    assert storage_info(main_cursor)["status"] == "ready"
    assert get_vertex_count(main_cursor) == 5000

    # Bring the replica back: it syncs the cured tenant from the main and serves it.
    interactive_mg_runner.start(instances, "instance_2")
    wait_replica(coord_cursor, "instance_2")
    replica_cursor = connect(host="localhost", port=7688).cursor()
    use_tenant(replica_cursor)
    mg_sleep_and_assert(5000, partial(get_vertex_count, replica_cursor))

    # The cured cluster keeps replicating: a fresh write on the main reaches the replica. Re-resolve
    # the main first because the replica rejoining can trigger a failover, and wait for the replica
    # to be in sync so the SYNC write lands on the first try (a retried write would double-insert).
    cured_main_cursor, _ = resolve_main_cursor(coord_cursor)
    use_tenant(cured_main_cursor)
    wait_replica_ready_on_main(cured_main_cursor)
    wait_until_main_writeable(cured_main_cursor, "CREATE (:Node {id: 999999})")
    assert get_vertex_count(cured_main_cursor) == 5001

    def tenant_count(port):
        c = connect(host="localhost", port=port).cursor()
        use_tenant(c)
        return get_vertex_count(c)

    mg_sleep_and_assert(5001, partial(tenant_count, 7687))
    mg_sleep_and_assert(5001, partial(tenant_count, 7688))

    os.remove(good_snapshot)


# ---------------------------------------------------------------------------------------------
# Scenario 2: both main and replica boot corrupt -> status verified on both, then cured.
# ---------------------------------------------------------------------------------------------
def test_both_corrupt_status_then_cure(test_name):
    instances, coord_cursor = setup_cluster_with_tenant(test_name)

    good_snapshot = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", f"{file}_{test_name}_good_snapshot")
    stash_good_tenant_snapshot(data_dir_of(test_name, "instance_1"), good_snapshot)

    # Take both instances down and make the tenant fail recovery on both. The main has its own
    # snapshot to corrupt; the replica received the tenant via replication and has only WAL, so we
    # plant a corrupt copy of the (good) snapshot into its tenant dir. Bring the main up first and
    # let it settle as a stable broken main before bringing the replica up, so the cluster does not
    # churn roles while both tenants are broken.
    interactive_mg_runner.kill(instances, "instance_1")
    interactive_mg_runner.kill(instances, "instance_2")
    corrupt_tenant_durability(data_dir_of(test_name, "instance_1"))
    plant_corrupt_snapshot(data_dir_of(test_name, "instance_2"), good_snapshot)
    enable_recovery_failure(instances, "instance_1")
    enable_recovery_failure(instances, "instance_2")

    interactive_mg_runner.start(instances, "instance_1")
    wait_main(coord_cursor, "instance_1")
    main_cursor = connect(host="localhost", port=7687).cursor()
    use_tenant(main_cursor)
    assert storage_info(main_cursor)["status"] == "broken"

    interactive_mg_runner.start(instances, "instance_2")
    wait_replica(coord_cursor, "instance_2")
    replica_cursor = connect(host="localhost", port=7688).cursor()
    use_tenant(replica_cursor)

    # Verify broken on BOTH instances via BOTH SHOW STORAGE INFO and SHOW DATABASES.
    assert storage_info(main_cursor)["status"] == "broken"
    assert storage_info(replica_cursor)["status"] == "broken"
    assert databases_status(main_cursor).get(TENANT) == "broken"
    assert databases_status(replica_cursor).get(TENANT) == "broken"
    # The default database stayed healthy on both.
    assert databases_status(main_cursor).get("memgraph") == "ready"
    assert databases_status(replica_cursor).get("memgraph") == "ready"

    # Cure the main with RECOVER SNAPSHOT; the replica self-heals via snapshot sync from the main.
    execute_and_fetch_all(main_cursor, f"RECOVER SNAPSHOT '{good_snapshot}'")
    assert storage_info(main_cursor)["status"] == "ready"
    assert get_vertex_count(main_cursor) == 5000

    # Nudge the main with a write to the tenant so it drives the broken replica through recovery.
    nudge_main_write(main_cursor, "CREATE (:Node {id: 999999})")

    # Slice 7: the broken replica self-heals in place. The main notices it is behind and full-syncs
    # it (snapshot + WAL); the replica-side recovery handlers clear the broken flag on a successful
    # load. So the replica reaches ready and serves the tenant WITHOUT a restart. Fresh connections
    # are opened per poll because the self-heal replaces the storage object underneath any session
    # pinned to the old broken view.
    def replica_storage_status():
        c = connect(host="localhost", port=7688).cursor()
        use_tenant(c)
        return storage_info(c).get("status")

    def replica_databases_status():
        c = connect(host="localhost", port=7688).cursor()
        return databases_status(c).get(TENANT)

    def replica_tenant_count():
        c = connect(host="localhost", port=7688).cursor()
        use_tenant(c)
        return get_vertex_count(c)

    mg_sleep_and_assert("ready", replica_storage_status)
    mg_sleep_and_assert("ready", replica_databases_status)
    mg_sleep_and_assert(5001, replica_tenant_count)

    os.remove(good_snapshot)


# ---------------------------------------------------------------------------------------------
# Scenario 3: RESET DATABASE on the main + import -> data replicates to the replica.
# ---------------------------------------------------------------------------------------------
def test_main_corrupt_cured_with_reset_database_and_import(test_name):
    instances, coord_cursor = setup_cluster_with_tenant(test_name)

    # Keep the replica down while the operator resets the main so the coordinator cannot fail
    # over to a healthy instance: instance_1 stays main and RESET runs on the corrupt main.
    interactive_mg_runner.kill(instances, "instance_1")
    interactive_mg_runner.kill(instances, "instance_2")
    corrupt_tenant_durability(data_dir_of(test_name, "instance_1"))
    enable_recovery_failure(instances, "instance_1")
    interactive_mg_runner.start(instances, "instance_1")
    wait_main(coord_cursor, "instance_1")

    # RESET DATABASE resets the tenant to an empty, ready state on instance_1 while it is main.
    main1_cursor = connect(host="localhost", port=7687).cursor()
    use_tenant(main1_cursor)
    assert storage_info(main1_cursor)["status"] == "broken"
    execute_and_fetch_all(main1_cursor, "RESET DATABASE")
    assert storage_info(main1_cursor)["status"] == "ready"
    assert get_vertex_count(main1_cursor) == 0

    # Bring the replica back. Re-resolve the main afterwards: rejoining can trigger a failover, so
    # we drive the import at whichever instance is the stable main rather than assuming instance_1.
    interactive_mg_runner.start(instances, "instance_2")
    cursor, _ = resolve_main_cursor(coord_cursor)
    use_tenant(cursor)

    # Wait for the replica to be in sync on the main before the single import, so the SYNC write
    # lands on both instances on the first try (a retried write would double-insert).
    wait_replica_ready_on_main(cursor)

    # Import fresh data on the reset tenant and verify it replicates across the whole cluster.
    wait_until_main_writeable(cursor, "UNWIND range(1, 1234) AS i CREATE (:Imported {id: i})")
    assert execute_and_fetch_all(cursor, "MATCH (n:Imported) RETURN count(n)")[0][0] == 1234

    def tenant_imported_count(port):
        c = connect(host="localhost", port=port).cursor()
        use_tenant(c)
        return execute_and_fetch_all(c, "MATCH (n:Imported) RETURN count(n)")[0][0]

    mg_sleep_and_assert(1234, partial(tenant_imported_count, 7687))
    mg_sleep_and_assert(1234, partial(tenant_imported_count, 7688))


def setup_cluster_two_dbs(test_name):
    """Bring up the cluster with instance_1 MAIN / instance_2 REPLICA, populate and snapshot BOTH the
    default `memgraph` database and a named `db1`, and confirm both replicated to instance_2. Returns
    (instances_description, coord_cursor)."""
    test_data_root = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", get_data_path(file, test_name))
    shutil.rmtree(test_data_root, ignore_errors=True)

    instances = get_instances_description(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)

    coord_cursor = connect(host="localhost", port=7692).cursor()
    for query in get_setup_queries():
        execute_and_fetch_all(coord_cursor, query)

    wait_main(coord_cursor, "instance_1")
    wait_replica(coord_cursor, "instance_2")

    main_cursor = connect(host="localhost", port=7687).cursor()
    # Default db (current on connect): populate + snapshot.
    execute_and_fetch_all(main_cursor, f"UNWIND range(1, {DEFAULT_DB_COUNT}) AS i CREATE (:Node {{id: i}})")
    execute_and_fetch_all(main_cursor, "CREATE SNAPSHOT")
    # Named db1: create, populate + snapshot.
    execute_and_fetch_all(main_cursor, f"CREATE DATABASE {DB1}")
    execute_and_fetch_all(main_cursor, f"USE DATABASE {DB1}")
    execute_and_fetch_all(main_cursor, f"UNWIND range(1, {DB1_COUNT}) AS i CREATE (:Node {{id: i}})")
    execute_and_fetch_all(main_cursor, "CREATE SNAPSHOT")

    # Confirm both databases replicated to instance_2 before we take the cluster down.
    mg_sleep_and_assert(DEFAULT_DB_COUNT, partial(db_count_on, 7688, DEFAULT_DB))
    mg_sleep_and_assert(DB1_COUNT, partial(db_count_on, 7688, DB1))

    return instances, coord_cursor


# ---------------------------------------------------------------------------------------------
# Scenario 4: split broken databases + manual failover converges the whole cluster.
#
#   After restart each instance has exactly one broken db and one ready db, mirror images:
#     instance_1: memgraph BROKEN, db1 READY      instance_2: memgraph READY, db1 BROKEN
#   Broken-ness is invisible to the coordinator, so either instance may be elected main. Whoever
#   becomes main automatically heals the replica's copy of the db the main holds READY (a broken db on
#   the main is intentionally NOT driven, so the other db stays split). A manual failover then makes the
#   old replica the main, which heals the remaining broken db, converging to all-ready + all data.
# ---------------------------------------------------------------------------------------------
def test_split_broken_dbs_manual_failover_converges(test_name):
    instances, coord_cursor = setup_cluster_two_dbs(test_name)

    # Stash a good db1 snapshot (from the main) to plant a corruptible snapshot on the replica's db1,
    # which otherwise has only WAL (no snapshot) on disk.
    good_db1_snapshot = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", f"{file}_{test_name}_good_db1")
    stash_good_tenant_snapshot(data_dir_of(test_name, "instance_1"), good_db1_snapshot)

    # Take both instances down and corrupt DIFFERENT databases on each: the default `memgraph` db on
    # instance_1 (its own snapshot at the data-dir root) and `db1` on instance_2 (plant a corrupt
    # snapshot into its db1 dir). Restart both with --storage-allow-recovery-failure so each boots with
    # one broken db.
    interactive_mg_runner.kill(instances, "instance_1")
    interactive_mg_runner.kill(instances, "instance_2")
    corrupt_default_db_durability(data_dir_of(test_name, "instance_1"))
    plant_corrupt_snapshot(data_dir_of(test_name, "instance_2"), good_db1_snapshot)
    enable_recovery_failure(instances, "instance_1")
    enable_recovery_failure(instances, "instance_2")
    interactive_mg_runner.start(instances, "instance_1")
    interactive_mg_runner.start(instances, "instance_2")

    # Either instance can win the election; adapt to whoever became main.
    main_name = wait_stable_main(coord_cursor)
    replica_name = "instance_2" if main_name == "instance_1" else "instance_1"
    main_port = INSTANCE_BOLT_PORT[main_name]
    replica_port = INSTANCE_BOLT_PORT[replica_name]

    broken_on = {"instance_1": DEFAULT_DB, "instance_2": DB1}
    count_of = {DEFAULT_DB: DEFAULT_DB_COUNT, DB1: DB1_COUNT}
    # With only two dbs, the main's READY db is exactly the db broken on the (mirror) replica, and the
    # main's BROKEN db is the db that is ready on the replica.
    main_ready_db = broken_on[replica_name]
    main_broken_db = broken_on[main_name]

    # The main holds its ready db ready and its own broken db broken; a broken main db is not self-healed.
    assert db_status_on(main_port, main_ready_db) == "ready"
    assert db_status_on(main_port, main_broken_db) == "broken"

    # Whoever became main synced the db it holds ready onto the replica (curing the replica's broken copy).
    mg_sleep_and_assert("ready", partial(db_status_on, replica_port, main_ready_db))
    mg_sleep_and_assert(count_of[main_ready_db], partial(db_count_on, replica_port, main_ready_db))

    # The other db is still split: broken on the main, ready on the replica. Manual failover: demote the
    # current main and promote the old replica, which holds that db ready and will heal it on the new
    # replica (old main).
    execute_and_fetch_all(coord_cursor, f"DEMOTE INSTANCE {main_name}")
    execute_and_fetch_all(coord_cursor, f"SET INSTANCE {replica_name} TO MAIN")
    wait_main(coord_cursor, replica_name)
    wait_replica(coord_cursor, main_name)

    # The new main heals the previously-broken-on-old-main db on the new replica (old main).
    mg_sleep_and_assert("ready", partial(db_status_on, main_port, main_broken_db))
    mg_sleep_and_assert(count_of[main_broken_db], partial(db_count_on, main_port, main_broken_db))

    # Converged: every database is ready with all data present on both instances.
    for port in (main_port, replica_port):
        for db in (DEFAULT_DB, DB1):
            mg_sleep_and_assert("ready", partial(db_status_on, port, db))
            mg_sleep_and_assert(count_of[db], partial(db_count_on, port, db))

    os.remove(good_db1_snapshot)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
