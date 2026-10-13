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

# A replica that fails to load what MAIN sends it during recovery has already cleared its storage. It must not look like
# a healthy empty database: it has to be broken (refuses queries, writes no snapshots) so that restart recovery still
# finds the pre-failure data, and the next successful recovery from MAIN must heal it.
#
# The load failure is produced by corrupting the data region of MAIN's snapshot / WAL files (header stays readable, so
# MAIN selects and sends them and the replica's load fails). The snapshot tests use a plain MAIN + REPLICA; the WAL
# reset test needs a coordinator, because only a coordinator-managed MAIN resets a diverged replica.

import os
import shutil
import sys
import threading
import time
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

file = "replica_recovery_failure"

NUM_NODES = 5000
MAIN_BOLT = 7787
REPLICA_BOLT = 7788
REPLICA_REPL_PORT = 10701


@pytest.fixture
def test_name(request):
    return request.node.name


@pytest.fixture(autouse=True)
def cleanup_instances(test_name):
    # Instances stopped with keep_directories=True are no longer tracked by the runner, so remove their data here.
    data_root = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", get_data_path(file, test_name))
    logs_root = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "logs", get_logs_path(file, test_name))
    interactive_mg_runner.kill_all(keep_directories=False)
    # Logs are appended to across runs and the tests grep them.
    shutil.rmtree(data_root, ignore_errors=True)
    shutil.rmtree(logs_root, ignore_errors=True)
    yield
    interactive_mg_runner.kill_all(keep_directories=False)
    shutil.rmtree(data_root, ignore_errors=True)


def get_instances(test_name):
    common_args = [
        "--log-level=INFO",
        "--data-recovery-on-startup=true",
        "--replication-restore-state-on-startup=true",
        "--storage-wal-enabled=true",
        "--storage-wal-file-size-kib=64",
    ]
    return {
        "main": {
            "args": ["--bolt-port", str(MAIN_BOLT), "--storage-snapshot-retention-count=1"] + common_args,
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [],
        },
        "replica": {
            "args": ["--bolt-port", str(REPLICA_BOLT), "--storage-snapshot-interval-sec=1"] + common_args,
            "log_file": f"{get_logs_path(file, test_name)}/replica.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica",
            "setup_queries": [],
        },
    }


def data_dir(test_name, instance):
    return os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", get_data_path(file, test_name), instance)


def log_text(test_name, instance):
    # Memgraph appends the date to the log file name.
    logs_dir = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "logs", get_logs_path(file, test_name))
    text = ""
    for name in sorted(os.listdir(logs_dir)):
        if name.startswith(f"{instance}_"):
            with open(os.path.join(logs_dir, name), errors="replace") as fh:
                text += fh.read()
    return text


def files_in(directory):
    return {f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))}


def health(cursor):
    return {row[0]: row[2] for row in execute_and_fetch_all(cursor, "SHOW DATABASES")}["memgraph"]


def vertex_count(cursor):
    return execute_and_fetch_all(cursor, "MATCH (n) RETURN count(n)")[0][0]


def write_batches(main_cursor, batches, snapshot):
    for _ in range(batches):
        execute_and_fetch_all(main_cursor, "UNWIND range(1, 3000) AS i CREATE (:Node {id: i})")
        if snapshot:
            execute_and_fetch_all(main_cursor, "CREATE SNAPSHOT")


def prepare_replica_needing_snapshot(test_name):
    """Replica holding NUM_NODES nodes (and a snapshot of them) is stopped, MAIN moves on and prunes its WAL chain, so
    the replica needs a snapshot to catch up. Returns the instances and the replica's snapshot dir."""
    instances = get_instances(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)
    main_cursor = connect(host="localhost", port=MAIN_BOLT).cursor()
    replica_cursor = connect(host="localhost", port=REPLICA_BOLT).cursor()
    execute_and_fetch_all(replica_cursor, f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICA_REPL_PORT}")
    execute_and_fetch_all(main_cursor, f"REGISTER REPLICA r1 ASYNC TO 'localhost:{REPLICA_REPL_PORT}'")
    execute_and_fetch_all(main_cursor, f"UNWIND range(1, {NUM_NODES}) AS i CREATE (:Node {{id: i}})")
    mg_sleep_and_assert(NUM_NODES, partial(vertex_count, replica_cursor))
    replica_snapshots = os.path.join(data_dir(test_name, "replica"), "snapshots")
    mg_sleep_and_assert(True, lambda: len(files_in(replica_snapshots)) > 0)

    interactive_mg_runner.stop(instances, "replica", keep_directories=True)
    write_batches(main_cursor, batches=3, snapshot=True)
    return instances, replica_snapshots


def setup_failed_snapshot_load(test_name):
    """As above, but MAIN's snapshot is corrupted, so the replica's load fails. Returns the instances and the replica's
    snapshot files before the failure."""
    instances, replica_snapshots = prepare_replica_needing_snapshot(test_name)
    corrupt_snapshots(data_dir(test_name, "main"))
    before = files_in(replica_snapshots)
    interactive_mg_runner.start(instances, "replica")
    mg_sleep_and_assert(True, lambda: "Couldn't load the snapshot" in log_text(test_name, "replica"), max_duration=60)
    return instances, before


def test_failed_snapshot_load_is_broken_and_writes_no_snapshots(test_name):
    _, before = setup_failed_snapshot_load(test_name)
    replica_cursor = connect(host="localhost", port=REPLICA_BOLT).cursor()
    replica_snapshots = os.path.join(data_dir(test_name, "replica"), "snapshots")

    # The replica must never present the cleared storage as a healthy database.
    mg_sleep_and_assert("broken", partial(health, replica_cursor))
    with pytest.raises(Exception):
        vertex_count(replica_cursor)

    # No periodic snapshot of the emptied storage, and the failed received snapshot is gone.
    time.sleep(4)
    assert health(replica_cursor) == "broken"
    assert files_in(replica_snapshots) == before

    # Once MAIN has a loadable snapshot, the next recovery heals the replica.
    main_cursor = connect(host="localhost", port=MAIN_BOLT).cursor()
    execute_and_fetch_all(main_cursor, "CREATE SNAPSHOT")
    expected = vertex_count(main_cursor)
    mg_sleep_and_assert("ready", partial(health, replica_cursor), max_duration=60)
    mg_sleep_and_assert(expected, partial(vertex_count, replica_cursor))


def test_restart_after_failed_snapshot_load_restores_data(test_name):
    instances, _ = setup_failed_snapshot_load(test_name)

    # Let the 1s periodic snapshot fire: on a replica that wrongly looks healthy and empty, it writes an empty snapshot
    # that sorts newest and would be loaded on restart.
    time.sleep(5)

    # Only the snapshots may decide what restart recovers: no MAIN to heal the replica and no WAL to replay.
    interactive_mg_runner.kill(instances, "main", keep_directories=True)
    interactive_mg_runner.stop(instances, "replica", keep_directories=True)
    wal_dir = os.path.join(data_dir(test_name, "replica"), "wal")
    for name in files_in(wal_dir):
        os.remove(os.path.join(wal_dir, name))
    interactive_mg_runner.start(instances, "replica")

    replica_cursor = connect(host="localhost", port=REPLICA_BOLT).cursor()
    assert vertex_count(replica_cursor) == NUM_NODES


def test_unreadable_received_snapshot_leaves_replica_untouched(test_name):
    # MAIN only sends snapshots whose header it can read, so the received file is damaged on the replica itself: a
    # read transaction holds the replica's main lock after the snapshot lands in its snapshots dir, and the header is
    # overwritten before the transaction ends. MAIN is stopped while the replica comes up and is only started once the
    # blocker transaction and the watcher are in place, so the first snapshot it sends is already covered.
    instances, replica_snapshots = prepare_replica_needing_snapshot(test_name)
    interactive_mg_runner.stop(instances, "main", keep_directories=True)
    main_names = files_in(os.path.join(data_dir(test_name, "main"), "snapshots"))
    assert main_names
    stop = threading.Event()
    damaged = threading.Event()

    def damage_received_snapshot():
        while not stop.is_set():
            for name in main_names & files_in(replica_snapshots):
                try:
                    with open(os.path.join(replica_snapshots, name), "r+b") as fh:
                        fh.write(b"\xff" * 64)
                except FileNotFoundError:  # already deleted by the replica
                    continue
                damaged.set()

    interactive_mg_runner.start(instances, "replica")
    blocker = None
    for _ in range(100):
        try:
            blocker = connect(host="localhost", port=REPLICA_BOLT)
            break
        except Exception:
            time.sleep(0.1)
    assert blocker is not None
    blocker.autocommit = False
    blocker_cursor = blocker.cursor()
    assert vertex_count(blocker_cursor) == NUM_NODES

    watcher = threading.Thread(target=damage_received_snapshot)
    watcher.start()
    try:
        interactive_mg_runner.start(instances, "main")
        assert damaged.wait(timeout=60), "MAIN never sent its snapshot"
        # The replica's handler keeps going after the peer is gone, and MAIN can no longer re-send an undamaged file.
        interactive_mg_runner.kill(instances, "main", keep_directories=True)
        blocker.rollback()
        mg_sleep_and_assert(
            True,
            lambda: "Couldn't read the received snapshot" in log_text(test_name, "replica"),
            max_duration=60,
        )
    finally:
        stop.set()
        watcher.join()

    replica_cursor = connect(host="localhost", port=REPLICA_BOLT).cursor()
    assert health(replica_cursor) == "ready"
    assert vertex_count(replica_cursor) == NUM_NODES
    assert "Clearing database" not in log_text(test_name, "replica")
    # The damaged file is deleted by the replica.
    mg_sleep_and_assert(True, lambda: main_names.isdisjoint(files_in(replica_snapshots)))


def get_ha_instances(test_name):
    def data_instance(bolt, management, extra_args):
        return {
            "args": [
                "--bolt-port",
                str(bolt),
                "--management-port",
                str(management),
                "--log-level=INFO",
                "--replication-restore-state-on-startup=true",
                "--data-recovery-on-startup=true",
                "--storage-wal-enabled=true",
                "--storage-wal-file-size-kib=64",
            ]
            + extra_args,
        }

    instances = {
        "instance_1": data_instance(7788, 10711, ["--storage-snapshot-interval-sec=1"]),
        "instance_2": data_instance(7789, 10712, []),
        "coordinator": {
            "args": [
                "--bolt-port=7790",
                "--log-level=INFO",
                "--coordinator-id=1",
                "--coordinator-port=10811",
                "--coordinator-hostname=localhost",
                "--management-port=10821",
            ],
            "setup_queries": [
                "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7790', 'coordinator_server': 'localhost:10811', 'management_server': 'localhost:10821'}",
                "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7788', 'management_server': 'localhost:10711', 'replication_server': 'localhost:10701'};",
                "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7789', 'management_server': 'localhost:10712', 'replication_server': 'localhost:10702'};",
                "SET INSTANCE instance_1 TO MAIN",
            ],
        },
    }
    for name, description in instances.items():
        description["log_file"] = f"{get_logs_path(file, test_name)}/{name}.log"
        description["data_directory"] = f"{get_data_path(file, test_name)}/{name}"
        description.setdefault("setup_queries", [])
    return instances


def test_failed_wal_reset_is_broken(test_name):
    # instance_1 (MAIN) gets ahead of instance_2, then instance_2 is promoted and writes its own history. When
    # instance_1 rejoins, the new MAIN resets it (clears it) before sending WAL files. The new MAIN's finalized WAL
    # files are corrupted, so the load fails after the clear, on every retry.
    instances = get_ha_instances(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)
    coordinator_cursor = connect(host="localhost", port=7790).cursor()
    mg_sleep_and_assert(
        {"main", "replica"},
        lambda: {row[5] for row in show_instances(coordinator_cursor) if row[0].startswith("instance_")},
    )
    cursor_1 = connect(host="localhost", port=7788).cursor()
    cursor_2 = connect(host="localhost", port=7789).cursor()
    execute_and_fetch_all(cursor_1, "UNWIND range(1, 1000) AS i CREATE (:Node {id: i})")
    mg_sleep_and_assert(1000, partial(vertex_count, cursor_2))

    interactive_mg_runner.stop(instances, "instance_2", keep_directories=True)
    execute_and_fetch_all(cursor_1, "UNWIND range(1, 1000) AS i CREATE (:Node {id: i})")
    snapshots_1 = os.path.join(data_dir(test_name, "instance_1"), "snapshots")
    mg_sleep_and_assert(True, lambda: len(files_in(snapshots_1)) > 0)
    interactive_mg_runner.stop(instances, "instance_1", keep_directories=True)
    interactive_mg_runner.start(instances, "instance_2")

    cursor_2 = connect(host="localhost", port=7789).cursor()
    mg_sleep_and_assert(
        "main", lambda: {row[0]: row[5] for row in show_instances(coordinator_cursor)}["instance_2"], max_duration=60
    )

    def try_write():
        try:
            execute_and_fetch_all(cursor_2, "UNWIND range(1, 3000) AS i CREATE (:Node {id: i})")
            return True
        except Exception:  # still a replica
            return False

    mg_sleep_and_assert(True, try_write, max_duration=60)
    write_batches(cursor_2, batches=2, snapshot=False)
    wal_2 = os.path.join(data_dir(test_name, "instance_2"), "wal")
    wals = sorted(os.listdir(wal_2), key=lambda f: os.path.getmtime(os.path.join(wal_2, f)))
    assert len(wals) > 1
    for name in wals[:-1]:  # all finalized files
        path = os.path.join(wal_2, name)
        with open(path, "r+b") as fh:
            fh.seek(1024)
            fh.write(b"\xff" * (os.path.getsize(path) - 1024))

    interactive_mg_runner.start(instances, "instance_1")
    cursor_1 = connect(host="localhost", port=7788).cursor()

    mg_sleep_and_assert("broken", partial(health, cursor_1))

    marker = "because the reset is needed while recovering from WalFiles"
    assert marker in log_text(test_name, "instance_1")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
