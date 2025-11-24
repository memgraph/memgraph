# Copyright 2024 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

TMP_DIR = "/tmp/e2e_snapshot_recovery"

import datetime
import os


def get_snapshots():
    snapshot_dir = TMP_DIR + "/snapshots"
    entries = (os.path.join(snapshot_dir, entry) for entry in os.listdir(snapshot_dir))
    files = [f for f in entries if os.path.isfile(f)]
    if files:
        # Sort by creation time (ns), most recent first
        files.sort(key=lambda f: os.stat(f).st_ctime_ns, reverse=True)
        return files
    return None


@pytest.fixture
def global_snapshot():
    snapshots = get_snapshots()
    assert len(snapshots) != 0
    return snapshots[0]


@pytest.fixture
def global_old_snapshot():
    snapshots = get_snapshots()
    assert len(snapshots) > 1
    return snapshots[1]


def memgraph_instances(dir):
    return {
        "default": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=false",
                "--storage-snapshot-interval-sec=1000",
                "--storage-wal-enabled=true",
                "--storage-snapshot-on-exit=false",
                "--storage-wal-file-size-kib=1",
            ],
            "log_file": "snapshot_recovery_default.log",
            "data_directory": dir,
        },
        "recover_on_startup": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--storage-snapshot-interval-sec=1000",
                "--storage-wal-enabled=true",
                "--storage-snapshot-on-exit=false",
            ],
            "log_file": "snapshot_recovery_recover_on_startup.log",
            "data_directory": dir,
        },
    }


def generate_tmp_snapshot():
    interactive_mg_runner.start(memgraph_instances(TMP_DIR), "default")
    connection = connect(host="localhost", port=7687)
    cursor = connection.cursor()

    for i in range(0, 5):
        execute_and_fetch_all(cursor, f"CREATE ({{p: {i}}});")
    execute_and_fetch_all(cursor, "CREATE SNAPSHOT;")

    time.sleep(0.1)

    for i in range(5, 10):
        execute_and_fetch_all(cursor, f"CREATE ({{p: {i}}});")
    execute_and_fetch_all(cursor, "CREATE SNAPSHOT;")

    interactive_mg_runner.stop_all()


def fixture_snapshot_in_mg_data():
    pass


def data_check(cursor, max):
    index = 0
    step = 1
    result = execute_and_fetch_all(cursor, "MATCH(n) RETURN n.p ORDER BY n.p ASC;")
    for i in result:
        assert i[0] == index, f"Expecting {index} but got {i[0]}"
        index += step
    assert index == max, f"Expecting maximum {max} but got {index}"


def main_test(data_directory, snapshot, database, clean):
    # 1) recover from defined snapshot
    # 2) check data
    # 3) add data
    # 4) check data
    # 5) restart
    # 6) check data

    connection = connect(host="localhost", port=7687)
    cursor = mt_cursor(connection, database)

    # 1
    try:
        execute_and_fetch_all(cursor, f"RECOVER SNAPSHOT '{snapshot}';")
    except:
        assert not clean, "Failed to recover from snapshot even though force is not required"
    if not clean:
        execute_and_fetch_all(cursor, f"RECOVER SNAPSHOT '{snapshot}' FORCE;")

    # 2
    # Check that the snapshot has been copied over (datetime will be different)
    for entry in os.listdir(data_directory + "/snapshots"):
        if not os.path.isdir(data_directory + "/snapshots/" + entry):
            assert entry.endswith(
                os.path.basename(snapshot).split("_timestamp_")[1]
            ), "Missing recovered snapshot from local dir"
            break
    # Check that the old wal/snapshots have been moved to .old directory
    if not clean:
        found = False
        # Check for .old directory in snapshots
        old_snapshots_dir = data_directory + "/snapshots/.old"
        if os.path.exists(old_snapshots_dir) and os.path.isdir(old_snapshots_dir):
            found = len(os.listdir(old_snapshots_dir)) != 0
        # If not found in snapshots, check in wal
        if not found:
            old_wal_dir = data_directory + "/wal/.old"
            if os.path.exists(old_wal_dir) and os.path.isdir(old_wal_dir):
                found = len(os.listdir(old_wal_dir)) != 0
        print(f"Found .old directory with content: {found}")
        print(f"Snapshots directory contents: {os.listdir(data_directory + '/snapshots')}")
        print(f"WAL directory contents: {os.listdir(data_directory + '/wal')}")
        assert found, "Missing .old directory or .old directory is empty"
    # check data
    data_check(cursor, 10)

    # 3
    for i in range(10, 20):
        execute_and_fetch_all(cursor, f"CREATE ({{p: {i}}});")

    # 4
    data_check(cursor, 20)
    if database != "memgraph":
        execute_and_fetch_all(cursor, f"USE DATABASE memgraph;")
        assert execute_and_fetch_all(cursor, "MATCH(n) RETURN count(*);")[0][0] == 0, "Dirty default db"


def main_test_reboot(database):
    # 5
    connection = connect(host="localhost", port=7687)
    cursor = mt_cursor(connection, database)

    # 6
    data_check(cursor, 20)
    if database != "memgraph":
        execute_and_fetch_all(cursor, f"USE DATABASE memgraph;")
        assert execute_and_fetch_all(cursor, "MATCH(n) RETURN count(*);")[0][0] == 0, "Dirty default db"


def mt_setup():
    connection = connect(host="localhost", port=7687)
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "CREATE DATABASE other_db;")


def mt_data_dir(data_directory, database):
    if database != "memgraph":
        for entry in os.listdir(data_directory + "/databases"):
            if entry != "memgraph" and entry[0] != "." and os.path.isdir(data_directory + "/databases/" + entry):
                print(data_directory + "/databases/" + entry)
                return data_directory + "/databases/" + entry
    else:
        return data_directory
    return None


def mt_cursor(connection, database):
    cursor = connection.cursor()
    if database != "memgraph":
        execute_and_fetch_all(cursor, f"USE DATABASE {database};")
    return cursor


@pytest.mark.parametrize("database", ["memgraph", "other_db"])
def test_empty(global_snapshot, database):
    assert global_snapshot is not None, "To snapshot to recover from"
    data_directory = tempfile.TemporaryDirectory()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "default")
    mt_setup()
    main_test(mt_data_dir(data_directory.name, database), global_snapshot, database, True)
    interactive_mg_runner.kill_all()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "recover_on_startup")
    main_test_reboot(database)
    interactive_mg_runner.kill_all()


def test_empty_with_local_snapshot(global_snapshot):
    assert global_snapshot != None, "To snapshot to recover from"
    data_directory = tempfile.TemporaryDirectory()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "default")
    local_snapshot = data_directory.name + "/snapshots/" + os.path.basename(global_snapshot)
    shutil.copyfile(global_snapshot, local_snapshot)
    main_test(mt_data_dir(data_directory.name, "memgraph"), local_snapshot, "memgraph", True)
    interactive_mg_runner.kill_all()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "recover_on_startup")
    main_test_reboot("memgraph")
    interactive_mg_runner.kill_all()


@pytest.mark.parametrize("database", ["memgraph", "other_db"])
def test_only_current_wal(global_snapshot, database):
    assert global_snapshot != None, "To snapshot to recover from"
    data_directory = tempfile.TemporaryDirectory()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "default")
    mt_setup()

    # Add new data so we have a current wal
    connection = connect(host="localhost", port=7687)
    cursor = mt_cursor(connection, database)
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")

    main_test(mt_data_dir(data_directory.name, database), global_snapshot, database, False)
    interactive_mg_runner.kill_all()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "recover_on_startup")
    main_test_reboot(database)
    interactive_mg_runner.kill_all()


@pytest.mark.parametrize("database", ["memgraph", "other_db"])
def test_only_finalized_wals(global_snapshot, database):
    assert global_snapshot != None, "To snapshot to recover from"
    data_directory = tempfile.TemporaryDirectory()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "default")
    mt_setup()

    # Add new data so we have only finalized wals
    connection = connect(host="localhost", port=7687)
    cursor = mt_cursor(connection, database)
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")

    main_test(mt_data_dir(data_directory.name, database), global_snapshot, database, False)
    interactive_mg_runner.kill_all()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "recover_on_startup")
    main_test_reboot(database)
    interactive_mg_runner.kill_all()


@pytest.mark.parametrize("database", ["memgraph", "other_db"])
def test_both_wals(global_snapshot, database):
    assert global_snapshot != None, "To snapshot to recover from"
    data_directory = tempfile.TemporaryDirectory()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "default")
    mt_setup()

    # Add new data so we have only finalized wals
    connection = connect(host="localhost", port=7687)
    cursor = mt_cursor(connection, database)
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")

    main_test(mt_data_dir(data_directory.name, database), global_snapshot, database, False)
    interactive_mg_runner.kill_all()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "recover_on_startup")
    main_test_reboot(database)
    interactive_mg_runner.kill_all()


def test_snapshot(global_snapshot, global_old_snapshot):
    assert global_snapshot != None, "To snapshot to recover from"
    assert global_old_snapshot != None, "To snapshot to recover from"
    data_directory = tempfile.TemporaryDirectory()
    os.mkdir(data_directory.name + "/snapshots")
    shutil.copyfile(global_old_snapshot, data_directory.name + "/snapshots/" + os.path.basename(global_old_snapshot))
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "recover_on_startup")
    data_check(connect(host="localhost", port=7687).cursor(), 5)
    main_test(mt_data_dir(data_directory.name, "memgraph"), global_snapshot, "memgraph", False)
    interactive_mg_runner.kill_all()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "recover_on_startup")
    main_test_reboot("memgraph")
    interactive_mg_runner.kill_all()


def test_snapshot_and_current_wal(global_snapshot, global_old_snapshot):
    assert global_snapshot != None, "To snapshot to recover from"
    assert global_old_snapshot != None, "To snapshot to recover from"
    data_directory = tempfile.TemporaryDirectory()
    os.mkdir(data_directory.name + "/snapshots")
    shutil.copyfile(global_old_snapshot, data_directory.name + "/snapshots/" + os.path.basename(global_old_snapshot))
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "recover_on_startup")
    data_check(connect(host="localhost", port=7687).cursor(), 5)

    # Add new data so we have a current wal
    connection = connect(host="localhost", port=7687)
    cursor = mt_cursor(connection, "memgraph")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")

    main_test(mt_data_dir(data_directory.name, "memgraph"), global_snapshot, "memgraph", False)
    interactive_mg_runner.kill_all()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "recover_on_startup")
    main_test_reboot("memgraph")
    interactive_mg_runner.kill_all()


def test_snapshot_and_finalized_wals(global_snapshot, global_old_snapshot):
    assert global_snapshot != None, "To snapshot to recover from"
    assert global_old_snapshot != None, "To snapshot to recover from"
    data_directory = tempfile.TemporaryDirectory()
    os.mkdir(data_directory.name + "/snapshots")
    shutil.copyfile(global_old_snapshot, data_directory.name + "/snapshots/" + os.path.basename(global_old_snapshot))
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "recover_on_startup")
    data_check(connect(host="localhost", port=7687).cursor(), 5)

    # Add new data so we have a current wal
    connection = connect(host="localhost", port=7687)
    cursor = mt_cursor(connection, "memgraph")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")

    main_test(mt_data_dir(data_directory.name, "memgraph"), global_snapshot, "memgraph", False)
    interactive_mg_runner.kill_all()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "recover_on_startup")
    main_test_reboot("memgraph")
    interactive_mg_runner.kill_all()


def test_snapshot_and_both_wals(global_snapshot, global_old_snapshot):
    assert global_snapshot != None, "To snapshot to recover from"
    assert global_old_snapshot != None, "To snapshot to recover from"
    data_directory = tempfile.TemporaryDirectory()
    os.mkdir(data_directory.name + "/snapshots")
    shutil.copyfile(global_old_snapshot, data_directory.name + "/snapshots/" + os.path.basename(global_old_snapshot))
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "recover_on_startup")
    data_check(connect(host="localhost", port=7687).cursor(), 5)

    # Add new data so we have a current wal
    connection = connect(host="localhost", port=7687)
    cursor = mt_cursor(connection, "memgraph")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")

    main_test(mt_data_dir(data_directory.name, "memgraph"), global_snapshot, "memgraph", False)
    interactive_mg_runner.kill_all()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "recover_on_startup")
    main_test_reboot("memgraph")
    interactive_mg_runner.kill_all()


@pytest.mark.parametrize("database", ["memgraph", "other_db"])
def test_local_snapshot(global_snapshot, database):
    assert global_snapshot != None, "To snapshot to recover from"
    data_directory = tempfile.TemporaryDirectory()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "default")
    mt_setup()
    connection = connect(host="localhost", port=7687)
    cursor = mt_cursor(connection, database)
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    result = execute_and_fetch_all(cursor, "CREATE SNAPSHOT;")
    # Verify that CREATE SNAPSHOT returned a path and it exists as a file
    assert len(result) == 1, "CREATE SNAPSHOT should return exactly one row"
    assert len(result[0]) == 1, "CREATE SNAPSHOT should return exactly one column (path)"
    snapshot_path = result[0][0]
    assert snapshot_path is not None, "CREATE SNAPSHOT should return a non-null path"
    assert isinstance(snapshot_path, str), "CREATE SNAPSHOT should return a string path"
    # Verify the path exists and is a file
    path_obj = Path(snapshot_path)
    assert path_obj.exists(), f"Snapshot file should exist at {snapshot_path}"
    assert path_obj.is_file(), f"Snapshot should be a file at {snapshot_path}"
    main_test(mt_data_dir(data_directory.name, database), global_snapshot, database, False)
    interactive_mg_runner.kill_all()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "recover_on_startup")
    main_test_reboot(database)
    interactive_mg_runner.kill_all()


@pytest.mark.parametrize("database", ["memgraph", "other_db"])
def test_local_snapshot_and_current_wal(global_snapshot, database):
    assert global_snapshot != None, "To snapshot to recover from"
    data_directory = tempfile.TemporaryDirectory()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "default")
    mt_setup()
    connection = connect(host="localhost", port=7687)
    cursor = mt_cursor(connection, database)
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    result = execute_and_fetch_all(cursor, "CREATE SNAPSHOT;")
    # Verify that CREATE SNAPSHOT returned a path and it exists as a file
    assert len(result) == 1, "CREATE SNAPSHOT should return exactly one row"
    assert len(result[0]) == 1, "CREATE SNAPSHOT should return exactly one column (path)"
    snapshot_path = result[0][0]
    assert snapshot_path is not None, "CREATE SNAPSHOT should return a non-null path"
    assert isinstance(snapshot_path, str), "CREATE SNAPSHOT should return a string path"
    # Verify the path exists and is a file
    path_obj = Path(snapshot_path)
    assert path_obj.exists(), f"Snapshot file should exist at {snapshot_path}"
    assert path_obj.is_file(), f"Snapshot should be a file at {snapshot_path}"
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    main_test(mt_data_dir(data_directory.name, database), global_snapshot, database, False)
    interactive_mg_runner.kill_all()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "recover_on_startup")
    main_test_reboot(database)
    interactive_mg_runner.kill_all()


def test_marked_commits_after_snapshot():
    # 1 create some data
    # 2 create snapshot
    # 3 a couple of read queries
    # 4 recover from snapshot
    # 5 create more data
    # 6 verify deltas are being released

    data_directory = tempfile.TemporaryDirectory()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "default")

    connection = connect(host="localhost", port=7687)
    cursor = mt_cursor(connection, "memgraph")

    # 1
    execute_and_fetch_all(cursor, "CREATE ()")
    execute_and_fetch_all(cursor, "CREATE ()")
    execute_and_fetch_all(cursor, "CREATE ()")
    # 2
    result = execute_and_fetch_all(cursor, "CREATE SNAPSHOT")
    # Verify that CREATE SNAPSHOT returned a path and it exists as a file
    assert len(result) == 1, "CREATE SNAPSHOT should return exactly one row"
    assert len(result[0]) == 1, "CREATE SNAPSHOT should return exactly one column (path)"
    snapshot_path = result[0][0]
    assert snapshot_path is not None, "CREATE SNAPSHOT should return a non-null path"
    assert isinstance(snapshot_path, str), "CREATE SNAPSHOT should return a string path"
    # Verify the path exists and is a file
    path_obj = Path(snapshot_path)
    assert path_obj.exists(), f"Snapshot file should exist at {snapshot_path}"
    assert path_obj.is_file(), f"Snapshot should be a file at {snapshot_path}"
    # 3
    execute_and_fetch_all(cursor, "MATCH (n) RETURN count(*)")
    execute_and_fetch_all(cursor, "MATCH (n) RETURN count(*)")
    execute_and_fetch_all(cursor, "MATCH (n) RETURN count(*)")
    # 4
    dir_path = Path(os.path.join(data_directory.name, "snapshots"))
    assert dir_path.exists()
    assert dir_path.is_dir()
    files = [f for f in dir_path.iterdir() if f.is_file()]
    assert files
    snapshot_path = max(files, key=lambda f: f.stat().st_mtime)
    execute_and_fetch_all(cursor, f'RECOVER SNAPSHOT "{snapshot_path}" FORCE')
    # 5
    execute_and_fetch_all(cursor, "CREATE ()")
    execute_and_fetch_all(cursor, "CREATE ()")
    execute_and_fetch_all(cursor, "CREATE ()")
    # 6
    execute_and_fetch_all(cursor, "FREE MEMORY")
    execute_and_fetch_all(cursor, "FREE MEMORY")
    info = execute_and_fetch_all(cursor, "SHOW STORAGE INFO")
    unreleased_deltas = None
    for [key, val] in info:
        if key == "unreleased_delta_objects":
            unreleased_deltas = val
    assert unreleased_deltas is not None
    assert unreleased_deltas == 0

    interactive_mg_runner.kill_all()


def test_recover_snapshot_already_in_local_dir(global_snapshot):
    """Test recovering from a snapshot that is already in the local snapshots directory."""
    assert global_snapshot is not None, "Need a snapshot to recover from"

    data_directory = tempfile.TemporaryDirectory()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "default")
    mt_setup()

    connection = connect(host="localhost", port=7687)
    cursor = mt_cursor(connection, "memgraph")

    # Create some data and a snapshot
    execute_and_fetch_all(cursor, "CREATE (:TestNode{id: 1});")
    result = execute_and_fetch_all(cursor, "CREATE SNAPSHOT;")
    local_snapshot_path = result[0][0]

    # Verify the snapshot exists in local directory
    assert os.path.exists(local_snapshot_path), f"Local snapshot should exist at {local_snapshot_path}"

    # Create more data
    execute_and_fetch_all(cursor, "CREATE (:TestNode{id: 2});")

    # Recover from the local snapshot
    execute_and_fetch_all(cursor, f'RECOVER SNAPSHOT "{local_snapshot_path}" FORCE;')

    # Verify only the original data exists (id: 1)
    result = execute_and_fetch_all(cursor, "MATCH (n:TestNode) RETURN n.id ORDER BY n.id;")
    assert len(result) == 1, "Should have only one node after recovery"
    assert result[0][0] == 1, "Should have the original node with id 1"

    # Verify .old directory was created and contains the old snapshot
    old_snapshots_dir = os.path.join(data_directory.name, "snapshots", ".old")
    assert os.path.exists(old_snapshots_dir), ".old directory should exist"
    old_files = os.listdir(old_snapshots_dir)
    assert len(old_files) > 0, ".old directory should contain files"

    # Recover from the local snapshot again
    local_snapshots_dir = os.path.join(data_directory.name, "snapshots")
    local_files = [f for f in os.listdir(local_snapshots_dir) if os.path.isfile(os.path.join(local_snapshots_dir, f))]
    assert len(local_files) == 1, "There should be exactly one snapshot file in the local directory"
    local_snapshot_path = os.path.join(local_snapshots_dir, local_files[0])
    execute_and_fetch_all(cursor, f'RECOVER SNAPSHOT "{local_snapshot_path}" FORCE;')

    # Verify only the original data exists (id: 1)
    result = execute_and_fetch_all(cursor, "MATCH (n:TestNode) RETURN n.id ORDER BY n.id;")
    assert len(result) == 1, "Should have only one node after recovery"
    assert result[0][0] == 1, "Should have the original node with id 1"

    # Verify .old directory was created and contains the old snapshot
    old_snapshots_dir = os.path.join(data_directory.name, "snapshots", ".old")
    assert os.path.exists(old_snapshots_dir), ".old directory should exist"
    old_files = os.listdir(old_snapshots_dir)
    assert len(old_files) > 0, ".old directory should contain files"

    interactive_mg_runner.kill_all()


def test_recover_snapshot_already_in_old_dir(global_snapshot):
    """Test recovering from a snapshot that is already in the .old directory."""
    assert global_snapshot is not None, "Need a snapshot to recover from"

    data_directory = tempfile.TemporaryDirectory()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "default")
    mt_setup()

    connection = connect(host="localhost", port=7687)
    cursor = mt_cursor(connection, "memgraph")

    # Create some data and a snapshot
    execute_and_fetch_all(cursor, "CREATE (:TestNode{id: 1});")
    result = execute_and_fetch_all(cursor, "CREATE SNAPSHOT;")
    local_snapshot_path = result[0][0]

    # Create more data and another snapshot to trigger .old directory creation
    execute_and_fetch_all(cursor, "CREATE (:TestNode{id: 2});")
    execute_and_fetch_all(cursor, "CREATE SNAPSHOT;")

    # Now recover from the first snapshot (which should be in .old directory)
    execute_and_fetch_all(cursor, f'RECOVER SNAPSHOT "{local_snapshot_path}" FORCE;')

    # Verify only the original data exists (id: 1)
    result = execute_and_fetch_all(cursor, "MATCH (n:TestNode) RETURN n.id ORDER BY n.id;")
    assert len(result) == 1, "Should have only one node after recovery"
    assert result[0][0] == 1, "Should have the original node with id 1"

    # Verify .old directory still exists and contains files
    old_snapshots_dir = os.path.join(data_directory.name, "snapshots", ".old")
    assert os.path.exists(old_snapshots_dir), ".old directory should exist"
    old_files = os.listdir(old_snapshots_dir)
    assert len(old_files) > 0, ".old directory should contain files"

    interactive_mg_runner.kill_all()


def test_recover_snapshot_uuid_and_name_update(global_snapshot):
    """Test that recovered snapshot gets new UUID and name."""
    assert global_snapshot is not None, "Need a snapshot to recover from"

    data_directory = tempfile.TemporaryDirectory()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "default")
    mt_setup()

    connection = connect(host="localhost", port=7687)
    cursor = mt_cursor(connection, "memgraph")

    # Create some data and a snapshot
    execute_and_fetch_all(cursor, "CREATE (:TestNode{id: 1});")
    result = execute_and_fetch_all(cursor, "CREATE SNAPSHOT;")
    original_snapshot_path = result[0][0]
    original_filename = os.path.basename(original_snapshot_path)

    # Create more data
    execute_and_fetch_all(cursor, "CREATE (:TestNode{id: 2});")

    # Recover from the original snapshot
    execute_and_fetch_all(cursor, f'RECOVER SNAPSHOT "{original_snapshot_path}" FORCE;')

    # Check that the recovered snapshot has a new name (different from original)
    snapshots_dir = os.path.join(data_directory.name, "snapshots")
    current_files = [f for f in os.listdir(snapshots_dir) if os.path.isfile(os.path.join(snapshots_dir, f))]
    assert len(current_files) == 1, "Should have exactly one snapshot file"
    new_filename = current_files[0]
    assert new_filename != original_filename, "Recovered snapshot should have a new name"

    # Verify the original snapshot is backed up in .old directory
    old_snapshots_dir = os.path.join(data_directory.name, "snapshots", ".old")
    assert os.path.exists(old_snapshots_dir), ".old directory should exist"
    old_files = os.listdir(old_snapshots_dir)
    assert original_filename in old_files, "Original snapshot should be backed up in .old directory"

    interactive_mg_runner.kill_all()


def test_snapshot_on_mode_change_analytical_to_transactional():
    data_directory = tempfile.TemporaryDirectory()
    snapshots_dir = data_directory.name + "/snapshots"

    interactive_mg_runner.start(memgraph_instances(data_directory.name), "default")
    connection = connect(host="localhost", port=7687)
    cursor = connection.cursor()

    execute_and_fetch_all(cursor, "STORAGE MODE IN_MEMORY_ANALYTICAL;")
    execute_and_fetch_all(cursor, "CREATE (:Node {id: 1, name: 'first'});")
    execute_and_fetch_all(cursor, "CREATE (:Node {id: 2, name: 'second'});")
    execute_and_fetch_all(cursor, "CREATE (:Node {id: 3, name: 'third'});")

    result = execute_and_fetch_all(cursor, "MATCH (n:Node) RETURN count(n) as count;")
    assert result[0][0] == 3, "Expected 3 nodes before mode change"

    execute_and_fetch_all(cursor, "STORAGE MODE IN_MEMORY_TRANSACTIONAL;")

    interactive_mg_runner.kill_all()

    interactive_mg_runner.start(memgraph_instances(data_directory.name), "recover_on_startup")
    connection = connect(host="localhost", port=7687)
    cursor = connection.cursor()

    result = execute_and_fetch_all(cursor, "MATCH (n:Node) RETURN count(n) as count;")
    assert result[0][0] == 3, "Expected 3 nodes after recovery"

    result = execute_and_fetch_all(cursor, "MATCH (n:Node) RETURN n.id, n.name ORDER BY n.id;")
    assert len(result) == 3, "Expected 3 nodes in result"
    assert result[0][0] == 1 and result[0][1] == "first"
    assert result[1][0] == 2 and result[1][1] == "second"
    assert result[2][0] == 3 and result[2][1] == "third"

    interactive_mg_runner.kill_all()


if __name__ == "__main__":
    # Setup
    try:
        shutil.rmtree(TMP_DIR)
        os.mkdir(TMP_DIR)
    except:
        pass
    generate_tmp_snapshot()

    # Run tests
    res = pytest.main([__file__, "-rA"])

    # Cleanup
    interactive_mg_runner.kill_all()
    try:
        shutil.rmtree(TMP_DIR)
    except:
        pass

    sys.exit(res)
