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


def get_snapshots():
    snapshot_dir = TMP_DIR + "/snapshots"
    entries = (os.path.join(snapshot_dir, entry) for entry in os.listdir(snapshot_dir))
    files = [f for f in entries if os.path.isfile(f)]
    if files:
        # Save last snapshot
        files.sort(key=os.path.getctime, reverse=True)
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
        index = index + step
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
    # Check that the snapshot has been copied over
    for entry in os.listdir(data_directory + "/snapshots"):
        if not os.path.isdir(data_directory + "/snapshots/" + entry):
            assert entry == os.path.basename(snapshot), "Missing recovered snapshot from local dir"
            break
    # Check that the old wal/snapshots have been moved
    if not clean:
        found = False
        for entry in os.listdir(data_directory + "/snapshots"):
            if os.path.isdir(data_directory + "/snapshots/" + entry) and ".old" in entry:
                found = len(os.listdir(data_directory + "/snapshots/" + entry)) != 0
                break
        if not found:
            print(found)
            for entry in os.listdir(data_directory + "/wal"):
                if os.path.isdir(data_directory + "/wal/" + entry) and ".old" in entry:
                    found = len(os.listdir(data_directory + "/wal/" + entry)) != 0
                    break
        print(os.listdir(data_directory + "/snapshots"))
        print(os.listdir(data_directory + "/wal"))
        assert found, "Missing .old"
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
    assert global_snapshot != None, "To snapshot to recover from"
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
    execute_and_fetch_all(cursor, "CREATE SNAPSHOT;")
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
    execute_and_fetch_all(cursor, "CREATE SNAPSHOT;")
    execute_and_fetch_all(cursor, "CREATE (:L{p:'random data'});")
    main_test(mt_data_dir(data_directory.name, database), global_snapshot, database, False)
    interactive_mg_runner.kill_all()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "recover_on_startup")
    main_test_reboot(database)
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
