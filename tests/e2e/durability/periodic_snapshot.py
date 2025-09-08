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
import threading
import time
from typing import Any, Dict

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))


def memgraph_instances(dir, mode="IN_MEMORY_TRANSACTIONAL"):
    assert mode == "IN_MEMORY_TRANSACTIONAL" or mode == "IN_MEMORY_ANALYTICAL"
    return {
        "no_flags": {
            "args": [
                "--log-level=TRACE",
                "--also-log-to-stderr",
                "--data-recovery-on-startup=false",
                "--storage-wal-enabled=false",
                "--storage-snapshot-interval-sec=0",
                "--storage-snapshot-retention-count=20",
                "--storage-mode",
                mode,
            ],
            "log_file": "periodic_snapshot_no_flags.log",
            "data_directory": dir,
        },
        "sec_flag": {
            "args": [
                "--log-level=TRACE",
                "--also-log-to-stderr",
                "--data-recovery-on-startup=false",
                "--storage-snapshot-interval-sec=1",
                "--storage-snapshot-retention-count=20",
                "--storage-mode",
                mode,
            ],
            "log_file": "periodic_snapshot_sec_flag.log",
            "data_directory": dir,
        },
        "interval_flag": {
            "args": [
                "--log-level=TRACE",
                "--also-log-to-stderr",
                "--data-recovery-on-startup=false",
                "--storage-snapshot-interval-sec=0",
                "--storage-snapshot-interval",
                "1",
                "--storage-snapshot-retention-count=20",
                "--storage-mode",
                mode,
            ],
            "log_file": "periodic_snapshot_interval_flag.log",
            "data_directory": dir,
        },
        "both_flags": {
            "args": [
                "--log-level=TRACE",
                "--also-log-to-stderr",
                "--data-recovery-on-startup=false",
                "--storage-snapshot-interval-sec=1",
                "--storage-snapshot-interval",
                "1",
                "--storage-snapshot-retention-count=20",
                "--storage-mode",
                mode,
            ],
            "log_file": "periodic_snapshot_both_flags.log",
            "data_directory": dir,
        },
    }


def number_of_snapshots(dir):
    try:
        entries = (os.path.join(dir, entry) for entry in os.listdir(dir))
        files = [f for f in entries if os.path.isfile(f)]
        print(files)
        return len(files)
    except:
        return 0


# Need to constantly make changes to the database to trigger snapshots
class StoppableThread(threading.Thread):
    def __init__(self):
        super().__init__()
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def run(self):
        connection = connect(host="localhost", port=7687)
        cursor = connection.cursor()
        while not self._stop_event.is_set():
            cursor.execute("CREATE ()")
            time.sleep(0.25)


def main_test(snapshots_dir):
    # 1) pause scheduler
    # 2) check snapshots
    # 3) update scheduler to 1s
    # 4) wait n seconds
    # 5) update scheduler to 10s
    # 6) check there are now n more
    # 7) check until there is one more and timeout if not quick enough

    connection = connect(host="localhost", port=7687)
    cursor = connection.cursor()

    # 1
    execute_and_fetch_all(cursor, "SET DATABASE SETTING 'storage.snapshot.interval' TO '';")

    # 2
    n_snapshots1 = number_of_snapshots(snapshots_dir)

    # 3
    execute_and_fetch_all(cursor, "SET DATABASE SETTING 'storage.snapshot.interval' TO '*/1 * * * * *';")

    thread = StoppableThread()
    thread.start()

    # 4
    time.sleep(5)

    # 5
    execute_and_fetch_all(cursor, "SET DATABASE SETTING 'storage.snapshot.interval' TO '5';")

    # 6
    n_snapshots2 = number_of_snapshots(snapshots_dir)
    assert n_snapshots1 + 7 >= n_snapshots2 >= n_snapshots1 + 4, f"Expected {n_snapshots1 + 5} got {n_snapshots2}"

    # 7
    tries = 0
    n_snapshots3 = n_snapshots2 + 1
    while n_snapshots3 > number_of_snapshots(snapshots_dir) and tries < 15:
        tries = tries + 1
        time.sleep(1)
    assert 2 < tries < 15, "Failed to wait for the next snapshot"
    # Test SHOW SNAPSHOTS (should return only existing snapshots)
    all_snapshots = execute_and_fetch_all(cursor, "SHOW SNAPSHOTS;")
    assert len(all_snapshots) == n_snapshots3  # Only existing snapshots

    # Test SHOW NEXT SNAPSHOT (should return only the next scheduled snapshot)
    next_snapshot = execute_and_fetch_all(cursor, "SHOW NEXT SNAPSHOT;")
    assert len(next_snapshot) == 1  # Should return exactly one row

    # Disable snapshots
    execute_and_fetch_all(cursor, "SET DATABASE SETTING 'storage.snapshot.interval' TO '';")

    # Test SHOW SNAPSHOTS (should still return existing snapshots)
    all_snapshots_after_disable = execute_and_fetch_all(cursor, "SHOW SNAPSHOTS;")
    assert len(all_snapshots_after_disable) == n_snapshots3  # Still the same existing snapshots

    # Test SHOW NEXT SNAPSHOT (should return no rows when no next snapshot is scheduled)
    next_snapshot_after_disable = execute_and_fetch_all(cursor, "SHOW NEXT SNAPSHOT;")
    assert len(next_snapshot_after_disable) == 0  # Should return no rows

    thread.stop()
    thread.join()


def main_test_analytical(snapshots_dir, set):
    # 1 (optional) set interval to 1s
    # 2 check number of snapshots under analytical
    # 3 set to transactional
    # 4 check number of snapshots under tranasctional
    # 5 set to analytical
    # 6 check number of snapshots under analytical

    connection = connect(host="localhost", port=7687)
    cursor = connection.cursor()

    n_snapshots1 = number_of_snapshots(snapshots_dir)

    # 1
    if set:
        execute_and_fetch_all(cursor, "SET DATABASE SETTING 'storage.snapshot.interval' TO '1';")

    # 2
    time.sleep(2)
    assert number_of_snapshots(snapshots_dir) == n_snapshots1, "Got new snapshots even though in analytical"

    # 3
    execute_and_fetch_all(cursor, "STORAGE MODE IN_MEMORY_TRANSACTIONAL;")

    thread = StoppableThread()
    thread.start()

    # 4
    time.sleep(2)
    assert number_of_snapshots(snapshots_dir) > n_snapshots1, "Didn't get new snapshots even though in transactional"

    # 5
    execute_and_fetch_all(cursor, "STORAGE MODE IN_MEMORY_ANALYTICAL;")

    # 6
    n_snapshots2 = number_of_snapshots(snapshots_dir)
    time.sleep(2)
    assert number_of_snapshots(snapshots_dir) == n_snapshots2, "Got new snapshots even though in analytical"

    thread.stop()
    thread.join()


def test_no_flags():
    data_directory = tempfile.TemporaryDirectory()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "no_flags")
    main_test(data_directory.name + "/snapshots")
    interactive_mg_runner.kill_all()


def test_sec_flag():
    data_directory = tempfile.TemporaryDirectory()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "sec_flag")
    main_test(data_directory.name + "/snapshots")
    interactive_mg_runner.kill_all()


def test_interval_flag():
    data_directory = tempfile.TemporaryDirectory()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "interval_flag")
    main_test(data_directory.name + "/snapshots")
    interactive_mg_runner.kill_all()


def test_no_flags_analytical():
    data_directory = tempfile.TemporaryDirectory()
    interactive_mg_runner.start(memgraph_instances(data_directory.name, "IN_MEMORY_ANALYTICAL"), "no_flags")
    main_test_analytical(data_directory.name + "/snapshots", True)
    interactive_mg_runner.kill_all()


@pytest.mark.parametrize("set", [True, False])
def test_sec_flag_analytical(set):
    data_directory = tempfile.TemporaryDirectory()
    interactive_mg_runner.start(memgraph_instances(data_directory.name, "IN_MEMORY_ANALYTICAL"), "sec_flag")
    main_test_analytical(data_directory.name + "/snapshots", set)
    interactive_mg_runner.kill_all()


@pytest.mark.parametrize("set", [True, False])
def test_interval_flag_analytical(set):
    data_directory = tempfile.TemporaryDirectory()
    interactive_mg_runner.start(memgraph_instances(data_directory.name, "IN_MEMORY_ANALYTICAL"), "interval_flag")
    main_test_analytical(data_directory.name + "/snapshots", set)
    interactive_mg_runner.kill_all()


# Interface doesn't support failure, so can't reliably test if both flags cause a fault
# def test_both_flags():
#     data_directory = tempfile.TemporaryDirectory()
#     interactive_mg_runner.start(memgraph_instances(data_directory.name), "both_flags")
#     main_test(data_directory.name + "/snapshots")
#     interactive_mg_runner.kill_all()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
