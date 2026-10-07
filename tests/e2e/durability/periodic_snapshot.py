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
import threading
import time
from contextlib import contextmanager
from typing import Any, Dict

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all, get_data_path, get_logs_path
from mg_utils import mg_sleep_and_assert_eval_function

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

FILE = "periodic_snapshot"


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


def snapshots_path(file, test_name):
    return os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", get_data_path(file, test_name), "snapshots")


def memgraph_instances(test_name, mode="IN_MEMORY_TRANSACTIONAL", file=FILE):
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
            "log_file": f"{get_logs_path(file, test_name)}/no_flags.log",
            "data_directory": get_data_path(file, test_name),
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
            "log_file": f"{get_logs_path(file, test_name)}/sec_flag.log",
            "data_directory": get_data_path(file, test_name),
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
            "log_file": f"{get_logs_path(file, test_name)}/interval_flag.log",
            "data_directory": get_data_path(file, test_name),
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
            "log_file": f"{get_logs_path(file, test_name)}/both_flags.log",
            "data_directory": get_data_path(file, test_name),
        },
    }


def snapshot_paths(cursor):
    return {snapshot[0] for snapshot in execute_and_fetch_all(cursor, "SHOW SNAPSHOTS;")}


# Need to constantly make changes to the database to trigger snapshots
class StoppableThread(threading.Thread):
    def __init__(self):
        # A daemon, so that a writer somehow left running cannot by itself keep
        # the interpreter from exiting. Stopping it remains the caller's job;
        # this only bounds what a missed stop costs.
        super().__init__(daemon=True)
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def run(self):
        connection = connect(host="localhost", port=7687)
        cursor = connection.cursor()
        while not self._stop_event.is_set():
            cursor.execute("CREATE ()")
            time.sleep(0.25)


@contextmanager
def writing_in_the_background():
    """Keep the database changing and stop the writer even if an assertion fails."""
    thread = StoppableThread()
    thread.start()
    try:
        yield thread
    finally:
        thread.stop()
        thread.join(timeout=30)


def main_test(snapshots_dir):
    connection = connect(host="localhost", port=7687)
    cursor = connection.cursor()

    execute_and_fetch_all(cursor, "SET DATABASE SETTING 'storage.snapshot.interval' TO '';")
    assert execute_and_fetch_all(cursor, "SHOW NEXT SNAPSHOT;") == []

    with writing_in_the_background():
        for interval in ("*/1 * * * * *", "5"):
            initial_paths = snapshot_paths(cursor)
            execute_and_fetch_all(cursor, f"SET DATABASE SETTING 'storage.snapshot.interval' TO '{interval}';")
            assert len(execute_and_fetch_all(cursor, "SHOW NEXT SNAPSHOT;")) == 1
            mg_sleep_and_assert_eval_function(
                lambda paths: len(paths - initial_paths) >= 2,
                lambda: snapshot_paths(cursor),
                max_duration=60,
            )

    execute_and_fetch_all(cursor, "SET DATABASE SETTING 'storage.snapshot.interval' TO '';")
    assert execute_and_fetch_all(cursor, "SHOW NEXT SNAPSHOT;") == []

    def snapshots_match_files():
        listed_paths = snapshot_paths(cursor)
        files = {
            os.path.join(snapshots_dir, entry)
            for entry in os.listdir(snapshots_dir)
            if os.path.isfile(os.path.join(snapshots_dir, entry))
        }
        return bool(listed_paths) and listed_paths == files

    mg_sleep_and_assert_eval_function(bool, snapshots_match_files, max_duration=60)
    cursor.close()
    connection.close()


def main_test_analytical(snapshots_dir, set):
    # 1 (optional) set interval to 1s
    # 2 check number of snapshots under analytical
    # 3 set to transactional
    # 4 check new snapshots under transactional
    # 5 set to analytical
    # 6 check number of snapshots under analytical

    connection = connect(host="localhost", port=7687)
    cursor = connection.cursor()

    initial_paths = snapshot_paths(cursor)

    # 1
    if set:
        execute_and_fetch_all(cursor, "SET DATABASE SETTING 'storage.snapshot.interval' TO '1';")

    # 2
    time.sleep(2)
    assert snapshot_paths(cursor) == initial_paths, "Got new snapshots even though in analytical"

    # 3
    execute_and_fetch_all(cursor, "STORAGE MODE IN_MEMORY_TRANSACTIONAL;")
    # Switching modes creates a snapshot; wait for another one from the scheduler.
    initial_paths = snapshot_paths(cursor)

    with writing_in_the_background():
        # 4
        mg_sleep_and_assert_eval_function(
            lambda paths: bool(paths - initial_paths),
            lambda: snapshot_paths(cursor),
            max_duration=60,
        )

        # 5
        execute_and_fetch_all(cursor, "STORAGE MODE IN_MEMORY_ANALYTICAL;")

        # 6
        assert execute_and_fetch_all(cursor, "SHOW NEXT SNAPSHOT;") == []
        initial_paths = snapshot_paths(cursor)
        time.sleep(2)
        assert snapshot_paths(cursor) == initial_paths, "Got new snapshots even though in analytical"


def test_a_failed_assertion_leaves_no_writer_running(test_name):
    """An assertion failure must not leave the background writer running."""
    interactive_mg_runner.start(memgraph_instances(test_name), "no_flags")
    try:
        writing = False
        with pytest.raises(AssertionError):
            with writing_in_the_background() as writer:
                writing = writer.is_alive()
                assert False, "as a missed tick would"

        assert writing, "the writer never ran, so this asks nothing"
        assert not writer.is_alive(), "the writer outlived the failure and would hold the interpreter open"
    finally:
        interactive_mg_runner.kill_all(keep_directories=False)


def test_no_flags(test_name):
    interactive_mg_runner.start(memgraph_instances(test_name), "no_flags")
    main_test(snapshots_path(FILE, test_name))
    interactive_mg_runner.kill_all(keep_directories=False)


def test_sec_flag(test_name):
    interactive_mg_runner.start(memgraph_instances(test_name), "sec_flag")
    main_test(snapshots_path(FILE, test_name))
    interactive_mg_runner.kill_all(keep_directories=False)


def test_sec_flag_with_retention(test_name):
    instances = memgraph_instances(test_name)
    args = instances["sec_flag"]["args"]
    args[args.index("--storage-snapshot-retention-count=20")] = "--storage-snapshot-retention-count=2"
    interactive_mg_runner.start(instances, "sec_flag")
    main_test(snapshots_path(FILE, test_name))
    interactive_mg_runner.kill_all(keep_directories=False)


def test_interval_flag(test_name):
    interactive_mg_runner.start(memgraph_instances(test_name), "interval_flag")
    main_test(snapshots_path(FILE, test_name))
    interactive_mg_runner.kill_all(keep_directories=False)


def test_no_flags_analytical(test_name):
    interactive_mg_runner.start(memgraph_instances(test_name, "IN_MEMORY_ANALYTICAL"), "no_flags")
    main_test_analytical(snapshots_path(FILE, test_name), True)
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.mark.parametrize("set", [True, False])
def test_sec_flag_analytical(set, test_name):
    interactive_mg_runner.start(memgraph_instances(test_name, "IN_MEMORY_ANALYTICAL"), "sec_flag")
    main_test_analytical(snapshots_path(FILE, test_name), set)
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.mark.parametrize("set", [True, False])
def test_interval_flag_analytical(set, test_name):
    interactive_mg_runner.start(memgraph_instances(test_name, "IN_MEMORY_ANALYTICAL"), "interval_flag")
    main_test_analytical(snapshots_path(FILE, test_name), set)
    interactive_mg_runner.kill_all(keep_directories=False)


# Interface doesn't support failure, so can't reliably test if both flags cause a fault
# def test_both_flags(test_name):
#     interactive_mg_runner.start(memgraph_instances(test_name), "both_flags")
#     main_test(snapshots_path(FILE, test_name))
#     interactive_mg_runner.kill_all(keep_directories=False)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
