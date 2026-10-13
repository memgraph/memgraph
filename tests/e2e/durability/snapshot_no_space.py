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

# CREATE SNAPSHOT on a full disk fails with an error instead of crashing the instance, and leaves no partial
# snapshot or snapshot part behind. The full disk is simulated by tests/e2e/no_space_preload.so, which fails
# the chosen calls on snapshot files with ENOSPC while its trigger file exists.

import glob
import os
import shutil
import sys

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all, get_data_path, get_logs_path

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

FILE = "snapshot_no_space"
PRELOAD_LIBRARY = os.path.normpath(os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "no_space_preload.so"))
ALL_CALLS = "write,sendfile,fsync,sync_file_range"
# Small enough that every snapshot below fills it many times over, so writeback pacing runs.
WRITEBACK_WINDOW_MIB = 1

# (id, parallel, calls that fail, the operation main logs when it runs out of space)
CASES = [
    ("single_threaded", False, ALL_CALLS, "write to"),
    ("parallel", True, ALL_CALLS, "write to"),
    # Parts are written normally; appending them to the snapshot is what runs out of space.
    ("parallel_append", True, "sendfile", "append to"),
    # Every write succeeds and the disk fills only once the written data is handed to writeback.
    ("single_threaded_writeback", False, "sync_file_range", "wait for writeback of"),
    ("parallel_writeback", True, "sync_file_range", "wait for writeback of"),
]


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


def instance_description(test_name, parallel):
    return {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=false",
                "--storage-snapshot-interval-sec=3600",
                f"--storage-parallel-snapshot-creation={str(parallel).lower()}",
                "--storage-snapshot-thread-count=4",
                "--storage-items-per-batch=100",
                f"--storage-snapshot-writeback-window-mib={WRITEBACK_WINDOW_MIB}",
            ],
            "log_file": f"{get_logs_path(FILE, test_name)}/main.log",
            "data_directory": get_data_path(FILE, test_name),
            "setup_queries": [],
        },
    }


def log_contains(test_name, *fragments):
    # The log is a daily file, so its name carries the date: main.log is written as main_<date>.log.
    stem = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "logs", get_logs_path(FILE, test_name), "main")
    for path in glob.glob(f"{glob.escape(stem)}*.log"):
        with open(path) as f:
            if any(all(fragment in line for fragment in fragments) for line in f):
                return True
    return False


def durability_files(data_directory):
    return [
        name
        for directory in ("snapshots", "wal")
        for name in os.listdir(os.path.join(data_directory, directory))
        if os.path.isfile(os.path.join(data_directory, directory, name))
    ]


@pytest.mark.parametrize(
    "parallel, failing_calls, failed_operation", [case[1:] for case in CASES], ids=[case[0] for case in CASES]
)
def test_create_snapshot_on_full_disk_fails_cleanly(test_name, parallel, failing_calls, failed_operation, monkeypatch):
    assert os.path.isfile(PRELOAD_LIBRARY), f"Missing {PRELOAD_LIBRARY}"
    data_directory = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", get_data_path(FILE, test_name))
    shutil.rmtree(data_directory, ignore_errors=True)
    trigger = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", f"{FILE}_{test_name}_trigger")
    if os.path.exists(trigger):
        os.remove(trigger)

    # The instance inherits the environment, so the shim is loaded but stays inert until the trigger exists.
    monkeypatch.setenv("LD_PRELOAD", PRELOAD_LIBRARY)
    monkeypatch.setenv("MG_TEST_NO_SPACE_TRIGGER", trigger)
    monkeypatch.setenv("MG_TEST_NO_SPACE_CALLS", failing_calls)
    interactive_mg_runner.start(instance_description(test_name, parallel), "main")
    monkeypatch.delenv("LD_PRELOAD")

    cursor = connect(host="localhost", port=7687).cursor()
    # About 4 MiB of payload, several writeback windows' worth.
    payload = "x" * 200
    execute_and_fetch_all(
        cursor,
        f"UNWIND range(1, 10000) AS i "
        f"CREATE (:Node {{id: i, payload: '{payload}'}})-[:LINK]->(:Node {{id: -i, payload: '{payload}'}})",
    )
    execute_and_fetch_all(cursor, "CREATE SNAPSHOT")
    files_before = durability_files(data_directory)

    # Trigger failure of system calls
    open(trigger, "w").close()
    try:
        with pytest.raises(Exception, match="Not enough disk space to write the snapshot"):
            execute_and_fetch_all(cursor, "CREATE SNAPSHOT")
    finally:
        os.remove(trigger)

    files_after = durability_files(data_directory)
    assert set(files_after) == set(files_before), f"before: {files_before}, after: {files_after}"
    assert log_contains(test_name, f"While trying to {failed_operation}", "/snapshots/", "No space left on device")

    # The instance is still up and creates snapshots once there is space again.
    assert execute_and_fetch_all(cursor, "MATCH (n) RETURN count(n)")[0][0] == 20000
    snapshot_path = execute_and_fetch_all(cursor, "CREATE SNAPSHOT")[0][0]
    assert os.path.isfile(snapshot_path)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
