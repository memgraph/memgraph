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
# writes to snapshot files with ENOSPC while its trigger file exists.

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
            ],
            "log_file": f"{get_logs_path(FILE, test_name)}/main.log",
            "data_directory": get_data_path(FILE, test_name),
            "setup_queries": [],
        },
    }


def durability_files(data_directory):
    return [
        name
        for directory in ("snapshots", "wal")
        for name in os.listdir(os.path.join(data_directory, directory))
        if os.path.isfile(os.path.join(data_directory, directory, name))
    ]


@pytest.mark.parametrize("parallel", [False, True], ids=["single_threaded", "parallel"])
def test_create_snapshot_on_full_disk_fails_cleanly(test_name, parallel, monkeypatch):
    assert os.path.isfile(PRELOAD_LIBRARY), f"Missing {PRELOAD_LIBRARY}"
    data_directory = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", get_data_path(FILE, test_name))
    shutil.rmtree(data_directory, ignore_errors=True)
    trigger = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", f"{FILE}_{test_name}_trigger")
    if os.path.exists(trigger):
        os.remove(trigger)

    # The instance inherits the environment, so the shim is loaded but stays inert until the trigger exists.
    monkeypatch.setenv("LD_PRELOAD", PRELOAD_LIBRARY)
    monkeypatch.setenv("MG_TEST_NO_SPACE_TRIGGER", trigger)
    interactive_mg_runner.start(instance_description(test_name, parallel), "main")
    monkeypatch.delenv("LD_PRELOAD")

    cursor = connect(host="localhost", port=7687).cursor()
    execute_and_fetch_all(cursor, "UNWIND range(1, 2000) AS i CREATE (:Node {id: i})-[:LINK]->(:Node {id: -i})")
    execute_and_fetch_all(cursor, "CREATE SNAPSHOT")
    files_before = durability_files(data_directory)

    open(trigger, "w").close()
    try:
        with pytest.raises(Exception, match="Not enough disk space to write the snapshot"):
            execute_and_fetch_all(cursor, "CREATE SNAPSHOT")
    finally:
        os.remove(trigger)

    files_after = durability_files(data_directory)
    assert len(files_after) <= len(files_before), f"before: {files_before}, after: {files_after}"
    assert not [name for name in files_after if "_part_" in name], files_after

    # The instance is still up and creates snapshots once there is space again.
    assert execute_and_fetch_all(cursor, "MATCH (n) RETURN count(n)")[0][0] == 4000
    snapshot_path = execute_and_fetch_all(cursor, "CREATE SNAPSHOT")[0][0]
    assert os.path.isfile(snapshot_path)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
