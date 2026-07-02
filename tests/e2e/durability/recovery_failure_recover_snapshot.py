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

import os
import shutil
import sys

import interactive_mg_runner
import pytest
from common import connect, corrupt_snapshots, execute_and_fetch_all, get_data_path

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))


@pytest.fixture
def test_name(request):
    return request.node.name


@pytest.fixture(autouse=True)
def cleanup_instances():
    interactive_mg_runner.kill_all()
    yield
    interactive_mg_runner.kill_all()


def storage_info(cursor):
    rows = execute_and_fetch_all(cursor, "SHOW STORAGE INFO ON CURRENT DATABASE")
    return {row[0]: row[1] for row in rows}


def test_recover_snapshot_cures_broken(test_name):
    """A broken default database is cured in place by RECOVER SNAPSHOT: after loading a
    known-good snapshot the data comes back, status flips to ready, and the tenant recovers
    healthy across a restart (it does not re-enter broken)."""
    data_directory = get_data_path("recovery_failure_recover_snapshot", test_name)
    full_data_directory = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", data_directory)
    shutil.rmtree(full_data_directory, ignore_errors=True)

    instances = {
        "default": {
            # WAL disabled so the snapshot is the only durability: corrupting it makes
            # recovery fail deterministically (no WAL fallback that would recover healthy).
            "args": ["--log-level=TRACE", "--data-recovery-on-startup=true", "--storage-wal-enabled=false"],
            "log_file": "recovery_failure_recover_snapshot.log",
            "data_directory": data_directory,
        }
    }

    # Healthy instance: create data and a snapshot.
    interactive_mg_runner.start(instances, "default")
    cursor = connect(host="localhost", port=7687).cursor()
    execute_and_fetch_all(cursor, "UNWIND range(1, 5000) AS i CREATE (:Node {id: i})")
    execute_and_fetch_all(cursor, "CREATE SNAPSHOT")
    assert storage_info(cursor)["status"] == "ready"
    interactive_mg_runner.kill_all()

    # Stash a known-good copy of the snapshot outside the data directory before corrupting it.
    snapshot_dir = os.path.join(full_data_directory, "snapshots")
    snapshot_files = [
        os.path.join(snapshot_dir, f) for f in os.listdir(snapshot_dir) if os.path.isfile(os.path.join(snapshot_dir, f))
    ]
    assert len(snapshot_files) == 1, f"Expected exactly one snapshot, got {snapshot_files}"
    good_snapshot_copy = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", "recover_snapshot_good_copy")
    shutil.copyfile(snapshot_files[0], good_snapshot_copy)

    # Corrupt the in-place snapshot and restart with the recovery-failure flag -> broken.
    corrupt_snapshots(full_data_directory)
    instances["default"]["args"].append("--storage-allow-recovery-failure=true")
    interactive_mg_runner.start(instances, "default")

    cursor = connect(host="localhost", port=7687).cursor()
    assert storage_info(cursor)["status"] == "broken"

    # Cure the broken database in place with the known-good snapshot copy.
    execute_and_fetch_all(cursor, f"RECOVER SNAPSHOT '{good_snapshot_copy}'")

    # The data is back and the tenant reports ready.
    count = execute_and_fetch_all(cursor, "MATCH (n:Node) RETURN count(n)")[0][0]
    assert count == 5000
    assert storage_info(cursor)["status"] == "ready"
    interactive_mg_runner.kill_all()

    # Restart: the cure left the durability directory restart-clean, so the tenant recovers
    # healthy with the data intact and does not re-enter the broken state.
    interactive_mg_runner.start(instances, "default")
    cursor = connect(host="localhost", port=7687).cursor()
    assert storage_info(cursor)["status"] == "ready"
    count = execute_and_fetch_all(cursor, "MATCH (n:Node) RETURN count(n)")[0][0]
    assert count == 5000
    interactive_mg_runner.stop_all()

    os.remove(good_snapshot_copy)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
