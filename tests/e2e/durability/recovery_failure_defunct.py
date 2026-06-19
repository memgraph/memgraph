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
import sys

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all, get_data_path

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

DEFUNCT_ERROR = "Database is in the defunct state because the recovery process failed."


@pytest.fixture
def test_name(request):
    return request.node.name


def corrupt_snapshots(full_data_directory):
    """Corrupt the data (vertex/edge/index) region of every snapshot while preserving the
    offsets block at the start and the metadata section at the end. This keeps the file
    readable by ReadSnapshotInfo (so it is selected for recovery) but makes LoadSnapshot
    fail, which is the "no usable snapshot" path that yields a defunct database."""
    snapshot_dir = os.path.join(full_data_directory, "snapshots")
    files = [
        os.path.join(snapshot_dir, f) for f in os.listdir(snapshot_dir) if os.path.isfile(os.path.join(snapshot_dir, f))
    ]
    assert files, "Expected at least one snapshot to corrupt"
    head_keep = 1024  # magic + version + SECTION_OFFSETS
    tail_keep = 4096  # SECTION_METADATA (uuid/epoch/counts) lives near the end
    for path in files:
        size = os.path.getsize(path)
        start = min(head_keep, size)
        end = max(start, size - tail_keep)
        assert end > start, f"Snapshot {path} too small ({size} bytes) to corrupt safely"
        with open(path, "r+b") as fh:
            fh.seek(start)
            fh.write(b"\xff" * (end - start))
    return files


def test_defunct_on_corrupt_snapshot(test_name):
    data_directory = get_data_path("recovery_failure_defunct", test_name)
    full_data_directory = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", data_directory)

    instances = {
        "default": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
            ],
            "log_file": "recovery_failure_defunct.log",
            "data_directory": data_directory,
        }
    }

    # 1. Start, create data, snapshot it, hard-kill.
    interactive_mg_runner.start(instances, "default")
    cursor = connect(host="localhost", port=7687).cursor()
    execute_and_fetch_all(cursor, "UNWIND range(1, 5000) AS i CREATE (:Node {id: i})")
    execute_and_fetch_all(cursor, "CREATE SNAPSHOT")
    interactive_mg_runner.kill_all()

    # 2. Corrupt the snapshot on disk.
    corrupt_snapshots(full_data_directory)

    # 3. Restart with --storage-allow-recovery-failure: the instance must come up.
    instances["default"]["args"].append("--storage-allow-recovery-failure=true")
    interactive_mg_runner.start(instances, "default")

    # 4. The default database is defunct: data queries throw the defunct error.
    cursor = connect(host="localhost", port=7687).cursor()
    with pytest.raises(Exception) as einfo:
        execute_and_fetch_all(cursor, "MATCH (n) RETURN n")
    assert DEFUNCT_ERROR in str(einfo.value)

    with pytest.raises(Exception) as einfo:
        execute_and_fetch_all(cursor, "CREATE (:Node)")
    assert DEFUNCT_ERROR in str(einfo.value)

    interactive_mg_runner.stop_all()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
