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


def test_storage_info_reports_ready_then_defunct(test_name):
    """SHOW STORAGE INFO reports status=ready for a healthy database and status=defunct
    after recovery fails."""
    data_directory = get_data_path("recovery_failure_status", test_name)
    full_data_directory = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", data_directory)
    shutil.rmtree(full_data_directory, ignore_errors=True)

    instances = {
        "default": {
            # WAL disabled so the snapshot is the only durability: corrupting it makes
            # recovery fail at the "no usable snapshot" path (covered by this slice).
            "args": ["--log-level=TRACE", "--data-recovery-on-startup=true", "--storage-wal-enabled=false"],
            "log_file": "recovery_failure_status.log",
            "data_directory": data_directory,
        }
    }

    # Healthy instance: status must be "ready".
    interactive_mg_runner.start(instances, "default")
    cursor = connect(host="localhost", port=7687).cursor()
    execute_and_fetch_all(cursor, "UNWIND range(1, 5000) AS i CREATE (:Node {id: i})")
    execute_and_fetch_all(cursor, "CREATE SNAPSHOT")
    assert storage_info(cursor)["status"] == "ready"
    interactive_mg_runner.kill_all()

    # Corrupt the snapshot and restart with the recovery-failure flag.
    corrupt_snapshots(full_data_directory)
    instances["default"]["args"].append("--storage-allow-recovery-failure=true")
    interactive_mg_runner.start(instances, "default")

    cursor = connect(host="localhost", port=7687).cursor()
    assert storage_info(cursor)["status"] == "broken"
    interactive_mg_runner.stop_all()


def test_show_databases_reports_status(test_name):
    """SHOW DATABASES shows a Status column: defunct for a tenant whose
    recovery failed, ready for a healthy tenant."""
    data_directory = get_data_path("recovery_failure_status", test_name)
    full_data_directory = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", data_directory)
    shutil.rmtree(full_data_directory, ignore_errors=True)

    instances = {
        "default": {
            "args": ["--log-level=TRACE", "--data-recovery-on-startup=true", "--storage-wal-enabled=false"],
            "log_file": "recovery_failure_status_mt.log",
            "data_directory": data_directory,
        }
    }

    interactive_mg_runner.start(instances, "default")
    connection = connect(host="localhost", port=7687)
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "CREATE DATABASE broken_db")

    # Populate and snapshot the tenant so it has durability to corrupt.
    execute_and_fetch_all(cursor, "USE DATABASE broken_db")
    execute_and_fetch_all(cursor, "UNWIND range(1, 5000) AS i CREATE (:Node {id: i})")
    execute_and_fetch_all(cursor, "CREATE SNAPSHOT")
    interactive_mg_runner.kill_all()

    # Corrupt the tenant's snapshot (each tenant lives under databases/<uuid>/).
    databases_dir = os.path.join(full_data_directory, "databases")
    tenant_dirs = [
        os.path.join(databases_dir, d)
        for d in os.listdir(databases_dir)
        if d not in ("memgraph", ".durability")
        and not d.startswith(".")
        and os.path.isdir(os.path.join(databases_dir, d))
    ]
    assert tenant_dirs, "Expected a tenant directory to corrupt"
    corrupt_snapshots(tenant_dirs[0])

    instances["default"]["args"].append("--storage-allow-recovery-failure=true")
    interactive_mg_runner.start(instances, "default")

    cursor = connect(host="localhost", port=7687).cursor()
    rows = {row[0]: row[1] for row in execute_and_fetch_all(cursor, "SHOW DATABASES")}
    assert rows.get("broken_db") == "broken"
    assert rows.get("memgraph") == "ready"
    interactive_mg_runner.stop_all()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
