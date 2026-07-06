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
import neo4j
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


def reset_notifications():
    """Run RESET DATABASE through the neo4j driver (mgclient does not expose Bolt
    notifications) and return the notifications carried in the query summary."""
    driver = neo4j.GraphDatabase.driver("bolt://localhost:7687", auth=("", ""))
    try:
        with driver.session() as session:
            result = session.run("RESET DATABASE")
            return list(result.consume().notifications or [])
    finally:
        driver.close()


<<<<<<< HEAD
def test_reset_database_cures_defunct(test_name):
=======
def test_reset_database_cures_broken(test_name):
>>>>>>> c34b072a5 (refactor: Repair database -> reset database)
    """A broken default database is cured in place by RESET DATABASE: it resets to an
    empty working state, emits the reset notification, accepts import queries, and recovers
    healthy across a restart (it does not re-enter broken)."""
    data_directory = get_data_path("recovery_failure_reset_database", test_name)
    full_data_directory = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", data_directory)
    shutil.rmtree(full_data_directory, ignore_errors=True)

    instances = {
        "default": {
            # WAL disabled so the snapshot is the only durability: corrupting it makes
            # recovery fail deterministically (no WAL fallback that would recover healthy).
            "args": ["--log-level=TRACE", "--data-recovery-on-startup=true", "--storage-wal-enabled=false"],
            "log_file": "recovery_failure_reset_database.log",
            "data_directory": data_directory,
        }
    }

    # Healthy instance: create data and a snapshot.
    interactive_mg_runner.start(instances, "default")
    cursor = connect(host="localhost", port=7687).cursor()
    execute_and_fetch_all(cursor, "UNWIND range(1, 5000) AS i CREATE (:Node {id: i})")
    execute_and_fetch_all(cursor, "CREATE SNAPSHOT")
    assert storage_info(cursor)["health"] == "ready"
    interactive_mg_runner.kill_all()

    # Corrupt the in-place snapshot and restart with the recovery-failure flag -> broken.
    corrupt_snapshots(full_data_directory)
    instances["default"]["args"].append("--storage-allow-recovery-failure=true")
    interactive_mg_runner.start(instances, "default")

    cursor = connect(host="localhost", port=7687).cursor()
    assert storage_info(cursor)["health"] == "broken"

    # Cure the broken database in place with RESET DATABASE and assert the success
    # notification is present in the query summary.
    notifications = reset_notifications()
    titles = [n.get("title", "") for n in notifications]
<<<<<<< HEAD
    assert any("resetted" in t for t in titles), f"Expected a reset notification, got {notifications}"
=======
    assert any("reset" in t for t in titles), f"Expected a reset notification, got {notifications}"
>>>>>>> c34b072a5 (refactor: Repair database -> reset database)

    # The tenant is now an empty, ready database that accepts import queries.
    cursor = connect(host="localhost", port=7687).cursor()
    assert storage_info(cursor)["health"] == "ready"
    assert execute_and_fetch_all(cursor, "MATCH (n) RETURN count(n)")[0][0] == 0
    execute_and_fetch_all(cursor, "UNWIND range(1, 1234) AS i CREATE (:Imported {id: i})")
    assert execute_and_fetch_all(cursor, "MATCH (n:Imported) RETURN count(n)")[0][0] == 1234
    execute_and_fetch_all(cursor, "CREATE SNAPSHOT")
    interactive_mg_runner.kill_all()

    # Restart: the reset left the durability directory restart-clean, so the tenant recovers
    # healthy with the imported data and does not re-enter the broken state.
    interactive_mg_runner.start(instances, "default")
    cursor = connect(host="localhost", port=7687).cursor()
    assert storage_info(cursor)["health"] == "ready"
    assert execute_and_fetch_all(cursor, "MATCH (n:Imported) RETURN count(n)")[0][0] == 1234
    interactive_mg_runner.stop_all()


def test_reset_database_rejected_on_healthy(test_name):
    """RESET DATABASE is rejected on a healthy (non-broken) database to prevent
    accidental data loss."""
    data_directory = get_data_path("recovery_failure_reset_database", test_name)
    full_data_directory = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", data_directory)
    shutil.rmtree(full_data_directory, ignore_errors=True)

    instances = {
        "default": {
            "args": ["--log-level=TRACE", "--data-recovery-on-startup=true", "--storage-wal-enabled=false"],
            "log_file": "recovery_failure_reset_database_healthy.log",
            "data_directory": data_directory,
        }
    }

    interactive_mg_runner.start(instances, "default")
    cursor = connect(host="localhost", port=7687).cursor()
    execute_and_fetch_all(cursor, "UNWIND range(1, 100) AS i CREATE (:Node {id: i})")
    assert storage_info(cursor)["health"] == "ready"

    with pytest.raises(Exception) as exc_info:
        execute_and_fetch_all(cursor, "RESET DATABASE")
    assert "broken" in str(exc_info.value)

    # The data is untouched.
    assert execute_and_fetch_all(cursor, "MATCH (n:Node) RETURN count(n)")[0][0] == 100
    interactive_mg_runner.stop_all()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
