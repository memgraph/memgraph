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

FILE = "durability_with_vertex_property_index"


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


def _get_vertex_property_indices(cursor):
    rows = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    return [row for row in rows if row[0] == "vertex-property"]


def test_index_survives_wal_recovery(test_name):
    """Create index + data, kill, restart with WAL recovery, verify index and data."""
    data_directory = get_data_path(FILE, test_name)
    desc = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
            ],
            "log_file": "wal_recovery.log",
            "data_directory": data_directory,
        },
    }
    interactive_mg_runner.start(desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    execute_and_fetch_all(cursor, "CREATE GLOBAL INDEX ON :(prop);")
    execute_and_fetch_all(cursor, "CREATE (:A {prop: 1});")
    execute_and_fetch_all(cursor, "CREATE (:B {prop: 2});")
    execute_and_fetch_all(cursor, "CREATE (:C {prop: 3});")

    indices = _get_vertex_property_indices(cursor)
    assert len(indices) == 1
    assert indices[0][2] == "prop"

    interactive_mg_runner.kill(desc, "main")
    interactive_mg_runner.start(desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    indices = _get_vertex_property_indices(cursor)
    assert len(indices) == 1, f"Expected 1 vertex-property index after WAL recovery, got {len(indices)}"
    assert indices[0][2] == "prop"

    result = execute_and_fetch_all(cursor, "MATCH (n) WHERE n.prop IS NOT NULL RETURN n.prop ORDER BY n.prop;")
    assert [r[0] for r in result] == [1, 2, 3]


def test_index_survives_snapshot_recovery(test_name):
    """Create index + data, snapshot, kill, restart with snapshot recovery, verify."""
    data_directory = get_data_path(FILE, test_name)
    desc = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
            ],
            "storage_snapshot_on_exit": True,
            "log_file": "snapshot_recovery.log",
            "data_directory": data_directory,
        },
    }
    interactive_mg_runner.start(desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    execute_and_fetch_all(cursor, "CREATE GLOBAL INDEX ON :(val);")
    execute_and_fetch_all(cursor, "CREATE (:X {val: 10});")
    execute_and_fetch_all(cursor, "CREATE (:Y {val: 20});")
    execute_and_fetch_all(cursor, "CREATE (:Z {val: 30});")

    # Graceful shutdown triggers snapshot-on-exit
    interactive_mg_runner.stop(desc, "main")
    interactive_mg_runner.start(desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    indices = _get_vertex_property_indices(cursor)
    assert len(indices) == 1, f"Expected 1 vertex-property index after snapshot recovery, got {len(indices)}"
    assert indices[0][2] == "val"

    result = execute_and_fetch_all(cursor, "MATCH (n) WHERE n.val IS NOT NULL RETURN n.val ORDER BY n.val;")
    assert [r[0] for r in result] == [10, 20, 30]


def test_drop_index_survives_wal_recovery(test_name):
    """Create then drop index, kill, restart, verify index is gone."""
    data_directory = get_data_path(FILE, test_name)
    desc = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
            ],
            "log_file": "drop_wal_recovery.log",
            "data_directory": data_directory,
        },
    }
    interactive_mg_runner.start(desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    execute_and_fetch_all(cursor, "CREATE GLOBAL INDEX ON :(prop);")
    execute_and_fetch_all(cursor, "CREATE (:A {prop: 1});")
    execute_and_fetch_all(cursor, "DROP GLOBAL INDEX ON :(prop);")

    assert len(_get_vertex_property_indices(cursor)) == 0

    interactive_mg_runner.kill(desc, "main")
    interactive_mg_runner.start(desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    assert len(_get_vertex_property_indices(cursor)) == 0, "Dropped index should not reappear after WAL recovery"


def test_index_with_data_added_after_snapshot(test_name):
    """Create index, snapshot, add more data, kill, restart, verify all data via WAL replay on top of snapshot."""
    data_directory = get_data_path(FILE, test_name)
    desc = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
            ],
            "log_file": "data_after_snapshot.log",
            "data_directory": data_directory,
        },
    }
    interactive_mg_runner.start(desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    execute_and_fetch_all(cursor, "CREATE GLOBAL INDEX ON :(id);")
    execute_and_fetch_all(cursor, "CREATE (:A {id: 1});")
    execute_and_fetch_all(cursor, "CREATE (:B {id: 2});")
    execute_and_fetch_all(cursor, "CREATE SNAPSHOT;")

    # Data added after snapshot — will be in WAL only
    execute_and_fetch_all(cursor, "CREATE (:C {id: 3});")
    execute_and_fetch_all(cursor, "CREATE (:D {id: 4});")

    interactive_mg_runner.kill(desc, "main")
    interactive_mg_runner.start(desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    indices = _get_vertex_property_indices(cursor)
    assert len(indices) == 1

    result = execute_and_fetch_all(cursor, "MATCH (n) WHERE n.id IS NOT NULL RETURN n.id ORDER BY n.id;")
    assert [r[0] for r in result] == [1, 2, 3, 4], "All data (snapshot + WAL) should be recovered"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
