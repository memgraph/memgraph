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

import glob
import os
import shutil
import sys

import interactive_mg_runner
import pytest

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

MEMGRAPH_INSTANCES_DESCRIPTION = {
    "main": {
        "args": [
            "--bolt-port",
            "7687",
            "--log-level=TRACE",
            "--storage-snapshot-on-exit=true",
        ],
        "log_file": "show_metrics/recovery/main.log",
        "data_directory": "show_metrics/recovery/main",
        "setup_queries": [],
    }
}

# Two instances so one can recover from the other's snapshot, which carries a different database uuid.
FOREIGN_SNAPSHOT_DESCRIPTION = {
    "first": {
        "args": [
            "--bolt-port",
            "7687",
            "--log-level=TRACE",
            "--storage-snapshot-on-exit=true",
        ],
        "log_file": "show_metrics/foreign/first.log",
        "data_directory": "show_metrics/foreign/first",
        "setup_queries": [],
    },
    "second": {
        "args": [
            "--bolt-port",
            "7688",
            "--log-level=TRACE",
            "--storage-snapshot-on-exit=true",
        ],
        "log_file": "show_metrics/foreign/second.log",
        "data_directory": "show_metrics/foreign/second",
        "setup_queries": [],
    },
}


@pytest.fixture(autouse=True)
def cleanup():
    interactive_mg_runner.kill_all(keep_directories=False)
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


def get_metric_value(instance, metric_name, on_clause=None):
    query = "SHOW METRICS INFO"
    if on_clause:
        query += f" {on_clause}"
    rows = instance.query(query)
    for row in rows:
        if row[0] == metric_name:
            return row[3]
    return None


def test_index_and_constraint_gauges_correct_after_recovery():
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)
    instance = interactive_mg_runner.MEMGRAPH_INSTANCES["main"]

    instance.query("CREATE INDEX ON :Person;")
    instance.query("CREATE INDEX ON :Person(name);")
    instance.query("CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
    instance.query("CREATE CONSTRAINT ON (n:Person) ASSERT n.id IS UNIQUE;")

    interactive_mg_runner.stop(MEMGRAPH_INSTANCES_DESCRIPTION, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "main")
    instance = interactive_mg_runner.MEMGRAPH_INSTANCES["main"]

    assert get_metric_value(instance, "ActiveLabelIndices") == 1
    assert get_metric_value(instance, "ActiveLabelPropertyIndices") == 1
    assert get_metric_value(instance, "ActiveExistenceConstraints") == 1
    assert get_metric_value(instance, "ActiveUniqueConstraints") == 1
    assert get_metric_value(instance, "SnapshotRecoveryLatency_us_50p") > 0


def database_uuid(instance):
    for row in instance.query("SHOW STORAGE INFO ON CURRENT DATABASE"):
        if row[0] == "database_uuid":
            return row[1]
    return None


def data_dir(name):
    return os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", "show_metrics", "foreign", name)


def test_metrics_available_after_recovering_a_foreign_snapshot():
    """A snapshot carries the uuid of the database that wrote it, and recovery adopts it. Metrics are
    registered before recovery runs, so they must follow that uuid or SHOW METRICS INFO cannot find the
    database. Recovering a data directory holding a snapshot written elsewhere, such as a restored
    backup, is the route into this state."""
    for name in FOREIGN_SNAPSHOT_DESCRIPTION:
        shutil.rmtree(data_dir(name), ignore_errors=True)

    interactive_mg_runner.start_all(FOREIGN_SNAPSHOT_DESCRIPTION)
    first = interactive_mg_runner.MEMGRAPH_INSTANCES["first"]
    second = interactive_mg_runner.MEMGRAPH_INSTANCES["second"]

    first.query("CREATE (:Node);")
    second.query("CREATE (:Node);")
    second.query("CREATE (:Node);")
    foreign_uuid = database_uuid(second)
    assert database_uuid(first) != foreign_uuid

    interactive_mg_runner.stop(FOREIGN_SNAPSHOT_DESCRIPTION, "first")
    interactive_mg_runner.stop(FOREIGN_SNAPSHOT_DESCRIPTION, "second")

    for stale in glob.glob(os.path.join(data_dir("first"), "snapshots", "*")) + glob.glob(
        os.path.join(data_dir("first"), "wal", "*")
    ):
        os.remove(stale)
    for snapshot in glob.glob(os.path.join(data_dir("second"), "snapshots", "*")):
        shutil.copy(snapshot, os.path.join(data_dir("first"), "snapshots"))

    interactive_mg_runner.start(FOREIGN_SNAPSHOT_DESCRIPTION, "first")
    first = interactive_mg_runner.MEMGRAPH_INSTANCES["first"]

    assert database_uuid(first) == foreign_uuid
    assert get_metric_value(first, "VertexCount") == 2


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
