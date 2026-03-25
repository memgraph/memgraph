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

    assert get_metric_value(instance, "ActiveLabelIndices", "ON CURRENT") == 1
    assert get_metric_value(instance, "ActiveLabelPropertyIndices", "ON CURRENT") == 1
    assert get_metric_value(instance, "ActiveExistenceConstraints", "ON CURRENT") == 1
    assert get_metric_value(instance, "ActiveUniqueConstraints", "ON CURRENT") == 1
    assert get_metric_value(instance, "SnapshotRecoveryLatency_us_50p") > 0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
