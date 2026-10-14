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

"""
Snapshot recovery of light edges that hits the global memory limit must fail cleanly. With
--storage-allow-recovery-failure the database comes up broken and the instance keeps running.
The limit is swept so the refused allocation lands at different points of edge recovery.
"""

import os
import sys

import interactive_mg_runner
import mgclient
import pytest
from common import connect, execute_and_fetch_all, get_data_path, get_logs_path

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

FILE = "light_edge_recovery_memory_limit"
NUM_VERTICES = 100000
NUM_EDGES = 2 * NUM_VERTICES - 3
MEMORY_LIMITS_MB = range(1, 49)

COMMON_ARGS = [
    "--log-level=WARNING",
    "--also-log-to-stderr",
    "--storage-properties-on-edges=true",
    "--storage-light-edge=true",
    "--storage-wal-enabled=false",
    "--storage-snapshot-interval-sec=0",
    "--storage-snapshot-on-exit=false",
]


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


def instance(test_name, extra_args):
    return {
        "main": {
            "args": COMMON_ARGS + extra_args,
            "log_file": f"{get_logs_path(FILE, test_name)}/main.log",
            "data_directory": get_data_path(FILE, test_name),
        }
    }


def test_light_edge_recovery_survives_memory_limit(test_name):
    interactive_mg_runner.start(instance(test_name, ["--data-recovery-on-startup=false"]), "main")
    cursor = connect(host="localhost", port=7687).cursor()
    execute_and_fetch_all(cursor, "CREATE INDEX ON :P(id)")
    execute_and_fetch_all(cursor, f"UNWIND range(1, {NUM_VERTICES}) AS i CREATE (:P {{id: i}})")
    execute_and_fetch_all(cursor, "MATCH (a:P), (b:P) WHERE b.id = a.id + 1 CREATE (a)-[:E]->(b)")
    execute_and_fetch_all(cursor, "MATCH (a:P), (b:P) WHERE b.id = a.id + 2 CREATE (a)-[:F]->(b)")
    execute_and_fetch_all(cursor, "CREATE SNAPSHOT")
    interactive_mg_runner.kill_all()

    for limit_mb in MEMORY_LIMITS_MB:
        limited = instance(
            test_name,
            [
                "--data-recovery-on-startup=true",
                "--storage-allow-recovery-failure=true",
                f"--memory-limit={limit_mb}",
            ],
        )
        interactive_mg_runner.start(limited, "main")
        try:
            execute_and_fetch_all(connect(host="localhost", port=7687).cursor(), "RETURN 1")
        except mgclient.DatabaseError as e:
            assert "Memory limit exceeded" in str(e), f"limit {limit_mb} MiB: unexpected error: {e}"
        assert interactive_mg_runner.MEMGRAPH_INSTANCES["main"].is_running(), f"limit {limit_mb} MiB"
        interactive_mg_runner.kill_all()

    interactive_mg_runner.start(instance(test_name, ["--data-recovery-on-startup=true"]), "main")
    cursor = connect(host="localhost", port=7687).cursor()
    assert execute_and_fetch_all(cursor, "MATCH ()-[e]->() RETURN count(e)") == [(NUM_EDGES,)]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
