# Copyright 2024 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

# Light-edge variant of snapshot_recovery.py. Drives a snapshot+WAL save then a
# recover-on-startup cycle for a light-edge storage instance and asserts the
# topology (vertices, edges, edge properties) survives the round-trip. Light
# edges require --storage-properties-on-edges=true. The flag is set explicitly.

import os
import sys
import tempfile

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

# Flags that turn on light edges. Light edges require properties-on-edges.
LIGHT_EDGE_FLAGS = [
    "--storage-properties-on-edges=true",
    "--storage-light-edge",
]


def memgraph_instances(dir):
    return {
        "default": {
            "args": [
                "--log-level=TRACE",
                "--also-log-to-stderr",
                "--data-recovery-on-startup=false",
                "--storage-snapshot-interval-sec=1000",
                "--storage-wal-enabled=true",
                "--storage-snapshot-on-exit=true",
            ]
            + LIGHT_EDGE_FLAGS,
            "log_file": "snapshot_recovery_light_edge_default.log",
            "data_directory": dir,
        },
        "recover_on_startup": {
            "args": [
                "--log-level=TRACE",
                "--also-log-to-stderr",
                "--data-recovery-on-startup=true",
                "--storage-snapshot-interval-sec=1000",
                "--storage-wal-enabled=true",
                "--storage-snapshot-on-exit=false",
            ]
            + LIGHT_EDGE_FLAGS,
            "log_file": "snapshot_recovery_light_edge_recover.log",
            "data_directory": dir,
        },
    }


def create_dataset(cursor):
    # A small connected graph with edge properties, self-loops and
    # multiple edges between the same pair (exercises the light-edge writer).
    execute_and_fetch_all(cursor, "CREATE (a:N {id: 0})-[:E {w: 10}]->(b:N {id: 1});")
    execute_and_fetch_all(cursor, "MATCH (a:N {id: 0}), (b:N {id: 1}) CREATE (a)-[:E {w: 11}]->(b);")
    execute_and_fetch_all(cursor, "MATCH (a:N {id: 0}) CREATE (a)-[:E {w: 99}]->(a);")  # self-loop
    execute_and_fetch_all(cursor, "MATCH (b:N {id: 1}) CREATE (b)-[:E {w: 20}]->(:N {id: 2});")


def topology(cursor):
    vertices = execute_and_fetch_all(cursor, "MATCH (n:N) RETURN n.id ORDER BY n.id;")
    edges = execute_and_fetch_all(
        cursor,
        "MATCH (a:N)-[e:E]->(b:N) RETURN a.id, b.id, e.w ORDER BY a.id, b.id, e.w;",
    )
    return [tuple(r) for r in vertices], [tuple(r) for r in edges]


def test_snapshot_wal_recovery_light_edge():
    data_directory = tempfile.TemporaryDirectory()

    # 1) Write the dataset, snapshot on exit.
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "default")
    connection = connect(host="localhost", port=7687)
    cursor = connection.cursor()
    create_dataset(cursor)
    before = topology(cursor)
    assert before[0] == [(0,), (1,), (2,)]
    assert before[1] == [(0, 0, 99), (0, 1, 10), (0, 1, 11), (1, 2, 20)]
    interactive_mg_runner.kill_all()

    # 2) Recover on startup and assert identical topology.
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "recover_on_startup")
    connection = connect(host="localhost", port=7687)
    cursor = connection.cursor()
    after = topology(cursor)
    interactive_mg_runner.kill_all()

    assert after == before, f"Topology changed across recovery: before={before} after={after}"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
