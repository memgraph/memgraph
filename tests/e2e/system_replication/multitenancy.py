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
import os
import sys

import interactive_mg_runner
import mgclient
import pytest
from common import execute_and_fetch_all
from mg_utils import mg_sleep_and_assert, mg_sleep_and_assert_collection
from multitenancy_common import (
    BOLT_PORTS,
    REPLICATION_PORTS,
    get_number_of_edges_func,
    get_number_of_nodes_func,
    show_replicas_func,
)

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))


def test_manual_databases_create_multitenancy_replication(connection):
    # Goal: to show that replication can be established against REPLICA which already
    # has the clean databases we need
    # 0/ MAIN CREATE DATABASE A + B
    #    REPLICA CREATE DATABASE A + B
    #    Setup replication
    # 1/ Write to MAIN A, Write to MAIN B
    # 2/ Validate replication of changes to A + B have arrived at REPLICA

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "replica_1": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_1']}",
                "--log-level=TRACE",
            ],
            "log_file": "replica1.log",
            "setup_queries": [
                "CREATE DATABASE A;",
                "CREATE DATABASE B;",
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};",
            ],
        },
        "replica_2": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_2']}",
                "--log-level=TRACE",
            ],
            "log_file": "replica2.log",
            "setup_queries": [
                "CREATE DATABASE A;",
                "CREATE DATABASE B;",
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_2']};",
            ],
        },
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
            ],
            "log_file": "main.log",
            "setup_queries": [
                "CREATE DATABASE A;",
                "CREATE DATABASE B;",
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
                f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL)
    cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    execute_and_fetch_all(cursor, "USE DATABASE A;")
    execute_and_fetch_all(cursor, "CREATE ();")
    execute_and_fetch_all(cursor, "USE DATABASE B;")
    execute_and_fetch_all(cursor, "CREATE ()-[:EDGE]->();")

    # 2/
    expected_data = [
        (
            "replica_1",
            f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
            "sync",
            {"ts": 2, "behind": None, "status": "ready"},
            {
                "A": {"ts": 1, "behind": 0, "status": "ready"},
                "B": {"ts": 1, "behind": 0, "status": "ready"},
                "memgraph": {"ts": 0, "behind": 0, "status": "ready"},
            },
        ),
        (
            "replica_2",
            f"127.0.0.1:{REPLICATION_PORTS['replica_2']}",
            "async",
            {"ts": 2, "behind": None, "status": "ready"},
            {
                "A": {"ts": 1, "behind": 0, "status": "ready"},
                "B": {"ts": 1, "behind": 0, "status": "ready"},
                "memgraph": {"ts": 0, "behind": 0, "status": "ready"},
            },
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, show_replicas_func(cursor))

    cursor_replica = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    assert get_number_of_nodes_func(cursor_replica, "A")() == 1
    assert get_number_of_edges_func(cursor_replica, "A")() == 0
    assert get_number_of_nodes_func(cursor_replica, "B")() == 2
    assert get_number_of_edges_func(cursor_replica, "B")() == 1

    cursor_replica2 = connection(BOLT_PORTS["replica_1"], "replica_2").cursor()
    assert get_number_of_nodes_func(cursor_replica2, "A")() == 1
    assert get_number_of_edges_func(cursor_replica2, "A")() == 0
    assert get_number_of_nodes_func(cursor_replica2, "B")() == 2
    assert get_number_of_edges_func(cursor_replica2, "B")() == 1


def test_manual_databases_create_multitenancy_replication_branching(connection):
    # Goal: to show that replication can be established against REPLICA which already
    # has all the databases and the same data
    # 0/ MAIN CREATE DATABASE A + B and fill with data
    #    REPLICA CREATE DATABASE A + B and fil with exact data
    #    Setup REPLICA
    # 1/ Registering REPLICA on MAIN should not fail due to tenant branching

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "replica_1": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_1']}",
                "--log-level=TRACE",
            ],
            "log_file": "replica1.log",
            "setup_queries": [
                "CREATE DATABASE A;",
                "USE DATABASE A;",
                "CREATE ()",
                "CREATE DATABASE B;",
                "USE DATABASE B;",
                "CREATE ()-[:EDGE]->()",
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};",
            ],
        },
        "replica_2": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_2']}",
                "--log-level=TRACE",
            ],
            "log_file": "replica2.log",
            "setup_queries": [
                "CREATE DATABASE A;",
                "USE DATABASE A;",
                "CREATE ()",
                "CREATE DATABASE B;",
                "USE DATABASE B;",
                "CREATE ()-[:EDGE]->()",
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_2']};",
            ],
        },
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
            ],
            "log_file": "main.log",
            "setup_queries": [
                "CREATE DATABASE A;",
                "USE DATABASE A;",
                "CREATE ()",
                "CREATE DATABASE B;",
                "USE DATABASE B;",
                "CREATE ()-[:EDGE]->()",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL)
    cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    failed = False
    try:
        execute_and_fetch_all(
            cursor, f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';"
        )
    except mgclient.DatabaseError:
        failed = True
    assert not failed

    try:
        execute_and_fetch_all(
            cursor, f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';"
        )
    except mgclient.DatabaseError:
        failed = True
    assert not failed


def test_manual_databases_create_multitenancy_replication_dirty_replica(connection):
    # Goal: to show that replication can be established against REPLICA which already
    # has all the databases we need, even when they branched
    # 0/ MAIN CREATE DATABASE A
    #    REPLICA CREATE DATABASE A
    #    REPLICA write to A
    #    Setup REPLICA
    # 1/ Register replica; should fail

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "replica_1": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_1']}",
                "--log-level=TRACE",
            ],
            "log_file": "replica1.log",
            "setup_queries": [
                "CREATE DATABASE A;",
                "USE DATABASE A;",
                "CREATE (:Node{from:'A'})",
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};",
            ],
        },
        "replica_2": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_2']}",
                "--log-level=TRACE",
            ],
            "log_file": "replica2.log",
            "setup_queries": [
                "CREATE DATABASE A;",
                "USE DATABASE A;",
                "CREATE (:Node{from:'A'})",
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_2']};",
            ],
        },
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
            ],
            "log_file": "main.log",
            "setup_queries": [
                "CREATE DATABASE A;",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL)
    cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    failed = False
    try:
        execute_and_fetch_all(
            cursor, f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';"
        )
    except mgclient.DatabaseError:
        failed = True
    assert not failed

    try:
        execute_and_fetch_all(
            cursor, f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';"
        )
    except mgclient.DatabaseError:
        failed = True
    assert not failed


if __name__ == "__main__":
    interactive_mg_runner.cleanup_directories_on_exit()
    sys.exit(pytest.main([__file__, "-rA"]))
