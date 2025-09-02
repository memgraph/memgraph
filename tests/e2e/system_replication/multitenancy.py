# Copyright 2022 Memgraph Ltd.
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
import time
from functools import partial
from pathlib import Path
from typing import Any, Dict

import interactive_mg_runner
import mgclient
import pytest
from common import execute_and_fetch_all, get_data_path, get_logs_path
from mg_utils import mg_sleep_and_assert, mg_sleep_and_assert_collection

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

BOLT_PORTS = {"main": 7687, "replica_1": 7688, "replica_2": 7689}
REPLICATION_PORTS = {"replica_1": 10001, "replica_2": 10002}
file = "multitenancy"


def set_eq(actual, expected):
    return len(actual) == len(expected) and all([x in actual for x in expected])


@pytest.fixture
def test_name(request):
    return request.node.name


@pytest.fixture(autouse=True)
def cleanup_after_test():
    # Run the test
    yield
    # Stop + delete directories after running the test
    interactive_mg_runner.kill_all(keep_directories=False)


def create_memgraph_instances_with_role_recovery(test_name: str) -> Dict[str, Any]:
    return {
        "replica_1": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_1']}",
                "--log-level",
                "TRACE",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                "false",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica1",
        },
        "replica_2": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_2']}",
                "--log-level=TRACE",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                "false",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/replica2.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica2",
        },
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [],
        },
    }


def do_manual_setting_up(connection):
    replica_1_cursor = connection(BOLT_PORTS["replica_1"], "replica_1").cursor()
    execute_and_fetch_all(
        replica_1_cursor, f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};"
    )

    replica_2_cursor = connection(BOLT_PORTS["replica_2"], "replica_2").cursor()
    execute_and_fetch_all(
        replica_2_cursor, f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_2']};"
    )

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    execute_and_fetch_all(
        main_cursor, f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';"
    )
    execute_and_fetch_all(
        main_cursor, f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';"
    )


def get_instances_with_recovery(test_name: str):
    return {
        "replica_1": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_1']}",
                "--log-level=TRACE",
                "--replication-restore-state-on-startup",
                "--data-recovery-on-startup",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica1",
        },
        "replica_2": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_2']}",
                "--log-level=TRACE",
                "--replication-restore-state-on-startup",
                "--data-recovery-on-startup",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/replica2.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica2",
        },
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
                "--replication-restore-state-on-startup",
                "--data-recovery-on-startup",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
        },
    }


def safe_execute(function, *args):
    try:
        function(*args)
    except:
        pass


def setup_replication(connection):
    # Setup replica1
    cursor = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    execute_and_fetch_all(cursor, f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};")
    # Setup replica2
    cursor = connection(BOLT_PORTS["replica_2"], "replica").cursor()
    execute_and_fetch_all(cursor, f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_2']};")
    # Setup main
    cursor = connection(BOLT_PORTS["main"], "main").cursor()
    execute_and_fetch_all(cursor, f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';")
    execute_and_fetch_all(cursor, f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';")


def setup_main(main_cursor):
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")

    execute_and_fetch_all(main_cursor, "USE DATABASE B;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")


def show_replicas_func(cursor):
    def func():
        return execute_and_fetch_all(cursor, "SHOW REPLICAS;")

    return func


def show_w_async_zeroing(cursor):
    def func():
        res = show_replicas_func(cursor)()
        # ASYCN replica, go through data and zero out all invalid timestamps
        for instance_state in res:
            if instance_state[2] == "async":
                # Zero out main replica timestamp if status is invalid
                if instance_state[3]["status"] == "invalid":
                    instance_state[3]["ts"] = 0
                    instance_state[3]["behind"] = None
                # Zero out component timestamps if their status is invalid
                for component, component_info in instance_state[4].items():
                    if component_info["status"] == "invalid":
                        component_info["ts"] = 0
                        component_info["behind"] = 0

        return res

    return func


def show_databases_func(cursor):
    def func():
        return execute_and_fetch_all(cursor, "SHOW DATABASES;")

    return func


def get_number_of_nodes_func(cursor, db_name):
    def func():
        execute_and_fetch_all(cursor, f"USE DATABASE {db_name};")
        return execute_and_fetch_all(cursor, "MATCH (n) RETURN count(*);")[0][0]

    return func


def get_number_of_edges_func(cursor, db_name):
    def func():
        execute_and_fetch_all(cursor, f"USE DATABASE {db_name};")
        return execute_and_fetch_all(cursor, "MATCH ()-[r]->() RETURN count(*);")[0][0]

    return func


def test_manual_databases_create_multitenancy_replication(connection, test_name):
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
            "log_file": f"{get_logs_path(file, test_name)}/replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica1",
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
            "log_file": f"{get_logs_path(file, test_name)}/replica2.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica2",
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
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [
                "CREATE DATABASE A;",
                "CREATE DATABASE B;",
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
                f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL, keep_directories=False)
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


def test_manual_databases_create_multitenancy_replication_branching(connection, test_name):
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
            "log_file": f"{get_logs_path(file, test_name)}/replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica1",
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
            "log_file": f"{get_logs_path(file, test_name)}/replica2.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica2",
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
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
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
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL, keep_directories=False)
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


def test_manual_databases_create_multitenancy_replication_dirty_replica(connection, test_name):
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
            "log_file": f"{get_logs_path(file, test_name)}/replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica1",
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
            "log_file": f"{get_logs_path(file, test_name)}/replica2.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica2",
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
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [
                "CREATE DATABASE A;",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL, keep_directories=False)
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


def test_manual_databases_create_multitenancy_replication_main_behind(connection, test_name):
    # Goal: to show that replication can be established against REPLICA which has
    # different branched databases
    # 0/ REPLICA CREATE DATABASE A
    #    REPLICA write to A
    #    Setup replication
    # 1/ MAIN CREATE DATABASE A
    # 2/ Check that database has been replicated

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "replica_1": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_1']}",
                "--log-level=TRACE",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica1",
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
            "log_file": f"{get_logs_path(file, test_name)}/replica2.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica2",
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
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
                f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL, keep_directories=False)
    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")

    # 2/
    expected_data = [
        (
            "replica_1",
            f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
            "sync",
            {"ts": 3, "behind": None, "status": "ready"},
            {"A": {"ts": 0, "behind": 0, "status": "ready"}, "memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_2",
            f"127.0.0.1:{REPLICATION_PORTS['replica_2']}",
            "async",
            {"ts": 3, "behind": None, "status": "ready"},
            {"A": {"ts": 0, "behind": 0, "status": "ready"}, "memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, show_replicas_func(main_cursor))

    databases_on_main = show_databases_func(main_cursor)()

    replica_cursor = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    mg_sleep_and_assert(databases_on_main, show_databases_func(replica_cursor))

    replica_cursor = connection(BOLT_PORTS["replica_2"], "replica").cursor()
    mg_sleep_and_assert(databases_on_main, show_databases_func(replica_cursor))


def test_automatic_databases_create_multitenancy_replication(connection, test_name):
    # Goal: to show that replication can be established against REPLICA where a new databases
    # needs replication
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A
    # 2/ Write to MAIN A
    # 3/ Validate replication of changes to A have arrived at REPLICA

    # 0/
    MEMGRAPH_INSTANCES_DESCRIPTION = create_memgraph_instances_with_role_recovery(test_name)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)

    do_manual_setting_up(connection)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")

    # 2/
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")

    # 3/
    expected_data = [
        (
            "replica_1",
            f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
            "sync",
            {"ts": 4, "behind": None, "status": "ready"},
            {
                "A": {"ts": 7, "behind": 0, "status": "ready"},
                "B": {"ts": 0, "behind": 0, "status": "ready"},
                "memgraph": {"ts": 0, "behind": 0, "status": "ready"},
            },
        ),
        (
            "replica_2",
            f"127.0.0.1:{REPLICATION_PORTS['replica_2']}",
            "async",
            {"ts": 4, "behind": None, "status": "ready"},
            {
                "A": {"ts": 7, "behind": 0, "status": "ready"},
                "B": {"ts": 0, "behind": 0, "status": "ready"},
                "memgraph": {"ts": 0, "behind": 0, "status": "ready"},
            },
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, show_replicas_func(main_cursor))

    cursor_replica = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    assert get_number_of_nodes_func(cursor_replica, "A")() == 7
    assert get_number_of_edges_func(cursor_replica, "A")() == 3
    assert get_number_of_nodes_func(cursor_replica, "B")() == 0
    assert get_number_of_edges_func(cursor_replica, "B")() == 0

    cursor_replica = connection(BOLT_PORTS["replica_2"], "replica").cursor()
    assert get_number_of_nodes_func(cursor_replica, "A")() == 7
    assert get_number_of_edges_func(cursor_replica, "A")() == 3
    assert get_number_of_nodes_func(cursor_replica, "B")() == 0
    assert get_number_of_edges_func(cursor_replica, "B")() == 0


def test_automatic_databases_multitenancy_replication_predefined(connection, test_name):
    # Goal: to show that replication can be established against REPLICA which doesn't
    # have any additional databases; MAIN's database clean at registration time
    # 0/ MAIN CREATE DATABASE A + B
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
            "log_file": f"{get_logs_path(file, test_name)}/replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica1",
            "setup_queries": [
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};",
            ],
        },
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [
                "CREATE DATABASE A;",
                "CREATE DATABASE B;",
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL, keep_directories=False)
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
    ]
    mg_sleep_and_assert_collection(expected_data, show_replicas_func(cursor))

    cursor_replica = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    assert get_number_of_nodes_func(cursor_replica, "A")() == 1
    assert get_number_of_edges_func(cursor_replica, "A")() == 0
    assert get_number_of_nodes_func(cursor_replica, "B")() == 2
    assert get_number_of_edges_func(cursor_replica, "B")() == 1


def test_automatic_databases_create_multitenancy_replication_dirty_main(connection, test_name):
    # Goal: to show that replication can be established against REPLICA which doesn't
    # have any additional databases; MAIN's database dirty at registration time
    # 0/ MAIN CREATE DATABASE A
    #    MAIN write to A
    #    Setup replication
    # 1/ Validate

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "replica_1": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_1']}",
                "--log-level=TRACE",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica1",
            "setup_queries": [
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};",
            ],
        },
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [
                "CREATE DATABASE A;",
                "USE DATABASE A;",
                "CREATE (:Node{from:'A'})",
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL, keep_directories=False)
    cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    expected_data = [
        (
            "replica_1",
            f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
            "sync",
            {"ts": 1, "behind": None, "status": "ready"},
            {"A": {"ts": 1, "behind": 0, "status": "ready"}, "memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, show_replicas_func(cursor))

    cursor_replica = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    execute_and_fetch_all(cursor_replica, "USE DATABASE A;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 1  # one node
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 0  # zero relationships


@pytest.mark.parametrize("replica_name", [("replica_1"), ("replica_2")])
def test_multitenancy_replication_restart_replica_w_fc(connection, replica_name, test_name):
    # Goal: show that a replica can be recovered with the frequent checker
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A and B
    # 2/ Write on MAIN to A and B
    # 3/ Restart replica
    # 4/ Validate data on replica

    MEMGRAPH_INSTANCES_DESCRIPTION = create_memgraph_instances_with_role_recovery(test_name)
    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)

    do_manual_setting_up(connection)

    # 1/
    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")

    # 2/
    setup_main(main_cursor)

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, replica_name)
    time.sleep(3)  # In order for the frequent check to run
    # Check that the FC did invalidate
    expected_data = {
        "replica_1": [
            (
                "replica_1",
                f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
                "sync",
                {"ts": 4, "behind": None, "status": "invalid"},
                {
                    "A": {"ts": 7, "behind": 0, "status": "invalid"},
                    "B": {"ts": 3, "behind": 0, "status": "invalid"},
                    "memgraph": {"ts": 0, "behind": 0, "status": "invalid"},
                },
            ),
            (
                "replica_2",
                f"127.0.0.1:{REPLICATION_PORTS['replica_2']}",
                "async",
                {"ts": 4, "behind": None, "status": "ready"},
                {
                    "A": {"ts": 7, "behind": 0, "status": "ready"},
                    "B": {"ts": 3, "behind": 0, "status": "ready"},
                    "memgraph": {"ts": 0, "behind": 0, "status": "ready"},
                },
            ),
        ],
        "replica_2": [
            (
                "replica_1",
                f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
                "sync",
                {"ts": 4, "behind": None, "status": "ready"},
                {
                    "A": {"ts": 7, "behind": 0, "status": "ready"},
                    "B": {"ts": 3, "behind": 0, "status": "ready"},
                    "memgraph": {"ts": 0, "behind": 0, "status": "ready"},
                },
            ),
            (
                "replica_2",
                f"127.0.0.1:{REPLICATION_PORTS['replica_2']}",
                "async",
                {"ts": 0, "behind": None, "status": "invalid"},
                {
                    "A": {"ts": 0, "behind": 0, "status": "invalid"},
                    "B": {"ts": 0, "behind": 0, "status": "invalid"},
                    "memgraph": {"ts": 0, "behind": 0, "status": "invalid"},
                },
            ),
        ],
    }
    mg_sleep_and_assert_collection(expected_data[replica_name], show_w_async_zeroing(main_cursor))
    # Restart
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, replica_name)

    # 4/
    expected_data = [
        (
            "replica_1",
            f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
            "sync",
            {"ts": 4, "behind": None, "status": "ready"},
            {
                "A": {"ts": 7, "behind": 0, "status": "ready"},
                "B": {"ts": 3, "behind": 0, "status": "ready"},
                "memgraph": {"ts": 0, "behind": 0, "status": "ready"},
            },
        ),
        (
            "replica_2",
            f"127.0.0.1:{REPLICATION_PORTS['replica_2']}",
            "async",
            {"ts": 4, "behind": None, "status": "ready"},
            {
                "A": {"ts": 7, "behind": 0, "status": "ready"},
                "B": {"ts": 3, "behind": 0, "status": "ready"},
                "memgraph": {"ts": 0, "behind": 0, "status": "ready"},
            },
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, show_replicas_func(main_cursor))

    cursor_replica = connection(BOLT_PORTS[replica_name], "replica").cursor()

    assert get_number_of_nodes_func(cursor_replica, "A")() == 7
    assert get_number_of_edges_func(cursor_replica, "A")() == 3
    assert get_number_of_nodes_func(cursor_replica, "B")() == 2
    assert get_number_of_edges_func(cursor_replica, "B")() == 0


@pytest.mark.parametrize("replica_name", [("replica_1"), ("replica_2")])
def test_multitenancy_replication_restart_replica_wo_fc(connection, replica_name, test_name):
    # Goal: show that a replica can be recovered without the frequent checker detecting it being down
    # needs replicating over
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A and B
    # 2/ Write on MAIN to A and B
    # 3/ Restart replica
    # 4/ Validate data on replica

    # 0/
    MEMGRAPH_INSTANCES_DESCRIPTION = create_memgraph_instances_with_role_recovery(test_name)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)

    do_manual_setting_up(connection)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")

    # 2/
    setup_main(main_cursor)

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, replica_name)
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, replica_name)

    # 4/
    expected_data = [
        (
            "replica_1",
            f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
            "sync",
            {"ts": 4, "behind": None, "status": "ready"},
            {
                "A": {"ts": 7, "behind": 0, "status": "ready"},
                "B": {"ts": 3, "behind": 0, "status": "ready"},
                "memgraph": {"ts": 0, "behind": 0, "status": "ready"},
            },
        ),
        (
            "replica_2",
            f"127.0.0.1:{REPLICATION_PORTS['replica_2']}",
            "async",
            {"ts": 4, "behind": None, "status": "ready"},
            {
                "A": {"ts": 7, "behind": 0, "status": "ready"},
                "B": {"ts": 3, "behind": 0, "status": "ready"},
                "memgraph": {"ts": 0, "behind": 0, "status": "ready"},
            },
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, show_replicas_func(main_cursor))

    cursor_replica = connection(BOLT_PORTS[replica_name], replica_name).cursor()
    assert get_number_of_nodes_func(cursor_replica, "A")() == 7
    assert get_number_of_edges_func(cursor_replica, "A")() == 3
    assert get_number_of_nodes_func(cursor_replica, "B")() == 2
    assert get_number_of_edges_func(cursor_replica, "B")() == 0


@pytest.mark.parametrize("replica_name", [("replica_1"), ("replica_2")])
def test_multitenancy_replication_restart_replica_w_fc_w_rec(connection, replica_name, test_name):
    # Goal: show that a replica recovers data on reconnect
    # needs replicating over
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A and B
    # 2/ Write on MAIN to A and B
    # 3/ Restart replica
    # 4/ Validate data on replica

    # 0/
    MEMGRAPH_INSTANCES_DESCRIPTION_WITH_RECOVERY = get_instances_with_recovery(test_name)

    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_WITH_RECOVERY, keep_directories=False)
    setup_replication(connection)
    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")

    # 2/
    setup_main(main_cursor)

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION_WITH_RECOVERY, replica_name)
    safe_execute(execute_and_fetch_all, main_cursor, "USE DATABASE A;")
    safe_execute(execute_and_fetch_all, main_cursor, "CREATE (:Node{on:'A'});")
    safe_execute(execute_and_fetch_all, main_cursor, "USE DATABASE B;")
    safe_execute(execute_and_fetch_all, main_cursor, "CREATE (:Node{on:'B'});")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION_WITH_RECOVERY, replica_name)

    # 4/
    cursor_replica = connection(BOLT_PORTS[replica_name], "replica").cursor()

    mg_sleep_and_assert(8, get_number_of_nodes_func(cursor_replica, "A"))
    mg_sleep_and_assert(3, get_number_of_edges_func(cursor_replica, "A"))

    mg_sleep_and_assert(3, get_number_of_nodes_func(cursor_replica, "B"))
    mg_sleep_and_assert(0, get_number_of_edges_func(cursor_replica, "B"))


@pytest.mark.parametrize("replica_name", [("replica_1"), ("replica_2")])
def test_multitenancy_replication_drop_replica(connection, replica_name, test_name):
    # Goal: show that the cluster can recover if a replica is dropped and registered again
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A and B
    # 2/ Write on MAIN to A and B
    # 3/ Drop and add the same replica
    # 4/ Validate data on replica

    # 0/
    MEMGRAPH_INSTANCES_DESCRIPTION = create_memgraph_instances_with_role_recovery(test_name)

    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)
    do_manual_setting_up(connection)
    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")

    # 2/
    setup_main(main_cursor)

    # 3/
    execute_and_fetch_all(main_cursor, f"DROP REPLICA {replica_name};")
    sync = {"replica_1": "SYNC", "replica_2": "ASYNC"}
    execute_and_fetch_all(
        main_cursor,
        f"REGISTER REPLICA  {replica_name} {sync[replica_name]} TO '127.0.0.1:{REPLICATION_PORTS[replica_name]}';",
    )

    cursor_replica = connection(BOLT_PORTS[replica_name], "replica").cursor()
    mg_sleep_and_assert(7, get_number_of_nodes_func(cursor_replica, "A"))
    mg_sleep_and_assert(3, get_number_of_edges_func(cursor_replica, "A"))
    mg_sleep_and_assert(2, get_number_of_nodes_func(cursor_replica, "B"))
    mg_sleep_and_assert(0, get_number_of_edges_func(cursor_replica, "B"))


def test_multitenancy_replication_restart_main(connection, test_name):
    # Goal: show that the cluster can restore to a correct state if the MAIN restarts
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A and B
    # 2/ Write on MAIN to A and B
    # 3/ Restart main and write new data
    # 4/ Validate data on replica

    # 0/
    MEMGRAPH_INSTANCES_DESCRIPTION_WITH_RECOVERY = get_instances_with_recovery(test_name)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_WITH_RECOVERY, keep_directories=False)
    setup_replication(connection)
    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")

    # 2/
    setup_main(main_cursor)

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION_WITH_RECOVERY, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION_WITH_RECOVERY, "main")
    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")
    execute_and_fetch_all(main_cursor, "USE DATABASE B;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")

    # 4/
    cursor_replica = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    execute_and_fetch_all(cursor_replica, "USE DATABASE A;")
    assert get_number_of_nodes_func(cursor_replica, "A")() == 8
    assert get_number_of_edges_func(cursor_replica, "A")() == 3
    assert get_number_of_nodes_func(cursor_replica, "B")() == 3
    assert get_number_of_edges_func(cursor_replica, "B")() == 0

    cursor_replica = connection(BOLT_PORTS["replica_2"], "replica").cursor()
    execute_and_fetch_all(cursor_replica, "USE DATABASE A;")
    assert get_number_of_nodes_func(cursor_replica, "A")() == 8
    assert get_number_of_edges_func(cursor_replica, "A")() == 3
    assert get_number_of_nodes_func(cursor_replica, "B")() == 3
    assert get_number_of_edges_func(cursor_replica, "B")() == 0


def test_automatic_databases_drop_multitenancy_replication(connection, test_name):
    # Goal: show that drop database can be replicated
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A
    # 2/ Write to MAIN A
    # 3/ Validate replication of changes to A have arrived at REPLICA
    # 4/ DROP DATABASE A/B
    # 5/ Check that the drop replicated

    # 0/
    MEMGRAPH_INSTANCES_DESCRIPTION = create_memgraph_instances_with_role_recovery(test_name)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)

    do_manual_setting_up(connection)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")

    # 2/
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")

    # 3/
    expected_data = [
        (
            "replica_1",
            f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
            "sync",
            {"ts": 4, "behind": None, "status": "ready"},
            {
                "A": {"ts": 1, "behind": 0, "status": "ready"},
                "B": {"ts": 0, "behind": 0, "status": "ready"},
                "memgraph": {"ts": 0, "behind": 0, "status": "ready"},
            },
        ),
        (
            "replica_2",
            f"127.0.0.1:{REPLICATION_PORTS['replica_2']}",
            "async",
            {"ts": 4, "behind": None, "status": "ready"},
            {
                "A": {"ts": 1, "behind": 0, "status": "ready"},
                "B": {"ts": 0, "behind": 0, "status": "ready"},
                "memgraph": {"ts": 0, "behind": 0, "status": "ready"},
            },
        ),
    ]
    mg_sleep_and_assert(expected_data, show_replicas_func(main_cursor))

    # 4/
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "DROP DATABASE A;")
    execute_and_fetch_all(main_cursor, "DROP DATABASE B;")

    # 5/
    databases_on_main = show_databases_func(main_cursor)()

    replica_cursor = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    mg_sleep_and_assert(databases_on_main, show_databases_func(replica_cursor))

    replica_cursor = connection(BOLT_PORTS["replica_2"], "replica").cursor()
    mg_sleep_and_assert(databases_on_main, show_databases_func(replica_cursor))


@pytest.mark.parametrize("replica_name", [("replica_1"), ("replica_2")])
def test_drop_multitenancy_replication_restart_replica(connection, replica_name, test_name):
    # Goal: show that the drop database can be restored
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A and B
    # 2/ Write on MAIN to A and B
    # 3/ Restart SYNC replica and drop database
    # 4/ Validate data on replica

    # 0/
    MEMGRAPH_INSTANCES_DESCRIPTION = create_memgraph_instances_with_role_recovery(test_name)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)

    do_manual_setting_up(connection)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")

    # 2/
    setup_main(main_cursor)

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, replica_name)
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "DROP DATABASE B;")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, replica_name)

    # 4/
    databases_on_main = show_databases_func(main_cursor)()

    replica_cursor = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    mg_sleep_and_assert(databases_on_main, show_databases_func(replica_cursor))

    replica_cursor = connection(BOLT_PORTS["replica_2"], "replica").cursor()
    mg_sleep_and_assert(databases_on_main, show_databases_func(replica_cursor))


def test_multitenancy_drop_while_replica_using(connection, test_name):
    # Goal: show that the replica can handle a transaction on a database being dropped (will persist until tx finishes)
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A
    # 2/ Write to MAIN A
    # 3/ Validate replication of changes to A have arrived at REPLICA
    # 4/ Start A transaction on replica 1, Use A on replica2
    # 5/ Check that the drop replicated
    # 6/ Validate that the transaction is still active and working and that the replica2 is not pointing to anything

    # 0/
    MEMGRAPH_INSTANCES_DESCRIPTION = create_memgraph_instances_with_role_recovery(test_name)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)

    do_manual_setting_up(connection)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")

    # 2/
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")

    # 3/
    expected_data = [
        (
            "replica_1",
            f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
            "sync",
            {"ts": 3, "behind": None, "status": "ready"},
            {"A": {"ts": 1, "behind": 0, "status": "ready"}, "memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_2",
            f"127.0.0.1:{REPLICATION_PORTS['replica_2']}",
            "async",
            {"ts": 3, "behind": None, "status": "ready"},
            {"A": {"ts": 1, "behind": 0, "status": "ready"}, "memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, show_replicas_func(main_cursor))

    # 4/
    replica1_cursor = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    replica2_cursor = connection(BOLT_PORTS["replica_2"], "replica").cursor()

    execute_and_fetch_all(replica1_cursor, "USE DATABASE A;")
    execute_and_fetch_all(replica1_cursor, "BEGIN")
    execute_and_fetch_all(replica2_cursor, "USE DATABASE A;")

    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "DROP DATABASE A;")

    # 5/
    # TODO Remove this once there is a replica state for the system
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")
    execute_and_fetch_all(main_cursor, "USE DATABASE B;")

    expected_data = [
        (
            "replica_1",
            f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
            "sync",
            {"ts": 5, "behind": None, "status": "ready"},
            {"B": {"ts": 0, "behind": 0, "status": "ready"}, "memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_2",
            f"127.0.0.1:{REPLICATION_PORTS['replica_2']}",
            "async",
            {"ts": 5, "behind": None, "status": "ready"},
            {"B": {"ts": 0, "behind": 0, "status": "ready"}, "memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    mg_sleep_and_assert(expected_data, show_replicas_func(main_cursor))

    # 6/
    assert execute_and_fetch_all(replica1_cursor, "MATCH(n) RETURN count(*);")[0][0] == 1
    execute_and_fetch_all(replica1_cursor, "COMMIT")
    failed = False
    try:
        execute_and_fetch_all(replica1_cursor, "MATCH(n) RETURN n;")
    except mgclient.DatabaseError:
        failed = True
    assert failed

    failed = False
    try:
        execute_and_fetch_all(replica2_cursor, "MATCH(n) RETURN n;")
    except mgclient.DatabaseError:
        failed = True
    assert failed


def test_multitenancy_force_drop_while_replica_using(connection, test_name):
    # Goal: show that force drop database works correctly in replication when replica is using the database
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A
    # 2/ Write to MAIN A
    # 3/ Validate replication of changes to A have arrived at REPLICA
    # 4/ Start A transaction on replica 1, Use A on replica2
    # 5/ Force drop database A on main while replica is using it
    # 6/ Check that the force drop replicated and terminated transactions
    # 7/ Validate that the replica can no longer access the database

    # 0/
    MEMGRAPH_INSTANCES_DESCRIPTION = create_memgraph_instances_with_role_recovery(test_name)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)

    do_manual_setting_up(connection)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")

    # 2/
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")

    # 3/
    expected_data = [
        (
            "replica_1",
            f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
            "sync",
            {"ts": 3, "behind": None, "status": "ready"},
            {"A": {"ts": 5, "behind": 0, "status": "ready"}, "memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_2",
            f"127.0.0.1:{REPLICATION_PORTS['replica_2']}",
            "async",
            {"ts": 3, "behind": None, "status": "ready"},
            {"A": {"ts": 5, "behind": 0, "status": "ready"}, "memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, show_replicas_func(main_cursor))

    # 4/
    replica1_cursor = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    replica2_cursor = connection(BOLT_PORTS["replica_2"], "replica").cursor()

    execute_and_fetch_all(replica1_cursor, "USE DATABASE A;")
    execute_and_fetch_all(replica1_cursor, "BEGIN")
    execute_and_fetch_all(replica2_cursor, "USE DATABASE A;")

    # Verify data exists on replicas
    assert execute_and_fetch_all(replica1_cursor, "MATCH(n) RETURN count(*);")[0][0] == 3
    assert execute_and_fetch_all(replica2_cursor, "MATCH(n) RETURN count(*);")[0][0] == 3

    # 5/ Force drop database A on main while replica is using it
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "DROP DATABASE A FORCE;")

    # 6/ Check that the force drop replicated and terminated transactions
    # Wait for replication to complete
    expected_data = [
        (
            "replica_1",
            f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
            "sync",
            {"ts": 4, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_2",
            f"127.0.0.1:{REPLICATION_PORTS['replica_2']}",
            "async",
            {"ts": 4, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, show_replicas_func(main_cursor))

    assert execute_and_fetch_all(replica1_cursor, "MATCH(n) RETURN count(*);")[0][0] == 3
    execute_and_fetch_all(replica1_cursor, "COMMIT")
    try:
        assert execute_and_fetch_all(replica1_cursor, "MATCH(n) RETURN count(*);")[0][0] == 3
        assert False, "Replica1 should not be able to access dropped database"
    except mgclient.DatabaseError:
        pass
    try:
        assert execute_and_fetch_all(replica2_cursor, "MATCH(n) RETURN count(*);")[0][0] == 3
        assert False, "Replica2 should not be able to access dropped database"
    except mgclient.DatabaseError:
        pass

    # Verify replicas can still work with default database
    execute_and_fetch_all(replica1_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(replica2_cursor, "USE DATABASE memgraph;")

    # 7/ Validate that the replica can no longer access the database
    # Try to use the dropped database
    try:
        execute_and_fetch_all(replica1_cursor, "USE DATABASE A;")
        assert False, "Replica1 should not be able to use dropped database"
    except mgclient.DatabaseError:
        pass

    try:
        execute_and_fetch_all(replica2_cursor, "USE DATABASE A;")
        assert False, "Replica2 should not be able to use dropped database"
    except mgclient.DatabaseError:
        pass


def test_multitenancy_drop_and_recreate_while_replica_using(connection, test_name):
    # Goal: show that the replica can handle a transaction on a database being dropped and the same name reused
    # Original storage should persist in a nameless state until tx is over
    # needs replicating over
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A
    # 2/ Write to MAIN A
    # 3/ Validate replication of changes to A have arrived at REPLICA
    # 4/ Start A transaction on replica 1, Use A on replica2
    # 5/ Check that the drop/create replicated
    # 6/ Validate that the transaction is still active and working and that the replica2 is not pointing to anything

    # 0/
    MEMGRAPH_INSTANCES_DESCRIPTION = create_memgraph_instances_with_role_recovery(test_name)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)

    do_manual_setting_up(connection)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")

    # 2/
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")

    # 3/
    expected_data = [
        (
            "replica_1",
            f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
            "sync",
            {"ts": 3, "behind": None, "status": "ready"},
            {"A": {"ts": 1, "behind": 0, "status": "ready"}, "memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_2",
            f"127.0.0.1:{REPLICATION_PORTS['replica_2']}",
            "async",
            {"ts": 3, "behind": None, "status": "ready"},
            {"A": {"ts": 1, "behind": 0, "status": "ready"}, "memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, show_replicas_func(main_cursor))

    # 4/
    replica1_cursor = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    replica2_cursor = connection(BOLT_PORTS["replica_2"], "replica").cursor()

    execute_and_fetch_all(replica1_cursor, "USE DATABASE A;")
    execute_and_fetch_all(replica1_cursor, "BEGIN")
    execute_and_fetch_all(replica2_cursor, "USE DATABASE A;")

    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "DROP DATABASE A;")

    # 5/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")

    expected_data = [
        (
            "replica_1",
            f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
            "sync",
            {"ts": 5, "behind": None, "status": "ready"},
            {"A": {"ts": 0, "behind": 0, "status": "ready"}, "memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_2",
            f"127.0.0.1:{REPLICATION_PORTS['replica_2']}",
            "async",
            {"ts": 5, "behind": None, "status": "ready"},
            {"A": {"ts": 0, "behind": 0, "status": "ready"}, "memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    mg_sleep_and_assert(expected_data, show_replicas_func(main_cursor))

    # 6/
    assert execute_and_fetch_all(replica1_cursor, "MATCH(n) RETURN count(*);")[0][0] == 1
    execute_and_fetch_all(replica1_cursor, "COMMIT")
    failed = False
    try:
        execute_and_fetch_all(replica1_cursor, "MATCH(n) RETURN n;")
    except mgclient.DatabaseError:
        failed = True
    assert failed

    failed = False
    try:
        execute_and_fetch_all(replica2_cursor, "MATCH(n) RETURN n;")
    except mgclient.DatabaseError:
        failed = True
    assert failed


def test_multitenancy_snapshot_recovery(connection, test_name):
    # Goal: show that the replica can handle a transaction on a database being dropped and the same name reused
    # Original storage should persist in a nameless state until tx is over
    # needs replicating over
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A
    # 2/ Write to MAIN A
    # 3/ Validate replication of changes to A have arrived at REPLICA
    # 4/ Start A transaction on replica 1, Use A on replica2
    # 5/ Check that the drop/create replicated
    # 6/ Validate that the transaction is still active and working and that the replica2 is not pointing to anything

    # 0/
    MEMGRAPH_INSTANCES_DESCRIPTION = create_memgraph_instances_with_role_recovery(test_name)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)

    do_manual_setting_up(connection)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")

    # 2/
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'M'});")
    execute_and_fetch_all(main_cursor, "CREATE SNAPSHOT;")
    execute_and_fetch_all(main_cursor, "MATCH (n) DELETE n;")
    execute_and_fetch_all(main_cursor, "CREATE (:New);")
    execute_and_fetch_all(main_cursor, "CREATE SNAPSHOT;")

    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")

    # 3/
    expected_data = [
        (
            "replica_1",
            f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
            "sync",
            {"ts": 3, "behind": None, "status": "ready"},
            {"A": {"ts": 5, "behind": 0, "status": "ready"}, "memgraph": {"ts": 7, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_2",
            f"127.0.0.1:{REPLICATION_PORTS['replica_2']}",
            "async",
            {"ts": 3, "behind": None, "status": "ready"},
            {"A": {"ts": 5, "behind": 0, "status": "ready"}, "memgraph": {"ts": 7, "behind": 0, "status": "ready"}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, show_replicas_func(main_cursor))

    def check_data(cursor):
        execute_and_fetch_all(cursor, "USE DATABASE memgraph;")
        res = execute_and_fetch_all(cursor, "MATCH (n) RETURN n;")
        assert len(res) == 1
        assert res[0][0].labels == {"New"}
        assert res[0][0].properties == {}
        execute_and_fetch_all(cursor, "USE DATABASE A;")
        res = execute_and_fetch_all(cursor, "MATCH (n) RETURN n;")
        assert len(res) == 1
        assert res[0][0].labels == {"Node"}
        assert res[0][0].properties == {"on": "M"}

    # 4/
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    snapshot_a = None
    try:
        main_dir = f"{get_data_path(file, test_name)}/main"
        main_dir = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", main_dir, "snapshots")
        dir_path = Path(main_dir)
        files = [f for f in dir_path.iterdir() if f.is_file() and not f.name.startswith(".")]
        assert files, f"No files found in '{main_dir}'."
        snapshot_a = min(files, key=lambda f: f.stat().st_mtime)
    except Exception as e:
        assert False, f"Error: {e}"

    execute_and_fetch_all(main_cursor, f'RECOVER SNAPSHOT "{snapshot_a}" FORCE;')
    check_data(main_cursor)

    expected_data = [
        (
            "replica_1",
            f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
            "sync",
            {"ts": 3, "behind": None, "status": "ready"},
            {"A": {"ts": 2, "behind": 0, "status": "ready"}, "memgraph": {"ts": 7, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_2",
            f"127.0.0.1:{REPLICATION_PORTS['replica_2']}",
            "async",
            {"ts": 3, "behind": None, "status": "ready"},
            {"A": {"ts": 2, "behind": 0, "status": "ready"}, "memgraph": {"ts": 7, "behind": 0, "status": "ready"}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, show_replicas_func(main_cursor))

    replica1_cursor = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    check_data(replica1_cursor)
    replica2_cursor = connection(BOLT_PORTS["replica_2"], "replica").cursor()
    check_data(replica2_cursor)

    # 5/
    interactive_mg_runner.stop_all(keep_directories=True)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=True)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()
    check_data(main_cursor)
    replica1_cursor = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    check_data(replica1_cursor)
    replica2_cursor = connection(BOLT_PORTS["replica_2"], "replica").cursor()
    check_data(replica2_cursor)

    # Cleanup
    interactive_mg_runner.stop_all(keep_directories=False)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
