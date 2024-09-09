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
import tempfile

import interactive_mg_runner
import pytest
from common import execute_and_fetch_all
from mg_utils import mg_sleep_and_assert, mg_sleep_and_assert_collection
from multitenancy_common import (
    BOLT_PORTS,
    REPLICATION_PORTS,
    create_memgraph_instances_with_role_recovery,
    do_manual_setting_up,
    get_number_of_edges_func,
    get_number_of_nodes_func,
    setup_main,
    show_databases_func,
    show_replicas_func,
)

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))


@pytest.mark.parametrize("replica_name", [("replica_1"), ("replica_2")])
def test_drop_multitenancy_replication_restart_replica(connection, replica_name):
    # Goal: show that the drop database can be restored
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A and B
    # 2/ Write on MAIN to A and B
    # 3/ Restart SYNC replica and drop database
    # 4/ Validate data on replica

    # 0/
    data_directory = tempfile.TemporaryDirectory()
    MEMGRAPH_INSTANCES_DESCRIPTION = create_memgraph_instances_with_role_recovery(data_directory.name)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

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


def test_manual_databases_create_multitenancy_replication_main_behind(connection):
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
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
                f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL)
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


def test_automatic_databases_create_multitenancy_replication(connection):
    # Goal: to show that replication can be established against REPLICA where a new databases
    # needs replication
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A
    # 2/ Write to MAIN A
    # 3/ Validate replication of changes to A have arrived at REPLICA

    # 0/
    data_directory = tempfile.TemporaryDirectory()
    MEMGRAPH_INSTANCES_DESCRIPTION = create_memgraph_instances_with_role_recovery(data_directory.name)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

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


def test_automatic_databases_multitenancy_replication_predefined(connection):
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
            "log_file": "replica1.log",
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
            "log_file": "main.log",
            "setup_queries": [
                "CREATE DATABASE A;",
                "CREATE DATABASE B;",
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
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
    ]
    mg_sleep_and_assert_collection(expected_data, show_replicas_func(cursor))

    cursor_replica = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    assert get_number_of_nodes_func(cursor_replica, "A")() == 1
    assert get_number_of_edges_func(cursor_replica, "A")() == 0
    assert get_number_of_nodes_func(cursor_replica, "B")() == 2
    assert get_number_of_edges_func(cursor_replica, "B")() == 1
