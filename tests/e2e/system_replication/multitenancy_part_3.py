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
import tempfile
import time

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
    show_replicas_func,
)

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))


def test_automatic_databases_create_multitenancy_replication_dirty_main(connection):
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
                "USE DATABASE A;",
                "CREATE (:Node{from:'A'})",
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL)
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
def test_multitenancy_replication_restart_replica_w_fc(connection, replica_name):
    # Goal: show that a replica can be recovered with the frequent checker
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A and B
    # 2/ Write on MAIN to A and B
    # 3/ Restart replica
    # 4/ Validate data on replica

    data_directory = tempfile.TemporaryDirectory()
    MEMGRAPH_INSTANCES_DESCRIPTION = create_memgraph_instances_with_role_recovery(data_directory.name)
    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

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
                {"ts": 4, "behind": None, "status": "ready"},
                {
                    "A": {"ts": 0, "behind": 0, "status": "invalid"},
                    "B": {"ts": 0, "behind": 0, "status": "invalid"},
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
                {"ts": 4, "behind": None, "status": "ready"},
                {
                    "A": {"ts": 0, "behind": 0, "status": "invalid"},
                    "B": {"ts": 0, "behind": 0, "status": "invalid"},
                    "memgraph": {"ts": 0, "behind": 0, "status": "invalid"},
                },
            ),
        ],
    }
    mg_sleep_and_assert_collection(expected_data[replica_name], show_replicas_func(main_cursor))
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
def test_multitenancy_replication_restart_replica_wo_fc(connection, replica_name):
    # Goal: show that a replica can be recovered without the frequent checker detecting it being down
    # needs replicating over
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A and B
    # 2/ Write on MAIN to A and B
    # 3/ Restart replica
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


if __name__ == "__main__":
    interactive_mg_runner.cleanup_directories_on_exit()
    sys.exit(pytest.main([__file__, "-rA"]))
