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
import time
from functools import partial

import interactive_mg_runner
import pytest
from common import (
    connect,
    execute_and_fetch_all,
    execute_and_ignore_dead_replica,
    get_data_path,
    get_logs_path,
    show_instances,
    wait_until_main_writeable,
)
from mg_utils import (
    mg_sleep_and_assert,
    mg_sleep_and_assert_collection,
    mg_sleep_and_assert_eval_function,
    mg_sleep_and_assert_multiple,
    mg_sleep_and_assert_until_role_change,
    wait_for_status_change,
)

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

file = "coords_request_forwarding"


@pytest.fixture(autouse=True)
def cleanup_after_test():
    # Run the test
    yield
    # Stop + delete directories after running the test
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


def get_instances_description_no_setup(test_name: str):
    return {
        "instance_1": {
            "args": [
                "--bolt-port=7687",
                "--log-level=TRACE",
                "--management-port=10011",
                "--storage-wal-file-size-kib=1",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_1",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--bolt-port=7688",
                "--log-level=TRACE",
                "--management-port=10012",
                "--storage-wal-file-size-kib=1",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_2",
            "setup_queries": [],
        },
        "coordinator_1": {
            "args": [
                "--bolt-port=7690",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10111",
                "--management-port=10121",
                "--coordinator-hostname=localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_1",
            "setup_queries": [],
        },
        "coordinator_2": {
            "args": [
                "--bolt-port=7691",
                "--log-level=TRACE",
                "--coordinator-id=2",
                "--coordinator-port=10112",
                "--management-port=10122",
                "--coordinator-hostname=localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_2",
            "setup_queries": [],
        },
        "coordinator_3": {
            "args": [
                "--bolt-port=7692",
                "--log-level=TRACE",
                "--coordinator-id=3",
                "--coordinator-port=10113",
                "--management-port=10123",
                "--coordinator-hostname=localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_3",
            "setup_queries": [],
        },
    }


def get_cluster():
    return [
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
        "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}",
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
        "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
        "SET INSTANCE instance_1 TO MAIN",
    ]


# Executes setup queries and returns cluster info
def setup_cluster(test_name, setup_queries, coord_port):
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)
    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)
    coord_cursor = connect(host="localhost", port=coord_port).cursor()

    for query in setup_queries:
        execute_and_fetch_all(coord_cursor, query)

    return inner_instances_description


def test_force_reset_fwd(test_name):
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)
    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)
    leader_cursor = connect(host="localhost", port=7690).cursor()
    # Add itself and coordinator 2. Coordinator 1 is therefore the leader at the beginning
    execute_and_fetch_all(
        leader_cursor,
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
    )
    execute_and_fetch_all(
        leader_cursor,
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
    )
    execute_and_fetch_all(
        leader_cursor,
        "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}",
    )
    follower_cursor = connect(host="localhost", port=7691).cursor()

    # Follower can register the instance
    execute_and_fetch_all(
        follower_cursor,
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'}",
    )

    # Follower can register the instance
    execute_and_fetch_all(
        follower_cursor,
        "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'}",
    )

    execute_and_fetch_all(follower_cursor, "SET INSTANCE instance_1 TO MAIN")
    execute_and_fetch_all(follower_cursor, "FORCE RESET CLUSTER STATE")


def test_register_instance_fwd(test_name):
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)
    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)
    leader_cursor = connect(host="localhost", port=7690).cursor()
    # Add itself and coordinator 2. Coordinator 1 is therefore the leader at the beginning
    execute_and_fetch_all(
        leader_cursor,
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
    )
    execute_and_fetch_all(
        leader_cursor,
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
    )
    execute_and_fetch_all(
        leader_cursor,
        "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}",
    )
    follower_cursor = connect(host="localhost", port=7691).cursor()

    # Follower can register the instance
    execute_and_fetch_all(
        follower_cursor,
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'}",
    )

    # Follower can register the instance
    execute_and_fetch_all(
        follower_cursor,
        "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'}",
    )

    execute_and_fetch_all(follower_cursor, "SET INSTANCE instance_1 TO MAIN")

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "main"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
    ]
    mg_sleep_and_assert(data, partial(show_instances, leader_cursor))
    follower_cursor = connect(host="localhost", port=7691).cursor()
    mg_sleep_and_assert(data, partial(show_instances, follower_cursor))
    follower2_cursor = connect(host="localhost", port=7692).cursor()
    mg_sleep_and_assert(data, partial(show_instances, follower2_cursor))

    execute_and_fetch_all(follower_cursor, "unregister instance instance_2")
    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "main"),
    ]
    mg_sleep_and_assert(data, partial(show_instances, leader_cursor))
    follower_cursor = connect(host="localhost", port=7691).cursor()
    mg_sleep_and_assert(data, partial(show_instances, follower_cursor))
    follower2_cursor = connect(host="localhost", port=7692).cursor()
    mg_sleep_and_assert(data, partial(show_instances, follower2_cursor))

    execute_and_fetch_all(follower_cursor, "demote instance instance_1")
    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
    ]
    mg_sleep_and_assert(data, partial(show_instances, leader_cursor))
    follower_cursor = connect(host="localhost", port=7691).cursor()
    mg_sleep_and_assert(data, partial(show_instances, follower_cursor))
    follower2_cursor = connect(host="localhost", port=7692).cursor()
    mg_sleep_and_assert(data, partial(show_instances, follower2_cursor))


def test_add_coordinator_update_config_fwd(test_name):
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)
    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)
    leader_cursor = connect(host="localhost", port=7690).cursor()
    # Add itself and coordinator 2. Coordinator 1 is therefore the leader at the beginning
    execute_and_fetch_all(
        leader_cursor,
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
    )
    execute_and_fetch_all(
        leader_cursor,
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
    )

    # Now try to add coordinator 3 from the follower = coordinator 2
    follower_cursor = connect(host="localhost", port=7691).cursor()
    execute_and_fetch_all(
        follower_cursor,
        "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}",
    )

    leader_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "follower"),
    ]
    mg_sleep_and_assert(leader_data, partial(show_instances, leader_cursor))
    mg_sleep_and_assert(leader_data, partial(show_instances, follower_cursor))
    follower2_cursor = connect(host="localhost", port=7692).cursor()
    mg_sleep_and_assert(leader_data, partial(show_instances, follower2_cursor))

    execute_and_fetch_all(follower_cursor, "UPDATE CONFIG FOR COORDINATOR 3 {'bolt_server': '127.0.0.1:7692'}")


def test_remove_coordinator_fwd(test_name):
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)
    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)
    leader_cursor = connect(host="localhost", port=7690).cursor()
    # Add itself and coordinator 2. Coordinator 1 is therefore the leader at the beginning
    execute_and_fetch_all(
        leader_cursor,
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
    )
    execute_and_fetch_all(
        leader_cursor,
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
    )
    execute_and_fetch_all(
        leader_cursor,
        "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}",
    )

    leader_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "follower"),
    ]
    mg_sleep_and_assert(leader_data, partial(show_instances, leader_cursor))

    follower_cursor = connect(host="localhost", port=7691).cursor()

    # Follower can remove follower
    execute_and_fetch_all(follower_cursor, "remove coordinator 3")

    leader_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
    ]
    mg_sleep_and_assert(leader_data, partial(show_instances, leader_cursor))

    # Follower cannot remove the current leader
    try:
        execute_and_fetch_all(follower_cursor, "remove coordinator 1")
        assert False
    except:
        pass

    # Follower can send the request to remove itself
    execute_and_fetch_all(follower_cursor, "remove coordinator 2")
    leader_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader"),
    ]
    mg_sleep_and_assert(leader_data, partial(show_instances, leader_cursor))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
