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
from functools import partial

import interactive_mg_runner
import pytest
from common import (
    connect,
    execute_and_fetch_all,
    get_data_path,
    get_logs_path,
    show_instances,
)
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

file = "disable_writing_on_main_after_restart"
test_name = "test_writing_disabled_on_main_restart"


MEMGRAPH_INSTANCES_DESCRIPTION = {
    "instance_1": {
        "args": [
            "--bolt-port",
            "7687",
            "--log-level",
            "TRACE",
            "--management-port",
            "10011",
            "--also-log-to-stderr",
            "--instance-health-check-frequency-sec",
            "1",
            "--instance-down-timeout-sec",
            "5",
        ],
        "log_file": f"{get_logs_path(file, test_name)}/instance_1.log",
        "data_directory": f"{get_data_path(file, test_name)}/instance_1",
        "setup_queries": [],
    },
    "instance_2": {
        "args": [
            "--bolt-port",
            "7688",
            "--log-level",
            "TRACE",
            "--management-port",
            "10012",
            "--also-log-to-stderr",
            "--instance-health-check-frequency-sec",
            "1",
            "--instance-down-timeout-sec",
            "5",
        ],
        "log_file": f"{get_logs_path(file, test_name)}/instance_2.log",
        "data_directory": f"{get_data_path(file, test_name)}/instance_2",
        "setup_queries": [],
    },
    "instance_3": {
        "args": [
            "--bolt-port",
            "7689",
            "--log-level",
            "TRACE",
            "--management-port",
            "10013",
            "--also-log-to-stderr",
            "--instance-health-check-frequency-sec",
            "5",
            "--instance-down-timeout-sec",
            "10",
        ],
        "log_file": f"{get_logs_path(file, test_name)}/instance_3.log",
        "data_directory": f"{get_data_path(file, test_name)}/instance_3",
        "setup_queries": [],
    },
    "coordinator_1": {
        "args": [
            "--bolt-port",
            "7690",
            "--log-level=TRACE",
            "--coordinator-id=1",
            "--coordinator-port=10111",
            "--coordinator-hostname=localhost",
            "--management-port=10121",
        ],
        "log_file": f"{get_logs_path(file, test_name)}/coordinator_1.log",
        "data_directory": f"{get_data_path(file, test_name)}/coordinator_1",
        "setup_queries": [],
    },
    "coordinator_2": {
        "args": [
            "--bolt-port",
            "7691",
            "--log-level=TRACE",
            "--coordinator-id=2",
            "--coordinator-port=10112",
            "--coordinator-hostname=localhost",
            "--management-port=10122",
        ],
        "log_file": f"{get_logs_path(file, test_name)}/coordinator_2.log",
        "data_directory": f"{get_data_path(file, test_name)}/coordinator_2",
        "setup_queries": [],
    },
    "coordinator_3": {
        "args": [
            "--bolt-port",
            "7692",
            "--log-level=TRACE",
            "--coordinator-id=3",
            "--coordinator-port=10113",
            "--also-log-to-stderr",
            "--coordinator-hostname=localhost",
            "--management-port=10123",
        ],
        "log_file": f"{get_logs_path(file, test_name)}/coordinator_3.log",
        "data_directory": f"{get_data_path(file, test_name)}/coordinator_3",
        "setup_queries": [],
    },
}


@pytest.fixture(autouse=True)
def cleanup_after_test():
    # Run the test
    yield
    # Stop + delete directories after running the test
    interactive_mg_runner.kill_all(keep_directories=False)


def test_writing_disabled_on_main_restart():
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)

    coordinator3_cursor = connect(host="localhost", port=7692).cursor()

    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
    )
    execute_and_fetch_all(coordinator3_cursor, "SET INSTANCE instance_3 TO MAIN")
    execute_and_fetch_all(
        coordinator3_cursor,
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}",
    )

    expected_cluster_coord3 = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]
    mg_sleep_and_assert(expected_cluster_coord3, partial(show_instances, coordinator3_cursor))

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    expected_cluster_coord3 = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "down", "unknown"),
    ]

    mg_sleep_and_assert(expected_cluster_coord3, partial(show_instances, coordinator3_cursor))

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    try:
        instance3_cursor = connect(host="localhost", port=7689).cursor()
        execute_and_fetch_all(instance3_cursor, "CREATE (n:Node {name: 'node'})")
    except Exception as e:
        assert str(e).startswith("Write queries currently forbidden on the main instance.")

    expected_cluster_coord3 = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    mg_sleep_and_assert(expected_cluster_coord3, partial(show_instances, coordinator3_cursor))
    execute_and_fetch_all(instance3_cursor, "CREATE (n:Node {name: 'node'})")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
