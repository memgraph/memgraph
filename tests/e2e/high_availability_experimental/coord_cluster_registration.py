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
import shutil
import sys
import tempfile

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all, safe_execute
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

TEMP_DIR = tempfile.TemporaryDirectory().name

MEMGRAPH_INSTANCES_DESCRIPTION = {
    "instance_1": {
        "args": [
            "--bolt-port",
            "7687",
            "--log-level",
            "TRACE",
            "--coordinator-server-port",
            "10011",
        ],
        "log_file": "instance_1.log",
        "data_directory": f"{TEMP_DIR}/instance_1",
        "setup_queries": [],
    },
    "instance_2": {
        "args": [
            "--bolt-port",
            "7688",
            "--log-level",
            "TRACE",
            "--coordinator-server-port",
            "10012",
        ],
        "log_file": "instance_2.log",
        "data_directory": f"{TEMP_DIR}/instance_2",
        "setup_queries": [],
    },
    "instance_3": {
        "args": [
            "--bolt-port",
            "7689",
            "--log-level",
            "TRACE",
            "--coordinator-server-port",
            "10013",
        ],
        "log_file": "instance_3.log",
        "data_directory": f"{TEMP_DIR}/instance_3",
        "setup_queries": [],
    },
    "coordinator_1": {
        "args": [
            "--bolt-port",
            "7690",
            "--log-level=TRACE",
            "--raft-server-id=1",
            "--raft-server-port=10111",
        ],
        "log_file": "coordinator1.log",
        "setup_queries": [],
    },
    "coordinator_2": {
        "args": [
            "--bolt-port",
            "7691",
            "--log-level=TRACE",
            "--raft-server-id=2",
            "--raft-server-port=10112",
        ],
        "log_file": "coordinator2.log",
        "setup_queries": [],
    },
    "coordinator_3": {
        "args": [
            "--bolt-port",
            "7692",
            "--log-level=TRACE",
            "--raft-server-id=3",
            "--raft-server-port=10113",
        ],
        "log_file": "coordinator3.log",
        "setup_queries": [],
    },
}


# NOTE: Repeated execution because it can fail if Raft server is not up
def add_coordinator(cursor, query):
    for _ in range(10):
        try:
            execute_and_fetch_all(cursor, query)
            return True
        except Exception:
            pass
    return False


def test_register_repl_instances_then_coordinators():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    coordinator3_cursor = connect(host="localhost", port=7692).cursor()

    execute_and_fetch_all(
        coordinator3_cursor, "REGISTER INSTANCE instance_1 ON '127.0.0.1:10011' WITH '127.0.0.1:10001'"
    )
    execute_and_fetch_all(
        coordinator3_cursor, "REGISTER INSTANCE instance_2 ON '127.0.0.1:10012' WITH '127.0.0.1:10002'"
    )
    execute_and_fetch_all(
        coordinator3_cursor, "REGISTER INSTANCE instance_3 ON '127.0.0.1:10013' WITH '127.0.0.1:10003'"
    )
    execute_and_fetch_all(coordinator3_cursor, "SET INSTANCE instance_3 TO MAIN")
    assert add_coordinator(coordinator3_cursor, "ADD COORDINATOR 1 ON '127.0.0.1:10111'")
    assert add_coordinator(coordinator3_cursor, "ADD COORDINATOR 2 ON '127.0.0.1:10112'")

    def check_coordinator3():
        return sorted(list(execute_and_fetch_all(coordinator3_cursor, "SHOW INSTANCES")))

    expected_cluster_coord3 = [
        ("coordinator_1", "127.0.0.1:10111", "", True, "coordinator"),
        ("coordinator_2", "127.0.0.1:10112", "", True, "coordinator"),
        ("coordinator_3", "127.0.0.1:10113", "", True, "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", True, "replica"),
        ("instance_2", "", "127.0.0.1:10012", True, "replica"),
        ("instance_3", "", "127.0.0.1:10013", True, "main"),
    ]
    mg_sleep_and_assert(expected_cluster_coord3, check_coordinator3)

    coordinator1_cursor = connect(host="localhost", port=7690).cursor()

    def check_coordinator1():
        return sorted(list(execute_and_fetch_all(coordinator1_cursor, "SHOW INSTANCES")))

    # TODO: (andi) This should be solved eventually
    expected_cluster_not_shared = [
        ("coordinator_1", "127.0.0.1:10111", "", True, "coordinator"),
        ("coordinator_2", "127.0.0.1:10112", "", True, "coordinator"),
        ("coordinator_3", "127.0.0.1:10113", "", True, "coordinator"),
    ]

    mg_sleep_and_assert(expected_cluster_not_shared, check_coordinator1)

    coordinator2_cursor = connect(host="localhost", port=7691).cursor()

    def check_coordinator2():
        return sorted(list(execute_and_fetch_all(coordinator2_cursor, "SHOW INSTANCES")))

    mg_sleep_and_assert(expected_cluster_not_shared, check_coordinator2)


def test_register_coordinator_then_repl_instances():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    coordinator3_cursor = connect(host="localhost", port=7692).cursor()

    assert add_coordinator(coordinator3_cursor, "ADD COORDINATOR 1 ON '127.0.0.1:10111'")
    assert add_coordinator(coordinator3_cursor, "ADD COORDINATOR 2 ON '127.0.0.1:10112'")
    execute_and_fetch_all(
        coordinator3_cursor, "REGISTER INSTANCE instance_1 ON '127.0.0.1:10011' WITH '127.0.0.1:10001'"
    )
    execute_and_fetch_all(
        coordinator3_cursor, "REGISTER INSTANCE instance_2 ON '127.0.0.1:10012' WITH '127.0.0.1:10002'"
    )
    execute_and_fetch_all(
        coordinator3_cursor, "REGISTER INSTANCE instance_3 ON '127.0.0.1:10013' WITH '127.0.0.1:10003'"
    )
    execute_and_fetch_all(coordinator3_cursor, "SET INSTANCE instance_3 TO MAIN")

    def check_coordinator3():
        return sorted(list(execute_and_fetch_all(coordinator3_cursor, "SHOW INSTANCES")))

    expected_cluster_coord3 = [
        ("coordinator_1", "127.0.0.1:10111", "", True, "coordinator"),
        ("coordinator_2", "127.0.0.1:10112", "", True, "coordinator"),
        ("coordinator_3", "127.0.0.1:10113", "", True, "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", True, "replica"),
        ("instance_2", "", "127.0.0.1:10012", True, "replica"),
        ("instance_3", "", "127.0.0.1:10013", True, "main"),
    ]
    mg_sleep_and_assert(expected_cluster_coord3, check_coordinator3)

    coordinator1_cursor = connect(host="localhost", port=7690).cursor()

    def check_coordinator1():
        return sorted(list(execute_and_fetch_all(coordinator1_cursor, "SHOW INSTANCES")))

    # TODO: (andi) This should be solved eventually
    expected_cluster_not_shared = [
        ("coordinator_1", "127.0.0.1:10111", "", True, "coordinator"),
        ("coordinator_2", "127.0.0.1:10112", "", True, "coordinator"),
        ("coordinator_3", "127.0.0.1:10113", "", True, "coordinator"),
    ]

    mg_sleep_and_assert(expected_cluster_not_shared, check_coordinator1)

    coordinator2_cursor = connect(host="localhost", port=7691).cursor()

    def check_coordinator2():
        return sorted(list(execute_and_fetch_all(coordinator2_cursor, "SHOW INSTANCES")))

    mg_sleep_and_assert(expected_cluster_not_shared, check_coordinator2)


def test_coordinators_communication_with_restarts():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    coordinator3_cursor = connect(host="localhost", port=7692).cursor()

    assert add_coordinator(coordinator3_cursor, "ADD COORDINATOR 1 ON '127.0.0.1:10111'")
    assert add_coordinator(coordinator3_cursor, "ADD COORDINATOR 2 ON '127.0.0.1:10112'")
    execute_and_fetch_all(
        coordinator3_cursor, "REGISTER INSTANCE instance_1 ON '127.0.0.1:10011' WITH '127.0.0.1:10001'"
    )
    execute_and_fetch_all(
        coordinator3_cursor, "REGISTER INSTANCE instance_2 ON '127.0.0.1:10012' WITH '127.0.0.1:10002'"
    )
    execute_and_fetch_all(
        coordinator3_cursor, "REGISTER INSTANCE instance_3 ON '127.0.0.1:10013' WITH '127.0.0.1:10003'"
    )
    execute_and_fetch_all(coordinator3_cursor, "SET INSTANCE instance_3 TO MAIN")

    expected_cluster_not_shared = [
        ("coordinator_1", "127.0.0.1:10111", "", True, "coordinator"),
        ("coordinator_2", "127.0.0.1:10112", "", True, "coordinator"),
        ("coordinator_3", "127.0.0.1:10113", "", True, "coordinator"),
    ]

    coordinator1_cursor = connect(host="localhost", port=7690).cursor()

    def check_coordinator1():
        return sorted(list(execute_and_fetch_all(coordinator1_cursor, "SHOW INSTANCES")))

    mg_sleep_and_assert(expected_cluster_not_shared, check_coordinator1)

    coordinator2_cursor = connect(host="localhost", port=7691).cursor()

    def check_coordinator2():
        return sorted(list(execute_and_fetch_all(coordinator2_cursor, "SHOW INSTANCES")))

    mg_sleep_and_assert(expected_cluster_not_shared, check_coordinator2)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_1")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_1")
    coordinator1_cursor = connect(host="localhost", port=7690).cursor()

    mg_sleep_and_assert(expected_cluster_not_shared, check_coordinator1)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_1")
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_2")

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_1")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_2")
    coordinator1_cursor = connect(host="localhost", port=7690).cursor()
    coordinator2_cursor = connect(host="localhost", port=7691).cursor()

    mg_sleep_and_assert(expected_cluster_not_shared, check_coordinator1)
    mg_sleep_and_assert(expected_cluster_not_shared, check_coordinator2)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
