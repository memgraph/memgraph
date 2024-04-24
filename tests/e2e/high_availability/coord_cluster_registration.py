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
            "--experimental-enabled=high-availability",
            "--bolt-port",
            "7687",
            "--log-level",
            "TRACE",
            "--management-port",
            "10011",
        ],
        "log_file": "instance_1.log",
        "data_directory": f"{TEMP_DIR}/instance_1",
        "setup_queries": [],
    },
    "instance_2": {
        "args": [
            "--experimental-enabled=high-availability",
            "--bolt-port",
            "7688",
            "--log-level",
            "TRACE",
            "--management-port",
            "10012",
        ],
        "log_file": "instance_2.log",
        "data_directory": f"{TEMP_DIR}/instance_2",
        "setup_queries": [],
    },
    "instance_3": {
        "args": [
            "--experimental-enabled=high-availability",
            "--bolt-port",
            "7689",
            "--log-level",
            "TRACE",
            "--management-port",
            "10013",
        ],
        "log_file": "instance_3.log",
        "data_directory": f"{TEMP_DIR}/instance_3",
        "setup_queries": [],
    },
    "coordinator_1": {
        "args": [
            "--experimental-enabled=high-availability",
            "--bolt-port",
            "7690",
            "--log-level=TRACE",
            "--coordinator-id=1",
            "--coordinator-port=10111",
        ],
        "log_file": "coordinator1.log",
        "setup_queries": [],
    },
    "coordinator_2": {
        "args": [
            "--experimental-enabled=high-availability",
            "--bolt-port",
            "7691",
            "--log-level=TRACE",
            "--coordinator-id=2",
            "--coordinator-port=10112",
        ],
        "log_file": "coordinator2.log",
        "setup_queries": [],
    },
    "coordinator_3": {
        "args": [
            "--experimental-enabled=high-availability",
            "--bolt-port",
            "7692",
            "--log-level=TRACE",
            "--coordinator-id=3",
            "--coordinator-port=10113",
        ],
        "log_file": "coordinator3.log",
        "setup_queries": [],
    },
}


def test_register_repl_instances_then_coordinators():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    coordinator3_cursor = connect(host="localhost", port=7692).cursor()

    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': '127.0.0.1:7687', 'management_server': '127.0.0.1:10011', 'replication_server': '127.0.0.1:10001'};",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': '127.0.0.1:7688', 'management_server': '127.0.0.1:10012', 'replication_server': '127.0.0.1:10002'};",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': '127.0.0.1:7689', 'management_server': '127.0.0.1:10013', 'replication_server': '127.0.0.1:10003'};",
    )
    execute_and_fetch_all(coordinator3_cursor, "SET INSTANCE instance_3 TO MAIN")
    execute_and_fetch_all(
        coordinator3_cursor,
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': '127.0.0.1:7690', 'coordinator_server': '127.0.0.1:10111'}",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': '127.0.0.1:7691', 'coordinator_server': '127.0.0.1:10112'}",
    )

    def check_coordinator3():
        return sorted(list(execute_and_fetch_all(coordinator3_cursor, "SHOW INSTANCES")))

    expected_cluster_coord3 = [
        ("coordinator_1", "127.0.0.1:7690", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:7691", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "0.0.0.0:7692", "0.0.0.0:10113", "", "unknown", "coordinator"),
        ("instance_1", "127.0.0.1:7687", "", "127.0.0.1:10011", "up", "replica"),
        ("instance_2", "127.0.0.1:7688", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "127.0.0.1:7689", "", "127.0.0.1:10013", "up", "main"),
    ]
    mg_sleep_and_assert(expected_cluster_coord3, check_coordinator3)

    coordinator1_cursor = connect(host="localhost", port=7690).cursor()

    def check_coordinator1():
        return sorted(list(execute_and_fetch_all(coordinator1_cursor, "SHOW INSTANCES")))

    expected_cluster_shared = [
        ("coordinator_1", "127.0.0.1:7690", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:7691", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "0.0.0.0:7692", "0.0.0.0:10113", "", "unknown", "coordinator"),
        ("instance_1", "127.0.0.1:7687", "", "", "unknown", "replica"),
        ("instance_2", "127.0.0.1:7688", "", "", "unknown", "replica"),
        ("instance_3", "127.0.0.1:7689", "", "", "unknown", "main"),
    ]

    mg_sleep_and_assert(expected_cluster_shared, check_coordinator1)

    coordinator2_cursor = connect(host="localhost", port=7691).cursor()

    def check_coordinator2():
        return sorted(list(execute_and_fetch_all(coordinator2_cursor, "SHOW INSTANCES")))

    mg_sleep_and_assert(expected_cluster_shared, check_coordinator2)


def test_register_coordinator_then_repl_instances():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    coordinator3_cursor = connect(host="localhost", port=7692).cursor()

    execute_and_fetch_all(
        coordinator3_cursor,
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': '127.0.0.1:7690', 'coordinator_server': '127.0.0.1:10111'}",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': '127.0.0.1:7691', 'coordinator_server': '127.0.0.1:10112'}",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': '127.0.0.1:7687', 'management_server': '127.0.0.1:10011', 'replication_server': '127.0.0.1:10001'};",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': '127.0.0.1:7688', 'management_server': '127.0.0.1:10012', 'replication_server': '127.0.0.1:10002'};",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': '127.0.0.1:7689', 'management_server': '127.0.0.1:10013', 'replication_server': '127.0.0.1:10003'};",
    )
    execute_and_fetch_all(coordinator3_cursor, "SET INSTANCE instance_3 TO MAIN")

    def check_coordinator3():
        return sorted(list(execute_and_fetch_all(coordinator3_cursor, "SHOW INSTANCES")))

    expected_cluster_coord3 = [
        ("coordinator_1", "127.0.0.1:7690", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:7691", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "0.0.0.0:7692", "0.0.0.0:10113", "", "unknown", "coordinator"),
        ("instance_1", "127.0.0.1:7687", "", "127.0.0.1:10011", "up", "replica"),
        ("instance_2", "127.0.0.1:7688", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "127.0.0.1:7689", "", "127.0.0.1:10013", "up", "main"),
    ]
    mg_sleep_and_assert(expected_cluster_coord3, check_coordinator3)

    coordinator1_cursor = connect(host="localhost", port=7690).cursor()

    def check_coordinator1():
        return sorted(list(execute_and_fetch_all(coordinator1_cursor, "SHOW INSTANCES")))

    expected_cluster_shared = [
        ("coordinator_1", "127.0.0.1:7690", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:7691", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "0.0.0.0:7692", "0.0.0.0:10113", "", "unknown", "coordinator"),
        ("instance_1", "127.0.0.1:7687", "", "", "unknown", "replica"),
        ("instance_2", "127.0.0.1:7688", "", "", "unknown", "replica"),
        ("instance_3", "127.0.0.1:7689", "", "", "unknown", "main"),
    ]

    mg_sleep_and_assert(expected_cluster_shared, check_coordinator1)

    coordinator2_cursor = connect(host="localhost", port=7691).cursor()

    def check_coordinator2():
        return sorted(list(execute_and_fetch_all(coordinator2_cursor, "SHOW INSTANCES")))

    mg_sleep_and_assert(expected_cluster_shared, check_coordinator2)


def test_coordinators_communication_with_restarts():
    # 1 Start all instances
    safe_execute(shutil.rmtree, TEMP_DIR)

    # 1
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    coordinator3_cursor = connect(host="localhost", port=7692).cursor()

    execute_and_fetch_all(
        coordinator3_cursor,
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': '127.0.0.1:7690', 'coordinator_server': '127.0.0.1:10111'}",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': '127.0.0.1:7691', 'coordinator_server': '127.0.0.1:10112'}",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': '127.0.0.1:7687', 'management_server': '127.0.0.1:10011', 'replication_server': '127.0.0.1:10001'};",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': '127.0.0.1:7688', 'management_server': '127.0.0.1:10012', 'replication_server': '127.0.0.1:10002'};",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': '127.0.0.1:7689', 'management_server': '127.0.0.1:10013', 'replication_server': '127.0.0.1:10003'};",
    )
    execute_and_fetch_all(coordinator3_cursor, "SET INSTANCE instance_3 TO MAIN")

    expected_cluster_shared = [
        ("coordinator_1", "127.0.0.1:7690", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:7691", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "0.0.0.0:7692", "0.0.0.0:10113", "", "unknown", "coordinator"),
        ("instance_1", "127.0.0.1:7687", "", "", "unknown", "replica"),
        ("instance_2", "127.0.0.1:7688", "", "", "unknown", "replica"),
        ("instance_3", "127.0.0.1:7689", "", "", "unknown", "main"),
    ]

    coordinator1_cursor = connect(host="localhost", port=7690).cursor()

    def check_coordinator1():
        return sorted(list(execute_and_fetch_all(coordinator1_cursor, "SHOW INSTANCES")))

    mg_sleep_and_assert(expected_cluster_shared, check_coordinator1)

    coordinator2_cursor = connect(host="localhost", port=7691).cursor()

    def check_coordinator2():
        return sorted(list(execute_and_fetch_all(coordinator2_cursor, "SHOW INSTANCES")))

    mg_sleep_and_assert(expected_cluster_shared, check_coordinator2)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_1")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_1")
    coordinator1_cursor = connect(host="localhost", port=7690).cursor()

    mg_sleep_and_assert(expected_cluster_shared, check_coordinator1)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_1")
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_2")

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_1")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_2")
    coordinator1_cursor = connect(host="localhost", port=7690).cursor()
    coordinator2_cursor = connect(host="localhost", port=7691).cursor()

    mg_sleep_and_assert(expected_cluster_shared, check_coordinator1)
    mg_sleep_and_assert(expected_cluster_shared, check_coordinator2)


# TODO: (andi) Test when dealing with distributed coordinators that you can register on one coordinator and unregister from any other coordinator
@pytest.mark.parametrize(
    "kill_instance",
    [True, False],
)
def test_unregister_replicas(kill_instance):
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    coordinator1_cursor = connect(host="localhost", port=7690).cursor()
    coordinator2_cursor = connect(host="localhost", port=7691).cursor()
    coordinator3_cursor = connect(host="localhost", port=7692).cursor()

    execute_and_fetch_all(
        coordinator3_cursor,
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': '127.0.0.1:7690', 'coordinator_server': '127.0.0.1:10111'}",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': '127.0.0.1:7691', 'coordinator_server': '127.0.0.1:10112'}",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': '127.0.0.1:7687', 'management_server': '127.0.0.1:10011', 'replication_server': '127.0.0.1:10001'};",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': '127.0.0.1:7688', 'management_server': '127.0.0.1:10012', 'replication_server': '127.0.0.1:10002'};",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': '127.0.0.1:7689', 'management_server': '127.0.0.1:10013', 'replication_server': '127.0.0.1:10003'};",
    )
    execute_and_fetch_all(coordinator3_cursor, "SET INSTANCE instance_3 TO MAIN")

    def check_coordinator1():
        return sorted(list(execute_and_fetch_all(coordinator1_cursor, "SHOW INSTANCES")))

    def check_coordinator2():
        return sorted(list(execute_and_fetch_all(coordinator2_cursor, "SHOW INSTANCES")))

    def check_coordinator3():
        return sorted(list(execute_and_fetch_all(coordinator3_cursor, "SHOW INSTANCES")))

    main_cursor = connect(host="localhost", port=7689).cursor()

    def check_main():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS")))

    expected_cluster = [
        ("coordinator_1", "127.0.0.1:7690", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:7691", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "0.0.0.0:7692", "0.0.0.0:10113", "", "unknown", "coordinator"),
        ("instance_1", "127.0.0.1:7687", "", "127.0.0.1:10011", "up", "replica"),
        ("instance_2", "127.0.0.1:7688", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "127.0.0.1:7689", "", "127.0.0.1:10013", "up", "main"),
    ]

    expected_cluster_shared = [
        ("coordinator_1", "127.0.0.1:7690", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:7691", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "0.0.0.0:7692", "0.0.0.0:10113", "", "unknown", "coordinator"),
        ("instance_1", "127.0.0.1:7687", "", "", "unknown", "replica"),
        ("instance_2", "127.0.0.1:7688", "", "", "unknown", "replica"),
        ("instance_3", "127.0.0.1:7689", "", "", "unknown", "main"),
    ]

    expected_replicas = [
        (
            "instance_1",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "instance_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]

    mg_sleep_and_assert(expected_cluster_shared, check_coordinator1)
    mg_sleep_and_assert(expected_cluster_shared, check_coordinator2)
    mg_sleep_and_assert(expected_cluster, check_coordinator3)
    mg_sleep_and_assert(expected_replicas, check_main)

    if kill_instance:
        interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_1")
    execute_and_fetch_all(coordinator3_cursor, "UNREGISTER INSTANCE instance_1")

    expected_cluster = [
        ("coordinator_1", "127.0.0.1:7690", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:7691", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "0.0.0.0:7692", "0.0.0.0:10113", "", "unknown", "coordinator"),
        ("instance_2", "127.0.0.1:7688", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "127.0.0.1:7689", "", "127.0.0.1:10013", "up", "main"),
    ]

    expected_cluster_shared = [
        ("coordinator_1", "127.0.0.1:7690", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:7691", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "0.0.0.0:7692", "0.0.0.0:10113", "", "unknown", "coordinator"),
        ("instance_2", "127.0.0.1:7688", "", "", "unknown", "replica"),
        ("instance_3", "127.0.0.1:7689", "", "", "unknown", "main"),
    ]

    expected_replicas = [
        (
            "instance_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]

    mg_sleep_and_assert(expected_cluster_shared, check_coordinator1)
    mg_sleep_and_assert(expected_cluster_shared, check_coordinator2)
    mg_sleep_and_assert(expected_cluster, check_coordinator3)
    mg_sleep_and_assert(expected_replicas, check_main)

    if kill_instance:
        interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_2")
    execute_and_fetch_all(coordinator3_cursor, "UNREGISTER INSTANCE instance_2")

    expected_cluster = [
        ("coordinator_1", "127.0.0.1:7690", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:7691", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "0.0.0.0:7692", "0.0.0.0:10113", "", "unknown", "coordinator"),
        ("instance_3", "127.0.0.1:7689", "", "127.0.0.1:10013", "up", "main"),
    ]

    expected_cluster_shared = [
        ("coordinator_1", "127.0.0.1:7690", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:7691", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "0.0.0.0:7692", "0.0.0.0:10113", "", "unknown", "coordinator"),
        ("instance_3", "127.0.0.1:7689", "", "", "unknown", "main"),
    ]
    expected_replicas = []

    mg_sleep_and_assert(expected_cluster_shared, check_coordinator1)
    mg_sleep_and_assert(expected_cluster_shared, check_coordinator2)
    mg_sleep_and_assert(expected_cluster, check_coordinator3)
    mg_sleep_and_assert(expected_replicas, check_main)


def test_unregister_main():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    coordinator1_cursor = connect(host="localhost", port=7690).cursor()
    coordinator2_cursor = connect(host="localhost", port=7691).cursor()
    coordinator3_cursor = connect(host="localhost", port=7692).cursor()

    execute_and_fetch_all(
        coordinator3_cursor,
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': '127.0.0.1:7690', 'coordinator_server': '127.0.0.1:10111'}",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': '127.0.0.1:7691', 'coordinator_server': '127.0.0.1:10112'}",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': '127.0.0.1:7687', 'management_server': '127.0.0.1:10011', 'replication_server': '127.0.0.1:10001'};",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': '127.0.0.1:7688', 'management_server': '127.0.0.1:10012', 'replication_server': '127.0.0.1:10002'};",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': '127.0.0.1:7689', 'management_server': '127.0.0.1:10013', 'replication_server': '127.0.0.1:10003'};",
    )
    execute_and_fetch_all(coordinator3_cursor, "SET INSTANCE instance_3 TO MAIN")

    def check_coordinator1():
        return sorted(list(execute_and_fetch_all(coordinator1_cursor, "SHOW INSTANCES")))

    def check_coordinator2():
        return sorted(list(execute_and_fetch_all(coordinator2_cursor, "SHOW INSTANCES")))

    def check_coordinator3():
        return sorted(list(execute_and_fetch_all(coordinator3_cursor, "SHOW INSTANCES")))

    expected_cluster = [
        ("coordinator_1", "127.0.0.1:7690", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:7691", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "0.0.0.0:7692", "0.0.0.0:10113", "", "unknown", "coordinator"),
        ("instance_1", "127.0.0.1:7687", "", "127.0.0.1:10011", "up", "replica"),
        ("instance_2", "127.0.0.1:7688", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "127.0.0.1:7689", "", "127.0.0.1:10013", "up", "main"),
    ]

    expected_cluster_shared = [
        ("coordinator_1", "127.0.0.1:7690", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:7691", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "0.0.0.0:7692", "0.0.0.0:10113", "", "unknown", "coordinator"),
        ("instance_1", "127.0.0.1:7687", "", "", "unknown", "replica"),
        ("instance_2", "127.0.0.1:7688", "", "", "unknown", "replica"),
        ("instance_3", "127.0.0.1:7689", "", "", "unknown", "main"),
    ]

    mg_sleep_and_assert(expected_cluster_shared, check_coordinator1)
    mg_sleep_and_assert(expected_cluster_shared, check_coordinator2)
    mg_sleep_and_assert(expected_cluster, check_coordinator3)

    try:
        execute_and_fetch_all(coordinator3_cursor, "UNREGISTER INSTANCE instance_3")
    except Exception as e:
        assert (
            str(e)
            == "Alive main instance can't be unregistered! Shut it down to trigger failover and then unregister it!"
        )

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    expected_cluster = [
        ("coordinator_1", "127.0.0.1:7690", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:7691", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "0.0.0.0:7692", "0.0.0.0:10113", "", "unknown", "coordinator"),
        ("instance_1", "127.0.0.1:7687", "", "127.0.0.1:10011", "up", "main"),
        ("instance_2", "127.0.0.1:7688", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "127.0.0.1:7689", "", "127.0.0.1:10013", "down", "unknown"),
    ]

    expected_cluster_shared = [
        ("coordinator_1", "127.0.0.1:7690", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:7691", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "0.0.0.0:7692", "0.0.0.0:10113", "", "unknown", "coordinator"),
        ("instance_1", "127.0.0.1:7687", "", "", "unknown", "main"),
        ("instance_2", "127.0.0.1:7688", "", "", "unknown", "replica"),
        ("instance_3", "127.0.0.1:7689", "", "", "unknown", "unknown"),
    ]

    mg_sleep_and_assert(expected_cluster_shared, check_coordinator1)
    mg_sleep_and_assert(expected_cluster_shared, check_coordinator2)
    mg_sleep_and_assert(expected_cluster, check_coordinator3)

    execute_and_fetch_all(coordinator3_cursor, "UNREGISTER INSTANCE instance_3")

    expected_cluster = [
        ("coordinator_1", "127.0.0.1:7690", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:7691", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "0.0.0.0:7692", "0.0.0.0:10113", "", "unknown", "coordinator"),
        ("instance_1", "127.0.0.1:7687", "", "127.0.0.1:10011", "up", "main"),
        ("instance_2", "127.0.0.1:7688", "", "127.0.0.1:10012", "up", "replica"),
    ]

    expected_cluster_shared = [
        ("coordinator_1", "127.0.0.1:7690", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:7691", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "0.0.0.0:7692", "0.0.0.0:10113", "", "unknown", "coordinator"),
        ("instance_1", "127.0.0.1:7687", "", "", "unknown", "main"),
        ("instance_2", "127.0.0.1:7688", "", "", "unknown", "replica"),
    ]

    expected_replicas = [
        (
            "instance_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]

    main_cursor = connect(host="localhost", port=7687).cursor()

    def check_main():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS")))

    mg_sleep_and_assert(expected_cluster_shared, check_coordinator1)
    mg_sleep_and_assert(expected_cluster_shared, check_coordinator2)
    mg_sleep_and_assert(expected_cluster, check_coordinator3)
    mg_sleep_and_assert(expected_replicas, check_main)


def test_add_coord_instance_fails():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    coordinator3_cursor = connect(host="localhost", port=7692).cursor()

    execute_and_fetch_all(
        coordinator3_cursor,
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': '127.0.0.1:7690', 'coordinator_server': '127.0.0.1:10111'}",
    )

    try:
        execute_and_fetch_all(
            coordinator3_cursor,
            "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': '127.0.0.1:7691', 'coordinator_server': '127.0.0.1:10112'}",
        )
    except Exception as e:
        assert "Couldn't add coordinator since instance with such id already exists!" == str(e)

    try:
        execute_and_fetch_all(
            coordinator3_cursor,
            "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': '127.0.0.1:7690', 'coordinator_server': '127.0.0.1:10112'}",
        )
    except Exception as e:
        assert "Couldn't add coordinator since instance with such bolt endpoint already exists!" == str(e)

    try:
        execute_and_fetch_all(
            coordinator3_cursor,
            "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': '127.0.0.1:7691', 'coordinator_server': '127.0.0.1:10111'}",
        )
    except Exception as e:
        assert "Couldn't add coordinator since instance with such coordinator server already exists!" == str(e)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
