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
from common import (
    connect,
    execute_and_fetch_all,
    ignore_elapsed_time_from_results,
    safe_execute,
)
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

TEMP_DIR = tempfile.TemporaryDirectory().name


def get_instances_description():
    return {
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
            "log_file": "high_availability/coord_cluster_registration/instance_1.log",
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
            "log_file": "high_availability/coord_cluster_registration/instance_2.log",
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
            "log_file": "high_availability/coord_cluster_registration/instance_3.log",
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
                "--coordinator-hostname=localhost",
                "--management-port=10121",
            ],
            "log_file": "high_availability/coord_cluster_registration/coordinator1.log",
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
                "--coordinator-hostname=localhost",
                "--management-port=10122",
            ],
            "log_file": "high_availability/coord_cluster_registration/coordinator2.log",
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
                "--coordinator-hostname=localhost",
                "--management-port=10123",
            ],
            "log_file": "high_availability/coord_cluster_registration/coordinator3.log",
            "setup_queries": [],
        },
    }


def get_instances_description_no_coord():
    return {
        "instance_1": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7687",
                "--log-level",
                "TRACE",
                "--management-port",
                "10011",
                "--data-recovery-on-startup=false",
                "--replication-restore-state-on-startup=true",
            ],
            "log_file": "high_availability/coord_cluster_registration/instance_1.log",
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
                "--data-recovery-on-startup=false",
                "--replication-restore-state-on-startup=true",
            ],
            "log_file": "high_availability/coord_cluster_registration/instance_2.log",
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
                "--data-recovery-on-startup=false",
                "--replication-restore-state-on-startup=true",
            ],
            "log_file": "high_availability/coord_cluster_registration/instance_3.log",
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
                "--coordinator-hostname=localhost",
                "--management-port=10121",
            ],
            "log_file": "high_availability/coord_cluster_registration/coordinator1.log",
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
                "--coordinator-hostname=localhost",
                "--management-port=10122",
            ],
            "log_file": "high_availability/coord_cluster_registration/coordinator2.log",
            "setup_queries": [],
        },
    }


def unset_env_flags():
    os.unsetenv("MEMGRAPH_EXPERIMENTAL_ENABLED")
    os.unsetenv("MEMGRAPH_COORDINATOR_PORT")
    os.unsetenv("MEMGRAPH_MANAGEMENT_PORT")
    os.unsetenv("MEMGRAPH_BOLT_PORT")
    os.unsetenv("MEMGRAPH_COORDINATOR_ID")
    os.unsetenv("MEMGRAPH_HA_CLUSTER_INIT_QUERIES")
    os.unsetenv("MEMGRAPH_COORDINATOR_HOSTNAME")


def test_register_repl_instances_then_coordinators():
    safe_execute(shutil.rmtree, TEMP_DIR)
    MEMGRAPH_INSTANCES_DESCRIPTION = get_instances_description()
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    coordinator3_cursor = connect(host="localhost", port=7692).cursor()

    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
    )
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

    def check_coordinator3():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator3_cursor, "SHOW INSTANCES")))
        )

    leader_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]
    mg_sleep_and_assert(leader_data, check_coordinator3)

    coordinator1_cursor = connect(host="localhost", port=7690).cursor()

    def check_coordinator1():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator1_cursor, "SHOW INSTANCES")))
        )

    mg_sleep_and_assert(leader_data, check_coordinator1)

    coordinator2_cursor = connect(host="localhost", port=7691).cursor()

    def check_coordinator2():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator2_cursor, "SHOW INSTANCES")))
        )

    mg_sleep_and_assert(leader_data, check_coordinator2)


def test_register_coordinator_then_repl_instances():
    safe_execute(shutil.rmtree, TEMP_DIR)
    MEMGRAPH_INSTANCES_DESCRIPTION = get_instances_description()
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    coordinator3_cursor = connect(host="localhost", port=7692).cursor()

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
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
    )
    execute_and_fetch_all(coordinator3_cursor, "SET INSTANCE instance_3 TO MAIN")

    def check_coordinator3():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator3_cursor, "SHOW INSTANCES")))
        )

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]
    mg_sleep_and_assert(data, check_coordinator3)

    coordinator1_cursor = connect(host="localhost", port=7690).cursor()

    def check_coordinator1():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator1_cursor, "SHOW INSTANCES")))
        )

    mg_sleep_and_assert(data, check_coordinator1)

    coordinator2_cursor = connect(host="localhost", port=7691).cursor()

    def check_coordinator2():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator2_cursor, "SHOW INSTANCES")))
        )

    mg_sleep_and_assert(data, check_coordinator2)


def test_coordinators_communication_with_restarts():
    # 1 Start all instances
    safe_execute(shutil.rmtree, TEMP_DIR)
    MEMGRAPH_INSTANCES_DESCRIPTION = get_instances_description()
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    coordinator3_cursor = connect(host="localhost", port=7692).cursor()

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
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
    )
    execute_and_fetch_all(coordinator3_cursor, "SET INSTANCE instance_3 TO MAIN")

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    coordinator1_cursor = connect(host="localhost", port=7690).cursor()

    def check_coordinator1():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator1_cursor, "SHOW INSTANCES")))
        )

    mg_sleep_and_assert(data, check_coordinator1)

    coordinator2_cursor = connect(host="localhost", port=7691).cursor()

    def check_coordinator2():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator2_cursor, "SHOW INSTANCES")))
        )

    mg_sleep_and_assert(data, check_coordinator2)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_1")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_1")
    coordinator1_cursor = connect(host="localhost", port=7690).cursor()

    mg_sleep_and_assert(data, check_coordinator1)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_1")
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_2")

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_1")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_2")
    coordinator1_cursor = connect(host="localhost", port=7690).cursor()
    coordinator2_cursor = connect(host="localhost", port=7691).cursor()

    mg_sleep_and_assert(data, check_coordinator1)
    mg_sleep_and_assert(data, check_coordinator2)


@pytest.mark.parametrize(
    "kill_instance",
    [True, False],
)
def test_unregister_replicas(kill_instance):
    safe_execute(shutil.rmtree, TEMP_DIR)
    MEMGRAPH_INSTANCES_DESCRIPTION = get_instances_description()
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    coordinator1_cursor = connect(host="localhost", port=7690).cursor()
    coordinator2_cursor = connect(host="localhost", port=7691).cursor()
    coordinator3_cursor = connect(host="localhost", port=7692).cursor()

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
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
    )
    execute_and_fetch_all(coordinator3_cursor, "SET INSTANCE instance_3 TO MAIN")

    def check_coordinator1():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator1_cursor, "SHOW INSTANCES")))
        )

    def check_coordinator2():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator2_cursor, "SHOW INSTANCES")))
        )

    def check_coordinator3():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator3_cursor, "SHOW INSTANCES")))
        )

    main_cursor = connect(host="localhost", port=7689).cursor()

    def check_main():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS")))

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    expected_replicas = [
        (
            "instance_1",
            "localhost:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "instance_2",
            "localhost:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]

    mg_sleep_and_assert(data, check_coordinator1)
    mg_sleep_and_assert(data, check_coordinator2)
    mg_sleep_and_assert(data, check_coordinator3)
    mg_sleep_and_assert(expected_replicas, check_main)

    if kill_instance:
        interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_1")
    execute_and_fetch_all(coordinator3_cursor, "UNREGISTER INSTANCE instance_1")

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    expected_replicas = [
        (
            "instance_2",
            "localhost:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]

    mg_sleep_and_assert(data, check_coordinator1)
    mg_sleep_and_assert(data, check_coordinator2)
    mg_sleep_and_assert(data, check_coordinator3)
    mg_sleep_and_assert(expected_replicas, check_main)

    if kill_instance:
        interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_2")
    execute_and_fetch_all(coordinator3_cursor, "UNREGISTER INSTANCE instance_2")

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    expected_replicas = []

    mg_sleep_and_assert(data, check_coordinator1)
    mg_sleep_and_assert(data, check_coordinator2)
    mg_sleep_and_assert(data, check_coordinator3)
    mg_sleep_and_assert(expected_replicas, check_main)


def test_unregister_main():
    safe_execute(shutil.rmtree, TEMP_DIR)
    MEMGRAPH_INSTANCES_DESCRIPTION = get_instances_description()
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    coordinator1_cursor = connect(host="localhost", port=7690).cursor()
    coordinator2_cursor = connect(host="localhost", port=7691).cursor()
    coordinator3_cursor = connect(host="localhost", port=7692).cursor()

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
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
    )
    execute_and_fetch_all(coordinator3_cursor, "SET INSTANCE instance_3 TO MAIN")

    def check_coordinator1():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator1_cursor, "SHOW INSTANCES")))
        )

    def check_coordinator2():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator2_cursor, "SHOW INSTANCES")))
        )

    def check_coordinator3():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator3_cursor, "SHOW INSTANCES")))
        )

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    mg_sleep_and_assert(data, check_coordinator1)
    mg_sleep_and_assert(data, check_coordinator2)
    mg_sleep_and_assert(data, check_coordinator3)

    try:
        execute_and_fetch_all(coordinator3_cursor, "UNREGISTER INSTANCE instance_3")
    except Exception as e:
        assert (
            str(e)
            == "Alive main instance can't be unregistered! Shut it down to trigger failover and then unregister it!"
        )

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "main"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "down", "unknown"),
    ]

    mg_sleep_and_assert(data, check_coordinator1)
    mg_sleep_and_assert(data, check_coordinator2)
    mg_sleep_and_assert(data, check_coordinator3)

    execute_and_fetch_all(coordinator3_cursor, "UNREGISTER INSTANCE instance_3")

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "main"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
    ]

    expected_replicas = [
        (
            "instance_2",
            "localhost:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]

    main_cursor = connect(host="localhost", port=7687).cursor()

    def check_main():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS")))

    mg_sleep_and_assert(data, check_coordinator1)
    mg_sleep_and_assert(data, check_coordinator2)
    mg_sleep_and_assert(data, check_coordinator3)
    mg_sleep_and_assert(expected_replicas, check_main)


def test_register_one_coord_with_env_vars():
    safe_execute(shutil.rmtree, TEMP_DIR)
    memgraph_instances_desc = get_instances_description_no_coord()
    interactive_mg_runner.start_all(memgraph_instances_desc)

    os.environ["MEMGRAPH_EXPERIMENTAL_ENABLED"] = "high-availability"
    os.environ["MEMGRAPH_COORDINATOR_PORT"] = "10113"
    os.environ["MEMGRAPH_BOLT_PORT"] = "7692"
    os.environ["MEMGRAPH_COORDINATOR_ID"] = "3"
    os.environ["MEMGRAPH_COORDINATOR_HOSTNAME"] = "localhost"
    os.environ["MEMGRAPH_COORDINATOR_HOSTNAME"] = "localhost"
    os.environ["MEMGRAPH_MANAGEMENT_PORT"] = "10123"

    file_path = os.path.join(TEMP_DIR, "ha_init.cypherl")
    os.environ["MEMGRAPH_HA_CLUSTER_INIT_QUERIES"] = file_path

    setup_queries = [
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'};",
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'};",
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
        "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
        "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
        "SET INSTANCE instance_3 TO MAIN",
    ]
    with open(file_path, "w") as writer:
        writer.writelines("\n".join(setup_queries))
    interactive_mg_runner.start(
        {
            "coordinator_3": {
                "args": [
                    "--log-level=TRACE",
                ],
                "log_file": "high_availability/coord_cluster_registration/coordinator3.log",
                "setup_queries": [],
                "default_bolt_port": 7692,
            },
        },
        "coordinator_3",
    )
    coordinator1_cursor = connect(host="localhost", port=7690).cursor()
    coordinator2_cursor = connect(host="localhost", port=7691).cursor()
    coordinator3_cursor = connect(host="localhost", port=7692).cursor()

    def check_coordinator1():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator1_cursor, "SHOW INSTANCES")))
        )

    def check_coordinator2():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator2_cursor, "SHOW INSTANCES")))
        )

    def check_coordinator3():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator3_cursor, "SHOW INSTANCES")))
        )

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    mg_sleep_and_assert(data, check_coordinator1)
    mg_sleep_and_assert(data, check_coordinator2)
    mg_sleep_and_assert(data, check_coordinator3)

    try:
        execute_and_fetch_all(coordinator3_cursor, "UNREGISTER INSTANCE instance_3")
    except Exception as e:
        assert (
            str(e)
            == "Alive main instance can't be unregistered! Shut it down to trigger failover and then unregister it!"
        )

    interactive_mg_runner.kill(memgraph_instances_desc, "instance_3")

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "main"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "down", "unknown"),
    ]

    mg_sleep_and_assert(data, check_coordinator1)
    mg_sleep_and_assert(data, check_coordinator2)
    mg_sleep_and_assert(data, check_coordinator3)

    execute_and_fetch_all(coordinator3_cursor, "UNREGISTER INSTANCE instance_3")

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "main"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
    ]

    expected_replicas = [
        (
            "instance_2",
            "localhost:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]

    main_cursor = connect(host="localhost", port=7687).cursor()

    def check_main():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS")))

    mg_sleep_and_assert(data, check_coordinator1)
    mg_sleep_and_assert(data, check_coordinator2)
    mg_sleep_and_assert(data, check_coordinator3)
    mg_sleep_and_assert(expected_replicas, check_main)

    unset_env_flags()


def test_register_one_data_with_env_vars():
    """
    This test checks that instance can be set with env flags

    1. Start other instances
    2. set env vars
    3. start last coord with setup
    4. check everything works
    """
    safe_execute(shutil.rmtree, TEMP_DIR)
    unset_env_flags()
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
                "--data-recovery-on-startup=false",
                "--replication-restore-state-on-startup=true",
            ],
            "log_file": "high_availability/coord_cluster_registration/instance_1.log",
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
                "--data-recovery-on-startup=false",
                "--replication-restore-state-on-startup=true",
            ],
            "log_file": "high_availability/coord_cluster_registration/instance_2.log",
            "data_directory": f"{TEMP_DIR}/instance_2",
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
                "--coordinator-hostname",
                "localhost",
                "--management-port=10121",
            ],
            "log_file": "high_availability/coord_cluster_registration/coordinator1.log",
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
                "--coordinator-hostname",
                "localhost",
                "--management-port=10122",
            ],
            "log_file": "high_availability/coord_cluster_registration/coordinator2.log",
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
                "--coordinator-hostname",
                "localhost",
                "--management-port=10123",
            ],
            "log_file": "high_availability/coord_cluster_registration/coordinator3.log",
            "setup_queries": [],
        },
    }
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)

    os.environ["MEMGRAPH_EXPERIMENTAL_ENABLED"] = "high-availability"
    os.environ["MEMGRAPH_BOLT_PORT"] = "7689"
    os.environ["MEMGRAPH_MANAGEMENT_PORT"] = "10013"

    interactive_mg_runner.start(
        {
            "instance_3": {
                "args": [
                    "--log-level",
                    "TRACE",
                    "--data-recovery-on-startup=false",
                    "--replication-restore-state-on-startup=true",
                ],
                "log_file": "high_availability/coord_cluster_registration/instance_3.log",
                "data_directory": f"{TEMP_DIR}/instance_3",
                "setup_queries": [],
                "default_bolt_port": 7689,
            },
        },
        "instance_3",
    )

    coordinator1_cursor = connect(host="localhost", port=7690).cursor()
    coordinator2_cursor = connect(host="localhost", port=7691).cursor()
    coordinator3_cursor = connect(host="localhost", port=7692).cursor()

    setup_queries = [
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server':'localhost:10121'};",
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server':'localhost:10122'};",
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
        "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
        "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
        "SET INSTANCE instance_3 TO MAIN",
    ]
    for query in setup_queries:
        execute_and_fetch_all(coordinator3_cursor, query)

    def check_coordinator1():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator1_cursor, "SHOW INSTANCES")))
        )

    def check_coordinator2():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator2_cursor, "SHOW INSTANCES")))
        )

    def check_coordinator3():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator3_cursor, "SHOW INSTANCES")))
        )

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    mg_sleep_and_assert(data, check_coordinator1)
    mg_sleep_and_assert(data, check_coordinator2)
    mg_sleep_and_assert(data, check_coordinator3)

    main_cursor = connect(host="localhost", port=7689).cursor()
    execute_and_fetch_all(main_cursor, "CREATE (n:Node {name: 'node'})")

    replica_2_cursor = connect(host="localhost", port=7688).cursor()

    def get_vertex_count():
        return execute_and_fetch_all(replica_2_cursor, "MATCH (n) RETURN count(n)")[0][0]

    mg_sleep_and_assert(1, get_vertex_count)

    replica_3_cursor = connect(host="localhost", port=7687).cursor()

    def get_vertex_count():
        return execute_and_fetch_all(replica_3_cursor, "MATCH (n) RETURN count(n)")[0][0]

    mg_sleep_and_assert(1, get_vertex_count)

    unset_env_flags()
    interactive_mg_runner.stop_all()


def test_register_one_coord_with_env_vars_no_instances_alive_on_start():
    # This test checks that there are no errors when starting coordinator
    # in case other instances are not set up yet.
    # On setting instances and resetting coordinator everything should be set up

    # 1. Start coordinator without all data instances
    # 2. Check that it doesn't crash
    # 3. Stop coord
    # 4. Start all other instances
    # 5. Again start coord which should use env setup
    # 6. Check everything works

    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.stop_all()
    memgraph_instances_desc = get_instances_description_no_coord()

    os.environ["MEMGRAPH_EXPERIMENTAL_ENABLED"] = "high-availability"
    os.environ["MEMGRAPH_COORDINATOR_PORT"] = "10113"
    os.environ["MEMGRAPH_BOLT_PORT"] = "7692"
    os.environ["MEMGRAPH_COORDINATOR_ID"] = "3"
    os.environ["MEMGRAPH_COORDINATOR_HOSTNAME"] = "localhost"
    os.environ["MEMGRAPH_MANAGEMENT_PORT"] = "10123"
    safe_execute(os.mkdir, TEMP_DIR)
    file_path = os.path.join(TEMP_DIR, "ha_init.cypherl")
    os.environ["MEMGRAPH_HA_CLUSTER_INIT_QUERIES"] = file_path

    setup_queries = [
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'};",
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'};",
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
        "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
        "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
        "SET INSTANCE instance_3 TO MAIN",
    ]
    with open(file_path, "w") as writer:
        writer.writelines("\n".join(setup_queries))

    coordinator_3_description = {
        "coordinator_3": {
            "args": [
                "--log-level=TRACE",
            ],
            "log_file": "high_availability/coord_cluster_registration/coordinator3.log",
            "setup_queries": [],
            "default_bolt_port": 7692,
        },
    }

    interactive_mg_runner.start(
        coordinator_3_description,
        "coordinator_3",
    )
    coordinator3_cursor = connect(host="localhost", port=7692).cursor()

    expected_cluster = [("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader")]

    def check_coordinator3():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator3_cursor, "SHOW INSTANCES")))
        )

    mg_sleep_and_assert(expected_cluster, check_coordinator3)

    interactive_mg_runner.kill(coordinator_3_description, "coordinator_3")

    # intentionally unset flags here because other instances might read them
    unset_env_flags()

    interactive_mg_runner.start_all(memgraph_instances_desc)
    coordinator1_cursor = connect(host="localhost", port=7690).cursor()
    coordinator2_cursor = connect(host="localhost", port=7691).cursor()

    os.environ["MEMGRAPH_EXPERIMENTAL_ENABLED"] = "high-availability"
    os.environ["MEMGRAPH_COORDINATOR_PORT"] = "10113"
    os.environ["MEMGRAPH_BOLT_PORT"] = "7692"
    os.environ["MEMGRAPH_COORDINATOR_ID"] = "3"
    os.environ["MEMGRAPH_HA_CLUSTER_INIT_QUERIES"] = file_path
    os.environ["MEMGRAPH_COORDINATOR_HOSTNAME"] = "localhost"
    os.environ["MEMGRAPH_MANAGEMENT_PORT"] = "10123"

    interactive_mg_runner.start(
        coordinator_3_description,
        "coordinator_3",
    )
    coordinator3_cursor = connect(host="localhost", port=7692).cursor()

    def check_coordinator1():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator1_cursor, "SHOW INSTANCES")))
        )

    def check_coordinator2():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator2_cursor, "SHOW INSTANCES")))
        )

    def check_coordinator3():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator3_cursor, "SHOW INSTANCES")))
        )

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    mg_sleep_and_assert(data, check_coordinator1)
    mg_sleep_and_assert(data, check_coordinator2)
    mg_sleep_and_assert(data, check_coordinator3)

    try:
        execute_and_fetch_all(coordinator3_cursor, "UNREGISTER INSTANCE instance_3")
    except Exception as e:
        assert (
            str(e)
            == "Alive main instance can't be unregistered! Shut it down to trigger failover and then unregister it!"
        )

    interactive_mg_runner.kill(memgraph_instances_desc, "instance_3")

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "main"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "down", "unknown"),
    ]

    mg_sleep_and_assert(data, check_coordinator1)
    mg_sleep_and_assert(data, check_coordinator2)
    mg_sleep_and_assert(data, check_coordinator3)

    execute_and_fetch_all(coordinator3_cursor, "UNREGISTER INSTANCE instance_3")

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "main"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
    ]

    expected_replicas = [
        (
            "instance_2",
            "localhost:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]

    main_cursor = connect(host="localhost", port=7687).cursor()

    def check_main():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS")))

    mg_sleep_and_assert(data, check_coordinator1)
    mg_sleep_and_assert(data, check_coordinator2)
    mg_sleep_and_assert(data, check_coordinator3)
    mg_sleep_and_assert(expected_replicas, check_main)

    unset_env_flags()


def test_add_coord_instance_fails():
    interactive_mg_runner.stop_all()
    safe_execute(shutil.rmtree, TEMP_DIR)
    memgraph_instances_description_all = get_instances_description()
    interactive_mg_runner.start_all(memgraph_instances_description_all)

    coordinator3_cursor = connect(host="localhost", port=7692).cursor()

    execute_and_fetch_all(
        coordinator3_cursor,
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
    )

    try:
        execute_and_fetch_all(
            coordinator3_cursor,
            "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10121'}",
        )
    except Exception as e:
        assert "Couldn't add coordinator since instance with such id already exists!" == str(e)

    try:
        execute_and_fetch_all(
            coordinator3_cursor,
            "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
        )
    except Exception as e:
        assert "Couldn't add coordinator since instance with such bolt endpoint already exists!" == str(e)

    try:
        execute_and_fetch_all(
            coordinator3_cursor,
            "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10122'}",
        )
    except Exception as e:
        assert "Couldn't add coordinator since instance with such coordinator server already exists!" == str(e)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
