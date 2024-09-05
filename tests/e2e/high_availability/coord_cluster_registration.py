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
import shutil
import sys
import tempfile

import interactive_mg_runner
import pytest
from common import (
    connect,
    execute_and_fetch_all,
    find_instance_and_assert_instances,
    ignore_elapsed_time_from_results,
    safe_execute,
    update_tuple_value,
)
from coord_cluster_registration_common import (
    TEMP_DIR,
    get_instances_description,
    get_instances_description_no_coord,
)
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))


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
    MEMGRAPH_INSTANCES_DESCRIPTION = get_instances_description(test_name="register_repl_instances_then_coordinators")
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)

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
    MEMGRAPH_INSTANCES_DESCRIPTION = get_instances_description(test_name="register_coordinator_then_repl_instances")
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)

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
    MEMGRAPH_INSTANCES_DESCRIPTION = get_instances_description(test_name="coordinators_communication_with_restarts")
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)

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

    coord3_leader_data = [
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

    mg_sleep_and_assert(coord3_leader_data, check_coordinator1)

    coordinator2_cursor = connect(host="localhost", port=7691).cursor()

    def check_coordinator2():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(coordinator2_cursor, "SHOW INSTANCES")))
        )

    mg_sleep_and_assert(coord3_leader_data, check_coordinator2)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_1")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_1")
    coordinator1_cursor = connect(host="localhost", port=7690).cursor()

    mg_sleep_and_assert(coord3_leader_data, check_coordinator1)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_1")
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_2")

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_1")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_2")
    coordinator1_cursor = connect(host="localhost", port=7690).cursor()
    coordinator2_cursor = connect(host="localhost", port=7691).cursor()

    leader_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    leader_name = find_instance_and_assert_instances(instance_role="leader", num_coordinators=3)

    leader_data = update_tuple_value(leader_data, leader_name, 0, -1, "leader")

    # After killing 2/3 of coordinators, leadership can change
    mg_sleep_and_assert(leader_data, check_coordinator1)
    mg_sleep_and_assert(leader_data, check_coordinator2)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
