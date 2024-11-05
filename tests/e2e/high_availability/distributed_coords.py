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

import concurrent.futures
import os
import sys
import time

import interactive_mg_runner
import pytest
from common import (
    connect,
    execute_and_fetch_all,
    find_instance_and_assert_instances,
    get_data_path,
    get_logs_path,
    ignore_elapsed_time_from_results,
    update_tuple_value,
)
from mg_utils import (
    mg_assert_until,
    mg_sleep_and_assert,
    mg_sleep_and_assert_collection,
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

file = "distributed_coords"


@pytest.fixture
def test_name(request):
    return request.node.name


def get_instances_description_no_setup(test_name: str, use_durability: bool = True):
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
            "log_file": f"{get_logs_path(file, test_name)}/instance_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_1",
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
            "log_file": f"{get_logs_path(file, test_name)}/instance_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_2",
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
            "log_file": f"{get_logs_path(file, test_name)}/instance_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_3",
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
                "--management-port=10121",
                f"--ha_durability={use_durability}",
                "--coordinator-hostname",
                "localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_1",
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
                "--management-port=10122",
                f"--ha_durability={use_durability}",
                "--coordinator-hostname",
                "localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_2",
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
                "--management-port=10123",
                f"--ha_durability={use_durability}",
                "--coordinator-hostname",
                "localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_3",
            "setup_queries": [],
        },
    }


def get_default_setup_queries():
    return [
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
        "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
        "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
        "SET INSTANCE instance_3 TO MAIN",
    ]


def get_instances_description_no_setup_4_coords(test_name: str, use_durability: bool = True):
    use_durability_str = "true" if use_durability else "false"
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
            "log_file": f"{get_logs_path(file, test_name)}/instance_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_1",
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
            "log_file": f"{get_logs_path(file, test_name)}/instance_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_2",
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
            "log_file": f"{get_logs_path(file, test_name)}/instance_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_3",
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
                f"--ha_durability={use_durability_str}",
                "--coordinator-hostname",
                "localhost",
                "--management-port=10121",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_1",
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
                f"--ha_durability={use_durability_str}",
                "--coordinator-hostname",
                "localhost",
                "--management-port=10122",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_2",
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
                f"--ha_durability={use_durability_str}",
                "--coordinator-hostname",
                "localhost",
                "--management-port=10123",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_3",
            "setup_queries": [],
        },
        "coordinator_4": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7693",
                "--log-level=TRACE",
                "--coordinator-id=4",
                "--coordinator-port=10114",
                f"--ha_durability={use_durability_str}",
                "--coordinator-hostname",
                "localhost",
                "--management-port=10124",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_4.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_4",
            "setup_queries": [],
        },
    }


@pytest.fixture(autouse=True)
def cleanup_after_test():
    # Run the test
    yield
    # Stop + delete directories after running the test
    interactive_mg_runner.kill_all(keep_directories=False)


def test_leadership_change(test_name):
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)

    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()
    for query in get_default_setup_queries():
        execute_and_fetch_all(coord_cursor_3, query)

    interactive_mg_runner.kill(inner_instances_description, "coordinator_3")

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;"))))

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    def show_instances_coord2():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_2, "SHOW INSTANCES;"))))

    leader_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "down", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    leader_name = find_instance_and_assert_instances(
        instance_role="leader", num_coordinators=3, coord_ids_to_skip_validation={3}
    )

    leader_data = update_tuple_value(leader_data, leader_name, 0, -1, "leader")

    mg_sleep_and_assert(leader_data, show_instances_coord1, time_between_attempt=3)
    mg_sleep_and_assert(leader_data, show_instances_coord2, time_between_attempt=3)

    interactive_mg_runner.start(inner_instances_description, "coordinator_3")


@pytest.mark.parametrize("use_durability", [True, False])
def test_even_number_coords(use_durability, test_name):
    # Goal is to check that nothing gets broken on even number of coords when 2 coords are down
    # 1. Start all instances.
    # 2. Check that all instances are up and that one of the instances is a main.
    # 3. Demote the main instance.
    # 4. Kill two coordinators.
    # 5. Wait for leader to become follower
    # 6. Bring back up two coordinators.
    # 7. Find leader
    # 8.

    # 1
    inner_instances_description = get_instances_description_no_setup_4_coords(
        test_name=test_name, use_durability=use_durability
    )

    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    setup_queries = [
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
        "ADD COORDINATOR 4 WITH CONFIG {'bolt_server': 'localhost:7693', 'coordinator_server': 'localhost:10114', 'management_server': 'localhost:10124'}",
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
        "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
        "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
        "SET INSTANCE instance_3 TO MAIN",
    ]

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()
    for query in setup_queries:
        execute_and_fetch_all(coord_cursor_3, query)

    # 2
    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;"))))

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    def show_instances_coord2():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_2, "SHOW INSTANCES;"))))

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()

    def show_instances_coord3():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_3, "SHOW INSTANCES;"))))

    coord_cursor_4 = connect(host="localhost", port=7693).cursor()

    def show_instances_coord4():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_4, "SHOW INSTANCES;"))))

    leader_data_original = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("coordinator_4", "localhost:7693", "localhost:10114", "localhost:10124", "up", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    mg_sleep_and_assert(leader_data_original, show_instances_coord3)
    mg_sleep_and_assert(leader_data_original, show_instances_coord1)
    mg_sleep_and_assert(leader_data_original, show_instances_coord2)
    mg_sleep_and_assert(leader_data_original, show_instances_coord4)

    instance_3_cursor = connect(host="localhost", port=7689).cursor()

    def show_replicas():
        return sorted(list(execute_and_fetch_all(instance_3_cursor, "SHOW REPLICAS;")))

    replicas = [
        (
            "instance_1",
            "localhost:10001",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
        (
            "instance_2",
            "localhost:10002",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
    ]
    mg_sleep_and_assert_collection(replicas, show_replicas)

    # 3

    execute_and_fetch_all(
        coord_cursor_3,
        "DEMOTE INSTANCE instance_3",
    )

    leader_data_demoted = leader_data_original.copy()
    leader_data_demoted = update_tuple_value(leader_data_demoted, "instance_3", 0, -1, "replica")

    mg_sleep_and_assert(leader_data_demoted, show_instances_coord3)
    mg_sleep_and_assert(leader_data_demoted, show_instances_coord1)
    mg_sleep_and_assert(leader_data_demoted, show_instances_coord2)
    mg_sleep_and_assert(leader_data_demoted, show_instances_coord4)

    mg_sleep_and_assert_until_role_change(
        lambda: execute_and_fetch_all(instance_3_cursor, "SHOW REPLICATION ROLE;")[0][0], "replica"
    )

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(instance_3_cursor, "SHOW REPLICAS;")
    assert str(e.value) == "Replica can't show registered replicas (it shouldn't have any)!"

    # 4
    interactive_mg_runner.kill(inner_instances_description, "coordinator_1")
    interactive_mg_runner.kill(inner_instances_description, "coordinator_2")

    # 5

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(coord_cursor_3, "SET INSTANCE instance_3 TO MAIN;")

    assert "Couldn't promote instance since raft server couldn't append the log!" in str(e.value)

    follower_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "unknown", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "unknown", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "unknown", "follower"),
        ("coordinator_4", "localhost:7693", "localhost:10114", "localhost:10124", "unknown", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "unknown", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "unknown", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "unknown", "replica"),
    ]

    mg_sleep_and_assert(follower_data, show_instances_coord3)

    # 6
    interactive_mg_runner.start(inner_instances_description, "coordinator_1")
    interactive_mg_runner.start(inner_instances_description, "coordinator_2")

    # 7

    leader_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "follower"),
        ("coordinator_4", "localhost:7693", "localhost:10114", "localhost:10124", "up", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    leader_coord_instance_3_demoted = find_instance_and_assert_instances(instance_role="leader", num_coordinators=3)

    assert leader_coord_instance_3_demoted is not None, "Leader not found"

    leader_data = update_tuple_value(leader_data, leader_coord_instance_3_demoted, 0, -1, "leader")

    port_mappings = {
        "coordinator_1": 7690,
        "coordinator_2": 7691,
        "coordinator_3": 7692,
        "coordinator_4": 7693,
    }

    for _, port in port_mappings.items():
        coord_cursor = connect(host="localhost", port=port).cursor()

        def show_instances():
            return ignore_elapsed_time_from_results(
                sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))
            )

        mg_sleep_and_assert(leader_data, show_instances)


def test_old_main_comes_back_on_new_leader_as_replica(test_name):
    # 1. Start all instances.
    # 2. Kill the main instance
    # 3. Kill the leader
    # 4. Start the old main instance
    # 5. Run SHOW INSTANCES on the new leader and check that the old main instance is registered as a replica
    # 6. Start again previous leader

    inner_instances_description = get_instances_description_no_setup(test_name=test_name)

    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()
    for query in get_default_setup_queries():
        execute_and_fetch_all(coord_cursor_3, query)

    interactive_mg_runner.kill(inner_instances_description, "coordinator_3")
    interactive_mg_runner.kill(inner_instances_description, "instance_3")

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;"))))

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    def show_instances_coord2():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_2, "SHOW INSTANCES;"))))

    # Wait until failover happens
    wait_for_status_change(show_instances_coord1, {"instance_1", "instance_2"}, "main")
    wait_for_status_change(show_instances_coord1, {"instance_3"}, "unknown")

    # Both instance_1 and instance_2 could become main depending on the order of pings in the system.
    # Both coordinator_1 and coordinator_2 could become leader depending on the NuRaft election.
    leader_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "down", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "down", "unknown"),
    ]

    leader_instance_3_down = find_instance_and_assert_instances(
        instance_role="leader", num_coordinators=3, coord_ids_to_skip_validation={3}
    )
    main_instance_3_down = find_instance_and_assert_instances(
        instance_role="main", num_coordinators=3, coord_ids_to_skip_validation={3}
    )

    leader_data = update_tuple_value(leader_data, leader_instance_3_down, 0, -1, "leader")
    leader_data = update_tuple_value(leader_data, main_instance_3_down, 0, -1, "main")

    def get_show_instances_to_coord(leader_instance):
        show_instances_mapping = {
            "coordinator_1": show_instances_coord1,
            "coordinator_2": show_instances_coord2,
        }

        return show_instances_mapping.get(leader_instance)

    all_live_coords = ["coordinator_1", "coordinator_2"]

    for coord in all_live_coords:
        mg_sleep_and_assert(leader_data, get_show_instances_to_coord(coord))

    interactive_mg_runner.start(inner_instances_description, "instance_3")

    coordinator_leader_instance = find_instance_and_assert_instances(
        instance_role="leader", num_coordinators=3, coord_ids_to_skip_validation={3}
    )
    main_instance_id_instance_3_start = find_instance_and_assert_instances(
        instance_role="main", num_coordinators=3, coord_ids_to_skip_validation={3}
    )

    assert (
        main_instance_id_instance_3_start == main_instance_3_down
    ), f"Main is not {main_instance_3_down} but {main_instance_id_instance_3_start}"
    assert (
        coordinator_leader_instance == leader_instance_3_down
    ), f"Leader is not same as before {leader_instance_3_down}, but {coordinator_leader_instance}"

    coord_leader_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "down", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "replica"),
    ]

    coord_leader_data = update_tuple_value(coord_leader_data, coordinator_leader_instance, 0, -1, "leader")
    coord_leader_data = update_tuple_value(coord_leader_data, main_instance_id_instance_3_start, 0, -1, "main")

    mg_sleep_and_assert(coord_leader_data, get_show_instances_to_coord(coordinator_leader_instance))

    def connect_to_main_instance():
        assert main_instance_id_instance_3_start is not None

        port_mapping = {"instance_1": 7687, "instance_2": 7688, "instance_3": 7689}

        assert (
            main_instance_id_instance_3_start in port_mapping
        ), f"Main is not in mappings, but main is {main_instance_id_instance_3_start}"

        main_instance_port = port_mapping.get(main_instance_id_instance_3_start)

        if main_instance_port is not None:
            return connect(host="localhost", port=main_instance_port).cursor()

        return None

    new_main_cursor = connect_to_main_instance()
    assert new_main_cursor is not None, "Main cursor is not found!"

    def show_replicas():
        return sorted(list(execute_and_fetch_all(new_main_cursor, "SHOW REPLICAS;")))

    replicas = [
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
        (
            "instance_3",
            "localhost:10003",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    replicas = [replica for replica in replicas if replica[0] != main_instance_id_instance_3_start]
    mg_sleep_and_assert_collection(replicas, show_replicas)

    execute_and_fetch_all(new_main_cursor, "CREATE (n:Node {name: 'node'})")

    replica_2_cursor = connect(host="localhost", port=7688).cursor()

    def get_vertex_count():
        return execute_and_fetch_all(replica_2_cursor, "MATCH (n) RETURN count(n)")[0][0]

    mg_sleep_and_assert(1, get_vertex_count)

    replica_3_cursor = connect(host="localhost", port=7689).cursor()

    def get_vertex_count():
        return execute_and_fetch_all(replica_3_cursor, "MATCH (n) RETURN count(n)")[0][0]

    mg_sleep_and_assert(1, get_vertex_count)

    interactive_mg_runner.start(inner_instances_description, "coordinator_3")


def test_distributed_automatic_failover(test_name):
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)

    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()
    for query in get_default_setup_queries():
        execute_and_fetch_all(coord_cursor_3, query)

    main_cursor = connect(host="localhost", port=7689).cursor()
    expected_data_on_main = [
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

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))

    mg_sleep_and_assert_collection(expected_data_on_main, retrieve_data_show_replicas)

    interactive_mg_runner.kill(inner_instances_description, "instance_3")

    coord_cursor = connect(host="localhost", port=7692).cursor()

    def retrieve_data_show_repl_cluster():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;"))))

    expected_data_on_coord = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "main"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "down", "unknown"),
    ]

    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_repl_cluster)

    new_main_cursor = connect(host="localhost", port=7687).cursor()

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(new_main_cursor, "SHOW REPLICAS;")))

    expected_data_on_new_main = [
        (
            "instance_2",
            "localhost:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "instance_3",
            "localhost:10003",
            "sync",
            {"ts": 0, "behind": None, "status": "invalid"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "invalid"}},
        ),
    ]

    mg_sleep_and_assert_until_role_change(
        lambda: execute_and_fetch_all(new_main_cursor, "SHOW REPLICATION ROLE;")[0][0], "main"
    )
    mg_sleep_and_assert_collection(expected_data_on_new_main, retrieve_data_show_replicas)

    interactive_mg_runner.start(inner_instances_description, "instance_3")
    expected_data_on_new_main_old_alive = [
        (
            "instance_2",
            "localhost:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "instance_3",
            "localhost:10003",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]

    mg_sleep_and_assert_collection(expected_data_on_new_main_old_alive, retrieve_data_show_replicas)


def test_distributed_automatic_failover_with_leadership_change(test_name):
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)

    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()
    for query in get_default_setup_queries():
        execute_and_fetch_all(coord_cursor_3, query)

    interactive_mg_runner.kill(inner_instances_description, "coordinator_3")
    interactive_mg_runner.kill(inner_instances_description, "instance_3")

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;"))))

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    def show_instances_coord2():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_2, "SHOW INSTANCES;"))))

    leader_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "down", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "down", "unknown"),
    ]

    wait_for_status_change(show_instances_coord1, {"instance_1", "instance_2"}, "main")

    leader_name = find_instance_and_assert_instances(
        instance_role="leader", num_coordinators=3, coord_ids_to_skip_validation={3}
    )
    main_name = find_instance_and_assert_instances(
        instance_role="main", num_coordinators=3, coord_ids_to_skip_validation={3}
    )

    leader_data = update_tuple_value(leader_data, main_name, 0, -1, "main")
    leader_data = update_tuple_value(leader_data, leader_name, 0, -1, "leader")

    mg_sleep_and_assert(leader_data, show_instances_coord1)
    mg_sleep_and_assert(leader_data, show_instances_coord2)

    def connect_to_main_instance():
        main_instance_name = main_name
        assert main_instance_name is not None

        port_mapping = {"instance_1": 7687, "instance_2": 7688}

        assert main_instance_name in port_mapping, f"Main is not in mappings, but main is {main_instance_name}"

        main_instance_port = port_mapping.get(main_instance_name)

        if main_instance_port is not None:
            return connect(host="localhost", port=main_instance_port).cursor()

        return None

    new_main_cursor = connect_to_main_instance()
    assert new_main_cursor is not None, "Main cursor is not found!"

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(new_main_cursor, "SHOW REPLICAS;")))

    all_possible_states = [
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
        (
            "instance_3",
            "localhost:10003",
            "sync",
            {"ts": 0, "behind": None, "status": "invalid"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "invalid"}},
        ),
    ]

    mg_sleep_and_assert_until_role_change(
        lambda: execute_and_fetch_all(new_main_cursor, "SHOW REPLICATION ROLE;")[0][0], "main"
    )
    expected_data_on_new_main = [state for state in all_possible_states if state[0] != main_name]
    mg_sleep_and_assert_collection(expected_data_on_new_main, retrieve_data_show_replicas)

    interactive_mg_runner.start(inner_instances_description, "instance_3")

    all_possible_states = [
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
        (
            "instance_3",
            "localhost:10003",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    expected_data_on_new_main_old_alive = [state for state in all_possible_states if state[0] != main_name]
    mg_sleep_and_assert_collection(expected_data_on_new_main_old_alive, retrieve_data_show_replicas)

    interactive_mg_runner.start(inner_instances_description, "coordinator_3")


def test_no_leader_after_leader_and_follower_die(test_name):
    # 1. Register all but one replication instance on the first leader.
    # 2. Kill the leader and a follower.
    # 3. Check that the remaining follower is not promoted to leader by trying to register remaining replication instance.

    inner_memgraph_instances = get_instances_description_no_setup(test_name=test_name)
    interactive_mg_runner.start_all(inner_memgraph_instances, keep_directories=False)

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()

    for query in get_default_setup_queries():
        execute_and_fetch_all(coord_cursor_3, query)

    interactive_mg_runner.kill(inner_memgraph_instances, "coordinator_3")
    interactive_mg_runner.kill(inner_memgraph_instances, "coordinator_2")

    coord_1_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "unknown", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "unknown", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "unknown", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "unknown", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "unknown", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "unknown", "main"),
    ]

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;"))))

    mg_sleep_and_assert(coord_1_data, show_instances_coord1)

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(
            coord_cursor_1,
            "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
        )
        assert "Couldn't register replica instance since coordinator is not a leader!" in str(e)


def test_old_main_comes_back_on_new_leader_as_main(test_name):
    # 1. Start all instances.
    # 2. Kill all instances
    # 3. Kill the leader
    # 4. Start the old main instance
    # 5. Run SHOW INSTANCES on the new leader and check that the old main instance is main once again

    inner_memgraph_instances = get_instances_description_no_setup(test_name=test_name)
    interactive_mg_runner.start_all(inner_memgraph_instances, keep_directories=False)

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()

    for query in get_default_setup_queries():
        execute_and_fetch_all(coord_cursor_3, query)

    interactive_mg_runner.kill(inner_memgraph_instances, "instance_1")
    interactive_mg_runner.kill(inner_memgraph_instances, "instance_2")
    interactive_mg_runner.kill(inner_memgraph_instances, "instance_3")
    interactive_mg_runner.kill(inner_memgraph_instances, "coordinator_3")

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;"))))

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    def show_instances_coord2():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_2, "SHOW INSTANCES;"))))

    interactive_mg_runner.start(inner_memgraph_instances, "instance_3")

    coord1_leader_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "down", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "down", "unknown"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "down", "unknown"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    coord2_leader_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "leader"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "down", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "down", "unknown"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "down", "unknown"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    mg_sleep_and_assert_multiple(
        [coord1_leader_data, coord2_leader_data], [show_instances_coord1, show_instances_coord2]
    )
    mg_sleep_and_assert_multiple(
        [coord1_leader_data, coord2_leader_data], [show_instances_coord1, show_instances_coord2]
    )

    interactive_mg_runner.start(inner_memgraph_instances, "instance_1")
    interactive_mg_runner.start(inner_memgraph_instances, "instance_2")

    new_main_cursor = connect(host="localhost", port=7689).cursor()

    def show_replicas():
        return sorted(list(execute_and_fetch_all(new_main_cursor, "SHOW REPLICAS;")))

    replicas = [
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
    mg_sleep_and_assert_collection(replicas, show_replicas)

    execute_and_fetch_all(new_main_cursor, "CREATE (n:Node {name: 'node'})")

    replica_1_cursor = connect(host="localhost", port=7687).cursor()
    assert len(execute_and_fetch_all(replica_1_cursor, "MATCH (n) RETURN n;")) == 1

    replica_2_cursor = connect(host="localhost", port=7688).cursor()
    assert len(execute_and_fetch_all(replica_2_cursor, "MATCH (n) RETURN n;")) == 1

    interactive_mg_runner.start(inner_memgraph_instances, "coordinator_3")


def test_registering_4_coords(test_name):
    # Goal of this test is to assure registering of multiple coordinators in row works
    INSTANCES_DESCRIPTION = {
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
            "log_file": f"{get_logs_path(file, test_name)}/instance_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_1",
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
            "log_file": f"{get_logs_path(file, test_name)}/instance_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_2",
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
            "log_file": f"{get_logs_path(file, test_name)}/instance_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_3",
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
                "--management-port=10121",
                "--coordinator-hostname=localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_1",
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
                "--management-port=10122",
                "--coordinator-hostname=localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_2",
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
                "--management-port=10123",
                "--coordinator-hostname=localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_3",
            "setup_queries": [],
        },
        "coordinator_4": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7693",
                "--log-level=TRACE",
                "--coordinator-id=4",
                "--coordinator-port=10114",
                "--management-port=10124",
                "--coordinator-hostname=localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_4.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_4",
            "setup_queries": [
                "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
                "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
                "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}",
                "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
                "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
                "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
                "SET INSTANCE instance_3 TO MAIN",
            ],
        },
    }

    interactive_mg_runner.start_all(INSTANCES_DESCRIPTION, keep_directories=False)

    coord_cursor = connect(host="localhost", port=7693).cursor()

    def retrieve_data_show_repl_cluster():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;"))))

    expected_data_on_coord = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "follower"),
        ("coordinator_4", "localhost:7693", "localhost:10114", "localhost:10124", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_repl_cluster)


def test_registering_coord_log_store(test_name):
    # Goal of this test is to assure registering a bunch of instances and de-registering works properly
    # w.r.t nuRaft log
    # 1. Start basic instances # 3 logs
    # 2. Check all is there
    # 3. Create 3 additional instances and add them to cluster # 3 logs -> 1st snapshot
    # 4. Check everything is there
    # 5. Set main # 1 log
    # 6. Check correct state
    # 7. Drop 2 new instances # 2 logs
    # 8. Check correct state
    # 9. Drop 1 new instance # 1 log -> 2nd snapshot
    # 10. Check correct state

    INSTANCES_DESCRIPTION = {
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
            "log_file": f"{get_logs_path(file, test_name)}/instance_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_1",
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
            "log_file": f"{get_logs_path(file, test_name)}/instance_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_2",
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
            "log_file": f"{get_logs_path(file, test_name)}/instance_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_3",
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
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_1",
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
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_2",
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
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_3",
            "setup_queries": [],
        },
        "coordinator_4": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7693",
                "--log-level=TRACE",
                "--coordinator-id=4",
                "--coordinator-port=10114",
                "--coordinator-hostname",
                "localhost",
                "--management-port=10124",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_4.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_4",
            "setup_queries": [
                "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
                "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
                "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113',  'management_server': 'localhost:10123'}",
                "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
                "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
                "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
            ],
        },
    }
    assert "SET INSTANCE instance_3 TO MAIN" not in INSTANCES_DESCRIPTION["coordinator_4"]["setup_queries"]

    # 1
    interactive_mg_runner.start_all(INSTANCES_DESCRIPTION, keep_directories=False)

    # 2
    coord_cursor = connect(host="localhost", port=7693).cursor()

    def retrieve_data_show_repl_cluster():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;"))))

    coordinators = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "follower"),
        ("coordinator_4", "localhost:7693", "localhost:10114", "localhost:10124", "up", "leader"),
    ]

    basic_instances = [
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "replica"),
    ]

    expected_data_on_coord = []
    expected_data_on_coord.extend(coordinators)
    expected_data_on_coord.extend(basic_instances)

    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_repl_cluster)

    # 3
    instances_ports_added = [10011, 10012, 10013]
    bolt_port_id = 7700
    manag_port_id = 10014

    additional_instances = []
    for i in range(4, 7):
        instance_name = f"instance_{i}"
        args_desc = [
            "--experimental-enabled=high-availability",
            "--log-level=TRACE",
        ]

        bolt_port = f"--bolt-port={bolt_port_id}"

        manag_server_port = f"--management-port={manag_port_id}"

        args_desc.append(bolt_port)
        args_desc.append(manag_server_port)

        instance_description = {
            "args": args_desc,
            "log_file": f"{get_logs_path(file, test_name)}/instance_{i}.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_{i}",
            "setup_queries": [],
        }

        full_instance_desc = {instance_name: instance_description}
        interactive_mg_runner.start(full_instance_desc, instance_name)
        repl_port_id = manag_port_id - 10
        assert repl_port_id < 10011, "Wrong test setup, repl port must be smaller than smallest coord port id"

        bolt_server = f"localhost:{bolt_port_id}"
        management_server = f"localhost:{manag_port_id}"
        repl_server = f"localhost:{repl_port_id}"

        config_str = f"{{'bolt_server': '{bolt_server}', 'management_server': '{management_server}', 'replication_server': '{repl_server}'}}"

        execute_and_fetch_all(
            coord_cursor,
            f"REGISTER INSTANCE {instance_name} WITH CONFIG {config_str}",
        )

        additional_instances.append((f"{instance_name}", bolt_server, "", management_server, "up", "replica"))
        instances_ports_added.append(manag_port_id)
        manag_port_id += 1
        bolt_port_id += 1

    # 4
    expected_data_on_coord.extend(additional_instances)

    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_repl_cluster)

    # 5
    execute_and_fetch_all(coord_cursor, "SET INSTANCE instance_3 TO MAIN")

    # 6
    basic_instances.pop()
    basic_instances.append(("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"))

    new_expected_data_on_coordinator = []

    new_expected_data_on_coordinator.extend(coordinators)
    new_expected_data_on_coordinator.extend(basic_instances)
    new_expected_data_on_coordinator.extend(additional_instances)

    mg_sleep_and_assert(new_expected_data_on_coordinator, retrieve_data_show_repl_cluster)

    # 7
    for i in range(6, 4, -1):
        execute_and_fetch_all(coord_cursor, f"UNREGISTER INSTANCE instance_{i};")
        additional_instances.pop()

    new_expected_data_on_coordinator = []
    new_expected_data_on_coordinator.extend(coordinators)
    new_expected_data_on_coordinator.extend(basic_instances)
    new_expected_data_on_coordinator.extend(additional_instances)

    # 8
    mg_sleep_and_assert(new_expected_data_on_coordinator, retrieve_data_show_repl_cluster)

    # 9

    new_expected_data_on_coordinator = []
    new_expected_data_on_coordinator.extend(coordinators)
    new_expected_data_on_coordinator.extend(basic_instances)

    execute_and_fetch_all(coord_cursor, f"UNREGISTER INSTANCE instance_4;")

    # 10
    mg_sleep_and_assert(new_expected_data_on_coordinator, retrieve_data_show_repl_cluster)


def test_multiple_failovers_in_row_no_leadership_change(test_name):
    # Goal of this test is to assure multiple failovers in row work without leadership change
    # 1. Start basic instances
    # 2. Check all is there
    # 3. Kill MAIN (instance_3)
    # 4. Expect failover (instance_1)
    # 5. Kill instance_1
    # 6. Expect failover instance_2
    # 7. Start instance_3
    # 8. Expect instance_3 and instance_2 (MAIN) up
    # 9. Kill instance_2
    # 10. Expect instance_3 MAIN
    # 11. Write some data on instance_3
    # 12. Start instance_2 and instance_1
    # 13. Expect instance_1 and instance2 to be up and cluster to have correct state
    # 13. Expect data to be replicated

    # 1
    inner_memgraph_instances = get_instances_description_no_setup(test_name=test_name)
    interactive_mg_runner.start_all(inner_memgraph_instances, keep_directories=False)

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()

    for query in get_default_setup_queries():
        execute_and_fetch_all(coord_cursor_3, query)

    # 2

    def get_func_show_instances(cursor):
        def show_instances_follower_coord():
            return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(cursor, "SHOW INSTANCES;"))))

        return show_instances_follower_coord

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()
    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    mg_sleep_and_assert_collection(data, get_func_show_instances(coord_cursor_1))
    mg_sleep_and_assert_collection(data, get_func_show_instances(coord_cursor_2))
    mg_sleep_and_assert_collection(data, get_func_show_instances(coord_cursor_3))

    # 3

    interactive_mg_runner.kill(inner_memgraph_instances, "instance_3")

    # 4

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "main"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "down", "unknown"),
    ]

    mg_sleep_and_assert_collection(data, get_func_show_instances(coord_cursor_1))
    mg_sleep_and_assert_collection(data, get_func_show_instances(coord_cursor_2))
    mg_sleep_and_assert_collection(data, get_func_show_instances(coord_cursor_3))

    # 5
    interactive_mg_runner.kill(inner_memgraph_instances, "instance_1")

    # 6
    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "down", "unknown"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "main"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "down", "unknown"),
    ]

    mg_sleep_and_assert_collection(data, get_func_show_instances(coord_cursor_1))
    mg_sleep_and_assert_collection(data, get_func_show_instances(coord_cursor_2))
    mg_sleep_and_assert_collection(data, get_func_show_instances(coord_cursor_3))

    # 7

    interactive_mg_runner.start(inner_memgraph_instances, "instance_3")

    # 8

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "down", "unknown"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "main"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "replica"),
    ]

    mg_sleep_and_assert_collection(data, get_func_show_instances(coord_cursor_1))
    mg_sleep_and_assert_collection(data, get_func_show_instances(coord_cursor_2))
    mg_sleep_and_assert_collection(data, get_func_show_instances(coord_cursor_3))

    # 9
    interactive_mg_runner.kill(inner_memgraph_instances, "instance_2")

    # 10
    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "down", "unknown"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "down", "unknown"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    mg_sleep_and_assert_collection(data, get_func_show_instances(coord_cursor_1))
    mg_sleep_and_assert_collection(data, get_func_show_instances(coord_cursor_2))
    mg_sleep_and_assert_collection(data, get_func_show_instances(coord_cursor_3))

    # 11

    instance_3_cursor = connect(port=7689, host="localhost").cursor()

    mg_sleep_and_assert_until_role_change(
        lambda: execute_and_fetch_all(instance_3_cursor, "SHOW REPLICATION ROLE;")[0][0], "main"
    )

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(instance_3_cursor, "CREATE ();")
    assert "At least one SYNC replica has not confirmed committing last transaction." in str(e.value)

    # 12
    interactive_mg_runner.start(inner_memgraph_instances, "instance_1")
    interactive_mg_runner.start(inner_memgraph_instances, "instance_2")

    # 13
    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    mg_sleep_and_assert_collection(data, get_func_show_instances(coord_cursor_1))
    mg_sleep_and_assert_collection(data, get_func_show_instances(coord_cursor_2))
    mg_sleep_and_assert_collection(data, get_func_show_instances(coord_cursor_3))

    # 14.

    def show_replicas():
        return sorted(list(execute_and_fetch_all(instance_3_cursor, "SHOW REPLICAS;")))

    replicas = [
        (
            "instance_1",
            "localhost:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 2, "behind": 0, "status": "ready"}},
        ),
        (
            "instance_2",
            "localhost:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 2, "behind": 0, "status": "ready"}},
        ),
    ]
    mg_sleep_and_assert_collection(replicas, show_replicas)

    def get_vertex_count_func(cursor):
        def get_vertex_count():
            return execute_and_fetch_all(cursor, "MATCH (n) RETURN count(n)")[0][0]

        return get_vertex_count

    mg_sleep_and_assert(1, get_vertex_count_func(connect(port=7687, host="localhost").cursor()))

    mg_sleep_and_assert(1, get_vertex_count_func(connect(port=7688, host="localhost").cursor()))


def test_multiple_old_mains_single_failover(test_name):
    # Goal of this test is to check when leadership changes
    # and we have old MAIN down, that we don't start failover
    # 1. Start all instances.
    # 2. Kill the main instance
    # 3. Do failover
    # 4. Kill other main
    # 5. Kill leader
    # 6. Leave first main down, and start second main
    # 7. Second main should write data to new instance all the time

    # 1
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)

    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()
    for query in get_default_setup_queries():
        execute_and_fetch_all(coord_cursor_3, query)

    def retrieve_data_show_repl_cluster():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_3, "SHOW INSTANCES;"))))

    coordinators = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
    ]

    basic_instances = [
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    expected_data_on_coord = []
    expected_data_on_coord.extend(coordinators)
    expected_data_on_coord.extend(basic_instances)

    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_repl_cluster)

    # 2

    interactive_mg_runner.kill(inner_instances_description, "instance_3")

    # 3

    basic_instances = [
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "main"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "down", "unknown"),
    ]

    expected_data_on_coord = []
    expected_data_on_coord.extend(coordinators)
    expected_data_on_coord.extend(basic_instances)

    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_repl_cluster)

    # 4

    interactive_mg_runner.kill(inner_instances_description, "instance_1")

    # 5
    interactive_mg_runner.kill(inner_instances_description, "coordinator_3")

    # 6

    interactive_mg_runner.start(inner_instances_description, "instance_1")

    # 7

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;"))))

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    def show_instances_coord2():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_2, "SHOW INSTANCES;"))))

    coord1_leader_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "down", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "main"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "down", "unknown"),
    ]

    coord2_leader_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "leader"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "down", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "main"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "down", "unknown"),
    ]

    mg_sleep_and_assert_multiple(
        [coord1_leader_data, coord2_leader_data], [show_instances_coord1, show_instances_coord2]
    )
    mg_sleep_and_assert_multiple(
        [coord1_leader_data, coord2_leader_data], [show_instances_coord1, show_instances_coord2]
    )

    instance_1_cursor = connect(host="localhost", port=7687).cursor()

    def show_replicas():
        return sorted(list(execute_and_fetch_all(instance_1_cursor, "SHOW REPLICAS;")))

    replicas = [
        (
            "instance_2",
            "localhost:10002",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
        (
            "instance_3",
            "localhost:10003",
            "sync",
            {"behind": None, "status": "invalid", "ts": 0},
            {"memgraph": {"behind": 0, "status": "invalid", "ts": 0}},
        ),
    ]
    mg_sleep_and_assert_collection(replicas, show_replicas)

    def get_vertex_count_func(cursor):
        def get_vertex_count():
            return execute_and_fetch_all(cursor, "MATCH (n) RETURN count(n)")[0][0]

        return get_vertex_count

    vertex_count = 0
    instance_1_cursor = connect(port=7687, host="localhost").cursor()
    instance_2_cursor = connect(port=7688, host="localhost").cursor()

    mg_sleep_and_assert(vertex_count, get_vertex_count_func(instance_1_cursor))
    mg_sleep_and_assert(vertex_count, get_vertex_count_func(instance_2_cursor))

    time_slept = 0
    failover_time = 5
    while time_slept < failover_time:
        with pytest.raises(Exception):
            execute_and_fetch_all(instance_1_cursor, "CREATE ();")
        vertex_count += 1

        assert vertex_count == execute_and_fetch_all(instance_1_cursor, "MATCH (n) RETURN count(n);")[0][0]
        assert vertex_count == execute_and_fetch_all(instance_2_cursor, "MATCH (n) RETURN count(n);")[0][0]
        time.sleep(0.1)
        time_slept += 0.1


def test_force_reset_works_after_failed_registration(test_name):
    # Goal of this test is to check that force reset works after failed registration
    # 1. Start all instances.
    # 2. Check everything works correctly
    # 3. Try register instance which doesn't exist
    # 4. Enter force reset
    # 5. Check that everything works correctly

    # 1
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)

    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()
    for query in get_default_setup_queries():
        execute_and_fetch_all(coord_cursor_3, query)

    # 2

    def show_instances_coord3():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_3, "SHOW INSTANCES;"))))

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;"))))

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    def show_instances_coord2():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_2, "SHOW INSTANCES;"))))

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    mg_sleep_and_assert(data, show_instances_coord3)
    mg_sleep_and_assert(data, show_instances_coord1)
    mg_sleep_and_assert(data, show_instances_coord2)

    instance_3_cursor = connect(host="localhost", port=7689).cursor()

    def show_replicas():
        return sorted(list(execute_and_fetch_all(instance_3_cursor, "SHOW REPLICAS;")))

    replicas = [
        (
            "instance_1",
            "localhost:10001",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
        (
            "instance_2",
            "localhost:10002",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
    ]
    mg_sleep_and_assert_collection(replicas, show_replicas)

    def get_vertex_count_func(cursor):
        def get_vertex_count():
            return execute_and_fetch_all(cursor, "MATCH (n) RETURN count(n)")[0][0]

        return get_vertex_count

    vertex_count = 0
    instance_1_cursor = connect(port=7687, host="localhost").cursor()
    instance_2_cursor = connect(port=7688, host="localhost").cursor()

    mg_sleep_and_assert(vertex_count, get_vertex_count_func(instance_1_cursor))
    mg_sleep_and_assert(vertex_count, get_vertex_count_func(instance_2_cursor))

    with pytest.raises(Exception):
        execute_and_fetch_all(
            coord_cursor_3,
            "REGISTER INSTANCE instance_4 WITH CONFIG {'bolt_server': 'localhost:7680', 'management_server': 'localhost:10050', 'replication_server': 'localhost:10051'};",
        )

    # This will trigger force reset and choosing of new instance as MAIN
    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "replica"),
    ]

    leader_name = find_instance_and_assert_instances(instance_role="leader", num_coordinators=3)

    main_name = find_instance_and_assert_instances(instance_role="main", num_coordinators=3)

    assert leader_name is not None, "leader is None"
    assert main_name is not None, "Main is None"

    data = update_tuple_value(data, leader_name, 0, -1, "leader")
    data = update_tuple_value(data, main_name, 0, -1, "main")

    mg_sleep_and_assert(data, show_instances_coord3)
    mg_sleep_and_assert(data, show_instances_coord1)
    mg_sleep_and_assert(data, show_instances_coord2)

    def get_port(instance_name):
        mappings = {
            "instance_1": 7687,
            "instance_2": 7688,
            "instance_3": 7689,
        }
        return mappings[instance_name]

    instance_main_cursor = connect(port=get_port(main_name), host="localhost").cursor()
    vertex_count = 10
    for _ in range(vertex_count):
        execute_and_fetch_all(instance_main_cursor, "CREATE ();")

    def get_vertex_count_func(cursor):
        def get_vertex_count():
            return execute_and_fetch_all(cursor, "MATCH (n) RETURN count(n)")[0][0]

        return get_vertex_count

    for instance in ["instance_1", "instance_2", "instance_3"]:
        cursor = connect(port=get_port(instance), host="localhost").cursor()
        mg_sleep_and_assert(vertex_count, get_vertex_count_func(cursor))


def test_force_reset_works_after_failed_registration_and_replica_down(test_name):
    # Goal of this test is to check when action fails, that force reset happens
    # and everything works correctly when REPLICA is down (can be demoted but doesn't have to - we demote it)
    # 1. Start all instances.
    # 2. Check everything works correctly
    # 3. Kill replica
    # 4. Try to register instance which doesn't exist
    # 4. Enter force reset
    # 5. Check that everything works correctly with two instances
    # 6. Start replica instance
    # 7. Check that replica is correctly demoted to replica

    # 1
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)

    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()
    for query in get_default_setup_queries():
        execute_and_fetch_all(coord_cursor_3, query)

    # 2

    def show_instances_coord3():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_3, "SHOW INSTANCES;"))))

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;"))))

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    def show_instances_coord2():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_2, "SHOW INSTANCES;"))))

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    mg_sleep_and_assert(data, show_instances_coord3)
    mg_sleep_and_assert(data, show_instances_coord1)
    mg_sleep_and_assert(data, show_instances_coord2)

    instance_3_cursor = connect(host="localhost", port=7689).cursor()

    def show_replicas():
        return sorted(list(execute_and_fetch_all(instance_3_cursor, "SHOW REPLICAS;")))

    replicas = [
        (
            "instance_1",
            "localhost:10001",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
        (
            "instance_2",
            "localhost:10002",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
    ]
    mg_sleep_and_assert_collection(replicas, show_replicas)

    def get_vertex_count_func(cursor):
        def get_vertex_count():
            return execute_and_fetch_all(cursor, "MATCH (n) RETURN count(n)")[0][0]

        return get_vertex_count

    vertex_count = 0
    instance_1_cursor = connect(port=7687, host="localhost").cursor()
    instance_2_cursor = connect(port=7688, host="localhost").cursor()

    mg_sleep_and_assert(vertex_count, get_vertex_count_func(instance_1_cursor))
    mg_sleep_and_assert(vertex_count, get_vertex_count_func(instance_2_cursor))

    # 3

    interactive_mg_runner.kill(inner_instances_description, "instance_2")

    # 4

    with pytest.raises(Exception):
        execute_and_fetch_all(
            coord_cursor_3,
            "REGISTER INSTANCE instance_4 WITH CONFIG {'bolt_server': 'localhost:7680', 'management_server': 'localhost:10050', 'replication_server': 'localhost:10051'};",
        )

    # 5
    # This will trigger verify and correct cluster state, where we shouldn't choose new MAIN
    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "down", "unknown"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    leader_name = "coordinator_3"

    main_name = "instance_3"

    assert leader_name is not None, "leader is None"
    assert main_name is not None, "Main is None"

    data = update_tuple_value(data, leader_name, 0, -1, "leader")
    data = update_tuple_value(data, main_name, 0, -1, "main")

    mg_sleep_and_assert(data, show_instances_coord3)
    mg_sleep_and_assert(data, show_instances_coord1)
    mg_sleep_and_assert(data, show_instances_coord2)

    # 6

    interactive_mg_runner.start(inner_instances_description, "instance_2")

    # 7

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "replica"),
    ]

    data = update_tuple_value(data, leader_name, 0, -1, "leader")
    data = update_tuple_value(data, main_name, 0, -1, "main")

    mg_sleep_and_assert(data, show_instances_coord3)
    mg_sleep_and_assert(data, show_instances_coord1)
    mg_sleep_and_assert(data, show_instances_coord2)

    def get_port(instance_name):
        mappings = {
            "instance_1": 7687,
            "instance_2": 7688,
            "instance_3": 7689,
        }
        return mappings[instance_name]

    main_cursor = connect(port=get_port(main_name), host="localhost").cursor()

    def show_replicas():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))

    replicas = [
        (
            "instance_1",
            "localhost:10001",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
        (
            "instance_2",
            "localhost:10002",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
        (
            "instance_3",
            "localhost:10003",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
    ]
    replicas = [replica for replica in replicas if replica[0] != main_name]
    mg_sleep_and_assert_collection(replicas, show_replicas)

    # 8

    vertex_count = 10
    for _ in range(vertex_count):
        execute_and_fetch_all(main_cursor, "CREATE ();")

    def get_vertex_count_func(cursor):
        def get_vertex_count():
            return execute_and_fetch_all(cursor, "MATCH (n) RETURN count(n)")[0][0]

        return get_vertex_count

    for instance in ["instance_1", "instance_2", "instance_3"]:
        cursor = connect(port=get_port(instance), host="localhost").cursor()
        mg_sleep_and_assert(vertex_count, get_vertex_count_func(cursor))


def test_force_reset_works_after_failed_registration_and_2_coordinators_down(test_name):
    # Goal of this test is to check when action fails, that force reset happens
    # and everything works correctly after majority of coordinators is back up
    # 1. Start all instances.
    # 2. Check everything works correctly
    # 3. Try to register instance which doesn't exist -> Enter force reset
    # 4. Kill 2 coordinators
    # 5. New action shouldn't succeed because of opened lock
    # 6. Start one coordinator
    # 7. Check that replica failover happens in force reset
    # 8. Check that everything works correctly

    # 1
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)

    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()
    for query in get_default_setup_queries():
        execute_and_fetch_all(coord_cursor_3, query)

    # 2

    def show_instances_coord3():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_3, "SHOW INSTANCES;"))))

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;"))))

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    def show_instances_coord2():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_2, "SHOW INSTANCES;"))))

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    mg_sleep_and_assert(data, show_instances_coord3)
    mg_sleep_and_assert(data, show_instances_coord1)
    mg_sleep_and_assert(data, show_instances_coord2)

    instance_3_cursor = connect(host="localhost", port=7689).cursor()

    def show_replicas():
        return sorted(list(execute_and_fetch_all(instance_3_cursor, "SHOW REPLICAS;")))

    replicas = [
        (
            "instance_1",
            "localhost:10001",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
        (
            "instance_2",
            "localhost:10002",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
    ]
    mg_sleep_and_assert_collection(replicas, show_replicas)

    def get_vertex_count_func(cursor):
        def get_vertex_count():
            return execute_and_fetch_all(cursor, "MATCH (n) RETURN count(n)")[0][0]

        return get_vertex_count

    vertex_count = 0
    instance_1_cursor = connect(port=7687, host="localhost").cursor()
    instance_2_cursor = connect(port=7688, host="localhost").cursor()

    mg_sleep_and_assert(vertex_count, get_vertex_count_func(instance_1_cursor))
    mg_sleep_and_assert(vertex_count, get_vertex_count_func(instance_2_cursor))

    # 3

    with pytest.raises(Exception) as _:
        execute_and_fetch_all(
            coord_cursor_3,
            "REGISTER INSTANCE instance_4 WITH CONFIG {'bolt_server': 'localhost:7680', 'management_server': 'localhost:10050', 'replication_server': 'localhost:10051'};",
        )

    # 4
    interactive_mg_runner.kill(inner_instances_description, "coordinator_2")
    interactive_mg_runner.kill(inner_instances_description, "coordinator_3")

    # 5
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(
            coord_cursor_1,
            "REGISTER INSTANCE instance_4 WITH CONFIG {'bolt_server': 'localhost:7680', 'management_server': "
            "'localhost:10050', 'replication_server': 'localhost:10051'};",
        )

    assert "Couldn't register replica instance since coordinator is not a leader!" in str(e.value)

    # 6

    interactive_mg_runner.start(inner_instances_description, "coordinator_2")

    # 7
    # main must be the same after leader election as before, we can't demote old main
    leader_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "down", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    leader_name = find_instance_and_assert_instances(
        instance_role="leader", num_coordinators=3, coord_ids_to_skip_validation={3}
    )

    leader_data = update_tuple_value(leader_data, leader_name, 0, -1, "leader")

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;"))))

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    def show_instances_coord2():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_2, "SHOW INSTANCES;"))))

    mg_sleep_and_assert(leader_data, show_instances_coord1)

    mg_sleep_and_assert(leader_data, show_instances_coord2)

    def show_replicas():
        return sorted(list(execute_and_fetch_all(instance_3_cursor, "SHOW REPLICAS;")))

    replicas = [
        (
            "instance_1",
            "localhost:10001",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
        (
            "instance_2",
            "localhost:10002",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
    ]
    mg_sleep_and_assert_collection(replicas, show_replicas)

    # 8

    vertex_count = 10
    for _ in range(vertex_count):
        execute_and_fetch_all(instance_3_cursor, "CREATE ();")

    def get_vertex_count_func(cursor):
        def get_vertex_count():
            return execute_and_fetch_all(cursor, "MATCH (n) RETURN count(n)")[0][0]

        return get_vertex_count

    def get_port(instance_name):
        mappings = {
            "instance_1": 7687,
            "instance_2": 7688,
            "instance_3": 7689,
        }
        return mappings[instance_name]

    for instance in ["instance_1", "instance_2", "instance_3"]:
        cursor = connect(port=get_port(instance), host="localhost").cursor()
        mg_sleep_and_assert(vertex_count, get_vertex_count_func(cursor))


def test_coordinator_gets_info_on_other_coordinators(test_name):
    # Goal of this test is to check that coordinator which has cluster state
    # after restart gets info on other cluster also which is added in meantime
    # 1. Start all instances.
    # 2. Check everything works correctly
    # 3. Kill one coordinator
    # 4. Wait until it is down
    # 5. Register new coordinator
    # 6. Check that leaders and followers see each other
    # 7. Start coordinator which is down
    # 8. Check that such coordinator has correct info

    # 1

    inner_instances_description = get_instances_description_no_setup(test_name=test_name)

    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()
    for query in get_default_setup_queries():
        execute_and_fetch_all(coord_cursor_3, query)

    # 2

    def show_instances_coord3():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_3, "SHOW INSTANCES;"))))

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;"))))

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    def show_instances_coord2():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_2, "SHOW INSTANCES;"))))

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    mg_sleep_and_assert(data, show_instances_coord3)
    mg_sleep_and_assert(data, show_instances_coord1)
    mg_sleep_and_assert(data, show_instances_coord2)

    instance_3_cursor = connect(host="localhost", port=7689).cursor()

    def show_replicas():
        return sorted(list(execute_and_fetch_all(instance_3_cursor, "SHOW REPLICAS;")))

    replicas = [
        (
            "instance_1",
            "localhost:10001",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
        (
            "instance_2",
            "localhost:10002",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
    ]
    mg_sleep_and_assert_collection(replicas, show_replicas)

    def get_vertex_count_func(cursor):
        def get_vertex_count():
            return execute_and_fetch_all(cursor, "MATCH (n) RETURN count(n)")[0][0]

        return get_vertex_count

    vertex_count = 0
    instance_1_cursor = connect(port=7687, host="localhost").cursor()
    instance_2_cursor = connect(port=7688, host="localhost").cursor()

    mg_sleep_and_assert(vertex_count, get_vertex_count_func(instance_1_cursor))
    mg_sleep_and_assert(vertex_count, get_vertex_count_func(instance_2_cursor))

    # 3

    interactive_mg_runner.kill(inner_instances_description, "coordinator_2")

    # 4

    while True:
        try:
            execute_and_fetch_all(coord_cursor_2, "SHOW INSTANCES;")
            time.sleep(0.1)
        except Exception:
            break

    # 5

    other_instances = {
        "coordinator_4": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7693",
                "--log-level=TRACE",
                "--coordinator-id=4",
                "--coordinator-port=10114",
                "--management-port=10124",
                "--coordinator-hostname",
                "localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_4.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_4",
            "setup_queries": [],
        },
    }

    interactive_mg_runner.start(other_instances, "coordinator_4")
    execute_and_fetch_all(
        coord_cursor_3,
        "ADD COORDINATOR 4 WITH CONFIG {'bolt_server': 'localhost:7693', 'coordinator_server': 'localhost:10114', 'management_server': 'localhost:10124'};",
    )

    # 6

    coord_cursor_4 = connect(host="localhost", port=7693).cursor()

    def show_instances_coord4():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_4, "SHOW INSTANCES;"))))

    coord2_down_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "down", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("coordinator_4", "localhost:7693", "localhost:10114", "localhost:10124", "up", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]
    mg_sleep_and_assert(coord2_down_data, show_instances_coord3)
    mg_sleep_and_assert(coord2_down_data, show_instances_coord1)
    mg_sleep_and_assert(coord2_down_data, show_instances_coord4)

    # 7

    interactive_mg_runner.start(inner_instances_description, "coordinator_2")

    # 8

    coord2_up_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("coordinator_4", "localhost:7693", "localhost:10114", "localhost:10124", "up", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    mg_sleep_and_assert(coord2_up_data, show_instances_coord2)


def test_registration_works_after_main_set(test_name):
    # This test tries to register first main and set it to main and afterwards register other instances
    # 1. Start all instances.
    # 2. Check everything works correctly

    # 1
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)

    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()
    for query in get_default_setup_queries():
        execute_and_fetch_all(coord_cursor_3, query)

    # 2

    def show_instances_coord3():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_3, "SHOW INSTANCES;"))))

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;"))))

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    def show_instances_coord2():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_2, "SHOW INSTANCES;"))))

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    mg_sleep_and_assert(data, show_instances_coord3)
    mg_sleep_and_assert(data, show_instances_coord1)
    mg_sleep_and_assert(data, show_instances_coord2)

    instance_3_cursor = connect(host="localhost", port=7689).cursor()

    def show_replicas():
        return sorted(list(execute_and_fetch_all(instance_3_cursor, "SHOW REPLICAS;")))

    replicas = [
        (
            "instance_1",
            "localhost:10001",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
        (
            "instance_2",
            "localhost:10002",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
    ]
    mg_sleep_and_assert_collection(replicas, show_replicas)

    def get_vertex_count_func(cursor):
        def get_vertex_count():
            return execute_and_fetch_all(cursor, "MATCH (n) RETURN count(n)")[0][0]

        return get_vertex_count

    vertex_count = 0
    instance_1_cursor = connect(port=7687, host="localhost").cursor()
    instance_2_cursor = connect(port=7688, host="localhost").cursor()

    mg_sleep_and_assert(vertex_count, get_vertex_count_func(instance_1_cursor))
    mg_sleep_and_assert(vertex_count, get_vertex_count_func(instance_2_cursor))


def test_coordinator_not_leader_registration_does_not_work(test_name):
    # Goal of this test is to check that it is not possible to register instance on follower coord
    # 1. Start all instances.
    # 2. Check everything works correctly
    # 3. Try to register instance on follower coord

    # 1
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)

    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()
    for query in get_default_setup_queries():
        execute_and_fetch_all(coord_cursor_3, query)

    # 2

    def show_instances_coord3():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_3, "SHOW INSTANCES;"))))

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;"))))

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    def show_instances_coord2():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_2, "SHOW INSTANCES;"))))

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]
    mg_sleep_and_assert(data, show_instances_coord3)
    mg_sleep_and_assert(data, show_instances_coord1)
    mg_sleep_and_assert(data, show_instances_coord2)

    instance_3_cursor = connect(host="localhost", port=7689).cursor()

    def show_replicas():
        return sorted(list(execute_and_fetch_all(instance_3_cursor, "SHOW REPLICAS;")))

    replicas = [
        (
            "instance_1",
            "localhost:10001",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
        (
            "instance_2",
            "localhost:10002",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
    ]
    mg_sleep_and_assert_collection(replicas, show_replicas)

    def get_vertex_count_func(cursor):
        def get_vertex_count():
            return execute_and_fetch_all(cursor, "MATCH (n) RETURN count(n)")[0][0]

        return get_vertex_count

    vertex_count = 0
    instance_1_cursor = connect(port=7687, host="localhost").cursor()
    instance_2_cursor = connect(port=7688, host="localhost").cursor()

    mg_sleep_and_assert(vertex_count, get_vertex_count_func(instance_1_cursor))
    mg_sleep_and_assert(vertex_count, get_vertex_count_func(instance_2_cursor))

    # 3

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(
            coord_cursor_1,
            "REGISTER INSTANCE instance_4 WITH CONFIG {'bolt_server': 'localhost:7680', 'management_server': "
            "'localhost:10050', 'replication_server': 'localhost:10051'};",
        )

    assert (
        "Couldn't register replica instance since coordinator is not a leader! Current leader is coordinator with id 3 with bolt socket address localhost:7692"
        == str(e.value)
    )


def test_coordinator_user_action_demote_instance_to_replica(test_name):
    # Goal of this test is to check that it is not possible to register instance on follower coord
    # 1. Start all instances.
    # 2. Check everything works correctly
    # 3. Try to demote instance to replica
    # 4. Check we have correct state

    # 1
    inner_instances_description = get_instances_description_no_setup(test_name=test_name, use_durability=True)

    FAILOVER_PERIOD = 2
    inner_instances_description["instance_1"]["args"].append(f"--instance-down-timeout-sec={FAILOVER_PERIOD}")
    inner_instances_description["instance_2"]["args"].append(f"--instance-down-timeout-sec={FAILOVER_PERIOD}")
    inner_instances_description["instance_3"]["args"].append(f"--instance-down-timeout-sec={FAILOVER_PERIOD}")

    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()
    for query in get_default_setup_queries():
        execute_and_fetch_all(coord_cursor_3, query)

    # 2

    def show_instances_coord3():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_3, "SHOW INSTANCES;"))))

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;"))))

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    def show_instances_coord2():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_2, "SHOW INSTANCES;"))))

    data_original = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    mg_sleep_and_assert(data_original, show_instances_coord3)
    mg_sleep_and_assert(data_original, show_instances_coord1)
    mg_sleep_and_assert(data_original, show_instances_coord2)

    instance_3_cursor = connect(host="localhost", port=7689).cursor()

    def show_replicas():
        return sorted(list(execute_and_fetch_all(instance_3_cursor, "SHOW REPLICAS;")))

    replicas = [
        (
            "instance_1",
            "localhost:10001",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
        (
            "instance_2",
            "localhost:10002",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
    ]
    mg_sleep_and_assert_collection(replicas, show_replicas)

    def get_vertex_count_func(cursor):
        def get_vertex_count():
            return execute_and_fetch_all(cursor, "MATCH (n) RETURN count(n)")[0][0]

        return get_vertex_count

    vertex_count = 0
    instance_1_cursor = connect(port=7687, host="localhost").cursor()
    instance_2_cursor = connect(port=7688, host="localhost").cursor()

    mg_sleep_and_assert(vertex_count, get_vertex_count_func(instance_1_cursor))
    mg_sleep_and_assert(vertex_count, get_vertex_count_func(instance_2_cursor))

    # 3

    execute_and_fetch_all(
        coord_cursor_3,
        "DEMOTE INSTANCE instance_3",
    )

    # 4.

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "replica"),
    ]

    mg_sleep_and_assert(data, show_instances_coord3)
    mg_sleep_and_assert(data, show_instances_coord1)
    mg_sleep_and_assert(data, show_instances_coord2)

    mg_sleep_and_assert_until_role_change(
        lambda: execute_and_fetch_all(instance_3_cursor, "SHOW REPLICATION ROLE;")[0][0], "replica"
    )

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(instance_3_cursor, "SHOW REPLICAS;")
    assert str(e.value) == "Replica can't show registered replicas (it shouldn't have any)!"

    mg_assert_until(data, show_instances_coord3, FAILOVER_PERIOD + 1)
    mg_sleep_and_assert(data, show_instances_coord1)
    mg_sleep_and_assert(data, show_instances_coord2)

    execute_and_fetch_all(coord_cursor_3, "SET INSTANCE instance_3 TO MAIN;")

    mg_sleep_and_assert(data_original, show_instances_coord3)
    mg_sleep_and_assert(data_original, show_instances_coord1)
    mg_sleep_and_assert(data_original, show_instances_coord2)


def test_coordinator_user_action_force_reset_works(test_name):
    # Goal of this test is to check that it is not possible to register instance on follower coord
    # 1. Start all instances.
    # 2. Check everything works correctly
    # 3. Try force reset
    # 4. Check we have correct state

    # 1
    inner_instances_description = get_instances_description_no_setup(test_name=test_name, use_durability=True)

    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()
    for query in get_default_setup_queries():
        execute_and_fetch_all(coord_cursor_3, query)

    # 2

    def show_instances_coord3():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_3, "SHOW INSTANCES;"))))

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;"))))

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    def show_instances_coord2():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_2, "SHOW INSTANCES;"))))

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    mg_sleep_and_assert(data, show_instances_coord3)
    mg_sleep_and_assert(data, show_instances_coord1)
    mg_sleep_and_assert(data, show_instances_coord2)

    instance_3_cursor = connect(host="localhost", port=7689).cursor()

    def show_replicas():
        return sorted(list(execute_and_fetch_all(instance_3_cursor, "SHOW REPLICAS;")))

    replicas = [
        (
            "instance_1",
            "localhost:10001",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
        (
            "instance_2",
            "localhost:10002",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
    ]
    mg_sleep_and_assert_collection(replicas, show_replicas)

    def get_vertex_count_func(cursor):
        def get_vertex_count():
            return execute_and_fetch_all(cursor, "MATCH (n) RETURN count(n)")[0][0]

        return get_vertex_count

    vertex_count = 0
    instance_1_cursor = connect(port=7687, host="localhost").cursor()
    instance_2_cursor = connect(port=7688, host="localhost").cursor()

    mg_sleep_and_assert(vertex_count, get_vertex_count_func(instance_1_cursor))
    mg_sleep_and_assert(vertex_count, get_vertex_count_func(instance_2_cursor))

    # 3

    execute_and_fetch_all(
        coord_cursor_3,
        "FORCE RESET CLUSTER STATE;",
    )

    # 4.

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    mg_sleep_and_assert(data, show_instances_coord3)
    mg_sleep_and_assert(data, show_instances_coord1)
    mg_sleep_and_assert(data, show_instances_coord2)


def test_all_coords_down_resume(test_name):
    # Goal of this test is to check that coordinators are able to resume, if cluster is able to resume where it
    # left off if all coordinators go down

    # 1. Start cluster
    # 2. Check everything works correctly
    # 3. Stop all coordinators
    # 4. Start 2 coordinators, one should be leader, and one follower
    # 5. Check everything works correctly

    # 1
    inner_instances_description = get_instances_description_no_setup(test_name=test_name, use_durability=True)

    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()
    for query in get_default_setup_queries():
        execute_and_fetch_all(coord_cursor_3, query)

    # 2
    def show_instances_coord3():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_3, "SHOW INSTANCES;"))))

    def show_instances_coord1():
        coord_cursor_1 = connect(host="localhost", port=7690).cursor()
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;"))))

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    def show_instances_coord2():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_2, "SHOW INSTANCES;"))))

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    mg_sleep_and_assert(data, show_instances_coord3)
    mg_sleep_and_assert(data, show_instances_coord1)
    mg_sleep_and_assert(data, show_instances_coord2)

    # 3

    interactive_mg_runner.kill(inner_instances_description, "coordinator_1")
    interactive_mg_runner.kill(inner_instances_description, "coordinator_2")
    interactive_mg_runner.kill(inner_instances_description, "coordinator_3")

    # 4

    with concurrent.futures.ThreadPoolExecutor(2) as executor:
        executor.submit(interactive_mg_runner.start, inner_instances_description, "coordinator_2")
        executor.submit(interactive_mg_runner.start, inner_instances_description, "coordinator_1")

    # 5

    leader_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "down", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "replica"),
    ]

    wait_for_status_change(show_instances_coord1, {"coordinator_1", "coordinator_2"}, "leader")

    leader = find_instance_and_assert_instances(
        instance_role="leader", num_coordinators=3, coord_ids_to_skip_validation={3}
    )

    main = find_instance_and_assert_instances(
        instance_role="main", num_coordinators=3, coord_ids_to_skip_validation={3}
    )
    leader_data = update_tuple_value(leader_data, leader, 0, -1, "leader")
    leader_data = update_tuple_value(leader_data, main, 0, -1, "main")

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    mg_sleep_and_assert(leader_data, show_instances_coord1)

    mg_sleep_and_assert(leader_data, show_instances_coord2)

    # 6
    interactive_mg_runner.kill(inner_instances_description, "instance_3")
    interactive_mg_runner.start(inner_instances_description, "coordinator_3")

    # 7

    leader_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "down", "unknown"),
    ]

    wait_for_status_change(show_instances_coord1, {"instance_3"}, "unknown")

    leader = find_instance_and_assert_instances(instance_role="leader", num_coordinators=3)

    main = find_instance_and_assert_instances(instance_role="main", num_coordinators=3)

    leader_data = update_tuple_value(leader_data, leader, 0, -1, "leader")
    leader_data = update_tuple_value(leader_data, main, 0, -1, "main")

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()

    mg_sleep_and_assert(leader_data, show_instances_coord1)

    mg_sleep_and_assert(leader_data, show_instances_coord2)

    mg_sleep_and_assert(leader_data, show_instances_coord3)


def test_one_coord_down_with_durability_resume(test_name):
    # Goal of this test is to check that coordinators are able to resume, if cluster is able to resume where it
    # left off if all coordinators go down

    # 1. Start cluster
    # 2. Check everything works correctly
    # 3. Stop all coordinators
    # 4. Start 2 coordinators, one should be leader, and one follower
    # 5. Check everything works correctly

    # 1
    inner_instances_description = get_instances_description_no_setup(test_name=test_name, use_durability=True)

    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()

    for query in get_default_setup_queries():
        execute_and_fetch_all(coord_cursor_3, query)

    # 2
    def show_instances_coord3():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_3, "SHOW INSTANCES;"))))

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;"))))

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    def show_instances_coord2():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_2, "SHOW INSTANCES;"))))

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    mg_sleep_and_assert(data, show_instances_coord3)
    mg_sleep_and_assert(data, show_instances_coord1)
    mg_sleep_and_assert(data, show_instances_coord2)

    # 3

    interactive_mg_runner.kill(inner_instances_description, "coordinator_1")

    leader_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "down", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]
    mg_sleep_and_assert(leader_data, show_instances_coord3)

    # 4
    interactive_mg_runner.start(inner_instances_description, "coordinator_1")

    # 5

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    mg_sleep_and_assert(data, show_instances_coord3)
    mg_sleep_and_assert(data, show_instances_coord1)
    mg_sleep_and_assert(data, show_instances_coord2)

    # 6
    interactive_mg_runner.kill(inner_instances_description, "instance_3")

    # 7

    leader_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "main"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "down", "unknown"),
    ]

    mg_sleep_and_assert(leader_data, show_instances_coord3)
    mg_sleep_and_assert(leader_data, show_instances_coord1)
    mg_sleep_and_assert(leader_data, show_instances_coord2)


def test_registration_does_not_deadlock_when_instance_is_down(test_name):
    # Goal of this test is to assert that system doesn't deadlock in case of failure on registration

    # 1
    inner_instances_description = get_instances_description_no_setup(test_name=test_name, use_durability=True)

    interactive_mg_runner.start(inner_instances_description, "coordinator_1")
    interactive_mg_runner.start(inner_instances_description, "coordinator_2")
    interactive_mg_runner.start(inner_instances_description, "coordinator_3")

    setup_queries = [
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
    ]

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()
    for query in setup_queries:
        execute_and_fetch_all(coord_cursor_3, query)

    query = "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};"

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(coord_cursor_3, query)

    assert "Couldn't register replica instance because setting instance to replica failed!" in str(e.value)

    interactive_mg_runner.start(inner_instances_description, "instance_1")
    execute_and_fetch_all(coord_cursor_3, query)

    interactive_mg_runner.start(inner_instances_description, "instance_2")
    interactive_mg_runner.start(inner_instances_description, "instance_3")

    setup_queries = [
        "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
        "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
        "SET INSTANCE instance_3 TO MAIN",
    ]

    for query in setup_queries:
        execute_and_fetch_all(coord_cursor_3, query)

    def show_instances_coord3():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_3, "SHOW INSTANCES;"))))

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;"))))

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    def show_instances_coord2():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_2, "SHOW INSTANCES;"))))

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    mg_sleep_and_assert(data, show_instances_coord3)
    mg_sleep_and_assert(data, show_instances_coord1)
    mg_sleep_and_assert(data, show_instances_coord2)


def test_follower_have_correct_health(test_name):
    # Goal of this test is to check that coordinators are able to resume, if cluster is able to resume where it
    # left off if all coordinators go down

    # 1. Start cluster
    # 2. Check everything works correctly
    # 3. Stop all coordinators
    # 4. Start 2 coordinators, one should be leader, and one follower
    # 5. Check everything works correctly

    # 1
    inner_instances_description = get_instances_description_no_setup(test_name=test_name, use_durability=True)

    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    setup_queries = get_default_setup_queries()

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()
    for query in setup_queries:
        execute_and_fetch_all(coord_cursor_3, query)

    # 2
    def show_instances_coord3():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_3, "SHOW INSTANCES;"))))

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;"))))

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    def show_instances_coord2():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_2, "SHOW INSTANCES;"))))

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]

    mg_sleep_and_assert(data, show_instances_coord3)
    mg_sleep_and_assert(data, show_instances_coord1)
    mg_sleep_and_assert(data, show_instances_coord2)


def test_first_coord_restarts(test_name):
    # Goal of this test is to check that first coordinator can restart without any issues

    # 1
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)

    interactive_mg_runner.start(inner_instances_description, "coordinator_1")
    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;"))))

    leader_data = [("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader")]
    mg_sleep_and_assert(leader_data, show_instances_coord1)

    interactive_mg_runner.kill_all(keep_directories=True)

    interactive_mg_runner.start(inner_instances_description, "coordinator_1")

    leader_data = [("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader")]

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()
    mg_sleep_and_assert(leader_data, show_instances_coord1)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
