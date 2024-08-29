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
from common import connect, execute_and_fetch_all, ignore_elapsed_time_from_results
from distributed_coords_common import (
    find_instance_and_assert_instances,
    get_default_setup_queries,
    get_instances_description_no_setup,
    update_tuple_value,
)
from mg_utils import (
    mg_assert_until,
    mg_sleep_and_assert,
    mg_sleep_and_assert_collection,
    mg_sleep_and_assert_multiple,
    wait_for_status_change,
)

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))


def test_multiple_old_mains_single_failover():
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
    temp_dir = tempfile.TemporaryDirectory()
    temp_dir_name = temp_dir.name

    inner_instances_description = get_instances_description_no_setup(
        temp_dir_name, test_name="test_multiple_old_mains_single_failover"
    )

    interactive_mg_runner.start_all(inner_instances_description)

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
        with pytest.raises(Exception) as e:
            execute_and_fetch_all(instance_1_cursor, "CREATE ();")
        vertex_count += 1

        assert vertex_count == execute_and_fetch_all(instance_1_cursor, "MATCH (n) RETURN count(n);")[0][0]
        assert vertex_count == execute_and_fetch_all(instance_2_cursor, "MATCH (n) RETURN count(n);")[0][0]
        time.sleep(0.1)
        time_slept += 0.1

    interactive_mg_runner.stop_all(keep_directories=False)


def test_force_reset_works_after_failed_registration():
    # Goal of this test is to check that force reset works after failed registration
    # 1. Start all instances.
    # 2. Check everything works correctly
    # 3. Try register instance which doesn't exist
    # 4. Enter force reset
    # 5. Check that everything works correctly

    # 1
    temp_dir = tempfile.TemporaryDirectory()
    temp_dir_name = temp_dir.name

    inner_instances_description = get_instances_description_no_setup(
        temp_dir_name, test_name="test_force_reset_works_after_failed_registration"
    )

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

    with pytest.raises(Exception) as e:
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

    interactive_mg_runner.stop_all(keep_directories=False)


def test_distributed_automatic_failover_with_leadership_change():
    temp_dir = tempfile.TemporaryDirectory()

    inner_instances_description = get_instances_description_no_setup(
        temp_dir.name, test_name="test_distributed_automatic_failover_with_leadership_change"
    )

    interactive_mg_runner.start_all(inner_instances_description)

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
    wait_for_status_change(show_instances_coord1, {"instance_3"}, "unknown")

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

    interactive_mg_runner.stop_all(keep_directories=False)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
