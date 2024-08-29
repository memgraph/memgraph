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


def test_force_reset_works_after_failed_registration_and_2_coordinators_down():
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
    temp_dir = tempfile.TemporaryDirectory()
    temp_dir_name = temp_dir.name

    inner_instances_description = get_instances_description_no_setup(
        temp_dir_name, test_name="test_force_reset_works_after_failed_registration_and_2_coordinators_down"
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

    interactive_mg_runner.stop_all(keep_directories=False)


def test_coordinator_gets_info_on_other_coordinators():
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
    temp_dir = tempfile.TemporaryDirectory()
    temp_dir_name = temp_dir.name

    inner_instances_description = get_instances_description_no_setup(
        temp_dir_name, test_name="test_coordinator_gets_info_on_other_coordinators"
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
            "log_file": "high_availability/distributed_coords/coordinator4.log",
            "data_directory": f"{temp_dir_name}/coordinator_4",
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

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("coordinator_4", "localhost:7693", "localhost:10114", "localhost:10124", "up", "follower"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]
    mg_sleep_and_assert(data, show_instances_coord3)
    mg_sleep_and_assert(data, show_instances_coord1)
    mg_sleep_and_assert(data, show_instances_coord4)

    # 7

    interactive_mg_runner.start(inner_instances_description, "coordinator_2")

    # 8

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    mg_sleep_and_assert(data, show_instances_coord2)

    interactive_mg_runner.stop_all(keep_directories=False)


def test_follower_have_correct_health():
    # Goal of this test is to check that coordinators are able to resume, if cluster is able to resume where it
    # left off if all coordinators go down

    # 1. Start cluster
    # 2. Check everything works correctly
    # 3. Stop all coordinators
    # 4. Start 2 coordinators, one should be leader, and one follower
    # 5. Check everything works correctly

    # 1
    temp_dir = tempfile.TemporaryDirectory()
    temp_dir_name = temp_dir.name

    inner_instances_description = get_instances_description_no_setup(
        temp_dir_name, test_name="test_one_coord_down_with_durability_resume", use_durability=True
    )

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

    interactive_mg_runner.stop_all(keep_directories=False)


def test_first_coord_restarts():
    # Goal of this test is to check that first coordinator can restart without any issues

    # 1
    temp_dir = tempfile.TemporaryDirectory()
    temp_dir_name = temp_dir.name

    inner_instances_description = get_instances_description_no_setup(
        temp_dir_name, test_name="test_first_coord_restarts"
    )

    interactive_mg_runner.start(inner_instances_description, "coordinator_1")
    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;"))))

    leader_data = [("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader")]
    mg_sleep_and_assert(leader_data, show_instances_coord1)

    interactive_mg_runner.stop_all(keep_directories=True)

    interactive_mg_runner.start(inner_instances_description, "coordinator_1")

    leader_data = [("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader")]

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()
    mg_sleep_and_assert(leader_data, show_instances_coord1)

    interactive_mg_runner.stop_all(keep_directories=False)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
