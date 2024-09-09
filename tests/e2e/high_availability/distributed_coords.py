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

import interactive_mg_runner
import pytest
from common import (
    connect,
    execute_and_fetch_all,
    find_instance_and_assert_instances,
    ignore_elapsed_time_from_results,
    update_tuple_value,
)
from distributed_coords_common import (
    get_default_setup_queries,
    get_instances_description_no_setup,
    get_instances_description_no_setup_4_coords,
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


@pytest.mark.parametrize("use_durability", [True, False])
def test_even_number_coords(use_durability):
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
    temp_dir = tempfile.TemporaryDirectory()

    inner_instances_description = get_instances_description_no_setup_4_coords(
        temp_dir.name, test_name="test_even_number_coords_" + str(use_durability), use_durability=use_durability
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

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(instance_3_cursor, "SHOW REPLICAS;")
    assert str(e.value) == "Replica can't show registered replicas (it shouldn't have any)!"

    # 4
    interactive_mg_runner.kill(inner_instances_description, "coordinator_1")
    interactive_mg_runner.kill(inner_instances_description, "coordinator_2")

    # 5

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(coord_cursor_3, "SET INSTANCE instance_3 TO MAIN;")

    assert "Couldn't set instance to main as cluster didn't accept start of action!" in str(e.value)

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
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "replica"),
    ]

    leader_coord_instance_3_demoted = find_instance_and_assert_instances(instance_role="leader", num_coordinators=3)

    main_instance_3_demoted = find_instance_and_assert_instances(instance_role="main", num_coordinators=3)

    assert leader_coord_instance_3_demoted is not None, "Leader not found"

    assert main_instance_3_demoted is not None, "Main not found"

    leader_data = update_tuple_value(leader_data, leader_coord_instance_3_demoted, 0, -1, "leader")
    leader_data = update_tuple_value(leader_data, main_instance_3_demoted, 0, -1, "main")

    port_mappings = {
        "coordinator_1": 7690,
        "coordinator_2": 7691,
        "coordinator_3": 7692,
        "coordinator_4": 7693,
    }

    for coord, port in port_mappings.items():
        coord_cursor = connect(host="localhost", port=port).cursor()

        def show_instances():
            return ignore_elapsed_time_from_results(
                sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))
            )

        mg_sleep_and_assert(leader_data, show_instances)

    interactive_mg_runner.stop_all(keep_directories=False)


def test_old_main_comes_back_on_new_leader_as_replica():
    # 1. Start all instances.
    # 2. Kill the main instance
    # 3. Kill the leader
    # 4. Start the old main instance
    # 5. Run SHOW INSTANCES on the new leader and check that the old main instance is registered as a replica
    # 6. Start again previous leader

    temp_dir = tempfile.TemporaryDirectory()

    inner_instances_description = get_instances_description_no_setup(
        temp_dir.name, test_name="test_old_main_comes_back_on_new_leader_as_replica"
    )

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
