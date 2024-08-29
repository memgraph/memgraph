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


def test_multiple_failovers_in_row_no_leadership_change():
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
    temp_dir = tempfile.TemporaryDirectory()
    temp_dir_name = temp_dir.name

    inner_memgraph_instances = get_instances_description_no_setup(
        temp_dir_name, test_name="test_multiple_failovers_in_row_no_leadership_change"
    )
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

    interactive_mg_runner.stop_all(keep_directories=False)


def test_force_reset_works_after_failed_registration_and_replica_down():
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
    temp_dir = tempfile.TemporaryDirectory()
    temp_dir_name = temp_dir.name

    inner_instances_description = get_instances_description_no_setup(
        temp_dir_name, test_name="test_force_reset_works_after_failed_registration_and_replica_down"
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

    interactive_mg_runner.kill(inner_instances_description, "instance_2")

    # 4

    with pytest.raises(Exception) as e:
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

    interactive_mg_runner.stop_all(keep_directories=False)


def test_no_leader_after_leader_and_follower_die():
    # 1. Register all but one replication instance on the first leader.
    # 2. Kill the leader and a follower.
    # 3. Check that the remaining follower is not promoted to leader by trying to register remaining replication instance.

    temp_dir = tempfile.TemporaryDirectory()

    inner_memgraph_instances = get_instances_description_no_setup(
        temp_dir, test_name="test_no_leader_after_leader_and_follower_die"
    )
    interactive_mg_runner.start_all(inner_memgraph_instances)

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

    interactive_mg_runner.stop_all(keep_directories=False)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
