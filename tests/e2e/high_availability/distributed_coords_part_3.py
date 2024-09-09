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
    get_default_setup_queries,
    get_instances_description_no_setup,
)
from mg_utils import (
    mg_sleep_and_assert,
    mg_sleep_and_assert_collection,
    mg_sleep_and_assert_multiple,
)

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))


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


def test_old_main_comes_back_on_new_leader_as_main():
    # 1. Start all instances.
    # 2. Kill all instances
    # 3. Kill the leader
    # 4. Start the old main instance
    # 5. Run SHOW INSTANCES on the new leader and check that the old main instance is main once again

    temp_dir = tempfile.TemporaryDirectory()

    inner_memgraph_instances = get_instances_description_no_setup(
        temp_dir.name, test_name="test_old_main_comes_back_on_new_leader_as_main"
    )
    interactive_mg_runner.start_all(inner_memgraph_instances)

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

    interactive_mg_runner.stop_all(keep_directories=False)


def test_registering_4_coords():
    # Goal of this test is to assure registering of multiple coordinators in row works
    temp_dir = tempfile.TemporaryDirectory()
    temp_dir_name = temp_dir.name

    test_name = "test_registering_4_coords"
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
            "log_file": f"high_availability/distributed_coords/{test_name}/instance_1.log",
            "data_directory": f"{temp_dir_name}/instance_1",
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
            "log_file": f"high_availability/distributed_coords/{test_name}/instance_2.log",
            "data_directory": f"{temp_dir_name}/instance_2",
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
            "log_file": f"high_availability/distributed_coords/{test_name}/instance_3.log",
            "data_directory": f"{temp_dir_name}/instance_3",
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
            "log_file": f"high_availability/distributed_coords/{test_name}/coordinator1.log",
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
            "log_file": f"high_availability/distributed_coords/{test_name}/coordinator2.log",
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
            "log_file": f"high_availability/distributed_coords/{test_name}/coordinator3.log",
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
            "log_file": f"high_availability/distributed_coords/{test_name}/coordinator4.log",
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

    interactive_mg_runner.start_all(INSTANCES_DESCRIPTION)

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

    interactive_mg_runner.stop_all(keep_directories=False)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
