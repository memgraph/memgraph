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
import sys
from functools import partial

import interactive_mg_runner
import pytest
from common import *
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

file = "ttl"


@pytest.fixture
def test_name(request):
    return request.node.name


def get_instances_description_ttl_ha(test_name: str):
    return {
        "instance_1": {
            "args": [
                "--bolt-port",
                "7687",
                "--log-level",
                "TRACE",
                "--management-port",
                "10011",
                "--storage-snapshot-interval-sec",
                "100000",
                "--storage-snapshot-on-exit",
                "false",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_1",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--bolt-port",
                "7688",
                "--log-level",
                "TRACE",
                "--management-port",
                "10012",
                "--storage-snapshot-interval-sec",
                "100000",
                "--storage-snapshot-on-exit",
                "false",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_2",
            "setup_queries": [],
        },
        "coordinator_1": {
            "args": [
                "--bolt-port",
                "7690",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10111",
                "--management-port=10121",
                "--coordinator-hostname",
                "localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_1",
            "setup_queries": [],
        },
        "coordinator_2": {
            "args": [
                "--bolt-port",
                "7691",
                "--log-level=TRACE",
                "--coordinator-id=2",
                "--coordinator-port=10112",
                "--management-port=10122",
                "--coordinator-hostname",
                "localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_2",
            "setup_queries": [],
        },
        "coordinator_3": {
            "args": [
                "--bolt-port",
                "7692",
                "--log-level=TRACE",
                "--coordinator-id=3",
                "--coordinator-port=10113",
                "--management-port=10123",
                "--coordinator-hostname",
                "localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_3",
            "setup_queries": [],
        },
    }


@pytest.fixture(autouse=True)
def cleanup_after_test():
    # Run the test
    yield
    # Stop + delete directories after running the test
    interactive_mg_runner.kill_all(keep_directories=True)


class VertexChecker:
    def __init__(self, cursor):
        self.cursor = cursor
        self.update()

    def n_vertices(self):
        return execute_and_fetch_all(self.cursor, "MATCH (n:TTL) RETURN count(n);")[0][0]

    def n_edges(self):
        return execute_and_fetch_all(self.cursor, "MATCH ()-[e]->() WHERE e.ttl > 0 RETURN count(e);")[0][0]

    def n_stable_vertices(self):
        return execute_and_fetch_all(self.cursor, "MATCH (n) WHERE length(labels(n)) = 0 RETURN count(n);")[0][0]

    def is_less_vertices(self):
        last_n_prev = self.last_vertices
        self.last_vertices = self.n_vertices()
        return self.last_vertices < last_n_prev

    def is_less_edges(self):
        last_n_prev = self.last_edges
        self.last_edges = self.n_edges()
        return self.last_edges < last_n_prev

    def is_same_vertices(self):
        last_n_prev = self.last_vertices
        self.last_vertices = self.n_vertices()
        return self.last_vertices == last_n_prev

    def is_same_edges(self):
        last_n_prev = self.last_edges
        self.last_edges = self.n_edges()
        return self.last_edges == last_n_prev

    def update(self):
        self.last_vertices = self.n_vertices()
        self.last_edges = self.n_edges()


def check_index(cursor):
    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    # Make the index checks order independent
    print(index_info)
    found_edge_property = False
    found_label_property = False
    for idx in index_info:
        if idx[0] == "edge-property" and idx[2] == "ttl":
            found_edge_property = True
        if idx[0] == "label+property" and idx[1] == "TTL" and idx[2] == ["ttl"]:
            found_label_property = True
    return found_edge_property and found_label_property


# 1. Set-up main (instance_1) and replica (instance_2). Create TTL data and enable TTL on instance_1
# 2. Kill replica (instance_2) and create more TTL data on instance_1
# 3. Kill main (instance_1)
# 4. Bring replica back (instance_2) and wait until it becomes main
# 5. Verify TTL is working on the new main (instance_2)
# 6. Bring old main back (instance_1) and wait until it becomes replica
# 7. Check that TTL data is properly replicated and TTL continues working
def test_ttl_high_availability_failover(test_name):
    instances_description = get_instances_description_ttl_ha(test_name=test_name)
    interactive_mg_runner.start_all(instances_description, keep_directories=False)

    setup_queries = [
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
        "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}",
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
        "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
        "SET INSTANCE instance_1 TO MAIN",
    ]

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()
    for query in setup_queries:
        execute_and_fetch_all(coord_cursor_3, query)

    # 1. Create TTL data and enable TTL on main
    instance1_cursor = connect(host="localhost", port=7687).cursor()
    instance2_cursor = connect(host="localhost", port=7688).cursor()

    # Create initial TTL data
    execute_and_fetch_all(
        instance1_cursor, "UNWIND RANGE(1,50) AS d CREATE (:TTL{ttl:timestamp() + timestamp(duration({second:d}))});"
    )
    execute_and_fetch_all(
        instance1_cursor,
        "UNWIND RANGE(1,50) AS d CREATE ()-[:E1{ttl:timestamp() + timestamp(duration({second:d}))}]->();",
    )

    # Wait for data to be replicated
    mg_sleep_and_assert(150, partial(get_vertex_count, instance1_cursor))
    mg_sleep_and_assert(150, partial(get_vertex_count, instance2_cursor))

    # Enable TTL
    execute_and_fetch_all(instance1_cursor, 'ENABLE TTL EVERY "1s";')
    
    # Verify TTL index is present
    mg_sleep_and_assert(True, partial(check_index, instance1_cursor))
    mg_sleep_and_assert(True, partial(check_index, instance2_cursor))

    # 2. Kill replica and create more TTL data on main
    interactive_mg_runner.kill(instances_description, "instance_2")
    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "main"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "down", "unknown"),
    ]
    mg_sleep_and_assert(data, partial(show_instances, coord_cursor_3))

    # Create additional TTL data while replica is down
    execute_and_ignore_dead_replica(
        instance1_cursor, "UNWIND RANGE(1,50) AS d CREATE (:TTL{ttl:timestamp() + timestamp(duration({second:d}))});"
    )
    execute_and_ignore_dead_replica(
        instance1_cursor,
        "UNWIND RANGE(1, 50) AS d CREATE ()-[:E1{ttl:timestamp() + timestamp(duration({second:d}))}]->();",
    )

    # Verify TTL is working on main
    v_checker = VertexChecker(instance1_cursor)
    mg_sleep_and_assert(True, v_checker.is_less_vertices, max_duration=5)
    mg_sleep_and_assert(True, v_checker.is_less_edges, max_duration=5)

    # 3. Kill main
    interactive_mg_runner.kill(instances_description, "instance_1")

    # 4. Bring replica back and wait until it becomes main
    interactive_mg_runner.start(instances_description, "instance_2")
    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "down", "unknown"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "main"),
    ]
    mg_sleep_and_assert(data, partial(show_instances, coord_cursor_3))

    instance2_cursor = connect(host="localhost", port=7688).cursor()

    # 5. Verify TTL is working on the new main
    v_checker = VertexChecker(instance2_cursor)
    mg_sleep_and_assert(True, v_checker.is_less_vertices, max_duration=5)
    mg_sleep_and_assert(True, v_checker.is_less_edges, max_duration=5)

    # Verify TTL index is present
    check_index(instance2_cursor)

    # 6. Bring old main back and wait until it becomes replica
    interactive_mg_runner.start(instances_description, "instance_1")
    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "main"),
    ]
    mg_sleep_and_assert(data, partial(show_instances, coord_cursor_3))

    # 7. Check that TTL data is properly replicated and TTL continues working
    instance1_cursor = connect(host="localhost", port=7687).cursor()

    # 8. Verify TTL is working on the old main
    v_checker = VertexChecker(instance1_cursor)
    mg_sleep_and_assert(True, v_checker.is_less_vertices, max_duration=5)
    mg_sleep_and_assert(True, v_checker.is_less_edges, max_duration=5)

    # Verify TTL index is replicated
    check_index(instance1_cursor)


# Test TTL behavior during role transitions in HA setup
def test_ttl_role_transition_ha(test_name):
    instances_description = get_instances_description_ttl_ha(test_name=test_name)
    interactive_mg_runner.start_all(instances_description, keep_directories=False)

    setup_queries = [
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
        "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}",
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
        "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
        "SET INSTANCE instance_1 TO MAIN",
    ]

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()
    for query in setup_queries:
        execute_and_fetch_all(coord_cursor_3, query)

    instance1_cursor = connect(host="localhost", port=7687).cursor()
    instance2_cursor = connect(host="localhost", port=7688).cursor()

    # Create TTL data
    execute_and_fetch_all(
        instance1_cursor, "UNWIND RANGE(1,50) AS d CREATE (:TTL{ttl:timestamp() + timestamp(duration({second:d}))});"
    )
    execute_and_fetch_all(
        instance1_cursor,
        "UNWIND RANGE(1,25) AS d CREATE ()-[:E1{ttl:timestamp() + timestamp(duration({second:d}))}]->();",
    )

    # Wait for replication
    mg_sleep_and_assert(100, partial(get_vertex_count, instance1_cursor))
    mg_sleep_and_assert(100, partial(get_vertex_count, instance2_cursor))

    # Enable TTL on main
    execute_and_fetch_all(instance1_cursor, 'ENABLE TTL EVERY "1s";')

    # Verify TTL is working on main
    v_checker_main = VertexChecker(instance1_cursor)
    mg_sleep_and_assert(True, v_checker_main.is_less_vertices, max_duration=5)
    mg_sleep_and_assert(True, v_checker_main.is_less_edges, max_duration=5)

    # Switch main to replica
    execute_and_fetch_all(coord_cursor_3, "DEMOTE INSTANCE instance_1")
    execute_and_fetch_all(coord_cursor_3, "SET INSTANCE instance_2 TO MAIN")
    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "main"),
    ]
    mg_sleep_and_assert(data, partial(show_instances, coord_cursor_3))

    # Verify TTL starts working on the new main
    v_checker_new_main = VertexChecker(instance2_cursor)
    mg_sleep_and_assert(True, v_checker_new_main.is_less_vertices, max_duration=5)
    mg_sleep_and_assert(True, v_checker_new_main.is_less_edges, max_duration=5)
    v_checker_old_main = VertexChecker(instance1_cursor)
    mg_sleep_and_assert(True, v_checker_old_main.is_less_vertices, max_duration=5)
    mg_sleep_and_assert(True, v_checker_old_main.is_less_edges, max_duration=5)

    # Verify TTL stops working on the old main (now replica)
    interactive_mg_runner.kill(instances_description, "instance_2")

    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "main"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "down", "unknown"),
    ]
    mg_sleep_and_assert(data, partial(show_instances, coord_cursor_3))

    v_checker_old_main = VertexChecker(instance1_cursor)
    v_checker_old_main.update()

    mg_sleep_and_assert(True, v_checker_old_main.is_less_vertices, max_duration=5)
    mg_sleep_and_assert(True, v_checker_old_main.is_less_edges, max_duration=5)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
