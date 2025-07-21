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
import time
from functools import partial

import interactive_mg_runner
import pytest
from common import execute_and_fetch_all, get_data_path, get_logs_path
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))


BOLT_PORTS = {"main": 7687, "replica_1": 7688, "replica_2": 7689}
REPLICATION_PORTS = {"replica_1": 10001, "replica_2": 10002}
file = "ttl"


@pytest.fixture(autouse=True)
def cleanup_after_test():
    # Run the test
    yield
    # Stop + delete directories after running the test
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


class VertexChecker:
    def __init__(self, check):
        self._check = check
        self.update()

    def is_less(self):
        last_n_prev = self.last_n
        self.last_n = self._check()
        return self.last_n < last_n_prev

    def is_same(self):
        last_n_prev = self.last_n
        self.last_n = self._check()
        return self.last_n == last_n_prev

    def update(self):
        self.last_n = self._check()


def test_ttl_replication(connection, test_name):
    # Goal: Execute TTL on MAIN and check results on REPLICA
    # 0/ Setup replication
    # 1/ MAIN Create dataset
    # 2/ MAIN Configure TTL
    # 3/ Validate that TTL is working on MAIN
    # 4/ Validate that nodes have been deleted on REPLICA as well
    # 5/ Check that the index has been replicated

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "replica_1": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_1']}",
                "--log-level=TRACE",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica1",
            "setup_queries": [
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};",
            ],
        },
        "replica_2": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_2']}",
                "--log-level=TRACE",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/replica2.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica2",
            "setup_queries": [
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_2']};",
            ],
        },
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
                f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL, keep_directories=False)
    cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    execute_and_fetch_all(
        cursor, "UNWIND RANGE(1,100) AS d CREATE (:TTL{ttl:timestamp() + timestamp(duration({second:d}))});"
    )
    execute_and_fetch_all(
        cursor, "UNWIND RANGE(1,50) AS d CREATE ()-[:E1{ttl:timestamp() + timestamp(duration({second:d}))}]->();"
    )
    execute_and_fetch_all(
        cursor, "UNWIND RANGE(51,100) AS d CREATE ()-[:E2{ttl:timestamp() + timestamp(duration({second:d}))}]->();"
    )

    # 2/
    execute_and_fetch_all(cursor, 'ENABLE TTL EVERY "1s";')

    # 3/
    def n_vertices(cursor):
        return execute_and_fetch_all(cursor, f"MATCH (n:TTL) RETURN count(n) < 95;")

    def n_edges(cursor):
        return execute_and_fetch_all(cursor, "MATCH ()-[e]->() WHERE e.ttl > 0 RETURN count(e) < 95;")

    def n_stable_vertices(cursor):
        return execute_and_fetch_all(cursor, f"MATCH (n) WHERE length(labels(n)) = 0 RETURN count(n) = 200;")

    mg_sleep_and_assert([(True,)], partial(n_vertices, cursor))
    mg_sleep_and_assert([(True,)], partial(n_edges, cursor))
    mg_sleep_and_assert([(True,)], partial(n_stable_vertices, cursor))

    # 4/
    cursor_replica = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    mg_sleep_and_assert([(True,)], partial(n_vertices, cursor_replica))
    mg_sleep_and_assert([(True,)], partial(n_edges, cursor_replica))
    mg_sleep_and_assert([(True,)], partial(n_stable_vertices, cursor_replica))
    cursor_replica2 = connection(BOLT_PORTS["replica_2"], "replica").cursor()
    mg_sleep_and_assert([(True,)], partial(n_vertices, cursor_replica2))
    mg_sleep_and_assert([(True,)], partial(n_edges, cursor_replica2))
    mg_sleep_and_assert([(True,)], partial(n_stable_vertices, cursor_replica2))

    # 5/
    def check_index(cursor):
        index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
        assert index_info[0][0] == "edge-property"
        assert index_info[0][2] == "ttl"
        assert index_info[0][3] > 0
        assert index_info[1][0] == "label+property"
        assert index_info[1][1] == "TTL"
        assert index_info[1][2] == ["ttl"]
        assert index_info[1][3] > 0

    check_index(cursor)
    check_index(cursor_replica)
    check_index(cursor_replica2)


def test_ttl_on_replica(connection, test_name):
    # Goal: Check that TTL can be configured on REPLICA,
    #       but is executed only when the instance is MAIN
    # 0/ Setup MAIN
    # 1/ MAIN Create dataset
    # 2/ MAIN Configure TTL
    # 3/ Switch MAIN to REPLICA
    # 4/ Verify that TTL is not running
    # 5/ Switch REPLICA back to MAIN
    # 6/ Verify that TTL is running

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL, keep_directories=False)
    cursor = connection(BOLT_PORTS["main"], "main").cursor()

    def n_vertices():
        return execute_and_fetch_all(cursor, "MATCH(n:TTL) RETURN count(n);")[0][0]

    def n_edges():
        return execute_and_fetch_all(cursor, "MATCH ()-[e]->() WHERE e.ttl > 0 RETURN count(e);")[0][0]

    # 1/
    execute_and_fetch_all(
        cursor, "UNWIND RANGE(1,100) AS d CREATE (:TTL{ttl:timestamp() + timestamp(duration({second:d}))});"
    )
    execute_and_fetch_all(
        cursor, "UNWIND RANGE(1,50) AS d CREATE ()-[:E1{ttl:timestamp() + timestamp(duration({second:d}))}]->();"
    )
    execute_and_fetch_all(
        cursor, "UNWIND RANGE(51,100) AS d CREATE ()-[:E2{ttl:timestamp() + timestamp(duration({second:d}))}]->();"
    )

    v_checker = VertexChecker(n_vertices)
    e_checker = VertexChecker(n_edges)

    # 2/
    execute_and_fetch_all(cursor, 'ENABLE TTL EVERY "1s";')
    mg_sleep_and_assert(True, v_checker.is_less, max_duration=3)
    mg_sleep_and_assert(True, e_checker.is_less, max_duration=3)

    # 3/
    execute_and_fetch_all(cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10000;")

    # 4/
    v_checker.update()
    e_checker.update()
    mg_sleep_and_assert(True, v_checker.is_same, max_duration=3)
    mg_sleep_and_assert(True, e_checker.is_same, max_duration=3)

    # 5/
    execute_and_fetch_all(cursor, "SET REPLICATION ROLE TO MAIN;")

    # 6/
    mg_sleep_and_assert(True, v_checker.is_less, max_duration=3)
    mg_sleep_and_assert(True, e_checker.is_less, max_duration=3)


def test_ttl_recovery_scenario(connection, test_name):
    # Goal: Test that TTL behaves correctly during recovery scenarios
    # 0/ Setup replication with main and replica
    # 1/ Create dataset and configure TTL
    # 2/ Kill both main and replica
    # 3/ Start replica first - verify TTL is not running
    # 4/ Start main - verify TTL starts running and deletes vertices on replica
    # 5/ Verify that vertices are being deleted on both instances

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "replica_1": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_1']}",
                "--log-level=TRACE",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica1",
            "setup_queries": [
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};",
            ],
        },
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
            ],
        },
    }
    MEMGRAPH_INSTANCES_DESCRIPTION_RESTART = {
        name: {k: v for k, v in instance.items() if k != "setup_queries"}
        for name, instance in MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL.items()
    }

    # 0/ Setup replication
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL, keep_directories=False)
    cursor_main = connection(BOLT_PORTS["main"], "main").cursor()
    cursor_replica = connection(BOLT_PORTS["replica_1"], "replica").cursor()

    # 1/ Create dataset and configure TTL
    execute_and_fetch_all(
        cursor_main, "UNWIND RANGE(1,50) AS d CREATE (:TTL{ttl:timestamp() + timestamp(duration({second:d}))});"
    )
    execute_and_fetch_all(
        cursor_main, "UNWIND RANGE(1,25) AS d CREATE ()-[:E1{ttl:timestamp() + timestamp(duration({second:d}))}]->();"
    )
    execute_and_fetch_all(
        cursor_main, "UNWIND RANGE(26,50) AS d CREATE ()-[:E2{ttl:timestamp() + timestamp(duration({second:d}))}]->();"
    )

    # Configure TTL
    execute_and_fetch_all(cursor_main, 'ENABLE TTL EVERY "1s";')

    # Verify initial state - TTL should be running and deleting vertices
    def n_vertices(cursor):
        return execute_and_fetch_all(cursor, "MATCH (n:TTL) RETURN count(n);")[0][0]

    def n_edges(cursor):
        return execute_and_fetch_all(cursor, "MATCH ()-[e]->() WHERE e.ttl > 0 RETURN count(e);")[0][0]

    # Wait for TTL to start working
    mg_sleep_and_assert(True, lambda: n_vertices(cursor_main) < 50, max_duration=5)
    mg_sleep_and_assert(True, lambda: n_edges(cursor_main) < 50, max_duration=5)

    # Verify replica also has vertices being deleted
    mg_sleep_and_assert(True, lambda: n_vertices(cursor_replica) < 50, max_duration=5)
    mg_sleep_and_assert(True, lambda: n_edges(cursor_replica) < 50, max_duration=5)

    # 2/ Kill both main and replica
    interactive_mg_runner.kill_all(keep_directories=True)

    # 3/ Start replica first - verify TTL is not running
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION_RESTART, "replica_1")
    cursor_replica = connection(BOLT_PORTS["replica_1"], "replica").cursor()

    # Wait for replica to fully start up
    mg_sleep_and_assert(True, lambda: execute_and_fetch_all(cursor_replica, "RETURN 1;")[0][0] == 1, max_duration=10)

    # Create checkers to track if vertices/edges are being deleted
    v_checker = VertexChecker(lambda: n_vertices(cursor_replica))
    e_checker = VertexChecker(lambda: n_edges(cursor_replica))

    # Wait and verify that vertices/edges are not being deleted (TTL should not run on replica)
    for _ in range(5):
        assert v_checker.is_same(), "Vertices are being deleted on replica, but TTL should not run on replica"
        assert e_checker.is_same(), "Edges are being deleted on replica, but TTL should not run on replica"
        time.sleep(1)

    # 4/ Start main - verify TTL starts running
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION_RESTART, "main")
    cursor_main = connection(BOLT_PORTS["main"], "main").cursor()

    # Wait for main to fully start up and reconnect to replica
    mg_sleep_and_assert(True, lambda: execute_and_fetch_all(cursor_main, "RETURN 1;")[0][0] == 1, max_duration=10)

    # Wait for replication to be established
    mg_sleep_and_assert(True, lambda: len(execute_and_fetch_all(cursor_main, "SHOW REPLICAS;")) > 0, max_duration=10)

    # 5/ Verify that vertices are being deleted on both instances
    # Main should start running TTL and deleting vertices
    v_checker = VertexChecker(lambda: n_vertices(cursor_main))
    e_checker = VertexChecker(lambda: n_edges(cursor_main))
    mg_sleep_and_assert(True, v_checker.is_less, max_duration=5)
    mg_sleep_and_assert(True, e_checker.is_less, max_duration=5)

    v_checker = VertexChecker(lambda: n_vertices(cursor_replica))
    e_checker = VertexChecker(lambda: n_edges(cursor_replica))
    mg_sleep_and_assert(True, v_checker.is_less, max_duration=5)
    mg_sleep_and_assert(True, e_checker.is_less, max_duration=5)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
