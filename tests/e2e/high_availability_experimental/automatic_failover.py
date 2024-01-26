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

import interactive_mg_runner
import pytest
from common import execute_and_fetch_all
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

MEMGRAPH_INSTANCES_DESCRIPTION = {
    "instance_1": {
        "args": ["--bolt-port", "7688", "--log-level", "TRACE", "--coordinator-server-port", "10011"],
        "log_file": "replica1.log",
        "setup_queries": [],
    },
    "instance_2": {
        "args": ["--bolt-port", "7689", "--log-level", "TRACE", "--coordinator-server-port", "10012"],
        "log_file": "replica2.log",
        "setup_queries": [],
    },
    "instance_3": {
        "args": ["--bolt-port", "7687", "--log-level", "TRACE", "--coordinator-server-port", "10013"],
        "log_file": "main.log",
        "setup_queries": [],
    },
    "coordinator": {
        "args": ["--bolt-port", "7690", "--log-level=TRACE", "--coordinator"],
        "log_file": "replica3.log",
        "setup_queries": [
            "REGISTER INSTANCE instance_1 ON '127.0.0.1:10011' WITH '127.0.0.1:10001';",
            "REGISTER INSTANCE instance_2 ON '127.0.0.1:10012' WITH '127.0.0.1:10002';",
            "REGISTER INSTANCE instance_3 ON '127.0.0.1:10013' WITH '127.0.0.1:10003';",
            "SET INSTANCE instance_3 TO MAIN",
        ],
    },
}


# def test_show_replication_cluster(connection):
#     # Goal of this test is to check the SHOW REPLICATION CLUSTER command.
#     # 1. We start all replicas, main and coordinator manually: we want to be able to kill them ourselves without relying on external tooling to kill processes.
#     # 2. We check that all replicas and main have the correct state: they should all be alive.
#     # 3. We kill one replica. It should not appear anymore in the SHOW REPLICATION CLUSTER command.
#     # 4. We kill main. It should not appear anymore in the SHOW REPLICATION CLUSTER command.
#
#     # 1.
#     interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)
#
#     cursor = connection(7690, "coordinator").cursor()
#
#     # 2.
#
#     # We leave some time for the coordinator to realise the replicas are down.
#     def retrieve_data():
#         return sorted(list(execute_and_fetch_all(cursor, "SHOW REPLICATION CLUSTER;")))
#
#     expected_data = [
#         ("instance_1", "127.0.0.1:10011", True, "replica"),
#         ("instance_2", "127.0.0.1:10012", True, "replica"),
#         ("instance_3", "127.0.0.1:10013", True, "main"),
#     ]
#     mg_sleep_and_assert(expected_data, retrieve_data)
#
#     # 3.
#     interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_1")
#
#     expected_data = [
#         ("instance_1", "127.0.0.1:10011", False, ""),
#         ("instance_2", "127.0.0.1:10012", True, "replica"),
#         ("instance_3", "127.0.0.1:10013", True, "main"),
#     ]
#     mg_sleep_and_assert(expected_data, retrieve_data)
#
#     # 4.
#     interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_2")
#
#     expected_data = [
#         ("instance_1", "127.0.0.1:10011", False, ""),
#         ("instance_2", "127.0.0.1:10012", False, ""),
#         ("instance_3", "127.0.0.1:10013", True, "main"),
#     ]
#     mg_sleep_and_assert(expected_data, retrieve_data)
#
#
# def test_simple_automatic_failover(connection):
#     interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)
#
#     main_cursor = connection(7687, "instance_3").cursor()
#     expected_data_on_main = [
#         ("instance_1", "127.0.0.1:10001", "sync", 0, 0, "ready"),
#         ("instance_2", "127.0.0.1:10002", "sync", 0, 0, "ready"),
#     ]
#     actual_data_on_main = sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))
#     assert actual_data_on_main == expected_data_on_main
#
#     interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")
#
#     coord_cursor = connection(7690, "coordinator").cursor()
#
#     def retrieve_data_show_repl_cluster():
#         return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW REPLICATION CLUSTER;")))
#
#     expected_data_on_coord = [
#         ("instance_1", "127.0.0.1:10011", True, "main"),
#         ("instance_2", "127.0.0.1:10012", True, "replica"),
#         ("instance_3", "127.0.0.1:10013", False, ""),
#     ]
#     mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_repl_cluster)
#
#     new_main_cursor = connection(7688, "instance_1").cursor()
#
#     def retrieve_data_show_replicas():
#         return sorted(list(execute_and_fetch_all(new_main_cursor, "SHOW REPLICAS;")))
#
#     expected_data_on_new_main = [
#         ("instance_2", "127.0.0.1:10002", "sync", 0, 0, "ready"),
#     ]
#     mg_sleep_and_assert(expected_data_on_new_main, retrieve_data_show_replicas)
#
#
# def test_registering_replica_fails_name_exists(connection):
#     interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)
#
#     coord_cursor = connection(7690, "coordinator").cursor()
#     with pytest.raises(Exception) as e:
#         execute_and_fetch_all(
#             coord_cursor,
#             "REGISTER INSTANCE instance_1 ON '127.0.0.1:10051' WITH '127.0.0.1:10111';",
#         )
#     assert str(e.value) == "Couldn't register replica instance since instance with such name already exists!"
#
#
# def test_registering_replica_fails_endpoint_exists(connection):
#     interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)
#
#     coord_cursor = connection(7690, "coordinator").cursor()
#     with pytest.raises(Exception) as e:
#         execute_and_fetch_all(
#             coord_cursor,
#             "REGISTER INSTANCE instance_5 ON '127.0.0.1:10001' WITH '127.0.0.1:10013';",
#         )
#     assert (
#         str(e.value)
#         == "Couldn't register replica because promotion on replica failed! Check logs on replica to find out more info!"
#     )
#
#
# def test_replica_instance_restarts(connection):
#     interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)
#
#     cursor = connection(7690, "coordinator").cursor()
#
#     def retrieve_data():
#         return sorted(list(execute_and_fetch_all(cursor, "SHOW REPLICATION CLUSTER;")))
#
#     expected_data_up = [
#         ("instance_1", "127.0.0.1:10011", True, "replica"),
#         ("instance_2", "127.0.0.1:10012", True, "replica"),
#         ("instance_3", "127.0.0.1:10013", True, "main"),
#     ]
#     mg_sleep_and_assert(expected_data_up, retrieve_data)
#
#     interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_1")
#
#     expected_data_down = [
#         ("instance_1", "127.0.0.1:10011", False, ""),
#         ("instance_2", "127.0.0.1:10012", True, "replica"),
#         ("instance_3", "127.0.0.1:10013", True, "main"),
#     ]
#     mg_sleep_and_assert(expected_data_down, retrieve_data)
#
#     interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_1")
#
#     mg_sleep_and_assert(expected_data_up, retrieve_data)


def test_automatic_failover_main_back_as_replica(connection):
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    coord_cursor = connection(7690, "coordinator").cursor()

    def retrieve_data_show_repl_cluster():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW REPLICATION CLUSTER;")))

    expected_data_after_failover = [
        ("instance_1", "127.0.0.1:10011", True, "main"),
        ("instance_2", "127.0.0.1:10012", True, "replica"),
        ("instance_3", "127.0.0.1:10013", False, ""),
    ]
    mg_sleep_and_assert(expected_data_after_failover, retrieve_data_show_repl_cluster)

    expected_data_after_main_coming_back = [
        ("instance_1", "127.0.0.1:10011", True, "main"),
        ("instance_2", "127.0.0.1:10012", True, "replica"),
        ("instance_3", "127.0.0.1:10013", True, "replica"),
    ]

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")
    mg_sleep_and_assert(expected_data_after_main_coming_back, retrieve_data_show_repl_cluster)


def test_automatic_failover_main_back_as_main(connection):
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_1")
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_2")
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    coord_cursor = connection(7690, "coordinator").cursor()

    def retrieve_data_show_repl_cluster():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW REPLICATION CLUSTER;")))

    expected_data_all_down = [
        ("instance_1", "127.0.0.1:10011", False, ""),
        ("instance_2", "127.0.0.1:10012", False, ""),
        ("instance_3", "127.0.0.1:10013", False, ""),
    ]

    mg_sleep_and_assert(expected_data_all_down, retrieve_data_show_repl_cluster)

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")
    expected_data_main_back = [
        ("instance_1", "127.0.0.1:10011", False, ""),
        ("instance_2", "127.0.0.1:10012", False, ""),
        ("instance_3", "127.0.0.1:10013", True, "main"),
    ]
    mg_sleep_and_assert(expected_data_main_back, retrieve_data_show_repl_cluster)

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_1")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_2")

    expected_data_replicas_back = [
        ("instance_1", "127.0.0.1:10011", True, "replica"),
        ("instance_2", "127.0.0.1:10012", True, "replica"),
        ("instance_3", "127.0.0.1:10013", True, "main"),
    ]

    mg_sleep_and_assert(expected_data_replicas_back, retrieve_data_show_repl_cluster)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
