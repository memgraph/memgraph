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
        "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10001;"],
    },
    "instance_2": {
        "args": ["--bolt-port", "7689", "--log-level", "TRACE", "--coordinator-server-port", "10012"],
        "log_file": "replica2.log",
        "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
    },
    "instance_3": {
        "args": ["--bolt-port", "7687", "--log-level", "TRACE", "--coordinator-server-port", "10013"],
        "log_file": "main.log",
        "setup_queries": [
            "REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001'",
            "REGISTER REPLICA replica_2 SYNC TO '127.0.0.1:10002'",
        ],
    },
    "coordinator": {
        "args": ["--bolt-port", "7690", "--log-level=TRACE", "--coordinator"],
        "log_file": "replica3.log",
        "setup_queries": [
            "REGISTER REPLICA instance_1 SYNC TO '127.0.0.1:10001' WITH COORDINATOR SERVER ON '127.0.0.1:10011';",
            "REGISTER REPLICA instance_2 SYNC TO '127.0.0.1:10002' WITH COORDINATOR SERVER ON '127.0.0.1:10012';",
            "REGISTER MAIN instance_3 WITH COORDINATOR SERVER ON '127.0.0.1:10013';",
        ],
    },
}


def test_show_replication_cluster(connection):
    # Goal of this test is to check the SHOW REPLICATION CLUSTER command.
    # 1. We start all replicas, main and coordinator manually: we want to be able to kill them ourselves without relying on external tooling to kill processes.
    # 2. We check that all replicas and main have the correct state: they should all be alive.
    # 3. We kill one replica. It should not appear anymore in the SHOW REPLICATION CLUSTER command.
    # 4. We kill main. It should not appear anymore in the SHOW REPLICATION CLUSTER command.

    # 1.
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    cursor = connection(7690, "coordinator").cursor()

    # 2.
    actual_data = set(execute_and_fetch_all(cursor, "SHOW REPLICATION CLUSTER;"))

    expected_column_names = {"name", "socket_address", "alive", "role"}
    actual_column_names = {x.name for x in cursor.description}
    assert actual_column_names == expected_column_names

    expected_data = {
        ("instance_1", "127.0.0.1:10011", True, "replica"),
        ("instance_2", "127.0.0.1:10012", True, "replica"),
        ("instance_3", "127.0.0.1:10013", True, "main"),
    }
    assert actual_data == expected_data

    # 3.
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_1")

    # We leave some time for the coordinator to realise the replicas are down.
    def retrieve_data():
        return set(execute_and_fetch_all(cursor, "SHOW REPLICATION CLUSTER;"))

    expected_data = {
        ("instance_2", "127.0.0.1:10012", True, "replica"),
        ("instance_3", "127.0.0.1:10013", True, "main"),
        ("instance_1", "127.0.0.1:10011", False, "replica"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    # 4.
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    expected_data = {
        ("instance_2", "127.0.0.1:10012", True, "replica"),
        ("instance_1", "127.0.0.1:10011", False, "replica"),
        ("instance_3", "127.0.0.1:10013", False, "main"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data


def test_simple_client_initiated_failover(connection):
    # 1. Start all instances
    # 2. Kill main
    # 3. Run DO FAILOVER on COORDINATOR
    # 4. Assert new config on coordinator by running show replication cluster
    # 5. Assert replicas on new main

    # 1.
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    # 2.
    main_cursor = connection(7687, "instance_3").cursor()
    expected_data_on_main = {
        ("instance_1", "127.0.0.1:10001", "sync", 0, 0, "ready"),
        ("instance_2", "127.0.0.1:10002", "sync", 0, 0, "ready"),
    }
    actual_data_on_main = set(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;"))
    assert actual_data_on_main == expected_data_on_main

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")
    coord_cursor = connection(7690, "coordinator").cursor()
    actual_data_on_coord = set(execute_and_fetch_all(coord_cursor, "SHOW REPLICATION CLUSTER;"))
    expected_data_on_coord = {
        ("instance_1", "127.0.0.1:10011", True, "replica"),
        ("instance_2", "127.0.0.1:10012", True, "replica"),
        ("instance_3", "127.0.0.1:10013", False, "main"),
    }
    assert actual_data_on_coord == expected_data_on_coord

    # 3.
    expected_data_on_coord = {
        ("instance_1", "127.0.0.1:10011", True, "main"),
        ("instance_2", "127.0.0.1:10012", True, "replica"),
    }
    failover_results = set(execute_and_fetch_all(coord_cursor, "DO FAILOVER;"))
    assert failover_results == expected_data_on_coord

    # 4.
    new_main_cursor = connection(7688, "instance_1").cursor()
    expected_data_on_new_main = {
        ("instance_2", "127.0.0.1:10002", "sync", 0, 0, "ready"),
    }
    actual_data_on_new_main = set(execute_and_fetch_all(new_main_cursor, "SHOW REPLICAS;"))
    assert actual_data_on_new_main == expected_data_on_new_main


def test_failover_fails_all_replicas_down(connection):
    # 1. Start all instances
    # 2. Kill all replicas
    # 3. Kill main
    # 4. Run DO FAILOVER on COORDINATOR. Assert exception is being thrown due to all replicas being down
    # 5. Assert cluster status didn't change

    # 1.
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    # 2.
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_1")
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_2")

    # 3.
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    coord_cursor = connection(7690, "coordinator").cursor()
    # 4.
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(coord_cursor, "DO FAILOVER;")
        assert str(e.value) == "Failover aborted since all replicas are down!"

    # 5.
    actual_data_on_coord = set(execute_and_fetch_all(coord_cursor, "SHOW REPLICATION CLUSTER;"))
    expected_data_on_coord = {
        ("instance_1", "127.0.0.1:10011", False, "replica"),
        ("instance_2", "127.0.0.1:10012", False, "replica"),
        ("instance_3", "127.0.0.1:10013", False, "main"),
    }
    assert actual_data_on_coord == expected_data_on_coord


def test_failover_fails_main_is_alive(connection):
    # 1. Start all instances
    # 2. Run DO FAILOVER on COORDINATOR. Assert exception is being thrown due to main is still live.
    # 3. Assert cluster status didn't change

    # 1.
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    # 2.
    coord_cursor = connection(7690, "coordinator").cursor()
    # 4.
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(coord_cursor, "DO FAILOVER;")
        assert str(e.value) == "Failover aborted since main is alive!"

    # 5.
    actual_data_on_coord = set(execute_and_fetch_all(coord_cursor, "SHOW REPLICATION CLUSTER;"))
    expected_data_on_coord = {
        ("instance_1", "127.0.0.1:10011", True, "replica"),
        ("instance_2", "127.0.0.1:10012", True, "replica"),
        ("instance_3", "127.0.0.1:10013", True, "main"),
    }
    assert actual_data_on_coord == expected_data_on_coord


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
