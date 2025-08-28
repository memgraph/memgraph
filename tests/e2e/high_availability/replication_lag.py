# Copyright 2025 Memgraph Ltd.
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
from common import (
  connect,
  execute_and_fetch_all,
  execute_and_ignore_dead_replica,
  get_data_path,
  get_logs_path,
  show_instances,
  wait_until_main_writeable,
)
from mg_utils import (
  mg_sleep_and_assert,
  mg_sleep_and_assert_collection,
  mg_sleep_and_assert_eval_function,
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

file = "replication_lag"


@pytest.fixture(autouse=True)
def cleanup_after_test():
    # Run the test
    yield
    # Stop + delete directories after running the test
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


def get_instances_description_no_setup(test_name: str):
    return {
        "instance_1": {
            "args": [
                "--bolt-port",
                "7687",
                "--log-level",
                "TRACE",
                "--management-port",
                "10011",
                "--storage-wal-file-size-kib=1",
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
                "--storage-wal-file-size-kib=1",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_2",
            "setup_queries": [],
        },
        "instance_3": {
            "args": [
                "--bolt-port",
                "7689",
                "--log-level",
                "TRACE",
                "--management-port",
                "10013",
                "--storage-wal-file-size-kib=1",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_3",
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


# Uses STRICT_SYNC and ASYNC replicas
def get_strict_sync_cluster():
    return [
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
        "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}",
        "REGISTER INSTANCE instance_1 AS STRICT_SYNC WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
        "REGISTER INSTANCE instance_2 AS ASYNC WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
        "REGISTER INSTANCE instance_3 AS STRICT_SYNC WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
        "SET INSTANCE instance_3 TO MAIN",
    ]


# Uses SYNC and ASYNC replicas
def get_sync_cluster():
    return [
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
        "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}",
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
        "REGISTER INSTANCE instance_2 AS ASYNC WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
        "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
        "SET INSTANCE instance_3 TO MAIN",
    ]


# Executes setup queries and returns cluster info
def setup_cluster(test_name, setup_queries):
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)
    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)
    coord_cursor_3 = connect(host="localhost", port=7692).cursor()

    for query in setup_queries:
        execute_and_fetch_all(coord_cursor_3, query)
    return inner_instances_description


def retrieve_lag(cursor):
    return execute_and_fetch_all(cursor, "SHOW REPLICATION LAG;")


@pytest.mark.parametrize("cluster", ["strict_sync", "sync"])
def test_replication_lag_strict_sync(test_name, cluster):
    func = None
    if cluster == "strict_sync":
        func = get_strict_sync_cluster
    else:
        func = get_sync_cluster

    inner_instances_description = setup_cluster(test_name, func())
    instance3_cursor = connect(host="localhost", port=7689).cursor()
    coord3_cursor = connect(host="localhost", port=7692).cursor()
    # Commit a txn on main and assert that num_committed_txns is incremented. This will trigger sending PrepareCommit
    # and CurrentWalRpc from main to replica
    execute_and_fetch_all(instance3_cursor, "CREATE ()")
    execute_and_fetch_all(instance3_cursor, "CREATE ()")
    execute_and_fetch_all(instance3_cursor, "CREATE ()")

    expected_data = [
        (
            "instance_1",
            {"memgraph": {"num_committed_txns": 3, "num_txns_behind_main": 0}},
        ),
        (
            "instance_2",
            {"memgraph": {"num_committed_txns": 3, "num_txns_behind_main": 0}},
        ),
        (
            "instance_3",
            {"memgraph": {"num_committed_txns": 3, "num_txns_behind_main": 0}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, partial(retrieve_lag, coord3_cursor))

    # 2ASYNC replica goes down, check that it is behind MAIN. WalFilesRpc should be sent
    interactive_mg_runner.kill(inner_instances_description, "instance_2")
    execute_and_fetch_all(instance3_cursor, "CREATE (:MyNode {embedding: [x IN range(1, 1024) | x]});")
    execute_and_fetch_all(instance3_cursor, "CREATE (:MyNode {embedding: [x IN range(1, 1024) | x]});")
    execute_and_fetch_all(instance3_cursor, "CREATE (:MyNode {embedding: [x IN range(1, 1024) | x]});")
    execute_and_fetch_all(instance3_cursor, "CREATE (:MyNode {embedding: [x IN range(1, 1024) | x]});")
    expected_data = [
        (
            "instance_1",
            {"memgraph": {"num_committed_txns": 7, "num_txns_behind_main": 0}},
        ),
        (
            "instance_2",
            {"memgraph": {"num_committed_txns": 3, "num_txns_behind_main": 4}},
        ),
        (
            "instance_3",
            {"memgraph": {"num_committed_txns": 7, "num_txns_behind_main": 0}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, partial(retrieve_lag, coord3_cursor))

    # Check that main correctly incremented after sending WalFilesRpc to the recovering replica
    interactive_mg_runner.start(inner_instances_description, "instance_2")
    expected_data = [
        (
            "instance_1",
            {"memgraph": {"num_committed_txns": 7, "num_txns_behind_main": 0}},
        ),
        (
            "instance_2",
            {"memgraph": {"num_committed_txns": 7, "num_txns_behind_main": 0}},
        ),
        (
            "instance_3",
            {"memgraph": {"num_committed_txns": 7, "num_txns_behind_main": 0}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, partial(retrieve_lag, coord3_cursor))

    # ASYNC replica goes down, test main's counting capabilities when replica is recovered using SNAPSHOT
    interactive_mg_runner.kill(inner_instances_description, "instance_2")
    execute_and_fetch_all(instance3_cursor, "CREATE ()")
    execute_and_fetch_all(instance3_cursor, "CREATE ()")
    execute_and_fetch_all(instance3_cursor, "CREATE ()")
    execute_and_fetch_all(instance3_cursor, "CREATE ()")
    execute_and_fetch_all(instance3_cursor, "CREATE SNAPSHOT")
    interactive_mg_runner.start(inner_instances_description, "instance_2")
    expected_data = [
        (
            "instance_1",
            {"memgraph": {"num_committed_txns": 11, "num_txns_behind_main": 0}},
        ),
        (
            "instance_2",
            {"memgraph": {"num_committed_txns": 11, "num_txns_behind_main": 0}},
        ),
        (
            "instance_3",
            {"memgraph": {"num_committed_txns": 11, "num_txns_behind_main": 0}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, partial(retrieve_lag, coord3_cursor))

    # Restart MAIN instance without triggering a failover and check that it sees the same output as before
    # Force a recovery from snapshot to test that part
    interactive_mg_runner.kill(inner_instances_description, "instance_1")
    interactive_mg_runner.kill(inner_instances_description, "instance_2")
    interactive_mg_runner.kill(inner_instances_description, "instance_3")
    interactive_mg_runner.start(inner_instances_description, "instance_3")
    interactive_mg_runner.start(inner_instances_description, "instance_1")
    interactive_mg_runner.start(inner_instances_description, "instance_2")
    leader_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]
    mg_sleep_and_assert(leader_data, partial(show_instances, coord3_cursor))
    mg_sleep_and_assert_collection(expected_data, partial(retrieve_lag, coord3_cursor))

    # Restart MAIN instance without triggering a failover and check that it sees the same output as before
    # Force a recovery from snapshot+wals to test that part
    instance3_cursor = connect(host="localhost", port=7689).cursor()
    wait_until_main_writeable(instance3_cursor, "CREATE (:MyNode {embedding: [x IN range(1, 1024) | x]});")
    execute_and_fetch_all(instance3_cursor, "CREATE (:MyNode {embedding: [x IN range(1, 1024) | x]});")
    execute_and_fetch_all(instance3_cursor, "CREATE (:MyNode {embedding: [x IN range(1, 1024) | x]});")
    interactive_mg_runner.kill(inner_instances_description, "instance_1")
    interactive_mg_runner.kill(inner_instances_description, "instance_2")
    interactive_mg_runner.kill(inner_instances_description, "instance_3")
    interactive_mg_runner.start(inner_instances_description, "instance_3")
    interactive_mg_runner.start(inner_instances_description, "instance_1")
    interactive_mg_runner.start(inner_instances_description, "instance_2")
    leader_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]
    mg_sleep_and_assert(leader_data, partial(show_instances, coord3_cursor))
    expected_data = [
        (
            "instance_1",
            {"memgraph": {"num_committed_txns": 14, "num_txns_behind_main": 0}},
        ),
        (
            "instance_2",
            {"memgraph": {"num_committed_txns": 14, "num_txns_behind_main": 0}},
        ),
        (
            "instance_3",
            {"memgraph": {"num_committed_txns": 14, "num_txns_behind_main": 0}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, partial(retrieve_lag, coord3_cursor))

    # 6. Check that the new MAIN will correct info after failover
    interactive_mg_runner.kill(inner_instances_description, "instance_3")
    # Instance 1 is new MAIN because by default we forbid failovering to ASYNC replica
    leader_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "main"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "down", "unknown"),
    ]
    mg_sleep_and_assert(leader_data, partial(show_instances, coord3_cursor))
    interactive_mg_runner.start(inner_instances_description, "instance_3")
    expected_data = [
        (
            "instance_1",
            {"memgraph": {"num_committed_txns": 14, "num_txns_behind_main": 0}},
        ),
        (
            "instance_2",
            {"memgraph": {"num_committed_txns": 14, "num_txns_behind_main": 0}},
        ),
        (
            "instance_3",
            {"memgraph": {"num_committed_txns": 14, "num_txns_behind_main": 0}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, partial(retrieve_lag, coord3_cursor))


def test_replication_lag_failover(test_name):
    inner_instances_description = setup_cluster(test_name, get_sync_cluster())
    instance3_cursor = connect(host="localhost", port=7689).cursor()
    coord3_cursor = connect(host="localhost", port=7692).cursor()

    # Set max lag to two txns
    execute_and_fetch_all(coord3_cursor, "SET COORDINATOR SETTING 'max_replica_lag' TO '2'")

    # Kill both replicas
    interactive_mg_runner.kill(inner_instances_description, "instance_1")
    interactive_mg_runner.kill(inner_instances_description, "instance_2")

    # Commit 3 txns on main
    execute_and_ignore_dead_replica(instance3_cursor, "CREATE ()")
    execute_and_ignore_dead_replica(instance3_cursor, "CREATE ()")
    execute_and_ignore_dead_replica(instance3_cursor, "CREATE ()")

    # So that coordinator sends StateCheckRpc
    time.sleep(3)

    # Kill main and start replicas, assert that they shouldn't become main because they are too much behind
    interactive_mg_runner.kill(inner_instances_description, "instance_3")
    interactive_mg_runner.start(inner_instances_description, "instance_1")
    interactive_mg_runner.start(inner_instances_description, "instance_2")

    # Sleep so that we wait enough for a failover to be started
    time.sleep(5)

    leader_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "down", "unknown"),
    ]
    mg_sleep_and_assert(leader_data, partial(show_instances, coord3_cursor))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
