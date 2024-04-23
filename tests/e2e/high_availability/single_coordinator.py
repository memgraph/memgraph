# Copyright 2022 Memgraph Ltd.
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import os
import shutil
import sys
import tempfile

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all, safe_execute
from mg_utils import mg_sleep_and_assert, mg_sleep_and_assert_collection

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

TEMP_DIR = tempfile.TemporaryDirectory().name

MEMGRAPH_INSTANCES_DESCRIPTION = {
    "instance_1": {
        "args": [
            "--experimental-enabled=high-availability",
            "--bolt-port",
            "7688",
            "--log-level",
            "TRACE",
            "--management-port",
            "10011",
            "--replication-restore-state-on-startup=true",
            "--storage-recover-on-startup=false",
            "--data-recovery-on-startup=false",
        ],
        "log_file": "instance_1.log",
        "data_directory": f"{TEMP_DIR}/instance_1",
        "setup_queries": [],
    },
    "instance_2": {
        "args": [
            "--experimental-enabled=high-availability",
            "--bolt-port",
            "7689",
            "--log-level",
            "TRACE",
            "--management-port",
            "10012",
            "--replication-restore-state-on-startup=true",
            "--storage-recover-on-startup=false",
            "--data-recovery-on-startup=false",
        ],
        "log_file": "instance_2.log",
        "data_directory": f"{TEMP_DIR}/instance_2",
        "setup_queries": [],
    },
    "instance_3": {
        "args": [
            "--experimental-enabled=high-availability",
            "--bolt-port",
            "7687",
            "--log-level",
            "TRACE",
            "--management-port",
            "10013",
            "--replication-restore-state-on-startup=true",
            "--storage-recover-on-startup=false",
            "--data-recovery-on-startup=false",
        ],
        "log_file": "instance_3.log",
        "data_directory": f"{TEMP_DIR}/instance_3",
        "setup_queries": [],
    },
    "coordinator": {
        "args": [
            "--experimental-enabled=high-availability",
            "--bolt-port",
            "7690",
            "--log-level=TRACE",
            "--coordinator-id=1",
            "--coordinator-port=10111",
        ],
        "log_file": "coordinator.log",
        "setup_queries": [
            "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': '127.0.0.1:7688', 'management_server': '127.0.0.1:10011', 'replication_server': '127.0.0.1:10001'};",
            "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': '127.0.0.1:7689', 'management_server': '127.0.0.1:10012', 'replication_server': '127.0.0.1:10002'};",
            "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': '127.0.0.1:7687', 'management_server': '127.0.0.1:10013', 'replication_server': '127.0.0.1:10003'};",
            "SET INSTANCE instance_3 TO MAIN",
        ],
    },
}


@pytest.mark.parametrize("data_recovery", ["false", "true"])
def test_replication_works_on_failover_replica_1_epoch_2_commits_away(data_recovery):
    # Goal of this test is to check the replication works after failover command.
    # 1. We start all replicas, main and coordinator manually
    # 2. We check that main has correct state
    # 3. Create initial data on MAIN
    # 4. Expect data to be copied on all replicas
    # 5. Kill instance_1 (replica 1)
    # 6. Create data on MAIN and expect to be copied to only one replica (instance_2)
    # 7. Kill main
    # 8. Instance_2 new MAIN
    # 9. Create vertex on instance 2
    # 10. Start instance_1(it should have one commit on old epoch and new epoch with new commit shouldn't be replicated)
    # 11. Expect data to be copied on instance_1
    # 12. Start old MAIN (instance_3)
    # 13. Expect data to be copied to instance_3

    temp_dir = tempfile.TemporaryDirectory().name

    MEMGRAPH_INNER_INSTANCES_DESCRIPTION = {
        "instance_1": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7688",
                "--log-level",
                "TRACE",
                "--management-port",
                "10011",
                "--replication-restore-state-on-startup",
                "true",
                f"--data-recovery-on-startup={data_recovery}",
                "--storage-recover-on-startup=false",
            ],
            "log_file": "instance_1.log",
            "data_directory": f"{temp_dir}/instance_1",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7689",
                "--log-level",
                "TRACE",
                "--management-port",
                "10012",
                "--replication-restore-state-on-startup",
                "true",
                f"--data-recovery-on-startup={data_recovery}",
                "--storage-recover-on-startup=false",
            ],
            "log_file": "instance_2.log",
            "data_directory": f"{temp_dir}/instance_2",
            "setup_queries": [],
        },
        "instance_3": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7687",
                "--log-level",
                "TRACE",
                "--management-port",
                "10013",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                f"{data_recovery}",
                "--storage-recover-on-startup=false",
            ],
            "log_file": "instance_3.log",
            "data_directory": f"{temp_dir}/instance_3",
            "setup_queries": [],
        },
        "coordinator": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7690",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10111",
            ],
            "log_file": "coordinator.log",
            "setup_queries": [
                "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': '127.0.0.1:7688', 'management_server': '127.0.0.1:10011', 'replication_server': '127.0.0.1:10001'};",
                "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': '127.0.0.1:7689', 'management_server': '127.0.0.1:10012', 'replication_server': '127.0.0.1:10002'};",
                "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': '127.0.0.1:7687', 'management_server': '127.0.0.1:10013', 'replication_server': '127.0.0.1:10003'};",
                "SET INSTANCE instance_3 TO MAIN",
            ],
        },
    }

    # 1
    interactive_mg_runner.start_all(MEMGRAPH_INNER_INSTANCES_DESCRIPTION)

    # 2
    main_cursor = connect(host="localhost", port=7687).cursor()

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))

    expected_data_on_main = [
        (
            "instance_1",
            "127.0.0.1:10001",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
        (
            "instance_2",
            "127.0.0.1:10002",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data_on_main, retrieve_data_show_replicas)

    # 3
    execute_and_fetch_all(main_cursor, "CREATE (:EpochVertex1 {prop:1});")

    # 4

    instance_1_cursor = connect(host="localhost", port=7688).cursor()
    instance_2_cursor = connect(host="localhost", port=7689).cursor()

    assert execute_and_fetch_all(instance_1_cursor, "MATCH (n) RETURN count(n);")[0][0] == 1
    assert execute_and_fetch_all(instance_2_cursor, "MATCH (n) RETURN count(n);")[0][0] == 1

    # 5
    interactive_mg_runner.kill(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "instance_1")

    # 6

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(main_cursor, "CREATE (:EpochVertex1 {prop:2});")
    assert "At least one SYNC replica has not confirmed committing last transaction." in str(e.value)

    assert execute_and_fetch_all(instance_2_cursor, "MATCH (n) RETURN count(n);")[0][0] == 2

    # 7
    interactive_mg_runner.kill(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "instance_3")

    # 8.
    coord_cursor = connect(host="localhost", port=7690).cursor()

    def retrieve_data_show_instances():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))

    expected_data_on_coord = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "down", "unknown"),
        ("instance_2", "", "127.0.0.1:10012", "up", "main"),
        ("instance_3", "", "127.0.0.1:10013", "down", "unknown"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_instances)

    # 9

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(instance_2_cursor, "CREATE (:Epoch3 {prop:3});")
    assert "At least one SYNC replica has not confirmed committing last transaction." in str(e.value)

    # 10
    interactive_mg_runner.start(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "instance_1")

    new_expected_data_on_coord = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "up", "replica"),
        ("instance_2", "", "127.0.0.1:10012", "up", "main"),
        ("instance_3", "", "127.0.0.1:10013", "down", "unknown"),
    ]
    mg_sleep_and_assert(new_expected_data_on_coord, retrieve_data_show_instances)

    # 11
    instance_1_cursor = connect(host="localhost", port=7688).cursor()

    def get_vertex_count():
        return execute_and_fetch_all(instance_1_cursor, "MATCH (n) RETURN count(n)")[0][0]

    mg_sleep_and_assert(3, get_vertex_count)

    # 12

    interactive_mg_runner.start(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "instance_3")

    new_expected_data_on_coord = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "up", "replica"),
        ("instance_2", "", "127.0.0.1:10012", "up", "main"),
        ("instance_3", "", "127.0.0.1:10013", "up", "replica"),
    ]
    mg_sleep_and_assert(new_expected_data_on_coord, retrieve_data_show_instances)

    # 13

    instance_3_cursor = connect(host="localhost", port=7687).cursor()

    def get_vertex_count():
        return execute_and_fetch_all(instance_3_cursor, "MATCH (n) RETURN count(n)")[0][0]

    mg_sleep_and_assert(3, get_vertex_count)


@pytest.mark.parametrize("data_recovery", ["false", "true"])
def test_replication_works_on_failover_replica_2_epochs_more_commits_away(data_recovery):
    # Goal of this test is to check the replication works after failover command if one
    # instance missed couple of epochs but data is still available on one of the instances

    # 1. We start all replicas, main and coordinator manually
    # 2. Main does commit
    # 3. instance_2 down
    # 4. Main commits more
    # 5. Main down
    # 6. Instance_1 new main
    # 7. Instance 1 commits
    # 8. Instance 4 gets data
    # 9. Instance 1 dies
    # 10. Instance 4 new main
    # 11. Instance 4 commits
    # 12. Instance 2 wakes up
    # 13. Instance 2 gets data from old epochs
    # 14. All other instances wake up
    # 15. Everything is replicated

    temp_dir = tempfile.TemporaryDirectory().name

    MEMGRAPH_INNER_INSTANCES_DESCRIPTION = {
        "instance_1": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7688",
                "--log-level",
                "TRACE",
                "--management-port",
                "10011",
                "--replication-restore-state-on-startup",
                "true",
                f"--data-recovery-on-startup={data_recovery}",
                "--storage-recover-on-startup=false",
            ],
            "log_file": "instance_1.log",
            "data_directory": f"{temp_dir}/instance_1",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7689",
                "--log-level",
                "TRACE",
                "--management-port",
                "10012",
                "--replication-restore-state-on-startup",
                "true",
                f"--data-recovery-on-startup={data_recovery}",
                "--storage-recover-on-startup=false",
            ],
            "log_file": "instance_2.log",
            "data_directory": f"{temp_dir}/instance_2",
            "setup_queries": [],
        },
        "instance_3": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7687",
                "--log-level",
                "TRACE",
                "--management-port",
                "10013",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                f"{data_recovery}",
                "--storage-recover-on-startup=false",
            ],
            "log_file": "instance_3.log",
            "data_directory": f"{temp_dir}/instance_3",
            "setup_queries": [],
        },
        "instance_4": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7691",
                "--log-level",
                "TRACE",
                "--management-port",
                "10014",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                f"{data_recovery}",
                "--storage-recover-on-startup=false",
            ],
            "log_file": "instance_4.log",
            "data_directory": f"{temp_dir}/instance_4",
            "setup_queries": [],
        },
        "coordinator": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7690",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10111",
            ],
            "log_file": "coordinator.log",
            "setup_queries": [
                "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': '127.0.0.1:7688', 'management_server': '127.0.0.1:10011', 'replication_server': '127.0.0.1:10001'};",
                "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': '127.0.0.1:7689', 'management_server': '127.0.0.1:10012', 'replication_server': '127.0.0.1:10002'};",
                "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': '127.0.0.1:7687', 'management_server': '127.0.0.1:10013', 'replication_server': '127.0.0.1:10003'};",
                "REGISTER INSTANCE instance_4 WITH CONFIG {'bolt_server': '127.0.0.1:7691', 'management_server': '127.0.0.1:10014', 'replication_server': '127.0.0.1:10004'};",
                "SET INSTANCE instance_3 TO MAIN",
            ],
        },
    }

    # 1

    interactive_mg_runner.start_all(MEMGRAPH_INNER_INSTANCES_DESCRIPTION)

    expected_data_on_main = [
        (
            "instance_1",
            "127.0.0.1:10001",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
        (
            "instance_2",
            "127.0.0.1:10002",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
        (
            "instance_4",
            "127.0.0.1:10004",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
    ]

    main_cursor = connect(host="localhost", port=7687).cursor()

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))

    mg_sleep_and_assert_collection(expected_data_on_main, retrieve_data_show_replicas)

    # 2

    execute_and_fetch_all(main_cursor, "CREATE (:EpochVertex1 {prop:1});")
    execute_and_fetch_all(main_cursor, "CREATE (:EpochVertex1 {prop:2});")

    instance_1_cursor = connect(host="localhost", port=7688).cursor()
    instance_2_cursor = connect(host="localhost", port=7689).cursor()
    instance_4_cursor = connect(host="localhost", port=7691).cursor()

    assert execute_and_fetch_all(instance_1_cursor, "MATCH (n) RETURN count(n);")[0][0] == 2
    assert execute_and_fetch_all(instance_2_cursor, "MATCH (n) RETURN count(n);")[0][0] == 2
    assert execute_and_fetch_all(instance_4_cursor, "MATCH (n) RETURN count(n);")[0][0] == 2

    # 3

    interactive_mg_runner.kill(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "instance_2")

    coord_cursor = connect(host="localhost", port=7690).cursor()

    def retrieve_data_show_instances():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))

    expected_data_on_coord = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "up", "replica"),
        ("instance_2", "", "127.0.0.1:10012", "down", "unknown"),
        ("instance_3", "", "127.0.0.1:10013", "up", "main"),
        ("instance_4", "", "127.0.0.1:10014", "up", "replica"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_instances)

    # 4

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(main_cursor, "CREATE (:EpochVertex1 {prop:1});")
    assert "At least one SYNC replica has not confirmed committing last transaction." in str(e.value)

    assert execute_and_fetch_all(instance_1_cursor, "MATCH (n) RETURN count(n);")[0][0] == 3
    assert execute_and_fetch_all(instance_4_cursor, "MATCH (n) RETURN count(n);")[0][0] == 3

    # 5

    interactive_mg_runner.kill(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "instance_3")

    # 6

    expected_data_on_coord = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "up", "main"),
        ("instance_2", "", "127.0.0.1:10012", "down", "unknown"),
        ("instance_3", "", "127.0.0.1:10013", "down", "unknown"),
        ("instance_4", "", "127.0.0.1:10014", "up", "replica"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_instances)

    # 7

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(instance_1_cursor, "CREATE (:Epoch2Vertex {prop:1});")
    assert "At least one SYNC replica has not confirmed committing last transaction." in str(e.value)

    # 8

    assert execute_and_fetch_all(instance_4_cursor, "MATCH (n) RETURN count(n);")[0][0] == 4

    # 9

    interactive_mg_runner.kill(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "instance_1")

    # 10

    expected_data_on_coord = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "down", "unknown"),
        ("instance_2", "", "127.0.0.1:10012", "down", "unknown"),
        ("instance_3", "", "127.0.0.1:10013", "down", "unknown"),
        ("instance_4", "", "127.0.0.1:10014", "up", "main"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_instances)

    # 11

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(instance_4_cursor, "CREATE (:Epoch3Vertex {prop:1});")
    assert "At least one SYNC replica has not confirmed committing last transaction." in str(e.value)

    # 12

    interactive_mg_runner.start(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "instance_2")

    expected_data_on_coord = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "down", "unknown"),
        ("instance_2", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "", "127.0.0.1:10013", "down", "unknown"),
        ("instance_4", "", "127.0.0.1:10014", "up", "main"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_instances)

    # 13

    instance_2_cursor = connect(host="localhost", port=7689).cursor()

    def get_vertex_count():
        return execute_and_fetch_all(instance_2_cursor, "MATCH (n) RETURN count(n)")[0][0]

    mg_sleep_and_assert(5, get_vertex_count)

    # 14

    interactive_mg_runner.start(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "instance_1")
    interactive_mg_runner.start(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "instance_3")

    expected_data_on_coord = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "up", "replica"),
        ("instance_2", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "", "127.0.0.1:10013", "up", "replica"),
        ("instance_4", "", "127.0.0.1:10014", "up", "main"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_instances)

    # 15
    instance_1_cursor = connect(host="localhost", port=7688).cursor()
    instance_4_cursor = connect(host="localhost", port=7691).cursor()

    def get_vertex_count():
        return execute_and_fetch_all(instance_1_cursor, "MATCH (n) RETURN count(n)")[0][0]

    mg_sleep_and_assert(5, get_vertex_count)

    def get_vertex_count():
        return execute_and_fetch_all(instance_4_cursor, "MATCH (n) RETURN count(n)")[0][0]

    mg_sleep_and_assert(5, get_vertex_count)


@pytest.mark.parametrize("data_recovery", ["true"])
def test_replication_forcefully_works_on_failover_replica_misses_epoch(data_recovery):
    # Goal of this test is to check the replication works forcefully if replica misses epoch
    # 1. We start all replicas, main and coordinator manually
    # 2. We check that main has correct state
    # 3. Create initial data on MAIN
    # 4. Expect data to be copied on all replicas
    # 5. Kill instance_1 ( this one will miss complete epoch)
    # 6. Kill main (instance_3)
    # 7. Instance_2
    # 8. Instance_2 commits
    # 9. Instance_2 down
    # 10. instance_4 down
    # 11. Instance 1 up (missed epoch)
    # 12 Instance 1 new main
    # 13 instance 2 up
    # 14 Force data from instance 1 to instance 2

    temp_dir = tempfile.TemporaryDirectory().name

    MEMGRAPH_INNER_INSTANCES_DESCRIPTION = {
        "instance_1": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7688",
                "--log-level",
                "TRACE",
                "--management-port",
                "10011",
                "--replication-restore-state-on-startup",
                "true",
                f"--data-recovery-on-startup={data_recovery}",
                "--storage-recover-on-startup=false",
            ],
            "log_file": "instance_1.log",
            "data_directory": f"{temp_dir}/instance_1",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7689",
                "--log-level",
                "TRACE",
                "--management-port",
                "10012",
                "--replication-restore-state-on-startup",
                "true",
                f"--data-recovery-on-startup={data_recovery}",
                "--storage-recover-on-startup=false",
            ],
            "log_file": "instance_2.log",
            "data_directory": f"{temp_dir}/instance_2",
            "setup_queries": [],
        },
        "instance_3": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7687",
                "--log-level",
                "TRACE",
                "--management-port",
                "10013",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                f"{data_recovery}",
                "--storage-recover-on-startup=false",
            ],
            "log_file": "instance_3.log",
            "data_directory": f"{temp_dir}/instance_3",
            "setup_queries": [],
        },
        "instance_4": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7691",
                "--log-level",
                "TRACE",
                "--management-port",
                "10014",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                f"{data_recovery}",
                "--storage-recover-on-startup=false",
            ],
            "log_file": "instance_4.log",
            "data_directory": f"{temp_dir}/instance_4",
            "setup_queries": [],
        },
        "coordinator": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7690",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10111",
            ],
            "log_file": "coordinator.log",
            "setup_queries": [
                "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': '127.0.0.1:7688', 'management_server': '127.0.0.1:10011', 'replication_server': '127.0.0.1:10001'};",
                "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': '127.0.0.1:7689', 'management_server': '127.0.0.1:10012', 'replication_server': '127.0.0.1:10002'};",
                "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': '127.0.0.1:7687', 'management_server': '127.0.0.1:10013', 'replication_server': '127.0.0.1:10003'};",
                "REGISTER INSTANCE instance_4 WITH CONFIG {'bolt_server': '127.0.0.1:7691', 'management_server': '127.0.0.1:10014', 'replication_server': '127.0.0.1:10004'};",
                "SET INSTANCE instance_3 TO MAIN",
            ],
        },
    }

    # 1

    interactive_mg_runner.start_all(MEMGRAPH_INNER_INSTANCES_DESCRIPTION)

    # 2

    main_cursor = connect(host="localhost", port=7687).cursor()
    expected_data_on_main = [
        (
            "instance_1",
            "127.0.0.1:10001",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
        (
            "instance_2",
            "127.0.0.1:10002",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
        (
            "instance_4",
            "127.0.0.1:10004",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
    ]

    main_cursor = connect(host="localhost", port=7687).cursor()

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))

    mg_sleep_and_assert_collection(expected_data_on_main, retrieve_data_show_replicas)

    coord_cursor = connect(host="localhost", port=7690).cursor()

    def retrieve_data_show_instances():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))

    expected_data_on_coord = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "up", "replica"),
        ("instance_2", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "", "127.0.0.1:10013", "up", "main"),
        ("instance_4", "", "127.0.0.1:10014", "up", "replica"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_instances)

    # 3

    execute_and_fetch_all(main_cursor, "CREATE (:Epoch1Vertex {prop:1});")
    execute_and_fetch_all(main_cursor, "CREATE (:Epoch1Vertex {prop:2});")

    # 4
    instance_1_cursor = connect(host="localhost", port=7688).cursor()
    instance_2_cursor = connect(host="localhost", port=7689).cursor()
    instance_4_cursor = connect(host="localhost", port=7691).cursor()

    assert execute_and_fetch_all(instance_1_cursor, "MATCH (n) RETURN count(n);")[0][0] == 2
    assert execute_and_fetch_all(instance_2_cursor, "MATCH (n) RETURN count(n);")[0][0] == 2
    assert execute_and_fetch_all(instance_4_cursor, "MATCH (n) RETURN count(n);")[0][0] == 2

    # 5

    interactive_mg_runner.kill(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "instance_1")

    # 6
    interactive_mg_runner.kill(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "instance_3")

    # 7

    expected_data_on_coord = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "down", "unknown"),
        ("instance_2", "", "127.0.0.1:10012", "up", "main"),
        ("instance_3", "", "127.0.0.1:10013", "down", "unknown"),
        ("instance_4", "", "127.0.0.1:10014", "up", "replica"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_instances)

    # 8

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(instance_2_cursor, "CREATE (:Epoch2Vertex {prop:1});")
    assert "At least one SYNC replica has not confirmed committing last transaction." in str(e.value)

    def get_vertex_count():
        return execute_and_fetch_all(instance_4_cursor, "MATCH (n) RETURN count(n)")[0][0]

    mg_sleep_and_assert(3, get_vertex_count)

    assert execute_and_fetch_all(instance_4_cursor, "MATCH (n) RETURN count(n);")[0][0] == 3

    # 9

    interactive_mg_runner.kill(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "instance_2")

    # 10

    interactive_mg_runner.kill(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "instance_4")

    # 11

    interactive_mg_runner.start(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "instance_1")

    expected_data_on_coord = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "up", "main"),
        ("instance_2", "", "127.0.0.1:10012", "down", "unknown"),
        ("instance_3", "", "127.0.0.1:10013", "down", "unknown"),
        ("instance_4", "", "127.0.0.1:10014", "down", "unknown"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_instances)

    # 12

    interactive_mg_runner.start(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "instance_2")

    # 13

    expected_data_on_coord = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "up", "main"),
        ("instance_2", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "", "127.0.0.1:10013", "down", "unknown"),
        ("instance_4", "", "127.0.0.1:10014", "down", "unknown"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_instances)

    # 12
    instance_1_cursor = connect(host="localhost", port=7688).cursor()
    instance_2_cursor = connect(host="localhost", port=7689).cursor()

    def get_vertex_count():
        return execute_and_fetch_all(instance_1_cursor, "MATCH (n) RETURN count(n)")[0][0]

    mg_sleep_and_assert(2, get_vertex_count)

    def get_vertex_count():
        return execute_and_fetch_all(instance_2_cursor, "MATCH (n) RETURN count(n)")[0][0]

    mg_sleep_and_assert(2, get_vertex_count)

    # 13
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(instance_1_cursor, "CREATE (:Epoch3Vertex {prop:1});")
    assert "At least one SYNC replica has not confirmed committing last transaction." in str(e.value)

    # 14

    def get_vertex_objects_func_creator(cursor):
        def get_vertex_objects():
            return list(
                execute_and_fetch_all(
                    cursor, "MATCH (n) " "WITH labels(n) as labels, properties(n) as props " "RETURN labels[0], props;"
                )
            )

        return get_vertex_objects

    vertex_objects = [("Epoch1Vertex", {"prop": 1}), ("Epoch1Vertex", {"prop": 2}), ("Epoch3Vertex", {"prop": 1})]

    mg_sleep_and_assert_collection(vertex_objects, get_vertex_objects_func_creator(instance_1_cursor))

    mg_sleep_and_assert_collection(vertex_objects, get_vertex_objects_func_creator(instance_2_cursor))

    # 15


@pytest.mark.parametrize("data_recovery", ["false", "true"])
def test_replication_correct_replica_chosen_up_to_date_data(data_recovery):
    # Goal of this test is to check that correct replica instance as new MAIN is chosen
    # 1. We start all replicas, main and coordinator manually
    # 2. We check that main has correct state
    # 3. Create initial data on MAIN
    # 4. Expect data to be copied on all replicas
    # 5. Kill instance_1 ( this one will miss complete epoch)
    # 6. Kill main (instance_3)
    # 7. Instance_2 new MAIN
    # 8. Instance_2 commits and replicates data
    # 9. Instance_4 down (not main)
    # 10. instance_2 down (MAIN), instance 1 up (missed epoch),
    # instance 4 up (In this case we should always choose instance_4 because it has up-to-date data)
    # 11 Instance 4 new main
    # 12 instance_1 gets up-to-date data, instance_4 has all data

    temp_dir = tempfile.TemporaryDirectory().name

    MEMGRAPH_INNER_INSTANCES_DESCRIPTION = {
        "instance_1": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7688",
                "--log-level",
                "TRACE",
                "--management-port",
                "10011",
                "--replication-restore-state-on-startup",
                "true",
                f"--data-recovery-on-startup={data_recovery}",
                "--storage-recover-on-startup=false",
            ],
            "log_file": "instance_1.log",
            "data_directory": f"{temp_dir}/instance_1",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7689",
                "--log-level",
                "TRACE",
                "--management-port",
                "10012",
                "--replication-restore-state-on-startup",
                "true",
                f"--data-recovery-on-startup={data_recovery}",
                "--storage-recover-on-startup=false",
            ],
            "log_file": "instance_2.log",
            "data_directory": f"{temp_dir}/instance_2",
            "setup_queries": [],
        },
        "instance_3": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7687",
                "--log-level",
                "TRACE",
                "--management-port",
                "10013",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                f"{data_recovery}",
                "--storage-recover-on-startup=false",
            ],
            "log_file": "instance_3.log",
            "data_directory": f"{temp_dir}/instance_3",
            "setup_queries": [],
        },
        "instance_4": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7691",
                "--log-level",
                "TRACE",
                "--management-port",
                "10014",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                f"{data_recovery}",
                "--storage-recover-on-startup=false",
            ],
            "log_file": "instance_4.log",
            "data_directory": f"{temp_dir}/instance_4",
            "setup_queries": [],
        },
        "coordinator": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7690",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10111",
            ],
            "log_file": "coordinator.log",
            "setup_queries": [
                "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': '127.0.0.1:7688', 'management_server': '127.0.0.1:10011', 'replication_server': '127.0.0.1:10001'};",
                "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': '127.0.0.1:7689', 'management_server': '127.0.0.1:10012', 'replication_server': '127.0.0.1:10002'};",
                "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': '127.0.0.1:7687', 'management_server': '127.0.0.1:10013', 'replication_server': '127.0.0.1:10003'};",
                "REGISTER INSTANCE instance_4 WITH CONFIG {'bolt_server': '127.0.0.1:7691', 'management_server': '127.0.0.1:10014', 'replication_server': '127.0.0.1:10004'};",
                "SET INSTANCE instance_3 TO MAIN",
            ],
        },
    }

    # 1

    interactive_mg_runner.start_all(MEMGRAPH_INNER_INSTANCES_DESCRIPTION)

    # 2

    main_cursor = connect(host="localhost", port=7687).cursor()
    expected_data_on_main = [
        (
            "instance_1",
            "127.0.0.1:10001",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
        (
            "instance_2",
            "127.0.0.1:10002",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
        (
            "instance_4",
            "127.0.0.1:10004",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
    ]

    main_cursor = connect(host="localhost", port=7687).cursor()

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))

    mg_sleep_and_assert_collection(expected_data_on_main, retrieve_data_show_replicas)

    coord_cursor = connect(host="localhost", port=7690).cursor()

    def retrieve_data_show_instances():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))

    # TODO(antoniofilipovic) Before fixing durability, if this is removed we also have an issue. Check after fix
    expected_data_on_coord = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "up", "replica"),
        ("instance_2", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "", "127.0.0.1:10013", "up", "main"),
        ("instance_4", "", "127.0.0.1:10014", "up", "replica"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_instances)

    # 3

    execute_and_fetch_all(main_cursor, "CREATE (:Epoch1Vertex {prop:1});")
    execute_and_fetch_all(main_cursor, "CREATE (:Epoch1Vertex {prop:2});")

    # 4
    instance_1_cursor = connect(host="localhost", port=7688).cursor()
    instance_2_cursor = connect(host="localhost", port=7689).cursor()
    instance_4_cursor = connect(host="localhost", port=7691).cursor()

    assert execute_and_fetch_all(instance_1_cursor, "MATCH (n) RETURN count(n);")[0][0] == 2
    assert execute_and_fetch_all(instance_2_cursor, "MATCH (n) RETURN count(n);")[0][0] == 2
    assert execute_and_fetch_all(instance_4_cursor, "MATCH (n) RETURN count(n);")[0][0] == 2

    # 5

    interactive_mg_runner.kill(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "instance_1")

    # 6
    interactive_mg_runner.kill(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "instance_3")

    # 7

    expected_data_on_coord = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "down", "unknown"),
        ("instance_2", "", "127.0.0.1:10012", "up", "main"),
        ("instance_3", "", "127.0.0.1:10013", "down", "unknown"),
        ("instance_4", "", "127.0.0.1:10014", "up", "replica"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_instances)

    # 8

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(instance_2_cursor, "CREATE (:Epoch2Vertex {prop:1});")
    assert "At least one SYNC replica has not confirmed committing last transaction." in str(e.value)

    def get_vertex_count():
        return execute_and_fetch_all(instance_4_cursor, "MATCH (n) RETURN count(n)")[0][0]

    mg_sleep_and_assert(3, get_vertex_count)

    assert execute_and_fetch_all(instance_4_cursor, "MATCH (n) RETURN count(n);")[0][0] == 3

    # 9

    interactive_mg_runner.kill(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "instance_4")

    expected_data_on_coord = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "down", "unknown"),
        ("instance_2", "", "127.0.0.1:10012", "up", "main"),
        ("instance_3", "", "127.0.0.1:10013", "down", "unknown"),
        ("instance_4", "", "127.0.0.1:10014", "down", "unknown"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_instances)

    # 10

    interactive_mg_runner.kill(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "instance_2")
    interactive_mg_runner.start(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "instance_1")
    interactive_mg_runner.start(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "instance_4")

    # 11

    expected_data_on_coord = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "up", "replica"),
        ("instance_2", "", "127.0.0.1:10012", "down", "unknown"),
        ("instance_3", "", "127.0.0.1:10013", "down", "unknown"),
        ("instance_4", "", "127.0.0.1:10014", "up", "main"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_instances)

    # 12
    instance_1_cursor = connect(host="localhost", port=7688).cursor()
    instance_4_cursor = connect(host="localhost", port=7691).cursor()

    def get_vertex_count():
        return execute_and_fetch_all(instance_1_cursor, "MATCH (n) RETURN count(n)")[0][0]

    mg_sleep_and_assert(3, get_vertex_count)

    def get_vertex_count():
        return execute_and_fetch_all(instance_4_cursor, "MATCH (n) RETURN count(n)")[0][0]

    mg_sleep_and_assert(3, get_vertex_count)


def test_replication_works_on_failover_simple():
    # Goal of this test is to check the replication works after failover command.
    # 1. We start all replicas, main and coordinator manually
    # 2. We check that main has correct state
    # 3. We kill main
    # 4. We check that coordinator and new main have correct state
    # 5. We insert one vertex on new main
    # 6. We check that vertex appears on new replica
    # 7. We bring back main up
    # 8. Expect data to be copied to main
    safe_execute(shutil.rmtree, TEMP_DIR)

    # 1
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    # 2
    main_cursor = connect(host="localhost", port=7687).cursor()
    expected_data_on_main = [
        (
            "instance_1",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "instance_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]

    def main_cursor_show_replicas():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))

    mg_sleep_and_assert_collection(expected_data_on_main, main_cursor_show_replicas)

    # 3
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    # 4
    coord_cursor = connect(host="localhost", port=7690).cursor()

    def retrieve_data_show_repl_cluster():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))

    expected_data_on_coord = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "up", "main"),
        ("instance_2", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "", "127.0.0.1:10013", "down", "unknown"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_repl_cluster)

    new_main_cursor = connect(host="localhost", port=7688).cursor()

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(new_main_cursor, "SHOW REPLICAS;")))

    expected_data_on_new_main = [
        (
            "instance_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "instance_3",
            "127.0.0.1:10003",
            "sync",
            {"ts": 0, "behind": None, "status": "invalid"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "invalid"}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data_on_new_main, retrieve_data_show_replicas)

    # 5
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(new_main_cursor, "CREATE ();")
    assert "At least one SYNC replica has not confirmed committing last transaction." in str(e.value)
    # 6
    alive_replica_cursor = connect(host="localhost", port=7689).cursor()
    res = execute_and_fetch_all(alive_replica_cursor, "MATCH (n) RETURN count(n) as count;")[0][0]
    assert res == 1, "Vertex should be replicated"

    # 7
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(new_main_cursor, "SHOW REPLICAS;")))

    new_main_cursor = connect(host="localhost", port=7688).cursor()

    expected_data_on_new_main = [
        (
            "instance_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 2, "behind": 0, "status": "ready"}},
        ),
        (
            "instance_3",
            "127.0.0.1:10003",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 2, "behind": 0, "status": "ready"}},
        ),
    ]
    mg_sleep_and_assert(expected_data_on_new_main, retrieve_data_show_replicas)

    # 8
    alive_main = connect(host="localhost", port=7687).cursor()

    def retrieve_vertices_count():
        return execute_and_fetch_all(alive_main, "MATCH (n) RETURN count(n) as count;")[0][0]

    mg_sleep_and_assert(1, retrieve_vertices_count)


def test_replication_works_on_replica_instance_restart():
    # Goal of this test is to check the replication works after replica goes down and restarts
    # 1. We start all replicas, main and coordinator manually: we want to be able to kill them ourselves without relying on external tooling to kill processes.
    # 2. We check that main has correct state
    # 3. We kill replica
    # 4. We check that main cannot replicate to replica
    # 5. We bring replica back up
    # 6. We check that replica gets data
    safe_execute(shutil.rmtree, TEMP_DIR)

    # 1
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    # 2
    main_cursor = connect(host="localhost", port=7687).cursor()
    expected_data_on_main = [
        (
            "instance_1",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "instance_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]

    def main_cursor_show_replicas():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))

    mg_sleep_and_assert_collection(expected_data_on_main, main_cursor_show_replicas)

    # 3
    coord_cursor = connect(host="localhost", port=7690).cursor()

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_2")

    def retrieve_data_show_repl_cluster():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))

    expected_data_on_coord = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "up", "replica"),
        ("instance_2", "", "127.0.0.1:10012", "down", "unknown"),
        ("instance_3", "", "127.0.0.1:10013", "up", "main"),
    ]
    mg_sleep_and_assert_collection(expected_data_on_coord, retrieve_data_show_repl_cluster)

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))

    expected_data_on_main = [
        (
            "instance_1",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "instance_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "invalid"}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data_on_main, retrieve_data_show_replicas)

    # 4
    instance_1_cursor = connect(host="localhost", port=7688).cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(main_cursor, "CREATE ();")
    assert "At least one SYNC replica has not confirmed committing last transaction." in str(e.value)

    res_instance_1 = execute_and_fetch_all(instance_1_cursor, "MATCH (n) RETURN count(n)")[0][0]
    assert res_instance_1 == 1

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))

    expected_data_on_main = [
        (
            "instance_1",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 2, "behind": 0, "status": "ready"}},
        ),
        (
            "instance_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "invalid"}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data_on_main, retrieve_data_show_replicas)

    # 5.

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_2")

    def retrieve_data_show_repl_cluster():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))

    expected_data_on_coord = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "up", "replica"),
        ("instance_2", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "", "127.0.0.1:10013", "up", "main"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_repl_cluster)

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))

    expected_data_on_main = [
        (
            "instance_1",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 2, "behind": 0, "status": "ready"}},
        ),
        (
            "instance_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 2, "behind": 0, "status": "ready"}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data_on_main, retrieve_data_show_replicas)

    # 6.
    instance_2_cursor = connect(port=7689, host="localhost").cursor()
    execute_and_fetch_all(main_cursor, "CREATE ();")
    res_instance_2 = execute_and_fetch_all(instance_2_cursor, "MATCH (n) RETURN count(n)")[0][0]
    assert res_instance_2 == 2


def test_show_instances():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    instance1_cursor = connect(host="localhost", port=7688).cursor()
    instance2_cursor = connect(host="localhost", port=7689).cursor()
    instance3_cursor = connect(host="localhost", port=7687).cursor()
    coord_cursor = connect(host="localhost", port=7690).cursor()

    def show_repl_cluster():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))

    expected_data = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "up", "replica"),
        ("instance_2", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "", "127.0.0.1:10013", "up", "main"),
    ]
    mg_sleep_and_assert(expected_data, show_repl_cluster)

    def retrieve_data_show_repl_role_instance1():
        return sorted(list(execute_and_fetch_all(instance1_cursor, "SHOW REPLICATION ROLE;")))

    def retrieve_data_show_repl_role_instance2():
        return sorted(list(execute_and_fetch_all(instance2_cursor, "SHOW REPLICATION ROLE;")))

    def retrieve_data_show_repl_role_instance3():
        return sorted(list(execute_and_fetch_all(instance3_cursor, "SHOW REPLICATION ROLE;")))

    mg_sleep_and_assert([("replica",)], retrieve_data_show_repl_role_instance1)
    mg_sleep_and_assert([("replica",)], retrieve_data_show_repl_role_instance2)
    mg_sleep_and_assert([("main",)], retrieve_data_show_repl_role_instance3)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_1")

    expected_data = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "down", "unknown"),
        ("instance_2", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "", "127.0.0.1:10013", "up", "main"),
    ]
    mg_sleep_and_assert(expected_data, show_repl_cluster)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_2")

    expected_data = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "down", "unknown"),
        ("instance_2", "", "127.0.0.1:10012", "down", "unknown"),
        ("instance_3", "", "127.0.0.1:10013", "up", "main"),
    ]
    mg_sleep_and_assert(expected_data, show_repl_cluster)


def test_simple_automatic_failover():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    main_cursor = connect(host="localhost", port=7687).cursor()
    expected_data_on_main = [
        (
            "instance_1",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "instance_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]

    def main_cursor_show_replicas():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))

    mg_sleep_and_assert_collection(expected_data_on_main, main_cursor_show_replicas)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    coord_cursor = connect(host="localhost", port=7690).cursor()

    def retrieve_data_show_repl_cluster():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))

    expected_data_on_coord = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "up", "main"),
        ("instance_2", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "", "127.0.0.1:10013", "down", "unknown"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_repl_cluster)

    new_main_cursor = connect(host="localhost", port=7688).cursor()

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(new_main_cursor, "SHOW REPLICAS;")))

    expected_data_on_new_main = [
        (
            "instance_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "instance_3",
            "127.0.0.1:10003",
            "sync",
            {"ts": 0, "behind": None, "status": "invalid"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "invalid"}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data_on_new_main, retrieve_data_show_replicas)

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")
    expected_data_on_new_main_old_alive = [
        (
            "instance_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "instance_3",
            "127.0.0.1:10003",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]

    mg_sleep_and_assert_collection(expected_data_on_new_main_old_alive, retrieve_data_show_replicas)


def test_registering_replica_fails_name_exists():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    coord_cursor = connect(host="localhost", port=7690).cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(
            coord_cursor,
            "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': '127.0.0.1:7693', 'management_server': '127.0.0.1:10051', 'replication_server': '127.0.0.1:10111'};",
        )
    assert str(e.value) == "Couldn't register replica instance since instance with such name already exists!"
    shutil.rmtree(TEMP_DIR)


def test_registering_replica_fails_endpoint_exists():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    coord_cursor = connect(host="localhost", port=7690).cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(
            coord_cursor,
            "REGISTER INSTANCE instance_5 WITH CONFIG {'bolt_server': '127.0.0.1:7693', 'management_server': '127.0.0.1:10011', 'replication_server': '127.0.0.1:10005'};",
        )
    assert (
        str(e.value) == "Couldn't register replica instance since instance with such management server already exists!"
    )


def test_replica_instance_restarts():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    cursor = connect(host="localhost", port=7690).cursor()

    def show_repl_cluster():
        return sorted(list(execute_and_fetch_all(cursor, "SHOW INSTANCES;")))

    expected_data_up = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "up", "replica"),
        ("instance_2", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "", "127.0.0.1:10013", "up", "main"),
    ]
    mg_sleep_and_assert(expected_data_up, show_repl_cluster)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_1")

    expected_data_down = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "down", "unknown"),
        ("instance_2", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "", "127.0.0.1:10013", "up", "main"),
    ]
    mg_sleep_and_assert(expected_data_down, show_repl_cluster)

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_1")

    mg_sleep_and_assert(expected_data_up, show_repl_cluster)

    instance1_cursor = connect(host="localhost", port=7688).cursor()

    def retrieve_data_show_repl_role_instance1():
        return sorted(list(execute_and_fetch_all(instance1_cursor, "SHOW REPLICATION ROLE;")))

    expected_data_replica = [("replica",)]
    mg_sleep_and_assert(expected_data_replica, retrieve_data_show_repl_role_instance1)


def test_automatic_failover_main_back_as_replica():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    coord_cursor = connect(host="localhost", port=7690).cursor()

    def retrieve_data_show_repl_cluster():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))

    expected_data_after_failover = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "up", "main"),
        ("instance_2", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "", "127.0.0.1:10013", "down", "unknown"),
    ]
    mg_sleep_and_assert(expected_data_after_failover, retrieve_data_show_repl_cluster)

    expected_data_after_main_coming_back = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "up", "main"),
        ("instance_2", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "", "127.0.0.1:10013", "up", "replica"),
    ]

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")
    mg_sleep_and_assert(expected_data_after_main_coming_back, retrieve_data_show_repl_cluster)

    instance3_cursor = connect(host="localhost", port=7687).cursor()

    def retrieve_data_show_repl_role_instance3():
        return sorted(list(execute_and_fetch_all(instance3_cursor, "SHOW REPLICATION ROLE;")))

    mg_sleep_and_assert([("replica",)], retrieve_data_show_repl_role_instance3)


def test_automatic_failover_main_back_as_main():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_1")
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_2")
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    coord_cursor = connect(host="localhost", port=7690).cursor()

    def retrieve_data_show_repl_cluster():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))

    expected_data_all_down = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "down", "unknown"),
        ("instance_2", "", "127.0.0.1:10012", "down", "unknown"),
        ("instance_3", "", "127.0.0.1:10013", "down", "unknown"),
    ]

    mg_sleep_and_assert(expected_data_all_down, retrieve_data_show_repl_cluster)

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")
    expected_data_main_back = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "down", "unknown"),
        ("instance_2", "", "127.0.0.1:10012", "down", "unknown"),
        ("instance_3", "", "127.0.0.1:10013", "up", "main"),
    ]
    mg_sleep_and_assert(expected_data_main_back, retrieve_data_show_repl_cluster)

    instance3_cursor = connect(host="localhost", port=7687).cursor()

    def retrieve_data_show_repl_role_instance3():
        return sorted(list(execute_and_fetch_all(instance3_cursor, "SHOW REPLICATION ROLE;")))

    mg_sleep_and_assert([("main",)], retrieve_data_show_repl_role_instance3)

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_1")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_2")

    expected_data_replicas_back = [
        ("coordinator_1", "0.0.0.0:10111", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "up", "replica"),
        ("instance_2", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "", "127.0.0.1:10013", "up", "main"),
    ]

    mg_sleep_and_assert(expected_data_replicas_back, retrieve_data_show_repl_cluster)

    instance1_cursor = connect(host="localhost", port=7688).cursor()
    instance2_cursor = connect(host="localhost", port=7689).cursor()

    def retrieve_data_show_repl_role_instance1():
        return sorted(list(execute_and_fetch_all(instance1_cursor, "SHOW REPLICATION ROLE;")))

    def retrieve_data_show_repl_role_instance2():
        return sorted(list(execute_and_fetch_all(instance2_cursor, "SHOW REPLICATION ROLE;")))

    mg_sleep_and_assert([("replica",)], retrieve_data_show_repl_role_instance1)
    mg_sleep_and_assert([("replica",)], retrieve_data_show_repl_role_instance2)
    mg_sleep_and_assert([("main",)], retrieve_data_show_repl_role_instance3)


def test_disable_multiple_mains():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    coord_cursor = connect(host="localhost", port=7690).cursor()

    try:
        execute_and_fetch_all(
            coord_cursor,
            "SET INSTANCE instance_1 TO MAIN;",
        )
    except Exception as e:
        assert str(e) == "Couldn't set instance to main since there is already a main instance in cluster!"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
