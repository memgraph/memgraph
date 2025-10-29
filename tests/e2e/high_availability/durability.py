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
import mgclient
import pytest
from common import (
  connect,
  count_files,
  execute_and_fetch_all,
  execute_and_ignore_dead_replica,
  get_data_path,
  get_logs_path,
  get_vertex_count,
  list_directory_contents,
  show_instances,
)
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

file = "durability"


@pytest.fixture
def test_name(request):
    return request.node.name


def get_instances_description_no_setup_snapshot_recovery(test_name: str):
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


def get_instances_description_no_setup_wal_files_recovery(test_name: str):
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
                "--storage-wal-file-size-kib",
                "1",
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
                "--storage-wal-file-size-kib",
                "1",
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


# 1. Set-up main (instance_1) and replica (instance_2). Replicate 5 vertices from instance_1 to instance_2
# 2. Kill replica (instance_2) and create another vertices on instance_1
# 3. Kill main (instance_1)
# 4. Bring replica back (instance_2) and wait until it becomes main
# 5. Bring main back (instance_1) and wait until it becomes replica
# 6. Check there are only 5 vertices there
def test_branching_point_snapshot_recovery(test_name):
    instances_description = get_instances_description_no_setup_snapshot_recovery(test_name=test_name)
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

    # 1.
    instance1_cursor = connect(host="localhost", port=7687).cursor()
    instance2_cursor = connect(host="localhost", port=7688).cursor()
    execute_and_fetch_all(instance1_cursor, "UNWIND RANGE(1, 5) AS i CREATE (:Common {id :i});")
    mg_sleep_and_assert(5, partial(get_vertex_count, instance1_cursor))
    mg_sleep_and_assert(5, partial(get_vertex_count, instance2_cursor))

    # 2.
    interactive_mg_runner.kill(instances_description, "instance_2")
    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "main"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "down", "unknown"),
    ]
    mg_sleep_and_assert(data, partial(show_instances, coord_cursor_3))
    execute_and_ignore_dead_replica(instance1_cursor, "UNWIND RANGE(1, 5) AS i CREATE (:Single {id :i});")

    # 3.
    # Assert there is only a single WAL (current) present and no snapshots on the current main
    build_dir = os.path.join(interactive_mg_runner.PROJECT_DIR, "build", "e2e", "data")
    data_dir_instance_1 = f"{build_dir}/{get_data_path(file, test_name)}/instance_1"
    wal_dir_instance_1 = f"{data_dir_instance_1}/wal"
    snapshot_dir_instance_1 = f"{data_dir_instance_1}/snapshots"
    assert count_files(wal_dir_instance_1) == 1
    assert count_files(snapshot_dir_instance_1) == 0

    interactive_mg_runner.kill(instances_description, "instance_1")

    # 4.
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
    while True:
        try:
            # We test whether the replica became main but replica may need to wait for a few moments before the snapshot
            # can be created on it. After the election on the coordinator's side is done, the instance receives PromoteRpc
            # message. To conclude, there is a bit of lag between coordinator electing new main and new main actually
            # being promoted, which this while loop tries to take care of.
            execute_and_fetch_all(instance2_cursor, "CREATE SNAPSHOT")
            break
        except mgclient.DatabaseError as e:
            # Fail or continue the while loop
            assert str(e) == "Failed to create a snapshot. Replica instances are not allowed to create them."

    # Count number of WALs and snapshots
    data_dir_instance_2 = f"{build_dir}/{get_data_path(file, test_name)}/instance_2"
    wal_dir_instance_2 = f"{data_dir_instance_2}/wal"
    snapshot_dir_instance_2 = f"{data_dir_instance_2}/snapshots"
    assert count_files(wal_dir_instance_2) == 1
    assert count_files(snapshot_dir_instance_2) == 1

    # 5.
    interactive_mg_runner.start(instances_description, "instance_1")
    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "main"),
    ]
    mg_sleep_and_assert(data, partial(show_instances, coord_cursor_3))

    # 6.
    instance1_cursor = connect(host="localhost", port=7687).cursor()
    mg_sleep_and_assert(5, partial(get_vertex_count, instance1_cursor))
    mg_sleep_and_assert(5, partial(get_vertex_count, instance2_cursor))

    assert count_files(snapshot_dir_instance_1) == 1
    instance1_wal_entries = list_directory_contents(wal_dir_instance_1)

    contains_wal_old_dir = False
    for entry in instance1_wal_entries:
        full_path = os.path.join(wal_dir_instance_1, entry)
        if os.path.isdir(full_path) and "old" in entry.lower():
            contains_wal_old_dir = True
    assert contains_wal_old_dir is True


# 1. Create a vertex with a large property to trigger WAL creation
# 2. Kill replica and create another two big vertices on the main
# 3. Kill main, restart replica. Replica will become new main.
# 4. When old main gets back, it should get WalFiles. Old main should move his old WAL files to the .old directory and
# keep only new main's WAL file
def test_branching_point_wal_files_recovery(test_name):
    instances_description = get_instances_description_no_setup_wal_files_recovery(test_name=test_name)
    interactive_mg_runner.start_all(instances_description, keep_directories=False)

    build_dir = os.path.join(interactive_mg_runner.PROJECT_DIR, "build", "e2e", "data")
    # Instance 1
    data_dir_instance_1 = f"{build_dir}/{get_data_path(file, test_name)}/instance_1"
    wal_dir_instance_1 = f"{data_dir_instance_1}/wal"
    # Instance 2
    data_dir_instance_2 = f"{build_dir}/{get_data_path(file, test_name)}/instance_2"
    wal_dir_instance_2 = f"{data_dir_instance_2}/wal"

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

    # 1.
    instance1_cursor = connect(host="localhost", port=7687).cursor()
    execute_and_fetch_all(instance1_cursor, "CREATE (:Common {prop: range(1, 128)});")
    assert count_files(wal_dir_instance_1) == 1
    assert count_files(wal_dir_instance_2) == 1

    # 2.
    interactive_mg_runner.kill(instances_description, "instance_2")
    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "main"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "down", "unknown"),
    ]
    mg_sleep_and_assert(data, partial(show_instances, coord_cursor_3))
    execute_and_ignore_dead_replica(instance1_cursor, "CREATE (:Single {prop: range(1, 128)});")
    execute_and_ignore_dead_replica(instance1_cursor, "CREATE (:Single {prop: range(1, 128)});")
    assert count_files(wal_dir_instance_1) == 3
    assert count_files(wal_dir_instance_2) == 1

    # 3.
    interactive_mg_runner.kill(instances_description, "instance_1")
    interactive_mg_runner.start(instances_description, "instance_2")
    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "down", "unknown"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "main"),
    ]
    mg_sleep_and_assert(data, partial(show_instances, coord_cursor_3))

    # 4.
    interactive_mg_runner.start(instances_description, "instance_1")
    data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7687", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7688", "", "localhost:10012", "up", "main"),
    ]
    mg_sleep_and_assert(data, partial(show_instances, coord_cursor_3))

    # 6.
    instance1_cursor = connect(host="localhost", port=7687).cursor()
    mg_sleep_and_assert(1, partial(get_vertex_count, instance1_cursor))

    assert count_files(wal_dir_instance_1) == 1
    instance1_wal_entries = list_directory_contents(wal_dir_instance_1)

    contains_wal_old_dir = False
    for entry in instance1_wal_entries:
        full_path = os.path.join(wal_dir_instance_1, entry)
        if os.path.isdir(full_path) and "old" in entry.lower():
            # There should be 3 WAL files in the old directory
            assert count_files(full_path) == 3
            contains_wal_old_dir = True
    assert contains_wal_old_dir is True


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
