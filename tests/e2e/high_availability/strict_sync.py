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


import concurrent.futures
import os
import subprocess
import sys
import time
from functools import partial

import interactive_mg_runner
import pytest
from common import (
  connect,
  execute_and_fetch_all,
  find_instance_and_assert_instances,
  get_data_path,
  get_logs_path,
  get_vertex_count,
  has_leader,
  has_main,
  show_instances,
  show_replicas,
  update_tuple_value,
  wait_until_main_writeable_assert_replica_down,
)
from mg_utils import (
  mg_assert_until,
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

file = "strict_sync"


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


# Uses STRICT_SYNC replicas
def get_default_setup_queries():
    return [
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
        "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}",
        "REGISTER INSTANCE instance_1 AS STRICT_SYNC WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
        "REGISTER INSTANCE instance_2 AS STRICT_SYNC WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
        "REGISTER INSTANCE instance_3 AS STRICT_SYNC WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
        "SET INSTANCE instance_3 TO MAIN",
    ]


# Executes setup queries and returns cluster info
# Should be used together with get_default_setup_queries
def setup_default_cluster(test_name):
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)
    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)
    coord_cursor_3 = connect(host="localhost", port=7692).cursor()

    for query in get_default_setup_queries():
        execute_and_fetch_all(coord_cursor_3, query)
    return inner_instances_description


# Tests that when all replicas are UP, 2PC should work
# After instances restart, they should still see the same data as upon committing
# @pytest.mark.skip(reason="Commit works properly")
def test_commit_works(test_name):
    inner_instances_description = setup_default_cluster(test_name)
    # Create data on MAIN
    instance3_cursor = connect(host="localhost", port=7689).cursor()
    execute_and_fetch_all(instance3_cursor, "CREATE (n:Node)")

    # Check if replicated on 1st replica
    instance_1_cursor = connect(host="localhost", port=7687).cursor()
    mg_sleep_and_assert(1, partial(get_vertex_count, instance_1_cursor))

    # Check if replicated on 2nd replica
    instance_2_cursor = connect(host="localhost", port=7688).cursor()
    mg_sleep_and_assert(1, partial(get_vertex_count, instance_2_cursor))

    def check_if_data_preserved_after_restart(instance_name, bolt_port):
        interactive_mg_runner.kill(inner_instances_description, instance_name)
        interactive_mg_runner.start(inner_instances_description, instance_name)
        instance_cursor = connect(host="localhost", port=bolt_port).cursor()
        mg_sleep_and_assert(1, partial(get_vertex_count, instance_cursor))

    check_if_data_preserved_after_restart("instance_1", 7687)
    check_if_data_preserved_after_restart("instance_2", 7688)
    check_if_data_preserved_after_restart("instance_3", 7689)


# One replica is down before commit starts on MAIN, hence in-memory state should be preserved and commit should fail
# @pytest.mark.skip(reason="In-memory Abort works properly")
def test_replica_down_before_commit(test_name):
    inner_instances_description = setup_default_cluster(test_name)

    # Replica goes down
    interactive_mg_runner.kill(inner_instances_description, "instance_1")

    # Try to commit transaction on the current main
    instance3_cursor = connect(host="localhost", port=7689).cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(instance3_cursor, "CREATE (n:Node)")
    assert (
        "At least one STRICT_SYNC replica has not confirmed committing last transaction. Transaction will be aborted on all instances."
        in str(e.value)
    )
    # Commit shouldn't be visible on the current main
    mg_sleep_and_assert(0, partial(get_vertex_count, instance3_cursor))

    # Restart replica 1
    interactive_mg_runner.start(inner_instances_description, "instance_1")
    instance1_cursor = connect(host="localhost", port=7687).cursor()
    mg_sleep_and_assert(0, partial(get_vertex_count, instance1_cursor))

    # Check data on replica 2
    instance2_cursor = connect(host="localhost", port=7688).cursor()
    mg_sleep_and_assert(0, partial(get_vertex_count, instance2_cursor))


# One of replicas was down during the commit hence the txn will get aborted
# Test that the other replica which was alive all the time and which receive PrepareRpc
# won't contain any data after the restart.
# @pytest.mark.skip(reason="Durable abort still doesn't work")
def test_replica_after_restart_no_committed_data(test_name):
    inner_instances_description = setup_default_cluster(test_name)

    # Instance 1 dies
    interactive_mg_runner.kill(inner_instances_description, "instance_1")

    # Try to commit transaction on the current main
    instance3_cursor = connect(host="localhost", port=7689).cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(instance3_cursor, "CREATE (n:Node)")
    assert (
        "At least one STRICT_SYNC replica has not confirmed committing last transaction. Transaction will be aborted on all instances."
        in str(e.value)
    )

    # Data restart shouldn't change the fact that txn got aborted
    instance2_cursor = connect(host="localhost", port=7688).cursor()
    mg_sleep_and_assert(0, partial(get_vertex_count, instance2_cursor))
    interactive_mg_runner.kill(inner_instances_description, "instance_2")
    interactive_mg_runner.start(inner_instances_description, "instance_2")
    instance2_cursor = connect(host="localhost", port=7688).cursor()
    mg_sleep_and_assert(0, partial(get_vertex_count, instance2_cursor))


# TODO: (andi) ADD MT tests


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
