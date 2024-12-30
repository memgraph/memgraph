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
from functools import partial

import interactive_mg_runner
import pytest
from common import (
    connect,
    execute_and_fetch_all,
    get_data_path,
    get_logs_path,
    show_instances,
)
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

file = "coordinator"


@pytest.fixture
def test_name(request):
    return request.node.name


def get_memgraph_instances_description(test_name: str):
    return {
        "instance_1": {
            "args": [
                "--bolt-port",
                "7688",
                "--log-level",
                "TRACE",
                "--management-port",
                "10011",
                "--replication-restore-state-on-startup=true",
                "--data-recovery-on-startup=false",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_1",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--bolt-port",
                "7689",
                "--log-level",
                "TRACE",
                "--management-port",
                "10012",
                "--replication-restore-state-on-startup=true",
                "--data-recovery-on-startup=false",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_2",
            "setup_queries": [],
        },
        "instance_3": {
            "args": [
                "--bolt-port",
                "7687",
                "--log-level",
                "TRACE",
                "--management-port",
                "10013",
                "--replication-restore-state-on-startup=true",
                "--data-recovery-on-startup=false",
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
                "--coordinator-hostname",
                "localhost",
                "--management-port",
                "10121",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_1",
            "setup_queries": [
                "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
                "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
                "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
                "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
                "SET INSTANCE instance_3 TO MAIN;",
            ],
        },
    }


def setup_test(test_name: str):
    memgraph_instances_description = get_memgraph_instances_description(test_name)
    interactive_mg_runner.start_all(memgraph_instances_description, keep_directories=False)

    return connect(host="localhost", port=7690).cursor()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    # Run the test
    yield
    # Clean after running
    interactive_mg_runner.kill_all(keep_directories=False)


def test_disable_cypher_queries(test_name):
    cursor = setup_test(test_name=test_name)

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(cursor, "CREATE (n:TestNode {prop: 'test'})")
    assert str(e.value) == "Coordinator can run only coordinator queries!"


def test_coordinator_cannot_be_replica_role(test_name):
    cursor = setup_test(test_name=test_name)
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")
    assert str(e.value) == "Coordinator can run only coordinator queries!"


def test_coordinator_cannot_run_show_repl_role(test_name):
    cursor = setup_test(test_name=test_name)
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(cursor, "SHOW REPLICATION ROLE;")
    assert str(e.value) == "Coordinator can run only coordinator queries!"


def test_coordinator_show_instances(test_name):
    cursor = setup_test(test_name=test_name)

    expected_data = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader"),
        ("instance_1", "localhost:7688", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7689", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7687", "", "localhost:10013", "up", "main"),
    ]
    mg_sleep_and_assert(expected_data, partial(show_instances, cursor))


def test_coordinator_cannot_call_show_replicas(test_name):
    cursor = setup_test(test_name=test_name)
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(cursor, "SHOW REPLICAS;")
    assert str(e.value) == "Coordinator can run only coordinator queries!"


def test_main_and_replicas_cannot_call_show_repl_cluster(test_name):
    setup_test(test_name=test_name)
    ports = [7687, 7688, 7689]
    for port in ports:
        cursor = connect(host="localhost", port=port).cursor()
        with pytest.raises(Exception) as e:
            execute_and_fetch_all(cursor, "SHOW INSTANCES;")
        assert str(e.value) == "Only coordinator can run SHOW INSTANCES."


def test_main_and_replicas_cannot_register_coord_server(test_name):
    setup_test(test_name=test_name)
    ports = [7687, 7688, 7689]
    for port in ports:
        cursor = connect(host="localhost", port=port).cursor()
        with pytest.raises(Exception) as e:
            execute_and_fetch_all(
                cursor,
                "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7690', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
            )
        assert str(e.value) == "Only coordinator can register coordinator server!"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
