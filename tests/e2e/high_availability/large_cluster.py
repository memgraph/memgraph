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

file = "large_cluster"


@pytest.fixture
def test_name(request):
    return request.node.name


# 15 data instances, 3 coordinators
def get_instances_description_no_setup(test_name: str):
    return {
        "instance_1": {
            "args": [
                "--bolt-port=7675",
                "--log-level=TRACE",
                "--management-port=10001",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_1",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--bolt-port=7676",
                "--log-level=TRACE",
                "--management-port=10002",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_2",
            "setup_queries": [],
        },
        "instance_3": {
            "args": [
                "--bolt-port=7677",
                "--log-level=TRACE",
                "--management-port=10003",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_3",
            "setup_queries": [],
        },
        "instance_4": {
            "args": [
                "--bolt-port=7678",
                "--log-level=TRACE",
                "--management-port=10004",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_4.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_4",
            "setup_queries": [],
        },
        "instance_5": {
            "args": [
                "--bolt-port=7679",
                "--log-level=TRACE",
                "--management-port=10005",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_5.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_5",
            "setup_queries": [],
        },
        "instance_6": {
            "args": [
                "--bolt-port=7680",
                "--log-level=TRACE",
                "--management-port=10006",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_6.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_6",
            "setup_queries": [],
        },
        "instance_7": {
            "args": [
                "--bolt-port=7681",
                "--log-level=TRACE",
                "--management-port=10007",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_7.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_7",
            "setup_queries": [],
        },
        "instance_8": {
            "args": [
                "--bolt-port=7682",
                "--log-level=TRACE",
                "--management-port=10008",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_8.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_8",
            "setup_queries": [],
        },
        "instance_9": {
            "args": [
                "--bolt-port=7683",
                "--log-level=TRACE",
                "--management-port=10009",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_9.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_9",
            "setup_queries": [],
        },
        "instance_10": {
            "args": [
                "--bolt-port=7684",
                "--log-level=TRACE",
                "--management-port=10010",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_10.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_10",
            "setup_queries": [],
        },
        "instance_11": {
            "args": [
                "--bolt-port=7685",
                "--log-level=TRACE",
                "--management-port=10011",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_11.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_11",
            "setup_queries": [],
        },
        "instance_12": {
            "args": [
                "--bolt-port=7686",
                "--log-level=TRACE",
                "--management-port=10012",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_12.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_12",
            "setup_queries": [],
        },
        "instance_13": {
            "args": [
                "--bolt-port=7687",
                "--log-level=TRACE",
                "--management-port=10013",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_13.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_13",
            "setup_queries": [],
        },
        "instance_14": {
            "args": [
                "--bolt-port=7688",
                "--log-level=TRACE",
                "--management-port=10014",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_14.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_14",
            "setup_queries": [],
        },
        "instance_15": {
            "args": [
                "--bolt-port=7689",
                "--log-level=TRACE",
                "--management-port=10015",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_15.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_15",
            "setup_queries": [],
        },
        "coordinator_1": {
            "args": [
                "--bolt-port=7690",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10016",
                "--management-port=10121",
                "--coordinator-hostname=localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_1",
            "setup_queries": [],
        },
        "coordinator_2": {
            "args": [
                "--bolt-port=7691",
                "--log-level=TRACE",
                "--coordinator-id=2",
                "--coordinator-port=10017",
                "--management-port=10122",
                "--coordinator-hostname=localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_2",
            "setup_queries": [],
        },
        "coordinator_3": {
            "args": [
                "--bolt-port=7692",
                "--log-level=TRACE",
                "--coordinator-id=3",
                "--coordinator-port=10018",
                "--management-port=10123",
                "--coordinator-hostname=localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_3",
            "setup_queries": [],
        },
    }


# All replicas will be registered as SYNC replicas
def get_sync_setup_queries():
    return [
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10016', 'management_server': 'localhost:10121'}",
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10017', 'management_server': 'localhost:10122'}",
        "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10018', 'management_server': 'localhost:10123'}",
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7675', 'management_server': 'localhost:10001', 'replication_server': 'localhost:10106'};",
        "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7676', 'management_server': 'localhost:10002', 'replication_server': 'localhost:10107'};",
        "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7677', 'management_server': 'localhost:10003', 'replication_server': 'localhost:10108'};",
        "REGISTER INSTANCE instance_4 WITH CONFIG {'bolt_server': 'localhost:7678', 'management_server': 'localhost:10004', 'replication_server': 'localhost:10109'};",
        "REGISTER INSTANCE instance_5 WITH CONFIG {'bolt_server': 'localhost:7679', 'management_server': 'localhost:10005', 'replication_server': 'localhost:10110'};",
        "REGISTER INSTANCE instance_6 WITH CONFIG {'bolt_server': 'localhost:7680', 'management_server': 'localhost:10006', 'replication_server': 'localhost:10111'};",
        "REGISTER INSTANCE instance_7 WITH CONFIG {'bolt_server': 'localhost:7681', 'management_server': 'localhost:10007', 'replication_server': 'localhost:10112'};",
        "REGISTER INSTANCE instance_8 WITH CONFIG {'bolt_server': 'localhost:7682', 'management_server': 'localhost:10008', 'replication_server': 'localhost:10113'};",
        "REGISTER INSTANCE instance_9 WITH CONFIG {'bolt_server': 'localhost:7683', 'management_server': 'localhost:10009', 'replication_server': 'localhost:10114'};",
        "REGISTER INSTANCE instance_10 WITH CONFIG {'bolt_server': 'localhost:7684', 'management_server': 'localhost:10010', 'replication_server': 'localhost:10115'};",
        "REGISTER INSTANCE instance_11 WITH CONFIG {'bolt_server': 'localhost:7685', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10116'};",
        "REGISTER INSTANCE instance_12 WITH CONFIG {'bolt_server': 'localhost:7686', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10117'};",
        "REGISTER INSTANCE instance_13 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10118'};",
        "REGISTER INSTANCE instance_14 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10014', 'replication_server': 'localhost:10119'};",
        "REGISTER INSTANCE instance_15 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10015', 'replication_server': 'localhost:10120'};",
        "SET INSTANCE instance_1 TO MAIN",
    ]


@pytest.fixture(autouse=True)
def cleanup_after_test():
    # Run the test
    yield
    # Stop + delete directories after running the test
    interactive_mg_runner.kill_all(keep_directories=False)


def test_replication_works(test_name):
    # Start cluster
    instances = get_instances_description_no_setup(test_name=test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)

    # Register data instances
    leader = connect(host="localhost", port=7692).cursor()
    for query in get_sync_setup_queries():
        execute_and_fetch_all(leader, query)

    # Test replication works
    instance_1 = connect(host="localhost", port=7675).cursor()
    execute_and_fetch_all(
        instance_1, "unwind range (1, 5000) as x create (n:Entity {embedding: [0.1+x, 1.1+x, 2.2+x, 3.3+x, 4.4+x]});"
    )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
