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
import shutil
import sys
import tempfile
import time

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all, safe_execute
from mg_utils import (
    mg_sleep_and_assert,
    mg_sleep_and_assert_any_function,
    mg_sleep_and_assert_collection,
)

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

TEMP_DIR = tempfile.TemporaryDirectory().name


def get_instances_description_memory_limit():
    return {
        "instance_1": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7687",
                "--log-level",
                "TRACE",
                "--management-port",
                "10011",
            ],
            "log_file": "instance_1.log",
            "data_directory": f"{TEMP_DIR}/instance_1",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7688",
                "--log-level",
                "TRACE",
                "--management-port",
                "10012",
            ],
            "log_file": "instance_2.log",
            "data_directory": f"{TEMP_DIR}/instance_2",
            "setup_queries": [],
        },
        "instance_3": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7689",
                "--log-level",
                "TRACE",
                "--management-port",
                "10013",
                "--memory-limit",
                "300",
            ],
            "log_file": "instance_3.log",
            "data_directory": f"{TEMP_DIR}/instance_3",
            "setup_queries": [],
        },
        "coordinator_1": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7690",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10111",
            ],
            "log_file": "coordinator1.log",
            "setup_queries": [],
        },
        "coordinator_2": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7691",
                "--log-level=TRACE",
                "--coordinator-id=2",
                "--coordinator-port=10112",
            ],
            "log_file": "coordinator2.log",
            "setup_queries": [],
        },
        "coordinator_3": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7692",
                "--log-level=TRACE",
                "--coordinator-id=3",
                "--coordinator-port=10113",
            ],
            "log_file": "coordinator3.log",
            "setup_queries": [
                "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': '127.0.0.1:7690', 'coordinator_server': '127.0.0.1:10111'}",
                "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': '127.0.0.1:7691', 'coordinator_server': '127.0.0.1:10112'}",
                "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': '127.0.0.1:7687', 'management_server': '127.0.0.1:10011', 'replication_server': '127.0.0.1:10001'};",
                "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': '127.0.0.1:7688', 'management_server': '127.0.0.1:10012', 'replication_server': '127.0.0.1:10002'};",
                "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': '127.0.0.1:7689', 'management_server': '127.0.0.1:10013', 'replication_server': '127.0.0.1:10003'};",
                "SET INSTANCE instance_3 TO MAIN",
            ],
        },
    }


def test_oom_error_handling():
    safe_execute(shutil.rmtree, TEMP_DIR)
    inner_instances_description = get_instances_description_memory_limit()

    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;")))

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    def show_instances_coord2():
        return sorted(list(execute_and_fetch_all(coord_cursor_2, "SHOW INSTANCES;")))

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()

    def show_instances_coord3():
        return sorted(list(execute_and_fetch_all(coord_cursor_3, "SHOW INSTANCES;")))

    leader_data = [
        ("coordinator_1", "127.0.0.1:7690", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:7691", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "0.0.0.0:7692", "0.0.0.0:10113", "", "unknown", "coordinator"),
        ("instance_1", "127.0.0.1:7687", "", "127.0.0.1:10011", "up", "replica"),
        ("instance_2", "127.0.0.1:7688", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "127.0.0.1:7689", "", "127.0.0.1:10013", "up", "main"),
    ]
    mg_sleep_and_assert(leader_data, show_instances_coord3)

    follower_data = [
        ("coordinator_1", "127.0.0.1:7690", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:7691", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "0.0.0.0:7692", "0.0.0.0:10113", "", "unknown", "coordinator"),
        ("instance_1", "127.0.0.1:7687", "", "127.0.0.1:10011", "unknown", "replica"),
        ("instance_2", "127.0.0.1:7688", "", "127.0.0.1:10012", "unknown", "replica"),
        ("instance_3", "127.0.0.1:7689", "", "127.0.0.1:10013", "unknown", "main"),
    ]
    mg_sleep_and_assert_any_function(follower_data, show_instances_coord1)
    mg_sleep_and_assert_any_function(follower_data, show_instances_coord2)

    main_cursor = connect(host="localhost", port=7689).cursor()

    def show_replicas():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))

    replicas = [
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
    mg_sleep_and_assert_collection(replicas, show_replicas)

    memory_limit_exceeded = False
    i = 100
    while i:
        try:
            execute_and_fetch_all(main_cursor, "FOREACH (i in range(1,100000) | CREATE (n:Node {name: 'node'}));")
        except Exception as e:
            if "Memory limit exceeded" in str(e):
                memory_limit_exceeded = True

            break
        i -= 1

    # TODO(define behavior)

    replica_2_cursor = connect(host="localhost", port=7688).cursor()

    def get_vertex_count():
        return execute_and_fetch_all(replica_2_cursor, "MATCH (n) RETURN count(n)")[0][0]

    mg_sleep_and_assert(1, get_vertex_count)

    replica_3_cursor = connect(host="localhost", port=7689).cursor()

    def get_vertex_count():
        return execute_and_fetch_all(replica_3_cursor, "MATCH (n) RETURN count(n)")[0][0]

    mg_sleep_and_assert(1, get_vertex_count)

    interactive_mg_runner.start(inner_instances_description, "coordinator_3")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
