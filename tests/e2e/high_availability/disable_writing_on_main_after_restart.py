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

import interactive_mg_runner
import pytest
from common import add_coordinator, connect, execute_and_fetch_all, safe_execute
from mg_utils import mg_sleep_and_assert

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
            "7687",
            "--log-level",
            "TRACE",
            "--coordinator-server-port",
            "10011",
            "--also-log-to-stderr",
            "--instance-health-check-frequency-sec",
            "1",
            "--instance-down-timeout-sec",
            "5",
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
            "--coordinator-server-port",
            "10012",
            "--also-log-to-stderr",
            "--instance-health-check-frequency-sec",
            "1",
            "--instance-down-timeout-sec",
            "5",
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
            "--coordinator-server-port",
            "10013",
            "--also-log-to-stderr",
            "--instance-health-check-frequency-sec",
            "5",
            "--instance-down-timeout-sec",
            "10",
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
            "--raft-server-id=1",
            "--raft-server-port=10111",
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
            "--raft-server-id=2",
            "--raft-server-port=10112",
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
            "--raft-server-id=3",
            "--raft-server-port=10113",
            "--also-log-to-stderr",
        ],
        "log_file": "coordinator3.log",
        "setup_queries": [],
    },
}


def test_writing_disabled_on_main_restart():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    coordinator3_cursor = connect(host="localhost", port=7692).cursor()

    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': '127.0.0.1:7689', 'management_server': '127.0.0.1:10013', 'replication_server': '127.0.0.1:10003'};",
    )
    execute_and_fetch_all(coordinator3_cursor, "SET INSTANCE instance_3 TO MAIN")
    assert add_coordinator(
        coordinator3_cursor,
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': '127.0.0.1:7690', 'coordinator_server': '127.0.0.1:10111'}",
    )
    assert add_coordinator(
        coordinator3_cursor,
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': '127.0.0.1:7691', 'coordinator_server': '127.0.0.1:10112'}",
    )

    def check_coordinator3():
        return sorted(list(execute_and_fetch_all(coordinator3_cursor, "SHOW INSTANCES")))

    expected_cluster_coord3 = [
        ("coordinator_1", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "127.0.0.1:10113", "", "unknown", "coordinator"),
        ("instance_3", "", "127.0.0.1:10013", "up", "main"),
    ]
    mg_sleep_and_assert(expected_cluster_coord3, check_coordinator3)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    expected_cluster_coord3 = [
        ("coordinator_1", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "127.0.0.1:10113", "", "unknown", "coordinator"),
        ("instance_3", "", "127.0.0.1:10013", "down", "unknown"),
    ]

    mg_sleep_and_assert(expected_cluster_coord3, check_coordinator3)

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    try:
        instance3_cursor = connect(host="localhost", port=7689).cursor()
        execute_and_fetch_all(instance3_cursor, "CREATE (n:Node {name: 'node'})")
    except Exception as e:
        assert (
            str(e)
            == "Write query forbidden on the main! Coordinator needs to enable writing on main by sending RPC message."
        )

    expected_cluster_coord3 = [
        ("coordinator_1", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "127.0.0.1:10113", "", "unknown", "coordinator"),
        ("instance_3", "", "127.0.0.1:10013", "up", "main"),
    ]

    mg_sleep_and_assert(expected_cluster_coord3, check_coordinator3)
    execute_and_fetch_all(instance3_cursor, "CREATE (n:Node {name: 'node'})")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
