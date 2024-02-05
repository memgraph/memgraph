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
from common import connect, execute_and_fetch_all, safe_execute
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

TEMP_DIR = tempfile.TemporaryDirectory().name

MEMGRAPH_INSTANCES_DESCRIPTION = {
    "coordinator1": {
        "args": [
            "--bolt-port",
            "7687",
            "--log-level=TRACE",
            "--raft-server-id=1",
            "--raft-server-port=10111",
        ],
        "log_file": "coordinator1.log",
        "setup_queries": [],
    },
    "coordinator2": {
        "args": [
            "--bolt-port",
            "7688",
            "--log-level=TRACE",
            "--raft-server-id=2",
            "--raft-server-port=10112",
        ],
        "log_file": "coordinator2.log",
        "setup_queries": [],
    },
    "coordinator3": {
        "args": [
            "--bolt-port",
            "7689",
            "--log-level=TRACE",
            "--raft-server-id=3",
            "--raft-server-port=10113",
        ],
        "log_file": "coordinator3.log",
        "setup_queries": [
            "ADD COORDINATOR 1 ON '127.0.0.1:10111'",
            "ADD COORDINATOR 2 ON '127.0.0.1:10112'",
        ],
    },
}


def test_start_distributed_coordinators():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    coordinator3_cursor = connect(host="localhost", port=7689).cursor()

    def check_coordinator3():
        return sorted(list(execute_and_fetch_all(coordinator3_cursor, "SHOW INSTANCES")))

    expected_cluster = [
        ("coordinator_1", "127.0.0.1:10111", "", True, "coordinator"),
        ("coordinator_2", "127.0.0.1:10112", "", True, "coordinator"),
        ("coordinator_3", "127.0.0.1:10113", "", True, "coordinator"),
    ]
    mg_sleep_and_assert(expected_cluster, check_coordinator3)

    coordinator1_cursor = connect(host="localhost", port=7687).cursor()

    def check_coordinator1():
        return sorted(list(execute_and_fetch_all(coordinator1_cursor, "SHOW INSTANCES")))

    mg_sleep_and_assert(expected_cluster, check_coordinator1)

    coordinator2_cursor = connect(host="localhost", port=7688).cursor()

    def check_coordinator2():
        return sorted(list(execute_and_fetch_all(coordinator2_cursor, "SHOW INSTANCES")))

    mg_sleep_and_assert(expected_cluster, check_coordinator2)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
