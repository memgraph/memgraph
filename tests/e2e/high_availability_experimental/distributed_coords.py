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
    "instance_1": {
        "args": [
            "--bolt-port",
            "7687",
            "--log-level",
            "TRACE",
            "--coordinator-server-port",
            "10011",
        ],
        "log_file": "instance_1.log",
        "data_directory": f"{TEMP_DIR}/instance_1",
        "setup_queries": [],
    },
    "instance_2": {
        "args": [
            "--bolt-port",
            "7688",
            "--log-level",
            "TRACE",
            "--coordinator-server-port",
            "10012",
        ],
        "log_file": "instance_2.log",
        "data_directory": f"{TEMP_DIR}/instance_2",
        "setup_queries": [],
    },
    "instance_3": {
        "args": [
            "--bolt-port",
            "7689",
            "--log-level",
            "TRACE",
            "--coordinator-server-port",
            "10013",
        ],
        "log_file": "instance_3.log",
        "data_directory": f"{TEMP_DIR}/instance_3",
        "setup_queries": [],
    },
    "coordinator_1": {
        "args": [
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
            "--bolt-port",
            "7692",
            "--log-level=TRACE",
            "--raft-server-id=3",
            "--raft-server-port=10113",
        ],
        "log_file": "coordinator3.log",
        "setup_queries": [
            "ADD COORDINATOR 1 ON '127.0.0.1:10111'",
            "ADD COORDINATOR 2 ON '127.0.0.1:10112'",
            "REGISTER INSTANCE instance_1 ON '127.0.0.1:10011' WITH '127.0.0.1:10001'",
            "REGISTER INSTANCE instance_2 ON '127.0.0.1:10012' WITH '127.0.0.1:10002'",
            "REGISTER INSTANCE instance_3 ON '127.0.0.1:10013' WITH '127.0.0.1:10003'",
            "SET INSTANCE instance_3 TO MAIN",
        ],
    },
}


def test_distributed_automatic_failover():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    main_cursor = connect(host="localhost", port=7689).cursor()
    expected_data_on_main = [
        ("instance_1", "127.0.0.1:10001", "sync", 0, 0, "ready"),
        ("instance_2", "127.0.0.1:10002", "sync", 0, 0, "ready"),
    ]
    actual_data_on_main = sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))
    assert actual_data_on_main == expected_data_on_main

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    coord_cursor = connect(host="localhost", port=7692).cursor()

    def retrieve_data_show_repl_cluster():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))

    expected_data_on_coord = [
        ("coordinator_1", "127.0.0.1:10111", "", True, "coordinator"),
        ("coordinator_2", "127.0.0.1:10112", "", True, "coordinator"),
        ("coordinator_3", "127.0.0.1:10113", "", True, "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", True, "main"),
        ("instance_2", "", "127.0.0.1:10012", True, "replica"),
        ("instance_3", "", "127.0.0.1:10013", False, "unknown"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_repl_cluster)

    new_main_cursor = connect(host="localhost", port=7687).cursor()

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(new_main_cursor, "SHOW REPLICAS;")))

    expected_data_on_new_main = [
        ("instance_2", "127.0.0.1:10002", "sync", 0, 0, "ready"),
        ("instance_3", "127.0.0.1:10003", "sync", 0, 0, "invalid"),
    ]
    mg_sleep_and_assert(expected_data_on_new_main, retrieve_data_show_replicas)

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")
    expected_data_on_new_main_old_alive = [
        ("instance_2", "127.0.0.1:10002", "sync", 0, 0, "ready"),
        ("instance_3", "127.0.0.1:10003", "sync", 0, 0, "ready"),
    ]

    mg_sleep_and_assert(expected_data_on_new_main_old_alive, retrieve_data_show_replicas)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
