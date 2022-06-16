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

import sys

import atexit
import os
import pytest
import time

from common import execute_and_fetch_all
import interactive_mg_runner

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

MEMGRAPH_INSTANCES_DESCRIPTION = {
    "replica_1": {
        "args": ["--bolt-port", "7688", "--log-level=TRACE"],
        "log_file": "replica1.log",
        "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10001;"],
    },
    "replica_2": {
        "args": ["--bolt-port", "7689", "--log-level=TRACE"],
        "log_file": "replica2.log",
        "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
    },
    "replica_3": {
        "args": ["--bolt-port", "7690", "--log-level=TRACE"],
        "log_file": "replica3.log",
        "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10003;"],
    },
    "replica_4": {
        "args": ["--bolt-port", "7691", "--log-level=TRACE"],
        "log_file": "replica4.log",
        "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10004;"],
    },
    "main": {
        "args": ["--bolt-port", "7687", "--log-level=TRACE"],
        "log_file": "main.log",
        "setup_queries": [
            "REGISTER REPLICA replica_1 SYNC WITH TIMEOUT 0 TO '127.0.0.1:10001';",
            "REGISTER REPLICA replica_2 SYNC WITH TIMEOUT 1 TO '127.0.0.1:10002';",
            "REGISTER REPLICA replica_3 ASYNC TO '127.0.0.1:10003';",
            "REGISTER REPLICA replica_4 ASYNC TO '127.0.0.1:10004';",
        ],
    },
}


def test_basic_recovery(connection):
    # Goal of this test is to check the recovery of main.
    # 0/ We start all replicas manually: we want to be able to kill them ourselves without relying on external tooling to kill processes.
    # 1/ We check that all replicas have the correct state: they should all be ready.
    # 2/ We kill main.
    # 3/ We re-start main.
    # 4/ We check that all replicas have the correct state: they should all be ready.

    # 0/
    atexit.register(
        interactive_mg_runner.stop_all
    )  # Needed in case the test fails due to an assert. One still want the instances to be stoped.
    mg_instances = interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    cursor = connection(7687, "main").cursor()

    # 1/
    actual_data = set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))
    EXPECTED_COLUMN_NAMES = {
        "name",
        "socket_address",
        "sync_mode",
        "timeout",
    }

    actual_column_names = {x.name for x in cursor.description}
    assert EXPECTED_COLUMN_NAMES == actual_column_names

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 0),
        ("replica_2", "127.0.0.1:10002", "sync", 1.0),
        ("replica_3", "127.0.0.1:10003", "async", None),
        ("replica_4", "127.0.0.1:10004", "async", None),
    }
    assert expected_data == actual_data

    # 2/
    mg_instances["main"].kill()

    # 3/
    mg_instances.update(interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "main"))
    cursor = connection(7687, "main").cursor()

    # 4/
    # We leave some time for the main to recover.
    time.sleep(2)
    actual_data = set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))
    assert expected_data == actual_data


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
