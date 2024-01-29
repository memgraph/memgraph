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

import interactive_mg_runner
import pytest
from common import execute_and_fetch_all
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

MEMGRAPH_INSTANCES_DESCRIPTION = {
    "instance_1": {
        "args": ["--bolt-port", "7688", "--log-level", "TRACE", "--coordinator-server-port", "10011"],
        "log_file": "replica1.log",
        "setup_queries": [],
    },
    "instance_2": {
        "args": ["--bolt-port", "7689", "--log-level", "TRACE", "--coordinator-server-port", "10012"],
        "log_file": "replica2.log",
        "setup_queries": [],
    },
    "instance_3": {
        "args": ["--bolt-port", "7687", "--log-level", "TRACE", "--coordinator-server-port", "10013"],
        "log_file": "main.log",
        "setup_queries": [],
    },
    "coordinator": {
        "args": ["--bolt-port", "7690", "--log-level=TRACE", "--coordinator"],
        "log_file": "replica3.log",
        "setup_queries": [
            "REGISTER INSTANCE instance_1 ON '127.0.0.1:10011' WITH '127.0.0.1:10001';",
            "REGISTER INSTANCE instance_2 ON '127.0.0.1:10012' WITH '127.0.0.1:10002';",
            "REGISTER INSTANCE instance_3 ON '127.0.0.1:10013' WITH '127.0.0.1:10003';",
            "SET INSTANCE instance_3 TO MAIN",
        ],
    },
}


def test_replication_works_on_failover(connection):
    # Goal of this test is to check the replication works after failover command,
    # and old main is not replicating to new replicas once it comes back alive

    ## Check if this test is testing actually what we want
    ## if old main comes back up as replica, we are not testing anything
    ## maybe add delay in code for main being pushed back to replica few seconds with some "unknown config"
    ## check if you can disable responses from old main to simulate network split

    # 1
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    # 2
    main_cursor = connection(7687, "instance_3").cursor()
    expected_data_on_main = [
        ("instance_1", "127.0.0.1:10001", "sync", 0, 0, "ready"),
        ("instance_2", "127.0.0.1:10002", "sync", 0, 0, "ready"),
    ]
    actual_data_on_main = sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))
    assert actual_data_on_main == expected_data_on_main

    # 3
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    # 4
    coord_cursor = connection(7690, "coordinator").cursor()

    def retrieve_data_show_repl_cluster():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW REPLICATION CLUSTER;")))

    expected_data_on_coord = [
        ("instance_1", "127.0.0.1:10011", True, "main"),
        ("instance_2", "127.0.0.1:10012", True, "replica"),
        ("instance_3", "127.0.0.1:10013", False, ""),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_repl_cluster)

    new_main_cursor = connection(7688, "instance_1").cursor()

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(new_main_cursor, "SHOW REPLICAS;")))

    expected_data_on_new_main = [
        ("instance_2", "127.0.0.1:10002", "sync", 0, 0, "ready"),
    ]
    mg_sleep_and_assert(expected_data_on_new_main, retrieve_data_show_replicas)

    # 5
    execute_and_fetch_all(new_main_cursor, "CREATE ();")

    # 6
    alive_replica_cursror = connection(7689, "instance_2").cursor()
    res = execute_and_fetch_all(alive_replica_cursror, "MATCH (n) RETURN count(n) as count;")[0][0]
    assert res == 1, "Vertex should be replicated"
    interactive_mg_runner.stop_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    # 7
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")
    old_main_cursor = connection(7687, "instance_3").cursor()
    execute_and_fetch_all(old_main_cursor, "CREATE ()-[]->();")

    res = execute_and_fetch_all(alive_replica_cursror, "MATCH (n) RETURN count(n) as count;")[0][0]
    assert res == 1, "Data from old main should not be replicated"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
