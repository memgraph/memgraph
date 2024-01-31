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

MEMGRAPH_FIRST_CLUSTER_DESCRIPTION = {
    "shared_replica": {
        "args": ["--bolt-port", "7688", "--log-level", "TRACE"],
        "log_file": "replica2.log",
        "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10001;"],
    },
    "main1": {
        "args": ["--bolt-port", "7687", "--log-level", "TRACE"],
        "log_file": "main.log",
        "setup_queries": ["REGISTER REPLICA shared_replica SYNC TO '127.0.0.1:10001' ;"],
    },
}


MEMGRAPH_INSTANCES_DESCRIPTION = {
    "replica": {
        "args": ["--bolt-port", "7689", "--log-level", "TRACE"],
        "log_file": "replica.log",
        "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
    },
    "main_2": {
        "args": ["--bolt-port", "7690", "--log-level", "TRACE"],
        "log_file": "main_2.log",
        "setup_queries": [
            "REGISTER REPLICA shared_replica SYNC TO '127.0.0.1:10001' ;",
            "REGISTER REPLICA replica SYNC TO '127.0.0.1:10002' ; ",
        ],
    },
}


def test_replication_works_on_failover(connection):
    # Goal of this test is to check that after changing `shared_replica`
    # to be part of new cluster, `main` (old cluster) can't write any more to it

    # 1
    interactive_mg_runner.start_all_keep_others(MEMGRAPH_FIRST_CLUSTER_DESCRIPTION)

    # 2
    main_cursor = connection(7687, "main1").cursor()
    expected_data_on_main = [
        ("shared_replica", "127.0.0.1:10001", "sync", 0, 0, "ready"),
    ]
    actual_data_on_main = sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))
    assert actual_data_on_main == expected_data_on_main

    # 3
    interactive_mg_runner.start_all_keep_others(MEMGRAPH_INSTANCES_DESCRIPTION)

    # 4
    new_main_cursor = connection(7690, "main_2").cursor()

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(new_main_cursor, "SHOW REPLICAS;")))

    expected_data_on_new_main = [
        ("replica", "127.0.0.1:10002", "sync", 0, 0, "ready"),
        ("shared_replica", "127.0.0.1:10001", "sync", 0, 0, "ready"),
    ]
    mg_sleep_and_assert(expected_data_on_new_main, retrieve_data_show_replicas)

    # 5
    shared_replica_cursor = connection(7688, "shared_replica").cursor()

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(main_cursor, "CREATE ();")
    assert (
        str(e.value)
        == "Replication Exception: At least one SYNC replica has not confirmed committing last transaction. Check the status of the replicas using 'SHOW REPLICAS' query."
    )

    res = execute_and_fetch_all(main_cursor, "MATCH (n) RETURN count(n) as count;")[0][0]
    assert res == 1, "Vertex should be created"

    res = execute_and_fetch_all(shared_replica_cursor, "MATCH (n) RETURN count(n) as count;")[0][0]
    assert res == 0, "Vertex shouldn't be replicated"

    # 7
    execute_and_fetch_all(new_main_cursor, "CREATE ();")

    res = execute_and_fetch_all(new_main_cursor, "MATCH (n) RETURN count(n) as count;")[0][0]
    assert res == 1, "Vertex should be created"

    res = execute_and_fetch_all(shared_replica_cursor, "MATCH (n) RETURN count(n) as count;")[0][0]
    assert res == 1, "Vertex should be replicated"

    interactive_mg_runner.stop_all()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
