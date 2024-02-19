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
import shutil
import sys
import tempfile

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

MEMGRAPH_FIRST_CLUSTER_DESCRIPTION = {
    "shared_replica": {
        "args": ["--experimental-enabled=high-availability", "--bolt-port", "7688", "--log-level", "TRACE"],
        "log_file": "replica2.log",
        "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10001;"],
    },
    "main1": {
        "args": ["--experimental-enabled=high-availability", "--bolt-port", "7687", "--log-level", "TRACE"],
        "log_file": "main.log",
        "setup_queries": ["REGISTER REPLICA shared_replica SYNC TO '127.0.0.1:10001' ;"],
    },
}


MEMGRAPH_SECOND_CLUSTER_DESCRIPTION = {
    "replica": {
        "args": ["--experimental-enabled=high-availability", "--bolt-port", "7689", "--log-level", "TRACE"],
        "log_file": "replica.log",
        "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
    },
    "main_2": {
        "args": ["--experimental-enabled=high-availability", "--bolt-port", "7690", "--log-level", "TRACE"],
        "log_file": "main_2.log",
        "setup_queries": [
            "REGISTER REPLICA shared_replica SYNC TO '127.0.0.1:10001' ;",
            "REGISTER REPLICA replica SYNC TO '127.0.0.1:10002' ; ",
        ],
    },
}


def test_replication_works_on_failover():
    # Goal of this test is to check that after changing `shared_replica`
    # to be part of new cluster, `main` (old cluster) can't write any more to it

    # 1
    interactive_mg_runner.start_all_keep_others(MEMGRAPH_FIRST_CLUSTER_DESCRIPTION)

    # 2
    main_cursor = connect(host="localhost", port=7687).cursor()
    expected_data_on_main = [
        ("shared_replica", "127.0.0.1:10001", "sync", 0, 0, "ready"),
    ]
    actual_data_on_main = sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))
    assert actual_data_on_main == expected_data_on_main

    # 3
    interactive_mg_runner.start_all_keep_others(MEMGRAPH_SECOND_CLUSTER_DESCRIPTION)

    # 4
    new_main_cursor = connect(host="localhost", port=7690).cursor()

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(new_main_cursor, "SHOW REPLICAS;")))

    expected_data_on_new_main = [
        ("replica", "127.0.0.1:10002", "sync", 0, 0, "ready"),
        ("shared_replica", "127.0.0.1:10001", "sync", 0, 0, "ready"),
    ]
    mg_sleep_and_assert(expected_data_on_new_main, retrieve_data_show_replicas)

    # 5
    shared_replica_cursor = connect(host="localhost", port=7688).cursor()

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(main_cursor, "CREATE ();")
    assert "At least one SYNC replica has not confirmed committing last transaction." in str(e.value)

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


def test_not_replicate_old_main_register_new_cluster():
    # Goal of this test is to check that although replica is registered in one cluster
    # it can be re-registered to new cluster
    # This flow checks if Registering replica is idempotent and that old main cannot talk to replica
    # 1. We start all replicas and main in one cluster
    # 2. Main from first cluster can see all replicas
    # 3. We start all replicas and main in second cluster, by reusing one replica from first cluster
    # 4. New main should see replica. Registration should pass (idempotent registration)
    # 5. Old main should not talk to new replica
    # 6. New main should talk to replica

    TEMP_DIR = tempfile.TemporaryDirectory().name
    MEMGRAPH_FISRT_COORD_CLUSTER_DESCRIPTION = {
        "shared_instance": {
            "args": [
                "--bolt-port",
                "7688",
                "--log-level",
                "TRACE",
                "--coordinator-server-port",
                "10011",
            ],
            "log_file": "instance_1.log",
            "data_directory": f"{TEMP_DIR}/shared_instance",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--bolt-port",
                "7689",
                "--log-level",
                "TRACE",
                "--coordinator-server-port",
                "10012",
            ],
            "log_file": "instance_2.log",
            "data_directory": f"{TEMP_DIR}/instance_2",
            "setup_queries": [],
        },
        "coordinator_1": {
            "args": ["--bolt-port", "7690", "--log-level=TRACE", "--raft-server-id=1", "--raft-server-port=10111"],
            "log_file": "coordinator.log",
            "setup_queries": [
                "REGISTER INSTANCE shared_instance ON '127.0.0.1:10011' WITH '127.0.0.1:10001';",
                "REGISTER INSTANCE instance_2 ON '127.0.0.1:10012' WITH '127.0.0.1:10002';",
                "SET INSTANCE instance_2 TO MAIN",
            ],
        },
    }

    # 1
    interactive_mg_runner.start_all_keep_others(MEMGRAPH_FISRT_COORD_CLUSTER_DESCRIPTION)

    # 2

    first_cluster_coord_cursor = connect(host="localhost", port=7690).cursor()

    def show_repl_cluster():
        return sorted(list(execute_and_fetch_all(first_cluster_coord_cursor, "SHOW INSTANCES;")))

    expected_data_up_first_cluster = [
        ("coordinator_1", "127.0.0.1:10111", "", True, "coordinator"),
        ("instance_2", "", "127.0.0.1:10012", True, "main"),
        ("shared_instance", "", "127.0.0.1:10011", True, "replica"),
    ]

    mg_sleep_and_assert(expected_data_up_first_cluster, show_repl_cluster)

    # 3

    MEMGRAPH_SECOND_COORD_CLUSTER_DESCRIPTION = {
        "instance_3": {
            "args": [
                "--bolt-port",
                "7687",
                "--log-level",
                "TRACE",
                "--coordinator-server-port",
                "10013",
            ],
            "log_file": "instance_3.log",
            "data_directory": f"{TEMP_DIR}/instance_3",
            "setup_queries": [],
        },
        "coordinator_2": {
            "args": ["--bolt-port", "7691", "--log-level=TRACE", "--raft-server-id=1", "--raft-server-port=10112"],
            "log_file": "coordinator.log",
            "setup_queries": [],
        },
    }

    interactive_mg_runner.start_all_keep_others(MEMGRAPH_SECOND_COORD_CLUSTER_DESCRIPTION)
    second_cluster_coord_cursor = connect(host="localhost", port=7691).cursor()
    execute_and_fetch_all(
        second_cluster_coord_cursor, "REGISTER INSTANCE shared_instance ON '127.0.0.1:10011' WITH '127.0.0.1:10001';"
    )
    execute_and_fetch_all(
        second_cluster_coord_cursor, "REGISTER INSTANCE instance_3 ON '127.0.0.1:10013' WITH '127.0.0.1:10003';"
    )
    execute_and_fetch_all(second_cluster_coord_cursor, "SET INSTANCE instance_3 TO MAIN")

    # 4

    def show_repl_cluster():
        return sorted(list(execute_and_fetch_all(second_cluster_coord_cursor, "SHOW INSTANCES;")))

    expected_data_up_second_cluster = [
        ("coordinator_1", "127.0.0.1:10112", "", True, "coordinator"),
        ("instance_3", "", "127.0.0.1:10013", True, "main"),
        ("shared_instance", "", "127.0.0.1:10011", True, "replica"),
    ]

    mg_sleep_and_assert(expected_data_up_second_cluster, show_repl_cluster)

    # 5
    main_1_cursor = connect(host="localhost", port=7689).cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(main_1_cursor, "CREATE ();")
    assert "At least one SYNC replica has not confirmed committing last transaction." in str(e.value)

    shared_replica_cursor = connect(host="localhost", port=7688).cursor()
    res = execute_and_fetch_all(shared_replica_cursor, "MATCH (n) RETURN count(n);")[0][0]
    assert res == 0, "Old main should not replicate to 'shared' replica"

    # 6
    main_2_cursor = connect(host="localhost", port=7687).cursor()

    execute_and_fetch_all(main_2_cursor, "CREATE ();")

    shared_replica_cursor = connect(host="localhost", port=7688).cursor()
    res = execute_and_fetch_all(shared_replica_cursor, "MATCH (n) RETURN count(n);")[0][0]
    assert res == 1, "New main should replicate to 'shared' replica"

    interactive_mg_runner.stop_all()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
