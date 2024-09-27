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
from common import (
    connect,
    execute_and_fetch_all,
    get_data_path,
    get_logs_path,
    ignore_elapsed_time_from_results,
)
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))


@pytest.fixture
def test_name(request):
    return request.node.name


@pytest.fixture(autouse=True)
def cleanup_after_test():
    # Run the test
    yield
    # Stop and delete directories after running the test
    interactive_mg_runner.kill_all(keep_directories=False)


file = "not_replicate_from_old_main"

MEMGRAPH_FIRST_CLUSTER_DESCRIPTION = {
    "shared_replica": {
        "args": ["--experimental-enabled=high-availability", "--bolt-port", "7688", "--log-level", "TRACE"],
        "log_file": f"{get_logs_path(file, 'test_replication_works_on_failover')}/replica2.log",
        "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10001;"],
    },
    "main1": {
        "args": ["--experimental-enabled=high-availability", "--bolt-port", "7687", "--log-level", "TRACE"],
        "log_file": f"{get_logs_path(file, 'test_replication_works_on_failover')}/main.log",
        "setup_queries": ["REGISTER REPLICA shared_replica SYNC TO 'localhost:10001' ;"],
    },
}


MEMGRAPH_SECOND_CLUSTER_DESCRIPTION = {
    "replica": {
        "args": ["--experimental-enabled=high-availability", "--bolt-port", "7689", "--log-level", "TRACE"],
        "log_file": "high_availability/not_replicate_from_old_main/replica.log",
        "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
    },
    "main_2": {
        "args": ["--experimental-enabled=high-availability", "--bolt-port", "7690", "--log-level", "TRACE"],
        "log_file": "high_availability/not_replicate_from_old_main/main_2.log",
        "setup_queries": [
            "REGISTER REPLICA shared_replica SYNC TO 'localhost:10001' ;",
            "REGISTER REPLICA replica SYNC TO 'localhost:10002' ; ",
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
        (
            "shared_replica",
            "localhost:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))

    mg_sleep_and_assert(expected_data_on_main, retrieve_data_show_replicas)

    # 3
    interactive_mg_runner.start_all_keep_others(MEMGRAPH_SECOND_CLUSTER_DESCRIPTION)

    # 4
    new_main_cursor = connect(host="localhost", port=7690).cursor()

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(new_main_cursor, "SHOW REPLICAS;")))

    expected_data_on_new_main = [
        (
            "replica",
            "localhost:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "shared_replica",
            "localhost:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
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


def test_not_replicate_old_main_register_new_cluster(test_name):
    # Goal of this test is to check that although replica is registered in one cluster
    # it can be re-registered to new cluster
    # This flow checks if Registering replica is idempotent and that old main cannot talk to replica
    # 1. We start all replicas and main in one cluster
    # 2. Main from first cluster can see all replicas
    # 3. We start all replicas and main in second cluster, by reusing one replica from first cluster
    # 4. New main should see replica. Registration should pass (idempotent registration)
    # 5. Old main should not talk to new replica
    # 6. New main should talk to replica

    MEMGRAPH_FISRT_COORD_CLUSTER_DESCRIPTION = {
        "shared_instance": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7688",
                "--log-level",
                "TRACE",
                "--management-port",
                "10011",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/shared_instance.log",
            "data_directory": f"{get_data_path(file, test_name)}/shared_instance",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7689",
                "--log-level",
                "TRACE",
                "--management-port",
                "10012",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_2",
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
                "--coordinator-hostname=localhost",
                "--management-port=10121",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_1",
            "setup_queries": [
                "REGISTER INSTANCE shared_instance WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
                "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
                "SET INSTANCE instance_2 TO MAIN",
            ],
        },
    }

    # 1
    interactive_mg_runner.start_all_keep_others(MEMGRAPH_FISRT_COORD_CLUSTER_DESCRIPTION)

    # 2

    first_cluster_coord_cursor = connect(host="localhost", port=7690).cursor()

    def show_repl_cluster():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(first_cluster_coord_cursor, "SHOW INSTANCES;")))
        )

    expected_data_up_first_cluster = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader"),
        ("instance_2", "localhost:7689", "", "localhost:10012", "up", "main"),
        ("shared_instance", "localhost:7688", "", "localhost:10011", "up", "replica"),
    ]

    mg_sleep_and_assert(expected_data_up_first_cluster, show_repl_cluster)

    # 3

    MEMGRAPH_SECOND_COORD_CLUSTER_DESCRIPTION = {
        "instance_3": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7687",
                "--log-level",
                "TRACE",
                "--management-port",
                "10013",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_3",
            "setup_queries": [],
        },
        "coordinator_2": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7691",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10112",
                "--coordinator-hostname=localhost",
                "--management-port=10122",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_2",
            "setup_queries": [],
        },
    }

    interactive_mg_runner.start_all_keep_others(MEMGRAPH_SECOND_COORD_CLUSTER_DESCRIPTION)
    second_cluster_coord_cursor = connect(host="localhost", port=7691).cursor()
    execute_and_fetch_all(
        second_cluster_coord_cursor,
        "REGISTER INSTANCE shared_instance WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
    )
    execute_and_fetch_all(
        second_cluster_coord_cursor,
        "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
    )
    execute_and_fetch_all(second_cluster_coord_cursor, "SET INSTANCE instance_3 TO MAIN")

    # 4

    def show_repl_cluster():
        return ignore_elapsed_time_from_results(
            sorted(list(execute_and_fetch_all(second_cluster_coord_cursor, "SHOW INSTANCES;")))
        )

    expected_data_up_second_cluster = [
        ("coordinator_1", "localhost:7691", "localhost:10112", "localhost:10122", "up", "leader"),
        ("instance_3", "localhost:7687", "", "localhost:10013", "up", "main"),
        ("shared_instance", "localhost:7688", "", "localhost:10011", "up", "replica"),
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
