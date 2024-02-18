# Copyright 2022 Memgraph Ltd.
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
            "7688",
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
    "coordinator": {
        "args": ["--bolt-port", "7690", "--log-level=TRACE", "--raft-server-id=1", "--raft-server-port=10111"],
        "log_file": "coordinator.log",
        "setup_queries": [
            "REGISTER INSTANCE instance_1 ON '127.0.0.1:10011' WITH '127.0.0.1:10001';",
            "REGISTER INSTANCE instance_2 ON '127.0.0.1:10012' WITH '127.0.0.1:10002';",
            "REGISTER INSTANCE instance_3 ON '127.0.0.1:10013' WITH '127.0.0.1:10003';",
            "SET INSTANCE instance_3 TO MAIN",
        ],
    },
}


def test_replication_works_on_failover():
    # Goal of this test is to check the replication works after failover command.
    # 1. We start all replicas, main and coordinator manually: we want to be able to kill them ourselves without relying on external tooling to kill processes.
    # 2. We check that main has correct state
    # 3. We kill main
    # 4. We check that coordinator and new main have correct state
    # 5. We insert one vertex on new main
    # 6. We check that vertex appears on new replica
    safe_execute(shutil.rmtree, TEMP_DIR)

    # 1
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    # 2
    main_cursor = connect(host="localhost", port=7687).cursor()
    expected_data_on_main = [
        ("instance_1", "127.0.0.1:10001", "sync", 0, 0, "ready"),
        ("instance_2", "127.0.0.1:10002", "sync", 0, 0, "ready"),
    ]
    actual_data_on_main = sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))
    assert actual_data_on_main == expected_data_on_main

    # 3
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    # 4
    coord_cursor = connect(host="localhost", port=7690).cursor()

    def retrieve_data_show_repl_cluster():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))

    expected_data_on_coord = [
        ("coordinator_1", "127.0.0.1:10111", "", True, "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", True, "main"),
        ("instance_2", "", "127.0.0.1:10012", True, "replica"),
        ("instance_3", "", "127.0.0.1:10013", False, "unknown"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_repl_cluster)

    new_main_cursor = connect(host="localhost", port=7688).cursor()

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(new_main_cursor, "SHOW REPLICAS;")))

    expected_data_on_new_main = [
        ("instance_2", "127.0.0.1:10002", "sync", 0, 0, "ready"),
        ("instance_3", "127.0.0.1:10003", "sync", 0, 0, "invalid"),
    ]
    mg_sleep_and_assert(expected_data_on_new_main, retrieve_data_show_replicas)

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")
    expected_data_on_new_main = [
        ("instance_2", "127.0.0.1:10002", "sync", 0, 0, "ready"),
        ("instance_3", "127.0.0.1:10003", "sync", 0, 0, "ready"),
    ]
    mg_sleep_and_assert(expected_data_on_new_main, retrieve_data_show_replicas)

    # 5
    execute_and_fetch_all(new_main_cursor, "CREATE ();")

    # 6
    alive_replica_cursror = connect(host="localhost", port=7689).cursor()
    res = execute_and_fetch_all(alive_replica_cursror, "MATCH (n) RETURN count(n) as count;")[0][0]
    assert res == 1, "Vertex should be replicated"
    interactive_mg_runner.stop_all(MEMGRAPH_INSTANCES_DESCRIPTION)


def test_replication_works_on_replica_instance_restart():
    # Goal of this test is to check the replication works after replica goes down and restarts
    # 1. We start all replicas, main and coordinator manually: we want to be able to kill them ourselves without relying on external tooling to kill processes.
    # 2. We check that main has correct state
    # 3. We kill replica
    # 4. We check that main cannot replicate to replica
    # 5. We bring replica back up
    # 6. We check that replica gets data
    safe_execute(shutil.rmtree, TEMP_DIR)

    # 1
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    # 2
    main_cursor = connect(host="localhost", port=7687).cursor()
    expected_data_on_main = [
        ("instance_1", "127.0.0.1:10001", "sync", 0, 0, "ready"),
        ("instance_2", "127.0.0.1:10002", "sync", 0, 0, "ready"),
    ]
    actual_data_on_main = sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))
    assert actual_data_on_main == expected_data_on_main

    # 3
    coord_cursor = connect(host="localhost", port=7690).cursor()

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_2")

    def retrieve_data_show_repl_cluster():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))

    expected_data_on_coord = [
        ("coordinator_1", "127.0.0.1:10111", "", True, "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", True, "replica"),
        ("instance_2", "", "127.0.0.1:10012", False, "unknown"),
        ("instance_3", "", "127.0.0.1:10013", True, "main"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_repl_cluster)

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))

    expected_data_on_main = [
        ("instance_1", "127.0.0.1:10001", "sync", 0, 0, "ready"),
        ("instance_2", "127.0.0.1:10002", "sync", 0, 0, "invalid"),
    ]
    mg_sleep_and_assert(expected_data_on_main, retrieve_data_show_replicas)

    # 4
    instance_1_cursor = connect(host="localhost", port=7688).cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(main_cursor, "CREATE ();")
    assert (
        str(e.value)
        == "Replication Exception: At least one SYNC replica has not confirmed committing last transaction. Check the status of the replicas using 'SHOW REPLICAS' query."
    )

    res_instance_1 = execute_and_fetch_all(instance_1_cursor, "MATCH (n) RETURN count(n)")[0][0]
    assert res_instance_1 == 1

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))

    expected_data_on_main = [
        ("instance_1", "127.0.0.1:10001", "sync", 2, 0, "ready"),
        ("instance_2", "127.0.0.1:10002", "sync", 0, 0, "invalid"),
    ]
    mg_sleep_and_assert(expected_data_on_main, retrieve_data_show_replicas)

    # 5.

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_2")

    def retrieve_data_show_repl_cluster():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))

    expected_data_on_coord = [
        ("coordinator_1", "127.0.0.1:10111", "", True, "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", True, "replica"),
        ("instance_2", "", "127.0.0.1:10012", True, "replica"),
        ("instance_3", "", "127.0.0.1:10013", True, "main"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_repl_cluster)

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))

    expected_data_on_main = [
        ("instance_1", "127.0.0.1:10001", "sync", 2, 0, "ready"),
        ("instance_2", "127.0.0.1:10002", "sync", 2, 0, "ready"),
    ]
    mg_sleep_and_assert(expected_data_on_main, retrieve_data_show_replicas)

    # 6.
    instance_2_cursor = connect(port=7689, host="localhost").cursor()
    execute_and_fetch_all(main_cursor, "CREATE ();")
    res_instance_2 = execute_and_fetch_all(instance_2_cursor, "MATCH (n) RETURN count(n)")[0][0]
    assert res_instance_2 == 2


def test_show_instances():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    instance1_cursor = connect(host="localhost", port=7688).cursor()
    instance2_cursor = connect(host="localhost", port=7689).cursor()
    instance3_cursor = connect(host="localhost", port=7687).cursor()
    coord_cursor = connect(host="localhost", port=7690).cursor()

    def show_repl_cluster():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))

    expected_data = [
        ("coordinator_1", "127.0.0.1:10111", "", True, "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", True, "replica"),
        ("instance_2", "", "127.0.0.1:10012", True, "replica"),
        ("instance_3", "", "127.0.0.1:10013", True, "main"),
    ]
    mg_sleep_and_assert(expected_data, show_repl_cluster)

    def retrieve_data_show_repl_role_instance1():
        return sorted(list(execute_and_fetch_all(instance1_cursor, "SHOW REPLICATION ROLE;")))

    def retrieve_data_show_repl_role_instance2():
        return sorted(list(execute_and_fetch_all(instance2_cursor, "SHOW REPLICATION ROLE;")))

    def retrieve_data_show_repl_role_instance3():
        return sorted(list(execute_and_fetch_all(instance3_cursor, "SHOW REPLICATION ROLE;")))

    mg_sleep_and_assert([("replica",)], retrieve_data_show_repl_role_instance1)
    mg_sleep_and_assert([("replica",)], retrieve_data_show_repl_role_instance2)
    mg_sleep_and_assert([("main",)], retrieve_data_show_repl_role_instance3)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_1")

    expected_data = [
        ("coordinator_1", "127.0.0.1:10111", "", True, "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", False, "unknown"),
        ("instance_2", "", "127.0.0.1:10012", True, "replica"),
        ("instance_3", "", "127.0.0.1:10013", True, "main"),
    ]
    mg_sleep_and_assert(expected_data, show_repl_cluster)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_2")

    expected_data = [
        ("coordinator_1", "127.0.0.1:10111", "", True, "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", False, "unknown"),
        ("instance_2", "", "127.0.0.1:10012", False, "unknown"),
        ("instance_3", "", "127.0.0.1:10013", True, "main"),
    ]
    mg_sleep_and_assert(expected_data, show_repl_cluster)


def test_simple_automatic_failover():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    main_cursor = connect(host="localhost", port=7687).cursor()
    expected_data_on_main = [
        ("instance_1", "127.0.0.1:10001", "sync", 0, 0, "ready"),
        ("instance_2", "127.0.0.1:10002", "sync", 0, 0, "ready"),
    ]
    actual_data_on_main = sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))
    assert actual_data_on_main == expected_data_on_main

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    coord_cursor = connect(host="localhost", port=7690).cursor()

    def retrieve_data_show_repl_cluster():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))

    expected_data_on_coord = [
        ("coordinator_1", "127.0.0.1:10111", "", True, "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", True, "main"),
        ("instance_2", "", "127.0.0.1:10012", True, "replica"),
        ("instance_3", "", "127.0.0.1:10013", False, "unknown"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_repl_cluster)

    new_main_cursor = connect(host="localhost", port=7688).cursor()

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


def test_registering_replica_fails_name_exists():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    coord_cursor = connect(host="localhost", port=7690).cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(
            coord_cursor,
            "REGISTER INSTANCE instance_1 ON '127.0.0.1:10051' WITH '127.0.0.1:10111';",
        )
    assert str(e.value) == "Couldn't register replica instance since instance with such name already exists!"
    shutil.rmtree(TEMP_DIR)


def test_registering_replica_fails_endpoint_exists():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    coord_cursor = connect(host="localhost", port=7690).cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(
            coord_cursor,
            "REGISTER INSTANCE instance_5 ON '127.0.0.1:10011' WITH '127.0.0.1:10005';",
        )
    assert str(e.value) == "Couldn't register replica instance since instance with such endpoint already exists!"


def test_replica_instance_restarts():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    cursor = connect(host="localhost", port=7690).cursor()

    def show_repl_cluster():
        return sorted(list(execute_and_fetch_all(cursor, "SHOW INSTANCES;")))

    expected_data_up = [
        ("coordinator_1", "127.0.0.1:10111", "", True, "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", True, "replica"),
        ("instance_2", "", "127.0.0.1:10012", True, "replica"),
        ("instance_3", "", "127.0.0.1:10013", True, "main"),
    ]
    mg_sleep_and_assert(expected_data_up, show_repl_cluster)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_1")

    expected_data_down = [
        ("coordinator_1", "127.0.0.1:10111", "", True, "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", False, "unknown"),
        ("instance_2", "", "127.0.0.1:10012", True, "replica"),
        ("instance_3", "", "127.0.0.1:10013", True, "main"),
    ]
    mg_sleep_and_assert(expected_data_down, show_repl_cluster)

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_1")

    mg_sleep_and_assert(expected_data_up, show_repl_cluster)

    instance1_cursor = connect(host="localhost", port=7688).cursor()

    def retrieve_data_show_repl_role_instance1():
        return sorted(list(execute_and_fetch_all(instance1_cursor, "SHOW REPLICATION ROLE;")))

    expected_data_replica = [("replica",)]
    mg_sleep_and_assert(expected_data_replica, retrieve_data_show_repl_role_instance1)


def test_automatic_failover_main_back_as_replica():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    coord_cursor = connect(host="localhost", port=7690).cursor()

    def retrieve_data_show_repl_cluster():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))

    expected_data_after_failover = [
        ("coordinator_1", "127.0.0.1:10111", "", True, "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", True, "main"),
        ("instance_2", "", "127.0.0.1:10012", True, "replica"),
        ("instance_3", "", "127.0.0.1:10013", False, "unknown"),
    ]
    mg_sleep_and_assert(expected_data_after_failover, retrieve_data_show_repl_cluster)

    expected_data_after_main_coming_back = [
        ("coordinator_1", "127.0.0.1:10111", "", True, "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", True, "main"),
        ("instance_2", "", "127.0.0.1:10012", True, "replica"),
        ("instance_3", "", "127.0.0.1:10013", True, "replica"),
    ]

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")
    mg_sleep_and_assert(expected_data_after_main_coming_back, retrieve_data_show_repl_cluster)

    instance3_cursor = connect(host="localhost", port=7687).cursor()

    def retrieve_data_show_repl_role_instance3():
        return sorted(list(execute_and_fetch_all(instance3_cursor, "SHOW REPLICATION ROLE;")))

    mg_sleep_and_assert([("replica",)], retrieve_data_show_repl_role_instance3)


def test_automatic_failover_main_back_as_main():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_1")
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_2")
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    coord_cursor = connect(host="localhost", port=7690).cursor()

    def retrieve_data_show_repl_cluster():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))

    expected_data_all_down = [
        ("coordinator_1", "127.0.0.1:10111", "", True, "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", False, "unknown"),
        ("instance_2", "", "127.0.0.1:10012", False, "unknown"),
        ("instance_3", "", "127.0.0.1:10013", False, "unknown"),
    ]

    mg_sleep_and_assert(expected_data_all_down, retrieve_data_show_repl_cluster)

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")
    expected_data_main_back = [
        ("coordinator_1", "127.0.0.1:10111", "", True, "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", False, "unknown"),
        ("instance_2", "", "127.0.0.1:10012", False, "unknown"),
        ("instance_3", "", "127.0.0.1:10013", True, "main"),
    ]
    mg_sleep_and_assert(expected_data_main_back, retrieve_data_show_repl_cluster)

    instance3_cursor = connect(host="localhost", port=7687).cursor()

    def retrieve_data_show_repl_role_instance3():
        return sorted(list(execute_and_fetch_all(instance3_cursor, "SHOW REPLICATION ROLE;")))

    mg_sleep_and_assert([("main",)], retrieve_data_show_repl_role_instance3)

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_1")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_2")

    expected_data_replicas_back = [
        ("coordinator_1", "127.0.0.1:10111", "", True, "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", True, "replica"),
        ("instance_2", "", "127.0.0.1:10012", True, "replica"),
        ("instance_3", "", "127.0.0.1:10013", True, "main"),
    ]

    mg_sleep_and_assert(expected_data_replicas_back, retrieve_data_show_repl_cluster)

    instance1_cursor = connect(host="localhost", port=7688).cursor()
    instance2_cursor = connect(host="localhost", port=7689).cursor()

    def retrieve_data_show_repl_role_instance1():
        return sorted(list(execute_and_fetch_all(instance1_cursor, "SHOW REPLICATION ROLE;")))

    def retrieve_data_show_repl_role_instance2():
        return sorted(list(execute_and_fetch_all(instance2_cursor, "SHOW REPLICATION ROLE;")))

    mg_sleep_and_assert([("replica",)], retrieve_data_show_repl_role_instance1)
    mg_sleep_and_assert([("replica",)], retrieve_data_show_repl_role_instance2)
    mg_sleep_and_assert([("main",)], retrieve_data_show_repl_role_instance3)


def test_disable_multiple_mains():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    coord_cursor = connect(host="localhost", port=7690).cursor()

    try:
        execute_and_fetch_all(
            coord_cursor,
            "SET INSTANCE instance_1 TO MAIN;",
        )
    except Exception as e:
        assert str(e) == "Couldn't set instance to main since there is already a main instance in cluster!"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
