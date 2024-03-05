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
from time import sleep

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
    # Goal of this test is to check that coordination works if one instance fails
    # with multiple coordinators

    # 1. Start all manually
    # 2. Everything is set up on main
    # 3. MAIN dies
    # 4. New main elected

    # 1
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    # 2
    main_cursor = connect(host="localhost", port=7689).cursor()
    expected_data_on_main = [
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

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))

    mg_sleep_and_assert_collection(expected_data_on_main, retrieve_data_show_replicas)

    # 3
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    # 4
    coord_cursor = connect(host="localhost", port=7692).cursor()

    def retrieve_data_show_repl_cluster():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))

    expected_data_on_coord = [
        ("coordinator_1", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "127.0.0.1:10113", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "up", "main"),
        ("instance_2", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "", "127.0.0.1:10013", "down", "unknown"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_repl_cluster)

    new_main_cursor = connect(host="localhost", port=7687).cursor()

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(new_main_cursor, "SHOW REPLICAS;")))

    expected_data_on_new_main = [
        (
            "instance_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "instance_3",
            "127.0.0.1:10003",
            "sync",
            {"ts": 0, "behind": None, "status": "invalid"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "invalid"}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data_on_new_main, retrieve_data_show_replicas)

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")
    expected_data_on_new_main_old_alive = [
        (
            "instance_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "instance_3",
            "127.0.0.1:10003",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]

    mg_sleep_and_assert_collection(expected_data_on_new_main_old_alive, retrieve_data_show_replicas)


def test_distributed_automatic_failover_after_coord_dies():
    # Goal of this test is to check if main and coordinator die at same time in the beginning that
    # everything works fine
    # 1. Start all manually
    # 2. Coordinator dies
    # 3. MAIN dies
    # 4.
    safe_execute(shutil.rmtree, TEMP_DIR)

    # 1
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    # 2
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_3")

    # 3
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;")))

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    def show_instances_coord2():
        return sorted(list(execute_and_fetch_all(coord_cursor_2, "SHOW INSTANCES;")))

    leader_data = [
        ("coordinator_1", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "127.0.0.1:10113", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "up", "main"),
        ("instance_2", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "", "127.0.0.1:10013", "down", "unknown"),
    ]
    mg_sleep_and_assert_any_function(leader_data, [show_instances_coord1, show_instances_coord2])

    follower_data = [
        ("coordinator_1", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "127.0.0.1:10113", "", "unknown", "coordinator"),
        ("instance_1", "", "", "unknown", "main"),
        ("instance_2", "", "", "unknown", "replica"),
        ("instance_3", "", "", "unknown", "main"),
    ]
    mg_sleep_and_assert_any_function(leader_data, [show_instances_coord1, show_instances_coord2])
    mg_sleep_and_assert_any_function(follower_data, [show_instances_coord1, show_instances_coord2])

    new_main_cursor = connect(host="localhost", port=7687).cursor()

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(new_main_cursor, "SHOW REPLICAS;")))

    expected_data_on_new_main = [
        (
            "instance_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "instance_3",
            "127.0.0.1:10003",
            "sync",
            {"ts": 0, "behind": None, "status": "invalid"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "invalid"}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data_on_new_main, retrieve_data_show_replicas)

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")
    expected_data_on_new_main_old_alive = [
        (
            "instance_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "instance_3",
            "127.0.0.1:10003",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]

    mg_sleep_and_assert_collection(expected_data_on_new_main_old_alive, retrieve_data_show_replicas)


@pytest.mark.parametrize("data_recovery", ["false"])
def test_distributed_coordinators_work_partial_failover(data_recovery):
    # Goal of this test is to check that correct MAIN instance is chosen
    # if coordinator dies while failover is being done
    # 1. We start all replicas, main and 4 coordinators manually
    # 2. We check that main has correct state
    # 3. Create initial data on MAIN
    # 4. Expect data to be copied on all replicas
    # 5. Kill MAIN: instance 3

    # 6. Failover should succeed to instance 1 -> SHOW ROLE -> MAIN
    # 7. Kill 2 coordinators (1 and 2) as soon as instance becomes MAIN
    # 8. coord 4 become follower (2 coords dead, no leader)
    # 8. instance_1 writes (MAIN, successfully)
    # 9. Kill coordinator 4
    # 10. Connect to coordinator 3
    # 11. Start coordinator_1 and coordinator_2 (coord 3 probably leader)
    # 12. Failover again
    # 13. Main can't write to replicas anymore, coordinator can't progress

    temp_dir = tempfile.TemporaryDirectory().name

    MEMGRAPH_INNER_INSTANCES_DESCRIPTION = {
        "instance_1": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7688",
                "--log-level",
                "TRACE",
                "--coordinator-server-port",
                "10011",
                "--replication-restore-state-on-startup",
                "true",
                f"--data-recovery-on-startup={data_recovery}",
                "--storage-recover-on-startup=false",
            ],
            "log_file": "instance_1.log",
            "data_directory": f"{temp_dir}/instance_1",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7689",
                "--log-level",
                "TRACE",
                "--coordinator-server-port",
                "10012",
                "--replication-restore-state-on-startup",
                "true",
                f"--data-recovery-on-startup={data_recovery}",
                "--storage-recover-on-startup=false",
            ],
            "log_file": "instance_2.log",
            "data_directory": f"{temp_dir}/instance_2",
            "setup_queries": [],
        },
        "instance_3": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7687",
                "--log-level",
                "TRACE",
                "--coordinator-server-port",
                "10013",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                f"{data_recovery}",
                "--storage-recover-on-startup=false",
            ],
            "log_file": "instance_3.log",
            "data_directory": f"{temp_dir}/instance_3",
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
            "data_directory": f"{temp_dir}/coordinator_1",
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
            "data_directory": f"{temp_dir}/coordinator_2",
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
            ],
            "log_file": "coordinator3.log",
            "data_directory": f"{temp_dir}/coordinator_3",
            "setup_queries": [],
        },
        "coordinator_4": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7693",
                "--log-level=TRACE",
                "--raft-server-id=4",
                "--raft-server-port=10114",
            ],
            "log_file": "coordinator4.log",
            "data_directory": f"{temp_dir}/coordinator_4",
            "setup_queries": [],
        },
    }

    # 1

    interactive_mg_runner.start_all_except(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, {"coordinator_4"})

    interactive_mg_runner.start(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "coordinator_4")

    coord_cursor = connect(host="localhost", port=7693).cursor()

    for query in [
        "ADD COORDINATOR 1 ON '127.0.0.1:10111';",
        "ADD COORDINATOR 2 ON '127.0.0.1:10112';",
        "ADD COORDINATOR 3 ON '127.0.0.1:10113';",
    ]:
        sleep(1)
        execute_and_fetch_all(coord_cursor, query)

    for query in [
        "REGISTER INSTANCE instance_3 ON '127.0.0.1:10013' WITH '127.0.0.1:10003';",
        "REGISTER INSTANCE instance_1 ON '127.0.0.1:10011' WITH '127.0.0.1:10001';",
        "REGISTER INSTANCE instance_2 ON '127.0.0.1:10012' WITH '127.0.0.1:10002';",
        "SET INSTANCE instance_3 TO MAIN;",
    ]:
        execute_and_fetch_all(coord_cursor, query)

    # 2

    main_cursor = connect(host="localhost", port=7687).cursor()
    expected_data_on_main = [
        (
            "instance_1",
            "127.0.0.1:10001",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
        (
            "instance_2",
            "127.0.0.1:10002",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
    ]

    main_cursor = connect(host="localhost", port=7687).cursor()

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))

    mg_sleep_and_assert_collection(expected_data_on_main, retrieve_data_show_replicas)

    coord_cursor = connect(host="localhost", port=7693).cursor()

    def retrieve_data_show_instances_main_coord():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))

    expected_data_on_coord = [
        ("coordinator_1", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "127.0.0.1:10113", "", "unknown", "coordinator"),
        ("coordinator_4", "127.0.0.1:10114", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "up", "replica"),
        ("instance_2", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "", "127.0.0.1:10013", "up", "main"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_instances_main_coord)

    # 3

    execute_and_fetch_all(main_cursor, "CREATE (:Epoch1Vertex {prop:1});")
    execute_and_fetch_all(main_cursor, "CREATE (:Epoch1Vertex {prop:2});")

    # 4
    instance_1_cursor = connect(host="localhost", port=7688).cursor()
    instance_2_cursor = connect(host="localhost", port=7689).cursor()

    assert execute_and_fetch_all(instance_1_cursor, "MATCH (n) RETURN count(n);")[0][0] == 2
    assert execute_and_fetch_all(instance_2_cursor, "MATCH (n) RETURN count(n);")[0][0] == 2

    # 5

    interactive_mg_runner.kill(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "instance_3")
    # 6

    sleep(4.5)
    max_tries = 100
    last_role = "replica"
    while max_tries:
        max_tries -= 1
        last_role = execute_and_fetch_all(instance_1_cursor, "SHOW REPLICATION ROLE;")[0][0]
        if "main" == last_role:
            interactive_mg_runner.kill(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "coordinator_1")
            interactive_mg_runner.kill(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "coordinator_2")
            break
        sleep(0.1)
    assert max_tries > 0 or last_role == "main"

    expected_data_on_coord = [
        ("coordinator_1", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "127.0.0.1:10113", "", "unknown", "coordinator"),
        ("coordinator_4", "127.0.0.1:10114", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "unknown", "main"),  # TODO although above we see it is MAIN
        ("instance_2", "", "127.0.0.1:10012", "unknown", "replica"),
        ("instance_3", "", "127.0.0.1:10013", "unknown", "main"),
    ]

    def retrieve_data_show_instances():
        return sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))

    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_instances)

    # 7
    def retrieve_role():
        return execute_and_fetch_all(instance_1_cursor, "SHOW REPLICATION ROLE;")[0]

    mg_sleep_and_assert("MAIN", retrieve_role)

    # 8

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(instance_1_cursor, "CREATE (:Epoch2Vertex {prop:1});")
    assert "At least one SYNC replica has not confirmed committing last transaction." in str(e.value)

    def get_vertex_count_instance_2():
        return execute_and_fetch_all(instance_2_cursor, "MATCH (n) RETURN count(n)")[0][0]

    mg_sleep_and_assert(3, get_vertex_count_instance_2)

    assert execute_and_fetch_all(instance_1_cursor, "MATCH (n) RETURN count(n);")[0][0] == 3

    # 9

    interactive_mg_runner.kill(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "coordinator_4")

    # 10

    coord_3_cursor = connect(port=7692, host="localhost").cursor()

    expected_data_on_coord = [
        ("coordinator_1", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "127.0.0.1:10113", "", "unknown", "coordinator"),
        ("coordinator_4", "127.0.0.1:10114", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "unknown", "replica"),  # TODO: bug this is main
        ("instance_2", "", "127.0.0.1:10012", "unknown", "replica"),
        ("instance_3", "", "127.0.0.1:10013", "unknown", "replica"),
    ]

    def retrieve_data_show_instances_new_leader():
        return sorted(list(execute_and_fetch_all(coord_3_cursor, "SHOW INSTANCES;")))

    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_instances_new_leader)

    # 11

    interactive_mg_runner.start(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "coordinator_1")
    interactive_mg_runner.start(MEMGRAPH_INNER_INSTANCES_DESCRIPTION, "coordinator_2")

    # 12

    expected_data_on_coord = [
        ("coordinator_1", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "127.0.0.1:10113", "", "unknown", "coordinator"),
        ("coordinator_4", "127.0.0.1:10114", "", "unknown", "coordinator"),
        (
            "instance_1",
            "",
            "127.0.0.1:10011",
            "up",
            "replica",
        ),  # TODO: bug this is main, this might crash instance, because we will maybe execute replica callback on MAIN, which won't work
        ("instance_2", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "", "127.0.0.1:10013", "down", "unknown"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_instances_new_leader)

    import time

    time.sleep(5)  # failover

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(instance_1_cursor, "CREATE (:Epoch2Vertex {prop:2});")
    assert "At least one SYNC replica has not confirmed committing last transaction." in str(e.value)

    def get_vertex_count_instance_2():
        return execute_and_fetch_all(instance_2_cursor, "MATCH (n) RETURN count(n)")[0][0]

    mg_sleep_and_assert(4, get_vertex_count_instance_2)  # MAIN will not be able to write to it anymore

    # 13


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-k", "test_distributed_coordinators_work_partial_failover", "-vv"]))
    sys.exit(pytest.main([__file__, "-rA"]))
