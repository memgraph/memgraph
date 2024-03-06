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
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

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

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

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


def test_distributed_automatic_failover_with_leadership_change():
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_3")
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
        ("instance_3", "", "", "unknown", "main"),  # TODO: (andi) Will become unknown.
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

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_3")


def test_no_leader_after_leader_and_follower_die():
    # 1. Register all but one replication instnce on the first leader.
    # 2. Kill the leader and a follower.
    # 3. Check that the remaining follower is not promoted to leader by trying to register remaining replication instance.

    safe_execute(shutil.rmtree, TEMP_DIR)

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
                "REGISTER INSTANCE instance_2 ON '127.0.0.1:10012' WITH '127.0.0.1:10002'",
                "REGISTER INSTANCE instance_3 ON '127.0.0.1:10013' WITH '127.0.0.1:10003'",
                "SET INSTANCE instance_3 TO MAIN",
            ],
        },
    }
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_3")
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_2")

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(coord_cursor_1, "REGISTER INSTANCE instance_1 ON '127.0.0.1:10011' WITH '127.0.0.10001'")
        assert str(e) == "Couldn't register replica instance since coordinator is not a leader!"


def test_old_main_comes_back_on_new_leader_as_replica():
    # 1. Start all instances.
    # 2. Kill the main instance
    # 3. Kill the leader
    # 4. Start the old main instance
    # 5. Run SHOW INSTANCES on the new leader and check that the old main instance is registered as a replica
    # 6. Start again previous leader

    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_3")
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
        ("instance_3", "", "", "unknown", "main"),  # TODO: (andi) Will become unknown.
    ]
    mg_sleep_and_assert_any_function(leader_data, [show_instances_coord1, show_instances_coord2])
    mg_sleep_and_assert_any_function(follower_data, [show_instances_coord1, show_instances_coord2])

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    leader_data = [
        ("coordinator_1", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "127.0.0.1:10113", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "up", "main"),
        ("instance_2", "", "127.0.0.1:10012", "up", "replica"),
        ("instance_3", "", "127.0.0.1:10013", "up", "replica"),
    ]
    mg_sleep_and_assert_any_function(leader_data, [show_instances_coord1, show_instances_coord2])

    new_main_cursor = connect(host="localhost", port=7687).cursor()

    def show_replicas():
        return sorted(list(execute_and_fetch_all(new_main_cursor, "SHOW REPLICAS;")))

    replicas = [
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
    mg_sleep_and_assert_collection(replicas, show_replicas)

    execute_and_fetch_all(new_main_cursor, "CREATE (n:Node {name: 'node'})")

    replica_2_cursor = connect(host="localhost", port=7688).cursor()
    assert len(execute_and_fetch_all(replica_2_cursor, "MATCH (n) RETURN n;")) == 1

    replica_3_cursor = connect(host="localhost", port=7689).cursor()
    assert len(execute_and_fetch_all(replica_3_cursor, "MATCH (n) RETURN n;")) == 1

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_3")


def test_old_main_comes_back_on_new_leader_as_main():
    # 1. Start all instances.
    # 2. Kill all instances
    # 3. Kill the leader
    # 4. Start the old main instance
    # 5. Run SHOW INSTANCES on the new leader and check that the old main instance is main once again

    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()

    setup_queries = [
        "ADD COORDINATOR 1 ON '127.0.0.1:10111'",
        "ADD COORDINATOR 2 ON '127.0.0.1:10112'",
        "REGISTER INSTANCE instance_1 ON '127.0.0.1:10011' WITH '127.0.0.1:10001'",
        "REGISTER INSTANCE instance_2 ON '127.0.0.1:10012' WITH '127.0.0.1:10002'",
        "REGISTER INSTANCE instance_3 ON '127.0.0.1:10013' WITH '127.0.0.1:10003'",
        "SET INSTANCE instance_3 TO MAIN",
    ]

    execute_setup_queries(coord_cursor_3, setup_queries)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_1")
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_2")
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_3")

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;")))

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    def show_instances_coord2():
        return sorted(list(execute_and_fetch_all(coord_cursor_2, "SHOW INSTANCES;")))

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    leader_data = [
        ("coordinator_1", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "127.0.0.1:10113", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "down", "unknown"),
        ("instance_2", "", "127.0.0.1:10012", "down", "unknown"),
        ("instance_3", "", "127.0.0.1:10013", "up", "main"),
    ]

    follower_data = [
        ("coordinator_1", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "127.0.0.1:10113", "", "unknown", "coordinator"),
        ("instance_1", "", "", "unknown", "replica"),
        ("instance_2", "", "", "unknown", "replica"),
        ("instance_3", "", "", "unknown", "main"),
    ]
    mg_sleep_and_assert_any_function(leader_data, [show_instances_coord1, show_instances_coord2])
    mg_sleep_and_assert_any_function(follower_data, [show_instances_coord1, show_instances_coord2])

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_1")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_2")

    new_main_cursor = connect(host="localhost", port=7689).cursor()

    def show_replicas():
        return sorted(list(execute_and_fetch_all(new_main_cursor, "SHOW REPLICAS;")))

    replicas = [
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
    mg_sleep_and_assert_collection(replicas, show_replicas)

    execute_and_fetch_all(new_main_cursor, "CREATE (n:Node {name: 'node'})")

    replica_1_cursor = connect(host="localhost", port=7687).cursor()
    assert len(execute_and_fetch_all(replica_1_cursor, "MATCH (n) RETURN n;")) == 1

    replica_2_cursor = connect(host="localhost", port=7688).cursor()
    assert len(execute_and_fetch_all(replica_2_cursor, "MATCH (n) RETURN n;")) == 1

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_3")


def test_old_main_comes_back_on_new_leader_as_main():
    # 1. Start all instances.
    # 2. Kill all instances
    # 3. Kill the leader
    # 4. Start the old main instance
    # 5. Run SHOW INSTANCES on the new leader and check that the old main instance is main once again

    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_1")
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_2")
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "coordinator_3")

    coord_cursor_1 = connect(host="localhost", port=7690).cursor()

    def show_instances_coord1():
        return sorted(list(execute_and_fetch_all(coord_cursor_1, "SHOW INSTANCES;")))

    coord_cursor_2 = connect(host="localhost", port=7691).cursor()

    def show_instances_coord2():
        return sorted(list(execute_and_fetch_all(coord_cursor_2, "SHOW INSTANCES;")))

    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "instance_3")

    leader_data = [
        ("coordinator_1", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "127.0.0.1:10113", "", "unknown", "coordinator"),
        ("instance_1", "", "127.0.0.1:10011", "down", "unknown"),
        ("instance_2", "", "127.0.0.1:10012", "down", "unknown"),
        ("instance_3", "", "127.0.0.1:10013", "up", "main"),
    ]
    mg_sleep_and_assert_any_function(leader_data, [show_instances_coord1, show_instances_coord2])

    follower_data = [
        ("coordinator_1", "127.0.0.1:10111", "", "unknown", "coordinator"),
        ("coordinator_2", "127.0.0.1:10112", "", "unknown", "coordinator"),
        ("coordinator_3", "127.0.0.1:10113", "", "unknown", "coordinator"),
        ("instance_1", "", "", "unknown", "replica"),
        ("instance_2", "", "", "unknown", "replica"),
        ("instance_3", "", "", "unknown", "main"),
    ]
    mg_sleep_and_assert_any_function(leader_data, [show_instances_coord1, show_instances_coord2])
    mg_sleep_and_assert_any_function(follower_data, [show_instances_coord1, show_instances_coord2])


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))

