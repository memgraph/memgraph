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
import random
import sys
import tempfile

import interactive_mg_runner
import mgclient
import pytest
from common import execute_and_fetch_all
from mg_utils import mg_sleep_and_assert, mg_sleep_and_assert_collection

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
            "REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001';",
            "REGISTER REPLICA replica_2 SYNC TO '127.0.0.1:10002';",
            "REGISTER REPLICA replica_3 ASYNC TO '127.0.0.1:10003';",
            "REGISTER REPLICA replica_4 ASYNC TO '127.0.0.1:10004';",
        ],
    },
}


def test_conflict_at_startup(connection):
    # Goal of this test is to check starting up several instance with different replicas' configuration directory works as expected.
    # main_1 and main_2 have different directory.

    data_directory1 = tempfile.TemporaryDirectory()
    data_directory2 = tempfile.TemporaryDirectory()
    CONFIGURATION = {
        "main_1": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE"],
            "log_file": "main1.log",
            "setup_queries": [],
            "data_directory": f"{data_directory1.name}",
        },
        "main_2": {
            "args": ["--bolt-port", "7688", "--log-level=TRACE"],
            "log_file": "main2.log",
            "setup_queries": [],
            "data_directory": f"{data_directory2.name}",
        },
    }

    interactive_mg_runner.start_all(CONFIGURATION)
    cursor_1 = connection(7687, "main_1").cursor()
    cursor_2 = connection(7688, "main_2").cursor()

    assert execute_and_fetch_all(cursor_1, "SHOW REPLICATION ROLE;")[0][0] == "main"
    assert execute_and_fetch_all(cursor_2, "SHOW REPLICATION ROLE;")[0][0] == "main"


def test_basic_recovery_when_replica_is_kill_when_main_is_down():
    # Goal of this test is to check the recovery of main.
    # 0/ We start all replicas manually: we want to be able to kill them ourselves without relying on external tooling to kill processes.
    # 1/ We check that all replicas have the correct state: they should all be ready.
    # 2/ We kill main then kill a replica.
    # 3/ We re-start main: it should be able to restart.
    # 4/ Check status of replica: replica_2 is invalid.

    data_directory = tempfile.TemporaryDirectory()
    CONFIGURATION = {
        "replica_1": {
            "args": ["--bolt-port", "7688", "--log-level=TRACE", "--replication-restore-state-on-startup=true"],
            "log_file": "replica1.log",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10001;"],
        },
        "replica_2": {
            "args": ["--bolt-port", "7689", "--log-level=TRACE", "--replication-restore-state-on-startup=true"],
            "log_file": "replica2.log",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
        },
        "main": {
            "args": [
                "--bolt-port",
                "7687",
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--replication-restore-state-on-startup=true",
            ],
            "log_file": "main.log",
            "setup_queries": [],
            "data_directory": f"{data_directory.name}",
        },
    }

    interactive_mg_runner.start_all(CONFIGURATION)

    # We want to execute manually and not via the configuration, otherwise re-starting main would also execute these registration.
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001';")
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("REGISTER REPLICA replica_2 SYNC TO '127.0.0.1:10002';")

    # 1/
    expected_data = [
        (
            "replica_1",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    actual_data = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICAS;")

    assert all([x in actual_data for x in expected_data])

    def check_roles():
        assert "main" == interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICATION ROLE;")[0][0]
        for index in range(1, 2):
            assert (
                "replica"
                == interactive_mg_runner.MEMGRAPH_INSTANCES[f"replica_{index}"].query("SHOW REPLICATION ROLE;")[0][0]
            )

    check_roles()

    # 2/
    interactive_mg_runner.kill(CONFIGURATION, "main")
    interactive_mg_runner.kill(CONFIGURATION, "replica_2")

    # 3/
    interactive_mg_runner.start(CONFIGURATION, "main")

    # 4/
    expected_data = [
        (
            "replica_1",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "invalid"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "invalid"}},
        ),
    ]
    actual_data = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICAS;")
    assert all([x in actual_data for x in expected_data])


def test_async_replication_when_main_is_killed():
    # Goal of the test is to check that when main is randomly killed:
    # -the ASYNC replica always contains a valid subset of data of main.
    # We run the test 20 times, it should never fail.

    # 0/ Start main and replicas.
    # 1/ Register replicas.
    # 2/ Insert data in main, and randomly kill it.
    # 3/ Check that the ASYNC replica has a valid subset.

    for test_repetition in range(20):
        # 0/
        data_directory_main = tempfile.TemporaryDirectory()
        data_directory_replica = tempfile.TemporaryDirectory()
        CONFIGURATION = {
            "async_replica": {
                "args": ["--bolt-port", "7688", "--log-level=TRACE"],
                "log_file": "async_replica.log",
                "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10001;"],
                "data_directory": f"{data_directory_replica.name}",
            },
            "main": {
                "args": ["--bolt-port", "7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
                "log_file": "main.log",
                "setup_queries": [],
                "data_directory": f"{data_directory_main.name}",
            },
        }
        interactive_mg_runner.kill_all(CONFIGURATION)
        interactive_mg_runner.start_all(CONFIGURATION)

        # 1/
        interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(
            "REGISTER REPLICA async_replica ASYNC TO '127.0.0.1:10001';"
        )

        # 2/
        # First make sure that anything has been replicated
        for index in range(0, 5):
            interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(f"CREATE (p:Number {{name:{index}}})")
        expected_data = [("async_replica", "127.0.0.1:10001", "async", "ready")]

        def retrieve_data():
            replicas = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICAS;")
            return [
                (replica_name, ip, mode, info["memgraph"]["status"])
                for replica_name, ip, mode, sys_info, info in replicas
            ]

        actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
        assert all([x in actual_data for x in expected_data])

        for index in range(5, 50):
            interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(f"CREATE (p:Number {{name:{index}}})")
            if random.randint(0, 100) > 95:
                main_killed = f"Main was killed at index={index}"
                print(main_killed)
                interactive_mg_runner.kill(CONFIGURATION, "main")
                break

        # 3/
        # short explaination:
        # res_from_async_replica is an arithmetic sequence with:
        # -first term 0
        # -common difference 1
        # So we check its properties. If properties are fullfilled, it means the ASYNC replicas received a correct subset of messages
        # from main in the correct order.
        # In other word: res_from_async_replica is as [0, 1, ..., n-1, n] where values are consecutive integers. $
        # It should have the two properties:
        # -list is sorted
        # -the sum of all elements is equal to nOfTerms * (firstTerm + lastTerm) / 2

        QUERY_TO_CHECK = "MATCH (n) RETURN COLLECT(n.name);"
        res_from_async_replica = interactive_mg_runner.MEMGRAPH_INSTANCES["async_replica"].query(QUERY_TO_CHECK)[0][0]
        assert res_from_async_replica == sorted(res_from_async_replica), main_killed
        total_sum = sum(res_from_async_replica)
        expected_sum = len(res_from_async_replica) * (res_from_async_replica[0] + res_from_async_replica[-1]) / 2
        assert total_sum == expected_sum, main_killed

        data_directory_main.cleanup()
        data_directory_replica.cleanup()


def test_sync_replication_when_main_is_killed():
    # Goal of the test is to check that when main is randomly killed:
    # -the SYNC replica always contains the exact data that was in main.
    # We run the test 20 times, it should never fail.

    # 0/ Start main and replica.
    # 1/ Register replica.
    # 2/ Insert data in main, and randomly kill it.
    # 3/ Check that the SYNC replica has exactly the same data than main.

    for test_repetition in range(20):
        # 0/
        data_directory_main = tempfile.TemporaryDirectory()
        data_directory_replica = tempfile.TemporaryDirectory()
        CONFIGURATION = {
            "sync_replica": {
                "args": ["--bolt-port", "7688", "--log-level=TRACE"],
                "log_file": "sync_replica.log",
                "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10001;"],
                "data_directory": f"{data_directory_replica.name}",
            },
            "main": {
                "args": ["--bolt-port", "7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
                "log_file": "main.log",
                "setup_queries": [],
                "data_directory": f"{data_directory_main.name}",
            },
        }
        interactive_mg_runner.kill_all(CONFIGURATION)
        interactive_mg_runner.start_all(CONFIGURATION)

        # 1/
        interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(
            "REGISTER REPLICA sync_replica SYNC TO '127.0.0.1:10001';"
        )

        # 2/
        QUERY_TO_CHECK = "MATCH (n) RETURN COUNT(n.name);"
        last_result_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)[0][0]
        for index in range(50):
            interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(f"CREATE (p:Number {{name:{index}}})")
            last_result_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)[0][0]
            if random.randint(0, 100) > 95:
                main_killed = f"Main was killed at index={index}"
                interactive_mg_runner.kill(CONFIGURATION, "main")
                break

        # 3/
        # The SYNC replica should have exactly the same data than main.
        res_from_sync_replica = interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica"].query(QUERY_TO_CHECK)[0][0]
        assert last_result_from_main == res_from_sync_replica, main_killed

        data_directory_main.cleanup()
        data_directory_replica.cleanup()


def test_attempt_to_write_data_on_main_when_async_replica_is_down():
    # Goal of this test is to check that main can write new data if an async replica is down.
    # 0/ Start main and async replicas.
    # 1/ Check status of replicas.
    # 2/ Add some nodes to main and check it is propagated to the async_replicas.
    # 3/ Kill an async replica.
    # 4/ Try to add some data to main.
    # 5/ Check the status of replicas.
    # 6/ Check that the data was added to main and remaining replica.

    CONFIGURATION = {
        "async_replica1": {
            "args": ["--bolt-port", "7688", "--log-level=TRACE"],
            "log_file": "async_replica1.log",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10001;"],
        },
        "async_replica2": {
            "args": ["--bolt-port", "7689", "--log-level=TRACE"],
            "log_file": "async_replica2.log",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
            "log_file": "main.log",
            "setup_queries": [
                "REGISTER REPLICA async_replica1 ASYNC TO '127.0.0.1:10001';",
                "REGISTER REPLICA async_replica2 ASYNC TO '127.0.0.1:10002';",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(CONFIGURATION)

    # 1/
    expected_data = [
        (
            "async_replica1",
            "127.0.0.1:10001",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "async_replica2",
            "127.0.0.1:10002",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    actual_data = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICAS;")
    assert all([x in actual_data for x in expected_data])

    # 2/
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("CREATE (p:Number {name:1});")

    QUERY_TO_CHECK = "MATCH (node) return node;"
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 1
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["async_replica1"].query(QUERY_TO_CHECK)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["async_replica2"].query(QUERY_TO_CHECK)

    # 3/
    interactive_mg_runner.kill(CONFIGURATION, "async_replica1")

    # 4/
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("CREATE (p:Number {name:2});")

    # 5/
    expected_data = [
        ("async_replica1", "async", 0, "invalid"),
        ("async_replica2", "async", 0, "ready"),
    ]

    def retrieve_data():
        replicas = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICAS;")
        return [
            (replica_name, mode, info["memgraph"]["behind"], info["memgraph"]["status"])
            for replica_name, ip, mode, sys_info, info in replicas
        ]

    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])

    # 6/
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 2
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["async_replica2"].query(QUERY_TO_CHECK)


def test_attempt_to_write_data_on_main_when_sync_replica_is_down(connection):
    # Goal of this test is to check that main cannot write new data if a sync replica is down.
    # 0/ Start main and sync replicas.
    # 1/ Check status of replicas.
    # 2/ Add some nodes to main and check it is propagated to the sync_replicas.
    # 3/ Kill a sync replica.
    # 4/ Add some data to main. It should be added to main and replica2
    # 5/ Check the status of replicas.
    # 6/ Restart the replica that was killed and check that it is up to date with main.

    data_directory = tempfile.TemporaryDirectory()
    CONFIGURATION = {
        "sync_replica1": {
            "args": [
                "--bolt-port",
                "7688",
                "--log-level",
                "TRACE",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                "false",
            ],
            "log_file": "sync_replica1.log",
            # We restart this replica so we want to set role manually
            "setup_queries": [],
            "data_directory": f"{data_directory.name}/sync_replica1",
        },
        "sync_replica2": {
            "args": ["--bolt-port", "7689", "--log-level", "TRACE"],
            "log_file": "sync_replica2.log",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE", "--data-recovery-on-startup", "true"],
            "log_file": "main.log",
            # need to do it manually
            "setup_queries": [],
            "data_directory": f"{data_directory.name}/main",
        },
    }

    # 0/
    interactive_mg_runner.start_all(CONFIGURATION)

    sync_replica1_cursor = connection(7688, "sync_replica1_cursor").cursor()
    execute_and_fetch_all(sync_replica1_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")

    main_cursor = connection(7687, "main").cursor()
    execute_and_fetch_all(main_cursor, "REGISTER REPLICA sync_replica1 SYNC TO '127.0.0.1:10001';")
    execute_and_fetch_all(main_cursor, "REGISTER REPLICA sync_replica2 SYNC TO '127.0.0.1:10002';")

    # 1/
    expected_data = [
        (
            "sync_replica1",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "sync_replica2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    actual_data = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICAS;")
    assert all([x in actual_data for x in expected_data])

    # 2/
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("CREATE (p:Number {name:1});")

    QUERY_TO_CHECK = "MATCH (node) return node;"
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 1
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica1"].query(QUERY_TO_CHECK)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)

    # 3/
    interactive_mg_runner.kill(CONFIGURATION, "sync_replica1")
    expected_data = [
        ("sync_replica1", "sync", 0, "invalid"),
        ("sync_replica2", "sync", 0, "ready"),
    ]

    def retrieve_data():
        replicas = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICAS;")
        return [
            (replica_name, mode, info["memgraph"]["behind"], info["memgraph"]["status"])
            for replica_name, ip, mode, sys_info, info in replicas
        ]

    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])

    # 4/
    with pytest.raises(mgclient.DatabaseError):
        interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("CREATE (p:Number {name:2});")

    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 2
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)

    # 5/
    expected_data = [
        (
            "sync_replica1",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "invalid"}},
        ),
        (
            "sync_replica2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 5, "behind": 0, "status": "ready"}},
        ),
    ]
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    actual_data = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICAS;")
    assert all([x in actual_data for x in expected_data])

    # 6/
    interactive_mg_runner.start(CONFIGURATION, "sync_replica1")
    expected_data = [
        ("sync_replica1", "sync", 0, "ready"),
        ("sync_replica2", "sync", 0, "ready"),
    ]
    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 2
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica1"].query(QUERY_TO_CHECK)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)


def test_attempt_to_create_indexes_on_main_when_async_replica_is_down():
    # Goal of this test is to check that main can create new indexes/constraints if an async replica is down.
    # 0/ Start main and async replicas.
    # 1/ Check status of replicas.
    # 2/ Add some indexes to main and check it is propagated to the async_replicas.
    # 3/ Kill an async replica.
    # 4/ Try to add some more indexes to main.
    # 5/ Check the status of replicas.
    # 6/ Check that the indexes were added to main and remaining replica.

    CONFIGURATION = {
        "async_replica1": {
            "args": ["--bolt-port", "7688", "--log-level=TRACE"],
            "log_file": "async_replica1.log",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10001;"],
        },
        "async_replica2": {
            "args": ["--bolt-port", "7689", "--log-level=TRACE"],
            "log_file": "async_replica2.log",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
            "log_file": "main.log",
            "setup_queries": [
                "REGISTER REPLICA async_replica1 ASYNC TO '127.0.0.1:10001';",
                "REGISTER REPLICA async_replica2 ASYNC TO '127.0.0.1:10002';",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(CONFIGURATION)

    # 1/
    expected_data = [
        (
            "async_replica1",
            "127.0.0.1:10001",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "async_replica2",
            "127.0.0.1:10002",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    actual_data = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICAS;")
    assert all([x in actual_data for x in expected_data])

    # 2/
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("CREATE INDEX ON :Number(value);")

    QUERY_TO_CHECK = "SHOW INDEX INFO;"
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 1
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["async_replica1"].query(QUERY_TO_CHECK)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["async_replica2"].query(QUERY_TO_CHECK)

    # 3/
    interactive_mg_runner.kill(CONFIGURATION, "async_replica1")

    # 4/
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("CREATE INDEX ON :Number(value2);")

    # 5/
    expected_data = [
        ("async_replica1", "async", 0, "invalid"),
        ("async_replica2", "async", 0, "ready"),
    ]

    def retrieve_data():
        replicas = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICAS;")
        return [
            (replica_name, mode, info["memgraph"]["behind"], info["memgraph"]["status"])
            for replica_name, ip, mode, sys_info, info in replicas
        ]

    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])

    # 6/
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 2
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["async_replica2"].query(QUERY_TO_CHECK)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
