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


@pytest.mark.parametrize(
    "recover_data_on_startup",
    [
        "true",
        "false",
    ],
)
def test_basic_recovery(recover_data_on_startup, connection):
    # Goal of this test is to check the recovery of main.
    # 0/ We start all replicas manually: we want to be able to kill them ourselves without relying on external tooling to kill processes.
    # 1/ We check that all replicas have the correct state: they should all be ready.
    # 2/ We kill main.
    # 3/ We re-start main.
    # 4/ We check that all replicas have the correct state: they should all be ready.
    # 5/ Drop one replica.
    # 6/ We add some data to main, then kill it and restart.
    # 7/ We check that all replicas but one have the expected data.
    # 8/ We kill another replica.
    # 9/ We add some data to main.
    # 10/ We re-add the two replicas dropped/killed and check the data.
    # 11/ We kill another replica.
    # 12/ Add some more data to main. It must still occur but exception is expected since one replica is down.
    # 13/ Restart the replica
    # 14/ Check the states of replicas.
    # 15/ Add some data again.
    # 16/ Check the data is added to all replicas.

    # 0/
    data_directory = tempfile.TemporaryDirectory()
    CONFIGURATION = {
        "replica_1": {
            "args": [
                "--bolt-port",
                "7688",
                "--log-level=TRACE",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                "false",
            ],
            "log_file": "replica1.log",
            # Need to set it up manually
            "setup_queries": [],
            "data_directory": f"{data_directory.name}/replica_1",
        },
        "replica_2": {
            "args": [
                "--bolt-port",
                "7689",
                "--log-level=TRACE",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                "false",
            ],
            "log_file": "replica2.log",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
            "data_directory": f"{data_directory.name}/replica_2",
        },
        "replica_3": {
            "args": [
                "--bolt-port",
                "7690",
                "--log-level=TRACE",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                f"{recover_data_on_startup}",
            ],
            "log_file": "replica3.log",
            # We restart this replica so we set replication role manually,
            # On restart we would set replication role again, we want to get it from data
            "setup_queries": [],
            "data_directory": f"{data_directory.name}/replica_3",
        },
        "replica_4": {
            "args": [
                "--bolt-port",
                "7691",
                "--log-level=TRACE",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                "false",
            ],
            "log_file": "replica4.log",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10004;"],
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
            "data_directory": f"{data_directory.name}/main",
        },
    }

    interactive_mg_runner.start_all(CONFIGURATION)

    replica_1_cursor = connection(7688, "replica_1").cursor()
    execute_and_fetch_all(replica_1_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")

    replica_3_cursor = connection(7690, "replica_3").cursor()
    execute_and_fetch_all(replica_3_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10003;")

    cursor = connection(7687, "main").cursor()

    # We want to execute manually and not via the configuration, otherwise re-starting main would also execute these registration.
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_2 SYNC TO '127.0.0.1:10002';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_3 ASYNC TO '127.0.0.1:10003';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_4 ASYNC TO '127.0.0.1:10004';")

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
        (
            "replica_3",
            "127.0.0.1:10003",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_4",
            "127.0.0.1:10004",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    actual_data = execute_and_fetch_all(cursor, "SHOW REPLICAS;")

    assert all([x in actual_data for x in expected_data])

    def check_roles():
        assert "main" == interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICATION ROLE;")[0][0]
        for index in range(1, 4):
            assert (
                "replica"
                == interactive_mg_runner.MEMGRAPH_INSTANCES[f"replica_{index}"].query("SHOW REPLICATION ROLE;")[0][0]
            )

    check_roles()

    # 2/
    interactive_mg_runner.kill(CONFIGURATION, "main")

    # 3/
    interactive_mg_runner.start(CONFIGURATION, "main")
    cursor = connection(7687, "main").cursor()
    check_roles()

    # 4/
    def retrieve_data():
        return execute_and_fetch_all(cursor, "SHOW REPLICAS;")

    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])

    # 5/
    execute_and_fetch_all(cursor, "DROP REPLICA replica_2;")

    # 6/
    execute_and_fetch_all(cursor, "CREATE (p1:Number {name:'Magic', value:42})")
    interactive_mg_runner.kill(CONFIGURATION, "main")
    interactive_mg_runner.start(CONFIGURATION, "main")
    cursor = connection(7687, "main").cursor()
    check_roles()

    # 7/
    QUERY_TO_CHECK = "MATCH (node) return node;"
    res_from_main = execute_and_fetch_all(cursor, QUERY_TO_CHECK)
    assert len(res_from_main) == 1
    for index in (1, 3, 4):
        assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES[f"replica_{index}"].query(QUERY_TO_CHECK)

    expected_data = [
        (
            "replica_1",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 2, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_3",
            "127.0.0.1:10003",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 2, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_4",
            "127.0.0.1:10004",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 2, "behind": 0, "status": "ready"}},
        ),
    ]
    actual_data = execute_and_fetch_all(cursor, "SHOW REPLICAS;")
    assert all([x in actual_data for x in expected_data])

    # Replica_2 was dropped, we check it does not have the data from main.
    assert len(interactive_mg_runner.MEMGRAPH_INSTANCES["replica_2"].query(QUERY_TO_CHECK)) == 0

    # 8/
    interactive_mg_runner.kill(CONFIGURATION, "replica_3")

    # 9/
    execute_and_fetch_all(cursor, "CREATE (p1:Number {name:'Magic_again', value:43})")
    res_from_main = execute_and_fetch_all(cursor, QUERY_TO_CHECK)
    assert len(res_from_main) == 2

    # 10/
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_2 SYNC TO '127.0.0.1:10002';")
    interactive_mg_runner.start(CONFIGURATION, "replica_3")

    expected_data = [
        (
            "replica_1",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 6, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 6, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_3",
            "127.0.0.1:10003",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 6, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_4",
            "127.0.0.1:10004",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 6, "behind": 0, "status": "ready"}},
        ),
    ]

    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)

    assert all([x in actual_data for x in expected_data])
    for index in (1, 2, 3, 4):
        assert interactive_mg_runner.MEMGRAPH_INSTANCES[f"replica_{index}"].query(QUERY_TO_CHECK) == res_from_main

    # 11/
    interactive_mg_runner.kill(CONFIGURATION, "replica_1")
    expected_data = [
        (
            "replica_1",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "invalid"}},
        ),
        (
            "replica_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 6, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_3",
            "127.0.0.1:10003",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 6, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_4",
            "127.0.0.1:10004",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 6, "behind": 0, "status": "ready"}},
        ),
    ]

    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])

    # 12/
    with pytest.raises(mgclient.DatabaseError):
        interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(
            "CREATE (p1:Number {name:'Magic_again_again', value:44})"
        )
    expected_data = [
        (
            "replica_1",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "invalid"}},
        ),
        (
            "replica_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 9, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_3",
            "127.0.0.1:10003",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 9, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_4",
            "127.0.0.1:10004",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 9, "behind": 0, "status": "ready"}},
        ),
    ]

    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])

    # 13/
    interactive_mg_runner.start(CONFIGURATION, "replica_1")

    # 14/
    expected_data = [
        (
            "replica_1",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 9, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 9, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_3",
            "127.0.0.1:10003",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 9, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_4",
            "127.0.0.1:10004",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 9, "behind": 0, "status": "ready"}},
        ),
    ]
    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    print("actual=", actual_data)
    assert all([x in actual_data for x in expected_data])

    res_from_main = execute_and_fetch_all(cursor, QUERY_TO_CHECK)
    assert len(res_from_main) == 3
    for index in (1, 2, 3, 4):
        assert interactive_mg_runner.MEMGRAPH_INSTANCES[f"replica_{index}"].query(QUERY_TO_CHECK) == res_from_main

    # 15/
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(
        "CREATE (p1:Number {name:'Magic_again_again_again', value:45})"
    )

    # 16/
    expected_data = [
        (
            "replica_1",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 12, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 12, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_3",
            "127.0.0.1:10003",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 12, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_4",
            "127.0.0.1:10004",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 12, "behind": 0, "status": "ready"}},
        ),
    ]
    actual_data = execute_and_fetch_all(cursor, "SHOW REPLICAS;")
    assert all([x in actual_data for x in expected_data])

    res_from_main = execute_and_fetch_all(cursor, QUERY_TO_CHECK)
    assert len(res_from_main) == 4
    for index in (1, 2, 3, 4):
        assert interactive_mg_runner.MEMGRAPH_INSTANCES[f"replica_{index}"].query(QUERY_TO_CHECK) == res_from_main


def test_replication_role_recovery(connection):
    # Goal of this test is to check the recovery of main and replica role.
    # 0/ We start all replicas manually: we want to be able to kill them ourselves without relying on external tooling to kill processes.
    # 1/ We check that all replicas have the correct state: they should all be ready.
    # 2/ We kill main.
    # 3/ We re-start main. We check that main indeed has the role main and replicas still have the correct state.
    # 4/ We kill the replica.
    # 5/ We observed that the replica result is in invalid state.
    # 6/ We start the replica again. We observe that indeed the replica has the replica state.
    # 7/ We observe that main has the replica ready.
    # 8/ We kill the replica again.
    # 9/ We add data to main.
    # 10/ We start the replica again. We observe that the replica has the same
    #     data as main because it synced and added lost data.

    # 0/
    data_directory = tempfile.TemporaryDirectory()
    CONFIGURATION = {
        "replica": {
            "args": [
                "--bolt-port",
                "7688",
                "--log-level=TRACE",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                "false",
            ],
            "log_file": "replica.log",
            "data_directory": f"{data_directory.name}/replica",
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
            "data_directory": f"{data_directory.name}/main",
        },
    }

    interactive_mg_runner.start_all(CONFIGURATION)

    replica_cursor = connection(7688, "replica").cursor()
    execute_and_fetch_all(replica_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")

    cursor = connection(7687, "main").cursor()

    # We want to execute manually and not via the configuration, otherwise re-starting main would also execute these registration.
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica SYNC TO '127.0.0.1:10001';")

    # 1/
    expected_data = [
        (
            "replica",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    actual_data = execute_and_fetch_all(cursor, "SHOW REPLICAS;")

    assert all([x in actual_data for x in expected_data])

    def check_roles():
        assert "main" == interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICATION ROLE;")[0][0]
        assert "replica" == interactive_mg_runner.MEMGRAPH_INSTANCES["replica"].query("SHOW REPLICATION ROLE;")[0][0]

    check_roles()

    # 2/
    interactive_mg_runner.kill(CONFIGURATION, "main")

    # 3/
    interactive_mg_runner.start(CONFIGURATION, "main")
    cursor = connection(7687, "main").cursor()
    check_roles()

    def retrieve_data():
        return execute_and_fetch_all(cursor, "SHOW REPLICAS;")

    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])

    # 4/
    interactive_mg_runner.kill(CONFIGURATION, "replica")

    # 5/
    expected_data = [
        (
            "replica",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "invalid"}},
        ),
    ]
    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)

    assert all([x in actual_data for x in expected_data])

    # 6/
    interactive_mg_runner.start(CONFIGURATION, "replica")
    check_roles()

    # 7/
    expected_data = [
        (
            "replica",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]

    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])

    # 8/
    interactive_mg_runner.kill(CONFIGURATION, "replica")

    # 9/
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(cursor, "CREATE (n:First)")

    # 10/
    interactive_mg_runner.start(CONFIGURATION, "replica")
    check_roles()

    expected_data = [
        (
            "replica",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 2, "behind": 0, "status": "ready"}},
        ),
    ]
    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])

    QUERY_TO_CHECK = "MATCH (node) return node;"
    res_from_main = execute_and_fetch_all(cursor, QUERY_TO_CHECK)
    assert len(res_from_main) == 1
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["replica"].query(QUERY_TO_CHECK)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
