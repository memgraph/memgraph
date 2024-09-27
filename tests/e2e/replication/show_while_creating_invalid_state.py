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
import random
import sys

import interactive_mg_runner
import mgclient
import pytest
from common import execute_and_fetch_all, get_data_path, get_logs_path
from mg_utils import mg_sleep_and_assert, mg_sleep_and_assert_collection

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))
file = "show_while_creating_invalid_state"


@pytest.fixture(autouse=True)
def cleanup_after_test():
    # Run the test
    yield
    # Stop + delete directories after running the test
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


def get_instances_description(test_name: str):
    return {
        "replica_1": {
            "args": ["--bolt-port", "7688", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(file, test_name)}/replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica1",
            "setup_queries": [],
        },
        "replica_2": {
            "args": ["--bolt-port", "7689", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(file, test_name)}/replica2.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica2",
            "setup_queries": [],
        },
        "replica_3": {
            "args": ["--bolt-port", "7690", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(file, test_name)}/replica3.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica3",
            "setup_queries": [],
        },
        "replica_4": {
            "args": ["--bolt-port", "7691", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(file, test_name)}/replica4.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica4",
            "setup_queries": [],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [],
        },
    }


def test_show_replicas(connection, test_name):
    # Goal of this test is to check the SHOW REPLICAS command.
    # 0/ We start all replicas manually: we want to be able to kill them ourselves without relying on external tooling to kill processes.
    # 1/ We check that all replicas have the correct state: they should all be ready.
    # 2/ We drop one replica. It should not appear anymore in the SHOW REPLICAS command.
    # 3/ We kill another replica. It should become invalid in the SHOW REPLICAS command.

    instances = get_instances_description(test_name)

    # 0/
    interactive_mg_runner.start_all(instances, keep_directories=False)

    replica1_cursor = connection(7688, "replica1").cursor()
    execute_and_fetch_all(replica1_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")

    replica2_cursor = connection(7689, "replica2").cursor()
    execute_and_fetch_all(replica2_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10002;")

    replica3_cursor = connection(7690, "replica3").cursor()
    execute_and_fetch_all(replica3_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10003;")

    replica4_cursor = connection(7691, "replica4").cursor()
    execute_and_fetch_all(replica4_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10004;")

    cursor = connection(7687, "main").cursor()
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_2 SYNC TO '127.0.0.1:10002';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_3 ASYNC TO '127.0.0.1:10003';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_4 ASYNC TO '127.0.0.1:10004';")

    # 1/
    actual_data = execute_and_fetch_all(cursor, "SHOW REPLICAS;")
    EXPECTED_COLUMN_NAMES = {
        "name",
        "socket_address",
        "sync_mode",
        "system_info",
        "data_info",
    }

    actual_column_names = {x.name for x in cursor.description}
    assert actual_column_names == EXPECTED_COLUMN_NAMES

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
    assert all([x in actual_data for x in expected_data])

    # 2/
    execute_and_fetch_all(cursor, "DROP REPLICA replica_2")
    actual_data = execute_and_fetch_all(cursor, "SHOW REPLICAS;")
    expected_data = [
        (
            "replica_1",
            "127.0.0.1:10001",
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
    assert all([x in actual_data for x in expected_data])

    # 3/
    interactive_mg_runner.kill(instances, "replica_1")
    interactive_mg_runner.kill(instances, "replica_3")
    interactive_mg_runner.stop(instances, "replica_4")

    # We leave some time for the main to realise the replicas are down.
    def retrieve_data():
        return execute_and_fetch_all(cursor, "SHOW REPLICAS;")

    expected_data = [
        (
            "replica_1",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "invalid"}},
        ),
        (
            "replica_3",
            "127.0.0.1:10003",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "invalid"}},
        ),
        (
            "replica_4",
            "127.0.0.1:10004",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "invalid"}},
        ),
    ]
    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])


def test_drop_replicas(connection, test_name):
    # Goal of this test is to check the DROP REPLICAS command.
    # 0/ Manually start main and all replicas
    # 1/ Check status of the replicas
    # 2/ Kill replica 3
    # 3/ Drop replica 3 and check status
    # 4/ Stop replica 4
    # 5/ Drop replica 4 and check status
    # 6/ Kill replica 1
    # 7/ Drop replica 1 and check status
    # 8/ Stop replica 2
    # 9/ Drop replica 2 and check status
    # 10/ Restart all replicas
    # 11/ Register them
    # 12/ Drop all and check status

    def retrieve_data():
        return execute_and_fetch_all(cursor, "SHOW REPLICAS;")

    instances = get_instances_description(test_name)

    # 0/
    interactive_mg_runner.start_all(instances, keep_directories=False)

    # Don't use setup queries to avoid errors with data directory.
    replica1_cursor = connection(7688, "replica1").cursor()
    execute_and_fetch_all(replica1_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")

    replica2_cursor = connection(7689, "replica2").cursor()
    execute_and_fetch_all(replica2_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10002;")

    replica3_cursor = connection(7690, "replica3").cursor()
    execute_and_fetch_all(replica3_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10003;")

    replica4_cursor = connection(7691, "replica4").cursor()
    execute_and_fetch_all(replica4_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10004;")

    cursor = connection(7687, "main").cursor()
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_2 SYNC TO '127.0.0.1:10002';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_3 ASYNC TO '127.0.0.1:10003';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_4 ASYNC TO '127.0.0.1:10004';")

    # 1/
    _ = execute_and_fetch_all(cursor, "SHOW REPLICAS;")
    EXPECTED_COLUMN_NAMES = {
        "name",
        "socket_address",
        "sync_mode",
        "system_info",
        "data_info",
    }

    actual_column_names = {x.name for x in cursor.description}
    assert actual_column_names == EXPECTED_COLUMN_NAMES

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
    mg_sleep_and_assert_collection(expected_data, retrieve_data)

    # 2/
    interactive_mg_runner.kill(instances, "replica_3", keep_directories=True)
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
            {"memgraph": {"ts": 0, "behind": 0, "status": "invalid"}},
        ),
        (
            "replica_4",
            "127.0.0.1:10004",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, retrieve_data)

    # 3/
    execute_and_fetch_all(cursor, "DROP REPLICA replica_3")
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
            "replica_4",
            "127.0.0.1:10004",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, retrieve_data)

    # 4/
    interactive_mg_runner.kill(instances, "replica_4", keep_directories=True)
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
            "replica_4",
            "127.0.0.1:10004",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "invalid"}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, retrieve_data)

    # 5/
    execute_and_fetch_all(cursor, "DROP REPLICA replica_4")
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
    mg_sleep_and_assert_collection(expected_data, retrieve_data)

    # 6/
    interactive_mg_runner.kill(instances, "replica_1", keep_directories=True)
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
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, retrieve_data)

    # 7/
    execute_and_fetch_all(cursor, "DROP REPLICA replica_1")
    expected_data = [
        (
            "replica_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, retrieve_data)

    # 8/
    interactive_mg_runner.kill(instances, "replica_2", keep_directories=True)
    expected_data = [
        (
            "replica_2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "invalid"}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, retrieve_data)

    # 9/
    execute_and_fetch_all(cursor, "DROP REPLICA replica_2")
    expected_data = set()
    mg_sleep_and_assert_collection(expected_data, retrieve_data)

    # 10/
    interactive_mg_runner.start(instances, "replica_1")
    interactive_mg_runner.start(instances, "replica_2")
    interactive_mg_runner.start(instances, "replica_3")
    interactive_mg_runner.start(instances, "replica_4")
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_2 SYNC TO '127.0.0.1:10002';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_3 ASYNC TO '127.0.0.1:10003';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_4 ASYNC TO '127.0.0.1:10004';")

    # 11/
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
    mg_sleep_and_assert_collection(expected_data, retrieve_data)

    # 12/
    execute_and_fetch_all(cursor, "DROP REPLICA replica_1")
    execute_and_fetch_all(cursor, "DROP REPLICA replica_2")
    execute_and_fetch_all(cursor, "DROP REPLICA replica_3")
    execute_and_fetch_all(cursor, "DROP REPLICA replica_4")
    expected_data = set()
    mg_sleep_and_assert_collection(expected_data, retrieve_data)


@pytest.mark.parametrize(
    "recover_data_on_startup",
    [
        "true",
        "false",
    ],
)
def test_basic_recovery(recover_data_on_startup, connection, test_name):
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
            "log_file": f"{get_logs_path(file, test_name)}/replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica1",
            "setup_queries": [],
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
            "log_file": f"{get_logs_path(file, test_name)}/replica2.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica2",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
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
            "log_file": f"{get_logs_path(file, test_name)}/replica3.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica3",
            # We restart this replica so we set replication role manually,
            # On restart we would set replication role again, we want to get it from data
            "setup_queries": [],
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
            "log_file": f"{get_logs_path(file, test_name)}/replica4.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica4",
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
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [],
        },
    }

    interactive_mg_runner.start_all(CONFIGURATION, keep_directories=False)

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


def test_replication_role_recovery(connection, test_name):
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
            "log_file": f"{get_logs_path(file, test_name)}/replica.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica",
        },
        "main": {
            "args": [
                "--bolt-port",
                "7687",
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--replication-restore-state-on-startup=true",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [],
        },
    }

    interactive_mg_runner.start_all(CONFIGURATION, keep_directories=False)

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


def test_conflict_at_startup(connection, test_name):
    # Goal of this test is to check starting up several instance with different replicas' configuration directory works as expected.
    # main_1 and main_2 have different directory.

    CONFIGURATION = {
        "main_1": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE"],
            "setup_queries": [],
            "log_file": f"{get_logs_path(file, test_name)}/main1.log",
            "data_directory": f"{get_data_path(file, test_name)}/main1",
        },
        "main_2": {
            "args": ["--bolt-port", "7688", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(file, test_name)}/main2.log",
            "data_directory": f"{get_data_path(file, test_name)}/main2",
            "setup_queries": [],
        },
    }

    interactive_mg_runner.start_all(CONFIGURATION, keep_directories=False)
    cursor_1 = connection(7687, "main_1").cursor()
    cursor_2 = connection(7688, "main_2").cursor()

    assert execute_and_fetch_all(cursor_1, "SHOW REPLICATION ROLE;")[0][0] == "main"
    assert execute_and_fetch_all(cursor_2, "SHOW REPLICATION ROLE;")[0][0] == "main"


def test_basic_recovery_when_replica_is_kill_when_main_is_down(test_name):
    # Goal of this test is to check the recovery of main.
    # 0/ We start all replicas manually: we want to be able to kill them ourselves without relying on external tooling to kill processes.
    # 1/ We check that all replicas have the correct state: they should all be ready.
    # 2/ We kill main then kill a replica.
    # 3/ We re-start main: it should be able to restart.
    # 4/ Check status of replica: replica_2 is invalid.

    CONFIGURATION = {
        "replica_1": {
            "args": ["--bolt-port", "7688", "--log-level=TRACE", "--replication-restore-state-on-startup=true"],
            "log_file": f"{get_logs_path(file, test_name)}/replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica1",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10001;"],
        },
        "replica_2": {
            "args": ["--bolt-port", "7689", "--log-level=TRACE", "--replication-restore-state-on-startup=true"],
            "log_file": f"{get_logs_path(file, test_name)}/replica2.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica2",
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
            "setup_queries": [],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
        },
    }

    interactive_mg_runner.start_all(CONFIGURATION, keep_directories=False)

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


def test_async_replication_when_main_is_killed(test_name):
    # Goal of the test is to check that when main is randomly killed:
    # -the ASYNC replica always contains a valid subset of data of main.
    # We run the test 20 times, it should never fail.

    # 0/ Start main and replicas.
    # 1/ Register replicas.
    # 2/ Insert data in main, and randomly kill it.
    # 3/ Check that the ASYNC replica has a valid subset.

    for test_repetition in range(20):
        # 0/
        CONFIGURATION = {
            "async_replica": {
                "args": ["--bolt-port", "7688", "--log-level=TRACE"],
                "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10001;"],
                "log_file": f"{get_logs_path(file, test_name)}/async_replica.log",
            },
            "main": {
                "args": ["--bolt-port", "7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
                "setup_queries": [],
                "log_file": f"{get_logs_path(file, test_name)}/main.log",
            },
        }
        interactive_mg_runner.kill_all(keep_directories=False)
        interactive_mg_runner.start_all(CONFIGURATION, keep_directories=False)

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


def test_sync_replication_when_main_is_killed(test_name):
    # Goal of the test is to check that when main is randomly killed:
    # -the SYNC replica always contains the exact data that was in main.
    # We run the test 20 times, it should never fail.

    # 0/ Start main and replica.
    # 1/ Register replica.
    # 2/ Insert data in main, and randomly kill it.
    # 3/ Check that the SYNC replica has exactly the same data than main.

    for _ in range(20):
        # 0/
        CONFIGURATION = {
            "sync_replica": {
                "args": ["--bolt-port", "7688", "--log-level=TRACE"],
                "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10001;"],
                "log_file": f"{get_logs_path(file, test_name)}/sync_replica.log",
            },
            "main": {
                "args": ["--bolt-port", "7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
                "setup_queries": [],
                "log_file": f"{get_logs_path(file, test_name)}/main.log",
            },
        }
        interactive_mg_runner.kill_all(keep_directories=False)
        interactive_mg_runner.start_all(CONFIGURATION, keep_directories=False)

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
    interactive_mg_runner.start_all(CONFIGURATION, keep_directories=False)

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


def test_attempt_to_write_data_on_main_when_sync_replica_is_down(connection, test_name):
    # Goal of this test is to check that main cannot write new data if a sync replica is down.
    # 0/ Start main and sync replicas.
    # 1/ Check status of replicas.
    # 2/ Add some nodes to main and check it is propagated to the sync_replicas.
    # 3/ Kill a sync replica.
    # 4/ Add some data to main. It should be added to main and replica2
    # 5/ Check the status of replicas.
    # 6/ Restart the replica that was killed and check that it is up to date with main.

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
            # We restart this replica so we want to set role manually
            "setup_queries": [],
            "log_file": f"{get_logs_path(file, test_name)}/sync_replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/sync_replica1",
        },
        "sync_replica2": {
            "args": ["--bolt-port", "7689", "--log-level", "TRACE"],
            "setup_queries": [],
            "log_file": f"{get_logs_path(file, test_name)}/sync_replica2.log",
            "data_directory": f"{get_data_path(file, test_name)}/sync_replica2",
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE", "--data-recovery-on-startup", "true"],
            # need to do it manually
            "setup_queries": [],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
        },
    }

    # 0/
    interactive_mg_runner.start_all(CONFIGURATION, keep_directories=False)

    sync_replica1_cursor = connection(7688, "sync_replica1_cursor").cursor()
    execute_and_fetch_all(sync_replica1_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")

    sync_replica2_cursor = connection(7689, "sync_replica2_cursor").cursor()
    execute_and_fetch_all(sync_replica2_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10002;")

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


def test_attempt_to_create_indexes_on_main_when_async_replica_is_down(connection, test_name):
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
            "log_file": f"{get_logs_path(file, test_name)}/async_replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/async_replica1",
            "setup_queries": [],
        },
        "async_replica2": {
            "args": ["--bolt-port", "7689", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(file, test_name)}/async_replica2.log",
            "data_directory": f"{get_data_path(file, test_name)}/async_replica2",
            "setup_queries": [],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [],
        },
    }

    # 0/
    interactive_mg_runner.start_all(CONFIGURATION, keep_directories=False)

    replica1_cursor = connection(7688, "replica_1").cursor()
    execute_and_fetch_all(replica1_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")

    replica2_cursor = connection(7689, "replica_2").cursor()
    execute_and_fetch_all(replica2_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10002;")

    main_cursor = connection(7687, "main").cursor()
    execute_and_fetch_all(main_cursor, "REGISTER REPLICA async_replica1 ASYNC TO '127.0.0.1:10001';")
    execute_and_fetch_all(main_cursor, "REGISTER REPLICA async_replica2 ASYNC TO '127.0.0.1:10002';")

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


def test_attempt_to_create_indexes_on_main_when_sync_replica_is_down(connection, test_name):
    # Goal of this test is to check creation of new indexes/constraints when a sync replica is down.
    # 0/ Start main and sync replicas.
    # 1/ Check status of replicas.
    # 2/ Add some indexes to main and check it is propagated to the sync_replicas.
    # 3/ Kill a sync replica.
    # 4/ Add some more indexes to main. It should be added to main and replica2
    # 5/ Check the status of replicas.
    # 6/ Restart the replica that was killed and check that it is up to date with main.

    CONFIGURATION = {
        "sync_replica1": {
            "args": [
                "--bolt-port",
                "7688",
                "--log-level=TRACE",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                "false",
            ],
            "setup_queries": [],
            "log_file": f"{get_logs_path(file, test_name)}/sync_replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/sync_replica1",
        },
        "sync_replica2": {
            "args": ["--bolt-port", "7689", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(file, test_name)}/sync_replica2.log",
            "data_directory": f"{get_data_path(file, test_name)}/sync_replica2",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            # Need to do it manually
            "setup_queries": [],
        },
    }

    # 0/
    interactive_mg_runner.start_all(CONFIGURATION, keep_directories=False)

    sync_replica1_cursor = connection(7688, "sync_replica1").cursor()
    execute_and_fetch_all(sync_replica1_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")

    cursor = connection(7687, "main").cursor()

    # We want to execute manually and not via the configuration, as we are setting replica manually because
    # of restart. Restart on replica would set role again.
    execute_and_fetch_all(cursor, "REGISTER REPLICA sync_replica1 SYNC TO '127.0.0.1:10001';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA sync_replica2 SYNC TO '127.0.0.1:10002';")

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
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("CREATE INDEX ON :Number(value);")

    QUERY_TO_CHECK = "SHOW INDEX INFO;"
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
        interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("CREATE INDEX ON :Number(value2);")

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
            {"memgraph": {"ts": 6, "behind": 0, "status": "ready"}},
        ),
    ]
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)
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


def test_trigger_on_create_before_commit_with_offline_sync_replica(connection, test_name):
    # 0/ Start all.
    # 1/ Create the trigger
    # 2/ Create a node. We expect two nodes created (our Not_Magic and the Magic created by trigger).
    # 3/ Check the nodes
    # 4/ We remove all nodes.
    # 5/ Kill a replica and check that it's offline.
    # 6/ Create new node.
    # 7/ Check that we have two nodes.
    # 8/ Re-start the replica and check it's online and that it has two nodes.

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
            # Need to do it manually since we kill this replica
            "setup_queries": [],
            "log_file": f"{get_logs_path(file, test_name)}/sync_replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/sync_replica1",
        },
        "sync_replica2": {
            "args": ["--bolt-port", "7689", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(file, test_name)}/sync_replica2.log",
            "data_directory": f"{get_data_path(file, test_name)}/sync_replica2",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            # Need to do it manually since we kill replica
            "setup_queries": [],
        },
    }

    # 0/
    interactive_mg_runner.start_all(CONFIGURATION, keep_directories=False)

    sync_replica1_cursor = connection(7688, "sync_replica1").cursor()
    execute_and_fetch_all(sync_replica1_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")

    cursor = connection(7687, "main").cursor()

    # We want to execute manually and not via the configuration, as we are setting replica manually because
    # of restart. Restart on replica would set role again.
    execute_and_fetch_all(cursor, "REGISTER REPLICA sync_replica1 SYNC TO '127.0.0.1:10001';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA sync_replica2 SYNC TO '127.0.0.1:10002';")

    # 1/
    QUERY_CREATE_TRIGGER = """
        CREATE TRIGGER exampleTrigger
        ON CREATE BEFORE COMMIT EXECUTE
        CREATE (p:Number {name:'Node_created_by_trigger'});
    """
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_TRIGGER)
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW TRIGGERS;")
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"

    # 2/
    QUERY_CREATE_NODE = "CREATE (p:Number {name:'Not_Magic'})"
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_NODE)

    # 3/
    QUERY_TO_CHECK = "MATCH (node) return node;"
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 2, f"Incorect result: {res_from_main}"
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica1"].query(QUERY_TO_CHECK)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)

    # 4/
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("MATCH (n) DETACH DELETE n;")

    # 5/
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

    # 6/
    with pytest.raises(mgclient.DatabaseError):
        interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_NODE)

    # 7/
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 2
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)

    # 8/
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


def test_trigger_on_update_before_commit_with_offline_sync_replica(connection, test_name):
    # 0/ Start all.
    # 1/ Create the trigger
    # 2/ Create a node.
    # 3/ Update the node: we expect another node to be created
    # 4/ Check the nodes
    # 5/ We remove all nodes and create new node again.
    # 6/ Kill a replica and check that it's offline.
    # 7/ Update the node.
    # 8/ Check that we have two nodes.
    # 9/ Re-start the replica and check it's online and that it has two nodes.

    CONFIGURATION = {
        "sync_replica1": {
            "args": [
                "--bolt-port",
                "7688",
                "--log-level=TRACE",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                "false",
            ],
            # Need to do it manually
            "setup_queries": [],
            "log_file": f"{get_logs_path(file, test_name)}/sync_replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/sync_replica1",
        },
        "sync_replica2": {
            "args": ["--bolt-port", "7689", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(file, test_name)}/sync_replica2.log",
            "data_directory": f"{get_data_path(file, test_name)}/sync_replica2",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [],
        },
    }

    # 0/
    interactive_mg_runner.start_all(CONFIGURATION, keep_directories=False)

    sync_replica1_cursor = connection(7688, "sync_replica1").cursor()
    execute_and_fetch_all(sync_replica1_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")

    cursor = connection(7687, "main").cursor()

    # We want to execute manually and not via the configuration, as we are setting replica manually because
    # of restart. Restart on replica would set role again.
    execute_and_fetch_all(cursor, "REGISTER REPLICA sync_replica1 SYNC TO '127.0.0.1:10001';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA sync_replica2 SYNC TO '127.0.0.1:10002';")

    # 1/
    QUERY_CREATE_TRIGGER = """
        CREATE TRIGGER exampleTrigger
        ON UPDATE BEFORE COMMIT EXECUTE
        CREATE (p:Number {name:'Node_created_by_trigger'});
    """
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_TRIGGER)
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW TRIGGERS;")
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"

    # 2/
    QUERY_CREATE_NODE = "CREATE (p:Number {name:'Not_Magic', value:0})"
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_NODE)

    # 3/
    QUERY_TO_UPDATE = "MATCH (node:Number {name:'Not_Magic'}) SET node.value=1 return node;"
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_UPDATE)

    # 4/
    QUERY_TO_CHECK = "MATCH (node) return node;"
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 2, f"Incorect result: {res_from_main}"
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica1"].query(QUERY_TO_CHECK)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)

    # 5/
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("MATCH (n) DETACH DELETE n;")
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_NODE)

    # 6/
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

    # 7/
    with pytest.raises(mgclient.DatabaseError):
        interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_UPDATE)

    # 8/
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 2
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)

    # 9/
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


def test_trigger_on_delete_before_commit_with_offline_sync_replica(connection, test_name):
    # 0/ Start all.
    # 1/ Create the trigger
    # 2/ Create a node.
    # 3/ Delete the node: we expect another node to be created
    # 4/ Check that we have one node.
    # 5/ We remove all triggers and all nodes and create new trigger and node again.
    # 6/ Kill a replica and check that it's offline.
    # 7/ Delete the node.
    # 8/ Check that we have one node.
    # 9/ Re-start the replica and check it's online and that it has one node, and the correct one.

    CONFIGURATION = {
        "sync_replica1": {
            "args": [
                "--bolt-port",
                "7688",
                "--log-level=TRACE",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                "false",
            ],
            # we need to set it manually
            "setup_queries": [],
            "log_file": f"{get_logs_path(file, test_name)}/sync_replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/sync_replica1",
        },
        "sync_replica2": {
            "args": ["--bolt-port", "7689", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(file, test_name)}/sync_replica2.log",
            "data_directory": f"{get_data_path(file, test_name)}/sync_replica2",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [],
        },
    }

    # 0/
    interactive_mg_runner.start_all(CONFIGURATION, keep_directories=False)

    sync_replica1_cursor = connection(7688, "sync_replica1").cursor()
    execute_and_fetch_all(sync_replica1_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")

    cursor = connection(7687, "main").cursor()

    # We want to execute manually and not via the configuration, as we are setting replica manually because
    # of restart. Restart on replica would set role again.
    execute_and_fetch_all(cursor, "REGISTER REPLICA sync_replica1 SYNC TO '127.0.0.1:10001';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA sync_replica2 SYNC TO '127.0.0.1:10002';")

    # 1/
    QUERY_CREATE_TRIGGER = """
        CREATE TRIGGER exampleTrigger
        ON DELETE BEFORE COMMIT EXECUTE
        CREATE (p:Number {name:'Node_created_by_trigger'});
    """
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_TRIGGER)
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW TRIGGERS;")
    assert len(res_from_main) == 1, f"Incorrect result: {res_from_main}"

    # 2/
    QUERY_CREATE_NODE = "CREATE (p:Number {name:'Not_Magic', value:0})"
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_NODE)

    # 3/
    QUERY_TO_DELETE = "MATCH (n) DETACH DELETE n;"
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_DELETE)

    # 4/
    QUERY_TO_CHECK = "MATCH (node) return node;"
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 1, f"Incorrect result: {res_from_main}"
    assert res_from_main[0][0].properties["name"] == "Node_created_by_trigger"
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica1"].query(QUERY_TO_CHECK)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)

    # 5/
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("DROP TRIGGER exampleTrigger;")
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("MATCH (n) DETACH DELETE n;")
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_TRIGGER)
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_NODE)

    # 6/
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

    # 7/
    with pytest.raises(mgclient.DatabaseError):
        interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_DELETE)

    # 8/
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"
    assert res_from_main[0][0].properties["name"] == "Node_created_by_trigger"
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)

    # 9/
    interactive_mg_runner.start(CONFIGURATION, "sync_replica1")
    expected_data = [
        ("sync_replica1", "sync", 0, "ready"),
        ("sync_replica2", "sync", 0, "ready"),
    ]
    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 1
    assert res_from_main[0][0].properties["name"] == "Node_created_by_trigger"
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica1"].query(QUERY_TO_CHECK)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)


def test_trigger_on_create_before_and_after_commit_with_offline_sync_replica(connection, test_name):
    # 0/ Start all.
    # 1/ Create the triggers
    # 2/ Create a node. We expect three nodes created (1 node created + the two created by triggers).
    # 3/ Check the nodes
    # 4/ We remove all nodes.
    # 5/ Kill a replica and check that it's offline.
    # 6/ Create new node.
    # 7/ Check that we have three nodes.
    # 8/ Re-start the replica and check it's online and that it has three nodes.

    CONFIGURATION = {
        "sync_replica1": {
            "args": [
                "--bolt-port",
                "7688",
                "--log-level=TRACE",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                "false",
            ],
            # we need to set it manually
            "setup_queries": [],
            "log_file": f"{get_logs_path(file, test_name)}/sync_replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/sync_replica1",
        },
        "sync_replica2": {
            "args": ["--bolt-port", "7689", "--log-level=TRACE"],
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
            "log_file": f"{get_logs_path(file, test_name)}/sync_replica2.log",
            "data_directory": f"{get_data_path(file, test_name)}/sync_replica2",
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
            "setup_queries": [],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
        },
    }

    # 0/
    interactive_mg_runner.start_all(CONFIGURATION, keep_directories=False)

    sync_replica1_cursor = connection(7688, "sync_replica1").cursor()
    sync_replica2_cursor = connection(7689, "sync_replica2").cursor()
    execute_and_fetch_all(sync_replica1_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")

    main_cursor = connection(7687, "main").cursor()

    # We want to execute manually and not via the configuration, as we are setting replica manually because
    # of restart. Restart on replica would set role again.
    execute_and_fetch_all(main_cursor, "REGISTER REPLICA sync_replica1 SYNC TO '127.0.0.1:10001';")
    execute_and_fetch_all(main_cursor, "REGISTER REPLICA sync_replica2 SYNC TO '127.0.0.1:10002';")

    # 1/
    QUERY_CREATE_TRIGGER_BEFORE = """
        CREATE TRIGGER exampleTriggerBefore
        ON CREATE BEFORE COMMIT EXECUTE
        CREATE (p:Number {name:'Node_created_by_trigger_before'});
    """
    QUERY_CREATE_TRIGGER_AFTER = """
        CREATE TRIGGER exampleTriggerAfter
        ON CREATE AFTER COMMIT EXECUTE
        CREATE (p:Number {name:'Node_created_by_trigger_after'});
    """

    execute_and_fetch_all(main_cursor, QUERY_CREATE_TRIGGER_BEFORE)
    execute_and_fetch_all(main_cursor, QUERY_CREATE_TRIGGER_AFTER)
    res_from_main = execute_and_fetch_all(main_cursor, "SHOW TRIGGERS;")
    assert len(res_from_main) == 2, f"Incorect result: {res_from_main}"

    # 2/
    QUERY_CREATE_NODE = "CREATE (p:Number {name:'Not_Magic'})"
    execute_and_fetch_all(main_cursor, QUERY_CREATE_NODE)

    # 3/
    QUERY_TO_CHECK = "MATCH (node) return node;"
    res_from_main = execute_and_fetch_all(main_cursor, QUERY_TO_CHECK)
    assert len(res_from_main) == 3, f"Incorect result: {res_from_main}"

    execute_and_fetch_all(sync_replica1_cursor, QUERY_TO_CHECK)
    execute_and_fetch_all(sync_replica2_cursor, QUERY_TO_CHECK)

    # 4/
    execute_and_fetch_all(main_cursor, "MATCH (n) DETACH DELETE n;")

    # 5/
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

    # 6/
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(main_cursor, QUERY_CREATE_NODE)

    # 7/
    res_from_main = execute_and_fetch_all(main_cursor, QUERY_TO_CHECK)
    assert len(res_from_main) == 3

    assert res_from_main == execute_and_fetch_all(sync_replica2_cursor, QUERY_TO_CHECK)

    # 8/
    interactive_mg_runner.start(CONFIGURATION, "sync_replica1")
    expected_data = [
        ("sync_replica1", "sync", 0, "ready"),
        ("sync_replica2", "sync", 0, "ready"),
    ]
    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])
    res_from_main = execute_and_fetch_all(main_cursor, QUERY_TO_CHECK)
    assert len(res_from_main) == 3
    sync_replica1_cursor = connection(7688, "sync_replica1").cursor()
    execute_and_fetch_all(sync_replica1_cursor, QUERY_TO_CHECK)
    execute_and_fetch_all(sync_replica2_cursor, QUERY_TO_CHECK)


def test_triggers_on_create_before_commit_with_offline_sync_replica(connection, test_name):
    # 0/ Start all.
    # 1/ Create the two triggers
    # 2/ Create a node. We expect three nodes.
    # 3/ Check the nodes
    # 4/ We remove all nodes.
    # 5/ Kill a replica and check that it's offline.
    # 6/ Create new node.
    # 7/ Check that we have three nodes.
    # 8/ Re-start the replica and check it's online and that it has two nodes.

    CONFIGURATION = {
        "sync_replica1": {
            "args": [
                "--bolt-port",
                "7688",
                "--log-level=TRACE",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                "false",
            ],
            # we need to set it manually
            "setup_queries": [],
            "log_file": f"{get_logs_path(file, test_name)}/sync_replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/sync_replica1",
        },
        "sync_replica2": {
            "args": ["--bolt-port", "7689", "--log-level=TRACE"],
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
            "log_file": f"{get_logs_path(file, test_name)}/sync_replica2.log",
            "data_directory": f"{get_data_path(file, test_name)}/sync_replica2",
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
            "setup_queries": [],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
        },
    }

    # 0/
    interactive_mg_runner.start_all(CONFIGURATION, keep_directories=False)

    sync_replica1_cursor = connection(7688, "sync_replica1").cursor()
    execute_and_fetch_all(sync_replica1_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")

    cursor = connection(7687, "main").cursor()

    # We want to execute manually and not via the configuration, as we are setting replica manually because
    # of restart. Restart on replica would set role again.
    execute_and_fetch_all(cursor, "REGISTER REPLICA sync_replica1 SYNC TO '127.0.0.1:10001';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA sync_replica2 SYNC TO '127.0.0.1:10002';")

    # 1/
    QUERY_CREATE_TRIGGER_FIRST = """
        CREATE TRIGGER exampleTriggerFirst
        ON CREATE BEFORE COMMIT EXECUTE
        CREATE (p:Number {name:'Node_created_by_trigger_first'});
    """
    QUERY_CREATE_TRIGGER_SECOND = """
        CREATE TRIGGER exampleTriggerSecond
        ON CREATE BEFORE COMMIT EXECUTE
        CREATE (p:Number {name:'Node_created_by_trigger_second'});
    """
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_TRIGGER_FIRST)
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_TRIGGER_SECOND)
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW TRIGGERS;")
    assert len(res_from_main) == 2, f"Incorect result: {res_from_main}"

    # 2/
    QUERY_CREATE_NODE = "CREATE (p:Number {name:'Not_Magic'})"
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_NODE)

    # 3/
    QUERY_TO_CHECK = "MATCH (node) return node;"
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 3, f"Incorect result: {res_from_main}"
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica1"].query(QUERY_TO_CHECK)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)

    # 4/
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("MATCH (n) DETACH DELETE n;")

    # 5/
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

    # 6/
    with pytest.raises(mgclient.DatabaseError):
        interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_NODE)

    # 7/
    def get_number_of_nodes():
        return len(interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK))

    mg_sleep_and_assert(3, get_number_of_nodes)

    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 3
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)

    # 8/
    interactive_mg_runner.start(CONFIGURATION, "sync_replica1")
    expected_data = [
        ("sync_replica1", "sync", 0, "ready"),
        ("sync_replica2", "sync", 0, "ready"),
    ]
    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 3
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica1"].query(QUERY_TO_CHECK)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)


def test_replication_not_messed_up_by_CreateSnapshot(connection, test_name):
    # Goal of this test is to check the replica can not run CreateSnapshot
    # 1/ CREATE SNAPSHOT should raise a DatabaseError

    MEMGRAPH_INSTANCES_DESCRIPTION = get_instances_description(test_name)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)

    replica1_cursor = connection(7688, "replica1").cursor()
    execute_and_fetch_all(replica1_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")

    replica2_cursor = connection(7689, "replica2").cursor()
    execute_and_fetch_all(replica2_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10002;")

    replica3_cursor = connection(7690, "replica3").cursor()
    execute_and_fetch_all(replica3_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10003;")

    replica4_cursor = connection(7691, "replica4").cursor()
    execute_and_fetch_all(replica4_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10004;")

    cursor = connection(7687, "main").cursor()
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_2 SYNC TO '127.0.0.1:10002';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_3 ASYNC TO '127.0.0.1:10003';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_4 ASYNC TO '127.0.0.1:10004';")

    # 1/
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(replica1_cursor, "CREATE SNAPSHOT;")


def test_replication_not_messed_up_by_ShowIndexInfo(connection):
    # Goal of this test is to check the replicas timestamp and hence ability to recieve MAINs writes
    # is uneffected by SHOW INDEX INFO

    # 1/ Run SHOW INDEX INFO; multiple times on REPLICA
    # 2/ Send a write from MAIN
    # 3/ Check REPLICA processed the write

    BASIC_MEMGRAPH_INSTANCES_DESCRIPTION = {
        "replica_1": {
            "args": ["--bolt-port", "7688", "--log-level=TRACE"],
            "log_file": "replica1.log",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10001;"],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE"],
            "log_file": "main.log",
            "setup_queries": [
                "REGISTER REPLICA replica_1 ASYNC TO '127.0.0.1:10001';",
            ],
        },
    }

    interactive_mg_runner.start_all(BASIC_MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)

    cursor = connection(7688, "replica_1").cursor()

    # 1/
    # This query use to incorrectly change REPLICA storage timestamp
    # run this multiple times to try and get into error case of MAIN timestamp < REPLICA timestamp
    for _ in range(20):
        execute_and_fetch_all(cursor, "SHOW INDEX INFO;")

    cursor = connection(7687, "main").cursor()

    # 2/
    execute_and_fetch_all(cursor, "CREATE ();")

    def retrieve_data():
        replicas = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICAS;")
        return replicas

    expected_data = [
        (
            "replica_1",
            "127.0.0.1:10001",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 2, "behind": 0, "status": "ready"}},
        ),
    ]
    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])

    # 3/
    cursor = connection(7688, "replica_1").cursor()
    result = execute_and_fetch_all(cursor, "MATCH () RETURN count(*);")

    assert len(result) == 1
    assert result[0][0] == 1  # The one node was replicated from MAIN to REPLICA


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
