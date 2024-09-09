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


def test_show_replicas(connection):
    # Goal of this test is to check the SHOW REPLICAS command.
    # 0/ We start all replicas manually: we want to be able to kill them ourselves without relying on external tooling to kill processes.
    # 1/ We check that all replicas have the correct state: they should all be ready.
    # 2/ We drop one replica. It should not appear anymore in the SHOW REPLICAS command.
    # 3/ We kill another replica. It should become invalid in the SHOW REPLICAS command.

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    cursor = connection(7687, "main").cursor()

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
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_1")
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_3")
    interactive_mg_runner.stop(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_4")

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


def test_drop_replicas(connection):
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

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    cursor = connection(7687, "main").cursor()

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
    mg_sleep_and_assert_collection(expected_data, retrieve_data)

    # 2/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_3")
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
    interactive_mg_runner.stop(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_4")
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
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_1")
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
    interactive_mg_runner.stop(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_2")
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
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_1")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_2")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_3")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_4")
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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
