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

import sys

import os
import pytest
import time

from common import execute_and_fetch_all
import interactive_mg_runner
import mgclient
import tempfile

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
            "REGISTER REPLICA replica_1 SYNC WITH TIMEOUT 2 TO '127.0.0.1:10001';",
            "REGISTER REPLICA replica_2 SYNC WITH TIMEOUT 1 TO '127.0.0.1:10002';",
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
    actual_data = set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))
    EXPECTED_COLUMN_NAMES = {"name", "socket_address", "sync_mode", "timeout", "state"}

    actual_column_names = {x.name for x in cursor.description}
    assert EXPECTED_COLUMN_NAMES == actual_column_names

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 2.0, "ready"),
        ("replica_2", "127.0.0.1:10002", "sync", 1.0, "ready"),
        ("replica_3", "127.0.0.1:10003", "async", None, "ready"),
        ("replica_4", "127.0.0.1:10004", "async", None, "ready"),
    }
    assert expected_data == actual_data

    # 2/
    execute_and_fetch_all(cursor, "DROP REPLICA replica_2")
    actual_data = set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))
    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 2.0, "ready"),
        ("replica_3", "127.0.0.1:10003", "async", None, "ready"),
        ("replica_4", "127.0.0.1:10004", "async", None, "ready"),
    }
    assert expected_data == actual_data

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_1")
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_3")
    interactive_mg_runner.stop(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_4")

    # We leave some time for the main to realise the replicas are down.
    time.sleep(2)
    actual_data = set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))
    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 2.0, "invalid"),
        ("replica_3", "127.0.0.1:10003", "async", None, "invalid"),
        ("replica_4", "127.0.0.1:10004", "async", None, "invalid"),
    }
    assert expected_data == actual_data


def test_add_replica_invalid_timeout(connection):
    # Goal of this test is to check the registration of replica with invalid timeout raises an exception
    CONFIGURATION = {
        "replica_1": {
            "args": ["--bolt-port", "7688", "--log-level=TRACE"],
            "log_file": "replica1.log",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10001;"],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE"],
            "log_file": "main.log",
            "setup_queries": [],
        },
    }

    interactive_mg_runner.start_all(CONFIGURATION)

    cursor = connection(7687, "main").cursor()
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(cursor, "REGISTER REPLICA replica_1 SYNC WITH TIMEOUT 0 TO '127.0.0.1:10001';")

    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(cursor, "REGISTER REPLICA replica_1 SYNC WITH TIMEOUT -5 TO '127.0.0.1:10001';")

    actual_data = execute_and_fetch_all(cursor, "SHOW REPLICAS;")
    assert 0 == len(actual_data)

    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_1 SYNC WITH TIMEOUT 1 TO '127.0.0.1:10001';")
    actual_data = execute_and_fetch_all(cursor, "SHOW REPLICAS;")
    assert 1 == len(actual_data)


def test_basic_recovery(connection):
    # Goal of this test is to check the recovery of main.
    # 0/ We start all replicas manually: we want to be able to kill them ourselves without relying on external tooling to kill processes.
    # 1/ We check that all replicas have the correct state: they should all be ready.
    # 2/ We kill main.
    # 3/ We re-start main.
    # 4/ We check that all replicas have the correct state: they should all be ready.
    # 5/ Drop one replica.
    # 6/ We add some data to main.
    # 7/ We check that all replicas but have the expected data.

    # 0/
    data_directory = tempfile.TemporaryDirectory()
    CONFIGURATION = {
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
            "setup_queries": [],
            "data_directory": data_directory.name,
        },
    }

    interactive_mg_runner.start_all(CONFIGURATION)
    cursor = connection(7687, "main").cursor()

    # We want to execute manually and not via the configuration, otherwise re-starting main would also execute these registration.
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_1 SYNC WITH TIMEOUT 2 TO '127.0.0.1:10001';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_2 SYNC WITH TIMEOUT 1 TO '127.0.0.1:10002';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_3 ASYNC TO '127.0.0.1:10003';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_4 ASYNC TO '127.0.0.1:10004';")

    # 1/
    EXPECTED_DATA = {
        ("replica_1", "127.0.0.1:10001", "sync", 2.0, "ready"),
        ("replica_2", "127.0.0.1:10002", "sync", 1.0, "ready"),
        ("replica_3", "127.0.0.1:10003", "async", None, "ready"),
        ("replica_4", "127.0.0.1:10004", "async", None, "ready"),
    }
    actual_data = set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))

    assert EXPECTED_DATA == actual_data

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
    time.sleep(2)

    # 3/
    interactive_mg_runner.start(CONFIGURATION, "main")
    cursor = connection(7687, "main").cursor()
    check_roles()

    # 4/
    # We leave some time for the main to recover.
    time.sleep(2)
    actual_data = set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))
    assert EXPECTED_DATA == actual_data

    # 5/
    execute_and_fetch_all(cursor, "DROP REPLICA replica_2;")

    # 6/
    execute_and_fetch_all(cursor, "CREATE (p1:Number {name:'Magic', value:42})")

    # 7/
    QUERY_TO_CHECK = "MATCH (node) return node;"
    res_from_main = execute_and_fetch_all(cursor, QUERY_TO_CHECK)
    assert 1 == len(res_from_main)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    for index in (1, 3, 4):
        assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES[f"replica_{index}"].query(QUERY_TO_CHECK)

    # Replica_2 was dropped, we check it does not have the data from main.
    assert 0 == len(interactive_mg_runner.MEMGRAPH_INSTANCES["replica_2"].query(QUERY_TO_CHECK))


# #NoCommit also a test where we kill a replica and bring it back to life
# #NoCommit Also a test where we kill main, start a new replica, then start main again (and then register the replica)

if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
