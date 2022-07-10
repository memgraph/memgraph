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

from common import execute_and_fetch_all
from mg_utils import mg_sleep_and_assert
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
    actual_data = set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))
    EXPECTED_COLUMN_NAMES = {
        "name",
        "socket_address",
        "sync_mode",
        "current_timestamp_of_replica",
        "number_of_timestamp_behind_master",
        "state",
    }

    actual_column_names = {x.name for x in cursor.description}
    assert actual_column_names == EXPECTED_COLUMN_NAMES

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 0, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "sync", 0, 0, "ready"),
        ("replica_3", "127.0.0.1:10003", "async", 0, 0, "ready"),
        ("replica_4", "127.0.0.1:10004", "async", 0, 0, "ready"),
    }
    assert actual_data == expected_data

    # 2/
    execute_and_fetch_all(cursor, "DROP REPLICA replica_2")
    actual_data = set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))
    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 0, 0, "ready"),
        ("replica_3", "127.0.0.1:10003", "async", 0, 0, "ready"),
        ("replica_4", "127.0.0.1:10004", "async", 0, 0, "ready"),
    }
    assert actual_data == expected_data

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_1")
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_3")
    interactive_mg_runner.stop(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_4")

    # We leave some time for the main to realise the replicas are down.
    def retrieve_data():
        return set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 0, 0, "invalid"),
        ("replica_3", "127.0.0.1:10003", "async", 0, 0, "invalid"),
        ("replica_4", "127.0.0.1:10004", "async", 0, 0, "invalid"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data


def test_basic_recovery(connection):
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
    # 10/ We re-add the two replicas droped/killed and check the data.
    # 11/ We kill another replica.
    # 12/ Add some more data to main.
    # 13/ Check the states of replicas.

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
            "args": ["--bolt-port", "7687", "--log-level=TRACE", "--storage-recover-on-startup=true"],
            "log_file": "main.log",
            "setup_queries": [],
            "data_directory": f"{data_directory.name}",
        },
    }

    interactive_mg_runner.start_all(CONFIGURATION)
    cursor = connection(7687, "main").cursor()

    # We want to execute manually and not via the configuration, otherwise re-starting main would also execute these registration.
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_2 SYNC TO '127.0.0.1:10002';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_3 ASYNC TO '127.0.0.1:10003';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_4 ASYNC TO '127.0.0.1:10004';")

    # 1/
    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 0, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "sync", 0, 0, "ready"),
        ("replica_3", "127.0.0.1:10003", "async", 0, 0, "ready"),
        ("replica_4", "127.0.0.1:10004", "async", 0, 0, "ready"),
    }
    actual_data = set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))

    assert actual_data == expected_data

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
        return set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))

    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

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

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 2, 0, "ready"),
        ("replica_3", "127.0.0.1:10003", "async", 2, 0, "ready"),
        ("replica_4", "127.0.0.1:10004", "async", 2, 0, "ready"),
    }
    actual_data = set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))
    assert actual_data == expected_data

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

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 6, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "sync", 6, 0, "ready"),
        ("replica_3", "127.0.0.1:10003", "async", 6, 0, "ready"),
        ("replica_4", "127.0.0.1:10004", "async", 6, 0, "ready"),
    }

    def retrieve_data2():
        return set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))

    actual_data = mg_sleep_and_assert(expected_data, retrieve_data2)

    assert actual_data == expected_data
    for index in (1, 2, 3, 4):
        assert interactive_mg_runner.MEMGRAPH_INSTANCES[f"replica_{index}"].query(QUERY_TO_CHECK) == res_from_main

    # 11/
    interactive_mg_runner.kill(CONFIGURATION, "replica_1")
    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 0, 0, "invalid"),
        ("replica_2", "127.0.0.1:10002", "sync", 6, 0, "ready"),
        ("replica_3", "127.0.0.1:10003", "async", 6, 0, "ready"),
        ("replica_4", "127.0.0.1:10004", "async", 6, 0, "ready"),
    }

    def retrieve_data3():
        return set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))

    actual_data = mg_sleep_and_assert(expected_data, retrieve_data3)
    assert actual_data == expected_data

    # 12/
    execute_and_fetch_all(cursor, "CREATE (p1:Number {name:'Magic_again_again', value:44})")
    res_from_main = execute_and_fetch_all(cursor, QUERY_TO_CHECK)
    assert len(res_from_main) == 3
    for index in (2, 3, 4):
        assert interactive_mg_runner.MEMGRAPH_INSTANCES[f"replica_{index}"].query(QUERY_TO_CHECK) == res_from_main

    # 13/
    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 0, 0, "invalid"),
        ("replica_2", "127.0.0.1:10002", "sync", 9, 0, "ready"),
        ("replica_3", "127.0.0.1:10003", "async", 9, 0, "ready"),
        ("replica_4", "127.0.0.1:10004", "async", 9, 0, "ready"),
    }
    actual_data = set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))
    assert actual_data == expected_data


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


def test_basic_recovery_when_replica_is_kill_when_main_is_down(connection):
    # Goal of this test is to check the recovery of main.
    # 0/ We start all replicas manually: we want to be able to kill them ourselves without relying on external tooling to kill processes.
    # 1/ We check that all replicas have the correct state: they should all be ready.
    # 2/ We kill main then kill a replica.
    # 3/ We re-start main: it should be able to restart.
    # 4/ Check status of replica: replica_2 is invalid.

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
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE", "--storage-recover-on-startup=true"],
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
    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 0, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "sync", 0, 0, "ready"),
    }
    actual_data = set(interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICAS;"))

    assert actual_data == expected_data

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
    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 0, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "sync", 0, 0, "invalid"),
    }
    actual_data = set(interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICAS;"))
    assert actual_data == expected_data


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
