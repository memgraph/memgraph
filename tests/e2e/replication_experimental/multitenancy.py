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
import sys

import interactive_mg_runner
import pytest
from common import execute_and_fetch_all
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))


MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
    "replica_1": {
        "args": ["--bolt-port", "7688", "--log-level=TRACE"],
        "log_file": "replica1.log",
        "setup_queries": [
            "CREATE DATABASE A;",
            "CREATE DATABASE B;",
            "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;",
        ],
    },
    "main": {
        "args": ["--bolt-port", "7687", "--log-level=TRACE"],
        "log_file": "main.log",
        "setup_queries": [
            "CREATE DATABASE A;",
            "CREATE DATABASE B;",
            "REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001';",
        ],
    },
}

MEMGRAPH_INSTANCES_DESCRIPTION = {
    "replica_1": {
        "args": ["--bolt-port", "7688", "--log-level=TRACE"],
        "log_file": "replica1.log",
        "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10001;"],
    },
    "main": {
        "args": ["--bolt-port", "7687", "--log-level=TRACE"],
        "log_file": "main.log",
        "setup_queries": [
            "REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001';",
        ],
    },
}


def test_manual_databases_create_multitenacy_replication(connection):
    # Goal: to show that replication can be established against REPLICA which already
    # has the database we need (which was unused so far)
    # 0/ MAIN CREATE DATABASE A + B
    #    REPLICA CREATE DATABASE A + B
    #    Setup replication
    # 1/ Write to MAIN A, Write to MAIN B
    # 2/ Validate replication of changes to A + B have arrived at REPLICA

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL)
    cursor = connection(7687, "main").cursor()

    # 1/
    execute_and_fetch_all(cursor, "USE DATABASE A;")
    execute_and_fetch_all(cursor, "CREATE ();")
    execute_and_fetch_all(cursor, "USE DATABASE B;")
    execute_and_fetch_all(cursor, "CREATE ()-[:EDGE]->();")

    # 2/
    def retrieve_data():
        execute_and_fetch_all(cursor, "USE DATABASE A;")
        return set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 1, 0, "ready"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    def retrieve_data():
        execute_and_fetch_all(cursor, "USE DATABASE B;")
        return set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 1, 0, "ready"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    cursor_replica = connection(7688, "replica_1").cursor()
    execute_and_fetch_all(cursor_replica, "USE DATABASE A;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 1  # one node
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 0  # zero relationships

    execute_and_fetch_all(cursor_replica, "USE DATABASE B;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 2  # two nodes
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 1  # one relationship


def test_automatic_databases_create_multitenacy_replication(connection):
    # Goal: to show that replication can be established against REPLICA where a new databases
    # needs replicating over
    # 0/ Setup replication
    # 0/ MAIN CREATE DATABASE A
    # 1/ Validate database A on REPLICA
    # 2/ Write to MAIN A
    # 3/ Validate replication of changes to A have arrived at REPLICA

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)
    cursor = connection(7687, "main").cursor()

    # 1/
    execute_and_fetch_all(cursor, "CREATE DATABASE A;")  # TODO we need to add this capability

    # 2/
    cursor_replica = connection(7688, "replica_1").cursor()
    result = set(execute_and_fetch_all(cursor_replica, "SHOW DATABASES"))

    # TODO: finish test


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
