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

import atexit
import os
import shutil
import sys
import tempfile
import time
from functools import partial

import interactive_mg_runner
import mgclient
import pytest
from common import execute_and_fetch_all
from mg_utils import mg_sleep_and_assert

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
    "main": {
        "args": ["--bolt-port", "7687", "--log-level=TRACE"],
        "log_file": "main.log",
        "setup_queries": [
            "REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001';",
            "REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:10002';",
        ],
    },
}

TEMP_DIR = tempfile.TemporaryDirectory().name

MEMGRAPH_INSTANCES_DESCRIPTION_WITH_RECOVERY = {
    "replica_1": {
        "args": [
            "--bolt-port",
            "7688",
            "--log-level=TRACE",
            "--replication-restore-state-on-startup",
            "--data-recovery-on-startup",
        ],
        "log_file": "replica1.log",
        "data_directory": TEMP_DIR + "/replica1",
    },
    "replica_2": {
        "args": [
            "--bolt-port",
            "7689",
            "--log-level=TRACE",
            "--replication-restore-state-on-startup",
            "--data-recovery-on-startup",
        ],
        "log_file": "replica2.log",
        "data_directory": TEMP_DIR + "/replica2",
    },
    "main": {
        "args": [
            "--bolt-port",
            "7687",
            "--log-level=TRACE",
            "--replication-restore-state-on-startup",
            "--data-recovery-on-startup",
        ],
        "log_file": "main.log",
        "data_directory": TEMP_DIR + "/main",
    },
}


def safe_execute(function, *args):
    try:
        function(*args)
    except:
        pass


def setup_replication(connection):
    # Setup replica1
    cursor = connection(7688, "replica1").cursor()
    execute_and_fetch_all(cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")
    # Setup replica2
    cursor = connection(7689, "replica2").cursor()
    execute_and_fetch_all(cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10002;")
    # Setup main
    cursor = connection(7687, "main").cursor()
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:10002';")


def test_manual_databases_create_multitenancy_replication(connection):
    # Goal: to show that replication can be established against REPLICA which already
    # has the database we need (which was unused so far)
    # 0/ MAIN CREATE DATABASE A + B
    #    REPLICA CREATE DATABASE A + B
    #    Setup replication
    # 1/ Write to MAIN A, Write to MAIN B
    # 2/ Validate replication of changes to A + B have arrived at REPLICA

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
        "replica_2": {
            "args": ["--bolt-port", "7689", "--log-level=TRACE"],
            "log_file": "replica2.log",
            "setup_queries": [
                "CREATE DATABASE A;",
                "CREATE DATABASE B;",
                "SET REPLICATION ROLE TO REPLICA WITH PORT 10002;",
            ],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE"],
            "log_file": "main.log",
            "setup_queries": [
                "CREATE DATABASE A;",
                "CREATE DATABASE B;",
                "REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001';",
                "REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:10002';",
            ],
        },
    }

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
        ("replica_2", "127.0.0.1:10002", "async", 1, 0, "ready"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    def retrieve_data():
        execute_and_fetch_all(cursor, "USE DATABASE B;")
        return set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 1, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "async", 1, 0, "ready"),
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

    cursor_replica2 = connection(7688, "replica_2").cursor()
    execute_and_fetch_all(cursor_replica2, "USE DATABASE A;")
    actual_data = execute_and_fetch_all(cursor_replica2, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 1  # one node
    actual_data = execute_and_fetch_all(cursor_replica2, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 0  # zero relationships

    execute_and_fetch_all(cursor_replica2, "USE DATABASE B;")
    actual_data = execute_and_fetch_all(cursor_replica2, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 2  # two nodes
    actual_data = execute_and_fetch_all(cursor_replica2, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 1  # one relationship


def test_manual_databases_create_multitenancy_replication_branching(connection):
    # Goal: to show that replication can be established against REPLICA which already
    # has the database we need (which was unused so far)
    # 0/ MAIN CREATE DATABASE A + B and fill with data
    #    REPLICA CREATE DATABASE A + B and fil with exact data
    #    Setup REPLICA
    # 1/ Registering REPLICA on MAIN should fail due to branching (even though the data is the same)

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "replica_1": {
            "args": ["--bolt-port", "7688", "--log-level=TRACE"],
            "log_file": "replica1.log",
            "setup_queries": [
                "CREATE DATABASE A;",
                "USE DATABASE A;",
                "CREATE ()",
                "CREATE DATABASE B;",
                "USE DATABASE B;",
                "CREATE ()-[:EDGE]->()",
                "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;",
            ],
        },
        "replica_2": {
            "args": ["--bolt-port", "7689", "--log-level=TRACE"],
            "log_file": "replica2.log",
            "setup_queries": [
                "CREATE DATABASE A;",
                "USE DATABASE A;",
                "CREATE ()",
                "CREATE DATABASE B;",
                "USE DATABASE B;",
                "CREATE ()-[:EDGE]->()",
                "SET REPLICATION ROLE TO REPLICA WITH PORT 10002;",
            ],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE"],
            "log_file": "main.log",
            "setup_queries": [
                "CREATE DATABASE A;",
                "USE DATABASE A;",
                "CREATE ()",
                "CREATE DATABASE B;",
                "USE DATABASE B;",
                "CREATE ()-[:EDGE]->()",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL)
    cursor = connection(7687, "main").cursor()

    # 1/
    failed = False
    try:
        execute_and_fetch_all(cursor, "REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001';")
    except mgclient.DatabaseError:
        failed = True
    assert not failed
    # Updated the test, since the consensus was that this shouldn't fail, instead the replica should follow main at any cost.

    try:
        execute_and_fetch_all(cursor, "REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:10002';")
    except mgclient.DatabaseError:
        failed = True
    assert not failed


def test_manual_databases_create_multitenancy_replication_dirty_replica(connection):
    # Goal: to show that replication can be established against REPLICA which already
    # has the database we need (which was unused so far)
    # 0/ MAIN CREATE DATABASE A
    #    REPLICA CREATE DATABASE A
    #    REPLICA write to A
    #    Setup REPLICA
    # 1/ Register replica; should fail

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "replica_1": {
            "args": ["--bolt-port", "7688", "--log-level=TRACE"],
            "log_file": "replica1.log",
            "setup_queries": [
                "CREATE DATABASE A;",
                "USE DATABASE A;",
                "CREATE (:Node{from:'A'})",
                "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;",
            ],
        },
        "replica_2": {
            "args": ["--bolt-port", "7689", "--log-level=TRACE"],
            "log_file": "replica2.log",
            "setup_queries": [
                "CREATE DATABASE A;",
                "USE DATABASE A;",
                "CREATE (:Node{from:'A'})",
                "SET REPLICATION ROLE TO REPLICA WITH PORT 10002;",
            ],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE"],
            "log_file": "main.log",
            "setup_queries": [
                "CREATE DATABASE A;",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL)
    cursor = connection(7687, "main").cursor()

    # 1/
    failed = False
    try:
        execute_and_fetch_all(cursor, "REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001';")
    except mgclient.DatabaseError:
        failed = True
    assert not failed
    # Updated the test, since the consensus was that this shouldn't fail, instead the replica should follow main at any cost.

    try:
        execute_and_fetch_all(cursor, "REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:10002';")
    except mgclient.DatabaseError:
        failed = True
    assert not failed


def test_manual_databases_create_multitenancy_replication_main_behind(connection):
    # Goal: to show that replication can be established against REPLICA which already
    # has the database we need (which was unused so far)
    # 0/ REPLICA CREATE DATABASE A
    #    REPLICA write to A
    #    Setup replication
    # 1/ MAIN CREATE DATABASE A
    # 2/ Check that database has been replicated

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "replica_1": {
            "args": ["--bolt-port", "7688", "--log-level=TRACE"],
            "log_file": "replica1.log",
            "setup_queries": [
                "CREATE DATABASE A;",
                "USE DATABASE A;",
                "CREATE (:Node{from:'A'})",
                "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;",
            ],
        },
        "replica_2": {
            "args": ["--bolt-port", "7689", "--log-level=TRACE"],
            "log_file": "replica2.log",
            "setup_queries": [
                "CREATE DATABASE A;",
                "USE DATABASE A;",
                "CREATE (:Node{from:'A'})",
                "SET REPLICATION ROLE TO REPLICA WITH PORT 10002;",
            ],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE"],
            "log_file": "main.log",
            "setup_queries": [
                "REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001';",
                "REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:10002';",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL)
    main_cursor = connection(7687, "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")

    # 2/
    def retrieve_data():
        execute_and_fetch_all(main_cursor, "USE DATABASE A;")
        return set(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 0, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "async", 0, 0, "ready"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    expected_data = set(execute_and_fetch_all(main_cursor, "SHOW DATABASES"))

    def retrieve_data(cursor):
        return set(execute_and_fetch_all(cursor, "SHOW DATABASES;"))

    actual_data = mg_sleep_and_assert(expected_data, partial(retrieve_data, connection(7688, "replica1").cursor()))
    assert actual_data == expected_data
    actual_data = mg_sleep_and_assert(expected_data, partial(retrieve_data, connection(7689, "replica2").cursor()))
    assert actual_data == expected_data


def test_automatic_databases_create_multitenancy_replication(connection):
    # Goal: to show that replication can be established against REPLICA where a new databases
    # needs replicating over
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A
    # 2/ Write to MAIN A
    # 3/ Validate replication of changes to A have arrived at REPLICA

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)
    main_cursor = connection(7687, "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")

    # 2/
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")

    # 3/
    def retrieve_data():
        execute_and_fetch_all(main_cursor, "USE DATABASE A;")
        return set(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 7, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "async", 7, 0, "ready"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    def retrieve_data():
        execute_and_fetch_all(main_cursor, "USE DATABASE B;")
        return set(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 0, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "async", 0, 0, "ready"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    cursor_replica = connection(7688, "replica_1").cursor()
    execute_and_fetch_all(cursor_replica, "USE DATABASE A;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 7  # seven node
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 3  # three relationships

    execute_and_fetch_all(cursor_replica, "USE DATABASE B;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 0  # zero node
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 0  # zero relationships

    cursor_replica = connection(7689, "replica_2").cursor()
    execute_and_fetch_all(cursor_replica, "USE DATABASE A;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 7  # seven node
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 3  # three relationships

    execute_and_fetch_all(cursor_replica, "USE DATABASE B;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 0  # zero node
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 0  # zero relationships


def test_automatic_databases_multitenancy_replication_predefined(connection):
    # Goal: to show that replication can be established against REPLICA which already
    # has the database we need (which was unused so far)
    # 0/ MAIN CREATE DATABASE A + B
    #    Setup replication
    # 1/ Write to MAIN A, Write to MAIN B
    # 2/ Validate replication of changes to A + B have arrived at REPLICA

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "replica_1": {
            "args": ["--bolt-port", "7688", "--log-level=TRACE"],
            "log_file": "replica1.log",
            "setup_queries": [
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


def test_automatic_databases_create_multitenancy_replication_dirty_main(connection):
    # Goal: to show that replication can be established against REPLICA which already
    # has the database we need (which was unused so far)
    # 0/ MAIN CREATE DATABASE A
    #    MAIN write to A
    #    Setup replication
    # 1/ Validate

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "replica_1": {
            "args": ["--bolt-port", "7688", "--log-level=TRACE"],
            "log_file": "replica1.log",
            "setup_queries": [
                "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;",
            ],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE"],
            "log_file": "main.log",
            "setup_queries": [
                "CREATE DATABASE A;",
                "USE DATABASE A;",
                "CREATE (:Node{from:'A'})",
                "REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001';",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL)
    cursor = connection(7687, "main").cursor()

    # 1/
    def retrieve_data():
        execute_and_fetch_all(cursor, "USE DATABASE A;")
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


def test_multitenancy_replication_restart_replica_w_fc(connection):
    # Goal: to show that replication can be established against REPLICA where a new databases
    # needs replicating over
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A and B
    # 2/ Write on MAIN to A and B
    # 3/ Restart replica
    # 4/ Validate data on replica

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)
    main_cursor = connection(7687, "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")

    # 2/
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")

    execute_and_fetch_all(main_cursor, "USE DATABASE B;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_1")
    time.sleep(3)  # In order for the frequent check to run
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_1")
    time.sleep(3)  # In order for the frequent check to run

    # 4/
    def retrieve_data():
        execute_and_fetch_all(main_cursor, "USE DATABASE A;")
        return set(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 7, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "async", 7, 0, "ready"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    def retrieve_data():
        execute_and_fetch_all(main_cursor, "USE DATABASE B;")
        return set(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 3, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "async", 3, 0, "ready"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    cursor_replica = connection(7688, "replica_1").cursor()
    execute_and_fetch_all(cursor_replica, "USE DATABASE A;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 7  # seven node
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 3  # three relationships

    execute_and_fetch_all(cursor_replica, "USE DATABASE B;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 2  # two node
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 0  # zero relationships


def test_multitenancy_replication_restart_replica_async_w_fc(connection):
    # Goal: to show that replication can be established against REPLICA where a new databases
    # needs replicating over
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A and B
    # 2/ Write on MAIN to A and B
    # 3/ Restart replica
    # 4/ Validate data on replica

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)
    main_cursor = connection(7687, "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")

    # 2/
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")

    execute_and_fetch_all(main_cursor, "USE DATABASE B;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_2")
    time.sleep(3)  # In order for the frequent check to run
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_2")
    time.sleep(3)  # In order for the frequent check to run

    # 4/
    def retrieve_data():
        execute_and_fetch_all(main_cursor, "USE DATABASE A;")
        return set(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 7, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "async", 7, 0, "ready"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    def retrieve_data():
        execute_and_fetch_all(main_cursor, "USE DATABASE B;")
        return set(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 3, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "async", 3, 0, "ready"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    cursor_replica = connection(7688, "replica_2").cursor()
    execute_and_fetch_all(cursor_replica, "USE DATABASE A;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 7  # seven node
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 3  # three relationships

    execute_and_fetch_all(cursor_replica, "USE DATABASE B;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 2  # two node
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 0  # zero relationships


def test_multitenancy_replication_restart_replica_wo_fc(connection):
    # Goal: to show that replication can be established against REPLICA where a new databases
    # needs replicating over
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A and B
    # 2/ Write on MAIN to A and B
    # 3/ Restart replica
    # 4/ Validate data on replica

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)
    main_cursor = connection(7687, "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")

    # 2/
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")

    execute_and_fetch_all(main_cursor, "USE DATABASE B;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_1")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_1")

    # 4/
    def retrieve_data():
        execute_and_fetch_all(main_cursor, "USE DATABASE A;")
        return set(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 7, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "async", 7, 0, "ready"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    def retrieve_data():
        execute_and_fetch_all(main_cursor, "USE DATABASE B;")
        return set(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 3, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "async", 3, 0, "ready"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    cursor_replica = connection(7688, "replica_1").cursor()
    execute_and_fetch_all(cursor_replica, "USE DATABASE A;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 7  # seven node
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 3  # three relationships

    execute_and_fetch_all(cursor_replica, "USE DATABASE B;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 2  # two node
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 0  # zero relationships


def test_multitenancy_replication_restart_replica_async_wo_fc(connection):
    # Goal: to show that replication can be established against REPLICA where a new databases
    # needs replicating over
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A and B
    # 2/ Write on MAIN to A and B
    # 3/ Restart replica
    # 4/ Validate data on replica

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)
    main_cursor = connection(7687, "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")

    # 2/
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")

    execute_and_fetch_all(main_cursor, "USE DATABASE B;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_2")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_2")

    # 4/
    def retrieve_data():
        execute_and_fetch_all(main_cursor, "USE DATABASE A;")
        return set(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 7, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "async", 7, 0, "ready"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    def retrieve_data():
        execute_and_fetch_all(main_cursor, "USE DATABASE B;")
        return set(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 3, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "async", 3, 0, "ready"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    cursor_replica = connection(7688, "replica_1").cursor()
    execute_and_fetch_all(cursor_replica, "USE DATABASE A;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 7  # seven node
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 3  # three relationships

    execute_and_fetch_all(cursor_replica, "USE DATABASE B;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 2  # two node
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 0  # zero relationships


def test_multitenancy_replication_restart_replica_w_fc_w_rec(connection):
    # Goal: to show that replication can be established against REPLICA where a new databases
    # needs replicating over
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A and B
    # 2/ Write on MAIN to A and B
    # 3/ Restart replica
    # 4/ Validate data on replica

    # 0/
    # Tmp dir should already be removed, but sometimes its not...
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_WITH_RECOVERY)
    setup_replication(connection)
    main_cursor = connection(7687, "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")

    # 2/
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")

    execute_and_fetch_all(main_cursor, "USE DATABASE B;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION_WITH_RECOVERY, "replica_1")
    safe_execute(execute_and_fetch_all, main_cursor, "USE DATABASE A;")
    safe_execute(execute_and_fetch_all, main_cursor, "CREATE (:Node{on:'A'});")
    safe_execute(execute_and_fetch_all, main_cursor, "USE DATABASE B;")
    safe_execute(execute_and_fetch_all, main_cursor, "CREATE (:Node{on:'B'});")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION_WITH_RECOVERY, "replica_1")

    time.sleep(3)  # Wait for the frequent check to get called

    # 4/
    cursor_replica = connection(7688, "replica_1").cursor()
    execute_and_fetch_all(cursor_replica, "USE DATABASE A;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 8  # seven node
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 3  # three relationships

    execute_and_fetch_all(cursor_replica, "USE DATABASE B;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 3  # two node
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 0  # zero relationships


def test_multitenancy_replication_restart_replica_async_w_fc_w_rec(connection):
    # Goal: to show that replication can be established against REPLICA where a new databases
    # needs replicating over
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A and B
    # 2/ Write on MAIN to A and B
    # 3/ Restart replica
    # 4/ Validate data on replica

    # 0/
    # Tmp dir should already be removed, but sometimes its not...
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_WITH_RECOVERY)
    setup_replication(connection)
    main_cursor = connection(7687, "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")

    # 2/
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")

    execute_and_fetch_all(main_cursor, "USE DATABASE B;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION_WITH_RECOVERY, "replica_2")
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")
    execute_and_fetch_all(main_cursor, "USE DATABASE B;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION_WITH_RECOVERY, "replica_2")

    time.sleep(3)  # Wait for the frequent check to get called

    # 4/
    cursor_replica = connection(7688, "replica_2").cursor()
    execute_and_fetch_all(cursor_replica, "USE DATABASE A;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 8  # seven node
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 3  # three relationships

    execute_and_fetch_all(cursor_replica, "USE DATABASE B;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 3  # two node
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 0  # zero relationships


def test_multitenancy_replication_drop_replica(connection):
    # Goal: to show that replication can be established against REPLICA where a new databases
    # needs replicating over
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A and B
    # 2/ Write on MAIN to A and B
    # 3/ Drop and add the same replica
    # 4/ Validate data on replica

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)
    main_cursor = connection(7687, "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")

    # 2/
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")

    execute_and_fetch_all(main_cursor, "USE DATABASE B;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")

    # 3/
    execute_and_fetch_all(main_cursor, "DROP REPLICA replica_1;")
    execute_and_fetch_all(main_cursor, "REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001';")

    # 4/
    def retrieve_data():
        execute_and_fetch_all(main_cursor, "USE DATABASE A;")
        return set(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 7, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "async", 7, 0, "ready"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    def retrieve_data():
        execute_and_fetch_all(main_cursor, "USE DATABASE B;")
        return set(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 3, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "async", 3, 0, "ready"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    cursor_replica = connection(7688, "replica_1").cursor()
    execute_and_fetch_all(cursor_replica, "USE DATABASE A;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 7  # seven node
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 3  # three relationships

    execute_and_fetch_all(cursor_replica, "USE DATABASE B;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 2  # two node
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 0  # zero relationships


def test_multitenancy_replication_drop_replica_async(connection):
    # Goal: to show that replication can be established against REPLICA where a new databases
    # needs replicating over
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A and B
    # 2/ Write on MAIN to A and B
    # 3/ Drop and add the same replica
    # 4/ Validate data on replica

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)
    main_cursor = connection(7687, "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")

    # 2/
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")

    execute_and_fetch_all(main_cursor, "USE DATABASE B;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")

    # 3/
    execute_and_fetch_all(main_cursor, "DROP REPLICA replica_2;")
    execute_and_fetch_all(main_cursor, "REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:10002';")

    # 4/
    def retrieve_data():
        execute_and_fetch_all(main_cursor, "USE DATABASE A;")
        return set(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 7, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "async", 7, 0, "ready"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    def retrieve_data():
        execute_and_fetch_all(main_cursor, "USE DATABASE B;")
        return set(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 3, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "async", 3, 0, "ready"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    cursor_replica = connection(7688, "replica_2").cursor()
    execute_and_fetch_all(cursor_replica, "USE DATABASE A;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 7  # seven node
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 3  # three relationships

    execute_and_fetch_all(cursor_replica, "USE DATABASE B;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 2  # two node
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 0  # zero relationships


def test_multitenancy_replication_restart_main(connection):
    # Goal: to show that replication can be established against REPLICA where a new databases
    # needs replicating over
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A and B
    # 2/ Write on MAIN to A and B
    # 3/ Restart main and write new data
    # 4/ Validate data on replica

    # 0/
    # Tmp dir should already be removed, but sometimes its not...
    safe_execute(shutil.rmtree, TEMP_DIR)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_WITH_RECOVERY)
    setup_replication(connection)
    main_cursor = connection(7687, "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")

    # 2/
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")

    execute_and_fetch_all(main_cursor, "USE DATABASE B;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION_WITH_RECOVERY, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION_WITH_RECOVERY, "main")
    main_cursor = connection(7687, "main").cursor()

    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")
    execute_and_fetch_all(main_cursor, "USE DATABASE B;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")

    # 4/
    cursor_replica = connection(7688, "replica_1").cursor()
    execute_and_fetch_all(cursor_replica, "USE DATABASE A;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 8  # seven node
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 3  # three relationships

    execute_and_fetch_all(cursor_replica, "USE DATABASE B;")
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH (n) RETURN count(*);")
    assert actual_data[0][0] == 3  # two node
    actual_data = execute_and_fetch_all(cursor_replica, "MATCH ()-[r]->() RETURN count(*);")
    assert actual_data[0][0] == 0  # zero relationships


def test_automatic_databases_drop_multitenancy_replication(connection):
    # Goal: to show that replication can be established against REPLICA where a new databases
    # needs replicating over
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A
    # 2/ Write to MAIN A
    # 3/ Validate replication of changes to A have arrived at REPLICA
    # 4/ DROP DATABASE A/B
    # 5/ Check that the drop replicated

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)
    main_cursor = connection(7687, "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")

    # 2/
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")

    # 3/
    def retrieve_data():
        execute_and_fetch_all(main_cursor, "USE DATABASE A;")
        return set(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 1, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "async", 1, 0, "ready"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    def retrieve_data():
        execute_and_fetch_all(main_cursor, "USE DATABASE B;")
        return set(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 0, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "async", 0, 0, "ready"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    # 4/
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "DROP DATABASE A;")
    execute_and_fetch_all(main_cursor, "DROP DATABASE B;")

    # 5/
    expected_data = execute_and_fetch_all(main_cursor, "SHOW DATABASES;")

    def retrieve_data(cursor):
        return execute_and_fetch_all(cursor, "SHOW DATABASES;")

    actual_data = mg_sleep_and_assert(expected_data, partial(retrieve_data, connection(7688, "replica_1").cursor()))
    assert actual_data == expected_data
    actual_data = mg_sleep_and_assert(expected_data, partial(retrieve_data, connection(7689, "replica_2").cursor()))
    assert actual_data == expected_data


def test_drop_multitenancy_replication_restart_replica(connection):
    # Goal: to show that replication can be established against REPLICA where a new databases
    # needs replicating over
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A and B
    # 2/ Write on MAIN to A and B
    # 3/ Restart SYNC replica and drop database
    # 4/ Validate data on replica

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)
    main_cursor = connection(7687, "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")

    # 2/
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")

    execute_and_fetch_all(main_cursor, "USE DATABASE B;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_1")
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "DROP DATABASE B;")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_1")

    # 4/
    expected_data = execute_and_fetch_all(main_cursor, "SHOW DATABASES;")

    def retrieve_data(cursor):
        return execute_and_fetch_all(cursor, "SHOW DATABASES;")

    actual_data = mg_sleep_and_assert(expected_data, partial(retrieve_data, connection(7688, "replica_1").cursor()))
    assert actual_data == expected_data
    actual_data = mg_sleep_and_assert(expected_data, partial(retrieve_data, connection(7689, "replica_2").cursor()))
    assert actual_data == expected_data


def test_drop_multitenancy_replication_restart_replica_async(connection):
    # Goal: to show that replication can be established against REPLICA where a new databases
    # needs replicating over
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A and B
    # 2/ Write on MAIN to A and B
    # 3/ Restart ASYNC replica and drop database
    # 4/ Validate data on replica

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)
    main_cursor = connection(7687, "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")

    # 2/
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")

    execute_and_fetch_all(main_cursor, "USE DATABASE B;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_2")
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "DROP DATABASE B;")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION, "replica_2")

    # 4/
    expected_data = execute_and_fetch_all(main_cursor, "SHOW DATABASES;")

    def retrieve_data(cursor):
        return execute_and_fetch_all(cursor, "SHOW DATABASES;")

    actual_data = mg_sleep_and_assert(expected_data, partial(retrieve_data, connection(7688, "replica_1").cursor()))
    assert actual_data == expected_data
    actual_data = mg_sleep_and_assert(expected_data, partial(retrieve_data, connection(7689, "replica_2").cursor()))
    assert actual_data == expected_data


def test_create_multitenancy_replication_restart_replica(connection):
    # Goal: to show that replication can be established against REPLICA where a new databases
    # needs replicating over
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A
    # 2/ Write to MAIN A
    # 3/ Validate replication of changes to A have arrived at REPLICA
    # 4/ Start A transaction on replica 1, Use A on replica2
    # 5/ Check that the drop replicated
    # 6/ Validate that the transaction is still active and working and that the replica2 is not pointing to anything

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)
    main_cursor = connection(7687, "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")

    # 2/
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")

    # 3/
    def retrieve_data():
        execute_and_fetch_all(main_cursor, "USE DATABASE A;")
        return set(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 1, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "async", 1, 0, "ready"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    # 4/
    replica1_cursor = connection(7688, "replica").cursor()
    replica2_cursor = connection(7689, "replica").cursor()

    execute_and_fetch_all(replica1_cursor, "USE DATABASE A;")
    execute_and_fetch_all(replica1_cursor, "BEGIN")
    execute_and_fetch_all(replica2_cursor, "USE DATABASE A;")

    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "DROP DATABASE A;")

    # 5/
    # TODO Remove this once there is a replica state for the system
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")
    execute_and_fetch_all(main_cursor, "USE DATABASE B;")

    def retrieve_data():
        return set(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 0, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "async", 0, 0, "ready"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    # 6/
    assert execute_and_fetch_all(replica1_cursor, "MATCH(n) RETURN count(*);")[0][0] == 1  # one node
    execute_and_fetch_all(replica1_cursor, "COMMIT")
    failed = False
    try:
        execute_and_fetch_all(replica1_cursor, "MATCH(n) RETURN n;")
    except mgclient.DatabaseError:
        failed = True
    assert failed

    failed = False
    try:
        execute_and_fetch_all(replica2_cursor, "MATCH(n) RETURN n;")
    except mgclient.DatabaseError:
        failed = True
    assert failed


def test_multitenancy_drop_and_recreate_while_replica_using(connection):
    # Goal: to show that replication can be established against REPLICA where a new databases
    # needs replicating over
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A
    # 2/ Write to MAIN A
    # 3/ Validate replication of changes to A have arrived at REPLICA
    # 4/ Start A transaction on replica 1, Use A on replica2
    # 5/ Check that the drop/create replicated
    # 6/ Validate that the transaction is still active and working and that the replica2 is not pointing to anything

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)
    main_cursor = connection(7687, "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")

    # 2/
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")

    # 3/
    def retrieve_data():
        execute_and_fetch_all(main_cursor, "USE DATABASE A;")
        return set(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 1, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "async", 1, 0, "ready"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    # 4/
    replica1_cursor = connection(7688, "replica").cursor()
    replica2_cursor = connection(7689, "replica").cursor()

    execute_and_fetch_all(replica1_cursor, "USE DATABASE A;")
    execute_and_fetch_all(replica1_cursor, "BEGIN")
    execute_and_fetch_all(replica2_cursor, "USE DATABASE A;")

    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "DROP DATABASE A;")

    # 5/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")

    def retrieve_data():
        return set(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 0, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "async", 0, 0, "ready"),
    }
    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data

    # 6/
    assert execute_and_fetch_all(replica1_cursor, "MATCH(n) RETURN count(*);")[0][0] == 1  # one node
    execute_and_fetch_all(replica1_cursor, "COMMIT")
    failed = False
    try:
        execute_and_fetch_all(replica1_cursor, "MATCH(n) RETURN n;")
    except mgclient.DatabaseError:
        failed = True
    assert failed

    failed = False
    try:
        execute_and_fetch_all(replica2_cursor, "MATCH(n) RETURN n;")
    except mgclient.DatabaseError:
        failed = True
    assert failed


# start main and replica
# add databases
# kill main
# start main with replication state recovery, no data recovery
# note: just for future-proofing
# ???


if __name__ == "__main__":
    interactive_mg_runner.cleanup_directories_on_exit()
    sys.exit(pytest.main([__file__, "-rA"]))
