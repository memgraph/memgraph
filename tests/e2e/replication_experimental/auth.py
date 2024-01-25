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

BOLT_PORTS = {"main": 7687, "replica_1": 7688, "replica_2": 7689}
REPLICATION_PORTS = {"replica_1": 10001, "replica_2": 10002}

MEMGRAPH_INSTANCES_DESCRIPTION = {
    "replica_1": {
        "args": ["--bolt-port", f"{BOLT_PORTS['replica_1']}", "--log-level=TRACE"],
        "log_file": "replica1.log",
        "setup_queries": [f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};"],
    },
    "replica_2": {
        "args": ["--bolt-port", f"{BOLT_PORTS['replica_2']}", "--log-level=TRACE"],
        "log_file": "replica2.log",
        "setup_queries": [f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_2']};"],
    },
    "main": {
        "args": ["--bolt-port", f"{BOLT_PORTS['main']}", "--log-level=TRACE"],
        "log_file": "main.log",
        "setup_queries": [
            f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
            f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';",
        ],
    },
}

TEMP_DIR = tempfile.TemporaryDirectory().name

MEMGRAPH_INSTANCES_DESCRIPTION_WITH_RECOVERY = {
    "replica_1": {
        "args": [
            "--bolt-port",
            f"{BOLT_PORTS['replica_1']}",
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
            f"{BOLT_PORTS['replica_2']}",
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
            f"{BOLT_PORTS['main']}",
            "--log-level=TRACE",
            "--replication-restore-state-on-startup",
            "--data-recovery-on-startup",
        ],
        "log_file": "main.log",
        "data_directory": TEMP_DIR + "/main",
    },
}


def show_users_func(cursor):
    def func():
        return set(execute_and_fetch_all(cursor, "SHOW USERS;"))

    return func


def show_roles_func(cursor):
    def func():
        return set(execute_and_fetch_all(cursor, "SHOW ROLES;"))

    return func


def show_users_for_role_func(cursor, rolename):
    def func():
        return set(execute_and_fetch_all(cursor, f"SHOW USERS FOR {rolename};"))

    return func


def show_role_for_user_func(cursor, username):
    def func():
        return set(execute_and_fetch_all(cursor, f"SHOW ROLE FOR {username};"))

    return func


def test_manual_users_replication(connection):
    # Goal: show system recovery in action at registration time
    # 0/ MAIN CREATE USER user1, user2
    #    REPLICA CREATE USER user3, user4
    #    Setup replication cluster
    # 1/ Check that both MAIN and REPLICA have user1 and user2
    # 2/ Check connections on REPLICAS

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "replica_1": {
            "args": ["--bolt-port", f"{BOLT_PORTS['replica_1']}", "--log-level=TRACE"],
            "log_file": "replica1.log",
            "setup_queries": [
                "CREATE USER user3;",
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};",
            ],
        },
        "replica_2": {
            "args": ["--bolt-port", f"{BOLT_PORTS['replica_2']}", "--log-level=TRACE"],
            "log_file": "replica2.log",
            "setup_queries": [
                "CREATE USER user4 IDENTIFIED BY 'password';",
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_2']};",
            ],
        },
        "main": {
            "args": ["--bolt-port", f"{BOLT_PORTS['main']}", "--log-level=TRACE"],
            "log_file": "main.log",
            "setup_queries": [
                "CREATE USER user1;",
                "CREATE USER user2 IDENTIFIED BY 'password';",
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
                f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL)
    cursor = connection(BOLT_PORTS["main"], "main", "user1").cursor()

    # 1/
    expected_data = {("user2",), ("user1",)}
    mg_sleep_and_assert(
        expected_data, show_users_func(connection(BOLT_PORTS["replica_1"], "replica", "user1").cursor())
    )
    mg_sleep_and_assert(
        expected_data, show_users_func(connection(BOLT_PORTS["replica_2"], "replica", "user1").cursor())
    )

    # 2/
    connection(BOLT_PORTS["replica_1"], "replica", "user1").cursor()
    connection(BOLT_PORTS["replica_1"], "replica", "user2", "password").cursor()
    connection(BOLT_PORTS["replica_2"], "replica", "user1").cursor()
    connection(BOLT_PORTS["replica_2"], "replica", "user2", "password").cursor()


def test_manual_roles_replication(connection):
    # Goal: show system recovery in action at registration time
    # 0/ MAIN CREATE USER user1, user2
    #    REPLICA CREATE USER user3, user4
    #    Setup replication cluster
    # 1/ Check that both MAIN and REPLICA have user1 and user2
    # 2/ Check that role1 and role2 are replicated
    # 3/ Check that user1 has role1

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "replica_1": {
            "args": ["--bolt-port", f"{BOLT_PORTS['replica_1']}", "--log-level=TRACE"],
            "log_file": "replica1.log",
            "setup_queries": [
                "CREATE ROLE role3;",
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};",
            ],
        },
        "replica_2": {
            "args": ["--bolt-port", f"{BOLT_PORTS['replica_2']}", "--log-level=TRACE"],
            "log_file": "replica2.log",
            "setup_queries": [
                "CREATE ROLE role4;",
                "CREATE USER user4;",
                "SET ROLE FOR user4 TO role4;",
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_2']};",
            ],
        },
        "main": {
            "args": ["--bolt-port", f"{BOLT_PORTS['main']}", "--log-level=TRACE"],
            "log_file": "main.log",
            "setup_queries": [
                "CREATE ROLE role1;",
                "CREATE ROLE role2;",
                "CREATE USER user2;",
                "SET ROLE FOR user2 TO role2;",
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
                f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL)
    connection(BOLT_PORTS["main"], "main", "user2").cursor()  # Just check if it connects
    cursor_replica_1 = connection(BOLT_PORTS["replica_1"], "replica", "user2").cursor()
    cursor_replica_2 = connection(BOLT_PORTS["replica_2"], "replica", "user2").cursor()

    # 1/
    expected_data = {
        ("user2",),
    }
    mg_sleep_and_assert(expected_data, show_users_func(cursor_replica_1))
    mg_sleep_and_assert(expected_data, show_users_func(cursor_replica_2))

    # 2/
    expected_data = {("role2",), ("role1",)}
    mg_sleep_and_assert(expected_data, show_roles_func(cursor_replica_1))
    mg_sleep_and_assert(expected_data, show_roles_func(cursor_replica_2))

    # 3/
    expected_data = {("role2",)}
    mg_sleep_and_assert(
        expected_data,
        show_role_for_user_func(cursor_replica_1, "user2"),
    )
    mg_sleep_and_assert(
        expected_data,
        show_role_for_user_func(cursor_replica_2, "user2"),
    )


if __name__ == "__main__":
    interactive_mg_runner.cleanup_directories_on_exit()
    sys.exit(pytest.main([__file__, "-rA"]))
