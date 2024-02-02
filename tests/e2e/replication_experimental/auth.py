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
TEMP_DIR = tempfile.TemporaryDirectory().name


def update_to_main(cursor):
    execute_and_fetch_all(cursor, "SET REPLICATION ROLE TO MAIN;")


def add_user(cursor, username, password=None):
    if password is not None:
        return execute_and_fetch_all(cursor, f"CREATE USER {username} IDENTIFIED BY '{password}';")
    return execute_and_fetch_all(cursor, f"CREATE USER {username};")


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


def show_privileges_func(cursor, user_or_role):
    def func():
        return set(execute_and_fetch_all(cursor, f"SHOW PRIVILEGES FOR {user_or_role};"))

    return func


def show_database_privileges_func(cursor, user):
    def func():
        return execute_and_fetch_all(cursor, f"SHOW DATABASE PRIVILEGES FOR {user};")

    return func


def show_database_func(cursor):
    def func():
        return execute_and_fetch_all(cursor, f"SHOW DATABASE;")

    return func


def try_and_count(cursor, query):
    try:
        execute_and_fetch_all(cursor, query)
    except:
        return 1
    return 0


def only_main_queries(cursor):
    n_exceptions = 0

    n_exceptions += try_and_count(cursor, f"CREATE USER user_name")
    n_exceptions += try_and_count(cursor, f"SET PASSWORD FOR user_name TO 'new_password'")
    n_exceptions += try_and_count(cursor, f"DROP USER user_name")
    n_exceptions += try_and_count(cursor, f"CREATE ROLE role_name")
    n_exceptions += try_and_count(cursor, f"DROP ROLE role_name")
    n_exceptions += try_and_count(cursor, f"CREATE USER user_name")
    n_exceptions += try_and_count(cursor, f"CREATE ROLE role_name")
    n_exceptions += try_and_count(cursor, f"SET ROLE FOR user_name TO role_name")
    n_exceptions += try_and_count(cursor, f"CLEAR ROLE FOR user_name")
    n_exceptions += try_and_count(cursor, f"GRANT AUTH TO role_name")
    n_exceptions += try_and_count(cursor, f"DENY AUTH, INDEX TO user_name")
    n_exceptions += try_and_count(cursor, f"REVOKE AUTH FROM role_name")
    n_exceptions += try_and_count(cursor, f"GRANT READ ON LABELS :l TO role_name;")
    n_exceptions += try_and_count(cursor, f"REVOKE EDGE_TYPES :e FROM user_name")
    n_exceptions += try_and_count(cursor, f"GRANT DATABASE memgraph TO user_name;")
    n_exceptions += try_and_count(cursor, f"SET MAIN DATABASE memgraph FOR user_name")
    n_exceptions += try_and_count(cursor, f"REVOKE DATABASE memgraph FROM user_name;")

    return n_exceptions


def main_and_repl_queries(cursor):
    n_exceptions = 0

    try_and_count(cursor, f"SHOW USERS")
    try_and_count(cursor, f"SHOW ROLES")
    try_and_count(cursor, f"SHOW USERS FOR ROLE role_name")
    try_and_count(cursor, f"SHOW ROLE FOR user_name")
    try_and_count(cursor, f"SHOW PRIVILEGES FOR role_name")
    try_and_count(cursor, f"SHOW DATABASE PRIVILEGES FOR user_name")

    return n_exceptions


def test_auth_queries_on_replica(connection):
    # Goal: check that write auth queries are forbidden on REPLICAs
    # 0/ Setup replication cluster
    # 1/ Check queries

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "replica_1": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_1']}",
                "--log-level=TRACE",
                "--data_directory",
                TEMP_DIR + "/replica1",
            ],
            "log_file": "replica1.log",
            "setup_queries": [
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};",
            ],
        },
        "replica_2": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_2']}",
                "--log-level=TRACE",
                "--data_directory",
                TEMP_DIR + "/replica2",
            ],
            "log_file": "replica2.log",
            "setup_queries": [
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_2']};",
            ],
        },
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
                "--data_directory",
                TEMP_DIR + "/main",
            ],
            "log_file": "main.log",
            "setup_queries": [
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
                f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL, keep_directories=False)
    cursor_main = connection(BOLT_PORTS["main"], "main", "UsErA", "pass").cursor()
    cursor_replica_1 = connection(BOLT_PORTS["replica_1"], "replica", "UsErA", "pass").cursor()
    cursor_replica_2 = connection(BOLT_PORTS["replica_2"], "replica", "UsErA", "pass").cursor()

    # 1/
    assert only_main_queries(cursor_main) == 0
    assert only_main_queries(cursor_replica_1) == 17
    assert only_main_queries(cursor_replica_2) == 17
    assert main_and_repl_queries(cursor_main) == 0
    assert main_and_repl_queries(cursor_replica_1) == 0
    assert main_and_repl_queries(cursor_replica_2) == 0


def test_manual_users_recovery(connection):
    # Goal: show system recovery in action at registration time
    # 0/ MAIN CREATE USER user1, user2
    #    REPLICA CREATE USER user3, user4
    #    Setup replication cluster
    # 1/ Check that both MAIN and REPLICA have user1 and user2
    # 2/ Check connections on REPLICAS

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "replica_1": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_1']}",
                "--log-level=TRACE",
                "--data_directory",
                TEMP_DIR + "/replica1",
            ],
            "log_file": "replica1.log",
            "setup_queries": [
                "CREATE USER user3;",
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};",
            ],
        },
        "replica_2": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_2']}",
                "--log-level=TRACE",
                "--data_directory",
                TEMP_DIR + "/replica2",
            ],
            "log_file": "replica2.log",
            "setup_queries": [
                "CREATE USER user4 IDENTIFIED BY 'password';",
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_2']};",
            ],
        },
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
                "--data_directory",
                TEMP_DIR + "/main",
            ],
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
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL, keep_directories=False)
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


def test_env_users_recovery(connection):
    # Goal: show system recovery in action at registration time
    # 0/ Set users from the environment
    #    MAIN gets users from the environment
    #    Setup replication cluster
    # 1/ Check that both MAIN and REPLICA have user1

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "replica_1": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_1']}",
                "--log-level=TRACE",
                "--data_directory",
                TEMP_DIR + "/replica1",
            ],
            "log_file": "replica1.log",
            "setup_queries": [
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};",
            ],
        },
        "replica_2": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_2']}",
                "--log-level=TRACE",
                "--data_directory",
                TEMP_DIR + "/replica2",
            ],
            "log_file": "replica2.log",
            "setup_queries": [
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_2']};",
            ],
        },
        "main": {
            "username": "user1",
            "password": "password",
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
                "--data_directory",
                TEMP_DIR + "/main",
            ],
            "log_file": "main.log",
            "setup_queries": [
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
                f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';",
            ],
        },
    }

    # 0/
    # Start only replicas without the env user
    interactive_mg_runner.stop_all(keep_directories=False)
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL, "replica_1")
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL, "replica_2")
    # Setup user
    try:
        os.environ["MEMGRAPH_USER"] = "user1"
        os.environ["MEMGRAPH_PASSWORD"] = "password"
        # Start main
        interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL, "main")
    finally:
        # Cleanup
        del os.environ["MEMGRAPH_USER"]
        del os.environ["MEMGRAPH_PASSWORD"]

    # 1/
    expected_data = {("user1",)}
    assert expected_data == show_users_func(connection(BOLT_PORTS["main"], "main", "user1", "password").cursor())()
    mg_sleep_and_assert(
        expected_data, show_users_func(connection(BOLT_PORTS["replica_1"], "replica", "user1", "password").cursor())
    )
    mg_sleep_and_assert(
        expected_data, show_users_func(connection(BOLT_PORTS["replica_2"], "replica", "user1", "password").cursor())
    )


def test_manual_roles_recovery(connection):
    # Goal: show system recovery in action at registration time
    # 0/ MAIN CREATE USER user1, user2
    #    REPLICA CREATE USER user3, user4
    #    Setup replication cluster
    # 1/ Check that both MAIN and REPLICA have user1 and user2
    # 2/ Check that role1 and role2 are replicated
    # 3/ Check that user1 has role1

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "replica_1": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_1']}",
                "--log-level=TRACE",
                "--data_directory",
                TEMP_DIR + "/replica1",
            ],
            "log_file": "replica1.log",
            "setup_queries": [
                "CREATE ROLE role3;",
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};",
            ],
        },
        "replica_2": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_2']}",
                "--log-level=TRACE",
                "--data_directory",
                TEMP_DIR + "/replica2",
            ],
            "log_file": "replica2.log",
            "setup_queries": [
                "CREATE ROLE role4;",
                "CREATE USER user4;",
                "SET ROLE FOR user4 TO role4;",
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_2']};",
            ],
        },
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
                "--data_directory",
                TEMP_DIR + "/main",
            ],
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
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL, keep_directories=False)
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


def test_auth_config_recovery(connection):
    # Goal: show we are replicating Auth::Config
    # 0/ Setup auth configuration and compliant users
    # 1/ Check that both MAIN and REPLICA have the same users
    # 2/ Check that REPLICAS have the same config

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "replica_1": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_1']}",
                "--log-level=TRACE",
                "--auth-password-strength-regex",
                "^[A-Z]+$",
                "--auth-password-permit-null=false",
                "--auth-user-or-role-name-regex",
                "^[O-o]+$",
                "--data_directory",
                TEMP_DIR + "/replica1",
            ],
            "log_file": "replica1.log",
            "setup_queries": [
                "CREATE USER OPQabc IDENTIFIED BY 'PASSWORD';",
                "CREATE ROLE defRST;",
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};",
            ],
        },
        "replica_2": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_2']}",
                "--log-level=TRACE",
                "--auth-password-strength-regex",
                "^[0-9]+$",
                "--auth-password-permit-null=true",
                "--auth-user-or-role-name-regex",
                "^[A-Np-z]+$",
                "--data_directory",
                TEMP_DIR + "/replica2",
            ],
            "log_file": "replica2.log",
            "setup_queries": [
                "CREATE ROLE ABCpqr;",
                "CREATE USER stuDEF;",
                "CREATE USER GvHwI IDENTIFIED BY '123456';",
                "SET ROLE FOR GvHwI TO ABCpqr;",
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_2']};",
            ],
        },
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
                "--auth-password-strength-regex",
                "^[a-z]+$",
                "--auth-password-permit-null=false",
                "--auth-user-or-role-name-regex",
                "^[A-z]+$",
                "--data_directory",
                TEMP_DIR + "/main",
            ],
            "log_file": "main.log",
            "setup_queries": [
                "CREATE USER UsErA IDENTIFIED BY 'pass';",
                "CREATE ROLE rOlE;",
                "CREATE USER uSeRB IDENTIFIED BY 'word';",
                "SET ROLE FOR uSeRB TO rOlE;",
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
                f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL, keep_directories=False)

    # 1/
    cursor_main = connection(BOLT_PORTS["main"], "main", "UsErA", "pass").cursor()
    cursor_replica_1 = connection(BOLT_PORTS["replica_1"], "replica", "UsErA", "pass").cursor()
    cursor_replica_2 = connection(BOLT_PORTS["replica_2"], "replica", "UsErA", "pass").cursor()

    # 2/ Only MAIN can update users
    def user_test(cursor):
        with pytest.raises(mgclient.DatabaseError, match="Invalid user name."):
            add_user(cursor, "UsEr1", "abcdef")
        with pytest.raises(mgclient.DatabaseError, match="Null passwords aren't permitted!"):
            add_user(cursor, "UsErC")
        with pytest.raises(mgclient.DatabaseError, match="The user password doesn't conform to the required strength!"):
            add_user(cursor, "UsErC", "123456")

    user_test(cursor_main)
    update_to_main(cursor_replica_1)
    user_test(cursor_replica_1)
    update_to_main(cursor_replica_2)
    user_test(cursor_replica_2)


def test_auth_replication(connection):
    # Goal: show that individual auth queries get replicated

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "replica_1": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_1']}",
                "--log-level=TRACE",
                "--data_directory",
                TEMP_DIR + "/replica1",
            ],
            "log_file": "replica1.log",
            "setup_queries": [
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};",
            ],
        },
        "replica_2": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_2']}",
                "--log-level=TRACE",
                "--data_directory",
                TEMP_DIR + "/replica2",
            ],
            "log_file": "replica2.log",
            "setup_queries": [
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_2']};",
            ],
        },
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
                "--data_directory",
                TEMP_DIR + "/main",
            ],
            "log_file": "main.log",
            "setup_queries": [
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
                f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL, keep_directories=False)
    cursor_main = connection(BOLT_PORTS["main"], "main", "user1").cursor()
    cursor_replica1 = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    cursor_replica2 = connection(BOLT_PORTS["replica_2"], "replica").cursor()

    # 1/
    def check(f, expected_data):
        # mg_sleep_and_assert(
        # REPLICA 1 is SYNC, should already be ready
        assert expected_data == f(cursor_replica1)()
        # )
        mg_sleep_and_assert(expected_data, f(cursor_replica2))

    # CREATE USER
    execute_and_fetch_all(cursor_main, "CREATE USER user1")
    check(
        show_users_func,
        {
            ("user1",),
        },
    )
    execute_and_fetch_all(cursor_main, "CREATE USER user2 IDENTIFIED BY 'pass'")
    check(
        show_users_func,
        {
            ("user2",),
            ("user1",),
        },
    )
    connection(BOLT_PORTS["replica_1"], "replica", "user1").cursor()  # Just check connection
    connection(BOLT_PORTS["replica_2"], "replica", "user1").cursor()  # Just check connection
    connection(BOLT_PORTS["replica_1"], "replica", "user2", "pass").cursor()  # Just check connection
    connection(BOLT_PORTS["replica_2"], "replica", "user2", "pass").cursor()  # Just check connection

    # SET PASSWORD
    execute_and_fetch_all(cursor_main, "SET PASSWORD FOR user1 TO '1234'")
    execute_and_fetch_all(cursor_main, "SET PASSWORD FOR user2 TO 'new_pass'")
    connection(BOLT_PORTS["replica_1"], "replica", "user1", "1234").cursor()  # Just check connection
    connection(BOLT_PORTS["replica_2"], "replica", "user1", "1234").cursor()  # Just check connection
    connection(BOLT_PORTS["replica_1"], "replica", "user2", "new_pass").cursor()  # Just check connection
    connection(BOLT_PORTS["replica_2"], "replica", "user2", "new_pass").cursor()  # Just check connection

    # DROP USER
    execute_and_fetch_all(cursor_main, "DROP USER user2")
    check(
        show_users_func,
        {
            ("user1",),
        },
    )
    execute_and_fetch_all(cursor_main, "DROP USER user1")
    check(show_users_func, set())
    connection(BOLT_PORTS["replica_1"], "replica").cursor()  # Just check connection
    connection(BOLT_PORTS["replica_2"], "replica").cursor()  # Just check connection

    # CREATE ROLE
    execute_and_fetch_all(cursor_main, "CREATE ROLE role1")
    check(
        show_roles_func,
        {
            ("role1",),
        },
    )
    execute_and_fetch_all(cursor_main, "CREATE ROLE role2")
    check(
        show_roles_func,
        {
            ("role2",),
            ("role1",),
        },
    )

    # DROP ROLE
    execute_and_fetch_all(cursor_main, "DROP ROLE role2")
    check(
        show_roles_func,
        {
            ("role1",),
        },
    )
    execute_and_fetch_all(cursor_main, "DROP ROLE role1")
    check(show_roles_func, set())

    # SET ROLE
    execute_and_fetch_all(cursor_main, "CREATE USER user3")
    execute_and_fetch_all(cursor_main, "CREATE ROLE role3")
    execute_and_fetch_all(cursor_main, "SET ROLE FOR user3 TO role3")
    check(partial(show_role_for_user_func, username="user3"), {("role3",)})
    execute_and_fetch_all(cursor_main, "CREATE USER user3b")
    execute_and_fetch_all(cursor_main, "SET ROLE FOR user3b TO role3")
    check(partial(show_role_for_user_func, username="user3b"), {("role3",)})
    check(
        partial(show_users_for_role_func, rolename="role3"),
        {
            ("user3",),
            ("user3b",),
        },
    )

    # CLEAR ROLE
    execute_and_fetch_all(cursor_main, "CLEAR ROLE FOR user3")
    check(partial(show_role_for_user_func, username="user3"), {("null",)})
    check(
        partial(show_users_for_role_func, rolename="role3"),
        {
            ("user3b",),
        },
    )

    # GRANT/REVOKE/DENY privileges TO user
    execute_and_fetch_all(cursor_main, "CREATE USER user4")
    execute_and_fetch_all(cursor_main, "REVOKE ALL PRIVILEGES FROM user4")
    execute_and_fetch_all(cursor_main, "GRANT CREATE, DELETE, SET TO user4")
    check(
        partial(show_privileges_func, user_or_role="user4"),
        {
            ("CREATE", "GRANT", "GRANTED TO USER"),
            ("DELETE", "GRANT", "GRANTED TO USER"),
            ("SET", "GRANT", "GRANTED TO USER"),
        },
    )
    execute_and_fetch_all(cursor_main, "REVOKE SET FROM user4")
    check(
        partial(show_privileges_func, user_or_role="user4"),
        {("CREATE", "GRANT", "GRANTED TO USER"), ("DELETE", "GRANT", "GRANTED TO USER")},
    )
    execute_and_fetch_all(cursor_main, "DENY DELETE TO user4")
    check(
        partial(show_privileges_func, user_or_role="user4"),
        {("CREATE", "GRANT", "GRANTED TO USER"), ("DELETE", "DENY", "DENIED TO USER")},
    )

    # GRANT/REVOKE/DENY privileges TO role
    execute_and_fetch_all(cursor_main, "REVOKE ALL PRIVILEGES FROM role3")
    execute_and_fetch_all(cursor_main, "REVOKE ALL PRIVILEGES FROM user3b")
    execute_and_fetch_all(cursor_main, "GRANT CREATE, DELETE, SET TO role3")
    check(
        partial(show_privileges_func, user_or_role="role3"),
        {
            ("CREATE", "GRANT", "GRANTED TO ROLE"),
            ("DELETE", "GRANT", "GRANTED TO ROLE"),
            ("SET", "GRANT", "GRANTED TO ROLE"),
        },
    )
    check(
        partial(show_privileges_func, user_or_role="user3b"),
        {
            ("CREATE", "GRANT", "GRANTED TO ROLE"),
            ("DELETE", "GRANT", "GRANTED TO ROLE"),
            ("SET", "GRANT", "GRANTED TO ROLE"),
        },
    )
    execute_and_fetch_all(cursor_main, "REVOKE SET FROM role3")
    check(
        partial(show_privileges_func, user_or_role="role3"),
        {("CREATE", "GRANT", "GRANTED TO ROLE"), ("DELETE", "GRANT", "GRANTED TO ROLE")},
    )
    check(
        partial(show_privileges_func, user_or_role="user3b"),
        {("CREATE", "GRANT", "GRANTED TO ROLE"), ("DELETE", "GRANT", "GRANTED TO ROLE")},
    )
    execute_and_fetch_all(cursor_main, "DENY DELETE TO role3")
    check(
        partial(show_privileges_func, user_or_role="role3"),
        {("CREATE", "GRANT", "GRANTED TO ROLE"), ("DELETE", "DENY", "DENIED TO ROLE")},
    )
    check(
        partial(show_privileges_func, user_or_role="user3b"),
        {("CREATE", "GRANT", "GRANTED TO ROLE"), ("DELETE", "DENY", "DENIED TO ROLE")},
    )

    # GRANT permission ON LABEL/EDGE to user/role
    execute_and_fetch_all(cursor_main, "REVOKE ALL PRIVILEGES FROM role3")
    execute_and_fetch_all(cursor_main, "REVOKE ALL PRIVILEGES FROM user4")
    execute_and_fetch_all(cursor_main, "REVOKE ALL PRIVILEGES FROM user3b")
    execute_and_fetch_all(cursor_main, "GRANT READ ON LABELS :l1 TO user4")
    execute_and_fetch_all(cursor_main, "GRANT UPDATE ON LABELS :l2, :l3 TO role3")
    check(
        partial(show_privileges_func, user_or_role="user4"),
        {
            ("LABEL :l1", "READ", "LABEL PERMISSION GRANTED TO USER"),
        },
    )
    check(
        partial(show_privileges_func, user_or_role="role3"),
        {
            ("LABEL :l3", "UPDATE", "LABEL PERMISSION GRANTED TO ROLE"),
            ("LABEL :l2", "UPDATE", "LABEL PERMISSION GRANTED TO ROLE"),
        },
    )
    check(
        partial(show_privileges_func, user_or_role="user3b"),
        {
            ("LABEL :l3", "UPDATE", "LABEL PERMISSION GRANTED TO ROLE"),
            ("LABEL :l2", "UPDATE", "LABEL PERMISSION GRANTED TO ROLE"),
        },
    )
    execute_and_fetch_all(cursor_main, "REVOKE LABELS :l1 FROM user4")
    execute_and_fetch_all(cursor_main, "REVOKE LABELS :l2 FROM role3")
    check(partial(show_privileges_func, user_or_role="user4"), set())
    check(
        partial(show_privileges_func, user_or_role="role3"),
        {("LABEL :l3", "UPDATE", "LABEL PERMISSION GRANTED TO ROLE")},
    )
    check(
        partial(show_privileges_func, user_or_role="user3b"),
        {("LABEL :l3", "UPDATE", "LABEL PERMISSION GRANTED TO ROLE")},
    )

    # GRANT/REVOKE DATABASE
    execute_and_fetch_all(cursor_main, "CREATE DATABASE auth_test")
    execute_and_fetch_all(cursor_main, "CREATE DATABASE auth_test2")
    execute_and_fetch_all(cursor_main, "GRANT DATABASE auth_test TO user4")
    check(partial(show_database_privileges_func, user="user4"), [(["auth_test", "memgraph"], [])])
    execute_and_fetch_all(cursor_main, "REVOKE DATABASE auth_test2 FROM user4")
    check(partial(show_database_privileges_func, user="user4"), [(["auth_test", "memgraph"], ["auth_test2"])])

    # SET MAIN DATABASE
    execute_and_fetch_all(cursor_main, "GRANT ALL PRIVILEGES TO user4")
    execute_and_fetch_all(cursor_main, "SET MAIN DATABASE auth_test FOR user4")
    # Reconnect and check current db
    assert (
        execute_and_fetch_all(connection(BOLT_PORTS["main"], "main", "user4").cursor(), "SHOW DATABASE")[0][0]
        == "auth_test"
    )
    assert (
        execute_and_fetch_all(connection(BOLT_PORTS["replica_1"], "replica", "user4").cursor(), "SHOW DATABASE")[0][0]
        == "auth_test"
    )
    assert (
        execute_and_fetch_all(connection(BOLT_PORTS["replica_2"], "replica", "user4").cursor(), "SHOW DATABASE")[0][0]
        == "auth_test"
    )


if __name__ == "__main__":
    interactive_mg_runner.cleanup_directories_on_exit()
    sys.exit(pytest.main([__file__, "-rA"]))
