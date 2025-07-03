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
from functools import partial

import interactive_mg_runner
import mgclient
import pytest
from common import *

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

BOLT_PORT = 7687

file = "memory"


@pytest.fixture
def test_name(request):
    return request.node.name


@pytest.fixture(autouse=True)
def cleanup_after_test():
    # Run the test
    yield
    # Stop + delete directories after running the test
    interactive_mg_runner.kill_all(keep_directories=False)


def add_user(cursor, username, password=None):
    if password is not None:
        return execute_and_fetch_all(cursor, f"CREATE USER {username} IDENTIFIED BY '{password}';")
    return execute_and_fetch_all(cursor, f"CREATE USER {username};")


def show_users_func(cursor):
    def func():
        return set(execute_and_fetch_all(cursor, "SHOW USERS;"))

    return func


def show_current_user_func(cursor):
    def func():
        return set(execute_and_fetch_all(cursor, "SHOW CURRENT USER;"))

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


def show_profiles_func(cursor):
    def func():
        return set(execute_and_fetch_all(cursor, "SHOW PROFILES;"))

    return func


def show_profile_func(cursor, profilename):
    def func():
        return set(execute_and_fetch_all(cursor, f"SHOW PROFILE {profilename};"))

    return func


def show_profile_for_user_func(cursor, username):
    def func():
        return set(execute_and_fetch_all(cursor, f"SHOW PROFILE FOR {username};"))

    return func


def show_users_for_profile_func(cursor, profilename):
    def func():
        return set(execute_and_fetch_all(cursor, f"SHOW USERS FOR PROFILE {profilename};"))

    return func


def show_roles_for_profile_func(cursor, profilename):
    def func():
        return set(execute_and_fetch_all(cursor, f"SHOW ROLES FOR PROFILE {profilename};"))

    return func


def show_resource_usage_func(cursor, user):
    def func():
        return set(execute_and_fetch_all(cursor, f"SHOW RESOURCE USAGE FOR {user};"))

    return func


def test_user_profile_setup(connection, test_name):
    # Goal: show that user profiles work correctly in a single instance

    MEMGRAPH_INSTANCES_DESCRIPTION = {
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORT}",
                "--log-level=TRACE",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "username": "user1",
            "setup_queries": [],
        },
    }

    # Start single instance
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=True)
    cursor_main = connection(BOLT_PORT, "main", "user1").cursor()

    def resource_check(username, expected_usage):
        usage = show_resource_usage_func(cursor_main, username)()
        assert expected_usage == usage

    # CREATE PROFILES
    execute_and_fetch_all(cursor_main, "CREATE PROFILE p1")
    execute_and_fetch_all(cursor_main, "CREATE PROFILE p2 LIMIT SESSIONS 1, TRANSACTIONS_MEMORY 100MB")

    assert {
        ("p2",),
        ("p1",),
    } == show_profiles_func(cursor_main)()

    assert {("sessions", "UNLIMITED"), ("transactions_memory", "UNLIMITED")} == show_profile_func(cursor_main, "p1")()
    assert {("sessions", 1), ("transactions_memory", "100MB")} == show_profile_func(cursor_main, "p2")()

    # UPDATE PROFILES
    execute_and_fetch_all(cursor_main, "UPDATE PROFILE p1 LIMIT SESSIONS 10, TRANSACTIONS_MEMORY 1000MB")
    execute_and_fetch_all(cursor_main, "UPDATE PROFILE p2 LIMIT SESSIONS 2, TRANSACTIONS_MEMORY 200MB")

    assert {("sessions", 10), ("transactions_memory", "1000MB")} == show_profile_func(cursor_main, "p1")()
    assert {("sessions", 2), ("transactions_memory", "200MB")} == show_profile_func(cursor_main, "p2")()

    # DROP PROFILES
    execute_and_fetch_all(cursor_main, "DROP PROFILE p1")
    assert {("p2",)} == show_profiles_func(cursor_main)()

    # CREATE USER
    execute_and_fetch_all(cursor_main, "CREATE USER user1")
    execute_and_fetch_all(cursor_main, "CREATE USER user2")
    execute_and_fetch_all(cursor_main, "GRANT PROFILE_RESTRICTION TO user2")
    execute_and_fetch_all(cursor_main, "GRANT AUTH TO user2")

    assert {
        ("user2",),
        ("user1",),
    } == show_users_func(cursor_main)()

    cursor_main = connection(BOLT_PORT, "main", "user1").cursor()
    resource_check("user1", {("transactions_memory", "0B", "UNLIMITED"), ("sessions", 1, "UNLIMITED")})
    resource_check("user2", {("transactions_memory", "0B", "UNLIMITED"), ("sessions", 0, "UNLIMITED")})

    # SET PROFILES
    execute_and_fetch_all(cursor_main, "SET PROFILE FOR user1 TO p2")
    execute_and_fetch_all(cursor_main, "SET PROFILE FOR user2 TO p2")

    assert {("p2",)} == show_profile_for_user_func(cursor_main, "user1")()
    assert {("p2",)} == show_profile_for_user_func(cursor_main, "user2")()
    assert {
        ("user2",),
        ("user1",),
    } == show_users_for_profile_func(cursor_main, "p2")()

    resource_check("user1", {("transactions_memory", "0B", "200.00MiB"), ("sessions", 1, 2)})
    resource_check("user2", {("transactions_memory", "0B", "200.00MiB"), ("sessions", 0, 2)})

    # CLEAR PROFILE
    execute_and_fetch_all(cursor_main, "CLEAR PROFILE FOR user1")

    assert {("null",)} == show_profile_for_user_func(cursor_main, "user1")()
    assert {("p2",)} == show_profile_for_user_func(cursor_main, "user2")()
    assert {("user2",)} == show_users_for_profile_func(cursor_main, "p2")()

    resource_check("user1", {("transactions_memory", "0B", "UNLIMITED"), ("sessions", 1, "UNLIMITED")})
    resource_check("user2", {("transactions_memory", "0B", "200.00MiB"), ("sessions", 0, 2)})

    # Restart instance and check values
    interactive_mg_runner.stop_all(keep_directories=True)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)
    cursor_main = connection(BOLT_PORT, "main", "user1").cursor()

    assert {("p2",)} == show_profiles_func(cursor_main)()
    assert {("sessions", 2), ("transactions_memory", "200MB")} == show_profile_func(cursor_main, "p2")()
    assert {("p2",)} == show_profile_for_user_func(cursor_main, "user2")()
    assert {("user2",)} == show_users_for_profile_func(cursor_main, "p2")()

    resource_check("user1", {("transactions_memory", "0B", "UNLIMITED"), ("sessions", 1, "UNLIMITED")})
    resource_check("user2", {("transactions_memory", "0B", "200.00MiB"), ("sessions", 0, 2)})

    # DROP USER WITH PROFILE
    execute_and_fetch_all(cursor_main, "DROP USER user2")
    assert set() == show_users_for_profile_func(cursor_main, "p2")()  # empty

    resource_check("user1", {("transactions_memory", "0B", "UNLIMITED"), ("sessions", 1, "UNLIMITED")})

    # DROP PROFILE WITH USER
    execute_and_fetch_all(cursor_main, "SET PROFILE FOR user1 TO p2")
    assert {("p2",)} == show_profile_for_user_func(cursor_main, "user1")()
    resource_check("user1", {("transactions_memory", "0B", "200.00MiB"), ("sessions", 1, 2)})

    execute_and_fetch_all(cursor_main, "DROP PROFILE p2")
    assert {("null",)} == show_profile_for_user_func(cursor_main, "user1")()
    assert set() == show_profiles_func(cursor_main)()  # empty
    resource_check("user1", {("transactions_memory", "0B", "UNLIMITED"), ("sessions", 1, "UNLIMITED")})

    # COMPREHENSIVE PROFILE COMBINATION TESTS
    # Test all possible combinations: no profile, user profile only, role profile only, both profiles

    # Create fresh profiles for testing
    execute_and_fetch_all(cursor_main, "CREATE PROFILE user_profile LIMIT SESSIONS 5, TRANSACTIONS_MEMORY 500MB")
    execute_and_fetch_all(cursor_main, "CREATE PROFILE role_profile LIMIT SESSIONS 10, TRANSACTIONS_MEMORY 1000MB")
    execute_and_fetch_all(cursor_main, "CREATE USER test_user")
    execute_and_fetch_all(cursor_main, "CREATE ROLE test_role")

    # Case 1: No profile (default/unlimited)
    assert {("null",)} == show_profile_for_user_func(cursor_main, "test_user")()
    resource_check("test_user", {("transactions_memory", "0B", "UNLIMITED"), ("sessions", 0, "UNLIMITED")})

    # Case 2: User profile only (no role)
    execute_and_fetch_all(cursor_main, "SET PROFILE FOR test_user TO user_profile")
    assert {("user_profile",)} == show_profile_for_user_func(cursor_main, "test_user")()
    resource_check("test_user", {("transactions_memory", "0B", "500.00MiB"), ("sessions", 0, 5)})

    # Case 3: Role profile only (no direct user profile)
    execute_and_fetch_all(cursor_main, "CLEAR PROFILE FOR test_user")
    execute_and_fetch_all(cursor_main, "SET ROLE FOR test_user TO test_role")
    execute_and_fetch_all(cursor_main, "SET PROFILE FOR test_role TO role_profile")
    assert {("null",)} == show_profile_for_user_func(cursor_main, "test_user")()  # No direct profile
    assert {("role_profile",)} == show_profile_for_user_func(cursor_main, "test_role")()  # Role has profile
    resource_check(
        "test_user", {("transactions_memory", "0B", "1000.00MiB"), ("sessions", 0, 10)}
    )  # Inherits from role

    # Case 4: Both user profile AND role profile (user profile should take precedence)
    execute_and_fetch_all(cursor_main, "SET PROFILE FOR test_user TO user_profile")
    assert {("user_profile",)} == show_profile_for_user_func(
        cursor_main, "test_user"
    )()  # Direct profile takes precedence
    assert {("role_profile",)} == show_profile_for_user_func(cursor_main, "test_role")()  # Role still has its profile
    resource_check(
        "test_user", {("transactions_memory", "0B", "500.00MiB"), ("sessions", 0, 5)}
    )  # Uses user profile, not role profile

    # Verify SHOW USERS FOR PROFILE and SHOW ROLES FOR PROFILE work correctly
    assert {("test_user",)} == show_users_for_profile_func(cursor_main, "user_profile")()
    assert set() == show_users_for_profile_func(cursor_main, "role_profile")()  # No users directly assigned
    assert {("test_role",)} == show_roles_for_profile_func(cursor_main, "role_profile")()
    assert set() == show_roles_for_profile_func(cursor_main, "user_profile")()  # No roles directly assigned

    # Test clearing user profile while role profile exists
    execute_and_fetch_all(cursor_main, "CLEAR PROFILE FOR test_user")
    assert {("null",)} == show_profile_for_user_func(cursor_main, "test_user")()  # No direct profile
    resource_check(
        "test_user", {("transactions_memory", "0B", "1000.00MiB"), ("sessions", 0, 10)}
    )  # Falls back to role profile

    # Test removing role while user has direct profile
    execute_and_fetch_all(cursor_main, "SET PROFILE FOR test_user TO user_profile")
    execute_and_fetch_all(cursor_main, "CLEAR ROLE FOR test_user")
    assert {("user_profile",)} == show_profile_for_user_func(cursor_main, "test_user")()  # Keeps direct profile
    resource_check("test_user", {("transactions_memory", "0B", "500.00MiB"), ("sessions", 0, 5)})  # Uses direct profile

    # Test DROP ROLE scenario - user should automatically lose role and role's profile
    execute_and_fetch_all(cursor_main, "CLEAR PROFILE FOR test_user")  # Remove direct profile
    execute_and_fetch_all(cursor_main, "SET ROLE FOR test_user TO test_role")  # Assign role again
    assert {("null",)} == show_profile_for_user_func(cursor_main, "test_user")()  # No direct profile
    assert {("role_profile",)} == show_profile_for_user_func(cursor_main, "test_role")()  # Role has profile
    resource_check(
        "test_user", {("transactions_memory", "0B", "1000.00MiB"), ("sessions", 0, 10)}
    )  # Inherits from role

    # Now drop the role - user should automatically lose the role and its profile
    execute_and_fetch_all(cursor_main, "DROP ROLE test_role")
    assert {("null",)} == show_role_for_user_func(cursor_main, "test_user")()  # User lost the role
    assert {("null",)} == show_profile_for_user_func(cursor_main, "test_user")()  # User lost the role's profile
    resource_check(
        "test_user", {("transactions_memory", "0B", "UNLIMITED"), ("sessions", 0, "UNLIMITED")}
    )  # Back to unlimited

    # Verify the role profile still exists but is no longer associated with any role
    assert {("role_profile",), ("user_profile",)} == show_profiles_func(cursor_main)()  # Profile still exists
    assert set() == show_roles_for_profile_func(cursor_main, "role_profile")()  # No roles associated

    # Test DROP PROFILE scenario - users and roles should automatically lose profile associations
    # Create fresh test objects for this scenario
    execute_and_fetch_all(cursor_main, "CREATE ROLE test_role2")
    execute_and_fetch_all(cursor_main, "CREATE USER test_user2")
    execute_and_fetch_all(cursor_main, "CREATE PROFILE shared_profile LIMIT SESSIONS 15, TRANSACTIONS_MEMORY 1500MB")
    assert {("role_profile",), ("user_profile",), ("shared_profile",)} == show_profiles_func(
        cursor_main
    )()  # Profile exists

    # Assign the profile to both user and role
    execute_and_fetch_all(cursor_main, "SET PROFILE FOR test_user2 TO shared_profile")
    execute_and_fetch_all(cursor_main, "SET PROFILE FOR test_role2 TO shared_profile")
    execute_and_fetch_all(cursor_main, "SET ROLE FOR test_user2 TO test_role2")

    # Verify initial state
    assert {("shared_profile",)} == show_profile_for_user_func(cursor_main, "test_user2")()  # Direct profile
    assert {("shared_profile",)} == show_profile_for_user_func(cursor_main, "test_role2")()  # Role profile
    assert {("test_user2",)} == show_users_for_profile_func(cursor_main, "shared_profile")()  # User assigned
    assert {("test_role2",)} == show_roles_for_profile_func(cursor_main, "shared_profile")()  # Role assigned
    resource_check("test_user2", {("transactions_memory", "0B", "1.46GiB"), ("sessions", 0, 15)})  # Uses direct profile

    # Now drop the profile - both user and role should automatically lose their profile associations
    execute_and_fetch_all(cursor_main, "DROP PROFILE shared_profile")

    # Verify user lost the profile
    assert {("null",)} == show_profile_for_user_func(cursor_main, "test_user2")()  # User lost direct profile
    resource_check(
        "test_user2", {("transactions_memory", "0B", "UNLIMITED"), ("sessions", 0, "UNLIMITED")}
    )  # Back to unlimited

    # Verify role lost the profile
    assert {("null",)} == show_profile_for_user_func(cursor_main, "test_role2")()  # Role lost profile

    # Verify profile no longer exists
    assert {("role_profile",), ("user_profile",)} == show_profiles_func(cursor_main)()  # Profile was dropped

    # Test DROP PROFILE with role inheritance scenario
    execute_and_fetch_all(cursor_main, "CREATE PROFILE role_only_profile LIMIT SESSIONS 20, TRANSACTIONS_MEMORY 2000MB")
    execute_and_fetch_all(cursor_main, "SET PROFILE FOR test_role2 TO role_only_profile")

    # User should inherit from role (no direct profile)
    assert {("null",)} == show_profile_for_user_func(cursor_main, "test_user2")()  # No direct profile
    assert {("role_only_profile",)} == show_profile_for_user_func(cursor_main, "test_role2")()  # Role has profile
    resource_check("test_user2", {("transactions_memory", "0B", "1.95GiB"), ("sessions", 0, 20)})  # Inherits from role

    # Drop the role-only profile
    execute_and_fetch_all(cursor_main, "DROP PROFILE role_only_profile")

    # Verify role lost the profile
    assert {("null",)} == show_profile_for_user_func(cursor_main, "test_role2")()  # Role lost profile

    # Verify user lost inherited profile
    assert {("null",)} == show_profile_for_user_func(cursor_main, "test_user2")()  # No direct profile
    resource_check(
        "test_user2", {("transactions_memory", "0B", "UNLIMITED"), ("sessions", 0, "UNLIMITED")}
    )  # Back to unlimited

    # Test edge case: User and role both have the same profile
    execute_and_fetch_all(cursor_main, "CREATE PROFILE same_profile LIMIT SESSIONS 25, TRANSACTIONS_MEMORY 2500MB")
    execute_and_fetch_all(cursor_main, "CREATE USER test_user3")
    execute_and_fetch_all(cursor_main, "CREATE ROLE test_role3")

    # Assign the same profile to both user and role
    execute_and_fetch_all(cursor_main, "SET PROFILE FOR test_user3 TO same_profile")
    execute_and_fetch_all(cursor_main, "SET PROFILE FOR test_role3 TO same_profile")
    execute_and_fetch_all(cursor_main, "SET ROLE FOR test_user3 TO test_role3")

    # Verify initial state - user should show direct profile (precedence)
    assert {("same_profile",)} == show_profile_for_user_func(
        cursor_main, "test_user3"
    )()  # Direct profile takes precedence
    assert {("same_profile",)} == show_profile_for_user_func(cursor_main, "test_role3")()  # Role has same profile
    assert {("test_user3",)} == show_users_for_profile_func(cursor_main, "same_profile")()  # User assigned
    assert {("test_role3",)} == show_roles_for_profile_func(cursor_main, "same_profile")()  # Role assigned
    resource_check("test_user3", {("transactions_memory", "0B", "2.44GiB"), ("sessions", 0, 25)})  # Uses direct profile

    # Test that precedence still works - user profile takes priority even if same as role
    execute_and_fetch_all(cursor_main, "UPDATE PROFILE same_profile LIMIT SESSIONS 30, TRANSACTIONS_MEMORY 3000MB")
    assert {("sessions", 30), ("transactions_memory", "3000MB")} == show_profile_func(cursor_main, "same_profile")()
    resource_check(
        "test_user3", {("transactions_memory", "0B", "2.93GiB"), ("sessions", 0, 30)}
    )  # User gets updated limits

    # Now drop the profile - both user and role should lose their associations
    execute_and_fetch_all(cursor_main, "DROP PROFILE same_profile")

    # Verify both user and role lost the profile
    assert {("null",)} == show_profile_for_user_func(cursor_main, "test_user3")()  # User lost profile
    assert {("null",)} == show_profile_for_user_func(cursor_main, "test_role3")()  # Role lost profile
    resource_check(
        "test_user3", {("transactions_memory", "0B", "UNLIMITED"), ("sessions", 0, "UNLIMITED")}
    )  # Back to unlimited

    # Verify profile no longer exists
    assert {("role_profile",), ("user_profile",)} == show_profiles_func(cursor_main)()  # Profile was dropped

    # Clean up test objects
    execute_and_fetch_all(cursor_main, "DROP USER user1")
    execute_and_fetch_all(cursor_main, "DROP USER test_user")
    execute_and_fetch_all(cursor_main, "DROP USER test_user2")
    execute_and_fetch_all(cursor_main, "DROP USER test_user3")
    execute_and_fetch_all(cursor_main, "DROP ROLE test_role2")
    execute_and_fetch_all(cursor_main, "DROP ROLE test_role3")
    execute_and_fetch_all(cursor_main, "DROP PROFILE user_profile")
    execute_and_fetch_all(cursor_main, "DROP PROFILE role_profile")


def test_user_profile_session_tracking(connection, test_name):
    # TEST SESSION AND TRANSACTION MEMORY TRACKING
    MEMGRAPH_INSTANCES_DESCRIPTION = {
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORT}",
                "--log-level=TRACE",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "username": "user1",
            "setup_queries": [],
        },
    }

    # Start single instance
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)
    cursor_main = connection(BOLT_PORT, "main", "user1").cursor()

    # Create users to test resource tracking
    execute_and_fetch_all(cursor_main, "CREATE USER tracking_user1")
    execute_and_fetch_all(cursor_main, "GRANT MATCH, CREATE TO tracking_user1")
    execute_and_fetch_all(cursor_main, "CREATE USER tracking_user2")
    execute_and_fetch_all(cursor_main, "GRANT MATCH, CREATE TO tracking_user2")

    def resource_check(username, expected_usage):
        usage = show_resource_usage_func(cursor_main, username)()
        assert expected_usage == usage

    # Initial state - no sessions, no memory usage
    resource_check("tracking_user1", {("transactions_memory", "0B", "UNLIMITED"), ("sessions", 0, "UNLIMITED")})
    resource_check("tracking_user2", {("transactions_memory", "0B", "UNLIMITED"), ("sessions", 0, "UNLIMITED")})

    # Simulate session creation for tracking_user1
    cursor_user1 = connection(BOLT_PORT, "main", "tracking_user1").cursor()

    # Check that session count increased
    resource_check("tracking_user1", {("transactions_memory", "0B", "UNLIMITED"), ("sessions", 1, "UNLIMITED")})
    resource_check("tracking_user2", {("transactions_memory", "0B", "UNLIMITED"), ("sessions", 0, "UNLIMITED")})

    # Create another session for the same user
    cursor_user1_2 = connection(BOLT_PORT, "main", "tracking_user1").cursor()

    # Check that session count increased to 2
    resource_check("tracking_user1", {("transactions_memory", "0B", "UNLIMITED"), ("sessions", 2, "UNLIMITED")})

    # Simulate session creation for tracking_user2
    cursor_user2 = connection(BOLT_PORT, "main", "tracking_user2").cursor()

    # Check that tracking_user2 now has 1 session while tracking_user1 still has 2
    resource_check("tracking_user1", {("transactions_memory", "0B", "UNLIMITED"), ("sessions", 2, "UNLIMITED")})
    resource_check("tracking_user2", {("transactions_memory", "0B", "UNLIMITED"), ("sessions", 1, "UNLIMITED")})

    # Close one session for tracking_user1 and verify session count decreases
    cursor_user1.close()

    # Check that session count decreased to 1
    resource_check("tracking_user1", {("transactions_memory", "0B", "UNLIMITED"), ("sessions", 1, "UNLIMITED")})
    resource_check("tracking_user2", {("transactions_memory", "0B", "UNLIMITED"), ("sessions", 1, "UNLIMITED")})

    # Clean up test users
    execute_and_fetch_all(cursor_main, "DROP USER tracking_user1")
    execute_and_fetch_all(cursor_main, "DROP USER tracking_user2")

    # NOTE: Leave this at the end; user1 needs to be the last connection (quick in the conftest)
    connection(BOLT_PORT, "main", "user1").cursor()


def test_user_profile_explicit_tx_memory_tracking(connection, test_name):
    # TEST SESSION AND TRANSACTION MEMORY TRACKING
    MEMGRAPH_INSTANCES_DESCRIPTION = {
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORT}",
                "--log-level=TRACE",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "username": "user1",
            "setup_queries": [],
        },
    }

    # Start single instance
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)
    cursor_main = connection(BOLT_PORT, "main", "user1").cursor()

    # Create users to test resource tracking
    execute_and_fetch_all(cursor_main, "CREATE USER tracking_user1")
    execute_and_fetch_all(cursor_main, "GRANT MATCH, CREATE TO tracking_user1")
    execute_and_fetch_all(cursor_main, "CREATE USER tracking_user2")
    execute_and_fetch_all(cursor_main, "GRANT MATCH, CREATE TO tracking_user2")

    def resource_check(username, expected_usage):
        usage = show_resource_usage_func(cursor_main, username)()
        assert expected_usage == usage

    # Test explicit transaction memory tracking
    print("Testing explicit transaction memory tracking...")

    # Create a profile with memory limit for testing
    execute_and_fetch_all(cursor_main, "CREATE PROFILE memory_test_profile LIMIT TRANSACTIONS_MEMORY 50MB")
    execute_and_fetch_all(cursor_main, "SET PROFILE FOR tracking_user1 TO memory_test_profile")

    # Test single explicit transaction
    connection_user1_tx = connect_no_autocommit(
        host="localhost", port=BOLT_PORT, username="tracking_user1", password=""
    )

    # Check initial memory usage
    resource_check("tracking_user1", {("transactions_memory", "0B", "50.00MiB"), ("sessions", 1, "UNLIMITED")})

    # Start explicit transaction and execute memory-intensive query
    execute_and_fetch_all(connection_user1_tx.cursor(), "CREATE (n:Test {prop: 'value'})")
    execute_and_fetch_all(connection_user1_tx.cursor(), "MATCH (n:Test) RETURN count(n)")

    # Check memory usage increased
    result = execute_and_fetch_all(cursor_main, "SHOW RESOURCE USAGE FOR tracking_user1")
    memory_usage = None
    for row in result:
        if row[0] == "transactions_memory":
            memory_usage = row[1]
            break
    assert memory_usage != "0B", f"Memory usage should increase, got: {memory_usage}"

    # Commit transaction
    connection_user1_tx.commit()

    # Check memory usage after commit (should reset or decrease)
    resource_check("tracking_user1", {("transactions_memory", "0B", "50.00MiB"), ("sessions", 1, "UNLIMITED")})

    # Test memory limit enforcement
    connection_user1_limit = connect_no_autocommit(
        host="localhost", port=BOLT_PORT, username="tracking_user1", password=""
    )

    # Try to exceed memory limit with large data creation
    try:
        # Create a large number of nodes to exceed 50MB limit
        for i in range(10000):
            execute_and_fetch_all(
                connection_user1_limit.cursor(), f"CREATE (n:BigNode {{id: {i}, data: '{'x' * 1000}'}})"
            )
        # If we get here, the limit wasn't enforced
        assert False, "Memory limit should have been exceeded and transaction aborted"
    except Exception as e:
        # Expected behavior - transaction should be aborted due to memory limit
        print(f"Expected memory limit enforcement: {e}")
        connection_user1_limit.rollback()

    # Test multiple explicit transactions by same user
    connection_user1_multi1 = connect_no_autocommit(
        host="localhost", port=BOLT_PORT, username="tracking_user1", password=""
    )
    connection_user1_multi2 = connect_no_autocommit(
        host="localhost", port=BOLT_PORT, username="tracking_user1", password=""
    )

    # Execute transactions in parallel
    execute_and_fetch_all(connection_user1_multi1.cursor(), "CREATE (n:Multi1 {id: 1})")
    execute_and_fetch_all(connection_user1_multi2.cursor(), "CREATE (n:Multi2 {id: 2})")

    # Check combined memory usage
    result = execute_and_fetch_all(cursor_main, "SHOW RESOURCE USAGE FOR tracking_user1")
    memory_usage = None
    for row in result:
        if row[0] == "transactions_memory":
            memory_usage = row[1]
            break
    assert memory_usage != "0B", f"Combined memory usage should be tracked, got: {memory_usage}"

    # Commit both transactions
    connection_user1_multi1.commit()
    connection_user1_multi2.commit()

    # Check memory usage after both commits
    resource_check("tracking_user1", {("transactions_memory", "0B", "50.00MiB"), ("sessions", 4, "UNLIMITED")})

    # Clean up
    connection_user1_tx.close()
    connection_user1_limit.close()
    connection_user1_multi1.close()
    connection_user1_multi2.close()

    # Remove test profile
    execute_and_fetch_all(cursor_main, "DROP PROFILE memory_test_profile")
    # Clean up test users
    execute_and_fetch_all(cursor_main, "DROP USER tracking_user1")
    execute_and_fetch_all(cursor_main, "DROP USER tracking_user2")

    print("Explicit transaction memory tracking tests completed successfully")

    # NOTE: Leave this at the end; user1 needs to be the last connection (quick in the conftest)
    connection(BOLT_PORT, "main", "user1").cursor()


def test_user_profile_implicit_tx_memory_tracking(connection, test_name):
    # TEST SESSION AND TRANSACTION MEMORY TRACKING
    MEMGRAPH_INSTANCES_DESCRIPTION = {
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORT}",
                "--log-level=TRACE",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "username": "user1",
            "setup_queries": [],
        },
    }

    # Start single instance
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)
    cursor_main = connection(BOLT_PORT, "main", "user1").cursor()

    # Create users to test resource tracking
    execute_and_fetch_all(cursor_main, "CREATE USER tracking_user1")
    execute_and_fetch_all(cursor_main, "GRANT MATCH, CREATE TO tracking_user1")
    execute_and_fetch_all(cursor_main, "CREATE USER tracking_user2")
    execute_and_fetch_all(cursor_main, "GRANT MATCH, CREATE TO tracking_user2")

    def resource_check(username, expected_usage):
        usage = show_resource_usage_func(cursor_main, username)()
        assert expected_usage == usage

    # Test implicit transaction memory tracking
    print("Testing implicit transaction memory tracking...")

    # Create a profile with memory limit for implicit transactions
    execute_and_fetch_all(cursor_main, "CREATE PROFILE implicit_test_profile LIMIT TRANSACTIONS_MEMORY 100MB")
    execute_and_fetch_all(cursor_main, "SET PROFILE FOR tracking_user1 TO implicit_test_profile")

    # Connect with autocommit (implicit transactions)
    connection_user1_implicit = connect(host="localhost", port=BOLT_PORT, username="tracking_user1", password="")
    cursor_user1_implicit = connection_user1_implicit.cursor()

    # Test that implicit transactions are tracked and limited
    # Create a large dataset that should exceed the 1MB limit
    exceptions_count = 0
    try:
        execute_and_fetch_all(cursor_user1_implicit, "UNWIND RANGE(1,1000000) AS i CREATE (n:Node {i:i})")
    except Exception as e:
        exceptions_count += 1
        # Should get memory limit exceeded error
        assert "memory" in str(e).lower() or "limit" in str(e).lower()

    # Should have hit the limit at some point
    assert exceptions_count > 0, "Memory limit should have been exceeded for implicit transactions"

    # Clean up
    connection_user1_implicit.close()
    execute_and_fetch_all(cursor_main, "DROP PROFILE implicit_test_profile")
    # Clean up test users
    execute_and_fetch_all(cursor_main, "DROP USER tracking_user1")
    execute_and_fetch_all(cursor_main, "DROP USER tracking_user2")

    print("Implicit transaction memory tracking tests completed successfully")


def test_user_profile_proc_memory_tracking(connection, test_name):
    # TEST SESSION AND TRANSACTION MEMORY TRACKING
    MEMGRAPH_INSTANCES_DESCRIPTION = {
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORT}",
                "--log-level=TRACE",
                "--query-modules-directory",
                f"{interactive_mg_runner.BUILD_DIR}/tests/e2e/memory/procedures/",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "username": "user1",
            "setup_queries": [],
        },
    }

    # Start single instance
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)
    cursor_main = connection(BOLT_PORT, "main", "user1").cursor()

    # Create users to test resource tracking
    execute_and_fetch_all(cursor_main, "CREATE USER tracking_user1")
    execute_and_fetch_all(cursor_main, "GRANT MATCH, CREATE TO tracking_user1")
    execute_and_fetch_all(cursor_main, "CREATE USER tracking_user2")
    execute_and_fetch_all(cursor_main, "GRANT MATCH, CREATE TO tracking_user2")

    def resource_check(username, expected_usage):
        usage = show_resource_usage_func(cursor_main, username)()
        assert expected_usage == usage

    # Test procedure memory tracking with different allocation methods
    # Create a profile with 250MB memory limit
    execute_and_fetch_all(cursor_main, "CREATE PROFILE proc_test_profile LIMIT TRANSACTIONS_MEMORY 250MB")
    execute_and_fetch_all(cursor_main, "SET PROFILE FOR tracking_user1 TO proc_test_profile")

    # Connect as tracking_user1
    connection_user1 = connect(host="localhost", port=BOLT_PORT, username="tracking_user1", password="")
    cursor_user1 = connection_user1.cursor()

    # Test local_heap() - should fail with 250MB limit
    exceptions_count = 0
    try:
        execute_and_fetch_all(cursor_user1, "CALL libquery_memory_limit_proc.local_heap() YIELD allocated RETURN *;")
    except Exception as e:
        exceptions_count += 1
        assert "user memory limit exceeded!" in str(e).lower()

    assert exceptions_count > 0, "local_heap() should have exceeded 250MB memory limit"

    # Test new() - should fail with 250MB limit
    exceptions_count = 0
    try:
        execute_and_fetch_all(cursor_user1, "CALL libquery_memory_limit_proc.new() YIELD allocated RETURN *;")
    except Exception as e:
        exceptions_count += 1
        assert "user memory limit exceeded!" in str(e).lower()

    assert exceptions_count > 0, "new() should have exceeded 250MB memory limit"

    # Test malloc() - should fail with 250MB limit but return false instead of throwing
    result = execute_and_fetch_all(cursor_user1, "CALL libquery_memory_limit_proc.malloc() YIELD allocated RETURN *;")
    assert len(result) > 0, "malloc() should return a result"
    assert result[0][0] == False, "malloc() should return false when memory limit is exceeded"

    # Test regular() - should fail with 250MB limit but return false instead of throwing
    result = execute_and_fetch_all(cursor_user1, "CALL libquery_memory_limit_proc.regular() YIELD allocated RETURN *;")
    assert len(result) > 0, "regular() should return a result"
    assert result[0][0] == False, "regular() should return false when memory limit is exceeded"

    # Update profile with 350MB memory limit - should succeed
    execute_and_fetch_all(cursor_main, "UPDATE PROFILE proc_test_profile LIMIT TRANSACTIONS_MEMORY 350MB")

    # Test local_heap() - should succeed with 350MB limit
    try:
        result = execute_and_fetch_all(
            cursor_user1, "CALL libquery_memory_limit_proc.local_heap() YIELD allocated RETURN *;"
        )
        assert len(result) > 0, "local_heap() should succeed with 350MB limit"
    except Exception as e:
        assert False, f"local_heap() should not fail with 350MB limit: {e}"

    # Test new() - should succeed with 350MB limit
    try:
        result = execute_and_fetch_all(cursor_user1, "CALL libquery_memory_limit_proc.new() YIELD allocated RETURN *;")
        assert len(result) > 0, "new() should succeed with 350MB limit"
    except Exception as e:
        assert False, f"new() should not fail with 350MB limit: {e}"

    # Test malloc() - should succeed with 350MB limit
    result = execute_and_fetch_all(cursor_user1, "CALL libquery_memory_limit_proc.malloc() YIELD allocated RETURN *;")
    assert len(result) > 0, "malloc() should return a result"
    assert result[0][0], "malloc() should return true when memory limit is not exceeded"

    # Test regular() - should succeed with 350MB limit
    result = execute_and_fetch_all(cursor_user1, "CALL libquery_memory_limit_proc.regular() YIELD allocated RETURN *;")
    assert len(result) > 0, "regular() should return a result"
    assert result[0][0], "regular() should return true when memory limit is not exceeded"

    # Clean up
    execute_and_fetch_all(cursor_main, "DROP PROFILE proc_test_profile")
    # Clean up test users
    execute_and_fetch_all(cursor_main, "DROP USER tracking_user1")
    execute_and_fetch_all(cursor_main, "DROP USER tracking_user2")

    print("Procedure memory tracking tests completed successfully")


def test_user_profile_local_proc_memory_tracking(connection, test_name):
    # TEST SESSION AND TRANSACTION MEMORY TRACKING
    MEMGRAPH_INSTANCES_DESCRIPTION = {
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORT}",
                "--log-level=TRACE",
                "--query-modules-directory",
                f"{interactive_mg_runner.BUILD_DIR}/tests/e2e/memory/procedures/",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "username": "user1",
            "setup_queries": [],
        },
    }

    # Start single instance
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)
    cursor_main = connection(BOLT_PORT, "main", "user1").cursor()

    # Create users to test resource tracking
    execute_and_fetch_all(cursor_main, "CREATE USER tracking_user1")
    execute_and_fetch_all(cursor_main, "GRANT MATCH, CREATE TO tracking_user1")
    execute_and_fetch_all(cursor_main, "CREATE USER tracking_user2")
    execute_and_fetch_all(cursor_main, "GRANT MATCH, CREATE TO tracking_user2")

    def resource_check(username, expected_usage):
        usage = show_resource_usage_func(cursor_main, username)()
        assert expected_usage == usage

    # Test procedure memory tracking with different allocation methods
    # Create a profile with 250MB memory limit
    execute_and_fetch_all(cursor_main, "CREATE PROFILE proc_test_profile LIMIT TRANSACTIONS_MEMORY 250MB")
    execute_and_fetch_all(cursor_main, "SET PROFILE FOR tracking_user1 TO proc_test_profile")

    # Connect as tracking_user1
    connection_user1 = connect(host="localhost", port=BOLT_PORT, username="tracking_user1", password="")
    cursor_user1 = connection_user1.cursor()

    # Test local_heap() - should fail with 250MB limit
    exceptions_count = 0
    try:
        execute_and_fetch_all(cursor_user1, "CALL libproc_memory_limit.local_heap_256_mib() YIELD allocated RETURN *;")
    except Exception as e:
        exceptions_count += 1
        assert "user memory limit exceeded!" in str(e).lower()

    assert exceptions_count > 0, "local_heap() should have exceeded 250MB memory limit"

    # Test new() - should fail with 250MB limit
    exceptions_count = 0
    try:
        execute_and_fetch_all(cursor_user1, "CALL libproc_memory_limit.new_256_mib() YIELD allocated RETURN *;")
    except Exception as e:
        exceptions_count += 1
        assert "user memory limit exceeded!" in str(e).lower()

    assert exceptions_count > 0, "new() should have exceeded 250MB memory limit"

    # Test malloc() - should fail with 250MB limit but return false instead of throwing
    result = execute_and_fetch_all(cursor_user1, "CALL libproc_memory_limit.malloc_256_mib() YIELD allocated RETURN *;")
    assert len(result) > 0, "malloc() should return a result"
    assert result[0][0] == False, "malloc() should return false when memory limit is exceeded"

    # Test alloc() - should fail with 250MB limit but return false instead of throwing
    result = execute_and_fetch_all(cursor_user1, "CALL libproc_memory_limit.alloc_256_mib() YIELD allocated RETURN *;")
    assert len(result) > 0, "alloc() should return a result"
    assert result[0][0] == False, "alloc() should return false when memory limit is exceeded"

    # Update profile with 350MB memory limit - should succeed
    execute_and_fetch_all(cursor_main, "UPDATE PROFILE proc_test_profile LIMIT TRANSACTIONS_MEMORY 350MB")

    # Test local_heap() - should succeed with 350MB limit
    try:
        result = execute_and_fetch_all(
            cursor_user1, "CALL libproc_memory_limit.local_heap_256_mib() YIELD allocated RETURN *;"
        )
        assert len(result) > 0, "local_heap() should succeed with 350MB limit"
    except Exception as e:
        assert False, f"local_heap() should not fail with 350MB limit: {e}"

    # Test new() - should succeed with 350MB limit
    try:
        result = execute_and_fetch_all(
            cursor_user1, "CALL libproc_memory_limit.new_256_mib() YIELD allocated RETURN *;"
        )
        assert len(result) > 0, "new() should succeed with 350MB limit"
    except Exception as e:
        assert False, f"new() should not fail with 350MB limit: {e}"

    # Test malloc() - should succeed with 350MB limit
    result = execute_and_fetch_all(cursor_user1, "CALL libproc_memory_limit.malloc_256_mib() YIELD allocated RETURN *;")
    assert len(result) > 0, "malloc() should return a result"
    assert result[0][0], "malloc() should return true when memory limit is not exceeded"

    # Test alloc() - should succeed with 350MB limit
    result = execute_and_fetch_all(cursor_user1, "CALL libproc_memory_limit.alloc_256_mib() YIELD allocated RETURN *;")
    assert len(result) > 0, "alloc() should return a result"
    assert result[0][0], "alloc() should return true when memory limit is not exceeded"

    # Clean up
    execute_and_fetch_all(cursor_main, "DROP PROFILE proc_test_profile")
    # Clean up test users
    execute_and_fetch_all(cursor_main, "DROP USER tracking_user1")
    execute_and_fetch_all(cursor_main, "DROP USER tracking_user2")

    print("Procedure memory tracking tests completed successfully")

    # NOTE: Leave this at the end; user1 needs to be the last connection (quick in the conftest)
    connection(BOLT_PORT, "main", "user1").cursor()


def test_user_profile_proc_w_limit_memory_tracking(connection, test_name):
    # TEST SESSION AND TRANSACTION MEMORY TRACKING
    MEMGRAPH_INSTANCES_DESCRIPTION = {
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORT}",
                "--log-level=TRACE",
                "--query-modules-directory",
                f"{interactive_mg_runner.BUILD_DIR}/tests/e2e/memory/procedures/",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "username": "user1",
            "setup_queries": [],
        },
    }

    # Start single instance
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)
    cursor_main = connection(BOLT_PORT, "main", "user1").cursor()

    # Create users to test resource tracking
    execute_and_fetch_all(cursor_main, "CREATE USER tracking_user1")
    execute_and_fetch_all(cursor_main, "GRANT MATCH, CREATE TO tracking_user1")
    execute_and_fetch_all(cursor_main, "CREATE USER tracking_user2")
    execute_and_fetch_all(cursor_main, "GRANT MATCH, CREATE TO tracking_user2")

    def resource_check(username, expected_usage):
        usage = show_resource_usage_func(cursor_main, username)()
        assert expected_usage == usage

    # Test procedure memory tracking with different allocation methods
    # Create a profile with 250MB memory limit
    execute_and_fetch_all(cursor_main, "CREATE PROFILE proc_test_profile LIMIT TRANSACTIONS_MEMORY 350MB")
    execute_and_fetch_all(cursor_main, "SET PROFILE FOR tracking_user1 TO proc_test_profile")

    # Connect as tracking_user1
    connection_user1 = connect(host="localhost", port=BOLT_PORT, username="tracking_user1", password="")
    cursor_user1 = connection_user1.cursor()

    # Test that query memory limit and user memory limit are independent
    exceptions_count = 0
    try:
        execute_and_fetch_all(
            cursor_user1,
            "CALL libproc_memory_limit.local_heap_256_mib() YIELD allocated RETURN * QUERY MEMORY LIMIT 250MB;",
        )
    except Exception as e:
        exceptions_count += 1
        assert str(e).lower().startswith("libproc_memory_limit.local_heap_256_mib: memory limit exceeded!")

    assert exceptions_count > 0, "local_heap() should have exceeded 250MB memory limit"

    resource_check("tracking_user1", {("transactions_memory", "0B", "350.00MiB"), ("sessions", 1, "UNLIMITED")})

    execute_and_fetch_all(cursor_main, "UPDATE PROFILE proc_test_profile LIMIT TRANSACTIONS_MEMORY 250MB")
    exceptions_count = 0
    try:
        execute_and_fetch_all(
            cursor_user1,
            "CALL libproc_memory_limit.local_heap_256_mib() YIELD allocated RETURN * QUERY MEMORY LIMIT 350MB;",
        )
    except Exception as e:
        exceptions_count += 1
        assert "user memory limit exceeded!" in str(e).lower()

    assert exceptions_count > 0, "local_heap() should have exceeded 250MB memory limit"

    resource_check("tracking_user1", {("transactions_memory", "0B", "250.00MiB"), ("sessions", 1, "UNLIMITED")})

    # Clean up
    execute_and_fetch_all(cursor_main, "DROP PROFILE proc_test_profile")
    # Clean up test users
    execute_and_fetch_all(cursor_main, "DROP USER tracking_user1")
    execute_and_fetch_all(cursor_main, "DROP USER tracking_user2")

    print("Procedure memory tracking with query memory limit tests completed successfully")

    # NOTE: Leave this at the end; user1 needs to be the last connection (quick in the conftest)
    connection(BOLT_PORT, "main", "user1").cursor()


def test_user_profile_explicit_implicit_tx_memory_tracking(connection, test_name):
    # TEST SESSION AND TRANSACTION MEMORY TRACKING
    MEMGRAPH_INSTANCES_DESCRIPTION = {
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORT}",
                "--log-level=TRACE",
                "--query-modules-directory",
                f"{interactive_mg_runner.BUILD_DIR}/tests/e2e/memory/procedures/",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "username": "user1",
            "setup_queries": [],
        },
    }

    # Start single instance
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)
    cursor_main = connection(BOLT_PORT, "main", "user1").cursor()

    # Create users to test resource tracking
    execute_and_fetch_all(cursor_main, "CREATE USER tracking_user1")
    execute_and_fetch_all(cursor_main, "GRANT MATCH, CREATE TO tracking_user1")
    execute_and_fetch_all(cursor_main, "CREATE USER tracking_user2")
    execute_and_fetch_all(cursor_main, "GRANT MATCH, CREATE TO tracking_user2")

    def resource_check(username, expected_usage):
        usage = show_resource_usage_func(cursor_main, username)()
        assert expected_usage == usage

    # Test procedure memory tracking with different allocation methods
    # Create a profile with 300MB memory limit
    execute_and_fetch_all(cursor_main, "CREATE PROFILE proc_test_profile LIMIT TRANSACTIONS_MEMORY 300MB")
    execute_and_fetch_all(cursor_main, "SET PROFILE FOR tracking_user1 TO proc_test_profile")

    # Connect as tracking_user1
    connection_user1_explicit = connect_no_autocommit(
        host="localhost", port=BOLT_PORT, username="tracking_user1", password=""
    )
    cursor_user1_explicit = connection_user1_explicit.cursor()
    connection_user1_implicit = connect(host="localhost", port=BOLT_PORT, username="tracking_user1", password="")
    cursor_user1_implicit = connection_user1_implicit.cursor()

    # Test that explicit and implicit transactions are tracked under the same user
    execute_and_fetch_all(
        cursor_user1_explicit, "UNWIND RANGE(1,200000) AS i CREATE (n:Node {i:i})"
    )  # Should be around 100MB

    exceptions_count = 0
    try:
        execute_and_fetch_all(
            cursor_user1_implicit, "CALL libproc_memory_limit.new_256_mib() YIELD allocated RETURN *;"
        )
    except Exception as e:
        exceptions_count += 1
        assert "user memory limit exceeded!" in str(e).lower()

    assert exceptions_count > 0, "new() should have exceeded 300MB memory limit"

    connection_user1_explicit.commit()

    resource_check("tracking_user1", {("transactions_memory", "0B", "300.00MiB"), ("sessions", 2, "UNLIMITED")})

    # Clean up
    execute_and_fetch_all(cursor_main, "DROP PROFILE proc_test_profile")
    # Clean up test users
    execute_and_fetch_all(cursor_main, "DROP USER tracking_user1")
    execute_and_fetch_all(cursor_main, "DROP USER tracking_user2")

    print("Procedure memory tracking with query memory limit tests completed successfully")

    # NOTE: Leave this at the end; user1 needs to be the last connection (quick in the conftest)
    connection(BOLT_PORT, "main", "user1").cursor()


def test_user_profile_memory_tracking_with_gc(connection, test_name):
    # TEST SESSION AND TRANSACTION MEMORY TRACKING
    MEMGRAPH_INSTANCES_DESCRIPTION = {
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORT}",
                "--log-level=TRACE",
                "--query-modules-directory",
                f"{interactive_mg_runner.BUILD_DIR}/tests/e2e/memory/procedures/",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "username": "user1",
            "setup_queries": [],
        },
    }

    # Start single instance
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)
    cursor_main = connection(BOLT_PORT, "main", "user1").cursor()

    # Create users to test resource tracking
    execute_and_fetch_all(cursor_main, "CREATE USER tracking_user1")
    execute_and_fetch_all(cursor_main, "GRANT MATCH, CREATE TO tracking_user1")
    execute_and_fetch_all(cursor_main, "CREATE USER tracking_user2")
    execute_and_fetch_all(cursor_main, "GRANT MATCH, CREATE TO tracking_user2")

    def resource_check(username, expected_usage):
        usage = show_resource_usage_func(cursor_main, username)()
        assert expected_usage == usage

    # Test procedure memory tracking with different allocation methods
    # Create a profile with 300MB memory limit
    execute_and_fetch_all(cursor_main, "CREATE PROFILE proc_test_profile LIMIT TRANSACTIONS_MEMORY 300MB")
    execute_and_fetch_all(cursor_main, "SET PROFILE FOR tracking_user1 TO proc_test_profile")

    # Connect as tracking_user1
    connection_user1_explicit = connect_no_autocommit(
        host="localhost", port=BOLT_PORT, username="tracking_user1", password=""
    )
    cursor_user1_explicit = connection_user1_explicit.cursor()
    connection_user1_implicit = connect(host="localhost", port=BOLT_PORT, username="tracking_user1", password="")
    cursor_user1_implicit = connection_user1_implicit.cursor()

    # Generate a lot of data and then abort
    execute_and_fetch_all(
        cursor_user1_explicit, "UNWIND RANGE(1,200000) AS i CREATE (n:Node {i:i})"
    )  # Should be around 100MB
    connection_user1_explicit.rollback()
    resource_check("tracking_user1", {("transactions_memory", "0B", "300.00MiB"), ("sessions", 2, "UNLIMITED")})

    # Make sure the next call that triggers gc via ~accessor will ignore the deletions
    execute_and_fetch_all(cursor_user1_implicit, "UNWIND RANGE(1,200000) AS i CREATE (n:Node {i:i})")
    resource_check("tracking_user1", {("transactions_memory", "0B", "300.00MiB"), ("sessions", 2, "UNLIMITED")})

    # Clean up
    execute_and_fetch_all(cursor_main, "DROP PROFILE proc_test_profile")
    # Clean up test users
    execute_and_fetch_all(cursor_main, "DROP USER tracking_user1")
    execute_and_fetch_all(cursor_main, "DROP USER tracking_user2")

    print("Procedure memory tracking with query memory limit tests completed successfully")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
