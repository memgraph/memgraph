#!/usr/bin/python3 -u

# Copyright 2021 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import argparse
import atexit
import os
import subprocess
import sys
import tempfile
import time

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", ".."))
SIGNAL_SIGTERM = 15

# When you create a new permission just add a testcase to this list (a tuple
# of query, touple of required permissions) and the test will automatically
# detect the new permission (from the query required permissions) and test all
# queries against all combinations of permissions.

QUERIES = [
    # CREATE
    ("CREATE (n)", ("CREATE",)),
    ("MATCH (n), (m) CREATE (n)-[:e]->(m)", ("CREATE", "MATCH")),
    # DELETE
    (
        "MATCH (n) DELETE n",
        ("DELETE", "MATCH"),
    ),
    (
        "MATCH (n) DETACH DELETE n",
        ("DELETE", "MATCH"),
    ),
    # MATCH
    ("MATCH (n) RETURN n", ("MATCH",)),
    ("MATCH (n), (m) RETURN count(n), count(m)", ("MATCH",)),
    # MERGE
    (
        "MERGE (n) ON CREATE SET n.created = timestamp() "
        "ON MATCH SET n.lastSeen = timestamp() "
        "RETURN n.name, n.created, n.lastSeen",
        ("MERGE",),
    ),
    # SET
    ("MATCH (n) SET n.value = 0 RETURN n", ("SET", "MATCH")),
    ("MATCH (n), (m) SET n.value = m.value", ("SET", "MATCH")),
    # REMOVE
    ("MATCH (n) REMOVE n.value", ("REMOVE", "MATCH")),
    ("MATCH (n), (m) REMOVE n.value, m.value", ("REMOVE", "MATCH")),
    # INDEX
    ("CREATE INDEX ON :User (id)", ("INDEX",)),
    # AUTH
    ("CREATE ROLE test_role", ("AUTH",)),
    ("DROP ROLE test_role", ("AUTH",)),
    ("SHOW ROLES", ("AUTH",)),
    ("CREATE USER test_user", ("AUTH",)),
    ("SET PASSWORD FOR test_user TO '1234'", ("AUTH",)),
    ("DROP USER test_user", ("AUTH",)),
    ("SHOW USERS", ("AUTH",)),
    ("SET ROLE FOR test_user TO test_role", ("AUTH",)),
    ("SET ROLES FOR test_user TO test_role", ("AUTH",)),
    ("SET ROLE FOR test_user TO role1, role2, role3", ("AUTH",)),
    ("SET ROLES FOR test_user TO role1, role2, role3", ("AUTH",)),
    ("SET ROLE FOR test_user TO role1 ON db1", ("AUTH",)),
    ("SET ROLES FOR test_user TO role1, role2 ON db1, db2", ("AUTH",)),
    ("CLEAR ROLE FOR test_user", ("AUTH",)),
    ("CLEAR ROLES FOR test_user", ("AUTH",)),
    ("CLEAR ROLE FOR test_user ON db1", ("AUTH",)),
    ("CLEAR ROLES FOR test_user ON db1, db2", ("AUTH",)),
    ("GRANT ALL PRIVILEGES TO test_user", ("AUTH",)),
    ("DENY ALL PRIVILEGES TO test_user", ("AUTH",)),
    ("REVOKE ALL PRIVILEGES FROM test_user", ("AUTH",)),
    ("SHOW PRIVILEGES FOR test_user ON MAIN", ("AUTH",)),
    ("SHOW ROLE FOR test_user", ("AUTH",)),
    ("SHOW ROLES FOR test_user", ("AUTH",)),
    ("SHOW ROLE FOR test_user ON MAIN", ("AUTH",)),
    ("SHOW ROLES FOR test_user ON MAIN", ("AUTH",)),
    ("SHOW ROLE FOR test_user ON CURRENT", ("AUTH",)),
    ("SHOW ROLES FOR test_user ON CURRENT", ("AUTH",)),
    ("SHOW ROLE FOR test_user ON DATABASE db1", ("AUTH",)),
    ("SHOW ROLES FOR test_user ON DATABASE db1", ("AUTH",)),
    ("SHOW USERS FOR test_role", ("AUTH",)),
]

UNAUTHORIZED_ERROR = r"^You are not authorized to execute this query.*?Please contact your database administrator\. This issue comes from the user having not enough role-based access privileges to execute this query\. If you want this issue to be resolved\, ask your database administrator to grant you a specific privilege for query execution\."


def wait_for_server(port, delay=0.1):
    cmd = ["nc", "-z", "-w", "1", "127.0.0.1", str(port)]
    while subprocess.call(cmd) != 0:
        time.sleep(0.01)
    time.sleep(delay)


def execute_tester(
    binary,
    queries,
    should_fail=False,
    failure_message="",
    username="",
    password="",
    check_failure=True,
    connection_should_fail=False,
):
    args = [binary, "--username", username, "--password", password]
    if should_fail:
        args.append("--should-fail")
    if failure_message:
        args.extend(["--failure-message", failure_message])
    if check_failure:
        args.append("--check-failure")
    if connection_should_fail:
        args.append("--connection-should-fail")
    args.extend(queries)
    subprocess.run(args).check_returncode()


def execute_checker(binary, grants):
    args = [binary] + grants
    subprocess.run(args).check_returncode()


def get_permissions(permissions, mask):
    ret, pos = [], 0
    while mask > 0:
        if mask & 1:
            ret.append(permissions[pos])
        mask >>= 1
        pos += 1
    return ret


def check_permissions(query_perms, user_perms):
    return set(query_perms).issubset(user_perms)


def execute_test(memgraph_binary, tester_binary, checker_binary):
    storage_directory = tempfile.TemporaryDirectory()
    memgraph_args = [memgraph_binary, "--data-directory", storage_directory.name]

    def execute_admin_queries(queries, should_fail=False):
        return execute_tester(
            tester_binary, queries, should_fail=should_fail, check_failure=True, username="admin", password="admin"
        )

    def execute_user_queries(
        queries,
        should_fail=False,
        failure_message="",
        check_failure=True,
        username="user",
        connection_should_fail=False,
    ):
        return execute_tester(
            tester_binary,
            queries,
            should_fail,
            failure_message,
            username,
            "user",
            check_failure,
            connection_should_fail,
        )

    # Start the memgraph binary
    memgraph = subprocess.Popen(list(map(str, memgraph_args)))
    time.sleep(0.1)
    assert memgraph.poll() is None, "Memgraph process died prematurely!"
    wait_for_server(7687)

    # Register cleanup function
    @atexit.register
    def cleanup():
        if memgraph.poll() is None:
            pid = memgraph.pid
            try:
                os.kill(pid, SIGNAL_SIGTERM)
            except os.OSError:
                assert False
            time.sleep(1)

    # Prepare the multi database environment
    execute_admin_queries(
        [
            "CREATE DATABASE db1",
            "CREATE DATABASE db2",
        ]
    )

    # Prepare all users
    execute_admin_queries(
        [
            "CREATE USER ADmin IDENTIFIED BY 'admin'",
            "GRANT ALL PRIVILEGES TO admIN",
            "GRANT DATABASE * TO admin",
            "CREATE USER usEr IDENTIFIED BY 'user'",
            "GRANT DATABASE db1 TO user",
            "GRANT DATABASE db2 TO user",
            "CREATE USER useR2 IDENTIFIED BY 'user'",
            "GRANT DATABASE db2 TO user2",
            # "DENY DATABASE memgraph FROM user2", memgraph needed for system queries
            "SET MAIN DATABASE db2 FOR user2",
            "CREATE USER user3 IDENTIFIED BY 'user'",
            "GRANT ALL PRIVILEGES TO user3",
            "GRANT DATABASE * TO user3",
            # "DENY DATABASE memgraph FROM user3", memgraph needed for system queries
        ]
    )

    # Find all existing permissions
    permissions = set()
    for query, perms in QUERIES:
        permissions.update(perms)
    permissions = list(sorted(permissions))

    # Run the test with all combinations of permissions
    print("\033[1;36m~~ Starting query test ~~\033[0m")
    for db in ["memgraph", "db1"]:
        print("\033[1;36m~~ Running against db {} ~~\033[0m".format(db))
        execute_user_queries(["USE DATABASE {}".format(db)], should_fail=True, failure_message=UNAUTHORIZED_ERROR)
        execute_admin_queries(["GRANT MULTI_DATABASE_USE TO User"])
        execute_user_queries(["USE DATABASE {}".format(db)], check_failure=False, failure_message=UNAUTHORIZED_ERROR)
        for mask in range(0, 2 ** len(permissions)):
            user_perms = get_permissions(permissions, mask)
            print("\033[1;34m~~ Checking queries with privileges: ", ", ".join(user_perms), " ~~\033[0m")
            admin_queries = ["REVOKE ALL PRIVILEGES FROM uSer"]
            if len(user_perms) > 0:
                admin_queries.append("GRANT {} TO User".format(", ".join(user_perms)))
            execute_admin_queries(admin_queries)
            authorized, unauthorized = [], []
            for query, query_perms in QUERIES:
                if check_permissions(query_perms, user_perms):
                    authorized.append(query)
                else:
                    unauthorized.append(query)
            execute_user_queries(authorized, check_failure=False, failure_message=UNAUTHORIZED_ERROR)
            execute_user_queries(unauthorized, should_fail=True, failure_message=UNAUTHORIZED_ERROR)
    print("\033[1;36m~~ Finished query test ~~\033[0m\n")

    # Run the user/role permissions test
    print("\033[1;36m~~ Starting permissions test ~~\033[0m")
    execute_admin_queries(
        [
            "CREATE ROLE roLe",
            "CREATE ROLE role2",
            "CREATE ROLE role3",
            "REVOKE ALL PRIVILEGES FROM uSeR",
        ]
    )
    execute_checker(checker_binary, [])
    for db in ["memgraph", "db1"]:
        print("\033[1;36m~~ Running against db {} ~~\033[0m".format(db))
        execute_user_queries(["USE DATABASE {}".format(db)], should_fail=True, failure_message=UNAUTHORIZED_ERROR)
        execute_admin_queries(["GRANT MULTI_DATABASE_USE TO User"])
        execute_user_queries(["USE DATABASE {}".format(db)], check_failure=False, failure_message=UNAUTHORIZED_ERROR)
        execute_admin_queries(["REVOKE MULTI_DATABASE_USE FROM User"])

        # Test single role scenarios
        for user_perm in ["GRANT", "DENY", "REVOKE"]:
            for role_perm in ["GRANT", "DENY", "REVOKE"]:
                for mapped in [True, False]:
                    print(
                        "\033[1;34m~~ Checking permissions with user ",
                        user_perm,
                        ", role ",
                        role_perm,
                        "user mapped to role:",
                        mapped,
                        " ~~\033[0m",
                    )
                    if mapped:
                        execute_admin_queries(["SET ROLE FOR USER TO roLE"])
                        # Also test SET ROLES variation
                        execute_admin_queries(["SET ROLES FOR USER TO roLE"])
                    else:
                        execute_admin_queries(["CLEAR ROLE FOR user"])
                        # Also test CLEAR ROLES variation
                        execute_admin_queries(["CLEAR ROLES FOR user"])
                    user_prep = "FROM" if user_perm == "REVOKE" else "TO"
                    role_prep = "FROM" if role_perm == "REVOKE" else "TO"
                    execute_admin_queries(
                        [
                            "{} MATCH {} user".format(user_perm, user_prep),
                            "{} MATCH {} rOLe".format(role_perm, role_prep),
                        ]
                    )
                    expected = []
                    perms = [user_perm, role_perm] if mapped else [user_perm]
                    if "DENY" in perms:
                        expected = ["MATCH", "DENY"]
                    elif "GRANT" in perms:
                        expected = ["MATCH", "GRANT"]
                    if len(expected) > 0:
                        details = []
                        if user_perm == "GRANT":
                            details.append("GRANTED TO USER")
                        elif user_perm == "DENY":
                            details.append("DENIED TO USER")
                        if mapped:
                            if role_perm == "GRANT":
                                details.append("GRANTED TO ROLE")
                            elif role_perm == "DENY":
                                details.append("DENIED TO ROLE")
                        expected.append(", ".join(details))
                    execute_checker(checker_binary, expected)

        # Test multiple roles scenarios
        print("\033[1;34m~~ Testing multiple roles permissions ~~\033[0m")
        for user_perm in ["GRANT", "DENY", "REVOKE"]:
            for role1_perm in ["GRANT", "DENY", "REVOKE"]:
                for role2_perm in ["GRANT", "DENY", "REVOKE"]:
                    print(
                        "\033[1;34m~~ Checking multiple roles permissions with user ",
                        user_perm,
                        ", role1 ",
                        role1_perm,
                        ", role2 ",
                        role2_perm,
                        " ~~\033[0m",
                    )
                    # Set user to have multiple roles
                    execute_admin_queries(["SET ROLE FOR USER TO roLe, role2"])
                    # Also test SET ROLES variation
                    execute_admin_queries(["SET ROLES FOR USER TO roLe, role2"])

                    user_prep = "FROM" if user_perm == "REVOKE" else "TO"
                    role1_prep = "FROM" if role1_perm == "REVOKE" else "TO"
                    role2_prep = "FROM" if role2_perm == "REVOKE" else "TO"

                    execute_admin_queries(
                        [
                            "{} MATCH {} user".format(user_perm, user_prep),
                            "{} MATCH {} rOLe".format(role1_perm, role1_prep),
                            "{} MATCH {} role2".format(role2_perm, role2_prep),
                        ]
                    )

                    expected = []
                    perms = [user_perm, role1_perm, role2_perm]
                    if "DENY" in perms:
                        expected = ["MATCH", "DENY"]
                    elif "GRANT" in perms:
                        expected = ["MATCH", "GRANT"]
                    if len(expected) > 0:
                        details = []
                        if user_perm == "GRANT":
                            details.append("GRANTED TO USER")
                        elif user_perm == "DENY":
                            details.append("DENIED TO USER")
                        if role1_perm == "GRANT" and role2_perm != "DENY":
                            details.append("GRANTED TO ROLE")
                        elif role1_perm == "DENY":
                            details.append("DENIED TO ROLE")
                        elif role2_perm == "GRANT":
                            details.append("GRANTED TO ROLE")
                        elif role2_perm == "DENY":
                            details.append("DENIED TO ROLE")
                        expected.append(", ".join(details))
                    execute_checker(checker_binary, expected)

    print("\033[1;36m~~ Finished permissions test ~~\033[0m\n")

    # Run the multiple roles test
    print("\033[1;36m~~ Starting multiple roles test ~~\033[0m")
    execute_admin_queries(
        [
            "CREATE ROLE role1",
            "CREATE USER multi_user IDENTIFIED BY 'user'",
            "GRANT DATABASE db1 TO multi_user",
            "GRANT DATABASE db2 TO multi_user",
        ]
    )

    # Test setting multiple roles
    print("\033[1;34m~~ Testing SET ROLE/ROLES with multiple roles ~~\033[0m")
    execute_admin_queries(["SET ROLE FOR multi_user TO role1, role2, role3"])
    execute_admin_queries(["SET ROLES FOR multi_user TO role1, role2, role3"])

    # Test permissions from multiple roles
    print("\033[1;34m~~ Testing permissions from multiple roles ~~\033[0m")
    execute_admin_queries(
        ["GRANT CREATE TO role1", "GRANT MATCH TO role2", "GRANT SET TO role3", "DENY DELETE TO role1"]
    )

    # Test that user has combined permissions from all roles
    authorized_queries = [
        "CREATE (n)",  # From role1
        "MATCH (n) RETURN n",  # From role2
        "MATCH (n) SET n.value = 1 RETURN n",  # From role2 + role3
    ]
    unauthorized_queries = [
        "MATCH (n) DELETE n",  # DENY from role1 overrides any GRANT
    ]

    execute_user_queries(authorized_queries, should_fail=False, username="multi_user")
    execute_user_queries(
        unauthorized_queries, should_fail=True, failure_message=UNAUTHORIZED_ERROR, username="multi_user"
    )

    # Test updating roles (replace all roles)
    print("\033[1;34m~~ Testing role replacement ~~\033[0m")
    execute_admin_queries(
        ["CREATE ROLE role4", "GRANT DELETE TO role4", "SET ROLE FOR multi_user TO role2, role4"]  # Replace roles
    )

    # Test that old roles are removed and new permissions apply
    execute_user_queries(["MATCH (n) DELETE n"], should_fail=False, username="multi_user")  # Now allowed via role4
    execute_user_queries(
        ["CREATE (n)"], should_fail=True, failure_message=UNAUTHORIZED_ERROR, username="multi_user"
    )  # No longer has role1

    # Test clearing all roles
    print("\033[1;34m~~ Testing CLEAR ROLE/ROLES ~~\033[0m")
    execute_admin_queries(["CLEAR ROLE FOR multi_user"])
    execute_admin_queries(["CLEAR ROLES FOR multi_user"])
    execute_user_queries(
        ["MATCH (n) RETURN n"], should_fail=True, failure_message=UNAUTHORIZED_ERROR, username="multi_user"
    )

    # Test role removal from multiple roles
    print("\033[1;34m~~ Testing role removal from multiple roles ~~\033[0m")
    execute_admin_queries(
        ["SET ROLE FOR multi_user TO role1, role2, role3", "GRANT CREATE TO role1", "GRANT MATCH TO role2"]
    )

    # Test that user has permissions from all roles
    execute_user_queries(["CREATE (n)", "MATCH (n) RETURN n"], should_fail=False, username="multi_user")

    # Remove one role and test that permissions are updated
    execute_admin_queries(["SET ROLES FOR multi_user TO role1, role3"])  # Remove role2 with ROLES
    execute_user_queries(["CREATE (n)"], should_fail=False, username="multi_user")  # Still has role1
    execute_user_queries(
        ["MATCH (n) RETURN n"], should_fail=True, failure_message=UNAUTHORIZED_ERROR, username="multi_user"
    )  # No longer has role2

    # Test database access with multiple roles
    print("\033[1;34m~~ Testing database access with multiple roles ~~\033[0m")
    execute_admin_queries(
        [
            "GRANT DATABASE db1 TO role1",
            "GRANT MATCH, MULTI_DATABASE_USE TO role1",
            "GRANT DATABASE db2 TO role2",
            "GRANT MATCH, MULTI_DATABASE_USE TO role2",
            "SET ROLE FOR multi_user TO role1, role2",
        ]
    )
    execute_admin_queries(
        [
            "GRANT DATABASE db1 TO role1",
            "GRANT MATCH, MULTI_DATABASE_USE TO role1",
            "GRANT DATABASE db2 TO role2",
            "GRANT MATCH, MULTI_DATABASE_USE TO role2",
            "SET ROLES FOR multi_user TO role1, role2",
        ]
    )

    # Test that user can access both databases
    execute_user_queries(["USE DATABASE db1", "MATCH (n) RETURN n"], should_fail=False, username="multi_user")
    execute_user_queries(["USE DATABASE db2", "MATCH (n) RETURN n"], should_fail=False, username="multi_user")

    # Test with roles that have conflicting database access
    execute_admin_queries(["DENY DATABASE db1 FROM role2", "SET ROLE FOR multi_user TO role1, role2"])

    # DENY should override GRANT, so user should not have access to db1
    execute_user_queries(
        ["USE DATABASE db1"], should_fail=True, failure_message=UNAUTHORIZED_ERROR, username="multi_user"
    )
    execute_user_queries(["USE DATABASE db2"], should_fail=False, username="multi_user")

    # Test error cases for multiple roles
    print("\033[1;34m~~ Testing multiple roles error cases ~~\033[0m")

    # Test setting non-existent roles (should fail)
    execute_admin_queries(["SET ROLE FOR multi_user TO nonexistent_role"], should_fail=True)

    # Test setting mix of existing and non-existing roles (should fail)
    execute_admin_queries(["SET ROLE FOR multi_user TO role1, nonexistent_role"], should_fail=True)

    print("\033[1;36m~~ Finished multiple roles test ~~\033[0m\n")

    # Test database-specific role functionality
    print("\033[1;36m~~ Starting database-specific role test ~~\033[0m")
    execute_admin_queries(
        [
            "CREATE ROLE db_role1",
            "CREATE ROLE db_role2",
            "CREATE USER db_user IDENTIFIED BY 'user'",
            "GRANT DATABASE db1 TO db_user",
            "GRANT DATABASE db2 TO db_user",
            "GRANT DATABASE db1 TO db_role1",
            "GRANT DATABASE db2 TO db_role1",
            "GRANT DATABASE * TO db_role2",
        ]
    )

    # Test SET ROLE/ROLES with database-specific clauses
    print("\033[1;34m~~ Testing database-specific SET ROLE/ROLES ~~\033[0m")
    execute_admin_queries(["SET ROLE FOR db_user TO db_role1 ON db1"])
    execute_admin_queries(["SET ROLES FOR db_user TO db_role1, db_role2 ON db2"])
    execute_admin_queries(["SET ROLE FOR db_user TO db_role2 ON db1, db2"])

    # Test CLEAR ROLE/ROLES with database-specific clauses
    print("\033[1;34m~~ Testing database-specific CLEAR ROLE/ROLES ~~\033[0m")
    execute_admin_queries(["CLEAR ROLE FOR db_user ON db1"])
    execute_admin_queries(["CLEAR ROLES FOR db_user ON db2"])

    # Test SHOW ROLE/ROLES with database-specific clauses
    print("\033[1;34m~~ Testing database-specific SHOW ROLE/ROLES ~~\033[0m")
    execute_admin_queries(["SET ROLE FOR db_user TO db_role1 ON db1"])
    execute_admin_queries(["SET ROLE FOR db_user TO db_role2 ON db2"])

    # Test all variations of SHOW ROLE/ROLES
    show_queries = [
        "SHOW ROLE FOR db_user ON MAIN",
        "SHOW ROLES FOR db_user ON MAIN",
        "SHOW ROLE FOR db_user ON CURRENT",
        "SHOW ROLES FOR db_user ON CURRENT",
        "SHOW ROLE FOR db_user ON DATABASE db1",
        "SHOW ROLES FOR db_user ON DATABASE db1",
        "SHOW ROLE FOR db_user ON DATABASE db2",
        "SHOW ROLES FOR db_user ON DATABASE db2",
    ]

    for query in show_queries:
        execute_admin_queries([query])

    print("\033[1;36m~~ Finished database-specific role test ~~\033[0m\n")

    # Check database access
    # user has access to every db (with global privileges) <- tested above
    # user2 has access only to db2 (and it set to default)
    # user3 has access only to db2, but the default db is set to default (shouldn't even connect)
    print("\033[1;36m~~ Checking privileges with custom default db ~~\033[0m\n")
    for mask in range(0, 2 ** len(permissions)):
        user_perms = get_permissions(permissions, mask)
        print("\033[1;34m~~ Checking queries with privileges: ", ", ".join(user_perms), " ~~\033[0m")
        admin_queries = ["REVOKE ALL PRIVILEGES FROM uSer2"]
        if len(user_perms) > 0:
            admin_queries.append("GRANT {} TO User2".format(", ".join(user_perms)))
        execute_admin_queries(admin_queries)
        authorized, unauthorized = [], []
        for query, query_perms in QUERIES:
            if check_permissions(query_perms, user_perms):
                authorized.append(query)
            else:
                unauthorized.append(query)
        execute_user_queries(authorized, check_failure=False, failure_message=UNAUTHORIZED_ERROR, username="user2")
        execute_user_queries(unauthorized, should_fail=True, failure_message=UNAUTHORIZED_ERROR, username="user2")
    print("\033[1;36m~~ Finished custom default db checks ~~\033[0m\n")

    print("\033[1;36m~~ Checking connections and database switching ~~\033[0m\n")
    # for db in ["memgraph", "db1"]:
    for db in ["db1"]:
        print("\033[1;36m~~ Running against db {} ~~\033[0m".format(db))
        execute_admin_queries(["GRANT {} TO User2".format("MULTI_DATABASE_USE")])
        execute_user_queries(
            ["USE DATABASE {}".format(db)], should_fail=True, failure_message=UNAUTHORIZED_ERROR, username="user2"
        )
    print("\033[1;36m~~ Running with user3 (no main db) ~~\033[0m")
    execute_admin_queries(["GRANT {} TO User3".format("MULTI_DATABASE_USE")])
    # for db in ["memgraph"]:
    #     print("\033[1;36m~~ Running against db {} ~~\033[0m".format(db))
    #     execute_user_queries(
    #         ["USE DATABASE {}".format(db)], should_fail=True, failure_message=UNAUTHORIZED_ERROR, username="user3"
    #     )
    for db in ["db1", "db2"]:
        print("\033[1;36m~~ Running against db {} ~~\033[0m".format(db))
        execute_user_queries(["USE DATABASE {}".format(db), "MATCH (n) RETURN n;"], should_fail=False, username="user3")
    print("\033[1;36m~~ Finished checking connections and database switching ~~\033[0m\n")

    # Shutdown the memgraph binary
    pid = memgraph.pid
    try:
        os.kill(pid, SIGNAL_SIGTERM)
    except os.OSError:
        assert False
    time.sleep(1)


if __name__ == "__main__":
    memgraph_binary = os.path.join(PROJECT_DIR, "build", "memgraph")
    tester_binary = os.path.join(PROJECT_DIR, "build", "tests", "integration", "auth", "tester")
    checker_binary = os.path.join(PROJECT_DIR, "build", "tests", "integration", "auth", "checker")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--tester", default=tester_binary)
    parser.add_argument("--checker", default=checker_binary)
    args = parser.parse_args()

    execute_test(args.memgraph, args.tester, args.checker)

    sys.exit(0)
