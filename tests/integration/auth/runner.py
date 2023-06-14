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

# When you create a new permission just add a testcase to this list (a tuple
# of query, tuple of required permissions) and the test will automatically
# detect the new permission (from the query required permissions) and test all
# queries against all combinations of permissions.

QUERIES = [
    # CREATE
    (
        "CREATE (n)",
        ("CREATE",)
    ),
    (
        "MATCH (n), (m) CREATE (n)-[:e]->(m)",
        ("CREATE", "MATCH")
    ),

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
    (
        "MATCH (n) RETURN n",
        ("MATCH",)
    ),
    (
        "MATCH (n), (m) RETURN count(n), count(m)",
        ("MATCH",)
    ),

    # MERGE
    (
        "MERGE (n) ON CREATE SET n.created = timestamp() "
        "ON MATCH SET n.lastSeen = timestamp() "
        "RETURN n.name, n.created, n.lastSeen",
        ("MERGE",)
    ),

    # SET
    (
        "MATCH (n) SET n.value = 0 RETURN n",
        ("SET", "MATCH")
    ),
    (
        "MATCH (n), (m) SET n.value = m.value",
        ("SET", "MATCH")
    ),

    # REMOVE
    (
        "MATCH (n) REMOVE n.value",
        ("REMOVE", "MATCH")
    ),
    (
        "MATCH (n), (m) REMOVE n.value, m.value",
        ("REMOVE", "MATCH")
    ),

    # INDEX
    (
        "CREATE INDEX ON :User (id)",
        ("INDEX",)
    ),

    # AUTH
    (
        "CREATE ROLE test_role",
        ("AUTH",)
    ),
    (
        "DROP ROLE test_role",
        ("AUTH",)
    ),
    (
        "SHOW ROLES",
        ("AUTH",)
    ),
    (
        "CREATE USER test_user",
        ("AUTH",)
    ),
    (
        "SET PASSWORD FOR test_user TO '1234'",
        ("AUTH",)
    ),
    (
        "DROP USER test_user",
        ("AUTH",)
    ),
    (
        "SHOW USERS",
        ("AUTH",)
    ),
    (
        "SET ROLE FOR test_user TO test_role",
        ("AUTH",)
    ),
    (
        "CLEAR ROLE FOR test_user",
        ("AUTH",)
    ),
    (
        "GRANT ALL PRIVILEGES TO test_user",
        ("AUTH",)
    ),
    (
        "DENY ALL PRIVILEGES TO test_user",
        ("AUTH",)
    ),
    (
        "REVOKE ALL PRIVILEGES FROM test_user",
        ("AUTH",)
    ),
    (
        "SHOW PRIVILEGES FOR test_user",
        ("AUTH",)
    ),
    (
        "SHOW ROLE FOR test_user",
        ("AUTH",)
    ),
    (
        "SHOW USERS FOR test_role",
        ("AUTH",)
    ),
]

UNAUTHORIZED_ERROR = "You are not authorized to execute this query! Please " \
                     "contact your database administrator."


def wait_for_server(port, delay=0.1):
    cmd = ["nc", "-z", "-w", "1", "127.0.0.1", str(port)]
    while subprocess.call(cmd) != 0:
        time.sleep(0.01)
    time.sleep(delay)


def execute_tester(binary, queries, should_fail=False, failure_message="",
                   username="", password="", check_failure=True):
    args = [binary, "--username", username, "--password", password]
    if should_fail:
        args.append("--should-fail")
    if failure_message:
        args.extend(["--failure-message", failure_message])
    if check_failure:
        args.append("--check-failure")
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
    memgraph_args = [memgraph_binary,
                     "--data-directory", storage_directory.name]

    def execute_admin_queries(queries):
        return execute_tester(tester_binary, queries, should_fail=False,
                              check_failure=True, username="admin",
                              password="admin")

    def execute_user_queries(queries, should_fail=False, failure_message="",
                             check_failure=True):
        return execute_tester(tester_binary, queries, should_fail,
                              failure_message, "user", "user", check_failure)

    # Start the memgraph binary
    memgraph = subprocess.Popen(list(map(str, memgraph_args)))
    time.sleep(0.1)
    assert memgraph.poll() is None, "Memgraph process died prematurely!"
    wait_for_server(7687)

    # Register cleanup function
    @atexit.register
    def cleanup():
        if memgraph.poll() is None:
            memgraph.terminate()
        assert memgraph.wait() == 0, "Memgraph process didn't exit cleanly!"

    # Prepare all users
    execute_admin_queries([
        "CREATE USER ADmin IDENTIFIED BY 'admin'",
        "GRANT ALL PRIVILEGES TO admIN",
        "CREATE USER usEr IDENTIFIED BY 'user'",
    ])

    # Find all existing permissions
    permissions = set()
    for query, perms in QUERIES:
        permissions.update(perms)
    permissions = list(sorted(permissions))

    # Run the test with all combinations of permissions
    print("\033[1;36m~~ Starting query test ~~\033[0m")
    for mask in range(0, 2 ** len(permissions)):
        user_perms = get_permissions(permissions, mask)
        print("\033[1;34m~~ Checking queries with privileges: ",
              ", ".join(user_perms), " ~~\033[0m")
        admin_queries = ["REVOKE ALL PRIVILEGES FROM uSer"]
        if len(user_perms) > 0:
            admin_queries.append(
                    "GRANT {} TO User".format(", ".join(user_perms)))
        execute_admin_queries(admin_queries)
        authorized, unauthorized = [], []
        for query, query_perms in QUERIES:
            if check_permissions(query_perms, user_perms):
                authorized.append(query)
            else:
                unauthorized.append(query)
        execute_user_queries(authorized, check_failure=False,
                             failure_message=UNAUTHORIZED_ERROR)
        execute_user_queries(unauthorized, should_fail=True,
                             failure_message=UNAUTHORIZED_ERROR)
    print("\033[1;36m~~ Finished query test ~~\033[0m\n")

    # Run the user/role permissions test
    print("\033[1;36m~~ Starting permissions test ~~\033[0m")
    execute_admin_queries([
        "CREATE ROLE roLe",
        "REVOKE ALL PRIVILEGES FROM uSeR",
    ])
    execute_checker(checker_binary, [])
    for user_perm in ["GRANT", "DENY", "REVOKE"]:
        for role_perm in ["GRANT", "DENY", "REVOKE"]:
            for mapped in [True, False]:
                print("\033[1;34m~~ Checking permissions with user ",
                      user_perm, ", role ", role_perm,
                      "user mapped to role:", mapped, " ~~\033[0m")
                if mapped:
                    execute_admin_queries(["SET ROLE FOR USER TO roLE"])
                else:
                    execute_admin_queries(["CLEAR ROLE FOR user"])
                user_prep = "FROM" if user_perm == "REVOKE" else "TO"
                role_prep = "FROM" if role_perm == "REVOKE" else "TO"
                execute_admin_queries([
                    "{} MATCH {} user".format(user_perm, user_prep),
                    "{} MATCH {} rOLe".format(role_perm, role_prep)
                ])
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
    print("\033[1;36m~~ Finished permissions test ~~\033[0m\n")

    # Shutdown the memgraph binary
    memgraph.terminate()
    assert memgraph.wait() == 0, "Memgraph process didn't exit cleanly!"


if __name__ == "__main__":
    memgraph_binary = os.path.join(PROJECT_DIR, "build", "memgraph")
    tester_binary = os.path.join(PROJECT_DIR, "build", "tests",
                                 "integration", "auth", "tester")
    checker_binary = os.path.join(PROJECT_DIR, "build", "tests",
                                  "integration", "auth", "checker")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--tester", default=tester_binary)
    parser.add_argument("--checker", default=checker_binary)
    args = parser.parse_args()

    execute_test(args.memgraph, args.tester, args.checker)

    sys.exit(0)
