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
from typing import List

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", ".."))

UNAUTHORIZED_ERROR = r"^You are not authorized to execute this query.*?Please contact your database administrator\."


def wait_for_server(port, delay=0.1):
    cmd = ["nc", "-z", "-w", "1", "127.0.0.1", str(port)]
    while subprocess.call(cmd) != 0:
        time.sleep(0.01)
    time.sleep(delay)


def execute_tester(
    binary, queries, should_fail=False, failure_message="", username="", password="", check_failure=True
):
    args = [binary, "--username", username, "--password", password]
    if should_fail:
        args.append("--should-fail")
    if failure_message:
        args.extend(["--failure-message", failure_message])
    if check_failure:
        args.append("--check-failure")
    args.extend(queries)
    subprocess.run(args).check_returncode()


def execute_filtering(
    binary: str, queries: List[str], expected: int, username: str = "", password: str = "", db: str = "memgraph"
) -> None:
    args = [binary, "--username", username, "--password", password, "--use-db", db]

    args.extend(queries)
    args.append(str(expected))

    subprocess.run(args).check_returncode()


def execute_test(memgraph_binary: str, tester_binary: str, filtering_binary: str) -> None:
    storage_directory = tempfile.TemporaryDirectory()
    memgraph_args = [memgraph_binary, "--data-directory", storage_directory.name]

    def execute_admin_queries(queries):
        return execute_tester(
            tester_binary, queries, should_fail=False, check_failure=True, username="admin", password="admin"
        )

    def execute_user_queries(queries, should_fail=False, failure_message="", check_failure=True):
        return execute_tester(tester_binary, queries, should_fail, failure_message, "user", "user", check_failure)

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
    def setup_user():
        execute_admin_queries(
            [
                "CREATE USER admin IDENTIFIED BY 'admin'",
                "GRANT ALL PRIVILEGES TO admin",
                "CREATE USER user IDENTIFIED BY 'user'",
                "GRANT ALL PRIVILEGES TO user",
                "GRANT LABELS :label1, :label2, :label3 TO user",
                "GRANT EDGE_TYPES :edgeType1, :edgeType2 TO user",
            ]
        )

    def db_setup():
        execute_admin_queries(
            [
                "MERGE (l1:label1 {name: 'test1'})",
                "MERGE (l2:label2  {name: 'test2'})",
                "MATCH (l1:label1),(l2:label2) WHERE l1.name = 'test1' AND l2.name = 'test2' CREATE (l1)-[r:edgeType1]->(l2)",
                "MERGE (l3:label3  {name: 'test3'})",
                "MATCH (l1:label1),(l3:label3) WHERE l1.name = 'test1' AND l3.name = 'test3' CREATE (l1)-[r:edgeType2]->(l3)",
                "MERGE (mix:label3:label1  {name: 'test4'})",
                "MATCH (l1:label1),(mix:label3) WHERE l1.name = 'test1' AND mix.name = 'test4' CREATE (l1)-[r:edgeType2]->(mix)",
            ]
        )

    db_setup()  # default db setup
    execute_admin_queries(["CREATE DATABASE db1", "USE DATABASE db1"])
    db_setup()  # db1 setup

    print("\033[1;36m~~ Starting edge filtering test ~~\033[0m")
    for db in ["memgraph", "db1"]:
        setup_user()
        # Run the test with all combinations of permissions
        execute_filtering(filtering_binary, ["MATCH (n)-[r]->(m) RETURN n,r,m"], 3, "user", "user", db)
        execute_admin_queries(["DENY EDGE_TYPES :edgeType1 TO user"])
        execute_filtering(filtering_binary, ["MATCH (n)-[r]->(m) RETURN n,r,m"], 2, "user", "user", db)
        execute_admin_queries(["GRANT EDGE_TYPES :edgeType1 TO user", "DENY LABELS :label3 TO user"])
        execute_filtering(filtering_binary, ["MATCH (n)-[r]->(m) RETURN n,r,m"], 1, "user", "user", db)
        execute_admin_queries(["DENY LABELS :label1 TO user"])
        execute_filtering(filtering_binary, ["MATCH (n)-[r]->(m) RETURN n,r,m"], 0, "user", "user", db)
        execute_admin_queries(["REVOKE LABELS * FROM user", "REVOKE EDGE_TYPES * FROM user"])
        execute_filtering(filtering_binary, ["MATCH (n)-[r]->(m) RETURN n,r,m"], 0, "user", "user", db)

    print("\033[1;36m~~ Finished edge filtering test ~~\033[0m\n")

    # Shutdown the memgraph binary
    memgraph.terminate()
    assert memgraph.wait() == 0, "Memgraph process didn't exit cleanly!"


if __name__ == "__main__":
    memgraph_binary = os.path.join(PROJECT_DIR, "build", "memgraph")
    tester_binary = os.path.join(PROJECT_DIR, "build", "tests", "integration", "lba", "tester")
    filtering_binary = os.path.join(PROJECT_DIR, "build", "tests", "integration", "lba", "filtering")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--tester", default=tester_binary)
    parser.add_argument("--filtering", default=filtering_binary)
    args = parser.parse_args()

    execute_test(args.memgraph, args.tester, args.filtering)

    sys.exit(0)
