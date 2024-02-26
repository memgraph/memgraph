#!/usr/bin/python3 -u

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

import argparse
import os
import subprocess
import sys
import tempfile
import time
from typing import List

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", ".."))
SIGNAL_SIGTERM = 15


def wait_for_server(port: int, delay: float = 0.1) -> float:
    cmd = ["nc", "-z", "-w", "1", "127.0.0.1", str(port)]
    while subprocess.call(cmd) != 0:
        time.sleep(0.01)
    time.sleep(delay)


def execute_tester(
    binary: str,
    queries: List[str],
    should_fail: bool = False,
    failure_message: str = "",
    username: str = "",
    password: str = "",
    check_failure: bool = True,
) -> None:
    args = [binary, "--username", username, "--password", password]
    if should_fail:
        args.append("--should-fail")
    if failure_message:
        args.extend(["--failure-message", failure_message])
    if check_failure:
        args.append("--check-failure")
    args.extend(queries)
    subprocess.run(args).check_returncode()


def execute_flag_check(binary: str, queries: List[str], expected: int, username: str = "", password: str = "") -> None:
    args = [binary, "--username", username, "--password", password]

    args.extend(queries)
    args.append(str(expected))

    subprocess.run(args).check_returncode()


def start_memgraph(memgraph_args: List[any]) -> subprocess:
    memgraph = subprocess.Popen(list(map(str, memgraph_args)))
    time.sleep(0.1)
    assert memgraph.poll() is None, "Memgraph process died prematurely!"
    wait_for_server(7687)

    return memgraph


def execute_with_user(tester_binary: str, queries: List[str]) -> None:
    return execute_tester(
        tester_binary, queries, should_fail=False, check_failure=True, username="admin", password="admin"
    )


def execute_without_user(
    tester_binary: str,
    queries: List[str],
    should_fail: bool = False,
    failure_message: str = "",
    check_failure: bool = True,
) -> None:
    return execute_tester(tester_binary, queries, should_fail, failure_message, "", "", check_failure)


def cleanup(memgraph: subprocess):
    if memgraph.poll() is None:
        pid = memgraph.pid
        try:
            os.kill(pid, SIGNAL_SIGTERM)
        except os.OSError:
            assert False
        time.sleep(1)


def test_without_any_files(tester_binary: str, memgraph_args: List[str]):
    memgraph = start_memgraph(memgraph_args)
    execute_without_user(tester_binary, ["MATCH (n) RETURN n"], False)
    cleanup(memgraph)


def test_init_file(tester_binary: str, memgraph_args: List[str]):
    memgraph = start_memgraph(memgraph_args)
    execute_with_user(tester_binary, ["MATCH (n) RETURN n"])
    execute_without_user(tester_binary, ["MATCH (n) RETURN n"], True, "Handshake with the server failed!", True)
    cleanup(memgraph)


def test_init_data_file(flag_checker_binary: str, memgraph_args: List[str]):
    memgraph = start_memgraph(memgraph_args)
    execute_flag_check(flag_checker_binary, ["MATCH (n) RETURN n"], 2, "user", "user")
    cleanup(memgraph)


def test_init_and_init_data_file(flag_checker_binary: str, tester_binary: str, memgraph_args: List[str]):
    memgraph = start_memgraph(memgraph_args)
    execute_with_user(tester_binary, ["MATCH (n) RETURN n"])
    execute_without_user(tester_binary, ["MATCH (n) RETURN n"], True, "Handshake with the server failed!", True)
    execute_flag_check(flag_checker_binary, ["MATCH (n) RETURN n"], 2, "user", "user")
    cleanup(memgraph)


def execute_test(memgraph_binary: str, tester_binary: str, flag_checker_binary: str) -> None:
    storage_directory = tempfile.TemporaryDirectory()
    memgraph_args = [
        memgraph_binary,
        "--data-directory",
        storage_directory.name,
        "--log-file=/tmp/memgraph_integration/logs/memgraph.log",
    ]

    # Start the memgraph binary
    with open(os.path.join(os.getcwd(), "dummy_init_file.cypherl"), "w") as temp_file:
        temp_file.write("CREATE USER admin IDENTIFIED BY 'admin';\n")
        temp_file.write("CREATE USER user IDENTIFIED BY 'user';\n")

    with open(os.path.join(os.getcwd(), "dummy_init_data_file.cypherl"), "w") as temp_file:
        temp_file.write("CREATE (n:RANDOM) RETURN n;\n")
        temp_file.write("CREATE (n:RANDOM {name:'1'}) RETURN n;\n")

    # Run the test with all combinations of permissions
    print("\033[1;36m~~ Starting env variable check test ~~\033[0m")
    test_without_any_files(tester_binary, memgraph_args)
    memgraph_args_with_init_file = memgraph_args + [
        "--init-file",
        os.path.join(os.getcwd(), "dummy_init_file.cypherl"),
    ]
    test_init_file(tester_binary, memgraph_args_with_init_file)
    memgraph_args_with_init_data_file = memgraph_args + [
        "--init-data-file",
        os.path.join(os.getcwd(), "dummy_init_data_file.cypherl"),
    ]

    test_init_data_file(flag_checker_binary, memgraph_args_with_init_data_file)
    memgraph_args_with_init_file_and_init_data_file = memgraph_args + [
        "--init-file",
        os.path.join(os.getcwd(), "dummy_init_file.cypherl"),
        "--init-data-file",
        os.path.join(os.getcwd(), "dummy_init_data_file.cypherl"),
    ]
    test_init_and_init_data_file(flag_checker_binary, tester_binary, memgraph_args_with_init_file_and_init_data_file)
    print("\033[1;36m~~ Ended env variable check test ~~\033[0m")

    os.remove(os.path.join(os.getcwd(), "dummy_init_data_file.cypherl"))
    os.remove(os.path.join(os.getcwd(), "dummy_init_file.cypherl"))


if __name__ == "__main__":
    memgraph_binary = os.path.join(PROJECT_DIR, "build", "memgraph")
    tester_binary = os.path.join(PROJECT_DIR, "build", "tests", "integration", "flag_check", "tester")
    flag_checker_binary = os.path.join(PROJECT_DIR, "build", "tests", "integration", "flag_check", "flag_check")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--tester", default=tester_binary)
    parser.add_argument("--flag_checker", default=flag_checker_binary)
    args = parser.parse_args()

    execute_test(args.memgraph, args.tester, args.flag_checker)

    sys.exit(0)
