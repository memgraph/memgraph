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


def execute_flag_check(binary: str, queries: List[str], expected: int, username: str = "", password: str = "") -> None:
    args = [binary, "--username", username, "--password", password]

    args.extend(queries)
    args.append(str(expected))

    subprocess.run(args).check_returncode()


def start_memgraph(memgraph_args):
    memgraph = subprocess.Popen(list(map(str, memgraph_args)))
    time.sleep(0.1)
    assert memgraph.poll() is None, "Memgraph process died prematurely!"
    wait_for_server(7687)

    return memgraph


def execute_test(memgraph_binary: str, tester_binary: str, flag_checker_binary: str) -> None:
    storage_directory = tempfile.TemporaryDirectory()
    memgraph_args = [memgraph_binary, "--data-directory", storage_directory.name]

    def execute_with_user(queries):
        return execute_tester(
            tester_binary, queries, should_fail=False, check_failure=True, username="admin", password="admin"
        )

    def execute_without_user(queries, should_fail=False, failure_message="", check_failure=True):
        return execute_tester(tester_binary, queries, should_fail, failure_message, "", "", check_failure)

    # Start the memgraph binary
    memgraph = start_memgraph(memgraph_args)
    with open(os.path.join(os.getcwd(), "dummy_init_file.cypherl"), "w") as temp_file:
        temp_file.write("CREATE USER admin IDENTIFIED BY 'admin';\n")
        temp_file.write("CREATE USER user IDENTIFIED BY 'user';\n")

    with open(os.path.join(os.getcwd(), "dummy_init_data_file.cypherl"), "w") as temp_file:
        temp_file.write("CREATE (n:RANDOM) RETURN n;\n")
        temp_file.write("CREATE (n:RANDOM {name:'1'}) RETURN n;\n")

    # Register cleanup function
    @atexit.register
    def cleanup():
        if memgraph.poll() is None:
            memgraph.terminate()
        assert memgraph.wait() == 0, "Memgraph process didn't exit cleanly!"

    # Run the test with all combinations of permissions
    print("\033[1;36m~~ Starting env variable check test ~~\033[0m")
    execute_without_user(["MATCH (n) RETURN n"], False)
    cleanup()
    memgraph_args_with_init_file = memgraph_args + [
        "--init-file",
        os.path.join(os.getcwd(), "dummy_init_file.cypherl"),
    ]
    # print(memgraph_args_with_init_file)
    memgraph = start_memgraph(memgraph_args_with_init_file)
    execute_with_user(["MATCH (n) RETURN n"])
    execute_without_user(["MATCH (n) RETURN n"], True, "Handshake with the server failed!", True)
    cleanup()

    memgraph_args_with_init_data_file = memgraph_args + [
        "--init-data-file",
        os.path.join(os.getcwd(), "dummy_init_data_file.cypherl"),
    ]
    memgraph = start_memgraph(memgraph_args_with_init_data_file)
    execute_flag_check(flag_check_binary, ["MATCH (n) RETURN n"], 2, "user", "user")
    cleanup()

    memgraph_args_with_init_file_and_init_data_file = memgraph_args + [
        "--init-file",
        os.path.join(os.getcwd(), "dummy_init_file.cypherl"),
        "--init-data-file",
        os.path.join(os.getcwd(), "dummy_init_data_file.cypherl"),
    ]
    memgraph = start_memgraph(memgraph_args_with_init_file_and_init_data_file)
    execute_with_user(["MATCH (n) RETURN n"])
    execute_without_user(["MATCH (n) RETURN n"], True, "Handshake with the server failed!", True)
    execute_flag_check(flag_checker_binary, ["MATCH (n) RETURN n"], 2, "user", "user")
    print("\033[1;36m~~ Ended env variable check test ~~\033[0m")
    cleanup()

    os.remove(os.path.join(os.getcwd(), "dummy_init_data_file.cypherl"))
    os.remove(os.path.join(os.getcwd(), "dummy_init_file.cypherl"))


if __name__ == "__main__":
    memgraph_binary = os.path.join(PROJECT_DIR, "build", "memgraph")
    tester_binary = os.path.join(PROJECT_DIR, "build", "tests", "integration", "flag_check", "tester")
    flag_check_binary = os.path.join(PROJECT_DIR, "build", "tests", "integration", "flag_check", "flag_check")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--tester", default=tester_binary)
    parser.add_argument("--flag_checker", default=flag_check_binary)
    args = parser.parse_args()

    execute_test(args.memgraph, args.tester, args.flag_checker)

    sys.exit(0)
