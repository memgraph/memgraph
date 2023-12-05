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
from pathlib import Path
from typing import List

SCRIPT_DIR = Path(__file__).absolute()
PROJECT_DIR = SCRIPT_DIR.parents[3]
SIGNAL_SIGTERM = 15


def wait_for_server(port, delay=0.1):
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


def start_memgraph(memgraph_args: List[any]) -> subprocess:
    memgraph = subprocess.Popen(list(map(str, memgraph_args)))
    time.sleep(0.1)
    assert memgraph.poll() is None, "Memgraph process died prematurely!"
    wait_for_server(7687)

    return memgraph


def execute_with_user(queries):
    return execute_tester(
        tester_binary, queries, should_fail=False, check_failure=True, username="admin", password="admin"
    )


def cleanup(memgraph):
    if memgraph.poll() is None:
        pid = memgraph.pid
        try:
            os.kill(pid, SIGNAL_SIGTERM)
        except os.OSError:
            assert False
        time.sleep(1)


def execute_without_user(queries, should_fail=False, failure_message="", check_failure=True):
    return execute_tester(tester_binary, queries, should_fail, failure_message, "", "", check_failure)


def test_without_env_variables(memgraph_args: List[any]) -> None:
    memgraph = start_memgraph(memgraph_args)
    execute_without_user(["MATCH (n) RETURN n"], False)
    cleanup(memgraph)


def test_with_user_password_env_variables(memgraph_args: List[any]) -> None:
    os.environ["MEMGRAPH_USER"] = "admin"
    os.environ["MEMGRAPH_PASSWORD"] = "admin"
    memgraph = start_memgraph(memgraph_args)
    execute_with_user(["MATCH (n) RETURN n"])
    execute_without_user(["MATCH (n) RETURN n"], True, "Handshake with the server failed!", True)
    cleanup(memgraph)
    del os.environ["MEMGRAPH_USER"]
    del os.environ["MEMGRAPH_PASSWORD"]


def test_with_passfile_env_variable(storage_directory: tempfile.TemporaryDirectory, memgraph_args: List[any]) -> None:
    with open(os.path.join(storage_directory.name, "passfile.txt"), "w") as temp_file:
        temp_file.write("admin:admin")

    os.environ["MEMGRAPH_PASSFILE"] = storage_directory.name + "/passfile.txt"
    memgraph = start_memgraph(memgraph_args)
    execute_with_user(["MATCH (n) RETURN n"])
    execute_without_user(["MATCH (n) RETURN n"], True, "Handshake with the server failed!", True)
    del os.environ["MEMGRAPH_PASSFILE"]
    cleanup(memgraph)


def execute_test(memgraph_binary: str, tester_binary: str) -> None:
    storage_directory = tempfile.TemporaryDirectory()
    memgraph_args = [memgraph_binary, "--data-directory", storage_directory.name]

    return_to_prev_state = {}
    if "MEMGRAPH_USER" in os.environ:
        return_to_prev_state["MEMGRAPH_USER"] = os.environ["MEMGRAPH_USER"]
        del os.environ["MG_USER"]
    if "MEMGRAPH_PASSWORD" in os.environ:
        return_to_prev_state["MEMGRAPH_PASSWORD"] = os.environ["MEMGRAPH_PASSWORD"]
        del os.environ["MEMGRAPH_PASSWORD"]
    if "MEMGRAPH_PASSFILE" in os.environ:
        return_to_prev_state["MEMGRAPH_PASSFILE"] = os.environ["MEMGRAPH_PASSFILE"]
        del os.environ["MEMGRAPH_PASSFILE"]

    # Start the memgraph binary

    # Run the test with all combinations of permissions
    print("\033[1;36m~~ Starting env variable check test ~~\033[0m")
    test_without_env_variables(memgraph_args)
    test_with_user_password_env_variables(memgraph_args)
    test_with_passfile_env_variable(storage_directory, memgraph_args)
    print("\033[1;36m~~ Ended env variable check test ~~\033[0m")

    if "MEMGRAPH_USER" in return_to_prev_state:
        os.environ["MEMGRAPH_USER"] = return_to_prev_state["MEMGRAPH_USER"]
    if "MEMGRAPH_PASSWORD" in return_to_prev_state:
        os.environ["MEMGRAPH_PASSWORD"] = return_to_prev_state["MEMGRAPH_PASSWORD"]
    if "MEMGRAPH_PASSFILE" in return_to_prev_state:
        os.environ["MEMGRAPH_PASSFILE"] = return_to_prev_state["MEMGRAPH_PASSFILE"]


if __name__ == "__main__":
    memgraph_binary = os.path.join(PROJECT_DIR, "build", "memgraph")
    tester_binary = os.path.join(PROJECT_DIR, "build", "tests", "integration", "env_variable_check", "tester")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--tester", default=tester_binary)
    args = parser.parse_args()

    execute_test(args.memgraph, args.tester)

    sys.exit(0)
