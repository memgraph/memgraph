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


def start_memgraph(memgraph_args):
    memgraph = subprocess.Popen(list(map(str, memgraph_args)))
    time.sleep(0.1)
    assert memgraph.poll() is None, "Memgraph process died prematurely!"
    wait_for_server(7687)

    return memgraph


def execute_test(memgraph_binary: str, tester_binary: str) -> None:
    storage_directory = tempfile.TemporaryDirectory()
    memgraph_args = [memgraph_binary, "--data-directory", storage_directory.name]

    def execute_with_user(queries):
        return execute_tester(
            tester_binary, queries, should_fail=False, check_failure=True, username="admin", password="admin"
        )

    def execute_without_user(queries, should_fail=False, failure_message="", check_failure=True):
        return execute_tester(tester_binary, queries, should_fail, failure_message, "", "", check_failure)

    return_to_prev_state = {}
    if "MG_USER" in os.environ:
        return_to_prev_state["MG_USER"] = os.environ["MG_USER"]
        del os.environ["MG_USER"]
    if "MG_PASSWORD" in os.environ:
        return_to_prev_state["MG_PASSWORD"] = os.environ["MG_PASSWORD"]
        del os.environ["MG_PASSWORD"]
    if "MG_PASSFILE" in os.environ:
        return_to_prev_state["MG_PASSFILE"] = os.environ["MG_PASSFILE"]
        del os.environ["MG_PASSFILE"]

    # Start the memgraph binary
    memgraph = start_memgraph(memgraph_args)

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
    os.environ["MG_USER"] = "admin"
    os.environ["MG_PASSWORD"] = "admin"
    memgraph = start_memgraph(memgraph_args)
    execute_with_user(["MATCH (n) RETURN n"])
    execute_without_user(["MATCH (n) RETURN n"], True, "Handshake with the server failed!", True)
    cleanup()
    del os.environ["MG_USER"]
    del os.environ["MG_PASSWORD"]
    with open(os.path.join(storage_directory.name, "passfile.txt"), "w") as temp_file:
        temp_file.write("admin:admin")

    os.environ["MG_PASSFILE"] = storage_directory.name + "/passfile.txt"
    memgraph = start_memgraph(memgraph_args)
    execute_with_user(["MATCH (n) RETURN n"])
    execute_without_user(["MATCH (n) RETURN n"], True, "Handshake with the server failed!", True)
    del os.environ["MG_PASSFILE"]
    print("\033[1;36m~~ Ended env variable check test ~~\033[0m")
    cleanup()

    if "MG_USER" in return_to_prev_state:
        os.environ["MG_USER"] = return_to_prev_state["MG_USER"]
    if "MG_PASSWORD" in return_to_prev_state:
        os.environ["MG_PASSWORD"] = return_to_prev_state["MG_PASSWORD"]
    if "MG_PASSFILE" in return_to_prev_state:
        os.environ["MG_PASSFILE"] = return_to_prev_state["MG_PASSFILE"]


if __name__ == "__main__":
    memgraph_binary = os.path.join(PROJECT_DIR, "build", "memgraph")
    tester_binary = os.path.join(PROJECT_DIR, "build", "tests", "integration", "env_variable_check", "tester")
    env_check_binary = os.path.join(PROJECT_DIR, "build", "tests", "integration", "env_variable_check", "env_check")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--tester", default=tester_binary)
    args = parser.parse_args()

    execute_test(args.memgraph, args.tester)

    sys.exit(0)
