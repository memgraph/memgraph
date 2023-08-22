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


def wait_for_server(port: int, delay: float = 0.1) -> float:
    cmd = ["nc", "-z", "-w", "1", "127.0.0.1", str(port)]
    while subprocess.call(cmd) != 0:
        time.sleep(0.01)
    time.sleep(delay)


def execute_query(binary: str, queries: List[str], username: str = "", password: str = "") -> None:
    args = [binary, "--username", username, "--password", password]
    args.extend(queries)
    subprocess.run(args).check_returncode()


def start_memgraph(memgraph_args: List[any]) -> subprocess:
    memgraph = subprocess.Popen(list(map(str, memgraph_args)))
    time.sleep(0.1)
    assert memgraph.poll() is None, "Memgraph process died prematurely!"
    wait_for_server(7687)

    return memgraph


def check_flag(tester_binary: str, flag: str, value: str) -> None:
    args = [tester_binary, "--field", flag, "--value", value]
    subprocess.run(args).check_returncode()


def cleanup(memgraph: subprocess):
    if memgraph.poll() is None:
        memgraph.terminate()
    assert memgraph.wait() == 0, "Memgraph process didn't exit cleanly!"


def run_test(tester_binary: str, memgraph_args: List[str], server_name: str, query_tx: str):
    memgraph = start_memgraph(memgraph_args)
    check_flag(tester_binary, "server.name", server_name)
    check_flag(tester_binary, "query.timeout", query_tx)
    cleanup(memgraph)


def run_test_w_query(tester_binary: str, memgraph_args: List[str]):
    memgraph = start_memgraph(memgraph_args)
    execute_query(tester_binary, ["SET DATABASE SETTING 'server.name' TO 'New Name';"])
    execute_query(tester_binary, ["SET DATABASE SETTING 'query.timeout' TO '123';"])
    check_flag(tester_binary, "server.name", "New Name")
    check_flag(tester_binary, "query.timeout", "123")
    cleanup(memgraph)


def execute_test(memgraph_binary: str, tester_binary: str, flag_checker_binary: str) -> None:
    storage_directory = tempfile.TemporaryDirectory()
    memgraph_args = [memgraph_binary, "--data-directory", storage_directory.name]

    print("\033[1;36m~~ Starting run-time settings check test ~~\033[0m")

    # Check default flags
    run_test(tester_binary, memgraph_args, "Neo4j/v5.11.0 compatible graph database server - memgraph", "600")

    # Check changing flags via command-line arguments
    run_test(
        tester_binary,
        memgraph_args + ["--bolt-server-name-for-init", "Memgraph", "--query-execution-timeout-sec", "1000"],
        "Memgraph",
        "1000",
    )

    # Check changing flags via query
    run_test_w_query(tester_binary, memgraph_args)

    print("\033[1;36m~~ Finished run-time settings check test ~~\033[0m")


if __name__ == "__main__":
    memgraph_binary = os.path.join(PROJECT_DIR, "build", "memgraph")
    tester_binary = os.path.join(PROJECT_DIR, "build", "tests", "integration", "run_time_settings", "tester")
    executor_binary = os.path.join(PROJECT_DIR, "build", "tests", "integration", "run_time_settings", "executor")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--tester", default=tester_binary)
    parser.add_argument("--executor", default=executor_binary)
    args = parser.parse_args()

    execute_test(args.memgraph, args.tester, args.executor)

    sys.exit(0)
