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
import atexit
import fcntl
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


def execute_query(binary: str, queries: List[str], username: str = "", password: str = "") -> None:
    args = [binary, "--username", username, "--password", password]
    args.extend(queries)
    subprocess.run(args).check_returncode()


def make_non_blocking(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)


def start_memgraph(memgraph_args: List[any]) -> subprocess:
    memgraph = subprocess.Popen(
        list(map(str, memgraph_args)), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    time.sleep(0.1)
    assert memgraph.poll() is None, "Memgraph process died prematurely!"
    wait_for_server(7687)
    # Make the stdout and stderr pipes non-blocking
    make_non_blocking(memgraph.stdout.fileno())
    make_non_blocking(memgraph.stderr.fileno())
    return memgraph


def check_flag(tester_binary: str, flag: str, value: str) -> None:
    args = [tester_binary, "--field", flag, "--value", value]
    subprocess.run(args).check_returncode()


def cleanup(memgraph: subprocess):
    if memgraph.poll() is None:
        pid = memgraph.pid
        try:
            os.kill(pid, SIGNAL_SIGTERM)
        except os.OSError:
            assert False, "Memgraph process didn't exit cleanly!"
        time.sleep(1)


def run_test(tester_binary: str, memgraph_args: List[str], server_name: str, query_tx: str):
    memgraph = start_memgraph(memgraph_args)
    atexit.register(cleanup, memgraph)
    check_flag(tester_binary, "server.name", server_name)
    check_flag(tester_binary, "query.timeout", query_tx)
    cleanup(memgraph)
    atexit.unregister(cleanup)


def run_test_w_query(tester_binary: str, memgraph_args: List[str], executor_binary: str):
    memgraph = start_memgraph(memgraph_args)
    atexit.register(cleanup, memgraph)
    execute_query(executor_binary, ["SET DATABASE SETTING 'server.name' TO 'New Name';"])
    execute_query(executor_binary, ["SET DATABASE SETTING 'query.timeout' TO '123';"])
    check_flag(tester_binary, "server.name", "New Name")
    check_flag(tester_binary, "query.timeout", "123")
    cleanup(memgraph)
    atexit.unregister(cleanup)


def consume(stream):
    res = []
    while True:
        line = stream.readline()
        if not line:
            break
        res.append(line.strip())
    return res


def run_log_test(tester_binary: str, memgraph_args: List[str], executor_binary: str):
    # Test if command line parameters work
    memgraph = start_memgraph(memgraph_args + ["--log-level", "TRACE", "--also-log-to-stderr"])
    atexit.register(cleanup, memgraph)
    std_err = consume(memgraph.stderr)
    assert len(std_err) > 5, "Failed to log to stderr"
    # Test if run-time setting log.to_stderr works
    execute_query(executor_binary, ["SET DATABASE SETTING 'log.to_stderr' TO 'false';"])
    consume(memgraph.stderr)
    execute_query(executor_binary, ["SET DATABASE SETTING 'query.timeout' TO '123';"])
    std_err = consume(memgraph.stderr)
    assert len(std_err) == 0, "Still writing to stderr even after disabling it"
    # Test if run-time setting log.level works
    execute_query(executor_binary, ["SET DATABASE SETTING 'log.to_stderr' TO 'true';"])
    execute_query(executor_binary, ["SET DATABASE SETTING 'log.level' TO 'CRITICAL';"])
    consume(memgraph.stderr)
    execute_query(executor_binary, ["SET DATABASE SETTING 'query.timeout' TO '123';"])
    std_err = consume(memgraph.stderr)
    assert len(std_err) == 0, "Log level not updated"
    # Tets that unsupported values cause an exception
    execute_tester(
        tester_binary,
        ["SET DATABASE SETTING 'log.to_stderr' TO 'something'"],
        should_fail=True,
        failure_message="'something' not valid for 'log.to_stderr'",
    )
    execute_tester(
        tester_binary,
        ["SET DATABASE SETTING 'log.level' TO 'something'"],
        should_fail=True,
        failure_message="'something' not valid for 'log.level'",
    )
    cleanup(memgraph)
    atexit.unregister(cleanup)


def execute_test(memgraph_binary: str, tester_binary: str, flag_tester_binary: str, executor_binary: str) -> None:
    storage_directory = tempfile.TemporaryDirectory()
    memgraph_args = [memgraph_binary, "--data-directory", storage_directory.name]

    print("\033[1;36m~~ Starting run-time settings check test ~~\033[0m")

    print("\033[1;34m~~ server.name and query.timeout ~~\033[0m")
    # Check default flags
    run_test(flag_tester_binary, memgraph_args, "Neo4j/v5.11.0 compatible graph database server - Memgraph", "600")

    # Check changing flags via command-line arguments
    run_test(
        flag_tester_binary,
        memgraph_args + ["--bolt-server-name-for-init", "Memgraph", "--query-execution-timeout-sec", "1000"],
        "Memgraph",
        "1000",
    )

    # Check changing flags via query
    run_test_w_query(flag_tester_binary, memgraph_args, executor_binary)

    print("\033[1;34m~~ log.level and log.to_stderr ~~\033[0m")
    # Check log settings
    run_log_test(tester_binary, memgraph_args, executor_binary)

    print("\033[1;36m~~ Finished run-time settings check test ~~\033[0m")


if __name__ == "__main__":
    memgraph_binary = os.path.join(PROJECT_DIR, "build", "memgraph")
    tester_binary = os.path.join(PROJECT_DIR, "build", "tests", "integration", "run_time_settings", "tester")
    flag_tester_binary = os.path.join(PROJECT_DIR, "build", "tests", "integration", "run_time_settings", "flag_tester")
    executor_binary = os.path.join(PROJECT_DIR, "build", "tests", "integration", "run_time_settings", "executor")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--tester", default=tester_binary)
    parser.add_argument("--flag_tester", default=flag_tester_binary)
    parser.add_argument("--executor", default=executor_binary)
    args = parser.parse_args()

    execute_test(args.memgraph, args.tester, args.flag_tester, args.executor)

    sys.exit(0)
