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
import glob
import os
import subprocess
import sys
import tempfile
import time
from typing import List

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", ".."))
SIGNAL_SIGTERM = 15
BOLT_PORT = int(os.environ.get("MG_INTEGRATION_BOLT_PORT", 7687))
MONITORING_PORT = int(os.environ.get("MG_INTEGRATION_MONITORING_PORT", 7444))
METRICS_PORT = int(os.environ.get("MG_INTEGRATION_METRICS_PORT", 9091))
PORT_ARGS = [f"--bolt-port={BOLT_PORT}", f"--monitoring-port={MONITORING_PORT}", f"--metrics-port={METRICS_PORT}"]


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
    args = [binary, "--port", str(BOLT_PORT), "--username", username, "--password", password]
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
    args = [binary, "--port", str(BOLT_PORT), "--username", username, "--password", password]
    args.extend(queries)
    subprocess.run(args).check_returncode()


def make_non_blocking(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)


def start_memgraph(memgraph_args: List[any], env=None) -> subprocess:
    memgraph = subprocess.Popen(
        list(map(str, memgraph_args)), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env
    )
    time.sleep(0.1)
    assert memgraph.poll() is None, "Memgraph process died prematurely!"
    wait_for_server(BOLT_PORT)
    # Make the stdout and stderr pipes non-blocking
    make_non_blocking(memgraph.stdout.fileno())
    make_non_blocking(memgraph.stderr.fileno())
    return memgraph


def check_flag(tester_binary: str, flag: str, value: str) -> None:
    args = [tester_binary, "--port", str(BOLT_PORT), "--field", flag, "--value", value]
    subprocess.run(args).check_returncode()


def check_config(tester_binary: str, flag: str, value: str) -> None:
    args = [tester_binary, "--port", str(BOLT_PORT), "--config", flag, "--value", value]
    subprocess.run(args).check_returncode()


def cleanup(memgraph: subprocess):
    if memgraph.poll() is None:
        pid = memgraph.pid
        try:
            os.kill(pid, SIGNAL_SIGTERM)
        except os.OSError:
            assert False, "Memgraph process didn't exit cleanly!"
        time.sleep(1)


def stop(memgraph: subprocess):
    cleanup(memgraph)
    atexit.unregister(cleanup)
    # The settings store must be released before anything else opens it
    memgraph.wait(timeout=30)


def store_get(store_binary: str, data_directory: str, key: str):
    args = [store_binary, "--data-directory", data_directory, "--key", key]
    result = subprocess.run(args, capture_output=True, text=True)
    if result.returncode == 2:
        return None
    result.check_returncode()
    return result.stdout.strip()


def store_put(store_binary: str, data_directory: str, key: str, value: str) -> None:
    args = [store_binary, "--data-directory", data_directory, "--key", key, "--value", value, "--put"]
    subprocess.run(args).check_returncode()


def run_test(
    tester_binary: str, memgraph_args: List[str], server_name: str, query_tx: str, storage_access_timeout: str = "1"
):
    memgraph = start_memgraph(memgraph_args)
    atexit.register(cleanup, memgraph)
    check_flag(tester_binary, "server.name", server_name)
    check_flag(tester_binary, "query.timeout", query_tx)
    check_flag(tester_binary, "storage.access_timeout_sec", storage_access_timeout)
    cleanup(memgraph)
    atexit.unregister(cleanup)


def run_test_w_query(tester_binary: str, memgraph_args: List[str], executor_binary: str):
    memgraph = start_memgraph(memgraph_args)
    atexit.register(cleanup, memgraph)
    execute_query(executor_binary, ["SET DATABASE SETTING 'server.name' TO 'New Name';"])
    execute_query(executor_binary, ["SET DATABASE SETTING 'query.timeout' TO '123';"])
    execute_query(executor_binary, ["SET DATABASE SETTING 'storage.access_timeout_sec' TO '30';"])
    check_flag(tester_binary, "server.name", "New Name")
    check_flag(tester_binary, "query.timeout", "123")
    check_flag(tester_binary, "storage.access_timeout_sec", "30")
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
        failure_message="Cannot update setting 'log.to_stderr': Boolean value supports only 'false' or 'true' as the input.",
    )
    execute_tester(
        tester_binary,
        ["SET DATABASE SETTING 'log.level' TO 'something'"],
        should_fail=True,
        failure_message="Cannot update setting 'log.level': Unsupported log level. Log level must be defined as one of the following strings: TRACE, DEBUG, INFO, WARNING, ERROR, CRITICAL",
    )
    cleanup(memgraph)
    atexit.unregister(cleanup)


def run_check_config(tester_binary: str, memgraph_args: List[str], executor_binary: str):
    memgraph = start_memgraph(memgraph_args)
    atexit.register(cleanup, memgraph)
    execute_query(executor_binary, ["SET DATABASE SETTING 'server.name' TO 'New Name';"])
    execute_query(executor_binary, ["SET DATABASE SETTING 'query.timeout' TO '123';"])
    execute_query(executor_binary, ["SET DATABASE SETTING 'log.level' TO 'CRITICAL';"])
    execute_query(executor_binary, ["SET DATABASE SETTING 'storage.access_timeout_sec' TO '30';"])
    check_config(tester_binary, "bolt_server_name_for_init", "New Name")
    check_config(tester_binary, "query_execution_timeout_sec", "123")
    check_config(tester_binary, "log_level", "CRITICAL")
    check_config(tester_binary, "storage_access_timeout_sec", "30")
    cleanup(memgraph)
    atexit.unregister(cleanup)


def run_storage_access_validation_test(tester_binary: str, memgraph_args: List[str]):
    memgraph = start_memgraph(memgraph_args)
    atexit.register(cleanup, memgraph)
    # Valid boundary values
    execute_tester(tester_binary, ["SET DATABASE SETTING 'storage.access_timeout_sec' TO '1'"])
    execute_tester(tester_binary, ["SET DATABASE SETTING 'storage.access_timeout_sec' TO '1000000'"])
    # Out of range
    execute_tester(
        tester_binary,
        ["SET DATABASE SETTING 'storage.access_timeout_sec' TO '0'"],
        should_fail=True,
        failure_message="Cannot update setting 'storage.access_timeout_sec': storage.access_timeout_sec must be in range [1, 1000000]",
    )
    execute_tester(
        tester_binary,
        ["SET DATABASE SETTING 'storage.access_timeout_sec' TO '1000001'"],
        should_fail=True,
        failure_message="Cannot update setting 'storage.access_timeout_sec': storage.access_timeout_sec must be in range [1, 1000000]",
    )
    # Non-integer
    execute_tester(
        tester_binary,
        ["SET DATABASE SETTING 'storage.access_timeout_sec' TO 'abc'"],
        should_fail=True,
        failure_message="Cannot update setting 'storage.access_timeout_sec': storage.access_timeout_sec must be a valid unsigned integer",
    )
    cleanup(memgraph)
    atexit.unregister(cleanup)


def run_persistence_test(
    flag_tester_binary: str,
    memgraph_args: List[str],
    executor_binary: str,
    store_binary: str,
    data_directory: str,
    default_server_name: str,
):
    # A license from the environment would overwrite organization.name on every start
    env = {
        k: v for k, v in os.environ.items() if k not in ("MEMGRAPH_ENTERPRISE_LICENSE", "MEMGRAPH_ORGANIZATION_NAME")
    }
    # The daily log sink inserts the date into the file name
    memgraph_args = memgraph_args + ["--log-file", os.path.join(data_directory, "memgraph.log")]

    def read_log() -> str:
        log_files = glob.glob(os.path.join(data_directory, "memgraph*.log"))
        assert log_files, "No log file written"
        return "".join(open(path).read() for path in log_files)

    def start(extra_args: List[str] = []):
        memgraph = start_memgraph(memgraph_args + extra_args, env)
        atexit.register(cleanup, memgraph)
        return memgraph

    # Run-time changes do not survive a restart; only the license does
    memgraph = start()
    execute_query(executor_binary, ["SET DATABASE SETTING 'server.name' TO 'New Name';"])
    execute_query(executor_binary, ["SET DATABASE SETTING 'query.timeout' TO '123';"])
    execute_query(executor_binary, ["SET DATABASE SETTING 'organization.name' TO 'Memgraph Ltd';"])
    stop(memgraph)
    assert store_get(store_binary, data_directory, "server.name") is None, "server.name was written to the store"
    assert store_get(store_binary, data_directory, "query.timeout") is None, "query.timeout was written to the store"
    assert store_get(store_binary, data_directory, "organization.name") == "Memgraph Ltd"
    memgraph = start()
    check_flag(flag_tester_binary, "server.name", default_server_name)
    check_flag(flag_tester_binary, "query.timeout", "600")
    check_flag(flag_tester_binary, "organization.name", "Memgraph Ltd")
    stop(memgraph)

    # A value persisted by an older version is still restored, with a warning, while a value that was never
    # restored is dropped
    store_put(store_binary, data_directory, "server.name", "Old Name")
    store_put(store_binary, data_directory, "query.timeout", "321")
    memgraph = start()
    check_flag(flag_tester_binary, "server.name", "Old Name")
    check_flag(flag_tester_binary, "query.timeout", "600")
    stop(memgraph)
    log = read_log()
    assert "Setting 'server.name' was restored from the data directory" in log, "Missing deprecation warning"
    assert "--bolt-server-name-for-init=Old Name" in log, "Deprecation warning does not name the flag"
    assert "Setting 'query.timeout' was restored" not in log, "query.timeout was never restorable"
    assert store_get(store_binary, data_directory, "server.name") == "Old Name", "Leftover dropped too early"
    assert store_get(store_binary, data_directory, "query.timeout") is None, "Leftover of a run-time only setting kept"

    # The leftover keeps being restored until the flag takes over
    memgraph = start()
    check_flag(flag_tester_binary, "server.name", "Old Name")
    stop(memgraph)
    memgraph = start(["--bolt-server-name-for-init", "Flag Name"])
    check_flag(flag_tester_binary, "server.name", "Flag Name")
    stop(memgraph)
    assert store_get(store_binary, data_directory, "server.name") is None, "Passing the flag did not drop the leftover"
    memgraph = start()
    check_flag(flag_tester_binary, "server.name", default_server_name)
    stop(memgraph)


def execute_test(
    memgraph_binary: str,
    tester_binary: str,
    flag_tester_binary: str,
    executor_binary: str,
    test_config_binary: str,
    store_binary: str,
) -> None:
    storage_directory = tempfile.TemporaryDirectory()
    memgraph_args = [
        memgraph_binary,
        "--data-directory",
        storage_directory.name,
        "--metrics-format=OpenMetrics",
        *PORT_ARGS,
    ]

    print("\033[1;36m~~ Starting run-time settings check test ~~\033[0m")

    default_server_name = "Neo4j/v5.11.0 compatible graph database server - Memgraph"

    print("\033[1;34m~~ server.name and query.timeout ~~\033[0m")
    # Check default flags
    run_test(flag_tester_binary, memgraph_args, default_server_name, "600")

    # Check changing flags via command-line arguments
    run_test(
        flag_tester_binary,
        memgraph_args
        + [
            "--bolt-server-name-for-init",
            "Memgraph",
            "--query-execution-timeout-sec",
            "1000",
            "--storage-access-timeout-sec",
            "60",
        ],
        "Memgraph",
        "1000",
        "60",
    )

    # Check changing flags via query
    run_test_w_query(flag_tester_binary, memgraph_args, executor_binary)

    print("\033[1;34m~~ log.level and log.to_stderr ~~\033[0m")
    # Check log settings
    run_log_test(tester_binary, memgraph_args, executor_binary)

    print("\033[1;34m~~ check show config ~~\033[0m")
    # Check log settings
    run_check_config(test_config_binary, memgraph_args, executor_binary)

    print("\033[1;34m~~ storage.access_timeout_sec validation ~~\033[0m")
    run_storage_access_validation_test(tester_binary, memgraph_args)

    print("\033[1;34m~~ persistence across restarts ~~\033[0m")
    run_persistence_test(
        flag_tester_binary, memgraph_args, executor_binary, store_binary, storage_directory.name, default_server_name
    )

    print("\033[1;36m~~ Finished run-time settings check test ~~\033[0m")


if __name__ == "__main__":
    memgraph_binary = os.path.join(PROJECT_DIR, "build", "memgraph")
    tester_binary = os.path.join(PROJECT_DIR, "build", "tests", "integration", "run_time_settings", "tester")
    flag_tester_binary = os.path.join(PROJECT_DIR, "build", "tests", "integration", "run_time_settings", "flag_tester")
    executor_binary = os.path.join(PROJECT_DIR, "build", "tests", "integration", "run_time_settings", "executor")
    config_checker_binary = os.path.join(
        PROJECT_DIR, "build", "tests", "integration", "run_time_settings", "config_checker"
    )
    settings_store_binary = os.path.join(
        PROJECT_DIR, "build", "tests", "integration", "run_time_settings", "settings_store"
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--tester", default=tester_binary)
    parser.add_argument("--flag_tester", default=flag_tester_binary)
    parser.add_argument("--executor", default=executor_binary)
    parser.add_argument("--config_checker", default=config_checker_binary)
    parser.add_argument("--settings_store", default=settings_store_binary)
    args = parser.parse_args()

    execute_test(args.memgraph, args.tester, args.flag_tester, args.executor, args.config_checker, args.settings_store)

    sys.exit(0)
