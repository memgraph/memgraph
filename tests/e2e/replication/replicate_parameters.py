# Copyright 2026 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import os
import shutil
import sys
import time

import interactive_mg_runner
import pytest
from common import execute_and_fetch_all, get_data_path, get_logs_path
from mg_utils import mg_sleep_and_assert_collection

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

BOLT_PORTS = {"main": 7687, "replica_1": 7688, "replica_2": 7689}
REPLICATION_PORTS = {"replica_1": 10001, "replica_2": 10002}
file = "replicate_parameters"


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)
    time.sleep(2)


@pytest.fixture
def test_name(request):
    return request.node.name


def _clean_test_dirs(test_name):
    base_data = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", get_data_path(file, test_name))
    base_logs = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "logs", get_logs_path(file, test_name))
    for path in (base_data, base_logs):
        if os.path.exists(path):
            shutil.rmtree(path)


@pytest.fixture
def clean_dirs(test_name):
    _clean_test_dirs(test_name)
    yield


def show_replicas_func(cursor):
    def func():
        return execute_and_fetch_all(cursor, "SHOW REPLICAS;")
    return func


def _instances(test_name):
    return {
        "replica_1": {
            "args": ["--bolt-port", f"{BOLT_PORTS['replica_1']}", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(file, test_name)}/replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica1",
            "setup_queries": [f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};"],
        },
        "replica_2": {
            "args": ["--bolt-port", f"{BOLT_PORTS['replica_2']}", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(file, test_name)}/replica2.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica2",
            "setup_queries": [f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_2']};"],
        },
        "main": {
            "args": ["--bolt-port", f"{BOLT_PORTS['main']}", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
                f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';",
            ],
        },
    }


def _expected_replicas_ts(ts):
    return [
        ("replica_1", f"127.0.0.1:{REPLICATION_PORTS['replica_1']}", "sync", {"behind": None, "status": "ready", "ts": ts}, {"memgraph": {"behind": 0, "status": "ready", "ts": 0}}),
        ("replica_2", f"127.0.0.1:{REPLICATION_PORTS['replica_2']}", "async", {"behind": None, "status": "ready", "ts": ts}, {"memgraph": {"behind": 0, "status": "ready", "ts": 0}}),
    ]


def _show_parameters(cursor):
    return execute_and_fetch_all(cursor, "SHOW PARAMETERS;")


def test_set_parameter_replication(connection, test_name, clean_dirs):
    interactive_mg_runner.start_all(_instances(test_name), keep_directories=False)
    cursor = connection(BOLT_PORTS["main"], "main").cursor()

    execute_and_fetch_all(cursor, 'SET GLOBAL PARAMETER x="value";')
    # Main runs 2x REGISTER REPLICA (ts 1, 2) then SET PARAMETER (ts 3)
    mg_sleep_and_assert_collection(_expected_replicas_ts(3), show_replicas_func(cursor))

    for port in (BOLT_PORTS["replica_1"], BOLT_PORTS["replica_2"]):
        repl_cursor = connection(port, "replica").cursor()
        rows = _show_parameters(repl_cursor)
        assert len(rows) == 1
        assert rows[0][0] == "x"


def test_unset_parameter_replication(connection, test_name, clean_dirs):
    interactive_mg_runner.start_all(_instances(test_name), keep_directories=False)
    cursor = connection(BOLT_PORTS["main"], "main").cursor()

    execute_and_fetch_all(cursor, 'SET GLOBAL PARAMETER x="value";')
    mg_sleep_and_assert_collection(_expected_replicas_ts(3), show_replicas_func(cursor))

    execute_and_fetch_all(cursor, "UNSET PARAMETER x;")
    mg_sleep_and_assert_collection(_expected_replicas_ts(4), show_replicas_func(cursor))

    for port in (BOLT_PORTS["replica_1"], BOLT_PORTS["replica_2"]):
        rows = _show_parameters(connection(port, "replica").cursor())
        assert len(rows) == 0


def test_delete_all_parameters_replication(connection, test_name, clean_dirs):
    interactive_mg_runner.start_all(_instances(test_name), keep_directories=False)
    cursor = connection(BOLT_PORTS["main"], "main").cursor()

    execute_and_fetch_all(cursor, 'SET GLOBAL PARAMETER a="1";')
    execute_and_fetch_all(cursor, 'SET GLOBAL PARAMETER b="2";')
    mg_sleep_and_assert_collection(_expected_replicas_ts(4), show_replicas_func(cursor))

    execute_and_fetch_all(cursor, "DELETE ALL PARAMETERS;")
    # After 2 REGISTER + 2 SET + 1 DELETE ALL: ts 5
    mg_sleep_and_assert_collection(_expected_replicas_ts(5), show_replicas_func(cursor))

    for port in (BOLT_PORTS["replica_1"], BOLT_PORTS["replica_2"]):
        rows = _show_parameters(connection(port, "replica").cursor())
        assert len(rows) == 0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
