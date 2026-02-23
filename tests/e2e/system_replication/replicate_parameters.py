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


def _instances_with_recovery(test_name):
    """Instance config with recovery flags so restarts can restore state from disk.
    No setup_queries on replicas so that restart only restores from disk (like multitenancy recovery tests).
    """
    return {
        "replica_1": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_1']}",
                "--log-level=TRACE",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                "false",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica1",
        },
        "replica_2": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_2']}",
                "--log-level=TRACE",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                "false",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/replica2.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica2",
        },
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
                "--data-recovery-on-startup",
                "true",
                "--replication-restore-state-on-startup",
                "true",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [],  # Register replicas manually so restart does not re-run registration
        },
    }


def _expected_replicas_ts(ts):
    return [
        (
            "replica_1",
            f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
            "sync",
            {"behind": None, "status": "ready", "ts": ts},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
        (
            "replica_2",
            f"127.0.0.1:{REPLICATION_PORTS['replica_2']}",
            "async",
            {"behind": None, "status": "ready", "ts": ts},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
    ]


def _expected_replicas_replica1_invalid(ts):
    """After killing replica_1: replica_1 is invalid, replica_2 is ready (main has committed ts)."""
    return [
        (
            "replica_1",
            f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
            "sync",
            {"behind": None, "status": "invalid", "ts": ts},
            {"memgraph": {"behind": 0, "status": "invalid", "ts": 0}},
        ),
        (
            "replica_2",
            f"127.0.0.1:{REPLICATION_PORTS['replica_2']}",
            "async",
            {"behind": None, "status": "ready", "ts": ts},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
    ]


def _show_parameters(cursor):
    return execute_and_fetch_all(cursor, "SHOW PARAMETERS;")


def test_set_parameter_replication(connection, test_name, clean_dirs):
    interactive_mg_runner.start_all(_instances(test_name), keep_directories=False)
    cursor = connection(BOLT_PORTS["main"], "main").cursor()

    execute_and_fetch_all(cursor, 'SET GLOBAL PARAMETER x="value";')
    # REGISTER REPLICA and SET PARAMETER both use system tx: 2 REGISTER (ts 1,2) + 1 SET (ts 3)
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

    execute_and_fetch_all(cursor, "UNSET GLOBAL PARAMETER x;")
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
    # 2 REGISTER + 2 SET + 1 DELETE ALL → ts 5
    mg_sleep_and_assert_collection(_expected_replicas_ts(5), show_replicas_func(cursor))

    for port in (BOLT_PORTS["replica_1"], BOLT_PORTS["replica_2"]):
        rows = _show_parameters(connection(port, "replica").cursor())
        assert len(rows) == 0


def test_parameter_replication_after_replica_recovery(connection, test_name, clean_dirs):
    # Goal: same as test_attempt_to_write_data_on_main_when_sync_replica_is_down (data) and
    #       test_multitenancy_replication_restart_replica_w_fc_w_rec (dbms) — replica down, op on main, recover replica, check sync.
    # 0/ Setup replication (recovery flags so replica can restore role and sync after restart).
    # 1/ Register replicas and wait until ready.
    # 2/ Put down one replica (kill, keep dirs).
    # 3/ Set parameters on main while replica is down.
    # 4/ Recover replica (start) and wait until it syncs.
    # 5/ Assert main and both replicas have the same parameters.
    instances = _instances_with_recovery(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)
    # Set replica role manually (no setup_queries on replicas so restart only restores from disk)
    for name, port in [("replica_1", BOLT_PORTS["replica_1"]), ("replica_2", BOLT_PORTS["replica_2"])]:
        repl_cursor = connection(port, "replica").cursor()
        execute_and_fetch_all(repl_cursor, f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS[name]};")
    cursor = connection(BOLT_PORTS["main"], "main").cursor()

    execute_and_fetch_all(cursor, f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';")
    execute_and_fetch_all(cursor, f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';")
    # REGISTER REPLICA does not add system actions, so ts stays 0 until a system query with actions (e.g. SET PARAMETER) runs
    mg_sleep_and_assert_collection(_expected_replicas_ts(0), show_replicas_func(cursor))

    interactive_mg_runner.kill(instances, "replica_1")
    time.sleep(1)

    execute_and_fetch_all(cursor, 'SET GLOBAL PARAMETER z="set_while_replica_down";')
    execute_and_fetch_all(cursor, 'SET GLOBAL PARAMETER w="second";')

    mg_sleep_and_assert_collection(_expected_replicas_replica1_invalid(4), show_replicas_func(cursor))

    interactive_mg_runner.start(instances, "replica_1")
    time.sleep(2)
    mg_sleep_and_assert_collection(_expected_replicas_ts(4), show_replicas_func(cursor))

    main_params = sorted(_show_parameters(cursor))
    repl1_params = sorted(_show_parameters(connection(BOLT_PORTS["replica_1"], "replica").cursor()))
    repl2_params = sorted(_show_parameters(connection(BOLT_PORTS["replica_2"], "replica").cursor()))
    assert main_params == repl1_params == repl2_params
    assert len(main_params) == 2


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
