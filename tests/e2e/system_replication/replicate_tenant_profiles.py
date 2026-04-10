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

BOLT_PORTS = {"main": 7687, "replica_1": 7688}
REPLICATION_PORTS = {"replica_1": 10001}
file = "replicate_tenant_profiles"


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)
    time.sleep(2)


@pytest.fixture
def test_name(request):
    return request.node.name


def _instances(test_name):
    return {
        "replica_1": {
            "args": ["--bolt-port", f"{BOLT_PORTS['replica_1']}", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(file, test_name)}/replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica1",
            "setup_queries": [f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};"],
        },
        "main": {
            "args": ["--bolt-port", f"{BOLT_PORTS['main']}", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
            ],
        },
    }


def show_replicas_func(cursor):
    def func():
        return execute_and_fetch_all(cursor, "SHOW REPLICAS;")

    return func


def _expected_replicas(ts):
    return [
        (
            "replica_1",
            f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
            "sync",
            {"behind": None, "status": "ready", "ts": ts},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 0}},
        ),
    ]


def _show_tenant_profiles(cursor):
    return execute_and_fetch_all(cursor, "SHOW TENANT PROFILES")


def test_create_tenant_profile_replication(connection, test_name):
    interactive_mg_runner.start_all(_instances(test_name), keep_directories=False)
    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    execute_and_fetch_all(main_cursor, "CREATE TENANT PROFILE repl_prof LIMIT memory_limit 200 MB")
    # 1 REGISTER + 1 CREATE TENANT PROFILE → ts 2
    mg_sleep_and_assert_collection(_expected_replicas(2), show_replicas_func(main_cursor))

    repl_cursor = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    rows = _show_tenant_profiles(repl_cursor)
    assert len(rows) == 1
    assert rows[0][0] == "repl_prof"
    assert "200" in rows[0][1]


def test_alter_tenant_profile_replication(connection, test_name):
    interactive_mg_runner.start_all(_instances(test_name), keep_directories=False)
    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    execute_and_fetch_all(main_cursor, "CREATE TENANT PROFILE alter_prof LIMIT memory_limit 100 MB")
    execute_and_fetch_all(main_cursor, "ALTER TENANT PROFILE alter_prof SET memory_limit 500 MB")
    # 1 REGISTER + 1 CREATE + 1 ALTER → ts 3
    mg_sleep_and_assert_collection(_expected_replicas(3), show_replicas_func(main_cursor))

    repl_cursor = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    rows = _show_tenant_profiles(repl_cursor)
    assert len(rows) == 1
    assert "500" in rows[0][1]


def test_drop_tenant_profile_replication(connection, test_name):
    interactive_mg_runner.start_all(_instances(test_name), keep_directories=False)
    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    execute_and_fetch_all(main_cursor, "CREATE TENANT PROFILE drop_prof LIMIT memory_limit 100 MB")
    execute_and_fetch_all(main_cursor, "DROP TENANT PROFILE drop_prof")
    # 1 REGISTER + 1 CREATE + 1 DROP → ts 3
    mg_sleep_and_assert_collection(_expected_replicas(3), show_replicas_func(main_cursor))

    repl_cursor = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    rows = _show_tenant_profiles(repl_cursor)
    assert len(rows) == 0


def test_set_tenant_profile_on_database_replication(connection, test_name):
    interactive_mg_runner.start_all(_instances(test_name), keep_directories=False)
    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    execute_and_fetch_all(main_cursor, "CREATE TENANT PROFILE db_prof LIMIT memory_limit 300 MB")
    execute_and_fetch_all(main_cursor, "SET TENANT PROFILE ON DATABASE memgraph TO db_prof")
    # 1 REGISTER + 1 CREATE + 1 SET → ts 3
    mg_sleep_and_assert_collection(_expected_replicas(3), show_replicas_func(main_cursor))

    repl_cursor = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    rows = _show_tenant_profiles(repl_cursor)
    assert len(rows) == 1
    assert "memgraph" in rows[0][2]  # databases column


def test_mutation_forbidden_on_replica(connection, test_name):
    interactive_mg_runner.start_all(_instances(test_name), keep_directories=False)
    repl_cursor = connection(BOLT_PORTS["replica_1"], "replica").cursor()

    with pytest.raises(Exception, match="replica"):
        execute_and_fetch_all(repl_cursor, "CREATE TENANT PROFILE forbidden LIMIT memory_limit 100 MB")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
