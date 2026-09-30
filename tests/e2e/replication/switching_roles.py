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

import os
import sys

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

BOLT_PORTS = {"instance1": 7687, "instance2": 7688}
REPLICATION_PORTS = {"instance1": 10000, "instance2": 10001}
LOG_DIR = "switching_roles"


@pytest.fixture
def test_name(request):
    return request.node.name


@pytest.fixture(autouse=True)
def cleanup_after_test():
    # Run the test
    yield
    # Stop + delete directories after running the test
    interactive_mg_runner.kill_all(keep_directories=False)


def show_replicas_func(cursor):
    def func():
        return execute_and_fetch_all(cursor, "SHOW REPLICAS;")

    return func


def test_switch_main_after_local_snapshot(connection, test_name):
    # Goal: Proof that vector types are replicated to REPLICAs
    # 0/ Setup replication
    # 1/ Write some data to MAIN and wait for it to replicate
    # 2/ Create a bunch of snapshots on MAIN (bumping up the ts)
    # 3/ Switch roles
    # 4/ Write data to new MAIN and wait for it to replicate
    # 5/ Switch roles and restart REPLICA
    # 6/ Validate the data

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "instance2": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['instance2']}",
                "--log-level=TRACE",
            ],
            "log_file": f"{get_logs_path(LOG_DIR, test_name)}/instance2.log",
            "setup_queries": [
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['instance2']};",
            ],
        },
        "instance1": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['instance1']}",
                "--log-level=TRACE",
            ],
            "log_file": f"{get_logs_path(LOG_DIR, test_name)}/instance1.log",
            "setup_queries": [
                f"REGISTER REPLICA replica SYNC TO '127.0.0.1:{REPLICATION_PORTS['instance2']}';",
            ],
        },
    }

    def wait_for_replication_change(cursor, port, ts):
        expected_data = [
            (
                "replica",
                f"127.0.0.1:{port}",
                "sync",
                {"behind": None, "status": "ready", "ts": 0},
                {"memgraph": {"behind": 0, "status": "ready", "ts": ts}},
            ),
        ]
        mg_sleep_and_assert_collection(expected_data, show_replicas_func(cursor))

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL)
    cursor1 = connection(BOLT_PORTS["instance1"], "main").cursor()

    # 1/
    execute_and_fetch_all(cursor1, "UNWIND range(0,9) AS i CREATE({id:i});")
    wait_for_replication_change(cursor1, REPLICATION_PORTS["instance2"], 2)

    # 2/
    execute_and_fetch_all(cursor1, "CREATE SNAPSHOT;")

    # 3/
    execute_and_fetch_all(cursor1, f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['instance1']};")
    cursor1 = connection(BOLT_PORTS["instance1"], "replica").cursor()
    cursor2 = connection(BOLT_PORTS["instance2"], "replica").cursor()
    execute_and_fetch_all(cursor2, f"SET REPLICATION ROLE TO MAIN;")
    cursor2 = connection(BOLT_PORTS["instance2"], "main").cursor()
    execute_and_fetch_all(cursor2, f"REGISTER REPLICA replica SYNC TO '127.0.0.1:{REPLICATION_PORTS['instance1']}';")

    # 4/
    execute_and_fetch_all(cursor2, "UNWIND range(10,19) AS i CREATE({id:i});")
    wait_for_replication_change(cursor2, REPLICATION_PORTS["instance1"], 4)

    # 5/
    interactive_mg_runner.stop(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL, "instance2", keep_directories=False)
    execute_and_fetch_all(cursor1, f"SET REPLICATION ROLE TO MAIN;")
    cursor1 = connection(BOLT_PORTS["instance1"], "main").cursor()
    interactive_mg_runner.start(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL, "instance2")
    execute_and_fetch_all(cursor1, f"REGISTER REPLICA replica SYNC TO '127.0.0.1:{REPLICATION_PORTS['instance2']}';")
    cursor2 = connection(BOLT_PORTS["instance2"], "replica").cursor()
    wait_for_replication_change(cursor1, REPLICATION_PORTS["instance2"], 4)

    # 6/
    assert execute_and_fetch_all(cursor1, f"MATCH(n) RETURN count(*);")[0][0] == 20, "Missing data on instance1"
    assert execute_and_fetch_all(cursor2, f"MATCH(n) RETURN count(*);")[0][0] == 20, "Missing data on instance2"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
