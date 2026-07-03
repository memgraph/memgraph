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
from functools import partial

import interactive_mg_runner
import pytest
from common import (
    connect,
    execute_and_fetch_all,
    get_data_path,
    get_logs_path,
    show_instances,
    wait_until_main_writeable_assert_replica_down,
)
from mg_utils import mg_sleep_and_assert, mg_sleep_and_assert_until_role_change

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

file = "global_read_only"
test_name = "test_global_read_only"

WRITE_FORBIDDEN_PREFIX = "Write queries currently forbidden on the main instance."


def get_failover_instances_description(test_name: str):
    return {
        "instance_1": {
            "args": [
                "--bolt-port",
                "7688",
                "--log-level",
                "TRACE",
                "--management-port",
                "10011",
                "--replication-restore-state-on-startup=true",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_1",
            "setup_queries": [],
        },
        "instance_3": {
            "args": [
                "--bolt-port",
                "7689",
                "--log-level",
                "TRACE",
                "--management-port",
                "10013",
                "--replication-restore-state-on-startup=true",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_3",
            "setup_queries": [],
        },
        "coordinator_3": {
            "args": [
                "--bolt-port",
                "7692",
                "--log-level=TRACE",
                "--coordinator-id=3",
                "--coordinator-port=10113",
                "--also-log-to-stderr",
                "--coordinator-hostname=localhost",
                "--management-port=10123",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_3",
            "setup_queries": [
                "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}",
                "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
                "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
                "SET INSTANCE instance_3 TO MAIN",
            ],
        },
    }


MEMGRAPH_INSTANCES_DESCRIPTION = {
    "instance_3": {
        "args": [
            "--bolt-port",
            "7689",
            "--log-level",
            "TRACE",
            "--management-port",
            "10013",
            "--also-log-to-stderr",
        ],
        "log_file": f"{get_logs_path(file, test_name)}/instance_3.log",
        "data_directory": f"{get_data_path(file, test_name)}/instance_3",
        "setup_queries": [],
    },
    "coordinator_1": {
        "args": [
            "--bolt-port",
            "7690",
            "--log-level=TRACE",
            "--coordinator-id=1",
            "--coordinator-port=10111",
            "--coordinator-hostname=localhost",
            "--management-port=10121",
        ],
        "log_file": f"{get_logs_path(file, test_name)}/coordinator_1.log",
        "data_directory": f"{get_data_path(file, test_name)}/coordinator_1",
        "setup_queries": [],
    },
    "coordinator_2": {
        "args": [
            "--bolt-port",
            "7691",
            "--log-level=TRACE",
            "--coordinator-id=2",
            "--coordinator-port=10112",
            "--coordinator-hostname=localhost",
            "--management-port=10122",
        ],
        "log_file": f"{get_logs_path(file, test_name)}/coordinator_2.log",
        "data_directory": f"{get_data_path(file, test_name)}/coordinator_2",
        "setup_queries": [],
    },
    "coordinator_3": {
        "args": [
            "--bolt-port",
            "7692",
            "--log-level=TRACE",
            "--coordinator-id=3",
            "--coordinator-port=10113",
            "--also-log-to-stderr",
            "--coordinator-hostname=localhost",
            "--management-port=10123",
        ],
        "log_file": f"{get_logs_path(file, test_name)}/coordinator_3.log",
        "data_directory": f"{get_data_path(file, test_name)}/coordinator_3",
        "setup_queries": [],
    },
}


@pytest.fixture(autouse=True)
def cleanup_after_test():
    # Run the test
    yield
    # Stop + delete directories after running the test
    interactive_mg_runner.kill_all(keep_directories=False)


def write_rejected_with_read_only_message(cursor):
    try:
        execute_and_fetch_all(cursor, "CREATE (n:Node {name: 'node'})")
        return False
    except Exception as e:
        return str(e).startswith(WRITE_FORBIDDEN_PREFIX)


def write_accepted(cursor):
    try:
        execute_and_fetch_all(cursor, "CREATE (n:Node {name: 'node'})")
        return True
    except Exception:
        return False


def test_global_read_only():
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION, keep_directories=False)

    coordinator3_cursor = connect(host="localhost", port=7692).cursor()

    execute_and_fetch_all(
        coordinator3_cursor,
        "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
    )
    execute_and_fetch_all(coordinator3_cursor, "SET INSTANCE instance_3 TO MAIN")
    execute_and_fetch_all(
        coordinator3_cursor,
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
    )
    execute_and_fetch_all(
        coordinator3_cursor,
        "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}",
    )

    expected_cluster_coord3 = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]
    mg_sleep_and_assert(expected_cluster_coord3, partial(show_instances, coordinator3_cursor))

    instance3_cursor = connect(host="localhost", port=7689).cursor()

    # Cluster is read/write by default: writes on the main are accepted.
    mg_sleep_and_assert(True, partial(write_accepted, instance3_cursor))

    # Enabling global read-only mode must, within a reconciliation cycle, cause the main to reject write queries with
    # the neutral read-only error message.
    execute_and_fetch_all(coordinator3_cursor, "SET COORDINATOR SETTING 'global_read_only' TO 'true'")
    settings = dict(execute_and_fetch_all(coordinator3_cursor, "SHOW COORDINATOR SETTINGS"))
    assert settings["global_read_only"] == "true"

    mg_sleep_and_assert(True, partial(write_rejected_with_read_only_message, instance3_cursor))

    # Reads and snapshots must keep working while the cluster is read-only.
    execute_and_fetch_all(instance3_cursor, "MATCH (n) RETURN n LIMIT 1")
    execute_and_fetch_all(instance3_cursor, "CREATE SNAPSHOT")

    # Clearing read-only mode re-enables writes within a reconciliation cycle.
    execute_and_fetch_all(coordinator3_cursor, "SET COORDINATOR SETTING 'global_read_only' TO 'false'")
    settings = dict(execute_and_fetch_all(coordinator3_cursor, "SHOW COORDINATOR SETTINGS"))
    assert settings["global_read_only"] == "false"

    mg_sleep_and_assert(True, partial(write_accepted, instance3_cursor))


def test_global_read_only_honored_across_failover():
    # A cluster that is in read-only mode when its main dies must promote a new main that is also read-only, instead of
    # silently starting to accept writes. Clearing read-only afterwards must re-enable writes on the promoted main.
    memgraph_instances_description = get_failover_instances_description(test_name="test_honored_across_failover")
    interactive_mg_runner.start_all(memgraph_instances_description, keep_directories=False)

    coordinator_cursor = connect(host="localhost", port=7692).cursor()

    expected_cluster = [
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7688", "", "localhost:10011", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]
    mg_sleep_and_assert(expected_cluster, partial(show_instances, coordinator_cursor))

    # instance_3 is the writeable main by default.
    main_cursor = connect(host="localhost", port=7689).cursor()
    mg_sleep_and_assert(True, partial(write_accepted, main_cursor))

    # Freeze the cluster, then kill the main to trigger a failover while read-only is in effect.
    execute_and_fetch_all(coordinator_cursor, "SET COORDINATOR SETTING 'global_read_only' TO 'true'")
    mg_sleep_and_assert(True, partial(write_rejected_with_read_only_message, main_cursor))

    interactive_mg_runner.kill(memgraph_instances_description, "instance_3")

    expected_cluster_after_failover = [
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7688", "", "localhost:10011", "up", "main"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "down", "unknown"),
    ]
    mg_sleep_and_assert(expected_cluster_after_failover, partial(show_instances, coordinator_cursor))

    # The newly promoted main comes up read-only: writes are rejected with the neutral read-only message (a replica
    # would reject with a different message, so this also confirms instance_1 is the main).
    new_main_cursor = connect(host="localhost", port=7688).cursor()
    mg_sleep_and_assert(True, partial(write_rejected_with_read_only_message, new_main_cursor))

    # Clearing read-only mode re-enables writes on the promoted main within a reconciliation cycle. instance_3 (the
    # promoted main's SYNC replica) is still down, so a successful local write surfaces as a SYNC-replication error;
    # the helper treats that as "main is writeable", which is what we assert here.
    execute_and_fetch_all(coordinator_cursor, "SET COORDINATOR SETTING 'global_read_only' TO 'false'")
    wait_until_main_writeable_assert_replica_down(new_main_cursor, "CREATE (n:Node {name: 'after_clear'})")


def test_failover_writeable_when_not_read_only():
    # The mirror image of the read-only failover: when the cluster is NOT read-only, killing the main promotes a
    # writeable new main (today's behavior, preserved by the writing_enabled = !global_read_only projection).
    memgraph_instances_description = get_failover_instances_description(test_name="test_writeable_failover")
    interactive_mg_runner.start_all(memgraph_instances_description, keep_directories=False)

    coordinator_cursor = connect(host="localhost", port=7692).cursor()

    expected_cluster = [
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7688", "", "localhost:10011", "up", "replica"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "up", "main"),
    ]
    mg_sleep_and_assert(expected_cluster, partial(show_instances, coordinator_cursor))

    interactive_mg_runner.kill(memgraph_instances_description, "instance_3")

    expected_cluster_after_failover = [
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7688", "", "localhost:10011", "up", "main"),
        ("instance_3", "localhost:7689", "", "localhost:10013", "down", "unknown"),
    ]
    mg_sleep_and_assert(expected_cluster_after_failover, partial(show_instances, coordinator_cursor))

    # Wait until instance_1 has actually processed the promotion before probing writes, so we don't race the promote
    # RPC and hit the (different) replica-side rejection.
    new_main_cursor = connect(host="localhost", port=7688).cursor()
    mg_sleep_and_assert_until_role_change(
        lambda: execute_and_fetch_all(new_main_cursor, "SHOW REPLICATION ROLE;")[0][0], "main"
    )

    # The promoted main accepts writes (its down SYNC replica surfaces as a replication error, which the helper treats
    # as "main is writeable").
    wait_until_main_writeable_assert_replica_down(new_main_cursor, "CREATE (n:Node {name: 'writeable'})")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
