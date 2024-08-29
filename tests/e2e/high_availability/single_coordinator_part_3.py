# Copyright 2022 Memgraph Ltd.
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
import tempfile

import interactive_mg_runner
import pytest
from common import (
    connect,
    execute_and_fetch_all,
    ignore_elapsed_time_from_results,
    safe_execute,
)
from mg_utils import mg_sleep_and_assert, mg_sleep_and_assert_collection

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

TEMP_DIR = tempfile.TemporaryDirectory().name


def get_memgraph_instances_description(test_name: str, data_recovery_on_startup: str = "false"):
    return {
        "instance_1": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7688",
                "--log-level",
                "TRACE",
                "--management-port",
                "10011",
                "--replication-restore-state-on-startup=true",
                f"--data-recovery-on-startup={data_recovery_on_startup}",
            ],
            "log_file": f"high_availability/single_coordinator/{test_name}/instance_1.log",
            "data_directory": f"{TEMP_DIR}/instance_1",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7689",
                "--log-level",
                "TRACE",
                "--management-port",
                "10012",
                "--replication-restore-state-on-startup=true",
                f"--data-recovery-on-startup={data_recovery_on_startup}",
            ],
            "log_file": f"high_availability/single_coordinator/{test_name}/instance_2.log",
            "data_directory": f"{TEMP_DIR}/instance_2",
            "setup_queries": [],
        },
        "instance_3": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7687",
                "--log-level",
                "TRACE",
                "--management-port",
                "10013",
                "--replication-restore-state-on-startup=true",
                f"--data-recovery-on-startup={data_recovery_on_startup}",
            ],
            "log_file": f"high_availability/single_coordinator/{test_name}/instance_3.log",
            "data_directory": f"{TEMP_DIR}/instance_3",
            "setup_queries": [],
        },
        "coordinator": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7690",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10111",
                "--coordinator-hostname=localhost",
                "--management-port=10121",
            ],
            "log_file": f"high_availability/single_coordinator/{test_name}/coordinator.log",
            "setup_queries": [
                "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
                "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
                "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
                "SET INSTANCE instance_3 TO MAIN",
            ],
        },
    }


def get_memgraph_instances_description_4_instances(test_name: str, data_recovery_on_startup: str = "false"):
    return {
        "instance_1": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7688",
                "--log-level",
                "TRACE",
                "--management-port",
                "10011",
                "--replication-restore-state-on-startup",
                "true",
                f"--data-recovery-on-startup={data_recovery_on_startup}",
            ],
            "log_file": f"high_availability/single_coordinator/{test_name}/instance_1.log",
            "data_directory": f"{TEMP_DIR}/instance_1",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7689",
                "--log-level",
                "TRACE",
                "--management-port",
                "10012",
                "--replication-restore-state-on-startup",
                "true",
                f"--data-recovery-on-startup={data_recovery_on_startup}",
            ],
            "log_file": f"high_availability/single_coordinator/{test_name}/instance_2.log",
            "data_directory": f"{TEMP_DIR}/instance_2",
            "setup_queries": [],
        },
        "instance_3": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7687",
                "--log-level",
                "TRACE",
                "--management-port",
                "10013",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                f"{data_recovery_on_startup}",
            ],
            "log_file": f"high_availability/single_coordinator/{test_name}/instance_3.log",
            "data_directory": f"{TEMP_DIR}/instance_3",
            "setup_queries": [],
        },
        "instance_4": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7691",
                "--log-level",
                "TRACE",
                "--management-port",
                "10014",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                f"{data_recovery_on_startup}",
            ],
            "log_file": f"high_availability/single_coordinator/{test_name}/instance_4.log",
            "data_directory": f"{TEMP_DIR}/instance_4",
            "setup_queries": [],
        },
        "coordinator": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7690",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10111",
                "--coordinator-hostname=localhost",
                "--management-port=10121",
            ],
            "log_file": f"high_availability/single_coordinator/{test_name}/coordinator.log",
            "setup_queries": [
                "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
                "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
                "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
                "REGISTER INSTANCE instance_4 WITH CONFIG {'bolt_server': 'localhost:7691', 'management_server': 'localhost:10014', 'replication_server': 'localhost:10004'};",
                "SET INSTANCE instance_3 TO MAIN",
            ],
        },
    }


def test_simple_automatic_failover():
    safe_execute(shutil.rmtree, TEMP_DIR)
    memgraph_instances_description = get_memgraph_instances_description(test_name="test_simple_automatic_failover")
    interactive_mg_runner.start_all(memgraph_instances_description, keep_directories=False)

    main_cursor = connect(host="localhost", port=7687).cursor()
    expected_data_on_main = [
        (
            "instance_1",
            "localhost:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "instance_2",
            "localhost:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]

    def main_cursor_show_replicas():
        return sorted(list(execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")))

    mg_sleep_and_assert_collection(expected_data_on_main, main_cursor_show_replicas)

    interactive_mg_runner.kill(memgraph_instances_description, "instance_3")

    coord_cursor = connect(host="localhost", port=7690).cursor()

    def retrieve_data_show_repl_cluster():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;"))))

    expected_data_on_coord = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader"),
        ("instance_1", "localhost:7688", "", "localhost:10011", "up", "main"),
        ("instance_2", "localhost:7689", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7687", "", "localhost:10013", "down", "unknown"),
    ]
    mg_sleep_and_assert(expected_data_on_coord, retrieve_data_show_repl_cluster)

    new_main_cursor = connect(host="localhost", port=7688).cursor()

    def retrieve_data_show_replicas():
        return sorted(list(execute_and_fetch_all(new_main_cursor, "SHOW REPLICAS;")))

    expected_data_on_new_main = [
        (
            "instance_2",
            "localhost:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "instance_3",
            "localhost:10003",
            "sync",
            {"ts": 0, "behind": None, "status": "invalid"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "invalid"}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data_on_new_main, retrieve_data_show_replicas)

    interactive_mg_runner.start(memgraph_instances_description, "instance_3")
    expected_data_on_new_main_old_alive = [
        (
            "instance_2",
            "localhost:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "instance_3",
            "localhost:10003",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]

    mg_sleep_and_assert_collection(expected_data_on_new_main_old_alive, retrieve_data_show_replicas)


def test_registering_replica_fails_name_exists():
    safe_execute(shutil.rmtree, TEMP_DIR)

    memgraph_instances_description = get_memgraph_instances_description(
        test_name="test_registering_replica_fails_name_exists"
    )
    interactive_mg_runner.start_all(memgraph_instances_description, keep_directories=False)

    coord_cursor = connect(host="localhost", port=7690).cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(
            coord_cursor,
            "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7693', 'management_server': 'localhost:10051', 'replication_server': 'localhost:10111'};",
        )
    assert str(e.value) == "Couldn't register replica instance since instance with such name already exists!"
    shutil.rmtree(TEMP_DIR)


def test_registering_replica_fails_endpoint_exists():
    safe_execute(shutil.rmtree, TEMP_DIR)
    memgraph_instances_description = get_memgraph_instances_description(
        test_name="test_registering_replica_fails_endpoint_exists"
    )

    interactive_mg_runner.start_all(memgraph_instances_description, keep_directories=False)

    coord_cursor = connect(host="localhost", port=7690).cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(
            coord_cursor,
            "REGISTER INSTANCE instance_5 WITH CONFIG {'bolt_server': 'localhost:7693', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10005'};",
        )
    assert (
        str(e.value) == "Couldn't register replica instance since instance with such management server already exists!"
    )


def test_replica_instance_restarts():
    safe_execute(shutil.rmtree, TEMP_DIR)
    memgraph_instances_description = get_memgraph_instances_description(test_name="test_replica_instance_restarts")
    interactive_mg_runner.start_all(memgraph_instances_description, keep_directories=False)

    cursor = connect(host="localhost", port=7690).cursor()

    def show_repl_cluster():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(cursor, "SHOW INSTANCES;"))))

    expected_data_up = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader"),
        ("instance_1", "localhost:7688", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7689", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7687", "", "localhost:10013", "up", "main"),
    ]
    mg_sleep_and_assert(expected_data_up, show_repl_cluster)

    interactive_mg_runner.kill(memgraph_instances_description, "instance_1")

    expected_data_down = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader"),
        ("instance_1", "localhost:7688", "", "localhost:10011", "down", "unknown"),
        ("instance_2", "localhost:7689", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7687", "", "localhost:10013", "up", "main"),
    ]
    mg_sleep_and_assert(expected_data_down, show_repl_cluster)

    interactive_mg_runner.start(memgraph_instances_description, "instance_1")

    mg_sleep_and_assert(expected_data_up, show_repl_cluster)

    instance1_cursor = connect(host="localhost", port=7688).cursor()

    def retrieve_data_show_repl_role_instance1():
        return sorted(list(execute_and_fetch_all(instance1_cursor, "SHOW REPLICATION ROLE;")))

    expected_data_replica = [("replica",)]
    mg_sleep_and_assert(expected_data_replica, retrieve_data_show_repl_role_instance1)


def test_automatic_failover_main_back_as_replica():
    safe_execute(shutil.rmtree, TEMP_DIR)
    memgraph_instances_description = get_memgraph_instances_description(
        test_name="test_automatic_failover_main_back_as_replica"
    )

    interactive_mg_runner.start_all(memgraph_instances_description, keep_directories=False)

    interactive_mg_runner.kill(memgraph_instances_description, "instance_3")

    coord_cursor = connect(host="localhost", port=7690).cursor()

    def retrieve_data_show_repl_cluster():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;"))))

    expected_data_after_failover = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader"),
        ("instance_1", "localhost:7688", "", "localhost:10011", "up", "main"),
        ("instance_2", "localhost:7689", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7687", "", "localhost:10013", "down", "unknown"),
    ]
    mg_sleep_and_assert(expected_data_after_failover, retrieve_data_show_repl_cluster)

    expected_data_after_main_coming_back = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader"),
        ("instance_1", "localhost:7688", "", "localhost:10011", "up", "main"),
        ("instance_2", "localhost:7689", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7687", "", "localhost:10013", "up", "replica"),
    ]

    interactive_mg_runner.start(memgraph_instances_description, "instance_3")
    mg_sleep_and_assert(expected_data_after_main_coming_back, retrieve_data_show_repl_cluster)

    instance3_cursor = connect(host="localhost", port=7687).cursor()

    def retrieve_data_show_repl_role_instance3():
        return sorted(list(execute_and_fetch_all(instance3_cursor, "SHOW REPLICATION ROLE;")))

    mg_sleep_and_assert([("replica",)], retrieve_data_show_repl_role_instance3)


def test_automatic_failover_main_back_as_main():
    safe_execute(shutil.rmtree, TEMP_DIR)

    memgraph_instances_description = get_memgraph_instances_description(
        test_name="test_automatic_failover_main_back_as_main"
    )

    interactive_mg_runner.start_all(memgraph_instances_description, keep_directories=False)

    interactive_mg_runner.kill(memgraph_instances_description, "instance_1")
    interactive_mg_runner.kill(memgraph_instances_description, "instance_2")
    interactive_mg_runner.kill(memgraph_instances_description, "instance_3")

    coord_cursor = connect(host="localhost", port=7690).cursor()

    def retrieve_data_show_repl_cluster():
        return ignore_elapsed_time_from_results(sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;"))))

    expected_data_all_down = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader"),
        ("instance_1", "localhost:7688", "", "localhost:10011", "down", "unknown"),
        ("instance_2", "localhost:7689", "", "localhost:10012", "down", "unknown"),
        ("instance_3", "localhost:7687", "", "localhost:10013", "down", "unknown"),
    ]

    mg_sleep_and_assert(expected_data_all_down, retrieve_data_show_repl_cluster)

    interactive_mg_runner.start(memgraph_instances_description, "instance_3")
    expected_data_main_back = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader"),
        ("instance_1", "localhost:7688", "", "localhost:10011", "down", "unknown"),
        ("instance_2", "localhost:7689", "", "localhost:10012", "down", "unknown"),
        ("instance_3", "localhost:7687", "", "localhost:10013", "up", "main"),
    ]
    mg_sleep_and_assert(expected_data_main_back, retrieve_data_show_repl_cluster)

    instance3_cursor = connect(host="localhost", port=7687).cursor()

    def retrieve_data_show_repl_role_instance3():
        return sorted(list(execute_and_fetch_all(instance3_cursor, "SHOW REPLICATION ROLE;")))

    mg_sleep_and_assert([("main",)], retrieve_data_show_repl_role_instance3)

    interactive_mg_runner.start(memgraph_instances_description, "instance_1")
    interactive_mg_runner.start(memgraph_instances_description, "instance_2")

    expected_data_replicas_back = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader"),
        ("instance_1", "localhost:7688", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7689", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7687", "", "localhost:10013", "up", "main"),
    ]

    mg_sleep_and_assert(expected_data_replicas_back, retrieve_data_show_repl_cluster)

    instance1_cursor = connect(host="localhost", port=7688).cursor()
    instance2_cursor = connect(host="localhost", port=7689).cursor()

    def retrieve_data_show_repl_role_instance1():
        return sorted(list(execute_and_fetch_all(instance1_cursor, "SHOW REPLICATION ROLE;")))

    def retrieve_data_show_repl_role_instance2():
        return sorted(list(execute_and_fetch_all(instance2_cursor, "SHOW REPLICATION ROLE;")))

    mg_sleep_and_assert([("replica",)], retrieve_data_show_repl_role_instance1)
    mg_sleep_and_assert([("replica",)], retrieve_data_show_repl_role_instance2)
    mg_sleep_and_assert([("main",)], retrieve_data_show_repl_role_instance3)


def test_disable_multiple_mains():
    safe_execute(shutil.rmtree, TEMP_DIR)

    memgraph_instances_description = get_memgraph_instances_description(test_name="disable_multiple_mains")
    interactive_mg_runner.start_all(memgraph_instances_description, keep_directories=False)

    coord_cursor = connect(host="localhost", port=7690).cursor()

    try:
        execute_and_fetch_all(
            coord_cursor,
            "SET INSTANCE instance_1 TO MAIN;",
        )
    except Exception as e:
        assert str(e) == "Couldn't set instance to main since there is already a main instance in cluster!"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
