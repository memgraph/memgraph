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
from common import connect, execute_and_fetch_all, get_data_path, get_logs_path, show_routing_table
from mg_utils import mg_sleep_and_assert, mg_sleep_and_assert_eval_function

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

file = "show_routing_table"

COORD_1_BOLT = "localhost:7690"
COORD_2_BOLT = "localhost:7691"
COORD_3_BOLT = "localhost:7692"
ALL_COORD_BOLT = sorted([COORD_1_BOLT, COORD_2_BOLT, COORD_3_BOLT])

INSTANCE_1_BOLT = "localhost:7687"
INSTANCE_2_BOLT = "localhost:7688"
INSTANCE_3_BOLT = "localhost:7689"


@pytest.fixture(autouse=True)
def cleanup_after_test():
    # Run the test
    yield
    # Stop + delete directories after running the test
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


def get_instances_description(test_name: str):
    return {
        "instance_1": {
            "args": [
                "--bolt-port=7687",
                "--log-level=TRACE",
                "--management-port=10011",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_1",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--bolt-port=7688",
                "--log-level=TRACE",
                "--management-port=10012",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_2",
            "setup_queries": [],
        },
        "instance_3": {
            "args": [
                "--bolt-port=7689",
                "--log-level=TRACE",
                "--management-port=10013",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_3",
            "setup_queries": [],
        },
        "coordinator_1": {
            "args": [
                "--bolt-port=7690",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10111",
                "--management-port=10121",
                "--coordinator-hostname=localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_1",
            "setup_queries": [],
        },
        "coordinator_2": {
            "args": [
                "--bolt-port=7691",
                "--log-level=TRACE",
                "--coordinator-id=2",
                "--coordinator-port=10112",
                "--management-port=10122",
                "--coordinator-hostname=localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_2",
            "setup_queries": [],
        },
        "coordinator_3": {
            "args": [
                "--bolt-port=7692",
                "--log-level=TRACE",
                "--coordinator-id=3",
                "--coordinator-port=10113",
                "--management-port=10123",
                "--coordinator-hostname=localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_3",
            "setup_queries": [],
        },
    }


def add_all_coordinators(leader_cursor):
    execute_and_fetch_all(
        leader_cursor,
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
    )
    execute_and_fetch_all(
        leader_cursor,
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
    )
    execute_and_fetch_all(
        leader_cursor,
        "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}",
    )


def register_instance(leader_cursor, instance_id: int):
    execute_and_fetch_all(
        leader_cursor,
        f"REGISTER INSTANCE instance_{instance_id} WITH CONFIG {{'bolt_server': 'localhost:{7686 + instance_id}', "
        f"'management_server': 'localhost:1001{instance_id}', 'replication_server': 'localhost:1000{instance_id}'}}",
    )


def setup_full_cluster(test_name: str, main_id: int = 3):
    """
    Starts three coordinators and three data instances, registers everything on coordinator 3 (which becomes the
    leader) and promotes `instance_<main_id>` to MAIN. Returns the instance description and the leader's cursor.
    """
    instances_description = get_instances_description(test_name=test_name)
    interactive_mg_runner.start_all(instances_description, keep_directories=False)

    leader_cursor = connect(host="localhost", port=7692).cursor()
    add_all_coordinators(leader_cursor)
    for instance_id in (1, 2, 3):
        register_instance(leader_cursor, instance_id)
    execute_and_fetch_all(leader_cursor, f"SET INSTANCE instance_{main_id} TO MAIN")

    return instances_description, leader_cursor


def test_show_routing_table(test_name):
    # A follower has no authority over the cluster state, so it forwards the query to the leader and must return the
    # very same routing table.
    _, leader_cursor = setup_full_cluster(test_name)

    expected_routing_table = {
        "WRITE": [INSTANCE_3_BOLT],
        "READ": sorted([INSTANCE_1_BOLT, INSTANCE_2_BOLT]),
        "ROUTE": ALL_COORD_BOLT,
    }
    mg_sleep_and_assert(expected_routing_table, partial(show_routing_table, leader_cursor))

    for follower_port in (7690, 7691, 7692):
        follower_cursor = connect(host="localhost", port=follower_port).cursor()
        mg_sleep_and_assert(expected_routing_table, partial(show_routing_table, follower_cursor))


def test_show_routing_table_no_data_instances(test_name):
    # With no data instance registered there is nothing to read from or write to, so only the ROUTE row is reported.
    # Empty roles must be omitted rather than returned as rows with an empty server list.
    instances_description = get_instances_description(test_name=test_name)
    interactive_mg_runner.start_all(instances_description, keep_directories=False)

    leader_cursor = connect(host="localhost", port=7692).cursor()
    add_all_coordinators(leader_cursor)

    mg_sleep_and_assert({"ROUTE": ALL_COORD_BOLT}, partial(show_routing_table, leader_cursor))


def test_show_routing_table_reads_on_main(test_name):
    # With 'enabled_reads_on_main' the main must show up as a reader too, and disabling it must take the main back out
    # of the READ row.
    _, leader_cursor = setup_full_cluster(test_name)

    reads_off = {
        "WRITE": [INSTANCE_3_BOLT],
        "READ": sorted([INSTANCE_1_BOLT, INSTANCE_2_BOLT]),
        "ROUTE": ALL_COORD_BOLT,
    }
    mg_sleep_and_assert(reads_off, partial(show_routing_table, leader_cursor))

    execute_and_fetch_all(leader_cursor, "SET COORDINATOR SETTING 'enabled_reads_on_main' TO 'true'")
    reads_on = {
        "WRITE": [INSTANCE_3_BOLT],
        "READ": sorted([INSTANCE_1_BOLT, INSTANCE_2_BOLT, INSTANCE_3_BOLT]),
        "ROUTE": ALL_COORD_BOLT,
    }
    mg_sleep_and_assert(reads_on, partial(show_routing_table, leader_cursor))

    execute_and_fetch_all(leader_cursor, "SET COORDINATOR SETTING 'enabled_reads_on_main' TO 'false'")
    mg_sleep_and_assert(reads_off, partial(show_routing_table, leader_cursor))


def test_show_routing_table_after_failover(test_name):
    # After a failover the promoted instance must be the one reported as the writer, and it must not be listed as a
    # reader because reads on main are disabled by default. Which of the two surviving instances gets promoted depends
    # on the order of pings, so only the invariants are asserted here.
    instances_description, leader_cursor = setup_full_cluster(test_name)

    mg_sleep_and_assert(
        {
            "WRITE": [INSTANCE_3_BOLT],
            "READ": sorted([INSTANCE_1_BOLT, INSTANCE_2_BOLT]),
            "ROUTE": ALL_COORD_BOLT,
        },
        partial(show_routing_table, leader_cursor),
    )

    interactive_mg_runner.kill(instances_description, "instance_3")

    def promoted_survivor_is_the_only_writer(routing_table):
        writers = routing_table.get("WRITE", [])
        return len(writers) == 1 and writers[0] in (INSTANCE_1_BOLT, INSTANCE_2_BOLT)

    routing_table = mg_sleep_and_assert_eval_function(
        promoted_survivor_is_the_only_writer, partial(show_routing_table, leader_cursor)
    )

    assert routing_table["WRITE"][0] not in routing_table.get("READ", [])
    assert routing_table["ROUTE"] == ALL_COORD_BOLT


def test_show_routing_table_on_data_instance(test_name):
    # SHOW ROUTING TABLE is a coordinator-only query: a data instance knows nothing about the cluster topology and
    # must reject it instead of answering with a partial table.
    setup_full_cluster(test_name)

    for instance_port in (7687, 7688, 7689):
        instance_cursor = connect(host="localhost", port=instance_port).cursor()
        with pytest.raises(Exception) as e:
            execute_and_fetch_all(instance_cursor, "SHOW ROUTING TABLE")
        assert "Only coordinator can run SHOW ROUTING TABLE query." in str(e.value)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
