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

import concurrent.futures
import os
import sys
from functools import partial

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all, get_data_path, get_logs_path
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

file = "regions"

LEADER_PORT = 7690
COORDINATOR_PORTS = [7690, 7691, 7692]
FOLLOWER_PORTS = [7691, 7692]
DATA_INSTANCE_PORT = 7687


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


def coordinator_description(test_name: str, coord_id: int):
    return {
        "args": [
            f"--bolt-port={7689 + coord_id}",
            "--log-level=TRACE",
            f"--coordinator-id={coord_id}",
            f"--coordinator-port={10110 + coord_id}",
            f"--management-port={10120 + coord_id}",
            "--coordinator-hostname=localhost",
        ],
        "log_file": f"{get_logs_path(file, test_name)}/coordinator_{coord_id}.log",
        "data_directory": f"{get_data_path(file, test_name)}/coordinator_{coord_id}",
        "setup_queries": [],
    }


def get_instances_description(test_name: str):
    return {
        "instance_1": {
            "args": [
                f"--bolt-port={DATA_INSTANCE_PORT}",
                "--log-level=TRACE",
                "--management-port=10011",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_1",
            "setup_queries": [],
        },
        "coordinator_1": coordinator_description(test_name, 1),
        "coordinator_2": coordinator_description(test_name, 2),
        "coordinator_3": coordinator_description(test_name, 3),
    }


def setup_cluster(test_name):
    instances_description = get_instances_description(test_name)
    interactive_mg_runner.start_all(instances_description, keep_directories=False)
    leader_cursor = connect(host="localhost", port=LEADER_PORT).cursor()
    for coord_id in (1, 2, 3):
        execute_and_fetch_all(
            leader_cursor,
            f"ADD COORDINATOR {coord_id} WITH CONFIG {{'bolt_server': 'localhost:{7689 + coord_id}', "
            f"'coordinator_server': 'localhost:{10110 + coord_id}', 'management_server': 'localhost:{10120 + coord_id}'}}",
        )
    return instances_description


def show_region_info(port):
    try:
        return execute_and_fetch_all(connect(host="localhost", port=port).cursor(), "SHOW REGION INFO")
    except Exception:
        return None


def assert_region_info_on_all_coordinators(expected):
    for port in COORDINATOR_PORTS:
        mg_sleep_and_assert(expected, partial(show_region_info, port))


def test_region_queries(test_name):
    setup_cluster(test_name)
    leader_cursor = connect(host="localhost", port=LEADER_PORT).cursor()
    follower_cursor = connect(host="localhost", port=FOLLOWER_PORTS[0]).cursor()

    assert_region_info_on_all_coordinators([])

    execute_and_fetch_all(leader_cursor, "ADD REGION 'us-east'")
    # Issued on a follower, the write is forwarded to the leader.
    execute_and_fetch_all(follower_cursor, "ADD REGION 'eu-west'")
    # Region names are case-sensitive.
    execute_and_fetch_all(follower_cursor, "ADD REGION 'EU-west'")

    assert_region_info_on_all_coordinators([("EU-west", [], False), ("eu-west", [], False), ("us-east", [], False)])

    for cursor in (leader_cursor, follower_cursor):
        with pytest.raises(Exception) as e:
            execute_and_fetch_all(cursor, "ADD REGION 'eu-west'")
        assert "Region 'eu-west' already exists." in str(e.value)

    execute_and_fetch_all(follower_cursor, "DROP REGION 'EU-west'")
    execute_and_fetch_all(leader_cursor, "DROP REGION 'us-east'")
    assert_region_info_on_all_coordinators([("eu-west", [], False)])

    for cursor in (leader_cursor, follower_cursor):
        with pytest.raises(Exception) as e:
            execute_and_fetch_all(cursor, "DROP REGION 'us-east'")
        assert "Region 'us-east' doesn't exist." in str(e.value)

    for cursor in (leader_cursor, follower_cursor):
        for name in ("", "   ", " eu-west", "eu west", "x" * 129, "\U0001F600", "\\U0000D800"):
            for action in ("ADD", "DROP"):
                with pytest.raises(Exception) as e:
                    execute_and_fetch_all(cursor, f"{action} REGION '{name}'")
                assert "Invalid region name" in str(e.value)
    execute_and_fetch_all(leader_cursor, f"ADD REGION '{'x' * 128}'")
    execute_and_fetch_all(leader_cursor, f"DROP REGION '{'x' * 128}'")
    assert_region_info_on_all_coordinators([("eu-west", [], False)])

    execute_and_fetch_all(follower_cursor, "DROP REGION 'eu-west'")
    assert_region_info_on_all_coordinators([])


def test_region_queries_rejected_on_data_instance(test_name):
    setup_cluster(test_name)
    cursor = connect(host="localhost", port=DATA_INSTANCE_PORT).cursor()

    for query, expected in (
        ("ADD REGION 'eu-west'", "Only coordinator can run ADD REGION query."),
        ("DROP REGION 'eu-west'", "Only coordinator can run DROP REGION query."),
        ("SHOW REGION INFO", "Only coordinator can run SHOW REGION INFO query."),
    ):
        with pytest.raises(Exception) as e:
            execute_and_fetch_all(cursor, query)
        assert str(e.value) == expected


def test_regions_survive_coordinator_restart(test_name):
    instances_description = setup_cluster(test_name)
    leader_cursor = connect(host="localhost", port=LEADER_PORT).cursor()
    execute_and_fetch_all(leader_cursor, "ADD REGION 'us-east'")
    execute_and_fetch_all(leader_cursor, "ADD REGION 'eu-west'")
    expected = [("eu-west", [], False), ("us-east", [], False)]
    assert_region_info_on_all_coordinators(expected)

    coordinators = ("coordinator_1", "coordinator_2", "coordinator_3")
    for coordinator in coordinators:
        interactive_mg_runner.kill(instances_description, coordinator)
    # A restarted coordinator opens its Bolt port only after it rejoins a Raft quorum, so starting them one by one
    # would block on the first; start them concurrently.
    with concurrent.futures.ThreadPoolExecutor(len(coordinators)) as executor:
        futures = [
            executor.submit(interactive_mg_runner.start, instances_description, coordinator)
            for coordinator in coordinators
        ]
        for future in concurrent.futures.as_completed(futures):
            future.result()

    assert_region_info_on_all_coordinators(expected)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
