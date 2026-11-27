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


def data_instance_description(test_name: str, instance_id: int):
    return {
        "args": [
            f"--bolt-port={DATA_INSTANCE_PORT + instance_id - 1}",
            "--log-level=TRACE",
            f"--management-port={10010 + instance_id}",
        ],
        "log_file": f"{get_logs_path(file, test_name)}/instance_{instance_id}.log",
        "data_directory": f"{get_data_path(file, test_name)}/instance_{instance_id}",
        "setup_queries": [],
    }


def get_instances_description(test_name: str):
    return {
        "instance_1": data_instance_description(test_name, 1),
        "instance_2": data_instance_description(test_name, 2),
        "instance_3": data_instance_description(test_name, 3),
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


def register_instance_query(instance_id: int, region=None, mode=""):
    region_entry = "" if region is None else f", 'region': '{region}'"
    return (
        f"REGISTER INSTANCE instance_{instance_id} {mode} WITH CONFIG "
        f"{{'bolt_server': 'localhost:{DATA_INSTANCE_PORT + instance_id - 1}', "
        f"'management_server': 'localhost:{10010 + instance_id}', "
        f"'replication_server': 'localhost:{10000 + instance_id}'{region_entry}}}"
    )


def instance_bolt_servers(cursor):
    return {row[0]: row[1] for row in execute_and_fetch_all(cursor, "SHOW INSTANCES") if row[0].startswith("instance")}


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


def test_instance_regions(test_name):
    setup_cluster(test_name)
    leader_cursor = connect(host="localhost", port=LEADER_PORT).cursor()
    follower_cursor = connect(host="localhost", port=FOLLOWER_PORTS[0]).cursor()

    execute_and_fetch_all(leader_cursor, "ADD REGION 'eu'")
    execute_and_fetch_all(leader_cursor, "ADD REGION 'us'")

    for cursor in (leader_cursor, follower_cursor):
        with pytest.raises(Exception) as e:
            execute_and_fetch_all(cursor, register_instance_query(1, region="ap"))
        assert "Region 'ap' doesn't exist." in str(e.value)
        with pytest.raises(Exception) as e:
            execute_and_fetch_all(cursor, register_instance_query(1, region="a p"))
        assert "Invalid region name" in str(e.value)

    # The region is only a label: the replication mode is still the one given in the query.
    execute_and_fetch_all(follower_cursor, register_instance_query(1, region="eu"))
    execute_and_fetch_all(leader_cursor, register_instance_query(2, region="us", mode="AS ASYNC"))
    execute_and_fetch_all(leader_cursor, register_instance_query(3))
    execute_and_fetch_all(leader_cursor, "SET INSTANCE instance_1 TO MAIN")
    assert_region_info_on_all_coordinators([("eu", ["instance_1"], False), ("us", ["instance_2"], False)])

    main_cursor = connect(host="localhost", port=DATA_INSTANCE_PORT).cursor()
    mg_sleep_and_assert(
        [("instance_2", "async"), ("instance_3", "sync")],
        lambda: sorted((row[0], row[2]) for row in execute_and_fetch_all(main_cursor, "SHOW REPLICAS")),
    )

    for cursor in (leader_cursor, follower_cursor):
        with pytest.raises(Exception) as e:
            execute_and_fetch_all(cursor, "DROP REGION 'eu'")
        assert "Region 'eu' still has instances." in str(e.value)

    execute_and_fetch_all(follower_cursor, "UPDATE CONFIG FOR INSTANCE instance_3 {'region': 'us'}")
    execute_and_fetch_all(leader_cursor, "UPDATE CONFIG FOR INSTANCE instance_1 {'region': null}")
    assert_region_info_on_all_coordinators([("eu", [], False), ("us", ["instance_2", "instance_3"], False)])
    execute_and_fetch_all(follower_cursor, "DROP REGION 'eu'")
    assert_region_info_on_all_coordinators([("us", ["instance_2", "instance_3"], False)])

    # A failed region change leaves the bolt server untouched; a valid one updates both.
    bolt_servers = instance_bolt_servers(leader_cursor)
    for cursor in (leader_cursor, follower_cursor):
        with pytest.raises(Exception) as e:
            execute_and_fetch_all(
                cursor, "UPDATE CONFIG FOR INSTANCE instance_2 {'bolt_server': 'localhost:7700', 'region': 'eu'}"
            )
        assert "Region 'eu' doesn't exist." in str(e.value)
    assert instance_bolt_servers(leader_cursor) == bolt_servers

    execute_and_fetch_all(leader_cursor, "ADD REGION 'eu'")
    execute_and_fetch_all(
        follower_cursor, "UPDATE CONFIG FOR INSTANCE instance_2 {'bolt_server': 'localhost:7700', 'region': 'eu'}"
    )
    assert instance_bolt_servers(leader_cursor)["instance_2"] == "localhost:7700"
    execute_and_fetch_all(leader_cursor, "UPDATE CONFIG FOR INSTANCE instance_2 {'bolt_server': 'localhost:7688'}")
    assert instance_bolt_servers(leader_cursor)["instance_2"] == "localhost:7688"
    assert_region_info_on_all_coordinators([("eu", ["instance_2"], False), ("us", ["instance_3"], False)])

    for query, expected in (
        ("UPDATE CONFIG FOR INSTANCE instance_2 {}", "at least one of bolt_server and region"),
        ("UPDATE CONFIG FOR INSTANCE instance_2 {'foo': 'bar'}", "may contain only bolt_server"),
        ("UPDATE CONFIG FOR INSTANCE instance_2 {'region': 'a p'}", "Invalid region name"),
        ("UPDATE CONFIG FOR INSTANCE instance_9 {'region': 'eu'}", "doesn't exist"),
        ("UPDATE CONFIG FOR COORDINATOR 1 {'region': 'eu'}", "Coordinators don't belong to a region"),
        ("UPDATE CONFIG FOR COORDINATOR 1 {'bolt_server': 'localhost:7690', 'region': 'eu'}", "Coordinators"),
    ):
        for cursor in (leader_cursor, follower_cursor):
            with pytest.raises(Exception) as e:
                execute_and_fetch_all(cursor, query)
            assert expected in str(e.value), query

    # Unregistering an instance removes it from its region.
    execute_and_fetch_all(leader_cursor, "UNREGISTER INSTANCE instance_2")
    assert_region_info_on_all_coordinators([("eu", [], False), ("us", ["instance_3"], False)])
    execute_and_fetch_all(leader_cursor, "DROP REGION 'eu'")


def assert_raises_on_all(cursors, query, expected):
    for cursor in cursors:
        with pytest.raises(Exception) as e:
            execute_and_fetch_all(cursor, query)
        assert expected in str(e.value), query


def test_primary_region(test_name):
    setup_cluster(test_name)
    leader_cursor = connect(host="localhost", port=LEADER_PORT).cursor()
    follower_cursor = connect(host="localhost", port=FOLLOWER_PORTS[0]).cursor()
    cursors = (leader_cursor, follower_cursor)

    for region in ("eu", "us", "ap"):
        execute_and_fetch_all(leader_cursor, f"ADD REGION '{region}'")
    execute_and_fetch_all(leader_cursor, register_instance_query(1, region="eu"))
    execute_and_fetch_all(leader_cursor, register_instance_query(2, region="us", mode="AS ASYNC"))
    execute_and_fetch_all(leader_cursor, register_instance_query(3))
    execute_and_fetch_all(leader_cursor, "SET INSTANCE instance_1 TO MAIN")

    assert_raises_on_all(cursors, "SET 'xx' AS PRIMARY REGION", "Region 'xx' doesn't exist.")
    assert_raises_on_all(cursors, "SET 'a p' AS PRIMARY REGION", "Invalid region name")
    assert_raises_on_all(cursors, "SET 'eu' AS PRIMARY REGION", "Every instance must be in a region")

    execute_and_fetch_all(follower_cursor, "UPDATE CONFIG FOR INSTANCE instance_3 {'region': 'eu'}")
    assert_raises_on_all(cursors, "SET 'ap' AS PRIMARY REGION", "Region 'ap' has no instances")

    # The switch to cross-region mode, issued on a follower; setting the same primary region again is a no-op.
    execute_and_fetch_all(follower_cursor, "SET 'eu' AS PRIMARY REGION")
    execute_and_fetch_all(leader_cursor, "SET 'eu' AS PRIMARY REGION")
    assert_region_info_on_all_coordinators(
        [("ap", [], False), ("eu", ["instance_1", "instance_3"], True), ("us", ["instance_2"], False)]
    )

    assert_raises_on_all(cursors, register_instance_query(4), "must be registered with a region")
    for mode in ("AS ASYNC", "AS STRICT_SYNC"):
        assert_raises_on_all(
            cursors, register_instance_query(4, region="us", mode=mode), "replication mode is derived from"
        )
    assert_raises_on_all(cursors, "SET INSTANCE instance_2 TO MAIN", "A primary region is set")
    assert_raises_on_all(cursors, "SET INSTANCE instance_typo TO MAIN", "No instance with such name!")
    assert_raises_on_all(cursors, "DROP REGION 'eu'", "Region 'eu' is the primary region.")
    assert_raises_on_all(cursors, "UPDATE CONFIG FOR INSTANCE instance_2 {'region': null}", "can't be removed")

    # Later SETs only move the primary region. instance_2 is now the only instance in it.
    execute_and_fetch_all(leader_cursor, "SET 'us' AS PRIMARY REGION")
    assert_region_info_on_all_coordinators(
        [("ap", [], False), ("eu", ["instance_1", "instance_3"], False), ("us", ["instance_2"], True)]
    )
    assert_raises_on_all(cursors, "UNREGISTER INSTANCE instance_2", "last instance in the primary region")
    assert_raises_on_all(
        cursors, "UPDATE CONFIG FOR INSTANCE instance_2 {'region': 'eu'}", "last instance in the primary region"
    )
    # Moving an instance into the primary region, and within the non-primary regions, is allowed.
    execute_and_fetch_all(follower_cursor, "UPDATE CONFIG FOR INSTANCE instance_3 {'region': 'ap'}")
    execute_and_fetch_all(follower_cursor, "UPDATE CONFIG FOR INSTANCE instance_3 {'region': 'us'}")
    assert_region_info_on_all_coordinators(
        [("ap", [], False), ("eu", ["instance_1"], False), ("us", ["instance_2", "instance_3"], True)]
    )

    # Shrink to a single region: set the region that stays as primary, unregister the rest, drop the other regions.
    execute_and_fetch_all(leader_cursor, "UPDATE CONFIG FOR INSTANCE instance_3 {'region': 'eu'}")
    execute_and_fetch_all(follower_cursor, "SET 'eu' AS PRIMARY REGION")
    execute_and_fetch_all(follower_cursor, "UNREGISTER INSTANCE instance_2")
    execute_and_fetch_all(leader_cursor, "DROP REGION 'us'")

    # An instance registered in cross-region mode replicates in its region's mode: SYNC inside the primary region,
    # ASYNC elsewhere.
    main_cursor = connect(host="localhost", port=DATA_INSTANCE_PORT).cursor()

    def replica_modes():
        return sorted((row[0], row[2]) for row in execute_and_fetch_all(main_cursor, "SHOW REPLICAS"))

    execute_and_fetch_all(follower_cursor, register_instance_query(2, region="ap"))
    mg_sleep_and_assert([("instance_2", "async"), ("instance_3", "sync")], replica_modes)
    execute_and_fetch_all(leader_cursor, "UNREGISTER INSTANCE instance_2")
    execute_and_fetch_all(leader_cursor, "DROP REGION 'ap'")
    execute_and_fetch_all(follower_cursor, register_instance_query(2, region="eu"))
    mg_sleep_and_assert([("instance_2", "sync"), ("instance_3", "sync")], replica_modes)
    assert_region_info_on_all_coordinators([("eu", ["instance_1", "instance_2", "instance_3"], True)])


def test_region_queries_rejected_on_data_instance(test_name):
    setup_cluster(test_name)
    cursor = connect(host="localhost", port=DATA_INSTANCE_PORT).cursor()

    for query, expected in (
        ("ADD REGION 'eu-west'", "Only coordinator can run ADD REGION query."),
        ("DROP REGION 'eu-west'", "Only coordinator can run DROP REGION query."),
        ("SHOW REGION INFO", "Only coordinator can run SHOW REGION INFO query."),
        ("SET 'eu-west' AS PRIMARY REGION", "Only coordinator can run SET PRIMARY REGION query."),
    ):
        with pytest.raises(Exception) as e:
            execute_and_fetch_all(cursor, query)
        assert str(e.value) == expected


def test_regions_survive_coordinator_restart(test_name):
    instances_description = setup_cluster(test_name)
    leader_cursor = connect(host="localhost", port=LEADER_PORT).cursor()
    execute_and_fetch_all(leader_cursor, "ADD REGION 'us-east'")
    execute_and_fetch_all(leader_cursor, "ADD REGION 'eu-west'")
    execute_and_fetch_all(leader_cursor, register_instance_query(1, region="eu-west"))
    execute_and_fetch_all(leader_cursor, "SET 'eu-west' AS PRIMARY REGION")
    expected = [("eu-west", ["instance_1"], True), ("us-east", [], False)]
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
