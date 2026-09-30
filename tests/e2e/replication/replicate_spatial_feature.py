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

BOLT_PORTS = {"main": 7687, "replica_1": 7688, "replica_2": 7689}
REPLICATION_PORTS = {"replica_1": 10001, "replica_2": 10002}
file = "replicate_point"


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


def test_point_replication(connection, test_name):
    # Goal: That point types are replicated to REPLICAs
    # 0/ Setup replication
    # 1/ Create Vertex with points property on MAIN
    # 2/ Validate points property has arrived at REPLICA

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "replica_1": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_1']}",
                "--log-level=TRACE",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica1",
            "setup_queries": [
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};",
            ],
        },
        "replica_2": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_2']}",
                "--log-level=TRACE",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/replica2.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica2",
            "setup_queries": [
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_2']};",
            ],
        },
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
                f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL, keep_directories=False)
    cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    execute_and_fetch_all(
        cursor,
        "CREATE ({ "
        "prop1: point({x: 1, y: 2, srid: 4326}), "
        "prop2: point({x: 3, y: 4, z: 5, srid: 4979}), "
        "prop3: point({x: 6, y: 7, srid: 7203}), "
        "prop4: point({x: 8, y: 9, z: 10, srid: 9157}) "
        " });",
    )

    # 2/
    expected_data = [
        (
            "replica_1",
            "127.0.0.1:10001",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 2}},
        ),
        (
            "replica_2",
            "127.0.0.1:10002",
            "async",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 2}},
        ),
    ]

    mg_sleep_and_assert_collection(expected_data, show_replicas_func(cursor))

    def get_props(cursor):
        return execute_and_fetch_all(
            cursor,
            f"MATCH (n) RETURN "
            "n.prop1.x, n.prop1.y, n.prop1.srid, "
            "n.prop2.x, n.prop2.y, n.prop2.z, n.prop2.srid, "
            "n.prop3.x, n.prop3.y, n.prop3.srid, "
            "n.prop4.x, n.prop4.y, n.prop4.z, n.prop4.srid;",
        )

    expected_result = [(1, 2, 4326, 3, 4, 5, 4979, 6, 7, 7203, 8, 9, 10, 9157)]

    cursor_replica = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    replica_1_enums = get_props(cursor_replica)
    assert replica_1_enums == expected_result

    cursor_replica2 = connection(BOLT_PORTS["replica_2"], "replica").cursor()
    replica_2_enums = get_props(cursor_replica2)
    assert replica_2_enums == expected_result


def test_point_index_replication(connection):
    # Goal: That point types are replicated to REPLICAs
    # 0/ Setup replication
    # 1/ Create Point index on MAIN
    # 2/ Validate point index has arrived at REPLICA
    # 3/ Create point entries on MAIN
    # 4/ Validate index count on REPLICA is correct
    # 5/ Drop point index on MAIN
    # 6/ Validate index has been droped on REPLICA

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "replica_1": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_1']}",
                "--log-level=TRACE",
            ],
            "log_file": "replica1.log",
            "setup_queries": [
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};",
            ],
        },
        "replica_2": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_2']}",
                "--log-level=TRACE",
            ],
            "log_file": "replica2.log",
            "setup_queries": [
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_2']};",
            ],
        },
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
            ],
            "log_file": "main.log",
            "setup_queries": [
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
                f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL)
    cursor = connection(BOLT_PORTS["main"], "main").cursor()

    def wait_for_replication_change(cursor, ts):
        expected_data = [
            (
                "replica_1",
                "127.0.0.1:10001",
                "sync",
                {"behind": None, "status": "ready", "ts": 0},
                {"memgraph": {"behind": 0, "status": "ready", "ts": ts}},
            ),
            (
                "replica_2",
                "127.0.0.1:10002",
                "async",
                {"behind": None, "status": "ready", "ts": 0},
                {"memgraph": {"behind": 0, "status": "ready", "ts": ts}},
            ),
        ]

        mg_sleep_and_assert_collection(expected_data, show_replicas_func(cursor))

    # 1/
    execute_and_fetch_all(
        cursor,
        "CREATE POINT INDEX ON :Node(location);",
    )
    wait_for_replication_change(cursor, 2)

    # 2/
    def get_show_index_info(cursor):
        return execute_and_fetch_all(cursor, f"SHOW INDEX INFO;")

    def get_replica_cursor(name):
        return connection(BOLT_PORTS[name], "replica").cursor()

    expected_result = [("point", "Node", "location", 0)]

    replica_1_enums = get_show_index_info(get_replica_cursor("replica_1"))
    assert replica_1_enums == expected_result

    replica_2_enums = get_show_index_info(get_replica_cursor("replica_2"))
    assert replica_2_enums == expected_result

    # 3/
    execute_and_fetch_all(
        cursor,
        "CREATE (:Node{location: point({x:1, y:1})});",
    )
    wait_for_replication_change(cursor, 4)

    # 4/
    expected_result = [("point", "Node", "location", 1)]

    replica_1_enums = get_show_index_info(get_replica_cursor("replica_1"))
    assert replica_1_enums == expected_result

    replica_2_enums = get_show_index_info(get_replica_cursor("replica_2"))
    assert replica_2_enums == expected_result

    # 5/
    execute_and_fetch_all(
        cursor,
        "DROP POINT INDEX ON :Node(location);",
    )
    wait_for_replication_change(cursor, 6)

    # 6/
    expected_result = []

    replica_1_enums = get_show_index_info(get_replica_cursor("replica_1"))
    assert replica_1_enums == expected_result

    replica_2_enums = get_show_index_info(get_replica_cursor("replica_2"))
    assert replica_2_enums == expected_result


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
