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
from common import execute_and_fetch_all, get_logs_path
from mg_utils import mg_sleep_and_assert_collection

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

BOLT_PORTS = {"main": 7687, "replica_1": 7688, "replica_2": 7689}
REPLICATION_PORTS = {"replica_1": 10001, "replica_2": 10002}
LOG_DIR = "replicate_vector"


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


def create_instances_description(test_name):
    """Create the MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL dictionary for replication tests."""
    return {
        "replica_1": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_1']}",
                "--log-level=TRACE",
            ],
            "log_file": f"{get_logs_path(LOG_DIR, test_name)}/replica1.log",
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
            "log_file": f"{get_logs_path(LOG_DIR, test_name)}/replica2.log",
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
            "log_file": f"{get_logs_path(LOG_DIR, test_name)}/main.log",
            "setup_queries": [
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
                f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';",
            ],
        },
    }


def wait_for_replication_change(cursor, ts):
    """Wait for replication to reach the specified timestamp."""
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


def get_show_index_info(cursor):
    """Get index info from the database."""
    return execute_and_fetch_all(cursor, "SHOW INDEX INFO;")


def get_replica_cursor(connection, name):
    """Get a cursor for a replica instance."""
    return connection(BOLT_PORTS[name], "replica").cursor()


def test_vector_index_replication(connection, test_name):
    # Goal: Proof that vector types are replicated to REPLICAs
    # 0/ Setup replication
    # 1/ Create vector index on MAIN
    # 2/ Validate vector index has arrived at REPLICA
    # 3/ Create vector entries on MAIN
    # 4/ Validate index count on REPLICA is correct
    # 5/ Drop vector index on MAIN
    # 6/ Validate index has been droped on REPLICA

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = create_instances_description(test_name)

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL)
    cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    execute_and_fetch_all(
        cursor,
        "CREATE VECTOR INDEX test_index ON :Node(embedding) WITH CONFIG {'dimension': 2, 'capacity': 10};",
    )
    wait_for_replication_change(cursor, 2)

    # 2/
    expected_result = [("label+property_vector", "Node", "embedding", 0)]
    replica_1_enums = get_show_index_info(get_replica_cursor(connection, "replica_1"))
    assert replica_1_enums == expected_result
    replica_2_enums = get_show_index_info(get_replica_cursor(connection, "replica_2"))
    assert replica_2_enums == expected_result

    # 3/
    execute_and_fetch_all(
        cursor,
        "CREATE (:Node {embedding: [1.0, 2.0]});",
    )
    wait_for_replication_change(cursor, 4)

    # 4/
    expected_result = [("label+property_vector", "Node", "embedding", 1)]
    replica_1_enums = get_show_index_info(get_replica_cursor(connection, "replica_1"))
    assert replica_1_enums == expected_result
    replica_2_enums = get_show_index_info(get_replica_cursor(connection, "replica_2"))
    assert replica_2_enums == expected_result

    # 5/
    execute_and_fetch_all(
        cursor,
        "DROP VECTOR INDEX test_index;",
    )
    wait_for_replication_change(cursor, 6)

    # 6/
    expected_result = []
    replica_1_enums = get_show_index_info(get_replica_cursor(connection, "replica_1"))
    assert replica_1_enums == expected_result
    replica_2_enums = get_show_index_info(get_replica_cursor(connection, "replica_2"))
    assert replica_2_enums == expected_result


def test_vector_index_replication_property_changes(connection, test_name):
    # Goal: That setting properties to null and updating vector properties is correctly replicated.
    # 0/ Setup replication
    # 1/ Create vector index on MAIN, add nodes with vector properties
    # 2/ Validate index count on REPLICA
    # 3/ Set one property to null, update another property on MAIN
    # 4/ Validate index count changes are replicated
    # 5/ Validate property values on REPLICA

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = create_instances_description(test_name)

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL)
    cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    execute_and_fetch_all(
        cursor,
        'CREATE VECTOR INDEX test_index ON :L1(prop1) WITH CONFIG {"dimension": 2, "capacity": 10};',
    )
    wait_for_replication_change(cursor, 2)

    execute_and_fetch_all(
        cursor,
        """CREATE (:L1 {prop1: [1.0, 2.0]})
           CREATE (:L1 {prop1: [3.0, 4.0]})
           CREATE (:L1 {prop1: [5.0, 6.0]});""",
    )
    wait_for_replication_change(cursor, 4)

    # 2/
    expected_result = [("label+property_vector", "L1", "prop1", 3)]
    replica_1_enums = get_show_index_info(get_replica_cursor(connection, "replica_1"))
    assert replica_1_enums == expected_result
    replica_2_enums = get_show_index_info(get_replica_cursor(connection, "replica_2"))
    assert replica_2_enums == expected_result

    # 3/
    execute_and_fetch_all(cursor, "MATCH (n:L1 {prop1: [1.0, 2.0]}) SET n.prop1 = null;")
    wait_for_replication_change(cursor, 6)

    execute_and_fetch_all(cursor, "MATCH (n:L1 {prop1: [3.0, 4.0]}) SET n.prop1 = [7.0, 8.0];")
    wait_for_replication_change(cursor, 8)

    # 4/
    expected_result = [("label+property_vector", "L1", "prop1", 2)]
    replica_1_enums = get_show_index_info(get_replica_cursor(connection, "replica_1"))
    assert replica_1_enums == expected_result
    replica_2_enums = get_show_index_info(get_replica_cursor(connection, "replica_2"))
    assert replica_2_enums == expected_result

    # 5/
    replica_1_cursor = get_replica_cursor(connection, "replica_1")
    replica_2_cursor = get_replica_cursor(connection, "replica_2")

    # Check that property is not visible on node when stored in index
    nodes = execute_and_fetch_all(replica_1_cursor, "MATCH (n:L1) WHERE n.prop1 IS NOT NULL RETURN n LIMIT 1;")
    assert "prop1" not in nodes[0][0].properties, "Property should not be visible on node when stored in index"

    # Check null property
    null_prop = execute_and_fetch_all(replica_1_cursor, "MATCH (n:L1) WHERE n.prop1 IS NULL RETURN count(*) AS cnt;")
    assert null_prop[0][0] == 1

    # Check updated property
    updated_prop = execute_and_fetch_all(replica_1_cursor, "MATCH (n:L1 {prop1: [7.0, 8.0]}) RETURN n.prop1;")
    assert updated_prop[0][0] == [7.0, 8.0]

    # Check original property
    original_prop = execute_and_fetch_all(replica_1_cursor, "MATCH (n:L1 {prop1: [5.0, 6.0]}) RETURN n.prop1;")
    assert original_prop[0][0] == [5.0, 6.0]

    # Verify same on replica_2
    updated_prop2 = execute_and_fetch_all(replica_2_cursor, "MATCH (n:L1 {prop1: [7.0, 8.0]}) RETURN n.prop1;")
    assert updated_prop2[0][0] == [7.0, 8.0]

    interactive_mg_runner.kill_all(keep_directories=False)


def test_vector_index_replication_label_changes(connection, test_name):
    # Goal: That adding and removing labels from nodes with vector properties is correctly replicated.
    # 0/ Setup replication
    # 1/ Create vector index on MAIN, add nodes with labels and vector properties
    # 2/ Validate index count on REPLICA
    # 3/ Remove label from one node, add label to another on MAIN
    # 4/ Validate index count changes are replicated
    # 5/ Validate property access on REPLICA

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = create_instances_description(test_name)

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL)
    cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    execute_and_fetch_all(
        cursor,
        'CREATE VECTOR INDEX test_index ON :L1(prop1) WITH CONFIG {"dimension": 2, "capacity": 10};',
    )
    wait_for_replication_change(cursor, 2)

    execute_and_fetch_all(
        cursor,
        """CREATE (n1:L1 {prop1: [1.0, 2.0]})
           CREATE (n2:L1 {prop1: [3.0, 4.0]})
           CREATE (n3 {prop1: [5.0, 6.0]});""",
    )
    wait_for_replication_change(cursor, 4)

    # 2/
    expected_result = [("label+property_vector", "L1", "prop1", 2)]
    replica_1_enums = get_show_index_info(get_replica_cursor(connection, "replica_1"))
    assert replica_1_enums == expected_result
    replica_2_enums = get_show_index_info(get_replica_cursor(connection, "replica_2"))
    assert replica_2_enums == expected_result

    # 3/
    execute_and_fetch_all(cursor, "MATCH (n:L1 {prop1: [1.0, 2.0]}) REMOVE n:L1;")
    wait_for_replication_change(cursor, 6)

    execute_and_fetch_all(cursor, "MATCH (n {prop1: [5.0, 6.0]}) SET n:L1;")
    wait_for_replication_change(cursor, 8)

    # 4/
    expected_result = [("label+property_vector", "L1", "prop1", 2)]
    replica_1_enums = get_show_index_info(get_replica_cursor(connection, "replica_1"))
    assert replica_1_enums == expected_result
    replica_2_enums = get_show_index_info(get_replica_cursor(connection, "replica_2"))
    assert replica_2_enums == expected_result

    # 5/
    replica_1_cursor = get_replica_cursor(connection, "replica_1")
    replica_2_cursor = get_replica_cursor(connection, "replica_2")

    # Check node with label - property should not be visible
    node_with_label = execute_and_fetch_all(replica_1_cursor, "MATCH (n:L1) RETURN n LIMIT 1;")
    assert (
        "prop1" not in node_with_label[0][0].properties
    ), "Property should not be visible on node when stored in index"

    # Check node without label - property should be in property store
    node_without_label = execute_and_fetch_all(replica_1_cursor, "MATCH (n) WHERE NOT n:L1 RETURN n LIMIT 1;")
    assert (
        "prop1" in node_without_label[0][0].properties
    ), "Property should be in property store when node has no index label"

    # Check property access from index
    prop3_node = execute_and_fetch_all(replica_1_cursor, "MATCH (n:L1 {prop1: [5.0, 6.0]}) RETURN n.prop1;")
    print(prop3_node)
    assert prop3_node[0][0] == [5.0, 6.0]

    interactive_mg_runner.kill_all(keep_directories=False)


# def test_vector_index_replication_two_indices(connection, test_name):
#     # Goal: That two vector indices on different labels/properties are correctly replicated.
#     # 0/ Setup replication
#     # 1/ Create two vector indices on MAIN on different properties
#     # 2/ Add nodes to both indices on MAIN
#     # 3/ Validate both indices are replicated correctly
#     # 4/ Validate property access on REPLICA

#     MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = create_instances_description(test_name)

#     # 0/
#     interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL)
#     cursor = connection(BOLT_PORTS["main"], "main").cursor()

#     # 1/
#     execute_and_fetch_all(
#         cursor,
#         'CREATE VECTOR INDEX test_index ON :L1(prop1) WITH CONFIG {"dimension": 2, "capacity": 10};',
#     )
#     execute_and_fetch_all(
#         cursor,
#         'CREATE VECTOR INDEX test_index2 ON :L2(prop2) WITH CONFIG {"dimension": 2, "capacity": 10};',
#     )
#     wait_for_replication_change(cursor, 4)

#     # 2/
#     execute_and_fetch_all(
#         cursor,
#         """CREATE (:L1 {prop1: [1.0, 2.0]})
#            CREATE (:L1 {prop1: [3.0, 4.0]})
#            CREATE (:L2 {prop2: [5.0, 6.0]})
#            CREATE (:L2 {prop2: [7.0, 8.0]});""",
#     )
#     wait_for_replication_change(cursor, 8)

#     # 3/
#     replica_1_cursor = get_replica_cursor(connection, "replica_1")
#     replica_2_cursor = get_replica_cursor(connection, "replica_2")

#     index_info_1 = get_show_index_info(replica_1_cursor)
#     index_info_1 = sorted(index_info_1, key=lambda x: x[2])
#     assert len(index_info_1) == 2
#     assert index_info_1[0][6] == 2
#     assert index_info_1[1][6] == 2

#     index_info_2 = get_show_index_info(replica_2_cursor)
#     index_info_2 = sorted(index_info_2, key=lambda x: x[2])
#     assert len(index_info_2) == 2
#     assert index_info_2[0][6] == 2
#     assert index_info_2[1][6] == 2

#     # 4/
#     # Check that properties are not visible on nodes when stored in index
#     node_l1 = execute_and_fetch_all(replica_1_cursor, "MATCH (n:L1) RETURN n LIMIT 1;")
#     assert "prop1" not in node_l1[0][0].properties, "Property should not be visible on node when stored in index"

#     node_l2 = execute_and_fetch_all(replica_1_cursor, "MATCH (n:L2) RETURN n LIMIT 1;")
#     assert "prop2" not in node_l2[0][0].properties, "Property should not be visible on node when stored in index"

#     # Check property access from indices
#     prop1 = execute_and_fetch_all(replica_1_cursor, "MATCH (n:L1) RETURN n.prop1 LIMIT 1;")
#     assert prop1[0][0] in [[1.0, 2.0], [3.0, 4.0]]

#     prop2 = execute_and_fetch_all(replica_1_cursor, "MATCH (n:L2) RETURN n.prop2 LIMIT 1;")
#     assert prop2[0][0] in [[5.0, 6.0], [7.0, 8.0]]

#     # Verify same on replica_2
#     prop1_2 = execute_and_fetch_all(replica_2_cursor, "MATCH (n:L1) RETURN n.prop1 LIMIT 1;")
#     assert prop1_2[0][0] in [[1.0, 2.0], [3.0, 4.0]]

#     prop2_2 = execute_and_fetch_all(replica_2_cursor, "MATCH (n:L2) RETURN n.prop2 LIMIT 1;")
#     assert prop2_2[0][0] in [[5.0, 6.0], [7.0, 8.0]]

#     interactive_mg_runner.kill_all(keep_directories=False)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
