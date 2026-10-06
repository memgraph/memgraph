# Copyright 2025 Memgraph Ltd.
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

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))
interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.BUILD_DIR, "query_modules")
)

FILE = "durability_with_vector_index"


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


def test_durability_with_vector_index_basic(connection, test_name):
    # Goal: That vector indices and their data are correctly restored after restart.

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": f"{get_logs_path(FILE, test_name)}/main.log",
            "data_directory": get_data_path(FILE, test_name),
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX test_index ON :L1(prop1) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )

    execute_and_fetch_all(
        cursor,
        """CREATE (:L1 {prop1: [1.0, 2.0]})
           CREATE (:L1 {prop1: [3.0, 4.0]})
           CREATE (:L1 {prop1: [5.0, 6.0]});""",
    )

    def get_vector_index_info(cursor):
        return execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")

    def vector_search(cursor, index_name, limit, query_vector):
        return execute_and_fetch_all(
            cursor,
            f"CALL vector_search.search('{index_name}', {limit}, {query_vector}) YIELD * RETURN *;",
        )

    index_info = get_vector_index_info(cursor)
    assert len(index_info) == 1
    assert index_info[0][6] == 3

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    index_info = get_vector_index_info(cursor)
    assert len(index_info) == 1
    assert index_info[0][6] == 3

    nodes = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN n LIMIT 1;")
    node = nodes[0][0]
    assert "prop1" in node.properties, "Property should be visible on node"

    props = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN n.prop1 ORDER BY n.prop1[0] LIMIT 1;")
    assert len(props) == 1
    assert props[0][0] == [1.0, 2.0]

    property_size = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN propertySize(n, 'prop1') LIMIT 1;")
    assert property_size[0][0] == 11

    search_results = vector_search(cursor, "test_index", 1, [1.0, 2.0])
    assert len(search_results) == 1
    assert search_results[0][0] == 0.0

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main", keep_directories=False)


def test_durability_with_vector_index_label_changes(connection, test_name):
    # Goal: That adding and removing labels from nodes with vector properties is correctly persisted.

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": f"{get_logs_path(FILE, test_name)}/main.log",
            "data_directory": get_data_path(FILE, test_name),
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX test_index ON :L1(prop1) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )

    execute_and_fetch_all(
        cursor,
        """CREATE (n1:L1 {prop1: [1.0, 2.0]})
           CREATE (n2:L1 {prop1: [3.0, 4.0]})
           CREATE (n3 {prop1: [5.0, 6.0]});""",
    )

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert index_info[0][6] == 2

    execute_and_fetch_all(cursor, "MATCH (n:L1 {prop1: [1.0, 2.0]}) REMOVE n:L1;")
    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert index_info[0][6] == 1

    execute_and_fetch_all(cursor, "MATCH (n {prop1: [5.0, 6.0]}) SET n:L1;")
    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert index_info[0][6] == 2

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert index_info[0][6] == 2

    node_with_label = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN n LIMIT 1;")
    assert "prop1" in node_with_label[0][0].properties

    property_size_label = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN propertySize(n, 'prop1') LIMIT 1;")
    assert property_size_label[0][0] == 11

    node_without_label = execute_and_fetch_all(cursor, "MATCH (n) WHERE NOT n:L1 RETURN n LIMIT 1;")
    assert "prop1" in node_without_label[0][0].properties

    property_size_no_label = execute_and_fetch_all(
        cursor, "MATCH (n) WHERE NOT n:L1 RETURN propertySize(n, 'prop1') LIMIT 1;"
    )
    assert property_size_no_label[0][0] == 20

    prop3_node = execute_and_fetch_all(cursor, "MATCH (n:L1 {prop1: [5.0, 6.0]}) RETURN n.prop1;")
    assert prop3_node[0][0] == [5.0, 6.0]

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main", keep_directories=False)


def test_durability_with_vector_index_property_changes(connection, test_name):
    # Goal: That setting properties to null and updating vector properties is correctly persisted.

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": f"{get_logs_path(FILE, test_name)}/main.log",
            "data_directory": get_data_path(FILE, test_name),
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX test_index ON :L1(prop1) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )

    execute_and_fetch_all(
        cursor,
        """CREATE (:L1 {prop1: [1.0, 2.0]})
           CREATE (:L1 {prop1: [3.0, 4.0]})
           CREATE (:L1 {prop1: [5.0, 6.0]});""",
    )

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert index_info[0][6] == 3

    execute_and_fetch_all(cursor, "MATCH (n:L1 {prop1: [1.0, 2.0]}) SET n.prop1 = null;")
    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert index_info[0][6] == 2

    execute_and_fetch_all(cursor, "MATCH (n:L1 {prop1: [3.0, 4.0]}) SET n.prop1 = [7.0, 8.0];")

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert index_info[0][6] == 2

    nodes = execute_and_fetch_all(cursor, "MATCH (n:L1) WHERE n.prop1 IS NOT NULL RETURN n LIMIT 1;")
    assert "prop1" in nodes[0][0].properties

    property_size = execute_and_fetch_all(
        cursor, "MATCH (n:L1) WHERE n.prop1 IS NOT NULL RETURN propertySize(n, 'prop1') LIMIT 1;"
    )
    assert property_size[0][0] == 11

    null_prop = execute_and_fetch_all(cursor, "MATCH (n:L1) WHERE n.prop1 IS NULL RETURN count(*) AS cnt;")
    assert null_prop[0][0] == 1

    updated_prop = execute_and_fetch_all(cursor, "MATCH (n:L1 {prop1: [7.0, 8.0]}) RETURN n.prop1;")
    assert updated_prop[0][0] == [7.0, 8.0]

    original_prop = execute_and_fetch_all(cursor, "MATCH (n:L1 {prop1: [5.0, 6.0]}) RETURN n.prop1;")
    assert original_prop[0][0] == [5.0, 6.0]

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main", keep_directories=False)


def test_durability_with_two_vector_indices(connection, test_name):
    # Goal: That two vector indices on different labels/properties are correctly restored.

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": f"{get_logs_path(FILE, test_name)}/main.log",
            "data_directory": get_data_path(FILE, test_name),
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX test_index ON :L1(prop1) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )
    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX test_index2 ON :L2(prop2) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )

    execute_and_fetch_all(
        cursor,
        """CREATE (:L1 {prop1: [1.0, 2.0]})
           CREATE (:L1 {prop1: [3.0, 4.0]})
           CREATE (:L2 {prop2: [5.0, 6.0]})
           CREATE (:L2 {prop2: [7.0, 8.0]});""",
    )

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    index_info = sorted(index_info, key=lambda x: x[2])
    assert len(index_info) == 2
    assert index_info[0][6] == 2
    assert index_info[1][6] == 2

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    index_info = sorted(index_info, key=lambda x: x[2])
    assert len(index_info) == 2
    assert index_info[0][6] == 2
    assert index_info[1][6] == 2

    node_l1 = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN n LIMIT 1;")
    assert "prop1" in node_l1[0][0].properties

    property_size_l1 = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN propertySize(n, 'prop1') LIMIT 1;")
    assert property_size_l1[0][0] == 11

    node_l2 = execute_and_fetch_all(cursor, "MATCH (n:L2) RETURN n LIMIT 1;")
    assert "prop2" in node_l2[0][0].properties

    property_size_l2 = execute_and_fetch_all(cursor, "MATCH (n:L2) RETURN propertySize(n, 'prop2') LIMIT 1;")
    assert property_size_l2[0][0] == 11

    prop1 = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN n.prop1 LIMIT 1;")
    assert prop1[0][0] in [[1.0, 2.0], [3.0, 4.0]]

    prop2 = execute_and_fetch_all(cursor, "MATCH (n:L2) RETURN n.prop2 LIMIT 1;")
    assert prop2[0][0] in [[5.0, 6.0], [7.0, 8.0]]

    search1 = execute_and_fetch_all(cursor, "CALL vector_search.search('test_index', 1, [1.0, 2.0]) YIELD * RETURN *;")
    assert len(search1) == 1

    search2 = execute_and_fetch_all(cursor, "CALL vector_search.search('test_index2', 1, [5.0, 6.0]) YIELD * RETURN *;")
    assert len(search2) == 1

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main", keep_directories=False)


def test_durability_with_two_vector_indices_drop_one(connection, test_name):
    # Goal: That dropping one vector index preserves vectors for remaining index on node with both labels.

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": f"{get_logs_path(FILE, test_name)}/main.log",
            "data_directory": get_data_path(FILE, test_name),
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX test_index ON :L1(prop1) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )
    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX test_index2 ON :L2(prop1) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )

    execute_and_fetch_all(cursor, "CREATE (n:L1:L2 {prop1: [1.0, 2.0]});")

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    index_info = sorted(index_info, key=lambda x: x[2])
    assert len(index_info) == 2
    assert index_info[0][6] == 1
    assert index_info[1][6] == 1

    execute_and_fetch_all(cursor, "DROP VECTOR INDEX test_index;")

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert len(index_info) == 1
    assert index_info[0][6] == 1

    # Verify vector is still accessible via remaining index
    prop = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN n.prop1;")
    assert prop[0][0] == [1.0, 2.0]

    prop2 = execute_and_fetch_all(cursor, "MATCH (n:L2) RETURN n.prop1;")
    assert prop2[0][0] == [1.0, 2.0]

    node = execute_and_fetch_all(cursor, "MATCH (n:L1:L2) RETURN n LIMIT 1;")
    assert "prop1" in node[0][0].properties

    property_size = execute_and_fetch_all(cursor, "MATCH (n:L1:L2) RETURN propertySize(n, 'prop1') LIMIT 1;")
    assert property_size[0][0] == 11

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert len(index_info) == 1
    assert index_info[0][6] == 1

    node = execute_and_fetch_all(cursor, "MATCH (n:L1:L2) RETURN n LIMIT 1;")
    assert "prop1" in node[0][0].properties

    property_size = execute_and_fetch_all(cursor, "MATCH (n:L1:L2) RETURN propertySize(n, 'prop1') LIMIT 1;")
    assert property_size[0][0] == 11
    prop = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN n.prop1;")
    assert prop[0][0] == [1.0, 2.0]

    prop2 = execute_and_fetch_all(cursor, "MATCH (n:L2) RETURN n.prop1;")
    assert prop2[0][0] == [1.0, 2.0]

    search = execute_and_fetch_all(cursor, "CALL vector_search.search('test_index2', 1, [1.0, 2.0]) YIELD * RETURN *;")
    assert len(search) == 1
    assert search[0][0] == 0.0

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main", keep_directories=False)


def test_durability_with_vector_index_drop_single_index(connection, test_name):
    # Goal: That dropping a single vector index restores vectors to property store and persists correctly.

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": f"{get_logs_path(FILE, test_name)}/main.log",
            "data_directory": get_data_path(FILE, test_name),
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX test_index ON :L1(prop1) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )

    execute_and_fetch_all(
        cursor,
        """CREATE (:L1 {prop1: [1.0, 2.0]})
           CREATE (:L1 {prop1: [3.0, 4.0]})
           CREATE (:L1 {prop1: [5.0, 6.0]});""",
    )

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert len(index_info) == 1
    assert index_info[0][6] == 3

    node = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN n LIMIT 1;")
    assert "prop1" in node[0][0].properties

    property_size = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN propertySize(n, 'prop1') LIMIT 1;")
    assert property_size[0][0] == 11

    execute_and_fetch_all(cursor, "DROP VECTOR INDEX test_index;")

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert len(index_info) == 0

    node = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN n LIMIT 1;")
    assert "prop1" in node[0][0].properties

    property_size = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN propertySize(n, 'prop1') LIMIT 1;")
    assert property_size[0][0] == 20

    # Verify vectors are accessible
    props = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN n.prop1 ORDER BY n.prop1[0];")
    assert len(props) == 3
    assert props[0][0] == [1.0, 2.0]
    assert props[1][0] == [3.0, 4.0]
    assert props[2][0] == [5.0, 6.0]

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert len(index_info) == 0

    node = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN n LIMIT 1;")
    assert "prop1" in node[0][0].properties

    property_size = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN propertySize(n, 'prop1') LIMIT 1;")
    assert property_size[0][0] == 20

    # Verify all vectors are still accessible
    props = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN n.prop1 ORDER BY n.prop1[0];")
    assert len(props) == 3
    assert props[0][0] == [1.0, 2.0]
    assert props[1][0] == [3.0, 4.0]
    assert props[2][0] == [5.0, 6.0]

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main", keep_directories=False)


def test_durability_with_two_vector_indices_remove_one_label(connection, test_name):
    # Goal: That removing one label from node with two labels keeps property in remaining index, not on node.

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": f"{get_logs_path(FILE, test_name)}/main.log",
            "data_directory": get_data_path(FILE, test_name),
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX test_index ON :L1(prop1) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )
    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX test_index2 ON :L2(prop1) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )

    execute_and_fetch_all(cursor, "CREATE (n:L1:L2 {prop1: [1.0, 2.0]});")

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    index_info = sorted(index_info, key=lambda x: x[2])
    assert index_info[0][6] == 1
    assert index_info[1][6] == 1

    execute_and_fetch_all(cursor, "MATCH (n:L1:L2 {prop1: [1.0, 2.0]}) REMOVE n:L1;")

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert index_info[0][6] == 0 and index_info[1][6] == 1 or index_info[0][6] == 1 and index_info[1][6] == 0

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert index_info[0][6] == 0 and index_info[1][6] == 1 or index_info[0][6] == 1 and index_info[1][6] == 0

    node = execute_and_fetch_all(cursor, "MATCH (n:L2) RETURN n LIMIT 1;")
    assert "prop1" in node[0][0].properties

    property_size = execute_and_fetch_all(cursor, "MATCH (n:L2) RETURN propertySize(n, 'prop1') LIMIT 1;")
    assert property_size[0][0] == 11

    prop = execute_and_fetch_all(cursor, "MATCH (n:L2) RETURN n.prop1;")
    assert prop[0][0] == [1.0, 2.0]

    search = execute_and_fetch_all(cursor, "CALL vector_search.search('test_index2', 1, [1.0, 2.0]) YIELD * RETURN *;")
    assert len(search) == 1
    assert search[0][0] == 0.0

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main", keep_directories=False)


def test_durability_with_two_vector_indices_remove_both_labels(connection, test_name):
    # Goal: That removing both labels from node transfers property to property store, not in any index.

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": f"{get_logs_path(FILE, test_name)}/main.log",
            "data_directory": get_data_path(FILE, test_name),
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX test_index ON :L1(prop1) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )
    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX test_index2 ON :L2(prop1) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )

    execute_and_fetch_all(cursor, "CREATE (n:L1:L2 {prop1: [1.0, 2.0]});")

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert index_info[0][6] == 1 and index_info[1][6] == 1 or index_info[0][6] == 1 and index_info[1][6] == 1

    execute_and_fetch_all(cursor, "MATCH (n:L1:L2 {prop1: [1.0, 2.0]}) REMOVE n:L1, n:L2;")

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    index_info = sorted(index_info, key=lambda x: x[2])
    assert index_info[0][6] == 0
    assert index_info[1][6] == 0

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    index_info = sorted(index_info, key=lambda x: x[2])
    assert len(index_info) == 2
    assert index_info[0][6] == 0
    assert index_info[1][6] == 0

    node = execute_and_fetch_all(cursor, "MATCH (n {prop1: [1.0, 2.0]}) WHERE NOT n:L1 AND NOT n:L2 RETURN n LIMIT 1;")
    assert "prop1" in node[0][0].properties

    property_size = execute_and_fetch_all(
        cursor, "MATCH (n {prop1: [1.0, 2.0]}) WHERE NOT n:L1 AND NOT n:L2 RETURN propertySize(n, 'prop1') LIMIT 1;"
    )
    assert property_size[0][0] == 20

    prop_check = execute_and_fetch_all(cursor, "MATCH (n {prop1: [1.0, 2.0]}) RETURN n.prop1;")
    assert prop_check[0][0] == [1.0, 2.0]

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main", keep_directories=False)


def test_durability_with_vector_index_nodes_before_index_creation(connection, test_name):
    # Goal: That nodes created before index creation and nodes added after index creation are correctly restored.

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": f"{get_logs_path(FILE, test_name)}/main.log",
            "data_directory": get_data_path(FILE, test_name),
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    execute_and_fetch_all(
        cursor,
        """CREATE (:L1 {prop1: [1.0, 2.0]})
           CREATE (:L1 {prop1: [3.0, 4.0]});""",
    )

    # Verify nodes exist but no index yet
    node_count = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN count(*) AS cnt;")
    assert node_count[0][0] == 2

    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX test_index ON :L1(prop1) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )

    # Verify index was created and existing nodes were indexed
    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert len(index_info) == 1
    assert index_info[0][6] == 2

    execute_and_fetch_all(
        cursor,
        """CREATE (:L1 {prop1: [5.0, 6.0]})
           CREATE (:L1 {prop1: [7.0, 8.0]});""",
    )

    # Verify all nodes are now indexed
    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert index_info[0][6] == 4

    node_count = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN count(*) AS cnt;")
    assert node_count[0][0] == 4

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert len(index_info) == 1
    assert index_info[0][6] == 4

    node_count = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN count(*) AS cnt;")
    assert node_count[0][0] == 4

    nodes = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN n LIMIT 1;")
    assert "prop1" in nodes[0][0].properties

    property_size = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN propertySize(n, 'prop1') LIMIT 1;")
    assert property_size[0][0] == 11

    # Verify all vector properties are accessible from index
    props = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN n.prop1 ORDER BY n.prop1[0];")
    assert len(props) == 4
    assert props[0][0] == [1.0, 2.0]
    assert props[1][0] == [3.0, 4.0]
    assert props[2][0] == [5.0, 6.0]
    assert props[3][0] == [7.0, 8.0]

    # Verify vector search works for nodes created before index
    search_results = execute_and_fetch_all(
        cursor, "CALL vector_search.search('test_index', 1, [1.0, 2.0]) YIELD * RETURN *;"
    )
    assert len(search_results) == 1
    assert search_results[0][0] == 0.0

    # Verify vector search works for nodes created after index
    search_results = execute_and_fetch_all(
        cursor, "CALL vector_search.search('test_index', 1, [7.0, 8.0]) YIELD * RETURN *;"
    )
    assert len(search_results) == 1
    assert search_results[0][0] == 0.0

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main", keep_directories=False)


def test_durability_with_vector_index_snapshot_and_wal(connection, test_name):
    # Goal: That vector indices and their data are correctly restored after snapshot and WAL replay.

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": f"{get_logs_path(FILE, test_name)}/main.log",
            "data_directory": get_data_path(FILE, test_name),
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX test_index ON :L1(prop1) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )

    execute_and_fetch_all(
        cursor,
        """CREATE (:L1 {prop1: [1.0, 2.0]})
           CREATE (:L1 {prop1: [3.0, 4.0]});""",
    )

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert index_info[0][6] == 2

    execute_and_fetch_all(cursor, "CREATE SNAPSHOT;")

    execute_and_fetch_all(cursor, "CREATE (:L1 {prop1: [5.0, 6.0]});")

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert index_info[0][6] == 3

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert len(index_info) == 1
    assert index_info[0][6] == 3

    props = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN n.prop1 ORDER BY n.prop1[0];")
    assert len(props) == 3
    assert props[0][0] == [1.0, 2.0]
    assert props[1][0] == [3.0, 4.0]
    assert props[2][0] == [5.0, 6.0]

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main", keep_directories=False)


def test_durability_creating_index_after_vector_already_in_another_index(connection, test_name):
    # Goal: Verify that when a node has a vector property already indexed by one index,
    # creating a second index on the same property (but different label) works correctly after WAL recovery.

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": f"{get_logs_path(FILE, test_name)}/main.log",
            "data_directory": get_data_path(FILE, test_name),
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX test_index ON :L1(prop1) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )
    execute_and_fetch_all(cursor, "CREATE (n:L1:L2 {prop1: [1.0, 2.0]});")
    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX test_index2 ON :L2(prop1) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    index_info = sorted(index_info, key=lambda x: x[2])
    assert len(index_info) == 2
    assert index_info[0][6] == 1
    assert index_info[1][6] == 1

    assert (
        len(execute_and_fetch_all(cursor, "CALL vector_search.search('test_index', 2, [1.0, 2.0]) YIELD * RETURN *;"))
        == 1
    )
    assert (
        len(execute_and_fetch_all(cursor, "CALL vector_search.search('test_index2', 2, [1.0, 2.0]) YIELD * RETURN *;"))
        == 1
    )

    property_size = execute_and_fetch_all(cursor, "MATCH (n:L1:L2) RETURN propertySize(n, 'prop1');")
    assert property_size[0][0] == 19

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    index_info = sorted(index_info, key=lambda x: x[2])
    assert len(index_info) == 2
    assert index_info[0][6] == 1
    assert index_info[1][6] == 1

    property_size = execute_and_fetch_all(cursor, "MATCH (n:L1:L2) RETURN propertySize(n, 'prop1');")
    assert property_size[0][0] == 19

    assert (
        len(execute_and_fetch_all(cursor, "CALL vector_search.search('test_index', 2, [1.0, 2.0]) YIELD * RETURN *;"))
        == 1
    )
    assert (
        len(execute_and_fetch_all(cursor, "CALL vector_search.search('test_index2', 2, [1.0, 2.0]) YIELD * RETURN *;"))
        == 1
    )

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main", keep_directories=False)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))


def test_durability_with_wildcard_vector_index(connection, test_name):
    # Goal: Wildcard vector index (ON :*) indexes all vertices and survives restart.

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": f"{get_logs_path(FILE, test_name)}/main.log",
            "data_directory": get_data_path(FILE, test_name),
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX wildcard_idx ON (embedding) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )

    execute_and_fetch_all(
        cursor,
        """CREATE (:A {embedding: [1.0, 2.0]})
           CREATE (:B {embedding: [3.0, 4.0]})
           CREATE ({embedding: [5.0, 6.0]});""",
    )

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert len(index_info) == 1
    assert index_info[0][6] == 3  # size column

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert len(index_info) == 1
    assert index_info[0][6] == 3

    search = execute_and_fetch_all(
        cursor, "CALL vector_search.search('wildcard_idx', 3, [1.0, 2.0]) YIELD * RETURN * ORDER BY distance;"
    )
    assert len(search) == 3
    assert search[0][0] == 0.0  # exact match distance

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main", keep_directories=False)


def test_durability_with_or_vector_index(connection, test_name):
    # Goal: OR vector index (ON :A|B) indexes vertices with any matching label and survives restart.

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": f"{get_logs_path(FILE, test_name)}/main.log",
            "data_directory": get_data_path(FILE, test_name),
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX or_idx ON :A|B(embedding) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )

    execute_and_fetch_all(
        cursor,
        """CREATE (:A {embedding: [1.0, 2.0]})
           CREATE (:B {embedding: [3.0, 4.0]})
           CREATE (:C {embedding: [5.0, 6.0]});""",
    )

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert len(index_info) == 1
    assert index_info[0][6] == 2  # only A and B nodes, not C

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert len(index_info) == 1
    assert index_info[0][6] == 2

    search = execute_and_fetch_all(
        cursor, "CALL vector_search.search('or_idx', 10, [1.0, 2.0]) YIELD * RETURN * ORDER BY distance;"
    )
    assert len(search) == 2

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main", keep_directories=False)


def test_durability_with_and_vector_index(connection, test_name):
    # Goal: AND vector index (ON :A&B) indexes only vertices with ALL matching labels and survives restart.

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": f"{get_logs_path(FILE, test_name)}/main.log",
            "data_directory": get_data_path(FILE, test_name),
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX and_idx ON :A&B(embedding) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )

    execute_and_fetch_all(
        cursor,
        """CREATE (:A:B {embedding: [1.0, 2.0]})
           CREATE (:A {embedding: [3.0, 4.0]})
           CREATE (:B {embedding: [5.0, 6.0]});""",
    )

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert len(index_info) == 1
    assert index_info[0][6] == 1  # only the A:B node

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert len(index_info) == 1
    assert index_info[0][6] == 1

    search = execute_and_fetch_all(
        cursor, "CALL vector_search.search('and_idx', 10, [1.0, 2.0]) YIELD * RETURN * ORDER BY distance;"
    )
    assert len(search) == 1
    assert search[0][0] == 0.0

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main", keep_directories=False)


_POST_RESTART_REMOVE_LABEL = {
    "node_added_to_two_indexes_via_label_addition": ("MATCH (n:I) REMOVE n:I;", {"idxI": 0, "idxJ": 1}),
}


@pytest.mark.parametrize(
    "scenario,queries,expected_indexes,expected_embedding,prop",
    [
        (
            "index_after_node",
            [
                "CREATE (:A {embedding: [1.0, 2.0]});",
                'CREATE VECTOR INDEX idx ON :A(embedding) WITH CONFIG {"dimension": 2, "capacity": 100};',
                "MATCH (n:A) REMOVE n:A;",
            ],
            {"idx": 0},
            [1.0, 2.0],
            "embedding",
        ),
        (
            "and_index_member_loses_both_labels",
            [
                'CREATE VECTOR INDEX idx ON :A&B(embedding) WITH CONFIG {"dimension": 2, "capacity": 100};',
                "CREATE (:A:B {id: 1, embedding: [1.0, 2.0]});",
                "MATCH (n {id: 1}) REMOVE n:A;",
                "MATCH (n {id: 1}) REMOVE n:B;",
            ],
            {"idx": 0},
            [1.0, 2.0],
            "embedding",
        ),
        (
            "and_index_non_member_loses_filter_label",
            [
                'CREATE VECTOR INDEX idx ON :A&B(embedding) WITH CONFIG {"dimension": 2, "capacity": 100};',
                "CREATE (:A {id: 1, embedding: [1.0, 2.0]});",
                "MATCH (n {id: 1}) REMOVE n:A;",
            ],
            {"idx": 0},
            [1.0, 2.0],
            "embedding",
        ),
        (
            "control_member_created_after_index",
            [
                'CREATE VECTOR INDEX idx ON :A(embedding) WITH CONFIG {"dimension": 2, "capacity": 100};',
                "CREATE (:A {embedding: [1.0, 2.0]});",
                "MATCH (n:A) REMOVE n:A;",
            ],
            {"idx": 0},
            [1.0, 2.0],
            "embedding",
        ),
        (
            "label_added_after_node_creation",
            [
                'CREATE VECTOR INDEX idx ON :A(embedding) WITH CONFIG {"dimension": 2, "capacity": 100};',
                "CREATE (n {embedding: [1.0, 2.0]}) SET n:A;",
            ],
            {"idx": 1},
            [1.0, 2.0],
            "embedding",
        ),
        (
            "label_cycle_with_property_update",
            [
                'CREATE VECTOR INDEX idx ON :A(embedding) WITH CONFIG {"dimension": 2, "capacity": 100};',
                "CREATE (:A {embedding: [1.0, 2.0]});",
                "MATCH (n:A) REMOVE n:A SET n.embedding = [3.0, 4.0] SET n:A;",
            ],
            {"idx": 1},
            [3.0, 4.0],
            "embedding",
        ),
        (
            "and_index_label_cycle_with_property_update",
            [
                'CREATE VECTOR INDEX idx ON :A&B(embedding) WITH CONFIG {"dimension": 2, "capacity": 100};',
                "CREATE (:A {id: 1, embedding: [1.0, 2.0]});",
                "MATCH (n {id: 1}) SET n.embedding = [3.0, 4.0] REMOVE n:A SET n:B SET n:A;",
            ],
            {"idx": 1},
            [3.0, 4.0],
            "embedding",
        ),
        (
            "node_added_to_two_indexes_via_label_addition",
            [
                'CREATE VECTOR INDEX idxI ON :I(embedding) WITH CONFIG {"dimension": 2, "capacity": 100};',
                'CREATE VECTOR INDEX idxJ ON :J(embedding) WITH CONFIG {"dimension": 2, "capacity": 100};',
                "CREATE (:J {embedding: [1.0, 2.0]});",
                "MATCH (n:J) SET n.embedding = [3.0, 4.0] SET n:I;",
            ],
            {"idxI": 1, "idxJ": 1},
            [3.0, 4.0],
            "embedding",
        ),
    ],
)
def test_durability_vector_index_membership_after_wal_replay(
    connection, test_name, scenario, queries, expected_indexes, expected_embedding, prop
):
    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--storage-wal-file-flush-every-n-tx=1",
                "--storage-snapshot-on-exit=false",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": f"{get_logs_path(FILE, test_name)}/main.log",
            "data_directory": get_data_path(FILE, test_name),
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    for query in queries:
        execute_and_fetch_all(cursor, query)

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert len(index_info) == len(expected_indexes)
    for row in index_info:
        assert row[0] in expected_indexes, f"Unexpected index before restart: {row[0]}"
        assert row[6] == expected_indexes[row[0]]

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert len(index_info) == len(expected_indexes)
    info_by_name = {row[0]: row for row in index_info}
    for name, size in expected_indexes.items():
        assert name in info_by_name, f"Index missing after restart: {name}"
        assert info_by_name[name][6] == size

    embedding = execute_and_fetch_all(cursor, f"MATCH (n) RETURN n.{prop};")
    assert len(embedding) == 1
    assert embedding[0][0] == expected_embedding

    if scenario in _POST_RESTART_REMOVE_LABEL:
        remove_query, sizes_after_removal = _POST_RESTART_REMOVE_LABEL[scenario]
        execute_and_fetch_all(cursor, remove_query)
        info_by_name = {row[0]: row for row in execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")}
        assert {name: row[6] for name, row in info_by_name.items()} == sizes_after_removal
        embedding = execute_and_fetch_all(cursor, f"MATCH (n) RETURN n.{prop};")
        assert len(embedding) == 1
        assert embedding[0][0] == expected_embedding

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main", keep_directories=False)


_IDX_CREATE = 'CREATE VECTOR INDEX idx ON :L(emb) WITH CONFIG {"dimension": 2, "capacity": 10};'


@pytest.mark.parametrize("mode", ["wal", "snapshot"])
@pytest.mark.parametrize(
    "scenario,queries,read_query,expected_indexes",
    [
        pytest.param(
            "set_member_to_empty",
            [_IDX_CREATE, "CREATE (:L {emb: [1.0, 2.0]});", "MATCH (n:L) SET n.emb = [];"],
            "MATCH (n:L) RETURN n.emb;",
            {"idx": 0},
            id="set_member_to_empty",
        ),
        pytest.param(
            "drop_index",
            [_IDX_CREATE, "CREATE (:L {emb: []});", "DROP VECTOR INDEX idx;"],
            "MATCH (n:L) RETURN n.emb;",
            {},
            id="drop_index",
        ),
        pytest.param(
            "index_over_existing_empty",
            ["CREATE (:L {emb: []});", _IDX_CREATE, "DROP VECTOR INDEX idx;"],
            "MATCH (n:L) RETURN n.emb;",
            {},
            id="index_over_existing_empty",
        ),
        pytest.param(
            "remove_label",
            [_IDX_CREATE, "CREATE (:L {id: 1, emb: []});", "MATCH (n {id: 1}) REMOVE n:L;"],
            "MATCH (n {id: 1}) RETURN n.emb;",
            {"idx": 0},
            id="remove_label",
        ),
        pytest.param(
            "add_then_remove_label",
            [
                _IDX_CREATE,
                "CREATE (:X {id: 1, emb: []});",
                "MATCH (n {id: 1}) SET n:L;",
                "MATCH (n {id: 1}) REMOVE n:L;",
            ],
            "MATCH (n {id: 1}) RETURN n.emb;",
            {"idx": 0},
            id="add_then_remove_label",
        ),
    ],
)
def test_durability_vector_index_empty_list(
    connection, test_name, mode, scenario, queries, read_query, expected_indexes
):
    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--storage-wal-file-flush-every-n-tx=1",
                "--storage-snapshot-on-exit=false",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": f"{get_logs_path(FILE, test_name)}/main.log",
            "data_directory": get_data_path(FILE, test_name),
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    for query in queries:
        execute_and_fetch_all(cursor, query)

    emb_before = execute_and_fetch_all(cursor, read_query)
    assert len(emb_before) == 1, f"[{scenario}/{mode}] Expected one row before restart, got {len(emb_before)}"
    assert emb_before[0][0] == [], f"[{scenario}/{mode}] Expected [] before restart, got {emb_before[0][0]!r}"

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert len(index_info) == len(
        expected_indexes
    ), f"[{scenario}/{mode}] Expected {len(expected_indexes)} index(es) before restart, got {len(index_info)}"
    for row in index_info:
        assert row[0] in expected_indexes, f"[{scenario}/{mode}] Unexpected index before restart: {row[0]}"
        assert (
            row[6] == expected_indexes[row[0]]
        ), f"[{scenario}/{mode}] Expected index '{row[0]}' size {expected_indexes[row[0]]} before restart, got {row[6]}"

    if mode == "snapshot":
        execute_and_fetch_all(cursor, "CREATE SNAPSHOT;")

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    emb_after = execute_and_fetch_all(cursor, read_query)
    assert len(emb_after) == 1, f"[{scenario}/{mode}] Expected one row after restart, got {len(emb_after)}"
    assert emb_after[0][0] == [], f"[{scenario}/{mode}] Expected [] after restart, got {emb_after[0][0]!r}"

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert len(index_info) == len(
        expected_indexes
    ), f"[{scenario}/{mode}] Expected {len(expected_indexes)} index(es) after restart, got {len(index_info)}"
    info_by_name = {row[0]: row for row in index_info}
    for name, size in expected_indexes.items():
        assert name in info_by_name, f"[{scenario}/{mode}] Index missing after restart: {name}"
        assert (
            info_by_name[name][6] == size
        ), f"[{scenario}/{mode}] Expected index '{name}' size {size} after restart, got {info_by_name[name][6]}"

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main", keep_directories=False)
