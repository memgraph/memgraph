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
import tempfile

import interactive_mg_runner
import pytest
from common import execute_and_fetch_all

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))
interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.BUILD_DIR, "query_modules")
)


def test_durability_with_vector_index_basic(connection):
    # Goal: That vector indices and their data are correctly restored after restart.
    # 1/ Create vector index, add nodes with vector properties
    # 2/ Kill and restart
    # 3/ Validate vector index and data are restored

    data_directory = tempfile.TemporaryDirectory()

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": "main_durability_with_vector_index_basic.log",
            "data_directory": data_directory.name,
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    # 1/
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

    # 2/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    # 3/
    index_info = get_vector_index_info(cursor)
    assert len(index_info) == 1
    assert index_info[0][6] == 3

    nodes = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN n LIMIT 1;")
    node = nodes[0][0]
    assert "prop1" not in node.properties, "Property should not be visible on node when stored in index"

    props = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN n.prop1 ORDER BY n.prop1[0] LIMIT 1;")
    assert len(props) == 1, "Property should be accessible from index"
    assert props[0][0] == [1.0, 2.0], "Property should be accessible from index"

    search_results = vector_search(cursor, "test_index", 1, [1.0, 2.0])
    assert len(search_results) == 1
    assert search_results[0][0] == 0.0

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")


def test_durability_with_vector_index_label_changes(connection):
    # Goal: That adding and removing labels from nodes with vector properties is correctly persisted.
    # 1/ Create vector index, add nodes with labels and vector properties
    # 2/ Remove label from one node, add label to another
    # 3/ Kill and restart
    # 4/ Validate all changes are restored correctly

    data_directory = tempfile.TemporaryDirectory()

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": "main_durability_vector_label_changes.log",
            "data_directory": data_directory.name,
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    # 1/
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

    # 2/
    execute_and_fetch_all(cursor, "MATCH (n:L1 {prop1: [1.0, 2.0]}) REMOVE n:L1;")
    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert index_info[0][6] == 1

    execute_and_fetch_all(cursor, "MATCH (n {prop1: [5.0, 6.0]}) SET n:L1;")
    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert index_info[0][6] == 2

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    # 4/
    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert index_info[0][6] == 2

    node_with_label = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN n LIMIT 1;")
    assert (
        "prop1" not in node_with_label[0][0].properties
    ), "Property should not be visible on node when stored in index"

    node_without_label = execute_and_fetch_all(cursor, "MATCH (n) WHERE NOT n:L1 RETURN n LIMIT 1;")
    assert (
        "prop1" in node_without_label[0][0].properties
    ), "Property should be in property store when node has no index label"

    prop3_node = execute_and_fetch_all(cursor, "MATCH (n:L1 {prop1: [5.0, 6.0]}) RETURN n.prop1;")
    assert prop3_node[0][0] == [5.0, 6.0]

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")


def test_durability_with_vector_index_property_changes(connection):
    # Goal: That setting properties to null and updating vector properties is correctly persisted.
    # 1/ Create vector index, add nodes with vector properties
    # 2/ Set one property to null, update another property
    # 3/ Kill and restart
    # 4/ Validate all changes are restored correctly

    data_directory = tempfile.TemporaryDirectory()

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": "main_durability_vector_property_changes.log",
            "data_directory": data_directory.name,
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    # 1/
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

    # 2/
    execute_and_fetch_all(cursor, "MATCH (n:L1 {prop1: [1.0, 2.0]}) SET n.prop1 = null;")
    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert index_info[0][6] == 2

    execute_and_fetch_all(cursor, "MATCH (n:L1 {prop1: [3.0, 4.0]}) SET n.prop1 = [7.0, 8.0];")

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    # 4/
    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert index_info[0][6] == 2

    nodes = execute_and_fetch_all(cursor, "MATCH (n:L1) WHERE n.prop1 IS NOT NULL RETURN n LIMIT 1;")
    assert "prop1" not in nodes[0][0].properties, "Property should not be visible on node when stored in index"

    null_prop = execute_and_fetch_all(cursor, "MATCH (n:L1) WHERE n.prop1 IS NULL RETURN count(*) AS cnt;")
    assert null_prop[0][0] == 1

    updated_prop = execute_and_fetch_all(cursor, "MATCH (n:L1 {prop1: [7.0, 8.0]}) RETURN n.prop1;")
    assert updated_prop[0][0] == [7.0, 8.0]

    original_prop = execute_and_fetch_all(cursor, "MATCH (n:L1 {prop1: [5.0, 6.0]}) RETURN n.prop1;")
    assert original_prop[0][0] == [5.0, 6.0]

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")


def test_durability_with_two_vector_indices(connection):
    # Goal: That two vector indices on different labels/properties are correctly restored.
    # 1/ Create two vector indices on different properties
    # 2/ Add nodes to both indices
    # 3/ Kill and restart
    # 4/ Validate both indices are restored correctly

    data_directory = tempfile.TemporaryDirectory()

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": "main_durability_two_vector_indices.log",
            "data_directory": data_directory.name,
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    # 1/
    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX test_index ON :L1(prop1) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )
    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX test_index2 ON :L2(prop2) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )

    # 2/
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

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    # 4/
    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    index_info = sorted(index_info, key=lambda x: x[2])
    assert len(index_info) == 2
    assert index_info[0][6] == 2
    assert index_info[1][6] == 2

    node_l1 = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN n LIMIT 1;")
    assert "prop1" not in node_l1[0][0].properties, "Property should not be visible on node when stored in index"

    node_l2 = execute_and_fetch_all(cursor, "MATCH (n:L2) RETURN n LIMIT 1;")
    assert "prop2" not in node_l2[0][0].properties, "Property should not be visible on node when stored in index"

    prop1 = execute_and_fetch_all(cursor, "MATCH (n:L1) RETURN n.prop1 LIMIT 1;")
    assert prop1[0][0] in [[1.0, 2.0], [3.0, 4.0]]

    prop2 = execute_and_fetch_all(cursor, "MATCH (n:L2) RETURN n.prop2 LIMIT 1;")
    assert prop2[0][0] in [[5.0, 6.0], [7.0, 8.0]]

    search1 = execute_and_fetch_all(cursor, "CALL vector_search.search('test_index', 1, [1.0, 2.0]) YIELD * RETURN *;")
    assert len(search1) == 1

    search2 = execute_and_fetch_all(cursor, "CALL vector_search.search('test_index2', 1, [5.0, 6.0]) YIELD * RETURN *;")
    assert len(search2) == 1

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")


def test_durability_with_two_vector_indices_remove_one_label(connection):
    # Goal: That removing one label from node with two labels keeps property in remaining index, not on node.
    # 1/ Create two vector indices on same property but different labels
    # 2/ Create node with both labels
    # 3/ Remove one label
    # 4/ Kill and restart
    # 5/ Validate property is still in remaining index, not on node

    data_directory = tempfile.TemporaryDirectory()

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": "main_durability_two_indices_remove_one_label.log",
            "data_directory": data_directory.name,
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    # 1/
    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX test_index ON :L1(prop1) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )
    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX test_index2 ON :L2(prop1) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )

    # 2/
    execute_and_fetch_all(cursor, "CREATE (n:L1:L2 {prop1: [1.0, 2.0]});")

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    index_info = sorted(index_info, key=lambda x: x[2])
    assert index_info[0][6] == 1
    assert index_info[1][6] == 1

    # 3/
    execute_and_fetch_all(cursor, "MATCH (n:L1:L2 {prop1: [1.0, 2.0]}) REMOVE n:L1;")

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert index_info[0][6] == 0 and index_info[1][6] == 1 or index_info[0][6] == 1 and index_info[1][6] == 0

    # 4/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    # 5/
    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert index_info[0][6] == 0 and index_info[1][6] == 1 or index_info[0][6] == 1 and index_info[1][6] == 0

    node = execute_and_fetch_all(cursor, "MATCH (n:L2) RETURN n LIMIT 1;")
    assert "prop1" not in node[0][0].properties, "Property should not be visible on node when stored in index"

    prop = execute_and_fetch_all(cursor, "MATCH (n:L2) RETURN n.prop1;")
    assert prop[0][0] == [1.0, 2.0]

    search = execute_and_fetch_all(cursor, "CALL vector_search.search('test_index2', 1, [1.0, 2.0]) YIELD * RETURN *;")
    assert len(search) == 1
    assert search[0][0] == 0.0

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")


def test_durability_with_two_vector_indices_remove_both_labels(connection):
    # Goal: That removing both labels from node transfers property to property store, not in any index.
    # 1/ Create two vector indices on same property but different labels
    # 2/ Create node with both labels
    # 3/ Remove both labels
    # 4/ Kill and restart
    # 5/ Validate property is in property store, not in any index

    data_directory = tempfile.TemporaryDirectory()

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": "main_durability_two_indices_remove_both_labels.log",
            "data_directory": data_directory.name,
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    # 1/
    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX test_index ON :L1(prop1) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )
    execute_and_fetch_all(
        cursor, 'CREATE VECTOR INDEX test_index2 ON :L2(prop1) WITH CONFIG {"dimension": 2, "capacity": 10};'
    )

    # 2/
    execute_and_fetch_all(cursor, "CREATE (n:L1:L2 {prop1: [1.0, 2.0]});")

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert index_info[0][6] == 1 and index_info[1][6] == 1 or index_info[0][6] == 1 and index_info[1][6] == 1

    # 3/
    execute_and_fetch_all(cursor, "MATCH (n:L1:L2 {prop1: [1.0, 2.0]}) REMOVE n:L1, n:L2;")

    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    index_info = sorted(index_info, key=lambda x: x[2])
    assert index_info[0][6] == 0
    assert index_info[1][6] == 0

    # 4/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    # 5/
    index_info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    index_info = sorted(index_info, key=lambda x: x[2])
    assert len(index_info) == 2
    assert index_info[0][6] == 0
    assert index_info[1][6] == 0

    node = execute_and_fetch_all(cursor, "MATCH (n {prop1: [1.0, 2.0]}) WHERE NOT n:L1 AND NOT n:L2 RETURN n LIMIT 1;")
    assert "prop1" in node[0][0].properties, "Property should be in property store when node has no index labels"

    prop_check = execute_and_fetch_all(cursor, "MATCH (n {prop1: [1.0, 2.0]}) RETURN n.prop1;")
    assert prop_check[0][0] == [1.0, 2.0]

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
