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


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


def get_vector_index_info(cursor):
    return execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")


def vector_edge_search(cursor, index_name, limit, query_vector):
    return execute_and_fetch_all(
        cursor,
        f"CALL vector_search.search_edges('{index_name}', {limit}, {query_vector}) YIELD * RETURN *;",
    )


def test_durability_with_vector_edge_index_basic(connection):
    data_directory = tempfile.TemporaryDirectory()

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": "main_durability_with_vector_edge_index_basic.log",
            "data_directory": data_directory.name,
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    execute_and_fetch_all(
        cursor,
        'CREATE VECTOR EDGE INDEX test_edge_index ON :REL(emb) WITH CONFIG {"dimension": 2, "capacity": 10};',
    )

    execute_and_fetch_all(
        cursor,
        """CREATE ({id: 1})-[:REL {emb: [1.0, 2.0]}]->({id: 2})
           CREATE ({id: 3})-[:REL {emb: [3.0, 4.0]}]->({id: 4})
           CREATE ({id: 5})-[:REL {emb: [5.0, 6.0]}]->({id: 6});""",
    )

    index_info = get_vector_index_info(cursor)
    assert len(index_info) == 1
    assert index_info[0][6] == 3

    property_size = execute_and_fetch_all(cursor, "MATCH ()-[r:REL]->() RETURN propertySize(r, 'emb') LIMIT 1;")
    assert property_size[0][0] == 11

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    index_info = get_vector_index_info(cursor)
    assert len(index_info) == 1
    assert index_info[0][6] == 3

    edges = execute_and_fetch_all(cursor, "MATCH ()-[r:REL]->() RETURN r.emb ORDER BY r.emb[0];")
    assert len(edges) == 3
    assert edges[0][0] == [1.0, 2.0]
    assert edges[1][0] == [3.0, 4.0]
    assert edges[2][0] == [5.0, 6.0]

    property_size = execute_and_fetch_all(cursor, "MATCH ()-[r:REL]->() RETURN propertySize(r, 'emb') LIMIT 1;")
    assert property_size[0][0] == 11

    search_results = vector_edge_search(cursor, "test_edge_index", 1, [1.0, 2.0])
    assert len(search_results) == 1
    assert search_results[0][0] == 0.0


def test_durability_with_vector_edge_index_property_changes(connection):
    data_directory = tempfile.TemporaryDirectory()

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": "main_durability_with_vector_edge_index_prop_changes.log",
            "data_directory": data_directory.name,
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    execute_and_fetch_all(
        cursor,
        'CREATE VECTOR EDGE INDEX test_edge_index ON :REL(emb) WITH CONFIG {"dimension": 2, "capacity": 10};',
    )

    execute_and_fetch_all(
        cursor,
        """CREATE ({id: 1})-[:REL {emb: [1.0, 2.0]}]->({id: 2})
           CREATE ({id: 3})-[:REL {emb: [3.0, 4.0]}]->({id: 4})
           CREATE ({id: 5})-[:REL {emb: [5.0, 6.0]}]->({id: 6});""",
    )

    execute_and_fetch_all(cursor, "MATCH ()-[r:REL {emb: [1.0, 2.0]}]->() SET r.emb = null;")
    execute_and_fetch_all(cursor, "MATCH ()-[r:REL {emb: [3.0, 4.0]}]->() SET r.emb = [7.0, 8.0];")

    index_info = get_vector_index_info(cursor)
    assert index_info[0][6] == 2

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    index_info = get_vector_index_info(cursor)
    assert len(index_info) == 1
    assert index_info[0][6] == 2

    property_size = execute_and_fetch_all(
        cursor, "MATCH ()-[r:REL]->() WHERE r.emb IS NOT NULL RETURN propertySize(r, 'emb') LIMIT 1;"
    )
    assert property_size[0][0] == 11

    updated = execute_and_fetch_all(cursor, "MATCH ()-[r:REL {emb: [7.0, 8.0]}]->() RETURN r.emb;")
    assert len(updated) == 1
    assert updated[0][0] == [7.0, 8.0]

    original = execute_and_fetch_all(cursor, "MATCH ()-[r:REL {emb: [5.0, 6.0]}]->() RETURN r.emb;")
    assert len(original) == 1
    assert original[0][0] == [5.0, 6.0]


def test_durability_with_vector_edge_index_snapshot_basic(connection):
    data_directory = tempfile.TemporaryDirectory()

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": "main_durability_vector_edge_snapshot_basic.log",
            "data_directory": data_directory.name,
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    execute_and_fetch_all(
        cursor,
        'CREATE VECTOR EDGE INDEX test_edge_index ON :REL(emb) WITH CONFIG {"dimension": 2, "capacity": 10};',
    )

    execute_and_fetch_all(
        cursor,
        """CREATE ({id: 1})-[:REL {emb: [1.0, 2.0]}]->({id: 2})
           CREATE ({id: 3})-[:REL {emb: [3.0, 4.0]}]->({id: 4})
           CREATE ({id: 5})-[:REL {emb: [5.0, 6.0]}]->({id: 6});""",
    )

    index_info = get_vector_index_info(cursor)
    assert len(index_info) == 1
    assert index_info[0][6] == 3

    property_size = execute_and_fetch_all(cursor, "MATCH ()-[r:REL]->() RETURN propertySize(r, 'emb') LIMIT 1;")
    assert property_size[0][0] == 11

    execute_and_fetch_all(cursor, "CREATE SNAPSHOT;")

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    index_info = get_vector_index_info(cursor)
    assert len(index_info) == 1
    assert index_info[0][6] == 3

    edges = execute_and_fetch_all(cursor, "MATCH ()-[r:REL]->() RETURN r.emb ORDER BY r.emb[0];")
    assert len(edges) == 3
    assert edges[0][0] == [1.0, 2.0]
    assert edges[1][0] == [3.0, 4.0]
    assert edges[2][0] == [5.0, 6.0]

    property_size = execute_and_fetch_all(cursor, "MATCH ()-[r:REL]->() RETURN propertySize(r, 'emb') LIMIT 1;")
    assert property_size[0][0] == 11

    search_results = vector_edge_search(cursor, "test_edge_index", 1, [1.0, 2.0])
    assert len(search_results) == 1
    assert search_results[0][0] == 0.0


def test_durability_with_vector_edge_index_snapshot_property_changes(connection):
    data_directory = tempfile.TemporaryDirectory()

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": "main_durability_vector_edge_snapshot_prop_changes.log",
            "data_directory": data_directory.name,
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    execute_and_fetch_all(
        cursor,
        'CREATE VECTOR EDGE INDEX test_edge_index ON :REL(emb) WITH CONFIG {"dimension": 2, "capacity": 10};',
    )

    execute_and_fetch_all(
        cursor,
        """CREATE ({id: 1})-[:REL {emb: [1.0, 2.0]}]->({id: 2})
           CREATE ({id: 3})-[:REL {emb: [3.0, 4.0]}]->({id: 4})
           CREATE ({id: 5})-[:REL {emb: [5.0, 6.0]}]->({id: 6});""",
    )

    execute_and_fetch_all(cursor, "MATCH ()-[r:REL {emb: [1.0, 2.0]}]->() SET r.emb = null;")
    execute_and_fetch_all(cursor, "MATCH ()-[r:REL {emb: [3.0, 4.0]}]->() SET r.emb = [7.0, 8.0];")

    index_info = get_vector_index_info(cursor)
    assert index_info[0][6] == 2

    execute_and_fetch_all(cursor, "CREATE SNAPSHOT;")

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    index_info = get_vector_index_info(cursor)
    assert len(index_info) == 1
    assert index_info[0][6] == 2

    property_size = execute_and_fetch_all(
        cursor, "MATCH ()-[r:REL]->() WHERE r.emb IS NOT NULL RETURN propertySize(r, 'emb') LIMIT 1;"
    )
    assert property_size[0][0] == 11

    updated = execute_and_fetch_all(cursor, "MATCH ()-[r:REL {emb: [7.0, 8.0]}]->() RETURN r.emb;")
    assert len(updated) == 1
    assert updated[0][0] == [7.0, 8.0]

    original = execute_and_fetch_all(cursor, "MATCH ()-[r:REL {emb: [5.0, 6.0]}]->() RETURN r.emb;")
    assert len(original) == 1
    assert original[0][0] == [5.0, 6.0]


def test_durability_with_vector_edge_index_snapshot_and_wal(connection):
    data_directory = tempfile.TemporaryDirectory()

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": "main_durability_vector_edge_snapshot_and_wal.log",
            "data_directory": data_directory.name,
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    execute_and_fetch_all(
        cursor,
        'CREATE VECTOR EDGE INDEX test_edge_index ON :REL(emb) WITH CONFIG {"dimension": 2, "capacity": 10};',
    )

    execute_and_fetch_all(
        cursor,
        """CREATE ({id: 1})-[:REL {emb: [1.0, 2.0]}]->({id: 2})
           CREATE ({id: 3})-[:REL {emb: [3.0, 4.0]}]->({id: 4});""",
    )

    index_info = get_vector_index_info(cursor)
    assert index_info[0][6] == 2

    execute_and_fetch_all(cursor, "CREATE SNAPSHOT;")

    execute_and_fetch_all(
        cursor,
        "CREATE ({id: 5})-[:REL {emb: [5.0, 6.0]}]->({id: 6});",
    )

    execute_and_fetch_all(cursor, "MATCH ()-[r:REL {emb: [1.0, 2.0]}]->() SET r.emb = [9.0, 10.0];")

    index_info = get_vector_index_info(cursor)
    assert index_info[0][6] == 3

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    index_info = get_vector_index_info(cursor)
    assert len(index_info) == 1
    assert index_info[0][6] == 3

    edges = execute_and_fetch_all(cursor, "MATCH ()-[r:REL]->() RETURN r.emb ORDER BY r.emb[0];")
    assert len(edges) == 3
    assert edges[0][0] == [3.0, 4.0]
    assert edges[1][0] == [5.0, 6.0]
    assert edges[2][0] == [9.0, 10.0]

    property_size = execute_and_fetch_all(cursor, "MATCH ()-[r:REL]->() RETURN propertySize(r, 'emb') LIMIT 1;")
    assert property_size[0][0] == 11

    search_results = vector_edge_search(cursor, "test_edge_index", 1, [9.0, 10.0])
    assert len(search_results) == 1
    assert search_results[0][0] == 0.0


def test_durability_with_vector_edge_index_drop_after_snapshot(connection):
    data_directory = tempfile.TemporaryDirectory()

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": "main_durability_vector_edge_drop_after_snapshot.log",
            "data_directory": data_directory.name,
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    execute_and_fetch_all(
        cursor,
        'CREATE VECTOR EDGE INDEX test_edge_index ON :REL(emb) WITH CONFIG {"dimension": 2, "capacity": 10};',
    )

    execute_and_fetch_all(
        cursor,
        """CREATE ({id: 1})-[:REL {emb: [1.0, 2.0]}]->({id: 2})
           CREATE ({id: 3})-[:REL {emb: [3.0, 4.0]}]->({id: 4})
           CREATE ({id: 5})-[:REL {emb: [5.0, 6.0]}]->({id: 6});""",
    )

    index_info = get_vector_index_info(cursor)
    assert index_info[0][6] == 3

    property_size = execute_and_fetch_all(cursor, "MATCH ()-[r:REL]->() RETURN propertySize(r, 'emb') LIMIT 1;")
    assert property_size[0][0] == 11

    execute_and_fetch_all(cursor, "CREATE SNAPSHOT;")

    execute_and_fetch_all(cursor, "DROP VECTOR INDEX test_edge_index;")

    index_info = get_vector_index_info(cursor)
    assert len(index_info) == 0

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    index_info = get_vector_index_info(cursor)
    assert len(index_info) == 0

    edges = execute_and_fetch_all(cursor, "MATCH ()-[r:REL]->() RETURN r.emb ORDER BY r.emb[0];")
    assert len(edges) == 3
    assert edges[0][0] == [1.0, 2.0]
    assert edges[1][0] == [3.0, 4.0]
    assert edges[2][0] == [5.0, 6.0]

    property_sizes = execute_and_fetch_all(cursor, "MATCH ()-[r:REL]->() RETURN propertySize(r, 'emb');")
    for ps in property_sizes:
        assert ps[0] != 11, "Property should no longer be VectorIndexId after index drop"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
