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

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all, get_data_path, get_logs_path

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))
interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.BUILD_DIR, "query_modules")
)

FILE = "durability_with_vector_edge_index"


@pytest.fixture
def test_name(request):
    return request.node.name


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


def test_durability_with_vector_edge_index_basic(connection, test_name):
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


def test_durability_with_vector_edge_index_property_changes(connection, test_name):
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


def test_durability_with_vector_edge_index_snapshot_basic(connection, test_name):
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


def test_durability_with_vector_edge_index_snapshot_property_changes(connection, test_name):
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


def test_durability_with_vector_edge_index_snapshot_and_wal(connection, test_name):
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


def test_durability_with_vector_edge_index_drop_after_snapshot(connection, test_name):
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


EDGE_INDEX = 'CREATE VECTOR EDGE INDEX {name} ON {types}(emb) WITH CONFIG {{"dimension": 2, "capacity": 10}};'


@pytest.mark.parametrize(
    "queries,snapshot_after,rolled_back_query,expected_indexes,expected_embedding",
    [
        pytest.param(
            [
                "CREATE ()-[:REL {emb: [1.0, 2.0]}]->();",
                EDGE_INDEX.format(name="idx", types=":REL"),
            ],
            None,
            None,
            {"idx": 1},
            [1.0, 2.0],
            id="edge_before_index",
        ),
        pytest.param(
            [
                EDGE_INDEX.format(name="idx", types=":REL"),
                "CREATE ()-[:REL {emb: [1.0, 2.0]}]->();",
                "MATCH ()-[r:REL]->() SET r.emb = [3.0, 4.0];",
                "MATCH ()-[r:REL]->() SET r.emb = [5.0, 6.0];",
            ],
            None,
            None,
            {"idx": 1},
            [5.0, 6.0],
            id="member_vector_updated_twice",
        ),
        pytest.param(
            [
                EDGE_INDEX.format(name="idx", types=":REL"),
                "CREATE ()-[:REL {emb: [1.0, 2.0]}]->();",
                "MATCH ()-[r:REL]->() SET r.emb = [];",
            ],
            None,
            None,
            {"idx": 0},
            [],
            id="member_set_to_empty_list",
        ),
        pytest.param(
            [
                EDGE_INDEX.format(name="idx", types=":REL"),
                "CREATE ()-[:REL {emb: [1.0, 2.0]}]->();",
                "MATCH ()-[r:REL]->() SET r.emb = [];",
            ],
            [],
            None,
            {"idx": 0},
            [],
            id="member_set_to_empty_list_snapshot",
        ),
        pytest.param(
            [
                EDGE_INDEX.format(name="idx", types=":REL"),
                "CREATE ()-[:REL {emb: []}]->();",
            ],
            None,
            "MATCH ()-[r:REL]->() SET r.emb = [1.0, 2.0];",
            {"idx": 0},
            [],
            id="rollback_overwrite_of_empty_list",
        ),
        pytest.param(
            [
                "CREATE ()-[:REL {emb: []}]->();",
                EDGE_INDEX.format(name="idx", types=":REL"),
            ],
            None,
            None,
            {"idx": 0},
            [],
            id="index_created_over_empty_list",
        ),
        pytest.param(
            [
                "CREATE ()-[:REL {emb: []}]->();",
                EDGE_INDEX.format(name="idx", types=":REL"),
                "DROP VECTOR INDEX idx;",
            ],
            None,
            None,
            {},
            [],
            id="index_created_over_empty_list_then_dropped",
        ),
        pytest.param(
            [
                "CREATE ()-[:REL {emb: []}]->();",
                EDGE_INDEX.format(name="idx", types=":REL"),
            ],
            [
                "DROP VECTOR INDEX idx;",
            ],
            None,
            {},
            [],
            id="index_created_over_empty_list_then_dropped_snapshot",
        ),
        pytest.param(
            [
                EDGE_INDEX.format(name="idx", types=":REL"),
                "CREATE ()-[:REL {emb: [7.0, 7.0]}]->();",
                "DROP VECTOR INDEX idx;",
            ],
            None,
            None,
            {},
            [7.0, 7.0],
            id="index_dropped_restores_plain_list",
        ),
        pytest.param(
            [
                EDGE_INDEX.format(name="idx_old", types=":REL"),
                "CREATE ()-[:REL {emb: [1.0, 2.0]}]->();",
                "DROP VECTOR INDEX idx_old;",
                EDGE_INDEX.format(name="idx_new", types=":REL"),
            ],
            None,
            None,
            {"idx_new": 1},
            [1.0, 2.0],
            id="drop_then_recreate_on_same_property",
        ),
        pytest.param(
            [
                EDGE_INDEX.format(name="idx_single", types=":REL"),
                EDGE_INDEX.format(name="idx_any", types=":REL|OTHER"),
                "CREATE ()-[:REL {emb: [1.0, 2.0]}]->();",
                "DROP VECTOR INDEX idx_single;",
            ],
            None,
            None,
            {"idx_any": 1},
            [1.0, 2.0],
            id="two_indexes_drop_one",
        ),
        pytest.param(
            [
                EDGE_INDEX.format(name="idx_a", types=":REL"),
                "CREATE ()-[:REL {emb: [1.0, 2.0]}]->();",
                EDGE_INDEX.format(name="idx_b", types=":REL|OTHER"),
                "DROP VECTOR INDEX idx_a;",
                "MATCH ()-[r:REL]->() SET r.emb = [3.0, 4.0];",
            ],
            None,
            None,
            {"idx_b": 1},
            [3.0, 4.0],
            id="second_index_backfills_then_first_dropped",
        ),
        pytest.param(
            [
                EDGE_INDEX.format(name="idx_old", types=":REL"),
                "CREATE ()-[:REL {emb: [1.0, 2.0]}]->();",
            ],
            [
                "DROP VECTOR INDEX idx_old;",
                EDGE_INDEX.format(name="idx_new", types=":REL"),
            ],
            None,
            {"idx_new": 1},
            [1.0, 2.0],
            id="snapshot_then_drop_and_recreate",
        ),
        pytest.param(
            [
                EDGE_INDEX.format(name="idx_a", types=":REL"),
                "CREATE ()-[:REL {emb: [1.0, 2.0]}]->();",
            ],
            [
                EDGE_INDEX.format(name="idx_b", types=":REL|OTHER"),
                "MATCH ()-[r:REL]->() SET r.emb = [9.0, 8.0];",
            ],
            None,
            {"idx_a": 1, "idx_b": 1},
            [9.0, 8.0],
            id="snapshot_then_second_index_and_update",
        ),
        pytest.param(
            [
                EDGE_INDEX.format(name="idx_a", types=":REL"),
                EDGE_INDEX.format(name="idx_b", types=":REL|OTHER"),
                "CREATE ()-[:REL {emb: [1.0, 2.0]}]->();",
            ],
            [
                "DROP VECTOR INDEX idx_a;",
                "DROP VECTOR INDEX idx_b;",
            ],
            None,
            {},
            [1.0, 2.0],
            id="snapshot_then_drop_both",
        ),
    ],
)
def test_durability_vector_edge_index_membership_after_replay(
    connection, test_name, queries, snapshot_after, rolled_back_query, expected_indexes, expected_embedding
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

    if rolled_back_query is not None:
        # Explicit transactions are Bolt messages, not Cypher, so roll back through a non-autocommit connection.
        tx_connection = connect(host="localhost", port=7687)
        tx_connection.autocommit = False
        execute_and_fetch_all(tx_connection.cursor(), rolled_back_query)
        tx_connection.rollback()
        tx_connection.close()

    if snapshot_after is not None:
        execute_and_fetch_all(cursor, "CREATE SNAPSHOT;")
        for query in snapshot_after:
            execute_and_fetch_all(cursor, query)

    index_info = get_vector_index_info(cursor)
    assert len(index_info) == len(expected_indexes)
    for row in index_info:
        assert row[0] in expected_indexes, f"Unexpected index before restart: {row[0]}"
        assert row[6] == expected_indexes[row[0]]

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    index_info = get_vector_index_info(cursor)
    assert len(index_info) == len(expected_indexes)
    info_by_name = {row[0]: row for row in index_info}
    for name, size in expected_indexes.items():
        assert name in info_by_name, f"Index missing after restart: {name}"
        assert info_by_name[name][6] == size

    embedding = execute_and_fetch_all(cursor, "MATCH ()-[r:REL]->() RETURN r.emb;")
    assert len(embedding) == 1
    assert embedding[0][0] == expected_embedding

    for name, size in expected_indexes.items():
        if size == 0:
            continue
        search_results = vector_edge_search(cursor, name, 1, expected_embedding)
        assert len(search_results) == 1
        assert search_results[0][0] == 0.0

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
