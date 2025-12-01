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
from functools import partial

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
interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.BUILD_DIR, "query_modules")
)

BOLT_PORTS = {"main": 7687, "replica_1": 7688, "replica_2": 7689}
REPLICATION_PORTS = {"replica_1": 10001, "replica_2": 10002}
LOG_DIR = "replicate_text_index"


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
    return partial(execute_and_fetch_all, cursor, "SHOW REPLICAS;")


def test_text_index_replication(connection, test_name):
    # Goal: Proof that text indices (both node and edge) are replicated to REPLICAs
    # 0/ Setup replication
    # 1/ Create text indices on MAIN (both node and edge)
    # 2/ Validate text indices have arrived at REPLICAs
    # 3/ Create text entries on MAIN (both nodes and edges)
    # 4/ Validate index functionality on REPLICAs is correct
    # 5/ Drop text indices on MAIN
    # 6/ Validate indices have been dropped on REPLICAs

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "replica_1": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_1']}",
                "--log-level=TRACE",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
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
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
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
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": f"{get_logs_path(LOG_DIR, test_name)}/main.log",
            "setup_queries": [
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
                f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';",
            ],
        },
    }

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

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL)
    cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    # Create text indices on both nodes and edges
    execute_and_fetch_all(
        cursor,
        "CREATE TEXT INDEX test_node_index ON :Node(name);",
    )
    execute_and_fetch_all(
        cursor,
        "CREATE TEXT EDGE INDEX test_edge_index ON :RELATES_TO(title);",
    )
    wait_for_replication_change(cursor, 4)

    # 2/
    def get_show_index_info(cursor):
        return execute_and_fetch_all(cursor, f"SHOW INDEX INFO;")

    def get_replica_cursor(name):
        return connection(BOLT_PORTS[name], "replica").cursor()

    expected_result = [
        ("label_text (name: test_node_index)", "Node", ["name"], 0),
        ("edge-type_text (name: test_edge_index)", "RELATES_TO", ["title"], 0),
    ]
    replica_1_info = get_show_index_info(get_replica_cursor("replica_1"))
    assert sorted(replica_1_info) == sorted(expected_result)
    replica_2_info = get_show_index_info(get_replica_cursor("replica_2"))
    assert sorted(replica_2_info) == sorted(expected_result)

    # 3/
    # Create data with both nodes and edges
    execute_and_fetch_all(
        cursor,
        """CREATE (n1:Node {name: 'test1', prop1: 'node value1', prop2: 'node value2'})
           -[:RELATES_TO {title: 'relation title', content: 'edge content for testing'}]->
           (n2:Node {name: 'test2', prop1: 'node value3', prop2: 'node value4'});""",
    )
    wait_for_replication_change(cursor, 6)

    # 4/
    # Test node index replication on replica_1
    search_results = execute_and_fetch_all(
        get_replica_cursor("replica_1"),
        "CALL text_search.search('test_node_index', 'data.name:test1') YIELD node RETURN node.name AS name;",
    )
    assert search_results == [("test1",)]

    # Test edge index replication on replica_1
    search_results = execute_and_fetch_all(
        get_replica_cursor("replica_1"),
        "CALL text_search.search_edges('test_edge_index', 'data.title:relation') YIELD edge RETURN edge.title AS title;",
    )
    assert search_results == [("relation title",)]

    # Test node index replication on replica_2
    search_results = execute_and_fetch_all(
        get_replica_cursor("replica_2"),
        "CALL text_search.search('test_node_index', 'data.name:test2') YIELD node RETURN node.name AS name;",
    )
    assert search_results == [("test2",)]

    # Test edge index replication on replica_2
    search_results = execute_and_fetch_all(
        get_replica_cursor("replica_2"),
        "CALL text_search.search_edges('test_edge_index', 'data.title:relation') YIELD edge RETURN edge.title AS title;",
    )
    assert search_results == [("relation title",)]

    # 5/
    # Drop both text indices
    execute_and_fetch_all(
        cursor,
        "DROP TEXT INDEX test_node_index;",
    )
    execute_and_fetch_all(
        cursor,
        "DROP TEXT INDEX test_edge_index;",
    )
    wait_for_replication_change(cursor, 10)

    # 6/
    expected_result = []
    replica_1_info = get_show_index_info(get_replica_cursor("replica_1"))
    assert replica_1_info == expected_result
    replica_2_info = get_show_index_info(get_replica_cursor("replica_2"))
    assert replica_2_info == expected_result


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
