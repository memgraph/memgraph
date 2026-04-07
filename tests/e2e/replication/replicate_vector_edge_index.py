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
LOG_DIR = "replicate_vector_edge"


@pytest.fixture
def test_name(request):
    return request.node.name


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


def show_replicas_func(cursor):
    def func():
        return execute_and_fetch_all(cursor, "SHOW REPLICAS;")

    return func


def create_instances_description(test_name):
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
    return execute_and_fetch_all(cursor, "SHOW INDEX INFO;")


def get_replica_cursor(connection, name):
    return connection(BOLT_PORTS[name], "replica").cursor()


def test_vector_edge_index_replication(connection, test_name):
    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = create_instances_description(test_name)

    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL)
    cursor = connection(BOLT_PORTS["main"], "main").cursor()

    execute_and_fetch_all(
        cursor,
        "CREATE VECTOR EDGE INDEX test_index ON :REL(embedding) WITH CONFIG {'dimension': 2, 'capacity': 10};",
    )
    wait_for_replication_change(cursor, 2)

    expected_result = [("edge-type+property_vector", "REL", "embedding", 0)]
    assert get_show_index_info(get_replica_cursor(connection, "replica_1")) == expected_result
    assert get_show_index_info(get_replica_cursor(connection, "replica_2")) == expected_result

    execute_and_fetch_all(cursor, "CREATE ()-[:REL {embedding: [0.5, 1.5]}]->();")
    wait_for_replication_change(cursor, 4)

    expected_result = [("edge-type+property_vector", "REL", "embedding", 1)]
    assert get_show_index_info(get_replica_cursor(connection, "replica_1")) == expected_result
    assert get_show_index_info(get_replica_cursor(connection, "replica_2")) == expected_result

    execute_and_fetch_all(cursor, "DROP VECTOR INDEX test_index;")
    wait_for_replication_change(cursor, 6)

    assert get_show_index_info(get_replica_cursor(connection, "replica_1")) == []
    assert get_show_index_info(get_replica_cursor(connection, "replica_2")) == []


def test_vector_edge_index_replication_property_changes(connection, test_name):
    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = create_instances_description(test_name)

    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL)
    cursor = connection(BOLT_PORTS["main"], "main").cursor()

    execute_and_fetch_all(
        cursor,
        'CREATE VECTOR EDGE INDEX test_index ON :REL(emb) WITH CONFIG {"dimension": 2, "capacity": 10};',
    )
    wait_for_replication_change(cursor, 2)

    execute_and_fetch_all(
        cursor,
        """CREATE ({id: 1})-[:REL {emb: [1.0, 2.0]}]->({id: 2})
           CREATE ({id: 3})-[:REL {emb: [3.0, 4.0]}]->({id: 4})
           CREATE ({id: 5})-[:REL {emb: [5.0, 6.0]}]->({id: 6});""",
    )
    wait_for_replication_change(cursor, 4)

    expected_result = [("edge-type+property_vector", "REL", "emb", 3)]
    assert get_show_index_info(get_replica_cursor(connection, "replica_1")) == expected_result
    assert get_show_index_info(get_replica_cursor(connection, "replica_2")) == expected_result

    execute_and_fetch_all(cursor, "MATCH ()-[r:REL {emb: [1.0, 2.0]}]->() SET r.emb = null;")
    wait_for_replication_change(cursor, 6)

    execute_and_fetch_all(cursor, "MATCH ()-[r:REL {emb: [3.0, 4.0]}]->() SET r.emb = [7.0, 8.0];")
    wait_for_replication_change(cursor, 8)

    expected_result = [("edge-type+property_vector", "REL", "emb", 2)]
    assert get_show_index_info(get_replica_cursor(connection, "replica_1")) == expected_result
    assert get_show_index_info(get_replica_cursor(connection, "replica_2")) == expected_result

    replica_1_cursor = get_replica_cursor(connection, "replica_1")
    replica_2_cursor = get_replica_cursor(connection, "replica_2")

    null_count = execute_and_fetch_all(
        replica_1_cursor, "MATCH ()-[r:REL]->() WHERE r.emb IS NULL RETURN count(*) AS cnt;"
    )
    assert null_count[0][0] == 1

    updated = execute_and_fetch_all(replica_1_cursor, "MATCH ()-[r:REL {emb: [7.0, 8.0]}]->() RETURN r.emb;")
    assert updated[0][0] == [7.0, 8.0]

    property_size = execute_and_fetch_all(
        replica_1_cursor, "MATCH ()-[r:REL]->() WHERE r.emb IS NOT NULL RETURN propertySize(r, 'emb') LIMIT 1;"
    )
    assert property_size[0][0] == 11

    original = execute_and_fetch_all(replica_1_cursor, "MATCH ()-[r:REL {emb: [5.0, 6.0]}]->() RETURN r.emb;")
    assert original[0][0] == [5.0, 6.0]

    updated2 = execute_and_fetch_all(replica_2_cursor, "MATCH ()-[r:REL {emb: [7.0, 8.0]}]->() RETURN r.emb;")
    assert updated2[0][0] == [7.0, 8.0]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
