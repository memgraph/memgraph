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
from functools import partial

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all, get_logs_path
from mg_utils import mg_sleep_and_assert_collection

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

BOLT_PORTS = {"main": 7687, "replica_1": 7688, "replica_2": 7689}
REPLICATION_PORTS = {"replica_1": 10001, "replica_2": 10002}
LOG_DIR = "replicate_descriptions"

ALL_SET_QUERIES = [
    'SET DESCRIPTION ON LABEL :Person "A person node";',
    'SET DESCRIPTION ON EDGE TYPE :KNOWS "Knows relationship";',
    'SET DESCRIPTION ON LABEL PROPERTY :Person(age) "Age of person";',
    'SET DESCRIPTION ON EDGE TYPE PROPERTY :KNOWS(since) "When they met";',
    'SET DESCRIPTION ON DATABASE memgraph "Test database";',
    'SET DESCRIPTION ON PROPERTY age "Age in years";',
    'SET DESCRIPTION ON EDGE TYPE (:Person)-[:KNOWS]->(:Person) "Person knows person";',
    'SET DESCRIPTION ON EDGE TYPE PROPERTY (:Person)-[:KNOWS]->(:Person)(since) "Year they met (pattern)";',
]

ALL_DELETE_QUERIES = [
    "DELETE DESCRIPTION ON LABEL :Person;",
    "DELETE DESCRIPTION ON EDGE TYPE :KNOWS;",
    "DELETE DESCRIPTION ON LABEL PROPERTY :Person(age);",
    "DELETE DESCRIPTION ON EDGE TYPE PROPERTY :KNOWS(since);",
    "DELETE DESCRIPTION ON DATABASE memgraph;",
    "DELETE DESCRIPTION ON PROPERTY age;",
    "DELETE DESCRIPTION ON EDGE TYPE (:Person)-[:KNOWS]->(:Person);",
    "DELETE DESCRIPTION ON EDGE TYPE PROPERTY (:Person)-[:KNOWS]->(:Person)(since);",
]

NUM_DESCRIPTION_TYPES = len(ALL_SET_QUERIES)


@pytest.fixture
def test_name(request):
    return request.node.name


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


def show_replicas_func(cursor):
    return partial(execute_and_fetch_all, cursor, "SHOW REPLICAS;")


def get_all_descriptions(cursor):
    results = execute_and_fetch_all(cursor, "SHOW DESCRIPTIONS;")
    return sorted(results, key=lambda row: tuple(str(col) for col in row))


def make_instances(test_name):
    return {
        "replica_1": {
            "args": ["--bolt-port", f"{BOLT_PORTS['replica_1']}", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(LOG_DIR, test_name)}/replica1.log",
            "setup_queries": [
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};",
            ],
        },
        "replica_2": {
            "args": ["--bolt-port", f"{BOLT_PORTS['replica_2']}", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(LOG_DIR, test_name)}/replica2.log",
            "setup_queries": [
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_2']};",
            ],
        },
        "main": {
            "args": ["--bolt-port", f"{BOLT_PORTS['main']}", "--log-level=TRACE"],
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


def test_replicate_set_all_types(connection, test_name):
    """All description types are replicated to REPLICAs."""
    instances = make_instances(test_name)
    interactive_mg_runner.start_all(instances)
    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    def get_replica_cursor(name):
        return connection(BOLT_PORTS[name], "replica").cursor()

    for query in ALL_SET_QUERIES:
        execute_and_fetch_all(main_cursor, query)

    # Each SET is a transaction with 2 deltas (start + end), so ts = NUM * 2
    wait_for_replication_change(main_cursor, NUM_DESCRIPTION_TYPES * 2)

    main_descriptions = get_all_descriptions(main_cursor)
    assert len(main_descriptions) == NUM_DESCRIPTION_TYPES

    for replica_name in ["replica_1", "replica_2"]:
        replica_descriptions = get_all_descriptions(get_replica_cursor(replica_name))
        assert replica_descriptions == main_descriptions


def test_replicate_delete_all_types(connection, test_name):
    """Deleting all description types is replicated to REPLICAs."""
    instances = make_instances(test_name)
    interactive_mg_runner.start_all(instances)
    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    def get_replica_cursor(name):
        return connection(BOLT_PORTS[name], "replica").cursor()

    for query in ALL_SET_QUERIES:
        execute_and_fetch_all(main_cursor, query)

    set_ts = NUM_DESCRIPTION_TYPES * 2
    wait_for_replication_change(main_cursor, set_ts)

    for query in ALL_DELETE_QUERIES:
        execute_and_fetch_all(main_cursor, query)

    delete_ts = set_ts + NUM_DESCRIPTION_TYPES * 2
    wait_for_replication_change(main_cursor, delete_ts)

    assert get_all_descriptions(main_cursor) == []

    for replica_name in ["replica_1", "replica_2"]:
        assert get_all_descriptions(get_replica_cursor(replica_name)) == []


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
