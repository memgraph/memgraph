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
    return sorted(execute_and_fetch_all(cursor, "SHOW DESCRIPTIONS;"))


def make_instances(test_name):
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


def test_description_replication(connection, test_name):
    # Goal: All description types are replicated to REPLICAs.

    instances = make_instances(test_name)

    interactive_mg_runner.start_all(instances)
    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    def get_replica_cursor(name):
        return connection(BOLT_PORTS[name], "replica").cursor()

    execute_and_fetch_all(main_cursor, 'SET DESCRIPTION ON LABEL :Person "A person node";')
    execute_and_fetch_all(main_cursor, 'SET DESCRIPTION ON EDGE TYPE :KNOWS "Knows relationship";')
    execute_and_fetch_all(main_cursor, 'SET DESCRIPTION ON PROPERTY :Person(age) "Age of person";')
    execute_and_fetch_all(main_cursor, 'SET DESCRIPTION ON DATABASE memgraph "Test database";')
    wait_for_replication_change(main_cursor, 8)

    expected_descriptions = sorted(
        [
            ("DATABASE", "memgraph", "Test database"),
            ("EDGE_TYPE", "KNOWS", "Knows relationship"),
            ("LABEL", "Person", "A person node"),
            ("PROPERTY", "Person(age)", "Age of person"),
        ]
    )

    replica_1_descriptions = get_all_descriptions(get_replica_cursor("replica_1"))
    assert replica_1_descriptions == expected_descriptions

    replica_2_descriptions = get_all_descriptions(get_replica_cursor("replica_2"))
    assert replica_2_descriptions == expected_descriptions

    execute_and_fetch_all(main_cursor, "DELETE DESCRIPTION ON LABEL :Person;")
    wait_for_replication_change(main_cursor, 10)

    expected_after_delete = sorted(
        [
            ("DATABASE", "memgraph", "Test database"),
            ("EDGE_TYPE", "KNOWS", "Knows relationship"),
            ("PROPERTY", "Person(age)", "Age of person"),
        ]
    )

    replica_1_descriptions = get_all_descriptions(get_replica_cursor("replica_1"))
    assert replica_1_descriptions == expected_after_delete

    replica_2_descriptions = get_all_descriptions(get_replica_cursor("replica_2"))
    assert replica_2_descriptions == expected_after_delete

    execute_and_fetch_all(main_cursor, 'SET DESCRIPTION ON EDGE TYPE :KNOWS "Updated knows relationship";')
    wait_for_replication_change(main_cursor, 12)

    expected_after_update = sorted(
        [
            ("DATABASE", "memgraph", "Test database"),
            ("EDGE_TYPE", "KNOWS", "Updated knows relationship"),
            ("PROPERTY", "Person(age)", "Age of person"),
        ]
    )

    replica_1_descriptions = get_all_descriptions(get_replica_cursor("replica_1"))
    assert replica_1_descriptions == expected_after_update

    replica_2_descriptions = get_all_descriptions(get_replica_cursor("replica_2"))
    assert replica_2_descriptions == expected_after_update


def test_multi_label_description_replication(connection, test_name):
    # Goal: Multi-label descriptions are replicated correctly.
    instances = make_instances(test_name)

    interactive_mg_runner.start_all(instances)
    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    def get_replica_cursor(name):
        return connection(BOLT_PORTS[name], "replica").cursor()

    execute_and_fetch_all(main_cursor, 'SET DESCRIPTION ON LABEL :Person:Student "A student person";')
    wait_for_replication_change(main_cursor, 2)

    # Check multi-label description arrived at replicas
    for replica_name in ["replica_1", "replica_2"]:
        result = execute_and_fetch_all(get_replica_cursor(replica_name), "SHOW DESCRIPTION ON LABEL :Person:Student;")
        assert result == [("A student person",)]

        # Single label should not match
        result = execute_and_fetch_all(get_replica_cursor(replica_name), "SHOW DESCRIPTION ON LABEL :Person;")
        assert result == []


def test_property_description_replication(connection, test_name):
    # Goal: Label-scoped property descriptions are replicated independently per label.
    instances = make_instances(test_name)

    interactive_mg_runner.start_all(instances)
    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    def get_replica_cursor(name):
        return connection(BOLT_PORTS[name], "replica").cursor()

    execute_and_fetch_all(main_cursor, 'SET DESCRIPTION ON PROPERTY :Person(age) "Age of person";')
    execute_and_fetch_all(main_cursor, 'SET DESCRIPTION ON PROPERTY :Student(age) "Age of student";')
    wait_for_replication_change(main_cursor, 4)

    expected = sorted(
        [
            ("PROPERTY", "Person(age)", "Age of person"),
            ("PROPERTY", "Student(age)", "Age of student"),
        ]
    )

    for replica_name in ["replica_1", "replica_2"]:
        result = get_all_descriptions(get_replica_cursor(replica_name))
        assert result == expected


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
