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
from common import connect, execute_and_fetch_all, get_data_path

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

FILE = "durability_with_descriptions"

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


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


def get_all_descriptions(cursor):
    results = execute_and_fetch_all(cursor, "SHOW DESCRIPTIONS;")
    return sorted(results, key=lambda row: tuple(str(col) for col in row))


def set_all_descriptions(cursor):
    for query in ALL_SET_QUERIES:
        execute_and_fetch_all(cursor, query)


def delete_all_descriptions(cursor):
    for query in ALL_DELETE_QUERIES:
        execute_and_fetch_all(cursor, query)


def test_durability_set_all_types(test_name):
    """All description types survive WAL-based restart."""
    data_directory = get_data_path(FILE, test_name)

    instance_desc = {
        "main": {
            "args": ["--log-level=TRACE", "--data-recovery-on-startup=true"],
            "log_file": "main_durability_set_all.log",
            "data_directory": data_directory,
        },
    }

    interactive_mg_runner.start(instance_desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    set_all_descriptions(cursor)
    descriptions = get_all_descriptions(cursor)
    assert len(descriptions) == NUM_DESCRIPTION_TYPES

    interactive_mg_runner.kill(instance_desc, "main")
    interactive_mg_runner.start(instance_desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    descriptions_after = get_all_descriptions(cursor)
    assert descriptions_after == descriptions


def test_durability_delete_all_types(test_name):
    """Deleting all description types is persisted across WAL-based restart."""
    data_directory = get_data_path(FILE, test_name)

    instance_desc = {
        "main": {
            "args": ["--log-level=TRACE", "--data-recovery-on-startup=true"],
            "log_file": "main_durability_delete_all.log",
            "data_directory": data_directory,
        },
    }

    interactive_mg_runner.start(instance_desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    set_all_descriptions(cursor)
    assert len(get_all_descriptions(cursor)) == NUM_DESCRIPTION_TYPES

    delete_all_descriptions(cursor)
    assert get_all_descriptions(cursor) == []

    interactive_mg_runner.kill(instance_desc, "main")
    interactive_mg_runner.start(instance_desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    assert get_all_descriptions(cursor) == []


def test_durability_snapshot_all_types(test_name):
    """All description types survive snapshot-based recovery."""
    data_directory = get_data_path(FILE, test_name)

    instance_desc = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--storage-snapshot-on-exit=true",
                "--storage-wal-enabled=false",
            ],
            "log_file": "main_durability_snapshot_all.log",
            "data_directory": data_directory,
        },
    }

    interactive_mg_runner.start(instance_desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    set_all_descriptions(cursor)
    descriptions = get_all_descriptions(cursor)
    assert len(descriptions) == NUM_DESCRIPTION_TYPES

    # Graceful shutdown triggers snapshot-on-exit.
    interactive_mg_runner.stop(instance_desc, "main")
    interactive_mg_runner.start(instance_desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    descriptions_after = get_all_descriptions(cursor)
    assert descriptions_after == descriptions


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
