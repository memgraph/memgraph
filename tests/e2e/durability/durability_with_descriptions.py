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


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


def get_all_descriptions(cursor):
    return sorted(execute_and_fetch_all(cursor, "SHOW DESCRIPTIONS;"))


def test_durability_label_description(test_name):
    # Goal: Label descriptions survive restart.
    data_directory = get_data_path(FILE, test_name)

    instance_desc = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
            ],
            "log_file": "main_durability_descriptions.log",
            "data_directory": data_directory,
        },
    }

    interactive_mg_runner.start(instance_desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    execute_and_fetch_all(cursor, 'SET DESCRIPTION ON LABEL :Person "A person node";')
    execute_and_fetch_all(cursor, 'SET DESCRIPTION ON LABEL :Company "A company node";')

    descriptions = get_all_descriptions(cursor)
    assert descriptions == [
        ("LABEL", "Company", "A company node"),
        ("LABEL", "Person", "A person node"),
    ]

    interactive_mg_runner.kill(instance_desc, "main")
    interactive_mg_runner.start(instance_desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    descriptions = get_all_descriptions(cursor)
    assert descriptions == [
        ("LABEL", "Company", "A company node"),
        ("LABEL", "Person", "A person node"),
    ]


def test_durability_edge_type_description(test_name):
    # Goal: Edge type descriptions survive restart.
    data_directory = get_data_path(FILE, test_name)

    instance_desc = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
            ],
            "log_file": "main_durability_edge_type_descriptions.log",
            "data_directory": data_directory,
        },
    }

    interactive_mg_runner.start(instance_desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    execute_and_fetch_all(cursor, 'SET DESCRIPTION ON EDGE TYPE :KNOWS "Knows relationship";')
    execute_and_fetch_all(cursor, 'SET DESCRIPTION ON EDGE TYPE :WORKS_AT "Works at relationship";')

    descriptions = get_all_descriptions(cursor)
    assert descriptions == [
        ("EDGE_TYPE", "KNOWS", "Knows relationship"),
        ("EDGE_TYPE", "WORKS_AT", "Works at relationship"),
    ]

    interactive_mg_runner.kill(instance_desc, "main")
    interactive_mg_runner.start(instance_desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    descriptions = get_all_descriptions(cursor)
    assert descriptions == [
        ("EDGE_TYPE", "KNOWS", "Knows relationship"),
        ("EDGE_TYPE", "WORKS_AT", "Works at relationship"),
    ]


def test_durability_property_description(test_name):
    # Goal: Label-scoped property descriptions survive restart.
    data_directory = get_data_path(FILE, test_name)

    instance_desc = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
            ],
            "log_file": "main_durability_property_descriptions.log",
            "data_directory": data_directory,
        },
    }

    interactive_mg_runner.start(instance_desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    execute_and_fetch_all(cursor, 'SET DESCRIPTION ON PROPERTY :Person(age) "Age of the person";')
    execute_and_fetch_all(cursor, 'SET DESCRIPTION ON PROPERTY :Person(name) "Name of the person";')

    descriptions = get_all_descriptions(cursor)
    assert descriptions == [
        ("PROPERTY", "Person(age)", "Age of the person"),
        ("PROPERTY", "Person(name)", "Name of the person"),
    ]

    interactive_mg_runner.kill(instance_desc, "main")
    interactive_mg_runner.start(instance_desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    descriptions = get_all_descriptions(cursor)
    assert descriptions == [
        ("PROPERTY", "Person(age)", "Age of the person"),
        ("PROPERTY", "Person(name)", "Name of the person"),
    ]


def test_durability_database_description(test_name):
    # Goal: Database description survives restart.
    data_directory = get_data_path(FILE, test_name)

    instance_desc = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
            ],
            "log_file": "main_durability_database_description.log",
            "data_directory": data_directory,
        },
    }

    interactive_mg_runner.start(instance_desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    execute_and_fetch_all(cursor, 'SET DESCRIPTION ON DATABASE memgraph "My graph database";')

    descriptions = get_all_descriptions(cursor)
    assert descriptions == [("DATABASE", "", "My graph database")]

    interactive_mg_runner.kill(instance_desc, "main")
    interactive_mg_runner.start(instance_desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    descriptions = get_all_descriptions(cursor)
    assert descriptions == [("DATABASE", "", "My graph database")]


def test_durability_all_description_types(test_name):
    # Goal: All description types together survive restart.
    data_directory = get_data_path(FILE, test_name)

    instance_desc = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
            ],
            "log_file": "main_durability_all_descriptions.log",
            "data_directory": data_directory,
        },
    }

    interactive_mg_runner.start(instance_desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    execute_and_fetch_all(cursor, 'SET DESCRIPTION ON LABEL :Person "A person node";')
    execute_and_fetch_all(cursor, 'SET DESCRIPTION ON EDGE TYPE :KNOWS "Knows relationship";')
    execute_and_fetch_all(cursor, 'SET DESCRIPTION ON PROPERTY :Person(age) "Age of person";')
    execute_and_fetch_all(cursor, 'SET DESCRIPTION ON DATABASE memgraph "Test database";')

    descriptions = get_all_descriptions(cursor)
    assert len(descriptions) == 4

    interactive_mg_runner.kill(instance_desc, "main")
    interactive_mg_runner.start(instance_desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    descriptions_after = get_all_descriptions(cursor)
    assert descriptions_after == descriptions


def test_durability_delete_description_persisted(test_name):
    # Goal: Deleting a description is persisted across restart.
    data_directory = get_data_path(FILE, test_name)

    instance_desc = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
            ],
            "log_file": "main_durability_delete_description.log",
            "data_directory": data_directory,
        },
    }

    interactive_mg_runner.start(instance_desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    execute_and_fetch_all(cursor, 'SET DESCRIPTION ON LABEL :Person "A person node";')
    execute_and_fetch_all(cursor, 'SET DESCRIPTION ON LABEL :Company "A company node";')
    execute_and_fetch_all(cursor, "DELETE DESCRIPTION ON LABEL :Person;")

    descriptions = get_all_descriptions(cursor)
    assert descriptions == [("LABEL", "Company", "A company node")]

    interactive_mg_runner.kill(instance_desc, "main")
    interactive_mg_runner.start(instance_desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    descriptions = get_all_descriptions(cursor)
    assert descriptions == [("LABEL", "Company", "A company node")]


def test_durability_update_description_persisted(test_name):
    # Goal: Updating a description is persisted across restart.
    data_directory = get_data_path(FILE, test_name)

    instance_desc = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
            ],
            "log_file": "main_durability_update_description.log",
            "data_directory": data_directory,
        },
    }

    interactive_mg_runner.start(instance_desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    execute_and_fetch_all(cursor, 'SET DESCRIPTION ON LABEL :Person "First description";')
    execute_and_fetch_all(cursor, 'SET DESCRIPTION ON LABEL :Person "Updated description";')

    result = execute_and_fetch_all(cursor, "SHOW DESCRIPTION ON LABEL :Person;")
    assert result == [("Updated description",)]

    interactive_mg_runner.kill(instance_desc, "main")
    interactive_mg_runner.start(instance_desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    result = execute_and_fetch_all(cursor, "SHOW DESCRIPTION ON LABEL :Person;")
    assert result == [("Updated description",)]


def test_durability_multi_label_description(test_name):
    # Goal: Multi-label descriptions survive restart.
    data_directory = get_data_path(FILE, test_name)

    instance_desc = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
            ],
            "log_file": "main_durability_multi_label.log",
            "data_directory": data_directory,
        },
    }

    interactive_mg_runner.start(instance_desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    execute_and_fetch_all(cursor, 'SET DESCRIPTION ON LABEL :Person:Student "A student person";')

    result = execute_and_fetch_all(cursor, "SHOW DESCRIPTION ON LABEL :Person:Student;")
    assert result == [("A student person",)]

    # Single label should not match
    result = execute_and_fetch_all(cursor, "SHOW DESCRIPTION ON LABEL :Person;")
    assert result == []

    interactive_mg_runner.kill(instance_desc, "main")
    interactive_mg_runner.start(instance_desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    result = execute_and_fetch_all(cursor, "SHOW DESCRIPTION ON LABEL :Person:Student;")
    assert result == [("A student person",)]

    result = execute_and_fetch_all(cursor, "SHOW DESCRIPTION ON LABEL :Person;")
    assert result == []


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
