#!/usr/bin/python3 -u

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

import argparse
import logging
import os
import shutil
import sys
import tempfile
from pathlib import Path

import mgclient
from memgraph_server_context import memgraph_server

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", ".."))
TESTS_DIR = os.path.join(SCRIPT_DIR, "tests")

TRIGGERS_DIR_NAME = "triggers"
EXPECTED_DUMP_FILE_NAME = "expected_dump.cypher"

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def read_content(file_path):
    with open(file_path, "r") as fin:
        return fin.readlines()


def list_to_string(data):
    ret = "[\n"
    for row in data:
        ret += "    " + row + "\n"
    ret += "]"
    return ret


def get_dump_triggers(connection):
    """Execute DUMP DATABASE and extract trigger statements."""
    cursor = connection.cursor()
    cursor.execute("DUMP DATABASE")
    rows = cursor.fetchall()
    cursor.close()

    # Extract trigger statements (they start with "CREATE TRIGGER")
    trigger_statements = []
    for row in rows:
        statement = row[0]
        if statement.startswith("CREATE TRIGGER"):
            trigger_statements.append(statement)

    # Sort for consistent comparison
    trigger_statements.sort()
    return trigger_statements


def execute_test(memgraph_binary: Path, test_directory, write_expected):
    print("\033[1;36m~~ Executing test {} ~~\033[0m".format(os.path.relpath(test_directory, TESTS_DIR)))

    working_data_directory = tempfile.TemporaryDirectory()
    triggers_source_dir = os.path.join(test_directory, TRIGGERS_DIR_NAME)

    if not os.path.isdir(triggers_source_dir):
        raise Exception("Missing triggers directory in test directory '{}'".format(test_directory))

    # Create the triggers directory structure in the working directory
    triggers_dest_dir = os.path.join(working_data_directory.name, "triggers")
    os.makedirs(triggers_dest_dir, exist_ok=True)

    # Copy entire trigger directory
    if os.path.exists(triggers_source_dir):
        # Remove destination if it exists and copy the whole directory
        if os.path.exists(triggers_dest_dir):
            shutil.rmtree(triggers_dest_dir)
        shutil.copytree(triggers_source_dir, triggers_dest_dir)

    extra_args = ["--data-recovery-on-startup"]
    with memgraph_server(memgraph_binary, Path(working_data_directory.name), 7687, logger, extra_args):
        # Execute `DUMP DATABASE`
        connection = mgclient.connect(host="localhost", port=7687, sslmode=mgclient.MG_SSLMODE_DISABLE)
        dump_triggers_got = get_dump_triggers(connection)
        connection.close()

    if write_expected:
        # Write expected dump file
        expected_dump_file = os.path.join(test_directory, EXPECTED_DUMP_FILE_NAME)
        with open(expected_dump_file, "w") as expected:
            for trigger in dump_triggers_got:
                expected.write(trigger + "\n")
    else:
        # Compare triggers from DUMP DATABASE
        expected_dump_file = os.path.join(test_directory, EXPECTED_DUMP_FILE_NAME)
        assert os.path.exists(expected_dump_file), "Could not find expected dump file {}".format(expected_dump_file)
        dump_triggers_expected = read_content(expected_dump_file)
        dump_triggers_expected = [line.strip() for line in dump_triggers_expected if line.strip()]
        dump_triggers_got = [line.strip() for line in dump_triggers_got if line.strip()]

        assert dump_triggers_got == dump_triggers_expected, "Expected dump\n{}\nto be equal to\n{}".format(
            list_to_string(dump_triggers_got), list_to_string(dump_triggers_expected)
        )

    print("\033[1;32m~~ Test successful ~~\033[0m\n")


def find_test_directories(directory, write_expected):
    """
    Finds all test directories. Test directory is a directory directly under the given directory
    which contains a 'triggers' directory and expected_dump.cypher file.
    """
    test_dirs = []
    for entry_version in os.listdir(directory):
        entry_version_path = os.path.join(directory, entry_version)
        if not os.path.isdir(entry_version_path):
            continue
        triggers_dir = os.path.join(entry_version_path, TRIGGERS_DIR_NAME)
        expected_dump_file = os.path.join(entry_version_path, EXPECTED_DUMP_FILE_NAME)
        # if write_expected then we are not missing the expected file
        missing_expected_dump_file = not write_expected and not os.path.isfile(expected_dump_file)
        if os.path.isdir(triggers_dir) and not missing_expected_dump_file:
            test_dirs.append(entry_version_path)
        else:
            raise Exception("Missing data in test directory '{}'".format(entry_version_path))
    return test_dirs


if __name__ == "__main__":
    memgraph_binary = os.path.join(PROJECT_DIR, "build", "memgraph")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument(
        "--write-expected", action="store_true", help="Overwrite the expected dump with results from current run"
    )
    args = parser.parse_args()

    test_directories = find_test_directories(TESTS_DIR, args.write_expected)
    assert len(test_directories) > 0, "No tests have been found!"

    # To reduce confusion, test in version order
    test_directories.sort()

    for test_directory in test_directories:
        execute_test(Path(args.memgraph), test_directory, args.write_expected)

    sys.exit(0)
