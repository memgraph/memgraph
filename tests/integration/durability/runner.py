#!/usr/bin/python3 -u

# Copyright 2021 Memgraph Ltd.
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
import atexit
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import mgclient
from memgraph_server_context import memgraph_server

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", ".."))
TESTS_DIR = os.path.join(SCRIPT_DIR, "tests")

SNAPSHOT_FILE_NAME = "snapshot.bin"
WAL_FILE_NAME = "wal.bin"

DUMP_SNAPSHOT_FILE_NAME = "expected_snapshot.cypher"
DUMP_WAL_FILE_NAME = "expected_wal.cypher"

SIGNAL_SIGTERM = 15

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


def dump_database(output_file):
    connection = mgclient.connect(host="localhost", port=7687, sslmode=mgclient.MG_SSLMODE_DISABLE)
    cursor = connection.cursor()
    cursor.execute("DUMP DATABASE")
    rows = cursor.fetchall()
    cursor.close()
    connection.close()

    with open(output_file, "w") as f:
        f.write("\n".join(row[0] for row in rows) + "\n")


def execute_test(memgraph_binary: Path, test_directory, test_type, write_expected):
    assert test_type in ["SNAPSHOT", "WAL"], "Test type should be either 'SNAPSHOT' or 'WAL'."
    print("\033[1;36m~~ Executing test {} ({}) ~~\033[0m".format(os.path.relpath(test_directory, TESTS_DIR), test_type))

    working_data_directory = tempfile.TemporaryDirectory()
    if test_type == "SNAPSHOT":
        snapshots_dir = os.path.join(working_data_directory.name, "snapshots")
        os.makedirs(snapshots_dir)
        shutil.copy(os.path.join(test_directory, SNAPSHOT_FILE_NAME), snapshots_dir)
    else:
        wal_dir = os.path.join(working_data_directory.name, "wal")
        os.makedirs(wal_dir)
        shutil.copy(os.path.join(test_directory, WAL_FILE_NAME), wal_dir)

    extra_args = ["--data-recovery-on-startup"]
    with memgraph_server(memgraph_binary, Path(working_data_directory.name), 7687, logger, extra_args):
        # Execute `database dump`
        dump_output_file = tempfile.NamedTemporaryFile()
        dump_database(dump_output_file.name)

    dump_file_name = DUMP_SNAPSHOT_FILE_NAME if test_type == "SNAPSHOT" else DUMP_WAL_FILE_NAME

    if write_expected:
        queries_got = read_content(dump_output_file.name)
        # Write dump files
        expected_dump_file = os.path.join(test_directory, dump_file_name)
        with open(expected_dump_file, "w") as expected:
            expected.writelines(queries_got)
    else:
        # Compare dump files
        expected_dump_file = os.path.join(test_directory, dump_file_name)
        assert os.path.exists(expected_dump_file), "Could not find expected dump path {}".format(expected_dump_file)
        queries_got = read_content(dump_output_file.name)
        queries_expected = read_content(expected_dump_file)
        assert queries_got == queries_expected, "Expected\n{}\nto be equal to\n" "{}".format(
            list_to_string(queries_got), list_to_string(queries_expected)
        )

    print("\033[1;32m~~ Test successful ~~\033[0m\n")


def find_test_directories(directory, write_expected):
    """
    Finds all test directories. Test directory is a directory two levels below
    the given directory which contains files 'snapshot.bin', 'wal.bin' and
    'expected.cypher'.
    """
    test_dirs = []
    for entry_version in os.listdir(directory):
        entry_version_path = os.path.join(directory, entry_version)
        if not os.path.isdir(entry_version_path):
            continue
        for test_dir in os.listdir(entry_version_path):
            test_dir_path = os.path.join(entry_version_path, test_dir)
            if not os.path.isdir(test_dir_path):
                continue
            snapshot_file = os.path.join(test_dir_path, SNAPSHOT_FILE_NAME)
            wal_file = os.path.join(test_dir_path, WAL_FILE_NAME)
            dump_snapshot_file = os.path.join(test_dir_path, DUMP_SNAPSHOT_FILE_NAME)
            dump_wal_file = os.path.join(test_dir_path, DUMP_WAL_FILE_NAME)
            # if write_expected then we are not missing those files
            missing_dump_files = not write_expected and (
                not os.path.isfile(dump_snapshot_file) or not os.path.isfile(dump_wal_file)
            )
            if os.path.isfile(snapshot_file) and os.path.isfile(wal_file) and not missing_dump_files:
                test_dirs.append(test_dir_path)
            else:
                raise Exception("Missing data in test directory '{}'".format(test_dir_path))
    return test_dirs


if __name__ == "__main__":
    memgraph_binary = os.path.join(PROJECT_DIR, "build", "memgraph")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument(
        "--write-expected", action="store_true", help="Overwrite the expected cypher with results from current run"
    )
    args = parser.parse_args()

    test_directories = find_test_directories(TESTS_DIR, args.write_expected)
    assert len(test_directories) > 0, "No tests have been found!"

    # To reduce confusion, test in version order
    test_directories.sort()

    for test_directory in test_directories:
        execute_test(Path(args.memgraph), test_directory, "SNAPSHOT", args.write_expected)
        execute_test(Path(args.memgraph), test_directory, "WAL", args.write_expected)

    sys.exit(0)
