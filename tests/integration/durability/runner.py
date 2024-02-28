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
import os
import shutil
import subprocess
import sys
import tempfile
import time

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", ".."))
TESTS_DIR = os.path.join(SCRIPT_DIR, "tests")

SNAPSHOT_FILE_NAME = "snapshot.bin"
WAL_FILE_NAME = "wal.bin"

DUMP_SNAPSHOT_FILE_NAME = "expected_snapshot.cypher"
DUMP_WAL_FILE_NAME = "expected_wal.cypher"

SIGNAL_SIGTERM = 15


def wait_for_server(port, delay=0.1):
    cmd = ["nc", "-z", "-w", "1", "127.0.0.1", str(port)]
    while subprocess.call(cmd) != 0:
        time.sleep(0.01)
    time.sleep(delay)


def sorted_content(file_path):
    with open(file_path, "r") as fin:
        return sorted(list(map(lambda x: x.strip(), fin.readlines())))


def list_to_string(data):
    ret = "[\n"
    for row in data:
        ret += "    " + row + "\n"
    ret += "]"
    return ret


def execute_test(memgraph_binary, dump_binary, test_directory, test_type, write_expected):
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

    memgraph_args = [
        memgraph_binary,
        "--data-recovery-on-startup",
        "--storage-properties-on-edges",
        "--data-directory",
        working_data_directory.name,
    ]

    # Start the memgraph binary
    memgraph = subprocess.Popen(memgraph_args)
    time.sleep(0.1)
    assert memgraph.poll() is None, "Memgraph process died prematurely!"
    wait_for_server(7687)

    # Register cleanup function
    @atexit.register
    def cleanup():
        if memgraph.poll() is None:
            pid = memgraph.pid
            try:
                os.kill(pid, SIGNAL_SIGTERM)
            except os.OSError:
                assert False
            time.sleep(1)

    # Execute `database dump`
    dump_output_file = tempfile.NamedTemporaryFile()
    dump_args = [dump_binary, "--use-ssl=false"]
    subprocess.run(dump_args, stdout=dump_output_file, check=True)

    # Shutdown the memgraph binary
    pid = memgraph.pid
    try:
        os.kill(pid, SIGNAL_SIGTERM)
    except os.OSError:
        assert False
    time.sleep(1)

    dump_file_name = DUMP_SNAPSHOT_FILE_NAME if test_type == "SNAPSHOT" else DUMP_WAL_FILE_NAME

    if write_expected:
        with open(dump_output_file.name, "r") as dump:
            queries_got = dump.readlines()
        # Write dump files
        expected_dump_file = os.path.join(test_directory, dump_file_name)
        with open(expected_dump_file, "w") as expected:
            expected.writelines(queries_got)
    else:
        # Compare dump files
        expected_dump_file = os.path.join(test_directory, dump_file_name)
        assert os.path.exists(expected_dump_file), "Could not find expected dump path {}".format(expected_dump_file)
        queries_got = sorted_content(dump_output_file.name)
        queries_expected = sorted_content(expected_dump_file)
        assert queries_got == queries_expected, "Expected\n{}\nto be equal to\n" "{}".format(
            list_to_string(queries_got), list_to_string(queries_expected)
        )

    print("\033[1;32m~~ Test successful ~~\033[0m\n")


def find_test_directories(directory):
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
            if (
                os.path.isfile(snapshot_file)
                and os.path.isfile(dump_snapshot_file)
                and os.path.isfile(wal_file)
                and os.path.isfile(dump_wal_file)
            ):
                test_dirs.append(test_dir_path)
            else:
                raise Exception("Missing data in test directory '{}'".format(test_dir_path))
    return test_dirs


if __name__ == "__main__":
    memgraph_binary = os.path.join(PROJECT_DIR, "build", "memgraph")
    dump_binary = os.path.join(PROJECT_DIR, "build", "tools", "src", "mg_dump")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--dump", default=dump_binary)
    parser.add_argument(
        "--write-expected", action="store_true", help="Overwrite the expected cypher with results from current run"
    )
    args = parser.parse_args()

    test_directories = find_test_directories(TESTS_DIR)
    assert len(test_directories) > 0, "No tests have been found!"

    for test_directory in test_directories:
        execute_test(args.memgraph, args.dump, test_directory, "SNAPSHOT", args.write_expected)
        execute_test(args.memgraph, args.dump, test_directory, "WAL", args.write_expected)

    sys.exit(0)
