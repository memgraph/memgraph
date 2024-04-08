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
import subprocess
import sys
import tempfile
import time

import yaml

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", ".."))
BUILD_DIR = os.path.join(BASE_DIR, "build")
SIGNAL_SIGTERM = 15


def wait_for_server(port, delay=0.1):
    cmd = ["nc", "-z", "-w", "1", "127.0.0.1", str(port)]
    while subprocess.call(cmd) != 0:
        time.sleep(0.01)
    time.sleep(delay)


def extract_rows(data):
    return list(map(lambda x: x.strip(), data.strip().split("\n")))


def list_to_string(data):
    ret = "[\n"
    for row in data:
        ret += "    " + row + "\n"
    ret += "]"
    return ret


def verify_lifetime(memgraph_binary, mg_import_csv_binary):
    print("\033[1;36m~~ Verifying that mg_import_csv can't be started while " "memgraph is running ~~\033[0m")
    storage_directory = tempfile.TemporaryDirectory()

    # Generate common args
    common_args = ["--data-directory", storage_directory.name, "--storage-properties-on-edges=false"]

    # Start the memgraph binary
    memgraph_args = [memgraph_binary, "--data-recovery-on-startup"] + common_args
    memgraph = subprocess.Popen(list(map(str, memgraph_args)))
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
                assert False, "Memgraph process didn't exit cleanly!"
            time.sleep(1)

    # Execute mg_import_csv.
    mg_import_csv_args = [mg_import_csv_binary, "--nodes", "/dev/null"] + common_args
    ret = subprocess.run(mg_import_csv_args)

    # Check the return code
    if ret.returncode == 0:
        raise Exception("The importer was able to run while memgraph was running!")

    # Shutdown the memgraph binary
    pid = memgraph.pid
    try:
        os.kill(pid, SIGNAL_SIGTERM)
    except os.OSError:
        assert False, "Memgraph process didn't exit cleanly!"
    time.sleep(1)

    print("\033[1;32m~~ Test successful ~~\033[0m\n")


def execute_test(name, test_path, test_config, memgraph_binary, mg_import_csv_binary, tester_binary, write_expected):
    print("\033[1;36m~~ Executing test", name, "~~\033[0m")
    storage_directory = tempfile.TemporaryDirectory()

    # Verify test configuration
    if ("import_should_fail" not in test_config and "expected" not in test_config) or (
        "import_should_fail" in test_config and "expected" in test_config
    ):
        raise Exception("The test should specify either 'import_should_fail' " "or 'expected'!")

    expected_path = test_config.pop("expected", "")
    import_should_fail = test_config.pop("import_should_fail", False)

    # Generate common args
    properties_on_edges = bool(test_config.pop("properties_on_edges", False))
    common_args = [
        "--data-directory",
        storage_directory.name,
        "--storage-properties-on-edges=" + str(properties_on_edges).lower(),
    ]

    # Generate mg_import_csv args using flags specified in the test
    mg_import_csv_args = [mg_import_csv_binary] + common_args
    for key, value in test_config.items():
        flag = "--" + key.replace("_", "-")
        if isinstance(value, list):
            for item in value:
                mg_import_csv_args.extend([flag, str(item)])
        elif isinstance(value, bool):
            mg_import_csv_args.append(flag + "=" + str(value).lower())
        else:
            mg_import_csv_args.extend([flag, str(value)])

    # Execute mg_import_csv
    ret = subprocess.run(mg_import_csv_args, cwd=test_path)

    if import_should_fail:
        if ret.returncode == 0:
            raise Exception("The import should have failed, but it " "succeeded instead!")
        else:
            print("\033[1;32m~~ Test successful ~~\033[0m\n")
            return
    else:
        if ret.returncode != 0:
            raise Exception("The import should have succeeded, but it " "failed instead!")

    # Start the memgraph binary
    memgraph_args = [memgraph_binary, "--data-recovery-on-startup"] + common_args
    memgraph = subprocess.Popen(list(map(str, memgraph_args)))
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
                assert False, "Memgraph process didn't exit cleanly!"
            time.sleep(1)

    # Get the contents of the database
    queries_got = extract_rows(
        subprocess.run([tester_binary], stdout=subprocess.PIPE, check=True).stdout.decode("utf-8")
    )

    # Shutdown the memgraph binary
    pid = memgraph.pid
    try:
        os.kill(pid, SIGNAL_SIGTERM)
    except os.OSError:
        assert False, "Memgraph process didn't exit cleanly!"
    time.sleep(1)

    if write_expected:
        with open(os.path.join(test_path, expected_path), "w") as expected:
            expected.write("\n".join(queries_got))

    else:
        if expected_path:
            with open(os.path.join(test_path, expected_path)) as f:
                queries_expected = extract_rows(f.read())
        else:
            queries_expected = ""

        # Verify the queries
        queries_expected.sort()
        queries_got.sort()
        assert queries_got == queries_expected, "Got:\n{}\nExpected:\n" "{}".format(
            list_to_string(queries_got), list_to_string(queries_expected)
        )
    print("\033[1;32m~~ Test successful ~~\033[0m\n")


if __name__ == "__main__":
    memgraph_binary = os.path.join(BUILD_DIR, "memgraph")
    mg_import_csv_binary = os.path.join(BUILD_DIR, "src", "mg_import_csv")
    tester_binary = os.path.join(BUILD_DIR, "tests", "integration", "mg_import_csv", "tester")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--mg-import-csv", default=mg_import_csv_binary)
    parser.add_argument("--tester", default=tester_binary)
    parser.add_argument(
        "--write-expected",
        action="store_true",
        help="Overwrite the expected values with the results of the current run",
    )
    args = parser.parse_args()

    # First test whether the CSV importer can be started while the main
    # Memgraph binary is running.
    verify_lifetime(memgraph_binary, mg_import_csv_binary)

    # Run all import scenarios.
    test_dir = os.path.join(SCRIPT_DIR, "tests")
    tests_list = sorted(os.listdir(test_dir))
    assert len(tests_list) > 0, "No tests were found!"
    for name in tests_list:
        print("\033[1;34m~~ Processing tests from", name, "~~\033[0m\n")
        test_path = os.path.join(test_dir, name)
        with open(os.path.join(test_path, "test.yaml")) as f:
            testcases = yaml.safe_load(f)
        for test_config in testcases:
            test_name = name + "/" + test_config.pop("name")
            execute_test(
                test_name, test_path, test_config, args.memgraph, args.mg_import_csv, args.tester, args.write_expected
            )

    sys.exit(0)
