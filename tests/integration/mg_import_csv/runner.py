#!/usr/bin/python3 -u
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


def wait_for_server(port, delay=0.1):
    cmd = ["nc", "-z", "-w", "1", "127.0.0.1", str(port)]
    while subprocess.call(cmd) != 0:
        time.sleep(0.01)
    time.sleep(delay)


def get_build_dir():
    if os.path.exists(os.path.join(BASE_DIR, "build_release")):
        return os.path.join(BASE_DIR, "build_release")
    if os.path.exists(os.path.join(BASE_DIR, "build_debug")):
        return os.path.join(BASE_DIR, "build_debug")
    if os.path.exists(os.path.join(BASE_DIR, "build_community")):
        return os.path.join(BASE_DIR, "build_community")
    return os.path.join(BASE_DIR, "build")


def extract_rows(data):
    return list(map(lambda x: x.strip(), data.strip().split("\n")))


def list_to_string(data):
    ret = "[\n"
    for row in data:
        ret += "    " + row + "\n"
    ret += "]"
    return ret


def verify_lifetime(memgraph_binary, mg_import_csv_binary):
    print("\033[1;36m~~ Verifying that mg_import_csv can't be started while "
          "memgraph is running ~~\033[0m")
    storage_directory = tempfile.TemporaryDirectory()

    # Generate common args
    common_args = ["--data-directory", storage_directory.name,
                   "--storage-properties-on-edges=false"]

    # Start the memgraph binary
    memgraph_args = [memgraph_binary, "--storage-recover-on-startup"] + \
        common_args
    memgraph = subprocess.Popen(list(map(str, memgraph_args)))
    time.sleep(0.1)
    assert memgraph.poll() is None, "Memgraph process died prematurely!"
    wait_for_server(7687)

    # Register cleanup function
    @atexit.register
    def cleanup():
        if memgraph.poll() is None:
            memgraph.terminate()
        assert memgraph.wait() == 0, "Memgraph process didn't exit cleanly!"

    # Execute mg_import_csv.
    mg_import_csv_args = [mg_import_csv_binary, "--nodes", "/dev/null"] + \
        common_args
    ret = subprocess.run(mg_import_csv_args)

    # Check the return code
    if ret.returncode == 0:
        raise Exception(
            "The importer was able to run while memgraph was running!")

    # Shutdown the memgraph binary
    memgraph.terminate()
    assert memgraph.wait() == 0, "Memgraph process didn't exit cleanly!"

    print("\033[1;32m~~ Test successful ~~\033[0m\n")


def execute_test(name, test_path, test_config, memgraph_binary,
                 mg_import_csv_binary, tester_binary):
    print("\033[1;36m~~ Executing test", name, "~~\033[0m")
    storage_directory = tempfile.TemporaryDirectory()

    # Verify test configuration
    if ("import_should_fail" not in test_config and
        "expected" not in test_config) or \
            ("import_should_fail" in test_config and
             "expected" in test_config):
        raise Exception("The test should specify either 'import_should_fail' "
                        "or 'expected'!")

    # Load test expected queries
    import_should_fail = test_config.pop("import_should_fail", False)
    expected_path = test_config.pop("expected", "")
    if expected_path:
        with open(os.path.join(test_path, expected_path)) as f:
            queries_expected = extract_rows(f.read())
    else:
        queries_expected = ""

    # Generate common args
    properties_on_edges = bool(test_config.pop("properties_on_edges", False))
    common_args = ["--data-directory", storage_directory.name,
                   "--storage-properties-on-edges=" +
                   str(properties_on_edges).lower()]

    # Generate mg_import_csv args using flags specified in the test
    mg_import_csv_args = [mg_import_csv_binary] + common_args
    for key, value in test_config.items():
        flag = "--" + key.replace("_", "-")
        if type(value) == list:
            for item in value:
                mg_import_csv_args.extend([flag, str(item)])
        elif type(value) == bool:
            mg_import_csv_args.append(flag + "=" + str(value).lower())
        else:
            mg_import_csv_args.extend([flag, str(value)])

    # Execute mg_import_csv
    ret = subprocess.run(mg_import_csv_args, cwd=test_path)

    # Check the return code
    if import_should_fail:
        if ret.returncode == 0:
            raise Exception("The import should have failed, but it "
                            "succeeded instead!")
        else:
            print("\033[1;32m~~ Test successful ~~\033[0m\n")
            return
    else:
        if ret.returncode != 0:
            raise Exception("The import should have succeeded, but it "
                            "failed instead!")

    # Start the memgraph binary
    memgraph_args = [memgraph_binary, "--storage-recover-on-startup"] + \
        common_args
    memgraph = subprocess.Popen(list(map(str, memgraph_args)))
    time.sleep(0.1)
    assert memgraph.poll() is None, "Memgraph process died prematurely!"
    wait_for_server(7687)

    # Register cleanup function
    @atexit.register
    def cleanup():
        if memgraph.poll() is None:
            memgraph.terminate()
        assert memgraph.wait() == 0, "Memgraph process didn't exit cleanly!"

    # Get the contents of the database
    queries_got = extract_rows(subprocess.run(
        [tester_binary], stdout=subprocess.PIPE,
        check=True).stdout.decode("utf-8"))

    # Shutdown the memgraph binary
    memgraph.terminate()
    assert memgraph.wait() == 0, "Memgraph process didn't exit cleanly!"

    # Verify the queries
    queries_expected.sort()
    queries_got.sort()
    assert queries_got == queries_expected, "Expected\n{}\nto be equal to\n" \
        "{}".format(list_to_string(queries_got),
                    list_to_string(queries_expected))
    print("\033[1;32m~~ Test successful ~~\033[0m\n")


if __name__ == "__main__":
    memgraph_binary = os.path.join(get_build_dir(), "memgraph")
    mg_import_csv_binary = os.path.join(
        get_build_dir(), "src", "mg_import_csv")
    tester_binary = os.path.join(
        get_build_dir(), "tests", "integration", "mg_import_csv", "tester")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--mg-import-csv", default=mg_import_csv_binary)
    parser.add_argument("--tester", default=tester_binary)
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
            execute_test(test_name, test_path, test_config, args.memgraph,
                         args.mg_import_csv, args.tester)

    sys.exit(0)
