import argparse
import atexit
import os
import subprocess
import sys
import tempfile
import time
from typing import List

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", ".."))


def wait_for_server(port, delay=0.1):
    cmd = ["nc", "-z", "-w", "1", "127.0.0.1", str(port)]
    while subprocess.call(cmd) != 0:
        time.sleep(0.01)
    time.sleep(delay)


def execute_tester(
    binary, queries, should_fail=False, failure_message="", username="", password="", check_failure=True
):
    args = [binary, "--username", username, "--password", password]
    if should_fail:
        args.append("--should-fail")
    if failure_message:
        args.extend(["--failure-message", failure_message])
    if check_failure:
        args.append("--check-failure")
    args.extend(queries)
    subprocess.run(args).check_returncode()


def execute_test_analytical_mode(memgraph_binary: str, tester_binary: str) -> None:
    storage_directory = tempfile.TemporaryDirectory()
    memgraph_args = [memgraph_binary, "--data-directory", storage_directory.name]

    def execute_queries(queries):
        return execute_tester(tester_binary, queries, should_fail=False, check_failure=True, username="", password="")

    # Start the memgraph binary
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

    execute_queries(["STORAGE MODE IN_MEMORY_ANALYTICAL"])

    node_queries = [
        "CREATE (:Label {prop:'1'})",
        "CREATE (:Label {prop:'2'})",
    ]
    edge_queries = ["MATCH (l1:Label),(l2:Label) WHERE l1.prop = '1' AND l2.prop = '2' CREATE (l1)-[r:edgeType1]->(l2)"]

    # Prepare all nodes
    execute_queries(node_queries)

    # Prepare all edges
    execute_queries(edge_queries)

    # Run the test with all combinations of permissions
    print("\033[1;36m~~ Starting creating & loading snapshot test ~~\033[0m")
    execute_queries(["CREATE SNAPSHOT;"])

    print("\033[1;36m~~ Created snapshot ~~\033[0m\n")

    # Shutdown the memgraph binary
    memgraph.terminate()
    time.sleep(0.1)
    assert memgraph.wait() == 0, "Memgraph process didn't exit cleanly!"

    memgraph_args = [memgraph_binary, "--data-directory", storage_directory.name, "--storage-recover-on-startup=true"]

    # Start the memgraph binary
    memgraph = subprocess.Popen(list(map(str, memgraph_args)))
    time.sleep(0.1)
    assert memgraph.poll() is None, "Memgraph process died prematurely!"
    wait_for_server(7687)

    execute_queries([f"MATCH (n) WITH count(n) as cnt RETURN assert(cnt={len(node_queries)});"])
    execute_queries([f"MATCH (n)-[e]->(m) WITH count(e) as cnt RETURN assert(cnt={len(edge_queries)});"])

    memgraph.terminate()
    assert memgraph.wait() == 0, "Memgraph process didn't exit cleanly!"


def execute_test_switch_modes(memgraph_binary: str, tester_binary: str) -> None:
    storage_directory = tempfile.TemporaryDirectory()
    memgraph_args = [memgraph_binary, "--data-directory", storage_directory.name]

    def execute_queries(queries):
        return execute_tester(tester_binary, queries, should_fail=False, check_failure=True, username="", password="")

    # Start the memgraph binary
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

    execute_queries(["STORAGE MODE IN_MEMORY_ANALYTICAL"])
    node_queries = [
        "CREATE (:Label {prop:'1'})",
        "CREATE (:Label {prop:'2'})",
    ]

    edge_queries = ["MATCH (l1:Label),(l2:Label) WHERE l1.prop = '1' AND l2.prop = '2' CREATE (l1)-[r:edgeType1]->(l2)"]

    # Prepare all nodes
    execute_queries(node_queries)

    execute_queries(["STORAGE MODE IN_MEMORY_TRANSACTIONAL"])

    # Prepare all edges
    execute_queries(edge_queries)

    # Run the test with all combinations of permissions
    print("\033[1;36m~~ Starting creating & loading snapshot test ~~\033[0m")
    execute_queries(["CREATE SNAPSHOT;"])

    print("\033[1;36m~~ Created snapshot ~~\033[0m\n")

    print("\033[1;36m~~ Terminating memgraph ~~\033[0m\n")
    # Shutdown the memgraph binary
    memgraph.terminate()
    time.sleep(0.1)
    assert memgraph.wait() == 0, "Memgraph process didn't exit cleanly!"

    memgraph_args = [memgraph_binary, "--data-directory", storage_directory.name, "--storage-recover-on-startup=true"]

    print("\033[1;36m~~ Loading memgraph with snapshot ~~\033[0m\n")
    # Start the memgraph binary
    memgraph = subprocess.Popen(list(map(str, memgraph_args)))
    time.sleep(0.1)
    assert memgraph.poll() is None, "Memgraph process died prematurely!"
    wait_for_server(7687)

    execute_queries([f"MATCH (n) WITH count(n) as cnt RETURN assert(cnt={len(node_queries)});"])
    execute_queries([f"MATCH (n)-[e]->(m) WITH count(e) as cnt RETURN assert(cnt={len(edge_queries)});"])

    print("\033[1;36m~~ Terminating memgraph ~~\033[0m\n")
    memgraph.terminate()
    assert memgraph.wait() == 0, "Memgraph process didn't exit cleanly!"


if __name__ == "__main__":
    memgraph_binary = os.path.join(PROJECT_DIR, "build", "memgraph")
    tester_binary = os.path.join(PROJECT_DIR, "build", "tests", "integration", "storage_mode", "tester")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--tester", default=tester_binary)
    args = parser.parse_args()

    execute_test_analytical_mode(args.memgraph, args.tester)
    execute_test_switch_modes(args.memgraph, args.tester)
    sys.exit(0)
