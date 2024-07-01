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

node_queries = [
    "CREATE (:Label {prop:'1'})",
    "CREATE (:Label {prop:'2'})",
]

edge_queries = ["MATCH (l1:Label),(l2:Label) WHERE l1.prop = '1' AND l2.prop = '2' CREATE (l1)-[r:edgeType1]->(l2)"]

assertion_queries = [
    f"MATCH (n) WITH count(n) as cnt RETURN assert(cnt={len(node_queries)});",
    f"MATCH (n)-[e]->(m) WITH count(e) as cnt RETURN assert(cnt={len(edge_queries)});",
]

SIGNAL_SIGTERM = 15


def wait_for_server(port, delay=0.1):
    cmd = ["nc", "-z", "-w", "1", "127.0.0.1", str(port)]
    while subprocess.call(cmd) != 0:
        time.sleep(0.01)
    time.sleep(delay)


def prepare_memgraph(memgraph_args):
    # Start the memgraph binary
    memgraph = subprocess.Popen(list(map(str, memgraph_args)))
    time.sleep(0.1)
    assert memgraph.poll() is None, "Memgraph process died prematurely!"
    wait_for_server(7687)
    return memgraph


def terminate_memgraph(memgraph):
    pid = memgraph.pid
    try:
        os.kill(pid, SIGNAL_SIGTERM)
    except os.OSError:
        assert False, "Memgraph process didn't exit cleanly!"
    time.sleep(1)


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
    def execute_queries(queries):
        return execute_tester(tester_binary, queries, should_fail=False, check_failure=True, username="", password="")

    storage_directory = tempfile.TemporaryDirectory()
    memgraph = prepare_memgraph([memgraph_binary, "--data-directory", storage_directory.name])

    print("\033[1;36m~~ Starting creating & loading snapshot test ~~\033[0m")

    execute_queries(["STORAGE MODE IN_MEMORY_ANALYTICAL"])

    # Prepare all nodes
    execute_queries(node_queries)

    # Prepare all edges
    execute_queries(edge_queries)

    execute_queries(["CREATE SNAPSHOT;"])

    print("\033[1;36m~~ Created snapshot ~~\033[0m\n")

    # Shutdown the memgraph binary with wait
    terminate_memgraph(memgraph)

    # Start the memgraph binary
    memgraph = prepare_memgraph(
        [memgraph_binary, "--data-directory", storage_directory.name, "--data-recovery-on-startup=true"]
    )

    execute_queries(assertion_queries)

    pid = memgraph.pid
    try:
        os.kill(pid, SIGNAL_SIGTERM)
    except os.OSError:
        assert False, "Memgraph process didn't exit cleanly!"
    time.sleep(1)


def execute_test_switch_analytical_transactional(memgraph_binary: str, tester_binary: str) -> None:
    def execute_queries(queries):
        return execute_tester(tester_binary, queries, should_fail=False, check_failure=True, username="", password="")

    storage_directory = tempfile.TemporaryDirectory()

    # Start the memgraph binary
    memgraph = prepare_memgraph([memgraph_binary, "--data-directory", storage_directory.name])

    print("\033[1;36m~~ Starting switch storage modes test ~~\033[0m")

    # switch to IN_MEMORY_ANALYTICAL
    execute_queries(["STORAGE MODE IN_MEMORY_ANALYTICAL"])

    # Prepare all nodes
    execute_queries(node_queries)

    # switch back to IN_MEMORY_TRANSACTIONAL
    execute_queries(["STORAGE MODE IN_MEMORY_TRANSACTIONAL"])

    # Prepare all edges
    execute_queries(edge_queries)

    execute_queries(["CREATE SNAPSHOT;"])

    print("\033[1;36m~~ Created snapshot ~~\033[0m\n")

    print("\033[1;36m~~ Terminating memgraph ~~\033[0m\n")

    # Shutdown the memgraph binary with wait
    terminate_memgraph(memgraph)

    print("\033[1;36m~~ Starting memgraph with snapshot recovery ~~\033[0m\n")

    memgraph = prepare_memgraph(
        [memgraph_binary, "--data-directory", storage_directory.name, "--data-recovery-on-startup=true"]
    )

    execute_queries(assertion_queries)

    print("\033[1;36m~~ Terminating memgraph ~~\033[0m\n")
    pid = memgraph.pid
    try:
        os.kill(pid, SIGNAL_SIGTERM)
    except os.OSError:
        assert False, "Memgraph process didn't exit cleanly!"
    time.sleep(1)


def execute_test_switch_transactional_analytical(memgraph_binary: str, tester_binary: str) -> None:
    def execute_queries(queries):
        return execute_tester(tester_binary, queries, should_fail=False, check_failure=True, username="", password="")

    storage_directory = tempfile.TemporaryDirectory()

    # Start the memgraph binary
    memgraph = prepare_memgraph([memgraph_binary, "--data-directory", storage_directory.name])

    print("\033[1;36m~~ Starting switch storage modes test ~~\033[0m")

    # Prepare all nodes
    execute_queries(node_queries)

    # switch to IN_MEMORY_ANALYTICAL
    execute_queries(["STORAGE MODE IN_MEMORY_ANALYTICAL"])

    # Prepare all edges
    execute_queries(edge_queries)

    execute_queries(["CREATE SNAPSHOT;"])

    print("\033[1;36m~~ Created snapshot ~~\033[0m\n")

    print("\033[1;36m~~ Terminating memgraph ~~\033[0m\n")

    # Shutdown the memgraph binary with wait
    terminate_memgraph(memgraph)

    print("\033[1;36m~~ Starting memgraph with snapshot recovery ~~\033[0m\n")

    memgraph = prepare_memgraph(
        [memgraph_binary, "--data-directory", storage_directory.name, "--data-recovery-on-startup=true"]
    )

    execute_queries(assertion_queries)

    print("\033[1;36m~~ Terminating memgraph ~~\033[0m\n")
    pid = memgraph.pid
    try:
        os.kill(pid, SIGNAL_SIGTERM)
    except os.OSError:
        assert False, "Memgraph process didn't exit cleanly!"
    time.sleep(1)


if __name__ == "__main__":
    memgraph_binary = os.path.join(PROJECT_DIR, "build", "memgraph")
    tester_binary = os.path.join(PROJECT_DIR, "build", "tests", "integration", "storage_mode", "tester")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--tester", default=tester_binary)
    args = parser.parse_args()

    execute_test_analytical_mode(args.memgraph, args.tester)
    execute_test_switch_analytical_transactional(args.memgraph, args.tester)
    execute_test_switch_transactional_analytical(args.memgraph, args.tester)
    sys.exit(0)
