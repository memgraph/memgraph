import argparse
import os
import subprocess
import sys
import tempfile
import time

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", ".."))
BUILD_DIR = os.path.join(PROJECT_DIR, "build")
INIT_FILE = os.path.join(SCRIPT_DIR, "auth.cypherl")
SIGNAL_SIGTERM = 15


def wait_for_server(port, delay=0.1):
    cmd = ["nc", "-z", "-w", "1", "127.0.0.1", str(port)]
    while subprocess.call(cmd) != 0:
        time.sleep(0.01)
    time.sleep(delay)


def prepare_memgraph(memgraph_args):
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


def execute_test_restart_memgraph_with_init_file(memgraph_binary: str, tester_binary: str) -> None:
    storage_directory = tempfile.TemporaryDirectory()
    tester_args = [tester_binary, "--username", "memgraph1", "--password", "1234"]
    memgraph = prepare_memgraph([memgraph_binary, "--data-directory", storage_directory.name, "--init-file", INIT_FILE])
    subprocess.run(tester_args, stdout=subprocess.PIPE, check=True).check_returncode()
    terminate_memgraph(memgraph)
    memgraph = prepare_memgraph([memgraph_binary, "--data-directory", storage_directory.name, "--init-file", INIT_FILE])
    subprocess.run(tester_args, stdout=subprocess.PIPE, check=True).check_returncode()
    terminate_memgraph(memgraph)


if __name__ == "__main__":
    memgraph_binary = os.path.join(PROJECT_DIR, "build", "memgraph")
    tester_binary = os.path.join(BUILD_DIR, "tests", "integration", "init_file", "tester")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--tester", default=tester_binary)
    args = parser.parse_args()

    execute_test_restart_memgraph_with_init_file(args.memgraph, args.tester)
    sys.exit(0)
