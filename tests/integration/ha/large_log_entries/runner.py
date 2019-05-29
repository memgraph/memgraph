#!/usr/bin/python3

import argparse
import os
import time
import random
import subprocess
import sys

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", "..", ".."))

# append parent directory
sys.path.append(os.path.join(SCRIPT_DIR, ".."))

from ha_test import HaTestBase


class HaLargeLogEntriesTest(HaTestBase):
    def execute_step(self, step, node_count):
        if step == "create":
            print("Executing create query")
            client = subprocess.Popen([self.tester_binary, "--step", "create",
                "--cluster_size", str(self.cluster_size),
                "--create_nodes", str(node_count)])

        elif step == "check":
            print("Executing check query")
            client = subprocess.Popen([self.tester_binary, "--step", "check",
                "--cluster_size", str(self.cluster_size),
                "--check_nodes", str(node_count)])
        else:
            return 0

        # Check what happened with query execution.
        try:
            code = client.wait(timeout=120)
        except subprocess.TimeoutExpired as e:
            print("HA client timed out!")
            client.kill()
            return 1

        return code


    def execute(self):
        # Number of nodes to be created in a single batch
        nodes = 250000

        self.start_cluster()

        for i in range(self.cluster_size):
            assert self.execute_step("create", nodes) == 0, \
                    "Error while executing create query"

            # Kill worker.
            print("Killing worker {}".format(i))
            self.kill_worker(i)

            assert self.execute_step("check", (i + 1) * nodes) == 0, \
                    "Error while executing check query"

            # Bring worker back to life.
            print("Starting worker {}".format(i))
            self.start_worker(i)


def find_correct_path(path):
    f = os.path.join(PROJECT_DIR, "build", path)
    if not os.path.exists(f):
        f = os.path.join(PROJECT_DIR, "build_debug", path)
    return f


if __name__ == "__main__":
    memgraph_binary = find_correct_path("memgraph_ha")
    tester_binary = find_correct_path(os.path.join("tests", "integration", "ha",
        "large_log_entries", "tester"))

    raft_config_file = os.path.join(PROJECT_DIR, "tests", "integration", "ha",
        "large_log_entries", "raft.json")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--raft_config_file", default=raft_config_file)
    args = parser.parse_args()

    for cluster_size in [3, 5]:
        print("\033[1;36m~~ Executing test with cluster size: %d~~\033[0m" % (cluster_size))
        HaLargeLogEntriesTest(
            args.memgraph, tester_binary, args.raft_config_file, cluster_size)
        print("\033[1;32m~~ The test finished successfully ~~\033[0m")

    sys.exit(0)
