#!/usr/bin/python3

"""
This test checks the the basic functionality of HA Memgraph. It incorporates
both leader election and log replication processes.

The test proceeds as follows for clusters of size 3 and 5:
    1) Start the whole cluster
    2) Kill random workers but leave the majority alive
    3) Create a single Node
    4) Bring dead nodes back to life
    5) Kill random workers but leave the majority alive
    6) Check if everything is ok with DB state
    7) GOTO 1) and repeat 25 times
"""

import argparse
import os
import time
import subprocess
import sys
import random

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", "..", ".."))

# append parent directory
sys.path.append(os.path.join(SCRIPT_DIR, ".."))

from ha_test import HaTestBase


class HaBasicTest(HaTestBase):
    def execute_step(self, step, node_count):
        if step == "create":
            print("Executing create query")
            client = subprocess.Popen([self.tester_binary,
                                       "--step", "create",
                                       "--cluster-size", str(self.cluster_size),
                                       "--node-count", str(node_count)])
        elif step == "count":
            print("Executing count query")
            client = subprocess.Popen([self.tester_binary,
                                       "--step", "count",
                                       "--cluster_size", str(self.cluster_size),
                                       "--node-count", str(node_count)])
        else:
            return 0

        # Check what happened with query execution.
        try:
            code = client.wait(timeout=30)
        except subprocess.TimeoutExpired as e:
            print("HA client timed out!")
            client.kill()
            return 1

        return code


    def start_workers(self, worker_ids):
        for wid in worker_ids:
            print("Starting worker {}".format(wid + 1))
            self.start_worker(wid)


    def kill_workers(self, worker_ids):
        for wid in worker_ids:
            print("Killing worker {}".format(wid + 1))
            self.kill_worker(wid)


    def execute(self):
        self.start_cluster()

        expected_results = 0

        # Make sure at least one node exists.
        assert self.execute_step("create", expected_results) == 0, \
                "Error while executing create query"
        expected_results = 1

        for i in range(20):
            # Create step
            partition = random.sample(range(self.cluster_size),
                     random.randint(0, int((self.cluster_size - 1) / 2)))

            self.kill_workers(partition)

            assert self.execute_step("create", expected_results) == 0, \
                    "Error while executing create query"
            expected_results += 1

            self.start_workers(partition)

            # Check step
            partition = random.sample(range(self.cluster_size),
                     random.randint(0, int((self.cluster_size - 1) / 2)))

            self.kill_workers(partition)

            assert self.execute_step("count", expected_results) == 0, \
                    "Error while executing count query"

            self.start_workers(partition)

        # Check that no data was lost.
        assert self.execute_step("count", expected_results) == 0, \
                "Error while executing count query"


def find_correct_path(path):
    return os.path.join(PROJECT_DIR, "build", path)


if __name__ == "__main__":
    memgraph_binary = find_correct_path("memgraph_ha")
    tester_binary = find_correct_path(os.path.join("tests", "integration", "ha",
        "basic", "tester"))

    raft_config_file = os.path.join(PROJECT_DIR, "tests", "integration", "ha",
        "basic", "raft.json")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--raft_config_file", default=raft_config_file)
    args = parser.parse_args()

    for cluster_size in [3, 5]:
        print("\033[1;36m~~ Executing test with cluster size: %d~~\033[0m" % (cluster_size))
        HaBasicTest(
            args.memgraph, tester_binary, args.raft_config_file, cluster_size)
        print("\033[1;32m~~ The test finished successfully ~~\033[0m")

    sys.exit(0)
