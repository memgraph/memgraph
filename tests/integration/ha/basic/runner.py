#!/usr/bin/python3

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
    def execute_step(self, step, expected_results):
        if step == "create":
            client = subprocess.Popen([self.tester_binary, "--step", "create",
                "--cluster_size", str(self.cluster_size)])

        elif step == "count":
            client = subprocess.Popen([self.tester_binary, "--step", "count",
                "--cluster_size", str(self.cluster_size), "--expected_results",
                str(expected_results)])
        else:
            return 0

        # Check what happened with query execution.
        try:
            code = client.wait(timeout=30)
        except subprocess.TimeoutExpired as e:
            client.kill()
            return 1

        return code


    def execute(self):
        self.start_cluster()

        expected_results = 0

        # Make sure at least one node exists.
        assert self.execute_step("create", expected_results) == 0, \
                "Error while executing create query"
        expected_results = 1

        for i in range(2 * self.cluster_size):
            partition = random.sample(range(self.cluster_size),
                    int((self.cluster_size - 1) / 2))

            # Kill workers.
            for worker_id in partition:
                self.kill_worker(worker_id)

            time.sleep(2) # allow some time for possible leader re-election

            if random.random() < 0.7:
                assert self.execute_step("create", expected_results) == 0, \
                        "Error while executing create query"
                expected_results += 1
            else:
                assert self.execute_step("count", expected_results) == 0, \
                        "Error while executing count query"

            # Bring workers back to life.
            for worker_id in partition:
                self.start_worker(worker_id)

        # Check that no data was lost.
        assert self.execute_step("count", expected_results) == 0, \
                "Error while executing count query"


def find_correct_path(path):
    f = os.path.join(PROJECT_DIR, "build", path)
    if not os.path.exists(f):
        f = os.path.join(PROJECT_DIR, "build_debug", path)
    return f


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
