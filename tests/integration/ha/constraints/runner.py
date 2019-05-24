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


class HaIndexTest(HaTestBase):
    def execute_step(self, step, property_value=None, expected_status=None,
            expected_result=None):
        if step == "create":
            print("Executing create query")
            client = subprocess.Popen([self.tester_binary, "--step", "create",
                "--cluster_size", str(self.cluster_size)])

        elif step == "drop":
            print("Executing drop query")
            client = subprocess.Popen([self.tester_binary, "--step", "drop",
                "--cluster_size", str(self.cluster_size)])
        elif step == "add_node":
            print("Executing add_node query ")
            client = subprocess.Popen([self.tester_binary, "--step", "add_node",
                "--cluster_size", str(self.cluster_size), "--expected_status",
                str(expected_status), "--property_value", str(property_value)])

        elif step == "check":
            print("Executing check query")
            client = subprocess.Popen([self.tester_binary, "--step", "check",
                "--cluster_size", str(self.cluster_size), "--expected_result",
                str(expected_result)])
        else:
            raise ValueError("Invalid step argument: " + step)

        # Check what happened with query execution.
        try:
            code = client.wait(timeout=30)
        except subprocess.TimeoutExpired as e:
            print("HA client timed out!")
            client.kill()
            return 1

        return code


    def execute(self):
        self.start_cluster()
        num_nodes = 1

        assert self.execute_step("add_node", expected_status=0, \
                property_value=num_nodes) == 0, \
                "Error while executing add_node query"

        assert self.execute_step("create") == 0, \
                "Error while executing create query"

        assert self.execute_step("check", expected_result=num_nodes) == 0, \
                "Error while executing check query"

        for i in range(self.cluster_size):
            # Kill worker.
            print("Killing worker {}".format(i))
            self.kill_worker(i)

            assert self.execute_step("add_node", expected_status=1, \
                    property_value=num_nodes) == 0, \
                    "Error while executing add_node query"

            assert self.execute_step("add_node", expected_status=0, \
                    property_value=num_nodes + 1) == 0, \
                    "Error while executing add_node query"

            num_nodes += 1

            # Bring worker back to life.
            print("Starting worker {}".format(i))
            self.start_worker(i)

        assert self.execute_step("drop") == 0, \
                "Error while executing drop query"

        assert self.execute_step("check", expected_result=num_nodes) == 0, \
                "Error while executing check query"


def find_correct_path(path):
    f = os.path.join(PROJECT_DIR, "build", path)
    if not os.path.exists(f):
        f = os.path.join(PROJECT_DIR, "build_debug", path)
    return f


if __name__ == "__main__":
    memgraph_binary = find_correct_path("memgraph_ha")
    tester_binary = find_correct_path(os.path.join("tests", "integration", "ha",
        "constraints", "tester"))

    raft_config_file = os.path.join(PROJECT_DIR, "tests", "integration", "ha",
        "constraints", "raft.json")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--raft_config_file", default=raft_config_file)
    args = parser.parse_args()

    for cluster_size in [3, 5]:
        print("\033[1;36m~~ Executing test with cluster size: %d~~\033[0m" % (cluster_size))
        HaIndexTest(
            args.memgraph, tester_binary, args.raft_config_file, cluster_size)
        print("\033[1;32m~~ The test finished successfully ~~\033[0m")

    sys.exit(0)
