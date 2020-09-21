#!/usr/bin/python3

"""
This test checks the correctness of a leader election process when its decoupled
from log replication. In other words, in this test we do not change the state of
the database, i.e., the Raft log remains empty.

The test proceeds as follows for clusters of size 3 and 5:
    1) Start a random subset of workers in the cluster
    2) Check if the leader has been elected
    3) Kill all living workers
    4) GOTO 1) and repeat 10 times

Naturally, throughout the process we keep track the number of alive workers and,
based on that number (majority or not), we decide whether the leader should have
been elected.
"""

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

def random_subset(l):
    return random.sample(l, random.randint(0, len(l) - 1))


class HaLeaderElectionTest(HaTestBase):
    def leader_check(self, has_majority):
        client = subprocess.Popen([self.tester_binary,
            "--has-majority", str(int(has_majority)),
            "--cluster-size", str(int(cluster_size))])

        # Check what happened with query execution.
        try:
            code = client.wait(timeout=60)
        except subprocess.TimeoutExpired as e:
            print("Error! client timeout expired.")
            client.kill()
            return 1

        return code


    def execute(self):
        for i in range(10):
            # Awake random subset of dead nodes
            alive = random_subset(range(0, self.cluster_size))
            for wid in alive:
                print("Starting worker {}".format(wid + 1))
                self.start_worker(wid)

            # Check if leader is elected
            assert self.leader_check(2 * len(alive) > self.cluster_size) == 0, \
                    "Error while trying to find leader"

            # Kill living nodes
            for wid in alive:
                print("Killing worker {}".format(wid + 1))
                self.kill_worker(wid)

        self.destroy_cluster()


def find_correct_path(path):
    return os.path.join(PROJECT_DIR, "build", path)


if __name__ == "__main__":
    memgraph_binary = find_correct_path("memgraph_ha")
    tester_binary = find_correct_path(os.path.join("tests", "integration", "ha",
        "leader_election", "tester"))

    raft_config_file = os.path.join(PROJECT_DIR, "tests", "integration", "ha",
        "leader_election", "raft.json")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--raft_config_file", default=raft_config_file)
    args = parser.parse_args()

    for cluster_size in [3, 5]:
        print("\033[1;36m~~ Executing test with cluster size: %d~~\033[0m" % (cluster_size))
        HaLeaderElectionTest(
            args.memgraph, tester_binary, args.raft_config_file, cluster_size)
        print("\033[1;32m~~ The test finished successfully ~~\033[0m")

    sys.exit(0)
