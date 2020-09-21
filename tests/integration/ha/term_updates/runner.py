#!/usr/bin/python3

import argparse
import os
import time
import random
import subprocess
import shutil
import sys

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", "..", ".."))

# append parent directory
sys.path.append(os.path.join(SCRIPT_DIR, ".."))

from ha_test import HaTestBase


class HaTermUpdatesTest(HaTestBase):
    def execute_step(self, step, expected_results=None):
        if step == "create":
            print("Executing create query")
            client = subprocess.Popen([self.tester_binary, "--step", "create",
                "--cluster_size", str(self.cluster_size)])

        elif step == "count":
            print("Executing count query")
            client = subprocess.Popen([self.tester_binary, "--step", "count",
                "--cluster_size", str(self.cluster_size), "--expected_results",
                str(expected_results)])
        else:
            raise ValueError("Invalid step argument: " + step)

        # Check what happened with query execution.
        try:
            code = client.wait(timeout=30)
        except subprocess.TimeoutExpired as e:
            print("Client timed out!")
            client.kill()
            return False

        return code == 0

    def find_leader(self):
        client = subprocess.run([self.tester_binary,
                                 "--step", "find_leader",
                                 "--cluster_size", str(self.cluster_size)],
                                stdout=subprocess.PIPE, check=True)
        return int(client.stdout.decode('utf-8')) - 1

    def execute(self):
        self.start_cluster() # start a whole cluster from scratch
        leader_id = self.find_leader()
        follower_ids = list(set(range(self.cluster_size)) - {leader_id})

        # Kill all followers.
        for i in follower_ids:
            print("Killing worker {}".format(i))
            self.kill_worker(i)

        # Try to execute a 'CREATE (n)' query on the leader.
        # The query hangs because the leader doesn't have consensus.
        assert not self.execute_step("create"), \
                "Error - a non-majorty cluster managed to execute a query"

        # Start a follower to create consensus so that the create succeeds.
        print("Starting worker {}".format(follower_ids[0]))
        self.start_worker(follower_ids[0])
        self.find_leader() # wait for leader re-election
        assert self.execute_step("count", expected_results=1), \
                "Error while executing count query"

        # Kill the leader.
        print("Killing leader (machine {})".format(leader_id))
        self.kill_worker(leader_id)
        time.sleep(1)

        # Start the second follower to create a consensus with the first
        # follower so that the first follower may become the new leader.
        print("Starting worker {}".format(follower_ids[1]))
        self.start_worker(follower_ids[1])
        self.find_leader() # wait for leader re-election

        # Verify that the data is there -> connect to the new leader and execute
        # "MATCH (n) RETURN n" -> the data should be there.
        assert self.execute_step("count", expected_results=1), \
                "Error while executing count query"

        # Try to assemble the whole cluster again by returning the old leader
        # to the cluster as a fresh machine.
        # (start the machine with its durability directory previously removed)
        shutil.rmtree(self.get_durability_directory(leader_id))
        self.start_worker(leader_id)
        self.find_leader() # wait for leader re-election
        assert self.execute_step("count", expected_results=1), \
                "Error while executing count query"


def find_correct_path(path):
    return os.path.join(PROJECT_DIR, "build", path)


if __name__ == "__main__":
    memgraph_binary = find_correct_path("memgraph_ha")
    tester_binary = find_correct_path(os.path.join("tests", "integration", "ha",
        "term_updates", "tester"))

    raft_config_file = os.path.join(PROJECT_DIR, "tests", "integration", "ha",
        "term_updates", "raft.json")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--raft_config_file", default=raft_config_file)
    parser.add_argument("--username", default="")
    parser.add_argument("--password", default="")
    parser.add_argument("--encrypted", type=bool, default=False)
    parser.add_argument("--address", type=str,
                        default="bolt://127.0.0.1")
    parser.add_argument("--port", type=int,
                        default=7687)
    args = parser.parse_args()

    cluster_size = 3 # test only works for 3 nodes
    print("\033[1;36m~~ Executing test with cluster size: %d~~\033[0m"
          % (cluster_size))
    HaTermUpdatesTest(
        args.memgraph, tester_binary, args.raft_config_file, cluster_size)
    print("\033[1;32m~~ The test finished successfully ~~\033[0m")

    sys.exit(0)
