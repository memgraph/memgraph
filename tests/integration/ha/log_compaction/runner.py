#!/usr/bin/python3

import argparse
import hashlib
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


def shasum(filename):
    with open(filename, 'rb') as f:
        return hashlib.sha1(f.read()).hexdigest()

    raise Exception("Couldn't get shasum for file {}".format(filename))


class HaLogCompactionTest(HaTestBase):
    def execute_step(self, query):
        client = subprocess.Popen(
                [self.tester_binary, "--cluster_size", str(self.cluster_size)],
                stdin=subprocess.PIPE, stdout=subprocess.DEVNULL)

        try:
            client.communicate(input=bytes(query, "UTF-8"), timeout=30)
        except subprocess.TimeoutExpired as e:
            client.kill()
            client.communicate()
            return 1

        return client.returncode


    def get_snapshot_path(self, worker_id):
        dur = os.path.join(self.get_durability_directory(worker_id), "snapshots")
        snapshots = os.listdir(dur)

        assert len(snapshots) == 1, \
                "More than one snapshot on worker {}!".format(worker_id + 1)
        return os.path.join(dur, snapshots[0])


    def execute(self):
        # custom cluster startup
        for worker_id in range(1, self.cluster_size):
            self.start_worker(worker_id)

        time.sleep(5)
        assert self.execute_step("CREATE (:Node)\n" * 128) == 0, \
                "Error while executing create query"

        self.start_worker(0)

        # allow some time for the snapshot transfer
        time.sleep(5)
        snapshot_shasum = shasum(self.get_snapshot_path(0))

        success = False
        for worker_id in range(1, self.cluster_size):
            if shasum(self.get_snapshot_path(worker_id)) == snapshot_shasum:
                success = True
                break

        # Check if the cluster is alive
        for worker_id in range(self.cluster_size):
            assert self.is_worker_alive(worker_id), \
                    "Worker {} died prematurely".format(worker_id + 1)

        assert success, "Snapshot didn't transfer successfully"


def find_correct_path(path):
    f = os.path.join(PROJECT_DIR, "build", path)
    if not os.path.exists(f):
        f = os.path.join(PROJECT_DIR, "build_debug", path)
    return f


if __name__ == "__main__":
    memgraph_binary = find_correct_path("memgraph_ha")
    tester_binary = find_correct_path(os.path.join("tests", "manual",
        "ha_client"))

    raft_config_file = os.path.join(PROJECT_DIR, "tests", "integration", "ha",
        "log_compaction", "raft.json")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--raft_config_file", default=raft_config_file)
    args = parser.parse_args()

    for cluster_size in [3, 5]:
        print("\033[1;36m~~ Executing test with cluster size: %d ~~\033[0m" % (cluster_size))
        HaLogCompactionTest(
            args.memgraph, tester_binary, args.raft_config_file, cluster_size)
        print("\033[1;32m~~ The test finished successfully ~~\033[0m")

    sys.exit(0)
