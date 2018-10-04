#!/usr/bin/python3
import argparse
import atexit
import json
import os
import subprocess
import tempfile
import time
import sys

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", ".."))

workers = []


@atexit.register
def cleanup():
    for worker in workers:
        worker.kill()
        worker.wait()
    workers.clear()


def wait_for_server(port, delay=0.1):
    cmd = ["nc", "-z", "-w", "1", "127.0.0.1", str(port)]
    while subprocess.call(cmd) != 0:
        time.sleep(0.01)
    time.sleep(delay)


def generate_args(memgraph_binary, temporary_dir, worker_id):
    args = [memgraph_binary]
    if worker_id == 0:
        args.append("--master")
    else:
        args.extend(["--worker", "--worker-id", str(worker_id)])
    args.extend(["--master-host", "127.0.0.1", "--master-port", "10000"])
    if worker_id != 0:
        args.extend(["--worker-host", "127.0.0.1", "--worker-port",
                     str(10000 + worker_id)])
    # All garbage collectors must be set to their lowest intervals to assure
    # that they won't terminate the memgraph process when communication between
    # the cluster fails.
    args.extend(["--skiplist-gc-interval", "1", "--gc-cycle-sec", "1"])
    # Each worker must have a unique durability directory.
    args.extend(["--durability-directory",
                 os.path.join(temporary_dir, "worker" + str(worker_id))])
    return args


def worker_id_to_name(worker_id):
    if worker_id == 0:
        return "master"
    return "worker {}".format(worker_id)


def execute_test(memgraph_binary, tester_binary, cluster_size, disaster,
                 on_worker_id, execute_query):
    args = {"cluster_size": cluster_size, "disaster": disaster,
            "on_worker_id": on_worker_id, "execute_query": execute_query}
    print("\033[1;36m~~ Executing test with arguments:",
          json.dumps(args, sort_keys=True), "~~\033[0m")

    # Get a temporary directory used for durability.
    tempdir = tempfile.TemporaryDirectory()

    # Start the cluster.
    cleanup()
    for worker_id in range(cluster_size):
        workers.append(subprocess.Popen(
            generate_args(memgraph_binary, tempdir.name, worker_id)))
        time.sleep(0.2)
        assert workers[worker_id].poll() is None, \
            "The {} process died prematurely!".format(
            worker_id_to_name(worker_id))
        if worker_id == 0:
            wait_for_server(10000)

    # Wait for the cluster to startup.
    wait_for_server(7687)

    # Execute the query if required.
    if execute_query:
        time.sleep(1)
        client = subprocess.Popen([tester_binary])

    # Perform the disaster.
    time.sleep(2)
    if disaster == "terminate":
        workers[on_worker_id].terminate()
    else:
        workers[on_worker_id].kill()
    time.sleep(2)

    # Array of exit codes.
    codes = []

    # Check what happened with query execution.
    if execute_query:
        try:
            code = client.wait(timeout=30)
        except subprocess.TimeoutExpired as e:
            client.kill()
            raise e
        if code != 0:
            print("The client process didn't exit cleanly!")
            codes.append(code)

    # Terminate the master and wait to see what happens with the cluster.
    workers[0].terminate()

    # Wait for all of the workers.
    for worker_id in range(cluster_size):
        code = workers[worker_id].wait(timeout=30)
        if worker_id == on_worker_id and disaster == "kill":
            if code == 0:
                print("The", worker_id_to_name(worker_id),
                      "process should have died but it exited cleanly!")
                codes.append(-1)
        elif code != 0:
            print("The", worker_id_to_name(worker_id),
                  "process didn't exit cleanly!")
            codes.append(code)

    assert not any(codes), "Something went wrong!"


if __name__ == "__main__":
    memgraph_binary = os.path.join(PROJECT_DIR, "build",
                                   "memgraph_distributed")
    if not os.path.exists(memgraph_binary):
        memgraph_binary = os.path.join(PROJECT_DIR, "build_debug",
                                       "memgraph_distributed")
    tester_binary = os.path.join(PROJECT_DIR, "build", "tests",
                                 "integration", "distributed", "tester")
    if not os.path.exists(tester_binary):
        tester_binary = os.path.join(PROJECT_DIR, "build_debug", "tests",
                                     "integration", "distributed", "tester")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--tester", default=tester_binary)
    args = parser.parse_args()

    for cluster_size in [3, 5]:
        for worker_id in [0, 1]:
            for disaster in ["terminate", "kill"]:
                for execute_query in [False, True]:
                    execute_test(args.memgraph, args.tester, cluster_size,
                                 disaster, worker_id, execute_query)

    print("\033[1;32m~~ The test finished successfully ~~\033[0m")
    sys.exit(0)
