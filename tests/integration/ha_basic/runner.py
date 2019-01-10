#!/usr/bin/python3
import argparse
import atexit
import json
import os
import subprocess
import tempfile
import time
import sys
import random

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", ".."))

workers = []


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


def generate_args(memgraph_binary, temporary_dir, worker_id, raft_config_file,
        coordination_config_file):
    args = [memgraph_binary]
    args.extend(["--server_id", str(worker_id + 1), "--port", str(7687 + worker_id)])
    args.extend(["--raft_config_file", raft_config_file])
    args.extend(["--coordination_config_file", coordination_config_file])

    # Each worker must have a unique durability directory.
    args.extend(["--durability_directory",
                 os.path.join(temporary_dir, "worker" + str(worker_id))])
    return args


def execute_step(tester_binary, cluster_size, expected_results, step):
    if step == "create":
        client = subprocess.Popen([tester_binary, "--step", "create",
            "--cluster_size", str(cluster_size)])

    elif step == "count":
        client = subprocess.Popen([tester_binary, "--step", "count",
            "--cluster_size", str(cluster_size), "--expected_results",
            str(expected_results)])
    else:
        return 0

    # Check what happened with query execution.
    try:
        code = client.wait(timeout=30)
    except subprocess.TimeoutExpired as e:
        client.kill()
        raise e

    return code


def execute_test(memgraph_binary, tester_binary, raft_config_file,
        coordination_config_file, cluster_size):

    print("\033[1;36m~~ Executing test with cluster size: %d~~\033[0m" % (cluster_size))
    global workers

    def kill_worker(worker_id):
        print("Killing worker %d" % (worker_id + 1))
        global workers
        workers[worker_id].kill()
        workers[worker_id].wait()

    def start_worker(worker_id):
        print("Starting worker %d" % (worker_id + 1))
        workers[worker_id] = subprocess.Popen(generate_args(memgraph_binary,
            tempdir.name, worker_id, raft_config_file,
            coordination_config_file))
        time.sleep(0.2)
        assert workers[worker_id].poll() is None, \
                "Worker{} process died prematurely!".format(worker_id)
        wait_for_server(7687 + worker_id)

    # Get a temporary directory used for durability.
    tempdir = tempfile.TemporaryDirectory()

    # Start the cluster.
    cleanup()
    for worker_id in range(cluster_size):
        workers.append(subprocess.Popen(generate_args(memgraph_binary,
            tempdir.name, worker_id, raft_config_file,
            coordination_config_file)))

        time.sleep(0.2)
        assert workers[worker_id].poll() is None, \
            "Worker{} process died prematurely!".format(worker_id)

    # Wait for the cluster to start.
    for worker_id in range(cluster_size):
        wait_for_server(7687 + worker_id)

    time.sleep(1)
    expected_results = 0

    # Array of exit codes.
    codes = []

    code = execute_step(tester_binary, cluster_size, expected_results, "create")
    if code == 0:
        expected_results += 1
    else:
        print("The client process didn't exit cleanly (code %d)!" % code)
        codes.append(code)

    for i in range(2 * cluster_size):
        partition = random.sample(range(cluster_size), int((cluster_size - 1) / 2))
        # kill workers
        for worker_id in partition:
            kill_worker(worker_id)

        time.sleep(2) # allow some time for possible leader re-election

        if random.random() < 0.5:
            print("Executing Create query")
            code = execute_step(tester_binary, cluster_size, expected_results,
                    "create")
            if code == 0:
                expected_results += 1
            else:
                print("The client process didn't exit cleanly (code %d)!" % code)
                codes.append(code)
                break
        else:
            print("Executing Count query")
            code = execute_step(tester_binary, cluster_size, expected_results,
                    "count")
            if code != 0:
                print("The client process didn't exit cleanly (code %d)!" % code)
                codes.append(code)
                break

        # Bring it back to life.
        for worker_id in partition:
            start_worker(worker_id)

    code = execute_step(tester_binary, cluster_size, expected_results, "count")
    if code != 0:
        print("The client process didn't exit cleanly (code %d)!" % code)
        codes.append(code)

    # Terminate all workers.
    cleanup()
    assert not any(codes), "Something went wrong!"


def find_correct_path(path):
    f = os.path.join(PROJECT_DIR, "build", path)
    if not os.path.exists(f):
        f = os.path.join(PROJECT_DIR, "build_debug", path)
    return f


def generate_json_coordination_config(cluster_size):
    data = []
    for i in range(cluster_size):
        data.append([i + 1, "127.0.0.1", 10000 + i])
    return json.dumps(data)


if __name__ == "__main__":
    memgraph_binary = find_correct_path("memgraph_ha")
    tester_binary = find_correct_path(os.path.join("tests", "integration",
        "ha_basic", "tester"))

    raft_config_file = os.path.join(PROJECT_DIR, "tests", "integration",
            "ha_basic", "raft.json")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--tester", default=tester_binary)
    parser.add_argument("--raft_config_file", default=raft_config_file)
    args = parser.parse_args()

    for cluster_size in [3, 5]:
        tmpfile = tempfile.NamedTemporaryFile()
        coordination = generate_json_coordination_config(cluster_size)
        tmpfile.write(bytes(coordination, "UTF-8"))
        tmpfile.flush()

        execute_test(args.memgraph, args.tester, args.raft_config_file,
                tmpfile.name, cluster_size)

        tmpfile.close()

    print("\033[1;32m~~ The test finished successfully ~~\033[0m")
    sys.exit(0)
