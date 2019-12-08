#!/usr/bin/python3

import json
import os
import subprocess
import tempfile
import time


class HaTestBase:
    def __init__(self, memgraph_binary, tester_binary, raft_config_file,
            cluster_size):

        self.workers = [None for worker in range(cluster_size)]
        self.memgraph_binary = memgraph_binary
        self.tester_binary = tester_binary
        self.raft_config_file = raft_config_file
        self.cluster_size = cluster_size

        # Get a temporary directory used for durability.
        self._tempdir = tempfile.TemporaryDirectory()

        # generate coordination config file
        self.coordination_config_file = tempfile.NamedTemporaryFile()
        coordination = self._generate_json_coordination_config()
        self.coordination_config_file.write(bytes(coordination, "UTF-8"))
        self.coordination_config_file.flush()

        self.execute()


    def __del__(self):
        self.destroy_cluster()


    def start_cluster(self):
        for worker_id in range(self.cluster_size):
            self.start_worker(worker_id)

        # allow some time for leader election
        time.sleep(5)


    def destroy_cluster(self):
        for worker in self.workers:
            if worker is None: continue
            worker.kill()
            worker.wait()
        self.workers.clear()
        self.coordination_config_file.close()


    def kill_worker(self, worker_id):
        assert worker_id >= 0 and worker_id < self.cluster_size, \
                "Invalid worker ID {}".format(worker_id)
        assert self.workers[worker_id] is not None, \
                "Worker {} doesn't exists".format(worker_id)

        self.workers[worker_id].kill()
        self.workers[worker_id].wait()
        self.workers[worker_id] = None


    def start_worker(self, worker_id):
        assert worker_id >= 0 and worker_id < self.cluster_size, \
                "Invalid worker ID {}".format(worker_id)
        assert self.workers[worker_id] is None, \
                "Worker already exists".format(worker_id)

        self.workers[worker_id] = subprocess.Popen(self._generate_args(worker_id))

        time.sleep(0.2)
        assert self.workers[worker_id].poll() is None, \
                "Worker{} process died prematurely!".format(worker_id)

        self._wait_for_server(7687 + worker_id)


    def is_worker_alive(self, worker_id):
        assert worker_id >= 0 and worker_id < self.cluster_size, \
                "Invalid worker ID {}".format(worker_id)
        return self.workers[worker_id] is None or \
                self.workers[worker_id].poll() is None


    def execute(self):
        raise NotImplementedError()


    def _wait_for_server(self, port, delay=0.1):
        cmd = ["nc", "-z", "-w", "1", "127.0.0.1", str(port)]
        while subprocess.call(cmd) != 0:
            time.sleep(delay)
        time.sleep(delay)


    def get_durability_directory(self, worker_id):
        return os.path.join(self._tempdir.name, "worker" + str(worker_id))


    def _generate_args(self, worker_id):
        args = [self.memgraph_binary]
        args.extend(["--server_id", str(worker_id + 1)])
        args.extend(["--bolt-port", str(7687 + worker_id)])
        args.extend(["--raft_config_file", self.raft_config_file])
        args.extend(["--coordination_config_file",
            self.coordination_config_file.name])

        # Each worker must have a unique durability directory.
        args.extend(["--durability_directory",
            self.get_durability_directory(worker_id)])
        return args


    def _generate_json_coordination_config(self):
        data = []
        for i in range(self.cluster_size):
            data.append([i + 1, "127.0.0.1", 10000 + i])
        return json.dumps(data)

