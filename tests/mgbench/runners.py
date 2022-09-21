# Copyright 2021 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import atexit
import json
import os
import re
import subprocess
import tempfile
import time


def wait_for_server(port, delay=0.1):
    cmd = ["nc", "-z", "-w", "1", "127.0.0.1", str(port)]
    while subprocess.call(cmd) != 0:
        time.sleep(0.01)
    time.sleep(delay)


def _convert_args_to_flags(*args, **kwargs):
    flags = list(args)
    for key, value in kwargs.items():
        key = "--" + key.replace("_", "-")
        if type(value) == bool:
            flags.append(key + "=" + str(value).lower())
        else:
            flags.append(key)
            flags.append(str(value))
    return flags


def _get_usage(pid):
    total_cpu = 0
    with open("/proc/{}/stat".format(pid)) as f:
        total_cpu = sum(map(int, f.read().split(")")[1].split()[11:15])) / os.sysconf(os.sysconf_names["SC_CLK_TCK"])
    peak_rss = 0
    with open("/proc/{}/status".format(pid)) as f:
        for row in f:
            tmp = row.split()
            if tmp[0] == "VmHWM:":
                peak_rss = int(tmp[1]) * 1024
    return {"cpu": total_cpu, "memory": peak_rss}


class Memgraph:
    def __init__(self, memgraph_binary, temporary_dir, properties_on_edges, bolt_port):
        self._memgraph_binary = memgraph_binary
        self._directory = tempfile.TemporaryDirectory(dir=temporary_dir)
        self._properties_on_edges = properties_on_edges
        self._proc_mg = None
        self._bolt_port = bolt_port
        atexit.register(self._cleanup)

        # Determine Memgraph version
        ret = subprocess.run([memgraph_binary, "--version"], stdout=subprocess.PIPE, check=True)
        version = re.search(r"[0-9]+\.[0-9]+\.[0-9]+", ret.stdout.decode("utf-8")).group(0)
        self._memgraph_version = tuple(map(int, version.split(".")))

    def __del__(self):
        self._cleanup()
        atexit.unregister(self._cleanup)

    def _get_args(self, **kwargs):
        data_directory = os.path.join(self._directory.name, "memgraph")
        kwargs["bolt_port"] = self._bolt_port
        if self._memgraph_version >= (0, 50, 0):
            kwargs["data_directory"] = data_directory
        else:
            kwargs["durability_directory"] = data_directory
        if self._memgraph_version >= (0, 50, 0):
            kwargs["storage_properties_on_edges"] = self._properties_on_edges
        else:
            assert self._properties_on_edges, "Older versions of Memgraph can't disable properties on edges!"
        return _convert_args_to_flags(self._memgraph_binary, **kwargs)

    def _start(self, **kwargs):
        if self._proc_mg is not None:
            raise Exception("The database process is already running!")
        args = self._get_args(**kwargs)
        self._proc_mg = subprocess.Popen(args, stdout=subprocess.DEVNULL)
        time.sleep(0.2)
        if self._proc_mg.poll() is not None:
            self._proc_mg = None
            raise Exception("The database process died prematurely!")
        wait_for_server(self._bolt_port)
        ret = self._proc_mg.poll()
        assert ret is None, "The database process died prematurely " "({})!".format(ret)

    def _cleanup(self):
        if self._proc_mg is None:
            return 0
        usage = _get_usage(self._proc_mg.pid)
        self._proc_mg.terminate()
        ret = self._proc_mg.wait()
        self._proc_mg = None
        return ret, usage

    def start_preparation(self):
        if self._memgraph_version >= (0, 50, 0):
            self._start(storage_snapshot_on_exit=True)
        else:
            self._start(snapshot_on_exit=True)

    def start_benchmark(self):
        # TODO: support custom benchmarking config files!
        if self._memgraph_version >= (0, 50, 0):
            self._start(storage_recover_on_startup=True)
        else:
            self._start(db_recover_on_startup=True)

    def stop(self):
        ret, usage = self._cleanup()
        assert ret == 0, "The database process exited with a non-zero " "status ({})!".format(ret)
        return usage


class Neo4j:
    def __init__(self, memgraph_binary, temporary_dir, properties_on_edges, bolt_port):
        self._memgraph_binary = memgraph_binary
        self._directory = tempfile.TemporaryDirectory(dir=temporary_dir)
        self._properties_on_edges = properties_on_edges
        self._proc_mg = None
        self._bolt_port = bolt_port
        atexit.register(self._cleanup)

        # Determine Memgraph version
        ret = subprocess.run([memgraph_binary, "--version"], stdout=subprocess.PIPE, check=True)
        version = re.search(r"[0-9]+\.[0-9]+\.[0-9]+", ret.stdout.decode("utf-8")).group(0)
        self._memgraph_version = tuple(map(int, version.split(".")))

    def __del__(self):
        self._cleanup()
        atexit.unregister(self._cleanup)

    def _get_args(self, **kwargs):
        data_directory = os.path.join(self._directory.name, "memgraph")
        kwargs["bolt_port"] = self._bolt_port
        if self._memgraph_version >= (0, 50, 0):
            kwargs["data_directory"] = data_directory
        else:
            kwargs["durability_directory"] = data_directory
        if self._memgraph_version >= (0, 50, 0):
            kwargs["storage_properties_on_edges"] = self._properties_on_edges
        else:
            assert self._properties_on_edges, "Older versions of Memgraph can't disable properties on edges!"
        return _convert_args_to_flags(self._memgraph_binary, **kwargs)

    def _start(self, **kwargs):
        if self._proc_mg is not None:
            raise Exception("The database process is already running!")
        args = self._get_args(**kwargs)
        self._proc_mg = subprocess.Popen(args, stdout=subprocess.DEVNULL)
        time.sleep(0.2)
        if self._proc_mg.poll() is not None:
            self._proc_mg = None
            raise Exception("The database process died prematurely!")
        wait_for_server(self._bolt_port)
        ret = self._proc_mg.poll()
        assert ret is None, "The database process died prematurely " "({})!".format(ret)

    def _cleanup(self):
        if self._proc_mg is None:
            return 0
        usage = _get_usage(self._proc_mg.pid)
        self._proc_mg.terminate()
        ret = self._proc_mg.wait()
        self._proc_mg = None
        return ret, usage

    def start_preparation(self):
        if self._memgraph_version >= (0, 50, 0):
            self._start(storage_snapshot_on_exit=True)
        else:
            self._start(snapshot_on_exit=True)

    def start_benchmark(self):
        # TODO: support custom benchmarking config files!
        if self._memgraph_version >= (0, 50, 0):
            self._start(storage_recover_on_startup=True)
        else:
            self._start(db_recover_on_startup=True)

    def stop(self):
        ret, usage = self._cleanup()
        assert ret == 0, "The database process exited with a non-zero " "status ({})!".format(ret)
        return usage


class Neo:
    """
    Knows how to start and stop neo4j.
    """

    def __init__(self, args, config):
        self.log = logging.getLogger("NeoRunner")
        argp = ArgumentParser("NeoArgumentParser")
        argp.add_argument("--runner-bin", default=get_absolute_path("neo4j/bin/neo4j", "libs"))
        argp.add_argument("--port", default="7687", help="Database and client port")
        argp.add_argument("--http-port", default="7474", help="Database and client port")
        self.log.info("Initializing Runner with arguments %r", args)
        self.args, _ = argp.parse_known_args(args)
        self.config = config
        self.database_bin = jail.get_process()
        self.name = "neo4j"
        set_cpus("database-cpu-ids", self.database_bin, args)

    def start(self):
        self.log.info("start")

        # create home directory
        self.neo4j_home_path = tempfile.mkdtemp(dir="/dev/shm")

        try:
            os.symlink(
                os.path.join(get_absolute_path("neo4j", "libs"), "lib"), os.path.join(self.neo4j_home_path, "lib")
            )
            neo4j_conf_dir = os.path.join(self.neo4j_home_path, "conf")
            neo4j_conf_file = os.path.join(neo4j_conf_dir, "neo4j.conf")
            os.mkdir(neo4j_conf_dir)
            shutil.copyfile(self.config, neo4j_conf_file)
            with open(neo4j_conf_file, "a") as f:
                f.write("\ndbms.connector.bolt.listen_address=:" + self.args.port + "\n")
                f.write("\ndbms.connector.http.listen_address=:" + self.args.http_port + "\n")

            # environment
            cwd = os.path.dirname(self.args.runner_bin)
            env = {"NEO4J_HOME": self.neo4j_home_path}

            self.database_bin.run(self.args.runner_bin, args=["console"], env=env, timeout=600, cwd=cwd)
        except:
            shutil.rmtree(self.neo4j_home_path)
            raise Exception("Couldn't run Neo4j!")

        wait_for_server(self.args.http_port, 2.0)

    def stop(self):
        self.database_bin.send_signal(jail.SIGTERM)
        self.database_bin.wait()
        if os.path.exists(self.neo4j_home_path):
            shutil.rmtree(self.neo4j_home_path)


class Client:
    def __init__(
        self, client_binary: str, temporary_directory: str, bolt_port: int, username: str = "", password: str = ""
    ):
        self._client_binary = client_binary
        self._directory = tempfile.TemporaryDirectory(dir=temporary_directory)
        self._username = username
        self._password = password
        self._bolt_port = bolt_port

    def _get_args(self, **kwargs):
        return _convert_args_to_flags(self._client_binary, **kwargs)

    def execute(self, queries=None, file_path=None, num_workers=1):
        if (queries is None and file_path is None) or (queries is not None and file_path is not None):
            raise ValueError("Either queries or input_path must be specified!")

        # TODO: check `file_path.endswith(".json")` to support advanced
        # input queries

        queries_json = False
        if queries is not None:
            queries_json = True
            file_path = os.path.join(self._directory.name, "queries.json")
            with open(file_path, "w") as f:
                for query in queries:
                    json.dump(query, f)
                    f.write("\n")

        args = self._get_args(
            input=file_path,
            num_workers=num_workers,
            queries_json=queries_json,
            username=self._username,
            password=self._password,
            port=self._bolt_port,
        )
        ret = subprocess.run(args, stdout=subprocess.PIPE, check=True)
        data = ret.stdout.decode("utf-8").strip().split("\n")
        # data = [x for x in data if not x.startswith("[")]
        return list(map(json.loads, data))
