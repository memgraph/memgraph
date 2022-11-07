# Copyright 2022 Memgraph Ltd.
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
from pathlib import Path


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
    def __init__(self, neo4j_path, temporary_dir, bolt_port):
        self._neo4j_path = Path(neo4j_path)
        self._neo4j_binary = Path(neo4j_path) / "bin" / "neo4j"
        self._neo4j_config = Path(neo4j_path) / "conf" / "neo4j.conf"
        if not self._neo4j_binary.is_file():
            raise Exception("Wrong path to binary!")
        self._directory = tempfile.TemporaryDirectory(dir=temporary_dir)
        self._proc_neo4j = None
        self._bolt_port = bolt_port
        atexit.register(self._cleanup)
        config = "dbms.security.auth_enabled=false\n"
        print("Check config securuty flag:")
        with self._neo4j_config.open("a+") as file:
            lines = file.readlines()
            line_exist = False
            for line in lines:
                if config == line:
                    line_exist = True
                    print("Config line exist at line: " + str(lines.index(line)))
                    print("Line content: " + line)
                    file.close()
                    break
            if not line_exist:
                print("Setting config line: " + config)
                file.write(config)
                file.close()

    def __del__(self):
        self._cleanup()
        atexit.unregister(self._cleanup)

    def _start(self, **kwargs):
        if self._proc_neo4j is not None:
            raise Exception("The database process is already running!")
        args = _convert_args_to_flags(
            self._neo4j_binary, "start", "--verbose", **kwargs
        )
        self._proc_neo4j = subprocess.Popen(args, stdout=subprocess.PIPE)
        time.sleep(10)
        if self._proc_neo4j.poll() == 0:
            print("Neo4j started!")
        if self._proc_neo4j.poll() != 0:
            self._proc_neo4j = None
            raise Exception("The database process died prematurely!")
        wait_for_server(self._bolt_port)
        ret = self._proc_neo4j.poll()
        assert ret == 0, "The database process died prematurely " "({})!".format(ret)

    def _cleanup(self):
        pid_file = self._neo4j_path / "run" / "neo4j.pid"
        if pid_file.exists():
            pid = pid_file.read_text()
            print("Clean up: " + pid)
            usage = _get_usage(pid)
            args = list()
            args.append(self._neo4j_binary)
            args.append("stop")
            exit_proc = subprocess.run(args, capture_output=True, check=True)
            self._proc_neo4j = None
            return exit_proc.returncode, usage
        else:
            return 0

    def start_preparation(self):
        self._start()

    def start_benchmark(self):
        self._start()

    def is_stopped(self):
        pid_file = self._neo4j_path / "run" / "neo4j.pid"
        if pid_file.exists():
            return False
        else:
            return True

    def stop(self):
        ret, usage = self._cleanup()
        assert ret == 0, "The database process exited with a non-zero " "status ({})!".format(ret)
        return usage


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
        ret = subprocess.run(args, capture_output=True, check=True)
        data = ret.stdout.decode("utf-8").strip().split("\n")
        # data = [x for x in data if not x.startswith("[")]
        return list(map(json.loads, data))
