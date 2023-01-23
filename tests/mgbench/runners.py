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
import threading
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


def _get_current_usage(pid):
    rss = 0
    with open("/proc/{}/status".format(pid)) as f:
        for row in f:
            tmp = row.split()
            if tmp[0] == "VmRSS:":
                rss = int(tmp[1])
    return rss / 1024


class Memgraph:
    def __init__(
        self, memgraph_binary, temporary_dir, properties_on_edges, bolt_port, performance_tracking, extra_args
    ):
        self._memgraph_binary = memgraph_binary
        self._directory = tempfile.TemporaryDirectory(dir=temporary_dir)
        self._properties_on_edges = properties_on_edges
        self._proc_mg = None
        self._bolt_port = bolt_port
        self.performance_tracking = performance_tracking
        self._stop_event = threading.Event()
        self._rss = []
        self._extra_args = extra_args
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
        if self._memgraph_version >= (0, 50, 0):
            kwargs["data_directory"] = data_directory
        else:
            kwargs["durability_directory"] = data_directory
        if self._memgraph_version >= (0, 50, 0):
            kwargs["storage_properties_on_edges"] = self._properties_on_edges
        else:
            assert self._properties_on_edges, "Older versions of Memgraph can't disable properties on edges!"

        if self._extra_args != "":
            args_list = self._extra_args.split(" ")
            assert len(args_list) % 2 == 0
            for i in range(0, len(args_list), 2):
                kwargs[args_list[i]] = args_list[i + 1]

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
        wait_for_server(7687)
        # TODO(gitbuda): Add better logging
        print("Memgraph is running...")
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

    def start_preparation(self, workload):
        if self.performance_tracking:
            p = threading.Thread(target=self.res_background_tracking, args=(self._rss, self._stop_event))
            self._stop_event.clear()
            self._rss.clear()
            p.start()
        self._start(storage_snapshot_on_exit=True)

    def start_benchmark(self, workload):
        if self.performance_tracking:
            p = threading.Thread(target=self.res_background_tracking, args=(self._rss, self._stop_event))
            self._stop_event.clear()
            self._rss.clear()
            p.start()
        self._start(storage_recover_on_startup=True)

    def res_background_tracking(self, res, stop_event):
        print("Started rss tracking.")
        while not stop_event.is_set():
            if self._proc_mg != None:
                self._rss.append(_get_current_usage(self._proc_mg.pid))
            time.sleep(0.05)
        print("Stopped rss tracking. ")

    def dump_rss(self, workload):
        file_name = workload + "_rss"
        Path.mkdir(Path().cwd() / "memgraph_memory", exist_ok=True)
        file = Path(Path().cwd() / "memgraph_memory" / file_name)
        file.touch()
        with file.open("r+") as f:
            for rss in self._rss:
                f.write(str(rss))
                f.write("\n")
            f.close()

    def stop(self, workload):
        if self.performance_tracking:
            self._stop_event.set()
            self.dump_rss(workload)
        ret, usage = self._cleanup()
        assert ret == 0, "The database process exited with a non-zero " "status ({})!".format(ret)
        return usage


class Neo4j:
    def __init__(self, neo4j_path, temporary_dir, bolt_port, performance_tracking):
        self._neo4j_path = Path(neo4j_path)
        self._neo4j_binary = Path(neo4j_path) / "bin" / "neo4j"
        self._neo4j_config = Path(neo4j_path) / "conf" / "neo4j.conf"
        self._neo4j_pid = Path(neo4j_path) / "run" / "neo4j.pid"
        self._neo4j_admin = Path(neo4j_path) / "bin" / "neo4j-admin"
        self.performance_tracking = performance_tracking
        self._stop_event = threading.Event()
        self._rss = []

        if not self._neo4j_binary.is_file():
            raise Exception("Wrong path to binary!")
        self._directory = tempfile.TemporaryDirectory(dir=temporary_dir)
        self._bolt_port = bolt_port
        atexit.register(self._cleanup)
        configs = []
        memory_flag = "server.jvm.additional=-XX:NativeMemoryTracking=detail"
        auth_flag = "dbms.security.auth_enabled=false"

        if self.performance_tracking:
            configs.append(memory_flag)
        else:
            lines = []
            with self._neo4j_config.open("r") as file:
                lines = file.readlines()
                file.close()

            for i in range(0, len(lines)):
                if lines[i].strip("\n") == memory_flag:
                    print("Clear up config flag:  " + memory_flag)
                    lines[i] = "\n"
                    print(lines[i])

            with self._neo4j_config.open("w") as file:
                file.writelines(lines)
                file.close()

        configs.append(auth_flag)
        print("Check neo4j config flags:")
        for conf in configs:
            with self._neo4j_config.open("r+") as file:
                lines = file.readlines()
                line_exist = False
                for line in lines:
                    if conf == line.rstrip():
                        line_exist = True
                        print("Config line exist at line: " + str(lines.index(line)))
                        print("Line content: " + line)
                        file.close()
                        break
                if not line_exist:
                    print("Setting config line: " + conf)
                    file.write(conf)
                    file.write("\n")
                    file.close()

    def __del__(self):
        self._cleanup()
        atexit.unregister(self._cleanup)

    def _start(self, **kwargs):
        if self._neo4j_pid.exists():
            raise Exception("The database process is already running!")
        args = _convert_args_to_flags(self._neo4j_binary, "start", **kwargs)
        start_proc = subprocess.run(args, check=True)
        time.sleep(5)
        if self._neo4j_pid.exists():
            print("Neo4j started!")
        else:
            raise Exception("The database process died prematurely!")
        print("Run server check:")
        wait_for_server(self._bolt_port)

    def _cleanup(self):
        if self._neo4j_pid.exists():
            pid = self._neo4j_pid.read_text()
            print("Clean up: " + pid)
            usage = _get_usage(pid)

            exit_proc = subprocess.run(args=[self._neo4j_binary, "stop"], capture_output=True, check=True)
            return exit_proc.returncode, usage
        else:
            return 0

    def start_preparation(self, workload):
        if self.performance_tracking:
            p = threading.Thread(target=self.res_background_tracking, args=(self._rss, self._stop_event))
            self._stop_event.clear()
            self._rss.clear()
            p.start()

        # Start DB
        self._start()

        if self.performance_tracking:
            self.get_memory_usage("start_" + workload)

    def start_benchmark(self, workload):
        if self.performance_tracking:
            p = threading.Thread(target=self.res_background_tracking, args=(self._rss, self._stop_event))
            self._stop_event.clear()
            self._rss.clear()
            p.start()
        # Start DB
        self._start()

        if self.performance_tracking:
            self.get_memory_usage("start_" + workload)

    def dump_db(self, path):
        print("Dumping the neo4j database...")
        if self._neo4j_pid.exists():
            raise Exception("Cannot dump DB because it is running.")
        else:
            subprocess.run(
                args=[
                    self._neo4j_admin,
                    "database",
                    "dump",
                    "--overwrite-destination=false",
                    "--to-path",
                    path,
                    "neo4j",
                ],
                check=True,
            )

    def load_db_from_dump(self, path):
        print("Loading the neo4j database from dump...")
        if self._neo4j_pid.exists():
            raise Exception("Cannot dump DB because it is running.")
        else:
            subprocess.run(
                args=[
                    self._neo4j_admin,
                    "database",
                    "load",
                    "--from-path=" + path,
                    "--overwrite-destination=true",
                    "neo4j",
                ],
                check=True,
            )

    def res_background_tracking(self, res, stop_event):
        print("Started rss tracking.")
        while not stop_event.is_set():
            if self._neo4j_pid.exists():
                pid = self._neo4j_pid.read_text()
                self._rss.append(_get_current_usage(pid))
            time.sleep(0.05)
        print("Stopped rss tracking. ")

    def is_stopped(self):
        pid_file = self._neo4j_path / "run" / "neo4j.pid"
        if pid_file.exists():

            return False
        else:
            return True

    def stop(self, workload):
        if self.performance_tracking:
            self._stop_event.set()
            self.get_memory_usage("stop_" + workload)
            self.dump_rss(workload)
        ret, usage = self._cleanup()
        assert ret == 0, "The database process exited with a non-zero " "status ({})!".format(ret)
        return usage

    def dump_rss(self, workload):
        file_name = workload + "_rss"
        Path.mkdir(Path().cwd() / "neo4j_memory", exist_ok=True)
        file = Path(Path().cwd() / "neo4j_memory" / file_name)
        file.touch()
        with file.open("r+") as f:
            for rss in self._rss:
                f.write(str(rss))
                f.write("\n")
            f.close()

    def get_memory_usage(self, workload):
        Path.mkdir(Path().cwd() / "neo4j_memory", exist_ok=True)

        pid = self._neo4j_pid.read_text()
        memory_usage = subprocess.run(args=["jcmd", pid, "VM.native_memory"], capture_output=True, text=True)
        file = Path(Path().cwd() / "neo4j_memory" / workload)
        if file.exists():
            with file.open("r+") as f:
                f.write(memory_usage.stdout)
                f.close()
        else:
            file.touch()
            with file.open("r+") as f:
                f.write(memory_usage.stdout)
                f.close()


class Client:
    def __init__(self, client_binary, temporary_directory, bolt_port: int, username: str = "", password: str = ""):
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
        data = [x for x in data if not x.startswith("[")]
        return list(map(json.loads, data))
