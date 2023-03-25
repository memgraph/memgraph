# Copyright 2023 Memgraph Ltd.
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
from abc import ABC, abstractmethod
from pathlib import Path

import log
from benchmark_context import BenchmarkContext


def _wait_for_server(port, ip="127.0.0.1", delay=0.1):
    cmd = ["nc", "-z", "-w", "1", ip, str(port)]
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


class BaseClient(ABC):
    @abstractmethod
    def __init__(self, benchmark_context: BenchmarkContext):
        self.benchmark_context = benchmark_context

    @abstractmethod
    def execute(self):
        pass


class BoltClient(BaseClient):
    def __init__(self, benchmark_context: BenchmarkContext):
        self._client_binary = benchmark_context.client_binary
        self._directory = tempfile.TemporaryDirectory(dir=benchmark_context.temporary_directory)
        self._username = ""
        self._password = ""
        self._bolt_port = (
            benchmark_context.vendor_args["bolt-port"] if "bolt-port" in benchmark_context.vendor_args.keys() else 7687
        )

    def _get_args(self, **kwargs):
        return _convert_args_to_flags(self._client_binary, **kwargs)

    def set_credentials(self, username: str, password: str):
        self._username = username
        self._password = password

    def execute(
        self,
        queries=None,
        file_path=None,
        num_workers=1,
        max_retries: int = 50,
        validation: bool = False,
        time_dependent_execution: int = 0,
    ):
        if (queries is None and file_path is None) or (queries is not None and file_path is not None):
            raise ValueError("Either queries or input_path must be specified!")

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
            max_retries=max_retries,
            queries_json=queries_json,
            username=self._username,
            password=self._password,
            port=self._bolt_port,
            validation=validation,
            time_dependent_execution=time_dependent_execution,
        )

        ret = None
        try:
            ret = subprocess.run(args, capture_output=True)
        finally:
            error = ret.stderr.decode("utf-8").strip().split("\n")
            data = ret.stdout.decode("utf-8").strip().split("\n")
            if error and error[0] != "":
                log.warning("Reported errors from client:")
                log.warning("There is a possibility that query from: {} is not executed properly".format(file_path))
                log.error(error)
                log.error("Results for this query or benchmark run are probably invalid!")
            data = [x for x in data if not x.startswith("[")]
            return list(map(json.loads, data))


class BaseRunner(ABC):
    subclasses = {}

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        cls.subclasses[cls.__name__.lower()] = cls
        return

    @classmethod
    def create(cls, benchmark_context: BenchmarkContext):
        if benchmark_context.vendor_name not in cls.subclasses:
            raise ValueError("Missing runner with name: {}".format(benchmark_context.vendor_name))

        return cls.subclasses[benchmark_context.vendor_name](
            benchmark_context=benchmark_context,
        )

    @abstractmethod
    def __init__(self, benchmark_context: BenchmarkContext):
        self.benchmark_context = benchmark_context

    @abstractmethod
    def start_db_init(self):
        pass

    @abstractmethod
    def stop_db_init(self):
        pass

    @abstractmethod
    def start_db(self):
        pass

    @abstractmethod
    def stop_db(self):
        pass

    @abstractmethod
    def clean_db(self):
        pass

    @abstractmethod
    def fetch_client(self) -> BaseClient:
        pass


class Memgraph(BaseRunner):
    def __init__(self, benchmark_context: BenchmarkContext):
        super().__init__(benchmark_context=benchmark_context)
        self._memgraph_binary = benchmark_context.vendor_binary
        self._performance_tracking = benchmark_context.performance_tracking
        self._directory = tempfile.TemporaryDirectory(dir=benchmark_context.temporary_directory)
        self._vendor_args = benchmark_context.vendor_args
        self._properties_on_edges = (
            self._vendor_args["no-properties-on-edges"]
            if "no-properties-on-edges" in self._vendor_args.keys()
            else False
        )
        self._bolt_port = self._vendor_args["bolt-port"] if "bolt-port" in self._vendor_args.keys() else 7687
        self._proc_mg = None
        self._stop_event = threading.Event()
        self._rss = []

        # Determine Memgraph version
        ret = subprocess.run([self._memgraph_binary, "--version"], stdout=subprocess.PIPE, check=True)
        version = re.search(r"[0-9]+\.[0-9]+\.[0-9]+", ret.stdout.decode("utf-8")).group(0)
        self._memgraph_version = tuple(map(int, version.split(".")))

        atexit.register(self._cleanup)

    def __del__(self):
        self._cleanup()
        atexit.unregister(self._cleanup)

    def _set_args(self, **kwargs):
        data_directory = os.path.join(self._directory.name, "memgraph")
        kwargs["bolt_port"] = self._bolt_port
        kwargs["data_directory"] = data_directory
        kwargs["storage_properties_on_edges"] = self._properties_on_edges
        return _convert_args_to_flags(self._memgraph_binary, **kwargs)

    def _start(self, **kwargs):
        if self._proc_mg is not None:
            raise Exception("The database process is already running!")
        args = self._set_args(**kwargs)
        self._proc_mg = subprocess.Popen(args, stdout=subprocess.DEVNULL)
        time.sleep(0.2)
        if self._proc_mg.poll() is not None:
            self._proc_mg = None
            raise Exception("The database process died prematurely!")
        _wait_for_server(self._bolt_port)
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

    def start_db_init(self, workload):
        if self._performance_tracking:
            p = threading.Thread(target=self.res_background_tracking, args=(self._rss, self._stop_event))
            self._stop_event.clear()
            self._rss.clear()
            p.start()
        self._start(storage_snapshot_on_exit=True)

    def stop_db_init(self, workload):
        if self._performance_tracking:
            self._stop_event.set()
            self.dump_rss(workload)
        ret, usage = self._cleanup()
        assert ret == 0, "The database process exited with a non-zero " "status ({})!".format(ret)
        return usage

    def start_db(self, workload):
        if self._performance_tracking:
            p = threading.Thread(target=self.res_background_tracking, args=(self._rss, self._stop_event))
            self._stop_event.clear()
            self._rss.clear()
            p.start()
        self._start(storage_recover_on_startup=True)

    def stop_db(self, workload):
        if self._performance_tracking:
            self._stop_event.set()
            self.dump_rss(workload)
        ret, usage = self._cleanup()
        assert ret == 0, "The database process exited with a non-zero " "status ({})!".format(ret)
        return usage

    def clean_db(self):
        if self._proc_mg is not None:
            raise Exception("The database process is already running, cannot clear data it!")
        else:
            out = subprocess.run(
                args="rm -Rf memgraph/snapshots/*",
                cwd=self._directory.name,
                capture_output=True,
                shell=True,
            )
            print(out.stderr.decode("utf-8"))
            print(out.stdout.decode("utf-8"))

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

    def fetch_client(self) -> BoltClient:
        return BoltClient(benchmark_context=self.benchmark_context)


class Neo4j(BaseRunner):
    def __init__(self, benchmark_context: BenchmarkContext):
        super().__init__(benchmark_context=benchmark_context)
        self._neo4j_binary = Path(benchmark_context.vendor_binary)
        self._neo4j_path = Path(benchmark_context.vendor_binary).parents[1]
        self._neo4j_config = self._neo4j_path / "conf" / "neo4j.conf"
        self._neo4j_pid = self._neo4j_path / "run" / "neo4j.pid"
        self._neo4j_admin = self._neo4j_path / "bin" / "neo4j-admin"
        self._performance_tracking = benchmark_context.performance_tracking
        self._vendor_args = benchmark_context.vendor_args
        self._stop_event = threading.Event()
        self._rss = []

        if not self._neo4j_binary.is_file():
            raise Exception("Wrong path to binary!")

        tempfile.TemporaryDirectory(dir=benchmark_context.temporary_directory)
        self._bolt_port = (
            self.benchmark_context.vendor_args["bolt-port"]
            if "bolt-port" in self.benchmark_context.vendor_args.keys()
            else 7687
        )
        atexit.register(self._cleanup)
        configs = []
        memory_flag = "server.jvm.additional=-XX:NativeMemoryTracking=detail"
        auth_flag = "dbms.security.auth_enabled=false"
        bolt_flag = "server.bolt.listen_address=:7687"
        http_flag = "server.http.listen_address=:7474"
        if self._performance_tracking:
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
        configs.append(bolt_flag)
        configs.append(http_flag)
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
        _wait_for_server(self._bolt_port)

    def _cleanup(self):
        if self._neo4j_pid.exists():
            pid = self._neo4j_pid.read_text()
            print("Clean up: " + pid)
            usage = _get_usage(pid)

            exit_proc = subprocess.run(args=[self._neo4j_binary, "stop"], capture_output=True, check=True)
            return exit_proc.returncode, usage
        else:
            return 0

    def start_db_init(self, workload):
        if self._performance_tracking:
            p = threading.Thread(target=self.res_background_tracking, args=(self._rss, self._stop_event))
            self._stop_event.clear()
            self._rss.clear()
            p.start()

        # Start DB
        self._start()

        if self._performance_tracking:
            self.get_memory_usage("start_" + workload)

    def stop_db_init(self, workload):
        if self._performance_tracking:
            self._stop_event.set()
            self.get_memory_usage("stop_" + workload)
            self.dump_rss(workload)
        ret, usage = self._cleanup()
        assert ret == 0, "The database process exited with a non-zero " "status ({})!".format(ret)
        return usage

    def start_db(self, workload):
        if self._performance_tracking:
            p = threading.Thread(target=self.res_background_tracking, args=(self._rss, self._stop_event))
            self._stop_event.clear()
            self._rss.clear()
            p.start()
        # Start DB
        self._start()

        if self._performance_tracking:
            self.get_memory_usage("start_" + workload)

    def stop_db(self, workload):
        if self._performance_tracking:
            self._stop_event.set()
            self.get_memory_usage("stop_" + workload)
            self.dump_rss(workload)
        ret, usage = self._cleanup()
        assert ret == 0, "The database process exited with a non-zero " "status ({})!".format(ret)
        return usage

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

    def clean_db(self):
        print("Cleaning the database")
        if self._neo4j_pid.exists():
            raise Exception("Cannot clean DB because it is running.")
        else:
            out = subprocess.run(
                args="rm -Rf data/databases/* data/transactions/*",
                cwd=self._neo4j_path,
                capture_output=True,
                shell=True,
            )
            print(out.stderr.decode("utf-8"))
            print(out.stdout.decode("utf-8"))

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
                    "--from-path",
                    path,
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

    def fetch_client(self) -> BoltClient:
        return BoltClient(benchmark_context=self.benchmark_context)


class MemgraphDocker(BaseRunner):
    def __init__(self, benchmark_context: BenchmarkContext):
        super().__init__(benchmark_context=benchmark_context)
        self._directory = tempfile.TemporaryDirectory(dir=benchmark_context.temporary_directory)
        self._vendor_args = benchmark_context.vendor_args
        self._properties_on_edges = (
            self._vendor_args["no-properties-on-edges"]
            if "no-properties-on-edges" in self._vendor_args.keys()
            else False
        )
        self._bolt_port = self._vendor_args["bolt-port"] if "bolt-port" in self._vendor_args.keys() else 7687
        self._container_name = "memgraph_benchmark"
        self._container_ip = None
        self._config_file = None

    def start_db_init(self, message):
        command = [
            "docker",
            "run",
            "--detach",
            "--name",
            self._container_name,
            "-it",
            "-p",
            "7687:7687",
            "memgraph/memgraph",
            "--telemetry_enabled=false",
            "--storage_wal_enabled=false",
            "--storage_recover_on_startup=true",
            "--storage_snapshot_interval_sec=0",
        ]
        ret = self._run_command(command)

        command = [
            "docker",
            "cp",
            self._container_name + ":etc/memgraph/memgraph.conf",
            self._directory.name + "/memgraph.conf",
        ]
        self._run_command(command)
        self._config_file = Path(self._directory.name + "/memgraph.conf")

        command = ["docker", "inspect", "--format", "{{ .NetworkSettings.IPAddress }}", self._container_name]
        ret = subprocess.run(command, check=True, capture_output=True, text=True)
        ip_address = ret.stdout.strip("\n")
        _wait_for_server(self._bolt_port, ip=ip_address)

    def stop_db_init(self, message):
        # Stop to save the snapshot
        command = ["docker", "stop", self._container_name]
        self._run_command(command)

        # Change config back to default
        argument = "--storage-snapshot-on-exit=false"
        self._replace_config_args(argument)
        command = [
            "docker",
            "cp",
            self._config_file.resolve(),
            self._container_name + ":etc/memgraph/memgraph.conf",
        ]
        self._run_command(command)

        return {"cpu": 0, "memory": 0}

    def start_db(self, message):
        command = ["docker", "start", self._container_name]
        self._run_command(command)
        command = ["docker", "inspect", "--format", "{{ .NetworkSettings.IPAddress }}", self._container_name]
        ret = subprocess.run(command, check=True, capture_output=True, text=True)
        ip_address = ret.stdout.strip("\n")
        _wait_for_server(self._bolt_port, ip=ip_address)

    def stop_db(self, message):
        command = ["docker", "stop", self._container_name]
        self._run_command(command)
        return {"cpu": 0, "memory": 0}

    def clean_db(self):
        self.remove_container(self._container_name)

    def fetch_client(self) -> BaseClient:
        return BoltClient(benchmark_context=self.benchmark_context)

    def remove_container(self, containerName):
        command = ["docker", "rm", "-f", containerName]
        self._run_command(command)

    def _replace_config_args(self, argument):
        config_lines = []
        with self._config_file.open("r") as file:
            lines = file.readlines()
            file.close()
            key, value = argument.split("=")
            for line in lines:

                if line[0] == "#" or line.strip("\n") == "":
                    config_lines.append(line)
                else:
                    key_file, value_file = line.split("=")
                    if key_file == key and value != value_file:
                        print("replacing: " + key_file + value_file + "with " + argument)
                        line = argument + "\n"
                    config_lines.append(line)

        with self._config_file.open("w") as file:
            file.writelines(config_lines)
            file.close()

    def _run_command(self, command):
        print(command)
        ret = subprocess.run(command, text=True)

        time.sleep(1)
        return ret
