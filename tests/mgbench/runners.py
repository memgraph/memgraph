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
import socket
import subprocess
import tempfile
import threading
import time
from abc import ABC, abstractmethod
from pathlib import Path

import log
from benchmark_context import BenchmarkContext
from constants import BenchmarkInstallationType, GraphVendors

DOCKER_NETWORK_NAME = "mgbench_network"


def _wait_for_server_socket(port, ip="127.0.0.1", delay=0.1):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    while s.connect_ex((ip, int(port))) != 0:
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


def _setup_docker_benchmark_network(network_name):
    command = ["docker", "network", "ls", "--format", "{{.Name}}"]
    networks = subprocess.run(command, check=True, capture_output=True, text=True).stdout.split("\n")
    if network_name in networks:
        return
    else:
        command = ["docker", "network", "create", network_name]
        subprocess.run(command, check=True, capture_output=True, text=True)


def _get_docker_container_ip(container_name):
    command = [
        "docker",
        "inspect",
        "--format",
        "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
        container_name,
    ]
    return subprocess.run(command, check=True, capture_output=True, text=True).stdout.strip()


def run_command(command):
    ret = subprocess.run(command, capture_output=True, check=True, text=True)
    time.sleep(0.2)
    return ret


def get_docker_cpu_usage(container_name):
    command = ["docker", "stats", container_name, "--no-stream", "--format", "{{.CPUPerc}}"]
    ret = run_command(command)
    if not ret:
        return 0

    cpu_perc = float(ret[0].strip("%")) / 100
    return cpu_perc


def get_docker_memory_usage(container_name):
    command = [
        "docker",
        "stats",
        "--no-stream",
        "--format",
        "{{.MemUsage}}",
        container_name,
    ]
    ret = run_command(command)

    # Example of ret.stdout = "79.52MiB / 58.56GiB"
    memory_usage = ret.stdout.split(" / ")

    if len(memory_usage) == 2:
        used_memory = memory_usage[0].strip()  # e.g., "79.52MiB"
        used_memory_value, used_memory_unit = re.findall(r"(\d+\.?\d*)([A-Za-z]+)", used_memory)[0]

        # Convert memory to bytes for consistency
        if used_memory_unit == "B":
            return int(float(used_memory_value))  # Bytes
        elif used_memory_unit == "KiB":
            return int(float(used_memory_value) * 1024)  # KiB to Bytes
        elif used_memory_unit == "MiB":
            return int(float(used_memory_value) * 1024 * 1024)  # MiB to Bytes
        elif used_memory_unit == "GiB":
            return int(float(used_memory_value) * 1024 * 1024 * 1024)  # GiB to Bytes
        elif used_memory_unit == "TiB":
            return int(float(used_memory_value) * 1024 * 1024 * 1024 * 1024)  # TiB to Bytes
        else:
            raise Exception(f"Unrecognized used memory: {used_memory}")
    else:
        raise Exception(f"Unrecognized memory usage: {memory_usage}")


class BaseClient(ABC):
    @abstractmethod
    def __init__(self, benchmark_context: BenchmarkContext):
        self.benchmark_context = benchmark_context
        self._vendor = benchmark_context.vendor_name

    @abstractmethod
    def execute(self):
        pass

    def get_check_db_query(self) -> str:
        match self._vendor:
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                return "RETURN 0;"
            case GraphVendors.POSTGRESQL:
                return "SELECT 1 AS result;"
            case _:
                raise Exception(f"Unknown vendor name {self._vendor} for sanity check query!")


class BoltClient(BaseClient):
    def __init__(self, benchmark_context: BenchmarkContext):
        super().__init__(benchmark_context=benchmark_context)
        self._client_binary = benchmark_context.client_binary
        self._directory = tempfile.TemporaryDirectory(dir=benchmark_context.temporary_directory)
        self._username = ""
        self._password = ""
        self._bolt_port = (
            benchmark_context.vendor_args["bolt-port"] if "bolt-port" in benchmark_context.vendor_args.keys() else 7687
        )
        self._bolt_address = benchmark_context.client_bolt_address
        self._databases = benchmark_context.databases

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
        check_db_query = Path(self._directory.name) / "check_db_query.json"
        with open(check_db_query, "w") as f:
            query = [self.get_check_db_query(), {}]
            json.dump(query, f)
            f.write("\n")

        client_args = self._get_args(
            input=check_db_query,
            num_workers=1,
            max_retries=max_retries,
            queries_json=True,
            username=self._username,
            password=self._password,
            port=self._bolt_port,
            address=self._bolt_address,
            validation=False,
            time_dependent_execution=time_dependent_execution,
            databases=self._databases,
        )

        log.info("Client args: {}".format(client_args))

        while True:
            try:
                subprocess.run(client_args, capture_output=True, text=True, check=True)
                break
            except subprocess.CalledProcessError as e:
                log.log("Checking if database is up and running failed...")
                log.warning("Reported errors from client:")
                log.warning("Error: {}".format(e.stderr))
                log.warning("Database is not up yet, waiting 3 seconds...")
                time.sleep(3)

        if (queries is None and file_path is None) or (queries is not None and file_path is not None):
            raise ValueError("Either queries or input_path must be specified!")

        queries_and_args_json = False
        if queries is not None:
            queries_and_args_json = True
            file_path = os.path.join(self._directory.name, "queries_and_args_json.json")
            with open(file_path, "w") as f:
                for query in queries:
                    json.dump(query, f)
                    f.write("\n")

        args = self._get_args(
            input=file_path,
            num_workers=num_workers,
            max_retries=max_retries,
            queries_json=queries_and_args_json,
            username=self._username,
            password=self._password,
            port=self._bolt_port,
            address=self._bolt_address,
            validation=validation,
            time_dependent_execution=time_dependent_execution,
            databases=self._databases,
        )

        log.info("Client args: {}".format(args))

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


class BoltClientDocker(BaseClient):
    def __init__(self, benchmark_context: BenchmarkContext):
        super().__init__(benchmark_context=benchmark_context)
        self._directory = tempfile.TemporaryDirectory(dir=benchmark_context.temporary_directory)
        self._username = ""
        self._password = ""
        self._bolt_port = (
            benchmark_context.vendor_args["bolt-port"] if "bolt-port" in benchmark_context.vendor_args.keys() else 7687
        )
        self._container_name = "mgbench-bolt-client"
        self._target_db_container = f"{benchmark_context.vendor_name}_benchmark"

    def _remove_container(self):
        command = ["docker", "rm", "-f", self._container_name]
        run_command(command)

    def _create_container(self, *args):
        command = [
            "docker",
            "create",
            "--name",
            self._container_name,
            "--network",
            DOCKER_NETWORK_NAME,
            "memgraph/mgbench-client",
            *args,
        ]
        run_command(command)

    def _get_logs(self):
        command = [
            "docker",
            "logs",
            self._container_name,
        ]
        ret = run_command(command)
        return ret

    def _get_args(self, **kwargs):
        return _convert_args_to_flags(**kwargs)

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

        self._remove_container()
        ip = _get_docker_container_ip(self._target_db_container)

        # Perform a check to make sure the database is up and running
        args = self._get_args(
            address=ip,
            input="/bin/check.json",
            num_workers=1,
            max_retries=max_retries,
            queries_json=True,
            username=self._username,
            password=self._password,
            port=self._bolt_port,
            validation=False,
            time_dependent_execution=0,
        )

        self._create_container(*args)

        check_file = Path(self._directory.name) / "check.json"
        with open(check_file, "w") as f:
            query = [self.get_check_db_query(), {}]
            json.dump(query, f)
            f.write("\n")

        command = [
            "docker",
            "cp",
            check_file.resolve().as_posix(),
            self._container_name + ":/bin/" + check_file.name,
        ]
        run_command(command)

        command = [
            "docker",
            "start",
            "-i",
            self._container_name,
        ]

        # Wait until the container is started
        time.sleep(2)

        while True:
            try:
                run_command(command)
                break
            except subprocess.CalledProcessError as e:
                log.log("Checking if database is up and running failed!")
                log.warning("Reported errors from client:")
                log.warning("Error: {}".format(e.stderr))
                log.warning("Database is not up yet, waiting 3 second")
                time.sleep(3)
                log.warning("Continuing execution...")

        self._remove_container()

        queries_and_args_json = False
        if queries is not None:
            queries_and_args_json = True
            file_path = os.path.join(self._directory.name, "queries.json")
            with open(file_path, "w") as f:
                for query in queries:
                    json.dump(query, f)
                    f.write("\n")

        self._remove_container()
        ip = _get_docker_container_ip(self._target_db_container)

        # Query file JSON or Cypher
        file = Path(file_path)

        args = self._get_args(
            address=ip,
            input="/bin/" + file.name,
            num_workers=num_workers,
            max_retries=max_retries,
            queries_json=queries_and_args_json,
            username=self._username,
            password=self._password,
            port=self._bolt_port,
            validation=validation,
            time_dependent_execution=time_dependent_execution,
        )

        self._create_container(*args)

        command = [
            "docker",
            "cp",
            file.resolve().as_posix(),
            self._container_name + ":/bin/" + file.name,
        ]
        run_command(command)
        log.log("Starting query execution...")
        try:
            command = [
                "docker",
                "start",
                "-i",
                self._container_name,
            ]
            run_command(command)
        except subprocess.CalledProcessError as e:
            log.warning("Reported errors from client:")
            log.warning("Error: {}".format(e.stderr))

        ret = self._get_logs()
        error = ret.stderr.strip().split("\n")
        if error and error[0] != "":
            log.warning("There is a possibility that query from: {} is not executed properly".format(file_path))
            log.warning(*error)
        data = ret.stdout.strip().split("\n")
        data = [x for x in data if not x.startswith("[")]
        return list(map(json.loads, data))


class PythonClient(BaseClient):
    def __init__(self, benchmark_context: BenchmarkContext, database_port: int):
        super().__init__(benchmark_context=benchmark_context)
        self._client_binary = os.path.join(os.path.dirname(os.path.abspath(__file__)), "python_client.py")
        self._directory = tempfile.TemporaryDirectory(dir=benchmark_context.temporary_directory)
        self._username = ""
        self._password = ""
        self._database_port = database_port

    def _get_args(self, **kwargs):
        return _convert_args_to_flags("python3", self._client_binary, **kwargs)

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
        check_db_query = Path(self._directory.name) / "check_db_query.json"
        with open(check_db_query, "w") as f:
            query = [self.get_check_db_query(), {}]
            json.dump(query, f)
            f.write("\n")

        check_db_args = self._get_args(
            vendor=self._vendor,
            input=check_db_query,
            num_workers=1,
            max_retries=max_retries,
            queries_json=True,
            username=self._username,
            password=self._password,
            port=self._database_port,
            validation=False,
            time_dependent_execution=time_dependent_execution,
        )

        while True:
            try:
                subprocess.run(check_db_args, capture_output=True, text=True, check=True)
                break
            except subprocess.CalledProcessError as e:
                log.log("Checking if database is up and running failed...")
                log.warning("Reported errors from client:")
                log.warning("Error: {}".format(e.stderr))
                log.warning("Database is not up yet, waiting 3 seconds...")
                time.sleep(3)

        if (queries is None and file_path is None) or (queries is not None and file_path is not None):
            raise ValueError("Either queries or input_path must be specified!")

        queries_and_args_json = False
        if queries is not None:
            queries_and_args_json = True
            file_path = os.path.join(self._directory.name, "queries_and_args_json.json")
            with open(file_path, "w") as f:
                for query in queries:
                    json.dump(query, f)
                    f.write("\n")

        args = self._get_args(
            vendor=self._vendor,
            input=file_path,
            num_workers=num_workers,
            max_retries=max_retries,
            queries_json=queries_and_args_json,
            username=self._username,
            password=self._password,
            port=self._database_port,
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
        if benchmark_context.installation_type == BenchmarkInstallationType.EXTERNAL:
            return ExternalVendor(benchmark_context=benchmark_context)

        subclass_name = (
            benchmark_context.vendor_name
            if benchmark_context.installation_type == BenchmarkInstallationType.NATIVE
            else f"{benchmark_context.vendor_name}{benchmark_context.installation_type}"
        )

        if subclass_name not in cls.subclasses:
            raise ValueError("Missing runner with name: {}".format(benchmark_context.vendor_name))

        return cls.subclasses[subclass_name](
            benchmark_context=benchmark_context,
        )

    @abstractmethod
    def __init__(self, benchmark_context: BenchmarkContext):
        self.benchmark_context = benchmark_context
        self._bolt_port = 7687

    @abstractmethod
    def start_db_init(self, arg):
        pass

    @abstractmethod
    def stop_db_init(self, arg):
        pass

    @abstractmethod
    def start_db(self, arg):
        pass

    @abstractmethod
    def stop_db(self, arg):
        pass

    @abstractmethod
    def clean_db(self):
        pass

    def get_database_port(self):
        return self._bolt_port


class ExternalVendor(BaseRunner):
    def __init__(self, benchmark_context: BenchmarkContext):
        super().__init__(benchmark_context=benchmark_context)

    def start_db_init(self, arg):
        pass

    def stop_db_init(self, arg):
        pass

    def start_db(self, arg):
        pass

    def stop_db(self, arg):
        pass

    def clean_db(self):
        pass


class Memgraph(BaseRunner):
    def __init__(self, benchmark_context: BenchmarkContext):
        super().__init__(benchmark_context=benchmark_context)
        self._memgraph_binary = benchmark_context.vendor_binary
        self._bolt_num_workers = benchmark_context.num_workers_for_benchmark
        self._performance_tracking = benchmark_context.performance_tracking
        self._directory = tempfile.TemporaryDirectory(dir=benchmark_context.temporary_directory)
        self._vendor_args = benchmark_context.vendor_args
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
        kwargs["storage_properties_on_edges"] = True
        kwargs["bolt_num_workers"] = self._bolt_num_workers
        for key, value in self._vendor_args.items():
            kwargs[key] = value
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
        _wait_for_server_socket(self._bolt_port)
        ret = self._proc_mg.poll()

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
        self._start(storage_snapshot_on_exit=True, **self._vendor_args)

    def stop_db_init(self, workload):
        if self._performance_tracking:
            self._stop_event.set()
            self.dump_rss(workload)
        ret, usage = self._cleanup()
        return usage

    def start_db(self, workload):
        if self._performance_tracking:
            p = threading.Thread(target=self.res_background_tracking, args=(self._rss, self._stop_event))
            self._stop_event.clear()
            self._rss.clear()
            p.start()
        self._start(data_recovery_on_startup=True, **self._vendor_args)

    def stop_db(self, workload):
        if self._performance_tracking:
            self._stop_event.set()
            self.dump_rss(workload)
        ret, usage = self._cleanup()
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


class Neo4j(BaseRunner):
    def __init__(self, benchmark_context: BenchmarkContext):
        super().__init__(benchmark_context=benchmark_context)
        self._neo4j_binary = Path(benchmark_context.vendor_binary)
        self._neo4j_path = Path(benchmark_context.vendor_binary).parents[1]
        self._neo4j_config = self._neo4j_path / "conf" / "neo4j.conf"
        self._neo4j_pid = self._neo4j_path / "run" / "neo4j.pid"
        self._neo4j_admin = self._neo4j_path / "bin" / "neo4j-admin"
        self._neo4j_dump = (
            Path()
            / ".cache"
            / "datasets"
            / self.benchmark_context.get_active_workload()
            / self.benchmark_context.get_active_variant()
            / "neo4j.dump"
        )
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
        time.sleep(0.5)
        if self._neo4j_pid.exists():
            print("Neo4j started!")
        else:
            raise Exception("The database process died prematurely!")
        print("Run server check:")
        _wait_for_server_socket(self._bolt_port)

    def _cleanup(self):
        if self._neo4j_pid.exists():
            pid = self._neo4j_pid.read_text()
            print("Clean up: " + pid)
            usage = _get_usage(pid)

            exit_proc = subprocess.run(args=[self._neo4j_binary, "stop"], capture_output=True, check=True)
            return exit_proc.returncode, usage
        else:
            return 0, 0

    def start_db_init(self, workload):
        if self._performance_tracking:
            p = threading.Thread(target=self.res_background_tracking, args=(self._rss, self._stop_event))
            self._stop_event.clear()
            self._rss.clear()
            p.start()

        self._start()

        if self._performance_tracking:
            self.get_memory_usage("start_" + workload)

    def stop_db_init(self, workload):
        if self._performance_tracking:
            self._stop_event.set()
            self.get_memory_usage("stop_" + workload)
            self.dump_rss(workload)
        ret, usage = self._cleanup()
        self.dump_db(path=self._neo4j_dump.parent)
        return usage

    def start_db(self, workload):
        if self._performance_tracking:
            p = threading.Thread(target=self.res_background_tracking, args=(self._rss, self._stop_event))
            self._stop_event.clear()
            self._rss.clear()
            p.start()

        neo4j_dump = (
            Path()
            / ".cache"
            / "datasets"
            / self.benchmark_context.get_active_workload()
            / self.benchmark_context.get_active_variant()
            / "neo4j.dump"
        )
        if neo4j_dump.exists():
            self.load_db_from_dump(path=neo4j_dump.parent)
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
                    "--overwrite-destination=true",
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


class MemgraphDocker(BaseRunner):
    def __init__(self, benchmark_context: BenchmarkContext):
        super().__init__(benchmark_context=benchmark_context)
        self._directory = tempfile.TemporaryDirectory(dir=benchmark_context.temporary_directory)
        self._vendor_args = benchmark_context.vendor_args
        self._bolt_port = self._vendor_args["bolt-port"] if "bolt-port" in self._vendor_args.keys() else "7687"
        self._container_name = "memgraph_benchmark"
        self._image_name = "memgraph/memgraph"
        self._image_version = "3.2.1"
        self._container_ip = None
        self._config_file = None
        _setup_docker_benchmark_network(network_name=DOCKER_NETWORK_NAME)

    def _get_args(self, **kwargs):
        return _convert_args_to_flags(**kwargs)

    def start_db_init(self, message):
        log.init("Starting database for import...")
        try:
            command = [
                "docker",
                "run",
                "--detach",
                "--network",
                DOCKER_NETWORK_NAME,
                "--name",
                self._container_name,
                "-it",
                "-p",
                self._bolt_port + ":" + self._bolt_port,
                f"{self._image_name}:{self._image_version}",
                "--storage_wal_enabled=false",
                "--data_recovery_on_startup=true",
                "--storage_snapshot_interval_sec=0",
            ]
            command.extend(self._get_args(**self._vendor_args))
            run_command(command)
        except subprocess.CalledProcessError as e:
            log.error("Failed to start Memgraph docker container.")
            log.error(
                "There is probably a database running on that port, please stop the running container and try again."
            )
            raise e

        command = [
            "docker",
            "cp",
            self._container_name + ":/etc/memgraph/memgraph.conf",
            self._directory.name + "/memgraph.conf",
        ]
        run_command(command)
        self._config_file = Path(self._directory.name + "/memgraph.conf")
        _wait_for_server_socket(self._bolt_port, delay=0.5)
        log.log("Database started.")

    def stop_db_init(self, message):
        log.init("Stopping database...")
        usage = self._get_cpu_memory_usage()

        # Stop to save the snapshot
        command = ["docker", "stop", self._container_name]
        run_command(command)

        # Change config back to default
        argument = "--storage-snapshot-on-exit=false"
        self._replace_config_args(argument)
        command = [
            "docker",
            "cp",
            self._config_file.resolve(),
            self._container_name + ":/etc/memgraph/memgraph.conf",
        ]
        run_command(command)
        log.log("Database stopped.")
        return usage

    def start_db(self, message):
        log.init("Starting database for benchmark...")
        command = ["docker", "start", self._container_name]
        run_command(command)
        _wait_for_server_socket(self._bolt_port, delay=0.5)
        log.log("Database started.")

    def stop_db(self, message):
        log.init("Stopping database...")
        usage = self._get_cpu_memory_usage()
        command = ["docker", "stop", self._container_name]
        run_command(command)
        log.log("Database stopped.")
        return usage

    def clean_db(self):
        self.remove_container(self._container_name)

    def remove_container(self, containerName):
        command = ["docker", "rm", "-f", containerName]
        run_command(command)

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
                        line = argument + "\n"
                    config_lines.append(line)

        with self._config_file.open("w") as file:
            file.writelines(config_lines)
            file.close()

    def _get_cpu_memory_usage(self):
        command = [
            "docker",
            "exec",
            "-it",
            self._container_name,
            "bash",
            "-c",
            "grep ^VmPeak /proc/1/status",
        ]
        usage = {"cpu": 0, "memory": 0}
        ret = run_command(command)
        memory = ret.stdout.split()
        usage["memory"] = int(memory[1]) * 1024

        command = [
            "docker",
            "exec",
            "-it",
            self._container_name,
            "bash",
            "-c",
            "cat /proc/1/stat",
        ]
        stat = run_command(command).stdout.strip("\n")

        command = [
            "docker",
            "exec",
            "-it",
            self._container_name,
            "bash",
            "-c",
            "getconf CLK_TCK",
        ]
        CLK_TCK = int(run_command(command).stdout.strip("\n"))

        cpu_time = sum(map(int, stat.split(")")[1].split()[11:15])) / CLK_TCK
        usage["cpu"] = cpu_time

        return usage

    def run_command(self, command):
        ret = subprocess.run(command, check=True, capture_output=True, text=True)

        time.sleep(0.2)
        return ret


class Neo4jDocker(BaseRunner):
    def __init__(self, benchmark_context: BenchmarkContext):
        super().__init__(benchmark_context=benchmark_context)
        self._directory = tempfile.TemporaryDirectory(dir=benchmark_context.temporary_directory)
        self._vendor_args = benchmark_context.vendor_args
        self._bolt_port = self._vendor_args["bolt-port"] if "bolt-port" in self._vendor_args.keys() else "7687"
        self._container_name = "neo4j_benchmark"
        self._container_ip = None
        self._config_file = None
        _setup_docker_benchmark_network(DOCKER_NETWORK_NAME)

    def _get_args(self, **kwargs):
        return _convert_args_to_flags(**kwargs)

    def start_db_init(self, message):
        log.init("Starting database for initialization...")
        try:
            command = [
                "docker",
                "run",
                "--detach",
                "--network",
                DOCKER_NETWORK_NAME,
                "--name",
                self._container_name,
                "-it",
                "-p",
                self._bolt_port + ":" + self._bolt_port,
                "--env",
                "NEO4J_AUTH=none",
                "--env",
                "NEO4J_ACCEPT_LICENSE_AGREEMENT=yes",
                "neo4j:5.26-enterprise",
            ]
            command.extend(self._get_args(**self._vendor_args))
            ret = run_command(command)
        except subprocess.CalledProcessError as e:
            log.error("There was an error starting the Neo4j container!")
            log.error(
                "There is probably a database running on that port, please stop the running container and try again."
            )
            raise e
        _wait_for_server_socket(self._bolt_port, delay=5)
        log.log("Database started.")

    def stop_db_init(self, message):
        log.init("Stopping database...")
        usage = self._get_cpu_memory_usage()

        command = ["docker", "stop", self._container_name]
        run_command(command)
        log.log("Database stopped.")

        return usage

    def start_db(self, message):
        log.init("Starting database...")
        command = ["docker", "start", self._container_name]
        run_command(command)
        _wait_for_server_socket(self._bolt_port, delay=5)
        log.log("Database started.")

    def stop_db(self, message):
        log.init("Stopping database...")
        usage = self._get_cpu_memory_usage()

        command = ["docker", "stop", self._container_name]
        run_command(command)
        log.log("Database stopped.")
        return usage

    def clean_db(self):
        self.remove_container(self._container_name)

    def remove_container(self, containerName):
        command = ["docker", "rm", "-f", containerName]
        run_command(command)

    def _get_cpu_memory_usage(self):
        command = [
            "docker",
            "exec",
            "-it",
            self._container_name,
            "bash",
            "-c",
            "cat /var/lib/neo4j/run/neo4j.pid",
        ]
        ret = run_command(command)
        pid = ret.stdout.split()[0]

        command = [
            "docker",
            "exec",
            "-it",
            self._container_name,
            "bash",
            "-c",
            "grep ^VmPeak /proc/{}/status".format(pid),
        ]
        usage = {"cpu": 0, "memory": 0}
        ret = run_command(command)
        memory = ret.stdout.split()
        usage["memory"] = int(memory[1]) * 1024

        command = [
            "docker",
            "exec",
            "-it",
            self._container_name,
            "bash",
            "-c",
            "cat /proc/{}/stat".format(pid),
        ]
        stat = run_command(command).stdout.strip("\n")

        command = [
            "docker",
            "exec",
            "-it",
            self._container_name,
            "bash",
            "-c",
            "getconf CLK_TCK",
        ]
        CLK_TCK = int(run_command(command).stdout.strip("\n"))

        cpu_time = sum(map(int, stat.split(")")[1].split()[11:15])) / CLK_TCK
        usage["cpu"] = cpu_time

        return usage

    def run_command(self, command):
        ret = subprocess.run(command, capture_output=True, check=True, text=True)
        time.sleep(0.2)
        return ret


class FalkorDBDocker(BaseRunner):
    def __init__(self, benchmark_context: BenchmarkContext):
        super().__init__(benchmark_context=benchmark_context)
        self._directory = tempfile.TemporaryDirectory(dir=benchmark_context.temporary_directory)
        self._vendor_args = benchmark_context.vendor_args
        self._falkordb_port = 6379
        self._bolt_port = 7687
        self._container_name = "falkordb_benchmark"
        self._image_name = "falkordb/falkordb"
        self._image_version = "v4.8.5"
        self._container_ip = None
        self._config_file = None
        _setup_docker_benchmark_network(network_name=DOCKER_NETWORK_NAME)

    def start_db_init(self, message):
        log.init("Starting FalkorDB for import (init)...")
        try:
            command = [
                "docker",
                "run",
                "--detach",
                "--network",
                DOCKER_NETWORK_NAME,
                "--name",
                self._container_name,
                "-it",
                "-p",
                f"{self._falkordb_port}:{self._falkordb_port}",
                "-p",
                f"{self._bolt_port}:{self._bolt_port}",
                f"{self._image_name}:{self._image_version}",
            ]
            command.extend(self._get_args(**self._vendor_args))
            run_command(command)
        except subprocess.CalledProcessError as e:
            log.error("Failed to start FalkorDB docker container.")
            log.error(
                "There is probably a database running on that port, please stop the running container and try again."
            )
            raise e

        _wait_for_server_socket(self._bolt_port, delay=0.5)
        log.log("Database started.")

    def start_db(self, message):
        log.init("Starting FalkorDB for benchmark...")
        command = ["docker", "start", self._container_name]
        run_command(command)
        _wait_for_server_socket(self._falkordb_port, delay=0.5)
        log.log("Database started.")

    def stop_db_init(self, message):
        log.init("Stopping database (init)...")
        usage = self._get_cpu_memory_usage()
        run_command(["docker", "exec", self._container_name, "redis-cli", "BGSAVE"])

        command = ["docker", "stop", self._container_name]
        run_command(command)
        log.log("Database stopped.")
        return usage

    def stop_db(self, message):
        log.init("Stopping database...")
        usage = self._get_cpu_memory_usage()
        run_command(["docker", "exec", self._container_name, "redis-cli", "BGSAVE"])

        command = ["docker", "stop", self._container_name]
        run_command(command)
        log.log("Database stopped.")
        return usage

    def clean_db(self):
        self.remove_container(self._container_name)

    def remove_container(self, container_name):
        command = ["docker", "rm", "-f", container_name]
        run_command(command)

    def get_database_port(self):
        return self._falkordb_port

    def _get_args(self, **kwargs):
        return _convert_args_to_flags(**kwargs)

    def _get_cpu_memory_usage(self):
        return {
            "cpu": get_docker_cpu_usage(self._container_name),
            "memory": get_docker_memory_usage(self._container_name),
        }

    def run_command(self, command):
        ret = subprocess.run(command, check=True, capture_output=True, text=True)
        time.sleep(3)
        return ret


class PostgreSQLDocker(BaseRunner):
    def __init__(self, benchmark_context: BenchmarkContext):
        super().__init__(benchmark_context)
        self._container_name = f"{benchmark_context.vendor_name}_benchmark"
        self._port = benchmark_context.vendor_args.get("port", 5432)
        self._user = benchmark_context.vendor_args.get("user", "postgres")
        self._password = benchmark_context.vendor_args.get("password", "postgres")
        self._database = benchmark_context.vendor_args.get("database", "postgres")
        self._image = "postgres:15"
        _setup_docker_benchmark_network(DOCKER_NETWORK_NAME)

    def start_db_init(self, message):
        command = [
            "docker",
            "run",
            "-d",
            "--name",
            self._container_name,
            "--network",
            DOCKER_NETWORK_NAME,
            "-e",
            f"POSTGRES_USER={self._user}",
            "-e",
            f"POSTGRES_PASSWORD={self._password}",
            "-e",
            f"POSTGRES_DB={self._database}",
            "-p",
            f"{self._port}:{self._port}/tcp",
            self._image,
        ]
        run_command(command)
        _wait_for_server_socket(self._port)
        self._wait_for_postgres_ready("127.0.0.1", self._port, self._user, self._password, self._database)

    def start_db(self, message):
        log.init("Starting database for benchmark...")
        command = ["docker", "start", self._container_name]
        run_command(command)
        _wait_for_server_socket(self._port)
        self._wait_for_postgres_ready("127.0.0.1", self._port, self._user, self._password, self._database)

        log.log("Database started.")

    def stop_db_init(self, message):
        log.init("Stopping database (init)...")
        usage = self._get_cpu_memory_usage()
        command = ["docker", "stop", self._container_name]
        run_command(command)
        log.log("Database stopped.")

        return usage

    def stop_db(self, message):
        log.init("Stopping database...")
        usage = self._get_cpu_memory_usage()
        command = ["docker", "stop", self._container_name]
        run_command(command)
        log.log("Database stopped.")

        return usage

    def clean_db(self):
        self.remove_container(self._container_name)

    def get_database_port(self):
        return self._port

    def remove_container(self, container_name):
        command = ["docker", "rm", "-f", container_name]
        run_command(command)

    def _get_args(self, **kwargs):
        return _convert_args_to_flags(**kwargs)

    def _get_cpu_memory_usage(self):
        return {
            "cpu": get_docker_cpu_usage(self._container_name),
            "memory": get_docker_memory_usage(self._container_name),
        }

    def run_command(self, command):
        ret = subprocess.run(command, capture_output=True, text=True)
        if ret.returncode != 0:
            return None
        return ret.stdout.strip().split("\n")

    def _wait_for_postgres_ready(self, host, port, user, password, database, timeout=30):
        import psycopg2

        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                conn = psycopg2.connect(host=host, port=port, user=user, password=password, database=database)
                conn.close()
                return
            except psycopg2.OperationalError as e:
                if "authentication failed" in str(e):
                    raise e  # Credentials are actually wrong
                time.sleep(0.5)
        raise TimeoutError("PostgreSQL did not become ready in time.")
