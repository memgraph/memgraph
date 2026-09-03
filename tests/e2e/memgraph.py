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

import contextlib
import copy
import ctypes
import json
import logging
import os
import re
import shutil
import socket
import subprocess
import sys
import time
from datetime import datetime
from typing import List, Optional

import mgclient

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", ".."))
BUILD_DIR = os.path.join(PROJECT_DIR, "build")
MEMGRAPH_BINARY = os.path.join(BUILD_DIR, "memgraph")
SIGNAL_SIGTERM = 15

log = logging.getLogger("memgraph.tests.e2e")

# Port remapping for parallel e2e runs. runner_parallel.py hands every test process a private port window through these
# env vars. Ports hardcoded by tests (7687, 10011, ...) are mapped into the window when instances start and when clients
# connect (see sitecustomize.py), and mapped back in query results, so tests keep asserting on the ports they were
# written with. Without the env vars everything is a no-op.
PORT_WINDOW_START_ENV = "MEMGRAPH_PORT_WINDOW_START"
PORT_WINDOW_SIZE_ENV = "MEMGRAPH_PORT_WINDOW_SIZE"
PORT_MAP_ENV = "MEMGRAPH_E2E_PORT_MAP"
HA_INIT_QUERIES_ENV = "MEMGRAPH_HA_CLUSTER_INIT_QUERIES"
DEFAULT_MONITORING_PORT = 7444
DEFAULT_METRICS_PORT = 9091
PORT_FLAGS = {
    "--bolt-port",
    "--bolt_port",
    "--management-port",
    "--management_port",
    "--coordinator-port",
    "--coordinator_port",
    "--replication-port",
    "--replication_port",
    "--monitoring-port",
    "--monitoring_port",
    "--metrics-port",
    "--metrics_port",
}
LOCAL_HOSTS = {"localhost", "127.0.0.1", "0.0.0.0", "::1", "::"}
# Only host:port endpoints and `WITH PORT n` are touched, so numbers in map literals or timestamps are left alone.
ENDPOINT_RE = re.compile(r"(?<![\w.])(localhost|127\.0\.0\.1|0\.0\.0\.0|\[::1\]):(\d{4,5})\b")
PORT_KEYWORD_RE = re.compile(r"(?i)(\bPORT\s+)(\d{4,5})\b")


class PortRemap:
    def __init__(self):
        self.window_start = int(os.getenv(PORT_WINDOW_START_ENV, "0") or 0)
        self.window_size = int(os.getenv(PORT_WINDOW_SIZE_ENV, "0") or 0)
        self.active = self.window_start > 0 and self.window_size > 0
        self.forward = {}
        self.reverse = {}
        if not self.active:
            return
        try:
            seed = json.loads(os.getenv(PORT_MAP_ENV, "") or "{}")
        except Exception:
            seed = {}
        for original, mapped in seed.items():
            self.forward[int(original)] = int(mapped)
            self.reverse[int(mapped)] = int(original)

    def is_candidate(self, port):
        return 1024 <= port < self.window_start

    def map_port(self, port):
        if not self.active or not isinstance(port, int) or not self.is_candidate(port):
            return port
        if port not in self.forward:
            mapped = next(
                (p for p in range(self.window_start, self.window_start + self.window_size) if p not in self.reverse),
                None,
            )
            if mapped is None:
                raise RuntimeError(
                    f"Port window {self.window_start}-{self.window_start + self.window_size - 1} exhausted, "
                    "increase --port-offset-step of runner_parallel.py."
                )
            self.forward[port] = mapped
            self.reverse[mapped] = port
        return self.forward[port]

    def unmap_port(self, port):
        return self.reverse.get(port, port) if self.active else port

    def map_text(self, text):
        if not self.active or not isinstance(text, str):
            return text
        text = ENDPOINT_RE.sub(lambda m: f"{m.group(1)}:{self.map_port(int(m.group(2)))}", text)
        return PORT_KEYWORD_RE.sub(lambda m: f"{m.group(1)}{self.map_port(int(m.group(2)))}", text)

    def unmap_text(self, text):
        if not self.active or not isinstance(text, str):
            return text
        text = ENDPOINT_RE.sub(lambda m: f"{m.group(1)}:{self.unmap_port(int(m.group(2)))}", text)
        return PORT_KEYWORD_RE.sub(lambda m: f"{m.group(1)}{self.unmap_port(int(m.group(2)))}", text)

    def map_args(self, args):
        if not self.active or not args:
            return args
        mapped = list(args)
        for i, arg in enumerate(mapped):
            if arg in PORT_FLAGS and i + 1 < len(mapped) and str(mapped[i + 1]).isdigit():
                mapped[i + 1] = str(self.map_port(int(mapped[i + 1])))
            elif (
                isinstance(arg, str)
                and "=" in arg
                and arg.split("=", 1)[0] in PORT_FLAGS
                and arg.split("=", 1)[1].isdigit()
            ):
                flag, value = arg.split("=", 1)
                mapped[i] = f"{flag}={self.map_port(int(value))}"
            else:
                mapped[i] = self.map_text(arg)
        return mapped

    def unmap_value(self, value):
        """Maps ports back in strings nested anywhere inside query results."""
        if not self.active:
            return value
        if isinstance(value, str):
            return self.unmap_text(value)
        if isinstance(value, tuple):
            return tuple(self.unmap_value(v) for v in value)
        if isinstance(value, list):
            return [self.unmap_value(v) for v in value]
        if isinstance(value, dict):
            return {k: self.unmap_value(v) for k, v in value.items()}
        return value

    @contextlib.contextmanager
    def child_env(self):
        """
        While active, MEMGRAPH_*_PORT variables and the HA init queries file are remapped for a Memgraph child process.
        Only the C-level environment is touched (os.putenv), so the child otherwise inherits exactly what it would have
        without remapping. Tests use os.unsetenv, which os.environ does not reflect, so a dict copy would be wrong.
        """
        if not self.active:
            yield
            return
        saved = []
        for name in list(os.environ):
            if not (name.startswith("MEMGRAPH_") and name.endswith("_PORT")):
                continue
            value = _c_getenv(name)
            if value is None or not value.isdigit():
                continue
            mapped = str(self.map_port(int(value)))
            if mapped != value:
                os.putenv(name, mapped)
                saved.append((name, value))
        init_queries = _c_getenv(HA_INIT_QUERIES_ENV)
        if init_queries and os.path.isfile(init_queries):
            with open(init_queries) as f:
                content = f.read()
            remapped_path = f"{init_queries}.w{self.window_start}"
            with open(remapped_path, "w") as f:
                f.write(self.map_text(content))
            os.putenv(HA_INIT_QUERIES_ENV, remapped_path)
            saved.append((HA_INIT_QUERIES_ENV, init_queries))
        try:
            yield
        finally:
            for name, value in saved:
                os.putenv(name, value)


def _c_getenv(name):
    """Current C-level value of an environment variable, which os.environ misses after os.putenv/os.unsetenv."""
    try:
        libc = ctypes.CDLL(None)
        libc.getenv.restype = ctypes.c_char_p
        value = libc.getenv(name.encode())
        return None if value is None else value.decode()
    except Exception:
        return os.environ.get(name)


PORT_REMAP = PortRemap()


def extract_bolt_port(args):
    for arg_index, arg in enumerate(args):
        if arg.startswith("--bolt-port=") or arg.startswith("--bolt_port="):
            maybe_port = arg.split("=")[1]
            if not maybe_port.isdigit():
                raise Exception("Unable to read Bolt port after --bolt-port= / --bolt_port=.")
            return int(maybe_port)
        elif arg in ("--bolt-port", "--bolt_port"):
            maybe_port = args[arg_index + 1]
            if not maybe_port.isdigit():
                raise Exception("Unable to read Bolt port after --bolt-port / --bolt_port.")
            return int(maybe_port)
    return 7687


def extract_management_port(args):
    for arg_index, arg in enumerate(args):
        if arg.startswith("--management-port=") or arg.startswith("--management_port="):
            maybe_port = arg.split("=")[1]
            if not maybe_port.isdigit():
                raise Exception("Unable to read management port after --management-port= / --management_port=.")
            return int(maybe_port)
        elif arg in ("--management-port", "--management_port"):
            maybe_port = args[arg_index + 1]
            if not maybe_port.isdigit():
                raise Exception("Unable to read management port after --management-port / --management_port.")
            return int(maybe_port)
    return None


def replace_paths(path):
    return path.replace("$PROJECT_DIR", PROJECT_DIR).replace("$SCRIPT_DIR", SCRIPT_DIR).replace("$BUILD_DIR", BUILD_DIR)


def connectable_port(port: int) -> bool:
    """
    Checks if it is possible to connect to port.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) == 0


class MemgraphInstanceRunner:
    def __init__(
        self,
        binary_path=MEMGRAPH_BINARY,
        use_ssl=False,
        data_directory=None,
        username=None,
        password=None,
        gdb_port=None,
    ):
        self.host = "127.0.0.1"
        self.bolt_port = None
        self.binary_path = binary_path
        self.args = None
        self.proc_mg = None
        self.ssl = use_ssl
        self.data_directory = data_directory
        self.username = username
        self.password = password
        self.gdb_port = gdb_port  # If set, run under gdbserver on this port

    def _print_diagnostics(self):
        """Print diagnostic information when server fails to start or
        fails to listen to a connect, or refuses to stop."""
        print("\n" + "=" * 80)
        print(f"exit code: {self.proc_mg.returncode}")

        proc_dir = f"/proc/{self.proc_mg.pid}"
        if os.path.isdir(proc_dir):
            proc_files = {
                "wchan": f"{proc_dir}/wchan",
                "cmdline": f"{proc_dir}/cmdline",
                "status": f"{proc_dir}/status",
                "limits": f"{proc_dir}/limits",
                "stat": f"{proc_dir}/stat",
            }

            for name, path in proc_files.items():
                if os.path.exists(path):
                    try:
                        with open(path, "r") as f:
                            content = f.read().strip()
                            if name == "cmdline":
                                content = content.replace("\0", " ")
                            print(f"\n/proc/{name}")
                            print(content)
                    except Exception as e:
                        pass

        print("=" * 80 + "\n")

    # If the method with socket is ok, remove this TODO: (andi)
    def wait_for_succesful_connection(self, delay=0.1):
        """
        Wait for successful mgclient connection and return the connection. Connection will be closed in the caller.
        """
        timeout = 15
        elapsed = 0
        while elapsed < timeout:
            try:
                return mgclient.connect(
                    host=self.host,
                    port=self.bolt_port,
                    sslmode=self.ssl,
                    username=(self.username or ""),
                    password=(self.password or ""),
                )
            except Exception as e:
                if str(e) == "Authentication failure":
                    print("Authentication failure, instance auth wrong!")
                    break
                # Probably port not ready yet, wait a bit
                time.sleep(delay)
                elapsed += delay

        print(f"Could not wait for host {self.host} on port {self.bolt_port} to startup!")
        sys.exit(1)

    def query(self, query, conn=None, username="", password=""):
        """
        Reuses connection `conn` if possible. If not, creates new connection, runs the queries and returns all data by exhausting cursor.
        Connection is closed.
        """
        new_conn = conn is None
        if new_conn:
            conn = self.get_connection(username, password)

        cursor = conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()

        cursor.close()
        if new_conn:
            conn.close()

        return data

    def execute_setup_queries(self, setup_queries=List, ignore_failures=False, log_ignored_failures=True):
        """
        Executes setup queries. The element inside `setup_queries` can be a string or a list. Connection is closed at the end and cannot be
        reused. When `ignore_failures` is set, failures of individual setup queries are logged and skipped, e.g. when
        restarting an instance on which the queries were already applied. `log_ignored_failures` can be turned off when
        those failures are expected on every restart and would otherwise bury the interesting output, as when a
        benchmark restarts a cluster whose setup has already been applied.
        """
        conn = self.get_connection(self.username or "", self.password or "")
        conn.autocommit = True
        cursor = conn.cursor()

        def execute_one(cursor, query):
            try:
                cursor.execute(query)
                return cursor
            except Exception as e:
                if not ignore_failures:
                    raise
                if log_ignored_failures:
                    log.warning(f"Ignoring failed setup query '{query}': {e}")
                # The connection may be left in a bad state after a failed query, use a fresh one.
                nonlocal conn
                try:
                    conn.close()
                except Exception:
                    pass
                conn = self.get_connection(self.username or "", self.password or "")
                conn.autocommit = True
                return conn.cursor()

        for query_coll in setup_queries:
            if isinstance(query_coll, str):
                cursor = execute_one(cursor, query_coll)
            elif isinstance(query_coll, list):
                for query in query_coll:
                    cursor = execute_one(cursor, query)

        cursor.close()
        conn.close()

    def get_connection(self, username="", password=""):
        """
        Retrieves new mgclient connection with autocommit set to true.
        """
        conn = mgclient.connect(
            host=self.host, port=self.bolt_port, sslmode=self.ssl, username=username, password=password
        )
        conn.autocommit = True
        return conn

    def start(
        self,
        restart=False,
        args=None,
        setup_queries=None,
        bolt_port: Optional[int] = None,
        storage_snapshot_on_exit: bool = False,
        silence_output: bool = False,
    ):
        """
        Starts an instance which is not already running. Before doing anything, calls `stop` on instance.
        When `silence_output` is set, the instance's stdout and stderr are discarded instead of inherited. Everything
        Memgraph logs still reaches its `--log-file`; what is dropped is the startup banner, the flag deprecation
        notices and the query module import notes, which a caller starting a whole cluster repeatedly would otherwise
        see once per instance per start.
        """
        if not restart and self.is_running():
            return

        self.stop()

        if args is not None:
            self.args = copy.deepcopy(args)
        self.args = [replace_paths(arg) for arg in self.args]

        storage_snapshot_on_exit = "true" if storage_snapshot_on_exit else "false"
        default_args = [
            self.binary_path,
            "--storage-wal-enabled",
            "--storage-snapshot-interval-sec",
            "300",
            "--storage-properties-on-edges",
            f"--storage-snapshot-on-exit={storage_snapshot_on_exit}",
        ]
        # Default the metrics endpoint to OpenMetrics unless the workload opts out
        # (e.g. tests that exercise the deprecated JSON format set --metrics-format
        # explicitly, in which case their value wins).
        if not any(arg.startswith("--metrics-format") for arg in self.args):
            default_args.append("--metrics-format=OpenMetrics")
        if PORT_REMAP.active:
            # Give the implicit listeners explicit ports so they get remapped away from other workers too.
            if not any(arg.startswith("--bolt-port") or arg.startswith("--bolt_port") for arg in self.args):
                default_args += ["--bolt-port", "7687"]
            if not any(arg.startswith("--monitoring-port") or arg.startswith("--monitoring_port") for arg in self.args):
                default_args += ["--monitoring-port", str(DEFAULT_MONITORING_PORT)]
            if not any(arg.startswith("--metrics-port") or arg.startswith("--metrics_port") for arg in self.args):
                default_args += ["--metrics-port", str(DEFAULT_METRICS_PORT)]
        args_mg = PORT_REMAP.map_args(default_args + self.args)

        if bolt_port:
            bolt_port = PORT_REMAP.map_port(bolt_port)
            self.bolt_port = bolt_port
        else:
            self.bolt_port = extract_bolt_port(args_mg)
            bolt_port = self.bolt_port

        # If gdb_port is set, wrap with gdbserver
        if self.gdb_port:
            args_mg = ["gdbserver", f":{self.gdb_port}"] + args_mg
            print("\n" + "=" * 80)
            print(f"MEMGRAPH STARTED UNDER GDBSERVER ON PORT {self.gdb_port}")
            print(f"Connect with: gdb {self.binary_path} -ex 'target remote :{self.gdb_port}'")
            print("=" * 80)
            print("Waiting for debugger to attach... (press Enter in gdb to continue)")
            print("=" * 80 + "\n")

        output = subprocess.DEVNULL if silence_output else None
        with PORT_REMAP.child_env():
            self.proc_mg = subprocess.Popen(args_mg, stdout=output, stderr=output)

        # Use much longer timeout when debugging with gdb. Startup can take well over 15s on a loaded machine (e.g. a
        # parallel e2e run), so wait longer, but stop waiting as soon as the process is gone.
        timeout = 3600 if self.gdb_port else 60
        delay = 0.1
        elapsed = 0
        while connectable_port(bolt_port) is False and elapsed < timeout and self.is_running():
            time.sleep(delay)
            elapsed += delay

        is_running = self.is_running()
        is_connected = connectable_port(bolt_port)

        if not is_running or not is_connected:
            self._print_diagnostics()

        assert is_running, f"The Memgraph process failed to start in {timeout}s!"
        assert is_connected, f"The Memgraph process failed to listen in {timeout}s!"
        log.info(f"Instance started with bolt server on {self.host}:{bolt_port}.")

        if setup_queries:
            self.execute_setup_queries(setup_queries)
            log.info("Executed setup queries.")

    def is_running(self):
        """
        Checks if the underlying process is still running by calling `poll` on the process.
        """
        if self.proc_mg is None:
            return False

        if self.proc_mg.poll() is not None:
            return False

        return True

    def stop(self, keep_directories=False):
        """
        Sends SIGTERM signal to `self.proc_mg` and if `keep_directories=False`, deletes its data_directory.
        """
        if not self.is_running():
            return

        signal_time = datetime.now()
        self.proc_mg.terminate()

        for _ in range(150):
            if not self.is_running():
                break
            time.sleep(0.1)

        is_running = self.is_running()
        if is_running:
            self._print_diagnostics()

        assert (
            is_running is False
        ), f"Stopped instance at {self.host}:{self.bolt_port} still running. Signal sent at: {signal_time}. Now is: {datetime.now()}"

        if not keep_directories:
            self.safe_delete_data_directory()

    def kill(self, keep_directories=False):
        """
        Sends SIGKILL to `self.proc_mg` and if `keep_directories=False`, deletes its data_directory.
        """
        if not self.is_running():
            return

        self.proc_mg.kill()
        code = self.proc_mg.wait()

        assert code == -9, "The killed Memgraph process exited with non-nine!"

        for _ in range(150):
            if not self.is_running():
                break
            time.sleep(0.1)

        assert self.is_running() is False, "Killed instance still running."

        if not keep_directories:
            self.safe_delete_data_directory()

    def safe_delete_data_directory(self):
        """
        Deletes `self.data_directory` and asserts there were no exceptions thrown during deletion.
        """
        try:
            shutil.rmtree(self.data_directory)
        except Exception as e:
            print(e)
            pass
