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

import copy
import logging
import os
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


def extract_bolt_port(args):
    for arg_index, arg in enumerate(args):
        if arg.startswith("--bolt-port="):
            maybe_port = arg.split("=")[1]
            if not maybe_port.isdigit():
                raise Exception("Unable to read Bolt port after --bolt-port=.")
            return int(maybe_port)
        elif arg == "--bolt-port":
            maybe_port = args[arg_index + 1]
            if not maybe_port.isdigit():
                raise Exception("Unable to read Bolt port after --bolt-port.")
            return int(maybe_port)
    return 7687


def extract_management_port(args):
    for arg_index, arg in enumerate(args):
        if arg.startswith("--management-port="):
            maybe_port = arg.split("=")[1]
            if not maybe_port.isdigit():
                raise Exception("Unable to read management port after --management-port=.")
            return int(maybe_port)
        elif arg == "--management-port":
            maybe_port = args[arg_index + 1]
            if not maybe_port.isdigit():
                raise Exception("Unable to read management port after --management-port.")
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
    def __init__(self, binary_path=MEMGRAPH_BINARY, use_ssl=False, data_directory=None, username=None, password=None):
        self.host = "127.0.0.1"
        self.bolt_port = None
        self.binary_path = binary_path
        self.args = None
        self.proc_mg = None
        self.ssl = use_ssl
        self.data_directory = data_directory
        self.username = username
        self.password = password

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

    def execute_setup_queries(self, setup_queries=List):
        """
        Executes setup queries. The element inside `setup_queries` can be a string or a list. Connection is closed at the end and cannot be
        reused.
        """
        conn = self.get_connection(self.username or "", self.password or "")
        conn.autocommit = True
        cursor = conn.cursor()

        for query_coll in setup_queries:
            if isinstance(query_coll, str):
                cursor.execute(query_coll)
            elif isinstance(query_coll, list):
                for query in query_coll:
                    cursor.execute(query)

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
    ):
        """
        Starts an instance which is not already running. Before doing anything, calls `stop` on instance.
        """
        if not restart and self.is_running():
            return

        self.stop()

        if args is not None:
            self.args = copy.deepcopy(args)
        self.args = [replace_paths(arg) for arg in self.args]

        storage_snapshot_on_exit = "true" if storage_snapshot_on_exit else "false"
        args_mg = [
            self.binary_path,
            "--storage-wal-enabled",
            "--storage-snapshot-interval-sec",
            "300",
            "--storage-properties-on-edges",
            f"--storage-snapshot-on-exit={storage_snapshot_on_exit}",
        ] + self.args

        if bolt_port:
            self.bolt_port = bolt_port
        else:
            self.bolt_port = extract_bolt_port(args_mg)

        self.proc_mg = subprocess.Popen(args_mg)

        timeout = 15
        delay = 0.1
        elapsed = 0
        while connectable_port(bolt_port) is False and elapsed < timeout:
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
