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
import subprocess
import sys
import time
from typing import Optional

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


def replace_paths(path):
    return path.replace("$PROJECT_DIR", PROJECT_DIR).replace("$SCRIPT_DIR", SCRIPT_DIR).replace("$BUILD_DIR", BUILD_DIR)


class MemgraphInstanceRunner:
    def __init__(self, binary_path=MEMGRAPH_BINARY, use_ssl=False, delete_on_stop=None, username=None, password=None):
        self.host = "127.0.0.1"
        self.bolt_port = None
        self.binary_path = binary_path
        self.args = None
        self.proc_mg = None
        self.ssl = use_ssl
        self.delete_on_stop = delete_on_stop
        self.username = username
        self.password = password

    def wait_for_succesful_connection(self, delay=0.01):
        count = 0
        while count < 1000:
            try:
                conn = mgclient.connect(
                    host=self.host,
                    port=self.bolt_port,
                    sslmode=self.ssl,
                    username=(self.username or ""),
                    password=(self.password or ""),
                )
                return conn
            except Exception:
                count += 1
                time.sleep(delay)
                continue

        print(f"Could not wait for host {self.host} on port {self.bolt_port} to startup!")
        sys.exit(1)

    def execute_setup_queries(self, conn, setup_queries):
        if setup_queries is None:
            return
        conn.autocommit = True
        cursor = conn.cursor()
        log.info(f"Executing setup queries on instance {self.host}:{self.bolt_port}: {setup_queries}")
        for query_coll in setup_queries:
            if isinstance(query_coll, str):
                cursor.execute(query_coll)
                log.info(f"Query executed {query_coll} on instance {self.host}:{self.bolt_port}")
            elif isinstance(query_coll, list):
                for query in query_coll:
                    cursor.execute(query)
                    log.info(f"Query executed {query} on instance {self.host}:{self.bolt_port}")
        cursor.close()
        conn.close()

    def start(self, restart=False, args=None, setup_queries=None, bolt_port: Optional[int] = None):
        if not restart and self.is_running():
            return
        self.stop()
        if args is not None:
            self.args = copy.deepcopy(args)
        self.args = [replace_paths(arg) for arg in self.args]
        args_mg = [
            self.binary_path,
            "--storage-wal-enabled",
            "--storage-snapshot-interval-sec",
            "300",
            "--storage-properties-on-edges",
        ] + self.args
        if bolt_port:
            self.bolt_port = bolt_port
        else:
            self.bolt_port = extract_bolt_port(args_mg)
        self.proc_mg = subprocess.Popen(args_mg)
        log.info(f"Subprocess started with args {args_mg}")
        conn = self.wait_for_succesful_connection()
        log.info(f"Server started on instance with bolt port {self.host}:{bolt_port}")
        self.execute_setup_queries(conn, setup_queries)
        assert self.is_running(), "The Memgraph process died!"

    def is_running(self):
        if self.proc_mg is None:
            return False
        if self.proc_mg.poll() is not None:
            return False
        return True

    def stop(self, keep_directories=False):
        if not self.is_running():
            return

        pid = self.proc_mg.pid
        try:
            os.kill(pid, SIGNAL_SIGTERM)
        except os.OSError:
            assert False

        time.sleep(1)

        if not keep_directories:
            for folder in self.delete_on_stop or {}:
                try:
                    shutil.rmtree(folder)
                except Exception as e:
                    pass  # couldn't delete folder, skip

    def kill(self, keep_directories=False):
        if not self.is_running():
            return
        self.proc_mg.kill()
        code = self.proc_mg.wait()
        if not keep_directories:
            for folder in self.delete_on_stop or {}:
                shutil.rmtree(folder)
        assert code == -9, "The killed Memgraph process exited with non-nine!"
