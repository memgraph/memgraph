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
import os
import subprocess
import sys
import time

import mgclient

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", ".."))
BUILD_DIR = os.path.join(PROJECT_DIR, "build")
MEMGRAPH_BINARY = os.path.join(BUILD_DIR, "memgraph")


def wait_for_server(port, delay=0.01):
    cmd = ["nc", "-z", "-w", "1", "127.0.0.1", str(port)]
    count = 0
    while subprocess.call(cmd) != 0:
        time.sleep(0.01)
        if count > 10 / 0.01:
            print("Could not wait for server on port", port, "to startup!")
            sys.exit(1)
        count += 1
    time.sleep(delay)


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
    def __init__(self, binary_path=MEMGRAPH_BINARY, use_ssl=False):
        self.host = "127.0.0.1"
        self.bolt_port = None
        self.binary_path = binary_path
        self.args = None
        self.proc_mg = None
        self.ssl = use_ssl

    def execute_setup_queries(self, setup_queries):
        if setup_queries is None:
            return
        # An assumption being database instance is fresh, no need for the auth.
        conn = mgclient.connect(host=self.host, port=self.bolt_port, sslmode=self.ssl)
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

    # NOTE: Both query and get_connection may esablish new connection -> auth
    # details required -> username/password should be optional arguments.
    def query(self, query, conn=None, username="", password=""):
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

    def get_connection(self, username="", password=""):
        conn = mgclient.connect(
            host=self.host, port=self.bolt_port, sslmode=self.ssl, username=username, password=password
        )
        conn.autocommit = True
        return conn

    def start(self, restart=False, args=None, setup_queries=None):
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
        self.bolt_port = extract_bolt_port(args_mg)
        self.proc_mg = subprocess.Popen(args_mg)
        wait_for_server(self.bolt_port)
        self.execute_setup_queries(setup_queries)
        assert self.is_running(), "The Memgraph process died!"

    def is_running(self):
        if self.proc_mg is None:
            return False
        if self.proc_mg.poll() is not None:
            return False
        return True

    def stop(self):
        if not self.is_running():
            return

        pid = self.proc_mg.pid
        try:
            os.kill(pid, 15)  # 15 is the signal number for SIGTERM
        except os.OSError:
            assert False

        time.sleep(1)

    def kill(self):
        if not self.is_running():
            return
        self.proc_mg.kill()
        code = self.proc_mg.wait()
        assert code == -9, "The killed Memgraph process exited with non-nine!"
