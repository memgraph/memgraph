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
import tempfile
import time

import mgclient

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_PATH = os.path.normpath(os.path.join(SCRIPT_DIR, "..", ".."))
BUILD_DIR = os.path.join(PROJECT_PATH, "build")
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
    return (
        path.replace("$PROJECT_PATH", PROJECT_PATH).replace("$SCRIPT_DIR", SCRIPT_DIR).replace("$BUILD_DIR", BUILD_DIR)
    )


class MemgraphInstanceRunner:
    def __init__(self, binary_path=MEMGRAPH_BINARY, use_ssl=False):
        self.host = "127.0.0.1"
        self.bolt_port = None
        self.binary_path = binary_path
        self.args = None
        self.proc_mg = None
        self.conn = None
        self.ssl = use_ssl

    def query(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def start(self, restart=False, args=[]):
        if not restart and self.is_running():
            return
        self.stop()
        self.args = copy.deepcopy(args)
        self.args = [replace_paths(arg) for arg in self.args if isinstance(arg, str)]
        self.data_directory = tempfile.TemporaryDirectory()
        args_mg = [
            self.binary_path,
            "--data-directory",
            self.data_directory.name,
            "--storage-wal-enabled",
            "--storage-snapshot-interval-sec",
            "300",
            "--storage-properties-on-edges",
        ] + self.args
        self.bolt_port = extract_bolt_port(args_mg)
        self.proc_mg = subprocess.Popen(args_mg)
        wait_for_server(self.bolt_port)
        self.conn = mgclient.connect(host=self.host, port=self.bolt_port, sslmode=self.ssl)
        self.conn.autocommit = True
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
        self.proc_mg.terminate()
        code = self.proc_mg.wait()
        assert code == 0, "The Memgraph process exited with non-zero!"
