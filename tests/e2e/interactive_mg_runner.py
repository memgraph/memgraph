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

# TODO(gitbuda): Execute multiple actions by a single command.
# TODO(gitbuda): Isolate details/descriptions in a form of YAML.
# TODO(gitbuda): Avoid the ugly ifs, use dict lookup instead.
# TODO(gitbuda): Print logs command.

import atexit
import logging
import os
import subprocess
from argparse import ArgumentParser
from pathlib import Path
import time
import sys

import yaml
from memgraph import MemgraphInstanceRunner

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", ".."))
BUILD_DIR = os.path.join(PROJECT_DIR, "build")
MEMGRAPH_BINARY = os.path.join(BUILD_DIR, "memgraph")
MEMGRAPH_INSTANCES_DESCRIPTION = [
    {
        "name": "replica1",
        "args": ["--bolt-port", "7688", "--log-level=TRACE"],
        "log_file": "replica1.log",
        "queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10001;"],
    },
    {
        "name": "replica2",
        "args": ["--bolt-port", "7689", "--log-level=TRACE"],
        "log_file": "replica2.log",
        "queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
    },
    {
        "name": "main",
        "args": ["--bolt-port", "7687", "--log-level=TRACE"],
        "log_file": "main.log",
        "queries": [
            "REGISTER REPLICA replica1 SYNC TO '127.0.0.1:10001'",
            "REGISTER REPLICA replica2 SYNC WITH TIMEOUT 1 TO '127.0.0.1:10002'",
        ],
    },
]
MEMGRAPH_INSTANCES = {}

log = logging.getLogger("memgraph.tests.e2e")


def load_args():
    parser = ArgumentParser()
    parser.add_argument(
        "--action", required=False, help="What action to run", default=""
    )
    parser.add_argument(
        "--name", required=False, help="What instance to interact with", default=""
    )
    return parser.parse_args()


def _start_instance(name, args, log_file, queries):
    mg_instance = MemgraphInstanceRunner(MEMGRAPH_BINARY, False)
    MEMGRAPH_INSTANCES[name] = mg_instance
    log_file_path = os.path.join(BUILD_DIR, "logs", log_file)
    binary_args = args + ["--log-file", log_file_path]
    mg_instance.start(args=binary_args)
    return mg_instance


def stop_instance(name):
    for details in MEMGRAPH_INSTANCES_DESCRIPTION:
        if name != details["name"]:
            continue
        MEMGRAPH_INSTANCES[name].stop()


def stop_all():
    for mg_instance in MEMGRAPH_INSTANCES.values():
        mg_instance.stop()


@atexit.register
def cleanup():
    stop_all()


def start_instance(name):
    for details in MEMGRAPH_INSTANCES_DESCRIPTION:
        if name != details["name"]:
            continue
        args = details["args"]
        log_file = details["log_file"]
        queries = details["queries"]
        instance = _start_instance(name, args, log_file, queries)
        for query in queries:
            instance.query(query)


def start_all(args):
    for details in MEMGRAPH_INSTANCES_DESCRIPTION:
        start_instance(details["name"])


def info():
    print("{:<15s}{:>6s}".format("NAME", "STATUS"))
    for description in MEMGRAPH_INSTANCES_DESCRIPTION:
        name = description["name"]
        if name not in MEMGRAPH_INSTANCES:
            continue
        instance = MEMGRAPH_INSTANCES[name]
        print("{:<15s}{:>6s}".format(name, "UP" if instance.is_running() else "DOWN"))


if __name__ == "__main__":
    args = load_args()
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(asctime)s %(name)s] %(message)s"
    )
    while True:
        choice = input("ACTION>")
        action = choice
        if " " in choice:
            action, name = choice.split(" ")
        if action == "exit" or action == "quit":
            sys.exit(0)
        if action == "info":
            info()
        if action == "start_all":
            start_all(args)
        if action == "stop_all":
            stop_all()
        if action == "start":
            start_instance(name)
        if action == "stop":
            stop_instance(name)
        action = None
        name = None
