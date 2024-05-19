#!/usr/bin/env python3
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

# TODO(gitbuda): Add action to print the context/cluster.
# TODO(gitbuda): Add action to print logs of each Memgraph instance.
# TODO(gitbuda): Polish naming within script.
# TODO(gitbuda): Consider moving this somewhere higher in the project or even put inside GQLAlchemy.

# The idea here is to implement simple interactive runner of Memgraph instances because:
#   * it should be possible to manually create new test cases first
#     by just running this script and executing command manually from e.g. mgconsole,
#     running single instance of Memgraph is easy but running multiple instances and
#     controlling them is not that easy
#   * it should be easy to create new operational test without huge knowledge overhead
#     by e.g. calling `process_actions` from any e2e Python test, the test will contain the
#     string with all actions and should run test code in a different thread.
#
# NOTE: The intention here is not to provide infrastructure to write data
# correctness tests or any heavy workload, the intention is to being able to
# easily test e2e "operational" cases, simple cluster setup and basic Memgraph
# operational queries. For any type of data correctness tests Jepsen or similar
# approaches have to be employed.
# NOTE: The instance description / context should be compatible with tests/e2e/runner.py

import atexit
import logging
import os
import sys
import tempfile
import time
from argparse import ArgumentParser
from inspect import signature
from typing import Optional

import yaml

from memgraph import MemgraphInstanceRunner, extract_bolt_port

log = logging.getLogger("memgraph.tests.e2e")

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", ".."))
BUILD_DIR = os.path.join(PROJECT_DIR, "build")
MEMGRAPH_BINARY = os.path.join(BUILD_DIR, "memgraph")

# Cluster description, injectable as the context.
# If the script argument is not provided, the following will be used as a default.
MEMGRAPH_INSTANCES_DESCRIPTION = {
    "replica1": {
        "args": ["--bolt-port", "7688", "--log-level=TRACE"],
        "log_file": "replica1.log",
        "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10001;"],
    },
    "replica2": {
        "args": ["--bolt-port", "7689", "--log-level=TRACE"],
        "log_file": "replica2.log",
        "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
    },
    "main": {
        "args": ["--bolt-port", "7687", "--log-level=TRACE"],
        "log_file": "main.log",
        "setup_queries": [
            "REGISTER REPLICA replica1 SYNC TO '127.0.0.1:10001'",
            "REGISTER REPLICA replica2 SYNC TO '127.0.0.1:10002'",
        ],
    },
}
MEMGRAPH_INSTANCES = {}
ACTIONS = {
    "info": lambda context: info(context),
    "stop": lambda context, name: stop(context, name),
    "start": lambda context, name: start(context, name),
    "sleep": lambda _, delta: time.sleep(float(delta)),
    "exit": lambda _: sys.exit(1),
    "quit": lambda _: sys.exit(1),
}

CLEANUP_DIRECTORIES_ON_EXIT = False

log = logging.getLogger("memgraph.tests.e2e")


def load_args():
    parser = ArgumentParser()
    parser.add_argument("--actions", required=False, help="What actions to run", default="")
    parser.add_argument(
        "--context-yaml",
        required=False,
        help="YAML file with the cluster description",
        default="",
    )
    return parser.parse_args()


def is_port_in_use(port: int) -> bool:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) == 0


def _start_instance(
    name,
    args,
    log_file,
    setup_queries,
    use_ssl,
    procdir,
    data_directory,
    username=None,
    password=None,
    bolt_port: Optional[int] = None,
    skip_auth: bool = False,
):
    assert (
        name not in MEMGRAPH_INSTANCES.keys()
    ), "If this raises, you are trying to start an instance with the same name as the one running."
    if not bolt_port:
        bolt_port = extract_bolt_port(args)
    assert not is_port_in_use(
        bolt_port
    ), "If this raises, you are trying to start an instance on a port used by the running instance."

    log_file_path = os.path.join(BUILD_DIR, "logs", log_file)
    data_directory_path = os.path.join(BUILD_DIR, data_directory)
    mg_instance = MemgraphInstanceRunner(
        MEMGRAPH_BINARY, use_ssl, {data_directory_path}, username=username, password=password
    )
    MEMGRAPH_INSTANCES[name] = mg_instance
    binary_args = args + ["--log-file", log_file_path] + ["--data-directory", data_directory_path]

    if len(procdir) != 0:
        binary_args.append("--query-modules-directory=" + procdir)
    log.info(f"Starting instance with name: {name} on bolt port {bolt_port}")
    mg_instance.start(args=binary_args, setup_queries=setup_queries, bolt_port=bolt_port, skip_auth=skip_auth)
    assert mg_instance.is_running(), "An error occurred after starting Memgraph instance: application stopped running."


def stop_all(keep_directories=True):
    for mg_instance in MEMGRAPH_INSTANCES.values():
        mg_instance.stop(keep_directories)
    MEMGRAPH_INSTANCES.clear()


def stop_instance(context, name, keep_directories=True):
    for key, _ in context.items():
        if key != name:
            continue
        MEMGRAPH_INSTANCES[name].stop(keep_directories)
        MEMGRAPH_INSTANCES.pop(name)


def stop(context, name, keep_directories=True):
    if name != "all":
        stop_instance(context, name, keep_directories)
        return

    stop_all()


def kill(context, name, keep_directories=True):
    for key in context.keys():
        if key != name:
            continue
        MEMGRAPH_INSTANCES[name].kill(keep_directories)
        MEMGRAPH_INSTANCES.pop(name)


def kill_all(context, keep_directories=True):
    for key in MEMGRAPH_INSTANCES.keys():
        MEMGRAPH_INSTANCES[key].kill(keep_directories)
    MEMGRAPH_INSTANCES.clear()


def cleanup_directories_on_exit(value=True):
    CLEANUP_DIRECTORIES_ON_EXIT = value


@atexit.register
def cleanup():
    stop_all(CLEANUP_DIRECTORIES_ON_EXIT)


def start_instance(context, name, procdir):
    mg_instances = {}

    for key, value in context.items():
        if key != name:
            continue
        args = value["args"]
        log_file = value["log_file"]
        queries = []
        if "setup_queries" in value:
            queries = value["setup_queries"]
        use_ssl = False
        if "ssl" in value:
            use_ssl = bool(value["ssl"])
            value.pop("ssl")
        data_directory = ""
        if "data_directory" in value:
            data_directory = value["data_directory"]
        else:
            data_directory = tempfile.TemporaryDirectory().name
        username = None
        if "username" in value:
            username = value["username"]
        password = None
        if "password" in value:
            password = value["password"]

        default_bolt_port = None
        if "default_bolt_port" in value:
            default_bolt_port = value["default_bolt_port"]

        skip_auth = value["skip_auth"] if "skip_auth" in value else False

        instance = _start_instance(
            name,
            args,
            log_file,
            queries,
            use_ssl,
            procdir,
            data_directory,
            username,
            password,
            default_bolt_port,
            skip_auth=skip_auth,
        )
        log.info(f"Instance with name {name} started")
        mg_instances[name] = instance

    assert len(mg_instances) == 1


def start_all(context, procdir="", keep_directories=True):
    stop_all(keep_directories)
    for key, _ in context.items():
        start_instance(context, key, procdir)


def start_all_keep_others(context, procdir="", keep_directories=True):
    for key, _ in context.items():
        start_instance(context, key, procdir)


def start(context, name, procdir=""):
    if name != "all":
        start_instance(context, name, procdir)
        return

    start_all(context)


def info(context):
    print("{:<15s}{:>6s}".format("NAME", "STATUS"))
    for name, _ in context.items():
        if name not in MEMGRAPH_INSTANCES:
            continue
        instance = MEMGRAPH_INSTANCES[name]
        print("{:<15s}{:>6s}".format(name, "UP" if instance.is_running() else "DOWN"))


def process_actions(context, actions):
    actions = actions.split(" ")
    actions.reverse()
    while len(actions) > 0:
        name = actions.pop()
        action = ACTIONS[name]
        args_no = len(signature(action).parameters) - 1
        assert (
            args_no >= 0
        ), "Wrong action definition, each action has to accept at least 1 argument which is the context."
        assert args_no <= 1, "Actions with more than one user argument are not yet supported"
        if args_no == 0:
            action(context)
        if args_no == 1:
            action(context, actions.pop())


if __name__ == "__main__":
    args = load_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(asctime)s %(name)s] %(message)s")

    if args.context_yaml == "":
        context = MEMGRAPH_INSTANCES_DESCRIPTION
    else:
        with open(args.context_yaml, "r") as f:
            context = yaml.load(f, Loader=yaml.FullLoader)
    if args.actions != "":
        process_actions(context, args.actions)
        sys.exit(0)

    while True:
        choice = input("ACTION>")
        process_actions(context, choice)
