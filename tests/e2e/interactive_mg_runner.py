#!/usr/bin/env python3
# Copyright 2025 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

# The idea here is to implement a simple interactive runner of Memgraph instances because:
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

import logging
import os
import secrets
import sys
import termios
import time
import tty
from argparse import ArgumentParser
from inspect import signature

import yaml

from memgraph import (
  MemgraphInstanceRunner,
  connectable_port,
  extract_bolt_port,
  extract_management_port,
)

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


def read_action_line(prompt="ACTION> "):
    sys.stdout.write(prompt)
    sys.stdout.flush()

    fd = sys.stdin.fileno()
    old_attrs = termios.tcgetattr(fd)
    new_attrs = termios.tcgetattr(fd)

    # Turn off ECHO and ICANON (so keys don't print themselves and we read byte-by-byte)
    new_attrs[3] &= ~(termios.ECHO | termios.ICANON)
    termios.tcsetattr(fd, termios.TCSADRAIN, new_attrs)

    try:
        buf = []
        while True:
            ch = os.read(fd, 1)
            if not ch:
                continue
            c = ch.decode("utf-8", "ignore")

            # Enter
            if c in ("\n", "\r"):
                sys.stdout.write("\n")
                sys.stdout.flush()
                return "".join(buf)

            # Backspace (DEL)
            if c == "\x7f":
                if buf:
                    buf.pop()
                    sys.stdout.write("\b \b")
                    sys.stdout.flush()
                continue

            # ESC sequences (arrows, Home/End, etc.) — swallow them completely
            if c == "\x1b":
                # Typical CSI: ESC [ ... final
                nxt = os.read(fd, 1).decode("utf-8", "ignore")
                if nxt == "[":
                    # Read until final byte of CSI sequence (@ A–Z a–z ~)
                    while True:
                        d = os.read(fd, 1).decode("utf-8", "ignore")
                        if not d:
                            break
                        if d.isalpha() or d in "@~":
                            break
                # Ignore whole sequence (don’t echo)
                continue

            # Regular printable characters
            if c.isprintable():
                buf.append(c)
                sys.stdout.write(c)
                sys.stdout.flush()
            # ignore everything else silently
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_attrs)


def clear_screen():
    """Clear terminal screen (cross-platform)."""
    # Use ANSI first (fast, no subprocess); fall back to cls/clear if needed.
    try:
        # Clear + move cursor to home
        sys.stdout.write("\033[2J\033[H")
        sys.stdout.flush()
    except Exception:
        os.system("cls" if os.name == "nt" else "clear")


import subprocess


def pids_for_port(port: int) -> list[str]:
    # -nP: no DNS/service lookups; -iTCP:<port>: filter by TCP and port
    # -sTCP:LISTEN: only listening sockets; -t: PIDs only
    cmd = ["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-t"]
    try:
        out = subprocess.check_output(cmd, text=True).strip()
        return sorted({x for x in out.split()}) if out else []
    except subprocess.CalledProcessError:
        return []  # no matches
    except FileNotFoundError:
        raise RuntimeError("lsof not found in PATH")


def pidof(instances, instance_name):
    if instance_name not in instances:
        log.error(f"{instance_name} is not an active instance")
        return

    val = instances[instance_name]
    mg_args = val["args"]
    port = int(next(mg_arg.split("=", 1)[1] for mg_arg in mg_args if mg_arg.startswith("--bolt-port=")))
    pids = pids_for_port(port)
    if len(pids) == 1:
        pids = pids[0]  # To avoid list output
    print("{:<15s}".format(str(pids)))


MEMGRAPH_INSTANCES = {}

ACTIONS = {
    "info": lambda instances: info(instances),
    "stop": lambda instances, name: stop(instances, name),
    "start": lambda instances, name: start_wrapper(instances, name),
    "sleep": lambda _, delta: time.sleep(float(delta)),
    "exit": lambda _: sys.exit(1),
    "quit": lambda _: sys.exit(1),
    "clear": lambda _: clear_screen(),
    "cls": lambda _: clear_screen(),
    "\x0c": lambda _: clear_screen(),  # Ctrl+L
    "^L": lambda _: clear_screen(),
    "pidof": lambda instances, instance_name: pidof(instances, instance_name),
}

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


def wait_until_port_is_free(port: int) -> bool:
    """
    Return True when port is free, False if port is still not free after 10s.
    """
    for _ in range(100):
        # If we can connect to the port that means previous process is still running and we have to wait for it to finish.
        if not connectable_port(port):
            return True
        else:
            time.sleep(0.1)
    return False


def _start(
    name,
    args,
    log_file,
    setup_queries,
    use_ssl,
    procdir,
    data_directory,
    username=None,
    password=None,
    storage_snapshot_on_exit: bool = False,
):
    assert (
        name not in MEMGRAPH_INSTANCES.keys()
    ), "If this raises, you are trying to start an instance with the same name as the one running."

    bolt_port = extract_bolt_port(args)
    assert wait_until_port_is_free(
        bolt_port
    ), f"If this raises, you are trying to start an instance on a port {bolt_port} used by the running instance."

    management_port = extract_management_port(args)
    if management_port:
        assert wait_until_port_is_free(
            management_port
        ), f"If this raises, you are trying to start with coordinator management port {management_port} which is already in use."

    log_file_path = os.path.join(BUILD_DIR, "e2e", "logs", log_file)
    data_directory_path = os.path.join(BUILD_DIR, "e2e", "data", data_directory)

    mg_instance = MemgraphInstanceRunner(
        MEMGRAPH_BINARY, use_ssl, data_directory_path, username=username, password=password
    )
    MEMGRAPH_INSTANCES[name] = mg_instance

    binary_args = args + ["--log-file", log_file_path] + ["--data-directory", data_directory_path]

    if len(procdir) != 0:
        binary_args.append("--query-modules-directory=" + procdir)

    log.info(f"Starting instance with name: {name} on bolt port {bolt_port}")
    mg_instance.start(
        args=binary_args,
        setup_queries=setup_queries,
        bolt_port=bolt_port,
        storage_snapshot_on_exit=storage_snapshot_on_exit,
    )
    assert mg_instance.is_running(), "An error occurred after starting Memgraph instance: application stopped running."


def stop_all(keep_directories=True):
    """
    Idempotent in a sense that if instances were already stopped, additional call to stop_all won't do anything wrong. Sends SIGTERM signal.
    """
    for mg_instance in MEMGRAPH_INSTANCES.values():
        mg_instance.stop(keep_directories)
    MEMGRAPH_INSTANCES.clear()


def stop(context, name, keep_directories=True):
    """
    Idempotent in a sense that stopping already stopped instance won't fail program.
    """
    if name not in context:
        log.error(f"{name} is not an active instance name")
        return
    for key, _ in context.items():
        if key != name:
            continue
        MEMGRAPH_INSTANCES[name].stop(keep_directories)
        MEMGRAPH_INSTANCES.pop(name)


def kill_all(keep_directories=True):
    """
    Idempotent in a sense that killing already dead instances won't fail. Sends SIGKILL signal.
    """
    for key in MEMGRAPH_INSTANCES.keys():
        MEMGRAPH_INSTANCES[key].kill(keep_directories)
    MEMGRAPH_INSTANCES.clear()


def kill(context, name, keep_directories=True):
    """
    Kills instance with name 'name' from the 'context'.
    """
    for key in context.keys():
        if key != name:
            continue
        MEMGRAPH_INSTANCES[name].kill(keep_directories)
        MEMGRAPH_INSTANCES.pop(name)


def start_wrapper(instances, instance_name, procdir=""):
    if instance_name == "all":
        start_all(instances, procdir)
    else:
        start(instances, instance_name, procdir)


def start(instances, instance_name, procdir=""):
    mg_instances = {}

    if instance_name not in instances:
        log.error(f"{instance_name} is not an active instance name")
        return

    for key, value in instances.items():
        if key != instance_name:
            continue
        args = value["args"]
        log_file = value["log_file"]

        setup_queries = value["setup_queries"] if "setup_queries" in value else []

        use_ssl = False
        if "ssl" in value:
            use_ssl = bool(value["ssl"])
            value.pop("ssl")

        # If nothing specified, use 8-character random string.
        data_directory = value["data_directory"] if "data_directory" in value else secrets.token_hex(4)

        username = value["username"] if "username" in value else None
        password = value["password"] if "password" in value else None

        storage_snapshot_on_exit = value["storage_snapshot_on_exit"] if "storage_snapshot_on_exit" in value else False

        instance = _start(
            instance_name,
            args,
            log_file,
            setup_queries,
            use_ssl,
            procdir,
            data_directory,
            username,
            password,
            storage_snapshot_on_exit=storage_snapshot_on_exit,
        )
        log.info(f"Instance with name {instance_name} started")
        mg_instances[instance_name] = instance

    assert len(mg_instances) == 1


def start_all(context, procdir="", keep_directories=True):
    """
    Start all instances by first stopping all instances and then calling start_instance for each instance from the `context`.
    """
    stop_all(keep_directories)
    for key, _ in context.items():
        start(context, key, procdir)


def start_all_keep_others(context, procdir=""):
    """
    Start all instances from the context but don't stop currently running instances.
    """
    for key, _ in context.items():
        start(context, key, procdir)


def info(context):
    """
    Prints information about the context.
    """
    print("{:<15s}{:>6s}".format("NAME", "STATUS"))
    for name, _ in context.items():
        if name not in MEMGRAPH_INSTANCES:
            continue
        instance = MEMGRAPH_INSTANCES[name]
        print("{:<15s}{:>6s}".format(name, "UP" if instance.is_running() else "DOWN"))


def process_actions(instances, data):
    """
    Processes all `actions` using the `context` as context.
    """
    data = data.split(" ")
    data.reverse()
    while len(data) > 0:
        arg = data.pop()
        if arg not in ACTIONS:
            log.error(f"{arg} is unknown action")
            continue
        action = ACTIONS[arg]

        args_no = len(signature(action).parameters) - 1
        assert (
            args_no == 0 or args_no == 1
        ), "Wrong action definition, each action has to accept at least [0,1] argument which is the context."

        if args_no == 0:
            action(instances)
        elif args_no == 1:
            if len(data) == 0:
                log.error(f"Not enough args provided. Expected 1 but found 0 for action {arg}")
            else:
                action(instances, data.pop())


if __name__ == "__main__":
    args = load_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(asctime)s %(name)s] %(message)s")

    context = None
    if args.context_yaml == "":
        context = MEMGRAPH_INSTANCES_DESCRIPTION
    else:
        with open(args.context_yaml, "r") as f:
            context = yaml.load(f, Loader=yaml.FullLoader)

    if args.actions != "" and context is not None:
        process_actions(context, args.actions)
        sys.exit(0)

    while True:
        choice = read_action_line("ACTION>")
        process_actions(context, choice)
