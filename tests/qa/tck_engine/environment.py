# -*- coding: utf-8 -*-

import atexit
import datetime
import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from fcntl import fcntl, F_GETFL, F_SETFL
from steps.test_parameters import TestParameters
from neo4j.v1 import GraphDatabase, basic_auth
from steps.graph_properties import GraphProperties
from test_results import TestResults

# Constants - Memgraph flags
COMMON_FLAGS = ["--durability-enabled=false",
                "--snapshot-on-exit=false",
                "--db-recover-on-startup=false"]
DISTRIBUTED_FLAGS = ["--num-workers", str(6),
                     "--rpc-num-client-workers", str(6),
                     "--rpc-num-server-workers", str(6)]
MASTER_PORT  = 10000
MASTER_FLAGS = ["--master",
                "--master-port", str(MASTER_PORT)]
MEMGRAPH_PORT = 7687

# Module-scoped variables
test_results = TestResults()
temporary_directory = tempfile.TemporaryDirectory()


# Helper functions
def get_script_path():
    return os.path.dirname(os.path.realpath(__file__))


def start_process(cmd, stdout=subprocess.DEVNULL,
                  stderr=subprocess.PIPE, **kwargs):
    ret = subprocess.Popen(cmd, stdout=stdout, stderr=stderr, **kwargs)
    # set the O_NONBLOCK flag of process stderr file descriptor
    if stderr == subprocess.PIPE:
        flags = fcntl(ret.stderr, F_GETFL)  # get current stderr flags
        fcntl(ret.stderr, F_SETFL, flags | os.O_NONBLOCK)
    return ret


def is_tested_system_active(context):
    return all(proc.poll() is None for proc in context.memgraph_processes)


def is_tested_system_inactive(context):
    return not any(proc.poll() is None for proc in context.memgraph_processes)


def get_worker_flags(worker_id):
    flags = ["--worker",
             "--worker-id", str(worker_id),
             "--worker-port", str(10000 + worker_id),
             "--master-port", str(10000)]
    return flags


def wait_for_server(port, delay=0.01):
    cmd = ["nc", "-z", "-w", "1", "127.0.0.1", str(port)]
    count = 0
    while subprocess.call(cmd) != 0:
        time.sleep(0.01)
        if count > 20 / 0.01:
            print("Could not wait for server on port", port, "to startup!")
            sys.exit(1)
        count += 1
    time.sleep(delay)


def run_memgraph(context, flags, distributed):
    if distributed:
        memgraph_binary = "memgraph_distributed"
    else:
        memgraph_binary = "memgraph"
    memgraph_cmd = [os.path.join(context.memgraph_dir, memgraph_binary)]
    memgraph_subprocess = start_process(memgraph_cmd + flags)
    context.memgraph_processes.append(memgraph_subprocess)


def start_memgraph(context):
    if context.config.distributed:  # Run distributed
        flags = COMMON_FLAGS.copy()
        if context.config.memgraph_params:
            flags += context.extra_flags
        master_flags = flags.copy()
        master_flags.append("--durability-directory=" + os.path.join(
            temporary_directory.name, "master"))
        run_memgraph(context, master_flags + DISTRIBUTED_FLAGS + MASTER_FLAGS,
                     context.config.distributed)
        wait_for_server(MASTER_PORT, 0.5)
        for i in range(1, int(context.config.num_machines)):
            worker_flags = flags.copy()
            worker_flags.append("--durability-directory=" + os.path.join(
                temporary_directory.name, "worker" + str(i)))
            run_memgraph(context, worker_flags + DISTRIBUTED_FLAGS +
                         get_worker_flags(i), context.config.distributed)
            wait_for_server(MASTER_PORT + i, 0.5)
    else:  # Run single machine memgraph
        flags = COMMON_FLAGS.copy()
        if context.config.memgraph_params:
            flags += context.extra_flags
        flags.append("--durability-directory=" + temporary_directory.name)
        run_memgraph(context, flags, context.config.distributed)
    assert is_tested_system_active(context), "Failed to start memgraph"
    wait_for_server(MEMGRAPH_PORT, 0.5)  # wait for memgraph to start


def cleanup(context):
    if context.config.database == "memgraph":
        list(map(lambda p: p.kill(), context.memgraph_processes))
        list(map(lambda p: p.wait(), context.memgraph_processes))
        assert is_tested_system_inactive(context), "Failed to stop memgraph"
        context.memgraph_processes.clear()


def get_test_suite(context):
    """
    Returns test suite from a test root folder.
    If test root is a feature file, name of file is returned without
    .feature extension.
    """
    root = context.config.root

    if root.endswith("/"):
        root = root[0:len(root) - 1]
    if root.endswith("features"):
        root = root[0: len(root) - len("features") - 1]

    test_suite = root.split('/')[-1]

    return test_suite


def set_logging(context):
    """
    Initializes log and sets logging level to debug.
    """
    logging.basicConfig(level="DEBUG")
    log = logging.getLogger(__name__)
    context.log = log


def create_db_driver(context):
    """
    Creates database driver and returns it.
    """
    uri = context.config.database_uri
    auth_token = basic_auth(
        context.config.database_username, context.config.database_password)
    if context.config.database == "neo4j" or \
            context.config.database == "memgraph":
        driver = GraphDatabase.driver(uri, auth=auth_token, encrypted=0)
    else:
        raise "Unsupported database type"
    return driver


# Behave specific functions
def before_step(context, step):
    """
    Executes before every step. Checks if step is execution
    step and sets context variable to true if it is.
    """
    context.execution_step = False
    if step.name == "executing query":
        context.execution_step = True


def before_scenario(context, scenario):
    """
    Executes before every scenario. Initializes test parameters,
    graph properties, exception and test execution time.
    """
    if context.config.database == "memgraph":
        # Check if memgraph is up and running
        if is_tested_system_active(context):
            context.is_tested_system_restarted = False
        else:
            cleanup(context)
            start_memgraph(context)
            context.is_tested_system_restarted = True
    context.test_parameters = TestParameters()
    context.graph_properties = GraphProperties()
    context.exception = None
    context.execution_time = None


def before_all(context):
    """
    Executes before running tests. Initializes driver and latency
    dict and creates needed directories.
    """
    timestamp = datetime.datetime.fromtimestamp(
        time.time()).strftime("%Y_%m_%d__%H_%M_%S")
    latency_file = "latency/" + context.config.database + "/" + \
        get_test_suite(context) + "/" + timestamp + ".json"
    if not os.path.exists(os.path.dirname(latency_file)):
        os.makedirs(os.path.dirname(latency_file))
    context.latency_file = latency_file
    context.js = dict()
    context.js["metadata"] = dict()
    context.js["metadata"]["execution_time_unit"] = "seconds"
    context.js["data"] = dict()
    set_logging(context)
    # set config for memgraph
    context.memgraph_processes = []
    script_path = get_script_path()
    context.memgraph_dir = os.path.realpath(
        os.path.join(script_path, "../../../build"))
    if not os.path.exists(context.memgraph_dir):
        context.memgraph_dir = os.path.realpath(
            os.path.join(script_path, "../../../build_debug"))
    if context.config.memgraph_params:
        params = context.config.memgraph_params.strip("\"")
        context.extra_flags = params.split()
    atexit.register(cleanup, context)
    if context.config.database == "memgraph":
        start_memgraph(context)
        context.driver = create_db_driver(context)


def after_scenario(context, scenario):
    """
    Executes after every scenario. Pauses execution if flags are set.
    Adds execution time to latency dict if it is not None.
    """
    err_output = [p.stderr.read()  # noqa unused variable
                  for p in context.memgraph_processes]
    # print error output for each subprocess if scenario failed
    if scenario.status == "failed":
        for i, err in enumerate(err_output):
            if err:
                err = err.decode("utf-8")
                print("\n", "-" * 5, "Machine {}".format(i), "-" * 5)
                list(map(print, [line for line in err.splitlines()]))
    test_results.add_test(scenario.status, context.is_tested_system_restarted)
    if context.config.single_scenario or \
            (context.config.single_fail and scenario.status == "failed"):
        print("Press enter to continue")
        sys.stdin.readline()

    if context.execution_time is not None:
        context.js['data'][scenario.name] = {
            "execution_time": context.execution_time, "status": scenario.status
        }


def after_feature(context, feature):
    """
    Executes after every feature. If flag is set, pauses before
    executing next scenario.
    """
    if context.config.single_feature:
        print("Press enter to continue")
        sys.stdin.readline()


def after_all(context):
    """
    Executes when testing is finished. Creates JSON files of test latency
    and test results.
    """
    context.driver.close()
    timestamp = datetime.datetime.fromtimestamp(
        time.time()).strftime("%Y_%m_%d__%H_%M")

    test_suite = get_test_suite(context)
    file_name = context.config.output_folder + timestamp + \
        "-" + context.config.database + "-" + context.config.test_name + \
        ".json"

    js = {
        "total": test_results.num_total(),
        "passed": test_results.num_passed(),
        "restarts": test_results.num_restarts(),
        "test_suite": test_suite,
        "timestamp": timestamp,
        "db": context.config.database
    }
    with open(file_name, 'w') as f:
        json.dump(js, f)

    with open(context.latency_file, "a") as f:
        json.dump(context.js, f)
