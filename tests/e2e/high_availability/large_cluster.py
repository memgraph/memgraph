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

import os
import sys
import time
from functools import partial

import interactive_mg_runner
import pytest
from common import (
  connect,
  execute_and_fetch_all,
  get_data_path,
  get_logs_path,
  get_vertex_count,
  show_instances,
)
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

file = "large_cluster"

# Cluster configuration constants
NUM_INSTANCES = 15
NUM_COORDINATORS = 3

INSTANCE_BOLT_PORT_START = 7675
INSTANCE_MANAGEMENT_PORT_START = 10001
INSTANCE_REPLICATION_PORT_START = 10106

COORDINATOR_BOLT_PORT_START = 7690
COORDINATOR_PORT_START = 10016
COORDINATOR_MANAGEMENT_PORT_START = 10121


@pytest.fixture
def test_name(request):
    return request.node.name


def get_instances_description_no_setup(test_name: str):
    """Generate instance descriptions for 15 data instances and 3 coordinators."""
    result = {}

    # Generate data instances
    for i in range(1, NUM_INSTANCES + 1):
        result[f"instance_{i}"] = {
            "args": [
                f"--bolt-port={INSTANCE_BOLT_PORT_START + i - 1}",
                "--log-level=TRACE",
                f"--management-port={INSTANCE_MANAGEMENT_PORT_START + i - 1}",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_{i}.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_{i}",
            "setup_queries": [],
        }

    # Generate coordinators
    for i in range(1, NUM_COORDINATORS + 1):
        result[f"coordinator_{i}"] = {
            "args": [
                f"--bolt-port={COORDINATOR_BOLT_PORT_START + i - 1}",
                "--log-level=TRACE",
                f"--coordinator-id={i}",
                f"--coordinator-port={COORDINATOR_PORT_START + i - 1}",
                f"--management-port={COORDINATOR_MANAGEMENT_PORT_START + i - 1}",
                "--coordinator-hostname=localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_{i}.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_{i}",
            "setup_queries": [],
        }

    return result


def _get_coordinator_queries():
    """Generate ADD COORDINATOR queries."""
    return [
        f"ADD COORDINATOR {i} WITH CONFIG {{"
        f"'bolt_server': 'localhost:{COORDINATOR_BOLT_PORT_START + i - 1}', "
        f"'coordinator_server': 'localhost:{COORDINATOR_PORT_START + i - 1}', "
        f"'management_server': 'localhost:{COORDINATOR_MANAGEMENT_PORT_START + i - 1}'}}"
        for i in range(1, NUM_COORDINATORS + 1)
    ]


def _get_register_instance_query(instance_num: int, mode: str = None) -> str:
    """Generate a REGISTER INSTANCE query."""
    mode_str = f" AS {mode}" if mode else ""
    return (
        f"REGISTER INSTANCE instance_{instance_num}{mode_str} WITH CONFIG {{"
        f"'bolt_server': 'localhost:{INSTANCE_BOLT_PORT_START + instance_num - 1}', "
        f"'management_server': 'localhost:{INSTANCE_MANAGEMENT_PORT_START + instance_num - 1}', "
        f"'replication_server': 'localhost:{INSTANCE_REPLICATION_PORT_START + instance_num - 1}'}};"
    )


def _get_setup_queries(mode: str = None):
    """Generate setup queries with uniform replication mode."""
    queries = _get_coordinator_queries()
    queries.extend([_get_register_instance_query(i, mode) for i in range(1, NUM_INSTANCES + 1)])
    queries.append("SET INSTANCE instance_1 TO MAIN")
    return queries


def _get_alternating_setup_queries(modes: tuple):
    """Generate setup queries with alternating replication modes."""
    queries = _get_coordinator_queries()
    queries.extend([_get_register_instance_query(i, modes[(i - 1) % 2]) for i in range(1, NUM_INSTANCES + 1)])
    queries.append("SET INSTANCE instance_1 TO MAIN")
    return queries


# All replicas will be registered as SYNC replicas
def get_sync_setup_queries():
    return _get_setup_queries(mode=None)


# All replicas will be registered as STRICT SYNC replicas
def get_strict_sync_setup_queries():
    return _get_setup_queries(mode="STRICT_SYNC")


# Replicas will be registered as SYNC and ASYNC replicas (alternating)
def get_async_sync_setup_queries():
    return _get_alternating_setup_queries(modes=(None, "ASYNC"))


# Replicas will be registered as ASYNC and STRICT_SYNC replicas (alternating)
def get_async_strict_sync_setup_queries():
    return _get_alternating_setup_queries(modes=("ASYNC", "STRICT_SYNC"))


@pytest.fixture(autouse=True)
def cleanup_after_test():
    # Run the test
    yield
    # Stop + delete directories after running the test
    interactive_mg_runner.kill_all(keep_directories=False)


def get_data_cursors():
    return [
        connect(host="localhost", port=port).cursor()
        for port in range(INSTANCE_BOLT_PORT_START, INSTANCE_BOLT_PORT_START + NUM_INSTANCES)
    ]


@pytest.mark.parametrize(
    "setup_queries",
    [
        get_sync_setup_queries,
        get_strict_sync_setup_queries,
        get_async_sync_setup_queries,
        get_async_strict_sync_setup_queries,
    ],
)
def test_replication_works(test_name, setup_queries):
    # Start cluster
    instances = get_instances_description_no_setup(test_name=test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)

    # Register data instances
    leader = connect(host="localhost", port=COORDINATOR_BOLT_PORT_START + NUM_COORDINATORS - 1).cursor()
    for query in setup_queries():
        execute_and_fetch_all(leader, query)

    # Test replication works
    instance_1 = connect(host="localhost", port=INSTANCE_BOLT_PORT_START).cursor()
    execute_and_fetch_all(
        instance_1, "unwind range (1, 5000) as x create (n:Entity {embedding: [0.1+x, 1.1+x, 2.2+x, 3.3+x, 4.4+x]});"
    )
    for cursor in get_data_cursors():
        mg_sleep_and_assert(5000, partial(get_vertex_count, cursor))

    # Test failover works
    interactive_mg_runner.kill(instances, "instance_1")
    time.sleep(7)
    instances = show_instances(leader)
    found_main = False
    for instance in instances:
        if instance[-1] == "main" and instance[0] != "instance_1":
            found_main = True
            break
    assert found_main, "New main not found"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
