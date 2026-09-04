# Copyright 2026 Memgraph Ltd.
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
import re
import sys
import urllib.request
from functools import partial

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all, get_data_path, get_logs_path, show_instances
from mg_utils import mg_sleep_and_assert, mg_sleep_and_assert_eval_function

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

file = "instance_metrics"

METRICS_URL = "http://localhost:9095/metrics"
INSTANCES = ["instance_1", "instance_2", "instance_3"]
INSTANCE_METRICS_PORTS = {"instance_1": 9091, "instance_2": 9092, "instance_3": 9093}


def get_memgraph_instances_description(test_name: str):
    return {
        "instance_1": {
            "args": [
                "--bolt-port",
                "7688",
                "--log-level",
                "TRACE",
                "--metrics-port=9091",
                "--management-port",
                "10011",
                "--replication-restore-state-on-startup=true",
                "--data-recovery-on-startup=false",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_1",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--bolt-port",
                "7689",
                "--log-level",
                "TRACE",
                "--metrics-port=9092",
                "--management-port",
                "10012",
                "--replication-restore-state-on-startup=true",
                "--data-recovery-on-startup=false",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_2",
            "setup_queries": [],
        },
        "instance_3": {
            "args": [
                "--bolt-port",
                "7687",
                "--log-level",
                "TRACE",
                "--metrics-port=9093",
                "--management-port",
                "10013",
                "--replication-restore-state-on-startup=true",
                "--data-recovery-on-startup=false",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_3",
            "setup_queries": [],
        },
        "coordinator_1": {
            "args": [
                "--bolt-port",
                "7690",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10111",
                "--coordinator-hostname",
                "localhost",
                "--management-port",
                "10121",
                "--metrics-port=9095",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_1",
            "setup_queries": [
                "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
                "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
                "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
                "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
                "SET INSTANCE instance_3 TO MAIN;",
            ],
        },
    }


def setup_test(test_name: str):
    interactive_mg_runner.start_all(get_memgraph_instances_description(test_name), keep_directories=False)
    return connect(host="localhost", port=7690).cursor()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


EXPECTED_INSTANCES = [
    ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader"),
    ("instance_1", "localhost:7688", "", "localhost:10011", "up", "replica"),
    ("instance_2", "localhost:7689", "", "localhost:10012", "up", "replica"),
    ("instance_3", "localhost:7687", "", "localhost:10013", "up", "main"),
]
UUID_LABEL = re.compile(r'uuid="([^"]*)"')


def scrape_metrics(port: int = 9095):
    with urllib.request.urlopen(f"http://localhost:{port}/metrics") as response:
        return response.read().decode("utf-8")


def default_db_uuids():
    """The uuid label each instance presents for its default database.

    Label order within a series is not guaranteed, so match the labels independently.
    """
    uuids = {}
    for instance, port in INSTANCE_METRICS_PORTS.items():
        uuids[instance] = None
        for line in scrape_metrics(port).splitlines():
            if not line.startswith("memgraph_vertex_count{") or 'database="memgraph"' not in line:
                continue
            if match := UUID_LABEL.search(line):
                uuids[instance] = match.group(1)
                break
    return uuids


def test_instance_metrics_present(test_name):
    cursor = setup_test(test_name)

    mg_sleep_and_assert(EXPECTED_INSTANCES, partial(show_instances, cursor))

    metrics = scrape_metrics()
    for instance in INSTANCES:
        assert f'memgraph_instance_up{{mg_instance="{instance}"}}' in metrics
        assert f'memgraph_instance_is_main{{mg_instance="{instance}"}}' in metrics
        assert f'memgraph_instance_last_response_seconds{{mg_instance="{instance}"}}' in metrics

    assert 'memgraph_instance_is_leader{mg_instance="coordinator_1"}' in metrics


# A data instance joining the cluster adopts the main's default-database uuid, so every instance must
# present the same uuid label for database="memgraph". Before this was fixed each instance kept the
# uuid of its own discarded local default database, which made the series impossible to aggregate.
def test_default_db_uuid_label_agrees_across_instances(test_name):
    cursor = setup_test(test_name)

    mg_sleep_and_assert(EXPECTED_INSTANCES, partial(show_instances, cursor))

    # The replicas realign onto the main's uuid during system recovery, so retry until they
    # converge. Retrying on the dict means a timeout reports the uuids each instance presented.
    def agreed(uuids):
        return None not in uuids.values() and len(set(uuids.values())) == 1

    mg_sleep_and_assert_eval_function(agreed, default_db_uuids)

    # The entry id keys the families internally and must never reach a scrape.
    for port in INSTANCE_METRICS_PORTS.values():
        assert "mgentry" not in scrape_metrics(port)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
