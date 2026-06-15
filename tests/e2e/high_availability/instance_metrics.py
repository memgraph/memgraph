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
import sys
import urllib.request
from functools import partial

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all, get_data_path, get_logs_path, show_instances
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

file = "instance_metrics"

METRICS_URL = "http://localhost:9095/metrics"
# Per-data-instance metrics ports — used to scrape the main (instance_3) for replica info.
DATA_METRICS_URLS = {
    "instance_3": "http://localhost:9096/metrics",
}
INSTANCES = ["instance_1", "instance_2", "instance_3"]


def get_memgraph_instances_description(test_name: str):
    return {
        "instance_1": {
            "args": [
                "--bolt-port",
                "7688",
                "--log-level",
                "TRACE",
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
                "--management-port",
                "10013",
                "--replication-restore-state-on-startup=true",
                "--data-recovery-on-startup=false",
                "--metrics-port=9096",
                "--metrics-format=OpenMetrics",
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
                "--metrics-format=OpenMetrics",
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


def scrape_metrics():
    with urllib.request.urlopen(METRICS_URL) as response:
        return response.read().decode("utf-8")


def test_instance_metrics_present(test_name):
    cursor = setup_test(test_name)

    expected_instances = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader"),
        ("instance_1", "localhost:7688", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7689", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7687", "", "localhost:10013", "up", "main"),
    ]
    mg_sleep_and_assert(expected_instances, partial(show_instances, cursor))

    metrics = scrape_metrics()
    for instance in INSTANCES:
        assert f'memgraph_instance_up{{mg_instance="{instance}"}}' in metrics
        assert f'memgraph_instance_is_main{{mg_instance="{instance}"}}' in metrics
        assert f'memgraph_instance_last_response_seconds{{mg_instance="{instance}"}}' in metrics

    assert 'memgraph_instance_is_leader{mg_instance="coordinator_1"}' in metrics


def _scrape(url):
    with urllib.request.urlopen(url) as response:
        return response.read().decode("utf-8")


def test_replica_info_metrics_on_main(test_name):
    """Tasks 1 + 3: per-(instance, db) replication latency histograms and replica info gauges."""
    setup_test(test_name)

    expected_instances = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader"),
        ("instance_1", "localhost:7688", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7689", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7687", "", "localhost:10013", "up", "main"),
    ]
    coord = connect(host="localhost", port=7690).cursor()
    mg_sleep_and_assert(expected_instances, partial(show_instances, coord))

    # Drive write traffic to the main so replication observations fire.
    main = connect(host="localhost", port=7687).cursor()
    for _ in range(5):
        execute_and_fetch_all(main, "CREATE (:Node {v: 1})")

    def all_replica_info_present():
        body = _scrape(DATA_METRICS_URLS["instance_3"])
        for replica in ("instance_1", "instance_2"):
            # Histogram bucket entries carry all three labels; checking for the shared prefix
            # is enough (OpenMetrics emits labels alphabetically: database, mg_instance, uuid).
            prefix_replica_stream = (
                f'memgraph_replica_stream_seconds_bucket{{database="memgraph",mg_instance="{replica}"'
            )
            prefix_start = f'memgraph_start_txn_replication_seconds_bucket{{database="memgraph",mg_instance="{replica}"'
            prefix_finalize = (
                f'memgraph_finalize_txn_replication_seconds_bucket{{database="memgraph",mg_instance="{replica}"'
            )
            ready_line = f'memgraph_replica_state{{database="memgraph",mg_instance="{replica}",state="READY",uuid='
            if not all(p in body for p in (prefix_replica_stream, prefix_start, prefix_finalize, ready_line)):
                return False
        return 'memgraph_main_commit_timestamp{database="memgraph"' in body

    mg_sleep_and_assert(True, all_replica_info_present)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
