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

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all, get_data_path, get_logs_path
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

file = "replication_metrics"

BOLT_PORTS = {"main": 7687, "replica_1": 7688, "replica_2": 7689}
METRICS_PORTS = {"main": 9091, "replica_1": 9092, "replica_2": 9093}
REPLICATION_PORTS = {"replica_1": 10001, "replica_2": 10002}
STATES = ["ready", "replicating", "recovery", "invalid", "diverged"]
METRIC_LINE = re.compile(r"^(\w+)(?:\{(.*)\})? (\S+)$")
LABEL = re.compile(r'(\w+)="([^"]*)"')


def instances_description(test_name):
    def instance(name, setup_queries):
        return {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS[name]}",
                f"--metrics-port={METRICS_PORTS[name]}",
                "--log-level=TRACE",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/{name}.log",
            "data_directory": f"{get_data_path(file, test_name)}/{name}",
            "setup_queries": setup_queries,
        }

    return {
        "replica_1": instance(
            "replica_1", [f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};"]
        ),
        "replica_2": instance(
            "replica_2", [f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_2']};"]
        ),
        "main": instance(
            "main",
            [
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
                f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';",
            ],
        ),
    }


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


def scrape(instance):
    with urllib.request.urlopen(f"http://localhost:{METRICS_PORTS[instance]}/metrics") as response:
        text = response.read().decode("utf-8")
    samples = {}
    for line in text.splitlines():
        if match := METRIC_LINE.match(line):
            name, labels, value = match.groups()
            samples[(name, frozenset(LABEL.findall(labels or "")))] = float(value)
    return samples


def sample(samples, name, **labels):
    return samples.get((name, frozenset(labels.items())))


def instance_view(instance):
    samples = scrape(instance)
    return {
        "role": {role: sample(samples, "memgraph_replication_role", role=role) for role in ["main", "replica"]},
        "writeable": sample(samples, "memgraph_main_writeable"),
        "registered_replicas": sample(samples, "memgraph_registered_replicas"),
    }


def replica_view(replica):
    samples = scrape("main")
    labels = {"mg_instance": replica, "database": "memgraph"}
    return {
        "state": {state: sample(samples, "memgraph_replica_state", state=state, **labels) for state in STATES},
        "txns_behind": sample(samples, "memgraph_replica_txns_behind", **labels),
    }


def replica_series(instance):
    return sorted(
        {dict(labels).get("mg_instance") for (name, labels) in scrape(instance) if name.startswith("memgraph_replica_")}
        - {None}
    )


def expected_replica(state, txns_behind):
    return {"state": {s: float(s == state) for s in STATES}, "txns_behind": float(txns_behind)}


def test_main_reports_healthy_replicas(test_name):
    interactive_mg_runner.start_all(instances_description(test_name), keep_directories=False)

    for replica in ["replica_1", "replica_2"]:
        mg_sleep_and_assert(expected_replica("ready", 0), lambda: replica_view(replica))

    expected_main = {"role": {"main": 1.0, "replica": 0.0}, "writeable": 1.0, "registered_replicas": 2.0}
    assert instance_view("main") == expected_main


def test_replica_reports_its_role(test_name):
    interactive_mg_runner.start_all(instances_description(test_name), keep_directories=False)

    expected_replica_instance = {"role": {"main": 0.0, "replica": 1.0}, "writeable": 0.0, "registered_replicas": 0.0}
    assert instance_view("replica_1") == expected_replica_instance
    assert replica_series("replica_1") == []


def test_down_replica_falls_behind(test_name):
    instances = instances_description(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)
    cursor = connect(host="localhost", port=BOLT_PORTS["main"]).cursor()
    mg_sleep_and_assert(expected_replica("ready", 0), lambda: replica_view("replica_2"))

    interactive_mg_runner.kill(instances, "replica_2", keep_directories=False)
    for _ in range(3):
        execute_and_fetch_all(cursor, "CREATE ();")

    mg_sleep_and_assert(expected_replica("invalid", 3), lambda: replica_view("replica_2"))
    assert replica_view("replica_1") == expected_replica("ready", 0)


def test_dropped_replica_series_disappear(test_name):
    interactive_mg_runner.start_all(instances_description(test_name), keep_directories=False)
    cursor = connect(host="localhost", port=BOLT_PORTS["main"]).cursor()
    mg_sleep_and_assert(["replica_1", "replica_2"], lambda: replica_series("main"))

    execute_and_fetch_all(cursor, "DROP REPLICA replica_2;")

    assert replica_series("main") == ["replica_1"]
    assert instance_view("main")["registered_replicas"] == 1.0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
