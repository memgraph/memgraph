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
from mg_utils import mg_sleep_and_assert_eval_function

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

file = "replication_latency_metrics"

BOLT_PORTS = {"main": 7687, "replica_1": 7688, "replica_2": 7689}
REPLICATION_PORTS = {"replica_1": 10001, "replica_2": 10002}
COUNT_LINE = re.compile(r"^(\w+)_count(?:\{(.*)\})? (\S+)$", re.MULTILINE)
LABEL = re.compile(r'(\w+)="([^"]*)"')

COMMIT_HISTOGRAMS = [
    "memgraph_start_txn_replication_seconds",
    "memgraph_finalize_txn_replication_seconds",
    "memgraph_prepare_commit_rpc_seconds",
    "memgraph_replica_stream_seconds",
]
RECOVERY_RPC_HISTOGRAMS = [
    "memgraph_snapshot_rpc_seconds",
    "memgraph_wal_files_rpc_seconds",
    "memgraph_current_wal_rpc_seconds",
]
LABELLED_FAMILIES = (
    COMMIT_HISTOGRAMS
    + RECOVERY_RPC_HISTOGRAMS
    + [
        "memgraph_heartbeat_rpc_seconds",
        "memgraph_frequent_heartbeat_rpc_seconds",
        "memgraph_system_recovery_rpc_seconds",
        "memgraph_replica_recovery_success_total",
        "memgraph_replica_recovery_fail_total",
        "memgraph_replica_recovery_skip_total",
    ]
)


def instances_description(test_name):
    def instance(name, setup_queries):
        return {
            "args": ["--bolt-port", f"{BOLT_PORTS[name]}", "--log-level=TRACE"]
            + (["--metrics-port=9091"] if name == "main" else []),
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


def scrape():
    with urllib.request.urlopen("http://localhost:9091/metrics") as response:
        return response.read().decode("utf-8")


def counts(name):
    """Observation count of histogram `name` (or value of counter `name`), keyed by the series' label set."""
    body = scrape()
    pattern = COUNT_LINE if name.endswith("_seconds") else re.compile(rf"^({name})(?:\{{(.*)\}})? (\S+)$", re.MULTILINE)
    return {
        frozenset(LABEL.findall(labels or "")): float(value)
        for family, labels, value in pattern.findall(body)
        if family == name
    }


def count(name, **labels):
    return counts(name).get(frozenset(labels.items()), 0.0)


def test_commit_latencies_are_labelled_by_replica(test_name):
    interactive_mg_runner.start_all(instances_description(test_name), keep_directories=False)
    cursor = connect(host="localhost", port=BOLT_PORTS["main"]).cursor()

    for _ in range(3):
        execute_and_fetch_all(cursor, "CREATE ();")

    for name in COMMIT_HISTOGRAMS:
        mg_sleep_and_assert_eval_function(lambda c: c >= 3, lambda: count(name, mg_instance="replica_1"))
    for replica in ["replica_1", "replica_2"]:
        mg_sleep_and_assert_eval_function(
            lambda c: c > 0, lambda: count("memgraph_heartbeat_rpc_seconds", mg_instance=replica)
        )
        mg_sleep_and_assert_eval_function(
            lambda c: c > 0, lambda: count("memgraph_frequent_heartbeat_rpc_seconds", mg_instance=replica)
        )


def test_no_replication_series_without_an_instance_label(test_name):
    interactive_mg_runner.start_all(instances_description(test_name), keep_directories=False)
    cursor = connect(host="localhost", port=BOLT_PORTS["main"]).cursor()
    execute_and_fetch_all(cursor, "CREATE ();")

    for name in LABELLED_FAMILIES:
        assert all("mg_instance" in dict(labels) for labels in counts(name)), f"{name} has a series without mg_instance"


def test_recovery_is_counted_for_the_recovered_replica(test_name):
    instances = instances_description(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)
    cursor = connect(host="localhost", port=BOLT_PORTS["main"]).cursor()

    interactive_mg_runner.kill(instances, "replica_2")
    execute_and_fetch_all(cursor, "CREATE ();")
    interactive_mg_runner.start(instances, "replica_2", run_setup_queries=False)

    mg_sleep_and_assert_eval_function(
        lambda c: c >= 1,
        lambda: count("memgraph_replica_recovery_success_total", mg_instance="replica_2"),
    )
    assert sum(count(name, mg_instance="replica_2") for name in RECOVERY_RPC_HISTOGRAMS) >= 1
    assert count("memgraph_replica_recovery_success_total", mg_instance="replica_1") == 0


def test_dropped_replica_series_disappear(test_name):
    interactive_mg_runner.start_all(instances_description(test_name), keep_directories=False)
    cursor = connect(host="localhost", port=BOLT_PORTS["main"]).cursor()
    execute_and_fetch_all(cursor, "CREATE ();")
    mg_sleep_and_assert_eval_function(
        lambda c: c >= 1,
        lambda: count("memgraph_start_txn_replication_seconds", mg_instance="replica_2"),
    )

    execute_and_fetch_all(cursor, "DROP REPLICA replica_2;")

    for name in LABELLED_FAMILIES:
        assert not any(dict(labels).get("mg_instance") == "replica_2" for labels in counts(name)), name


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
