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

file = "replication_failures"

REPLICATION_FAILURES = re.compile(r"^memgraph_replication_failures_total\{(.*)\} (\S+)$", re.MULTILINE)
LABEL = re.compile(r'(\w+)="([^"]*)"')


def instances_description(test_name):
    return {
        "replica": {
            "args": ["--bolt-port", "7688", "--metrics-port=9092", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(file, test_name)}/replica.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10001;"],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--metrics-port=9091", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": ["REGISTER REPLICA replica SYNC TO '127.0.0.1:10001';"],
        },
    }


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


# A down replica is reported as not in sync once main notices, or as an RPC error if the commit gets there first
DOWN_REPLICA_REASONS = {"not_in_sync", "rpc_error"}


def replication_failure_series(database="memgraph"):
    """Returns (labels, value) for each of the database's replication failure series."""
    with urllib.request.urlopen("http://localhost:9091/metrics") as response:
        body = response.read().decode("utf-8")
    series = [(dict(LABEL.findall(labels)), float(value)) for labels, value in REPLICATION_FAILURES.findall(body)]
    return [(labels, value) for labels, value in series if labels.get("database") == database]


def replication_failures(outcome, database="memgraph"):
    """Sums the failures for the replica, after checking each counted series names a reason a down replica can give."""
    total = 0
    for labels, value in replication_failure_series(database):
        if labels.get("outcome") == outcome:
            assert labels.get("mg_instance") == "replica"
            assert value == 0 or labels.get("reason") in DOWN_REPLICA_REASONS
            total += value
    return total


def start_with_replica_down(test_name):
    instances = instances_description(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)
    cursor = connect(host="localhost", port=7687).cursor()
    interactive_mg_runner.kill(instances, "replica", keep_directories=False)
    return cursor


def test_unconfirmed_sync_commit_counts_as_committed_failure(test_name):
    cursor = start_with_replica_down(test_name)
    # Registering the replica creates its series, one per outcome and reason, so the first failure is an increase
    assert [value for _, value in replication_failure_series()] == [0] * 8

    execute_and_fetch_all(cursor, "CREATE ();")

    assert replication_failures("committed") == 1
    assert replication_failures("aborted") == 0


def test_after_commit_trigger_counts_its_own_commit(test_name):
    cursor = start_with_replica_down(test_name)
    execute_and_fetch_all(
        cursor, "CREATE TRIGGER audit ON () CREATE AFTER COMMIT EXECUTE UNWIND createdVertices AS v CREATE (:Audit);"
    )
    before = replication_failures("committed")

    execute_and_fetch_all(cursor, "CREATE ();")

    mg_sleep_and_assert(before + 2, lambda: replication_failures("committed"))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
