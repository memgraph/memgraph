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

"""
E2E test for how the Bolt layer reports replication problems.

A SYNC replica the main cannot reach does not fail the transaction: it is committed on the main and on every
reachable replica, so it is reported through a WARNING notification (both for implicit and explicit
transactions) rather than as an error. Errors that do fail a query (e.g. a write on a replica) must still reach
the Neo4j driver as a ClientError, never as a TransientError, so drivers do not retry them.

Cluster is fully defined and started from this module (no cluster in workloads.yaml),
following the same pattern as show_while_creating_invalid_state.py and switching_roles.py.
"""

import os
import sys

import interactive_mg_runner
import pytest

# Neo4j driver is used to assert ClientError vs TransientError (Bolt failure code).
neo4j = pytest.importorskip("neo4j", reason="neo4j driver required for Bolt exception type checks")

from common import connect, execute_and_fetch_all, get_data_path, get_logs_path
from mg_utils import mg_sleep_and_assert
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError, TransientError

# interactive_mg_runner paths (same pattern as show_while_creating_invalid_state.py)
interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

FILE = "bolt_exception_handling"
MAIN_PORT = 7687
REPLICA_PORT = 7688


@pytest.fixture(autouse=True)
def cleanup_after_test():
    interactive_mg_runner.kill_all(keep_directories=False)
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


def get_instances_description(test_name: str):
    """Cluster: replica_1 and replica_2 SYNC, replica_3 ASYNC. Order: replicas first then main."""
    return {
        "replica_1": {
            "args": ["--bolt-port", "7688", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(FILE, test_name)}/replica1.log",
            "data_directory": f"{get_data_path(FILE, test_name)}/replica1",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10001;"],
        },
        "replica_2": {
            "args": ["--bolt-port", "7689", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(FILE, test_name)}/replica2.log",
            "data_directory": f"{get_data_path(FILE, test_name)}/replica2",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
        },
        "replica_3": {
            "args": ["--bolt-port", "7690", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(FILE, test_name)}/replica3.log",
            "data_directory": f"{get_data_path(FILE, test_name)}/replica3",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10003;"],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(FILE, test_name)}/main.log",
            "data_directory": f"{get_data_path(FILE, test_name)}/main",
            "setup_queries": [
                "REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001'",
                "REGISTER REPLICA replica_2 SYNC TO '127.0.0.1:10002'",
                "REGISTER REPLICA replica_3 ASYNC TO '127.0.0.1:10003'",
            ],
        },
    }


@pytest.mark.parametrize(
    "port, role",
    [(7687, "main"), (7688, "replica"), (7689, "replica"), (7690, "replica")],
)
def test_show_replication_role(port, role, connection, test_name):
    """Reuse same fixture as other replication tests to ensure cluster is up."""
    instances = get_instances_description(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)
    cursor = connection(port, role).cursor()
    data = execute_and_fetch_all(cursor, "SHOW REPLICATION ROLE;")
    assert data[0][0] == role


def _setup_cluster_with_sync_replica_down(test_name):
    """Start cluster, stop one SYNC replica, wait until main sees it invalid. Returns instances."""
    instances = get_instances_description(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)
    interactive_mg_runner.stop(instances, "replica_1")

    def replica_1_is_invalid():
        replicas = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICAS;")
        for name, _ip, _mode, _sys, info in replicas:
            if name == "replica_1":
                return info["memgraph"]["status"] == "invalid"
        return False

    mg_sleep_and_assert(True, replica_1_is_invalid)
    return instances


def _sync_replication_failure_notifications(summary):
    return [n for n in (summary.notifications or []) if n.get("code") == "SyncReplicationFailure"]


def test_commit_reports_notification_when_sync_replica_is_down_implicit_tx(connection, test_name):
    """
    When a SYNC replica is down, commit on main (implicit tx / auto-commit) still succeeds: the transaction is
    committed on the main and on the reachable replicas, and the replica that could not be reached is reported
    through a WARNING notification instead of an error.
    """
    _setup_cluster_with_sync_replica_down(test_name)

    driver = GraphDatabase.driver(f"bolt://localhost:{MAIN_PORT}", auth=None, encrypted=False)
    try:
        with driver.session() as session:
            summary = session.run("CREATE (n:CommitWhenSyncDown) RETURN n").consume()

            notifications = _sync_replication_failure_notifications(summary)
            assert len(notifications) == 1, f"Expected one SyncReplicationFailure notification, got {notifications}"
            notification = notifications[0]
            assert notification["severity"] == "WARNING"
            assert "Failed to replicate to SYNC replica 'replica_1'" in notification["title"]
            assert "SHOW REPLICAS" in notification["title"]

            # The write is visible on the main even though one SYNC replica never saw it.
            assert session.run("MATCH (n:CommitWhenSyncDown) RETURN count(n)").single()[0] == 1
    finally:
        driver.close()

    # The down replica is the only instance missing the write; the healthy SYNC replica has it.
    replica_2_cursor = connection(7689, "replica").cursor()
    assert execute_and_fetch_all(replica_2_cursor, "MATCH (n:CommitWhenSyncDown) RETURN count(n)")[0][0] == 1


def test_commit_succeeds_when_sync_replica_is_down_explicit_tx(connection, test_name):
    """
    Same as above for an explicit transaction: the COMMIT must not fail, so no ClientError or TransientError is
    raised and the write is visible on the main.
    """
    _setup_cluster_with_sync_replica_down(test_name)

    driver = GraphDatabase.driver(f"bolt://localhost:{MAIN_PORT}", auth=None, encrypted=False)
    try:
        with driver.session() as session:
            tx = session.begin_transaction()
            tx.run("CREATE (n:CommitWhenSyncDownExplicit) RETURN n").consume()
            tx.commit()

            assert session.run("MATCH (n:CommitWhenSyncDownExplicit) RETURN count(n)").single()[0] == 1
    finally:
        driver.close()


def test_write_on_replica_is_client_error(connection, test_name):
    """
    SessionHL must keep mapping QueryException to ClientError: a write on a replica is a non-retryable client
    error, not a TransientError.
    """
    instances = get_instances_description(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)

    driver = GraphDatabase.driver(f"bolt://localhost:{REPLICA_PORT}", auth=None, encrypted=False)
    try:
        with driver.session() as session:
            with pytest.raises(ClientError) as exc_info:
                session.run("CREATE (n:WriteOnReplica) RETURN n").consume()
            assert "Write queries are forbidden on the replica instance" in str(exc_info.value)
    except TransientError:
        pytest.fail("QueryException on a replica must be reported as ClientError, not TransientError")
    finally:
        driver.close()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v", "-rA"]))
