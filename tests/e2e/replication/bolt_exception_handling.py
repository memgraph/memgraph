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
E2E test for consistent Bolt exception handling (ClientError vs TransientError).

SessionHL must map QueryException and ReplicationException to ClientError
for both Pull and CommitTransaction paths, so the Neo4j driver sees the same
exception type regardless of where the error occurs (e.g. write on replica,
or commit on main when a sync replica is down).

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


def test_commit_fails_with_client_error_when_sync_replica_is_down_implicit_tx(connection, test_name):
    """
    When a SYNC replica is down, commit on main (implicit tx / auto-commit) fails to replicate.
    SessionHL must map ReplicationException to ClientError, not TransientError.
    """
    _setup_cluster_with_sync_replica_down(test_name)

    driver = GraphDatabase.driver(f"bolt://localhost:{MAIN_PORT}", auth=None, encrypted=False)
    try:
        with driver.session() as session:
            with pytest.raises(ClientError) as exc_info:
                session.run("CREATE (n:CommitWhenSyncDown) RETURN n").consume()
            err_text = str(exc_info.value).lower()
            assert "sync" in err_text or "replica" in err_text or "replication" in err_text
    except TransientError:
        pytest.fail("ReplicationException on commit must be reported as ClientError, not TransientError")
    finally:
        driver.close()


def test_commit_fails_with_client_error_when_sync_replica_is_down_explicit_tx(connection, test_name):
    """
    When a SYNC replica is down, commit on main (explicit transaction) fails to replicate.
    SessionHL must map ReplicationException to ClientError in CommitTransaction path, not TransientError.
    """
    _setup_cluster_with_sync_replica_down(test_name)

    driver = GraphDatabase.driver(f"bolt://localhost:{MAIN_PORT}", auth=None, encrypted=False)
    try:
        with driver.session() as session:
            tx = session.begin_transaction()
            got_client_error = False
            try:
                tx.run("CREATE (n:CommitWhenSyncDownExplicit) RETURN n").consume()
                tx.commit()
            except ClientError as e:
                got_client_error = True
                err_text = str(e).lower()
                assert "sync" in err_text or "replica" in err_text or "replication" in err_text
            except TransientError:
                pytest.fail("ReplicationException on commit must be reported as ClientError, not TransientError")
            assert got_client_error, "Expected ClientError when commit cannot replicate to sync replica"
    finally:
        driver.close()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v", "-rA"]))
