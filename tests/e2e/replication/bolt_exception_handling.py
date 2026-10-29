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

# Unparseable temporal inputs; date/localtime/localdatetime throw temporal::InvalidArgumentException, duration a bare utils::BasicException.
_INVALID_TEMPORAL = [
    ("RETURN duration('PXYZ')", "Invalid duration string", "duration"),
    ("RETURN date('not-a-date')", "Invalid year in the string", "date"),
    ("RETURN localtime('99:99')", "invalid hour parameter", "localtime"),
    ("RETURN localdatetime('bogus')", "Invalid LocalDateTime format", "localdatetime"),
]

# Enterprise-gated paths: impersonation (RuntimeConfig::Configure is compiled only under MG_ENTERPRISE) and
# SHOW METRICS INFO (license-gated in src/query/interpreter.cpp). A community run must skip rather than fail.
requires_enterprise = pytest.mark.skipif(
    not (os.environ.get("MEMGRAPH_ENTERPRISE_LICENSE") and os.environ.get("MEMGRAPH_ORGANIZATION_NAME")),
    reason="enterprise-gated feature (MEMGRAPH_ENTERPRISE_LICENSE + MEMGRAPH_ORGANIZATION_NAME)",
)


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


def _start_main(test_name):
    """Start a single main instance (no replicas): the Bolt classification tests need only one node."""
    instances = {
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE"],
            "log_file": f"{get_logs_path(FILE, test_name)}/main.log",
            "data_directory": f"{get_data_path(FILE, test_name)}/main",
            "setup_queries": [],
        }
    }
    interactive_mg_runner.start_all(instances, keep_directories=False)
    return instances


def _driver(**kwargs):
    return GraphDatabase.driver(f"bolt://localhost:{MAIN_PORT}", auth=None, encrypted=False, **kwargs)


def _transient_errors(session):
    """Value of the global TransientErrors counter (SHOW METRICS INFO)."""
    for record in session.run("SHOW METRICS INFO"):
        if record["name"] == "TransientErrors":
            return record["value"]
    raise AssertionError("TransientErrors metric not found in SHOW METRICS INFO")


@pytest.mark.parametrize(
    "query, message",
    [(q, m) for q, m, _ in _INVALID_TEMPORAL],
    ids=[i for _, _, i in _INVALID_TEMPORAL],
)
def test_invalid_temporal_input_is_client_error(query, message, test_name):
    """A permanently-invalid temporal string is a non-retryable ClientError, never a TransientError."""
    _start_main(test_name)
    driver = _driver()
    try:
        with driver.session() as session:
            with pytest.raises(ClientError) as exc_info:
                session.run(query).consume()
            assert exc_info.value.code.startswith("Memgraph.ClientError")
            assert message in str(exc_info.value)
    except TransientError as e:
        pytest.fail(f"{query} must be a ClientError, got TransientError: {e}")
    finally:
        driver.close()


@requires_enterprise
def test_invalid_temporal_input_does_not_increment_transient_errors(test_name):
    """A permanently-invalid temporal parse is never counted as a transient error (skipped on a community build: SHOW METRICS INFO is enterprise-gated)."""
    _start_main(test_name)
    driver = _driver()
    try:
        with driver.session() as session:
            before = _transient_errors(session)
            for query, _ in [(q, m) for q, m, _ in _INVALID_TEMPORAL]:
                try:
                    session.run(query).consume()
                except (ClientError, TransientError):
                    pass
            after = _transient_errors(session)
            assert after == before, f"TransientErrors grew by {after - before} on permanently-invalid input"
    finally:
        driver.close()


def test_project_distinct_graph_is_client_error(test_name):
    """An unwrapped TypedValueException (graph hashing) is a ClientError, not retryable."""
    _start_main(test_name)
    driver = _driver()
    try:
        with driver.session() as session:
            session.run("CREATE (a:A)-[:R]->(b:B)").consume()
            with pytest.raises(ClientError) as exc_info:
                session.run("MATCH p=(a)-->(b) WITH DISTINCT project(p) AS g RETURN g").consume()
            assert "Unsupported hash function for Graph" in str(exc_info.value)
    except TransientError as e:
        pytest.fail(f"Graph hashing failure must be a ClientError, got TransientError: {e}")
    finally:
        driver.close()


def test_managed_tx_invalid_temporal_fails_first_attempt(test_name):
    """The driver must not retry an invalid temporal query inside a managed transaction."""
    _start_main(test_name)
    driver = _driver(
        max_transaction_retry_time=5.0,
        initial_retry_delay=1.0,
        retry_delay_multiplier=2.0,
        retry_delay_jitter_factor=0.1,
    )
    attempts = {"n": 0}

    def work(tx):
        attempts["n"] += 1
        # An unambiguously invalid literal: `P1M` is valid ISO-8601 (one month) and only rejected because
        # months are currently unsupported, so it could become parseable; `PXYZ` never can.
        return list(tx.run("RETURN duration('PXYZ')"))

    try:
        with pytest.raises(ClientError) as exc_info:
            with driver.session() as session:
                session.execute_read(work)
        assert exc_info.value.code.startswith("Memgraph.ClientError")
    except TransientError as e:
        pytest.fail(f"Invalid temporal query must fail with ClientError on the first attempt, got TransientError: {e}")
    finally:
        driver.close()
    assert attempts["n"] == 1, f"driver retried the query {attempts['n'] - 1} time(s); a ClientError must not be retried"


def test_retryable_trigger_failure_stays_transient_error(test_name):
    """
    A before-commit trigger that fails with a declared-retryable exception (HintedAbortError, OOM, a serialization
    conflict) must stay TransientError after Interpreter::Commit re-wraps the trigger's BasicException. A lowered
    query timeout makes a deliberately long-running trigger body abort with a retryable HintedAbortError(TIMEOUT);
    the client must see TransientError carrying the trigger-rewrap message.
    """
    _start_main(test_name)
    driver = _driver()
    try:
        with driver.session() as session:
            # Seed enough nodes that a cartesian trigger body cannot finish before the (lowered) deadline.
            session.run("UNWIND range(1, 20000) AS x CREATE (:Seed)").consume()
            session.run("SET DATABASE SETTING 'query.timeout' TO '1'").consume()
            session.run(
                "CREATE TRIGGER slow_before_commit ON CREATE BEFORE COMMIT EXECUTE "
                "MATCH (a:Seed), (b:Seed), (c:Seed) WITH count(*) AS cnt CREATE (:SlowTriggerResult {cnt: cnt})"
            ).consume()
            with pytest.raises(TransientError) as exc_info:
                session.run("CREATE (:TriggerSource)").consume()
            assert "slow_before_commit" in str(exc_info.value)
            assert "caused the transaction to fail" in str(exc_info.value)
    finally:
        driver.close()


@requires_enterprise
def test_impersonation_failure_is_client_error(test_name):
    """A permanently-invalid impersonation request is a ClientError, never a TransientError."""
    _start_main(test_name)
    driver = _driver()
    try:
        with pytest.raises(ClientError) as exc_info:
            with driver.session(impersonated_user="ghost") as session:
                session.run("RETURN 1").consume()
        assert "Trying to impersonate" in str(exc_info.value)
    except TransientError as e:
        pytest.fail(f"Impersonating a nonexistent user must be a ClientError, got TransientError: {e}")
    finally:
        driver.close()


def test_storage_access_timeout_remains_transient_error(test_name):
    """The storage access-timeout family stays TransientError."""
    _start_main(test_name)
    driver = _driver()
    holder = None
    try:
        holder = driver.session()
        tx = holder.begin_transaction()
        # Hold shared (write) storage access in an open explicit transaction...
        tx.run("CREATE (n:HoldLock)").consume()
        # ...so a unique-access operation on another session times out (default 1s).
        with driver.session() as other:
            with pytest.raises(TransientError) as exc_info:
                other.run("DROP GRAPH").consume()
            assert "Cannot get unique access to the storage" in str(exc_info.value)
        tx.rollback()
    finally:
        driver.close()


def test_serialization_conflict_remains_transient_error(test_name):
    """A write-write serialization conflict stays TransientError."""
    _start_main(test_name)
    driver = _driver()
    try:
        with driver.session() as session:
            session.run("CREATE (n:Conflict {p: 0})").consume()
        first = driver.session()
        second = driver.session()
        tx1 = first.begin_transaction()
        try:
            tx1.run("MATCH (n:Conflict) SET n.p = 1").consume()
            with pytest.raises(TransientError) as exc_info:
                tx2 = second.begin_transaction()
                tx2.run("MATCH (n:Conflict) SET n.p = 2").consume()
                tx2.commit()
            assert "conflicting transactions" in str(exc_info.value)
        finally:
            try:
                tx1.rollback()
            except Exception:
                pass
            first.close()
            second.close()
    finally:
        driver.close()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v", "-rA"]))
