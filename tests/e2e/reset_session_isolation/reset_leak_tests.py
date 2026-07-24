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
E2E regression tests for the Bolt RESET-scoped "sticky state" leak fixes.

A pooled Bolt connection can be handed to an unrelated session after a RESET. Per-connection
state that used to leak across that boundary and is now cleared by ResetForConnectionReuse()
(called only from SessionHL::Abort(), i.e. RESET/LogOff, never a mid-session rollback):
isolation-level overrides (SET NEXT/SESSION TRANSACTION ISOLATION LEVEL) and USE DATABASE
(including a stale in_explicit_db_ that used to block a later USE). SET SESSION TRACE/SETTING
are the same leak class, unit-tested in tests/unit/logging.cpp.

These tests need a real wire-level RESET, which a clean session.close() does not send. The neo4j
driver auto-RESETs a connection only after a FAILURE, so force_bolt_reset() runs an invalid query
to get a deterministic RESET onto the wire (see force_bolt_reset).

Known protocol limitation (not a server bug, analogous to pgbouncer SET leaks): on a clean
no-RESET reuse of a database=None session the server sees identical traffic and keeps the sticky
state. The leak-free usage is driver.session(database=...), asserted as the MITIGATION below.
"""

import os
import sys

import interactive_mg_runner
import pytest

# Neo4j driver is used because it reliably triggers a real wire-level Bolt RESET (see the
# module docstring); pymgclient does not expose RESET at all.
neo4j = pytest.importorskip("neo4j", reason="neo4j driver required to trigger a real Bolt RESET")

import mgclient
from common import get_data_path, get_logs_path
from neo4j import GraphDatabase

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

FILE = "reset_session_isolation"
BOLT_PORT = 17691

# Multi-database (CREATE/USE DATABASE) is enterprise-gated, so tests that exercise it can only run
# with a license present. Isolation-only tests below stay runnable on community builds.
requires_enterprise = pytest.mark.skipif(
    not (os.environ.get("MEMGRAPH_ENTERPRISE_LICENSE") and os.environ.get("MEMGRAPH_ORGANIZATION_NAME")),
    reason="multi-database RESET tests need an enterprise license (MEMGRAPH_ENTERPRISE_LICENSE + MEMGRAPH_ORGANIZATION_NAME)",
)


def get_instances_description(test_name: str):
    """Single instance is enough: the leak is entirely connection/session-local."""
    return {
        "main": {
            "args": [
                "--bolt-port",
                str(BOLT_PORT),
                "--log-level=TRACE",
                # Pin the default so the leak probes are deterministic across builds.
                "--isolation-level=SNAPSHOT_ISOLATION",
            ],
            "log_file": f"{get_logs_path(FILE, test_name)}/main.log",
            "data_directory": f"{get_data_path(FILE, test_name)}/main",
            "setup_queries": [],
        },
    }


@pytest.fixture(autouse=True)
def cleanup_after_test():
    interactive_mg_runner.kill_all(keep_directories=False)
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


def _bolt_uri() -> str:
    return f"bolt://127.0.0.1:{BOLT_PORT}"


def force_bolt_reset(session) -> None:
    """Force a real wire-level Bolt RESET via an invalid query (see module docstring)."""
    with pytest.raises(Exception):
        session.run("THIS IS NOT VALID CYPHER SYNTAX !!!").consume()


def _mgclient_connect() -> mgclient.Connection:
    connection = mgclient.connect(host="127.0.0.1", port=BOLT_PORT)
    connection.autocommit = True
    return connection


def _create_clean_database(db_name: str) -> None:
    """Setup helper: (re)create `db_name` from scratch via a throwaway mgclient connection."""
    connection = _mgclient_connect()
    cursor = connection.cursor()
    cursor.execute("USE DATABASE memgraph")
    try:
        cursor.execute(f"DROP DATABASE {db_name}")
    except mgclient.DatabaseError:
        pass  # db_name did not exist yet
    cursor.execute(f"CREATE DATABASE {db_name}")
    connection.close()


def _sees_uncommitted_write(reader_session, writer_session, label: str) -> bool:
    """Probe reader_session's effective isolation via dirty-read visibility: returns True iff it
    sees writer_session's uncommitted write (READ UNCOMMITTED) vs. not (SNAPSHOT/READ COMMITTED).
    """
    tx_r = reader_session.begin_transaction()
    tx_r.run(f"MATCH (n:{label}) RETURN count(n) AS c").consume()  # starts the tx / takes its snapshot
    tx_w = writer_session.begin_transaction()
    tx_w.run(f"CREATE (:{label})").consume()  # left uncommitted on purpose
    count = tx_r.run(f"MATCH (n:{label}) RETURN count(n) AS c").single()["c"]
    tx_w.rollback()
    tx_r.commit()
    return count == 1


# ---------------------------------------------------------------------------
# Test A: isolation-level leak across RESET
# ---------------------------------------------------------------------------


def test_reset_clears_next_transaction_isolation_override(test_name):
    """SET NEXT TRANSACTION ISOLATION LEVEL must not leak across a Bolt RESET."""
    instances = get_instances_description(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)

    driver = GraphDatabase.driver(_bolt_uri(), auth=None, encrypted=False, max_connection_pool_size=1)
    writer_driver = GraphDatabase.driver(_bolt_uri(), auth=None, encrypted=False, max_connection_pool_size=1)
    try:
        pooled = driver.session()
        pooled.run("SET NEXT TRANSACTION ISOLATION LEVEL READ UNCOMMITTED").consume()

        # The overridden transaction never starts: RESET happens first.
        force_bolt_reset(pooled)

        writer = writer_driver.session()
        leaked = _sees_uncommitted_write(pooled, writer, "NextIsoLeakProbe")
        writer.close()
        pooled.close()

        assert not leaked, (
            "SET NEXT TRANSACTION ISOLATION LEVEL leaked across a Bolt RESET: the first "
            "transaction after RESET ran under READ UNCOMMITTED instead of the connection's "
            "default (SNAPSHOT ISOLATION) isolation level."
        )
    finally:
        driver.close()
        writer_driver.close()


def test_reset_clears_session_isolation_override(test_name):
    """SET SESSION TRANSACTION ISOLATION LEVEL must not leak across a Bolt RESET."""
    instances = get_instances_description(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)

    driver = GraphDatabase.driver(_bolt_uri(), auth=None, encrypted=False, max_connection_pool_size=1)
    writer_driver = GraphDatabase.driver(_bolt_uri(), auth=None, encrypted=False, max_connection_pool_size=1)
    try:
        pooled = driver.session()
        pooled.run("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED").consume()

        force_bolt_reset(pooled)

        writer = writer_driver.session()
        leaked = _sees_uncommitted_write(pooled, writer, "SessionIsoLeakProbe")
        writer.close()
        pooled.close()

        assert not leaked, (
            "SET SESSION TRANSACTION ISOLATION LEVEL leaked across a Bolt RESET: the next "
            "pooled session ran under READ UNCOMMITTED instead of the connection's default "
            "(SNAPSHOT ISOLATION) isolation level."
        )
    finally:
        driver.close()
        writer_driver.close()


# ---------------------------------------------------------------------------
# Test B: USE-db leak across RESET (enterprise / multi-database)
# ---------------------------------------------------------------------------


@requires_enterprise
def test_reset_clears_use_database_and_allows_subsequent_use(test_name):
    """USE DATABASE must not leak across a Bolt RESET, and a later USE must not be rejected
    (covers the in_explicit_db_ stuck-true regression)."""
    instances = get_instances_description(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)
    _create_clean_database("db_b")

    driver = GraphDatabase.driver(_bolt_uri(), auth=None, encrypted=False, max_connection_pool_size=1)
    try:
        pooled = driver.session()
        pooled.run("USE DATABASE db_b").consume()
        current = pooled.run("SHOW DATABASE").single()["Current"]
        assert current == "db_b", f"USE DATABASE db_b did not take effect (got '{current}')"

        force_bolt_reset(pooled)

        # No db routing metadata on this session (driver.session() with no `database=` param):
        # a query here must target the DEFAULT db, not the leaked db_b.
        current_after_reset = pooled.run("SHOW DATABASE").single()["Current"]
        assert current_after_reset == "memgraph", (
            f"USE DATABASE leaked across RESET: a query with no db routing metadata after RESET "
            f"targeted '{current_after_reset}' instead of the default database 'memgraph'."
        )

        # Regression check: a later explicit USE must not be rejected (in_explicit_db_ stuck-true).
        pooled.run("USE DATABASE db_b").consume()
        current_after_reuse = pooled.run("SHOW DATABASE").single()["Current"]
        assert current_after_reuse == "db_b", "A later explicit USE DATABASE was rejected/ignored after RESET"

        pooled.close()  # clean close: driver leaves the connection READY and sends NO wire RESET

        # KNOWN LIMITATION (protocol gap, see module docstring): a no-RESET reuse of a
        # database=None session keeps the sticky db_b, since the server sees identical traffic.
        reopened = driver.session()
        current_reopened = reopened.run("SHOW DATABASE").single()["Current"]
        assert current_reopened == "db_b", (
            "expected connection-sticky 'db_b' on clean no-RESET pooled reuse (documented protocol "
            f"limitation -- see module docstring); got '{current_reopened}'"
        )
        reopened.close()

        # MITIGATION: naming the database explicitly sends the `db` field on every RUN and routes
        # correctly regardless of stickiness -- the leak-free way to use pooled connections.
        explicit_default = driver.session(database="memgraph")
        assert explicit_default.run("SHOW DATABASE").single()["Current"] == "memgraph"
        explicit_default.close()
        explicit_b = driver.session(database="db_b")
        assert explicit_b.run("SHOW DATABASE").single()["Current"] == "db_b"
        explicit_b.close()
    finally:
        driver.close()


# ---------------------------------------------------------------------------
# Non-regression: mid-session ROLLBACK WITHOUT RESET must NOT clear sticky state
# ---------------------------------------------------------------------------


@requires_enterprise
def test_rollback_without_reset_preserves_use_database_and_session_isolation(test_name):
    """A mid-session BEGIN...ROLLBACK (no RESET) must keep USE'd db + SET SESSION isolation.

    Guard rail proving the fix is RESET-scoped, not applied to every transaction abort.
    """
    instances = get_instances_description(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)
    _create_clean_database("db_b")

    driver = GraphDatabase.driver(_bolt_uri(), auth=None, encrypted=False, max_connection_pool_size=1)
    writer_driver = GraphDatabase.driver(_bolt_uri(), auth=None, encrypted=False, max_connection_pool_size=1)
    try:
        pooled = driver.session()
        pooled.run("USE DATABASE db_b").consume()
        pooled.run("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED").consume()

        # Mid-session abort WITHOUT a Bolt RESET: an explicit BEGIN ... ROLLBACK.
        tx = pooled.begin_transaction()
        tx.run("RETURN 1").consume()
        tx.rollback()

        # Same session, same connection, no RESET in between: USE'd db must still be in effect.
        current = pooled.run("SHOW DATABASE").single()["Current"]
        assert current == "db_b", (
            "A mid-session ROLLBACK (without RESET) incorrectly cleared the USE'd database; the "
            "fix must be scoped to RESET/LogOff, not every transaction abort."
        )

        # ... and SET SESSION TRANSACTION ISOLATION LEVEL must still be in effect too.
        writer = writer_driver.session(database="db_b")
        still_read_uncommitted = _sees_uncommitted_write(pooled, writer, "RollbackNoResetProbe")
        writer.close()
        assert still_read_uncommitted, (
            "A mid-session ROLLBACK (without RESET) incorrectly cleared SET SESSION TRANSACTION "
            "ISOLATION LEVEL; the fix must be scoped to RESET/LogOff, not every transaction abort."
        )

        pooled.close()
    finally:
        driver.close()
        writer_driver.close()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
