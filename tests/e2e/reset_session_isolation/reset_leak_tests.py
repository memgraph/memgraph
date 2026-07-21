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

Background
----------
A pooled Bolt connection can be handed to a logically unrelated session after a
RESET (Bolt HandleReset -> session.Abort(), see
src/communication/bolt/v1/states/handlers.hpp and src/glue/SessionHL.cpp). Two
kinds of per-connection state used to leak across that boundary:

  1. Isolation level overrides: SET NEXT TRANSACTION ISOLATION LEVEL (one-shot,
     meant for the very next transaction on this connection) and SET SESSION
     TRANSACTION ISOLATION LEVEL (meant for the rest of this Bolt session) were
     never cleared on RESET, so the next, unrelated, pooled session silently
     inherited them.

  2. USE DATABASE (Bolt-level db routing / explicit `USE <db>`, enterprise
     multi-tenancy) similarly stuck around after RESET; a stale
     `in_explicit_db_ == true` flag additionally blocked a *subsequent*
     explicit `USE <db>` from taking effect.

The fix (see src/query/interpreter.{hpp,cpp}, src/glue/SessionHL.{hpp,cpp}):
  * Interpreter::ResetForConnectionReuse() clears next_transaction_isolation_level,
    interpreter_isolation_level, and (enterprise only) calls ResetDB() -- which now
    also resets in_explicit_db_ (src/query/interpreter.hpp CurrentDB::ResetDB()).
  * RuntimeConfig::ResetForConnectionReuse() (src/glue/SessionHL.hpp) invalidates the
    "run_time_info unchanged since last call => skip re-derivation" cache in
    RuntimeConfig::Configure(), so the very next RUN after RESET is forced through the
    full db/user re-derivation path instead of silently keeping the previous session's
    resolved db.
  * Both are called ONLY from SessionHL::Abort() (RESET and LogOff), never from the
    ordinary mid-session ROLLBACK / CheckAuthorized-failure / autocommit-abort paths
    (those call Interpreter::Abort()/RollbackTransaction() directly without touching
    the new reset hooks) -- so a mid-session abort must NOT clear this state.

Why a real Bolt RESET, not driver.session().close()
----------------------------------------------------
The high-level neo4j driver session API is designed to *avoid* ever sending a
wire-level RESET on a clean connection: Session.close() drains/rolls back
anything outstanding itself and only asks the connection pool to send RESET if
the connection isn't already back in the READY state (see the vendored
`neo4j` package's `_sync/io/_pool.py::release()` and `_bolt5.py::is_reset`). A
plain `SET ... ISOLATION LEVEL ...` or `USE DATABASE ...` followed by
`session.close()` never dirties the connection, so no RESET is ever put on the
wire that way.

The Bolt protocol *does* guarantee a real RESET is sent in one standard,
public-API situation: after a FAILURE response, the client must RESET before
reusing the connection (see HandleReset's signature check, Marker::TinyStruct,
0x0F, in src/communication/bolt/v1/states/handlers.hpp), and the neo4j driver
implements this automatically. `force_bolt_reset()` below uses exactly that --
a deliberately invalid query -- to get a deterministic, protocol-correct RESET
onto the wire without reaching into driver-private internals or hand-rolling
PackStream framing.

Known limitation: clean pooled reuse of a no-`database=` session
----------------------------------------------------------------
Because a clean session.close() sends no RESET (above) and a `database=None` session sends no
`db` field on its RUN, the server sees byte-identical traffic whether the same logical session
is continuing or a new logical session is reusing the pooled connection. It therefore cannot
know a boundary occurred, and (correctly) keeps the connection-sticky state -- so a `USE
DATABASE` (and equally SET SESSION isolation/trace/setting) from one no-`database=` session
persists into the next no-`database=` session on the same pooled connection. This is a protocol
limitation, analogous to Postgres `SET` leaking across pgbouncer transaction-pooled clients, not
a server bug: the RESET-scoped reset is complete for every case the driver *does* signal (a real
RESET, or an explicit `db` field on the RUN). The leak-free, recommended usage is
`driver.session(database=...)`, which sends the `db` field on every RUN and routes correctly
regardless of connection stickiness (asserted as the MITIGATION in the USE-DATABASE test below).

Coverage note: the same Interpreter::ResetForConnectionReuse() also clears the per-connection
SessionLogContext overlay -- SET SESSION TRACE and SET SESSION SETTING -- on RESET/LogOff (unit-
tested in tests/unit/logging.cpp), closing the same leak class for those in-band session settings.
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


def get_instances_description(test_name: str):
    """Single instance is enough: the leak is entirely connection/session-local."""
    return {
        "main": {
            "args": [
                "--bolt-port",
                str(BOLT_PORT),
                "--log-level=TRACE",
                # Pin the default so the leak probes (which compare against "the default
                # isolation level") are deterministic regardless of the build's compiled-in default.
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
    """Force a real wire-level Bolt RESET onto `session`'s underlying connection.

    See the module docstring for why this (an intentionally invalid query, which the
    driver reacts to by auto-sending RESET) is the deterministic, public-API way to do
    this, and why session.close() alone does not reach the same server-side code path.
    """
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
    """Behavioral probe for the isolation level actually in effect on `reader_session`.

    Starts a transaction on reader_session, has writer_session CREATE a node of `label`
    and leave it uncommitted, then checks whether reader_session's transaction can see
    it:
      * SNAPSHOT ISOLATION / READ COMMITTED -> does not see it (returns False)
      * READ UNCOMMITTED                    -> sees it (returns True)

    This mirrors tests/e2e/isolation_levels/isolation_levels.cpp's
    TestSnapshotIsolation/TestReadCommitted/TestReadUncommitted: there is no SHOW
    TRANSACTIONS column exposing "isolation level of this transaction", so the
    established way to verify it e2e is this dirty-read-visibility technique.
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
    """SET NEXT TRANSACTION ISOLATION LEVEL must not leak across a Bolt RESET.

    Needs the post-rebuild binary: on the pre-fix binary, `next_transaction_isolation_level`
    is never cleared by SessionHL::Abort(), so the first transaction after RESET can run
    under the leaked READ UNCOMMITTED override instead of the connection's default.
    """
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
    """SET SESSION TRANSACTION ISOLATION LEVEL must not leak across a Bolt RESET.

    Needs the post-rebuild binary: on the pre-fix binary, `interpreter_isolation_level`
    is never cleared by SessionHL::Abort(), so a session-scoped override set before RESET
    can still be in effect for the next, logically unrelated, pooled session.
    """
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


def test_reset_clears_use_database_and_allows_subsequent_use(test_name):
    """USE DATABASE must not leak across a Bolt RESET, and a later USE must not be rejected.

    Needs the post-rebuild binary: on the pre-fix binary, RuntimeConfig::Configure()'s
    "run_time_info unchanged since last call => skip" cache is never invalidated across a
    RESET, so a query with no db-routing metadata after RESET can still silently target the
    previously-USE'd database. Also covers the in_explicit_db_ stuck-true regression: before
    the CurrentDB::ResetDB() fix, a stale in_explicit_db_ == true could block a subsequent
    explicit USE from being accepted.
    """
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

        # KNOWN LIMITATION (documented protocol gap, NOT a fix failure): a pooled connection reused
        # WITHOUT a wire-level RESET keeps its connection-sticky USE'd db. On a clean close the neo4j
        # driver sends no RESET (it only RESETs a connection left mid-stream/FAILED) and nothing on
        # checkout; a session opened with no `database=` then sends no `db` field on its RUN. So the
        # server sees byte-identical traffic to "the same session continuing" and has no signal that
        # a new logical session began -- it therefore (correctly) keeps the sticky db. Analogous to
        # Postgres `SET` leaking across pgbouncer transaction-pooled clients. Assert the actual,
        # documented behaviour (sticky db_b), not an outcome the server has no signal to produce:
        reopened = driver.session()
        current_reopened = reopened.run("SHOW DATABASE").single()["Current"]
        assert current_reopened == "db_b", (
            "expected connection-sticky 'db_b' on clean no-RESET pooled reuse (documented protocol "
            f"limitation -- see module docstring); got '{current_reopened}'"
        )
        reopened.close()

        # MITIGATION (recommended usage): a session that names its database explicitly always routes
        # correctly regardless of connection stickiness -- the `db` field is sent on every RUN and
        # bypasses the run_time_info short-circuit. This is the leak-free way to use pooled connections.
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


def test_rollback_without_reset_preserves_use_database_and_session_isolation(test_name):
    """A mid-session BEGIN...ROLLBACK (no RESET) must keep USE'd db + SET SESSION isolation.

    This is the guard rail proving the fix is RESET-scoped: Interpreter::ResetForConnectionReuse()
    is called ONLY from SessionHL::Abort() (Bolt RESET / LogOff), never from the ordinary
    ROLLBACK / autocommit-abort path (Interpreter::RollbackTransaction() / Interpreter::Abort()).
    This test should already pass on the pre-fix binary (it never touches the new code path);
    it exists to catch a regression where the fix is applied too broadly (e.g. folded into
    Interpreter::Abort() directly) rather than scoped to RESET/LogOff as intended.
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
