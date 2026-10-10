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
LOGOFF (identity change on a pooled connection) clears per-connection sticky state: isolation-level
overrides (SET NEXT/SESSION TRANSACTION ISOLATION LEVEL) and the current database (USE DATABASE).
The RuntimeConfig run-metadata cache is also dropped so routing hints are re-applied fresh after
LOGON. RESET intentionally keeps all of that state — drivers send RESET inside a logical session
(Java/.NET on pool release, Python/JS after any FAILURE) and must not disrupt the session's context.
"""

import os
import sys
import time

import interactive_mg_runner
import pytest

# Neo4j driver is used because it exposes Bolt LOGOFF+LOGON (session re-auth, Bolt >= 5.1) and
# reliably triggers a wire-level RESET after FAILURE; pymgclient exposes neither.
neo4j = pytest.importorskip("neo4j", reason="neo4j driver required to trigger Bolt LOGOFF/RESET")

import mgclient
from common import connect, get_data_path, get_logs_path
from neo4j import GraphDatabase

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

FILE = "reset_session_isolation"
BOLT_PORT = 17691

_ALICE = ("alice", "alice")
_BOB = ("bob", "bob")
_GINA = ("gina", "gina")

# Multi-database (CREATE/USE DATABASE, GRANT DATABASE) is enterprise-gated.
requires_enterprise = pytest.mark.skipif(
    not (os.environ.get("MEMGRAPH_ENTERPRISE_LICENSE") and os.environ.get("MEMGRAPH_ORGANIZATION_NAME")),
    reason="multi-database tests need an enterprise license (MEMGRAPH_ENTERPRISE_LICENSE + MEMGRAPH_ORGANIZATION_NAME)",
)


def get_instances_description(test_name: str):
    """Single instance with two users; LOGOFF+LOGON is driven by switching session auth."""
    return {
        "main": {
            "args": [
                "--bolt-port",
                str(BOLT_PORT),
                "--log-level=TRACE",
                # Pin default isolation so probes are deterministic across builds.
                "--isolation-level=SNAPSHOT_ISOLATION",
            ],
            "log_file": f"{get_logs_path(FILE, test_name)}/main.log",
            "data_directory": f"{get_data_path(FILE, test_name)}/main",
            # GRANT DATABASE is enterprise-only, so it is issued inside @requires_enterprise tests.
            "setup_queries": [
                "CREATE USER alice IDENTIFIED BY 'alice'",
                "GRANT ALL PRIVILEGES TO alice",
                "CREATE USER bob IDENTIFIED BY 'bob'",
                "GRANT ALL PRIVILEGES TO bob",
            ],
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
    """Force a real wire-level Bolt RESET via an invalid query (driver auto-RESETs after FAILURE)."""
    with pytest.raises(Exception):
        session.run("THIS IS NOT VALID CYPHER SYNTAX !!!").consume()


def _create_clean_database(db_name: str) -> None:
    """(Re)create `db_name` from scratch via a throwaway mgclient connection as alice (superadmin)."""
    conn = connect(host="127.0.0.1", port=BOLT_PORT, username="alice", password="alice")
    cursor = conn.cursor()
    cursor.execute("USE DATABASE memgraph")
    try:
        cursor.execute(f"DROP DATABASE {db_name}")
    except mgclient.DatabaseError:
        pass  # db_name did not exist yet
    cursor.execute(f"CREATE DATABASE {db_name}")
    conn.close()


def _grant_database(db_name: str, *usernames: str) -> None:
    """Grant database access to users via alice's superadmin connection (enterprise-only, call inside
    @requires_enterprise tests only)."""
    conn = connect(host="127.0.0.1", port=BOLT_PORT, username="alice", password="alice")
    cursor = conn.cursor()
    for username in usernames:
        cursor.execute(f"GRANT DATABASE {db_name} TO {username}")
    conn.close()


def _isolation_overrides(session) -> tuple:
    """(session, next-transaction) isolation overrides of the connection's interpreter; "" when unset."""
    rows = {r["storage info"]: r["value"] for r in session.run("SHOW STORAGE INFO")}
    return rows["session_isolation_level"], rows["next_session_isolation_level"]


# ---------------------------------------------------------------------------
# Test 1: isolation-level cleared on LOGOFF (identity change = LOGOFF + LOGON)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "set_statement",
    [
        "SET NEXT TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
        "SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
    ],
    ids=["next_tx", "session"],
)
def test_logoff_clears_isolation_override(test_name, set_statement: str):
    """Isolation overrides set by alice must not survive LOGOFF+LOGON to bob on the same connection.

    max_connection_pool_size=1 makes driver.session(auth=bob) reuse alice's connection, so the
    driver re-authenticates it with LOGOFF+LOGON (Bolt 5.1+).
    """
    interactive_mg_runner.start_all(get_instances_description(test_name), keep_directories=False)

    driver = GraphDatabase.driver(_bolt_uri(), auth=_ALICE, max_connection_pool_size=1, encrypted=False)
    try:
        with driver.session() as alice:
            alice.run(set_statement).consume()
            assert "READ_UNCOMMITTED" in _isolation_overrides(alice), "SET did not take effect for alice"

        with driver.session(auth=_BOB) as bob:
            overrides = _isolation_overrides(bob)
        assert overrides == ("", ""), f"'{set_statement}' survived LOGOFF+LOGON into bob's session: {overrides}"
    finally:
        driver.close()


# ---------------------------------------------------------------------------
# Test 2: LOGOFF drops RuntimeConfig cache; routing metadata re-applied fresh after LOGON
# ---------------------------------------------------------------------------


@requires_enterprise
def test_logoff_reapplies_session_database(test_name):
    """LOGOFF must both clear USE DATABASE sticky state and invalidate the RuntimeConfig run-metadata
    cache so that routing hints (database=...) are applied fresh after LOGON, even if unchanged.
    """
    instances = get_instances_description(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)
    _create_clean_database("db_b")
    _grant_database("db_b", "alice", "bob")

    driver = GraphDatabase.driver(_bolt_uri(), auth=_ALICE, max_connection_pool_size=1, encrypted=False)
    try:
        # Sub-case 1: routing metadata db_b must be re-applied after LOGOFF+LOGON even though the
        # metadata value is identical to alice's last session (RuntimeConfig cache must be reset).
        with driver.session(database="db_b") as s:
            current = s.run("SHOW DATABASE").single()["Current"]
            assert current == "db_b", f"alice: expected db_b, got '{current}'"

        # auth differs → driver sends LOGOFF+LOGON on alice's pooled connection.
        with driver.session(database="db_b", auth=_BOB) as s:
            current = s.run("SHOW DATABASE").single()["Current"]
            assert current == "db_b", (
                f"bob: RuntimeConfig db_b routing not applied after LOGOFF+LOGON (got '{current}'). "
                "The server must not skip metadata application due to a stale RuntimeConfig cache."
            )

        # Sub-case 2: alice's USE DATABASE db_b (sticky, no routing hint) is cleared on LOGOFF.
        # After sub-case 1 the pooled connection auth=bob; driver.session() (driver auth=alice)
        # sends LOGOFF+LOGON back to alice first, then alice sets USE DATABASE db_b.
        with driver.session() as s:
            s.run("USE DATABASE db_b").consume()

        # bob's no-routing session must land on his default (memgraph), not alice's USE'd db_b.
        with driver.session(auth=_BOB) as s:
            current = s.run("SHOW DATABASE").single()["Current"]
            assert current == "memgraph", f"bob: USE DATABASE db_b was not cleared by LOGOFF (got '{current}')."
    finally:
        driver.close()


# ---------------------------------------------------------------------------
# Test 3: RESET preserves sticky state within the same logical session
# ---------------------------------------------------------------------------


@requires_enterprise
def test_reset_preserves_session_state(test_name):
    """A Bolt RESET inside alice's session must NOT clear USE DATABASE or SET SESSION isolation.

    Drivers send RESET inside a logical session (e.g. after any FAILURE); only LOGOFF marks the
    identity boundary where per-connection sticky state is cleared.
    """
    interactive_mg_runner.start_all(get_instances_description(test_name), keep_directories=False)
    _create_clean_database("db_b")
    _grant_database("db_b", "alice")

    driver = GraphDatabase.driver(_bolt_uri(), auth=_ALICE, max_connection_pool_size=1, encrypted=False)
    try:
        with driver.session() as alice:
            alice.run("USE DATABASE db_b").consume()
            alice.run("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED").consume()

            force_bolt_reset(alice)

            current = alice.run("SHOW DATABASE").single()["Current"]
            assert current == "db_b", f"USE DATABASE db_b lost after RESET (got '{current}')"
            overrides = _isolation_overrides(alice)
            assert overrides[0] == "READ_UNCOMMITTED", f"SET SESSION isolation lost after RESET: {overrides}"
    finally:
        driver.close()


# ---------------------------------------------------------------------------
# Test 4: LOGOFF drops RuntimeConfig cache; ImpersonateUserAuth re-checked after LOGON
# ---------------------------------------------------------------------------


@requires_enterprise
def test_logoff_reapplies_impersonated_user(test_name):
    """LOGOFF must clear the RuntimeConfig run-metadata cache so that ImpersonateUserAuth
    is re-checked for the new session user even when imp_user metadata is identical."""
    interactive_mg_runner.start_all(get_instances_description(test_name), keep_directories=False)

    # carol = impersonation target; dave = user with no right to impersonate carol.
    conn = connect(host="127.0.0.1", port=BOLT_PORT, username="alice", password="alice")
    cursor = conn.cursor()
    for stmt in [
        "CREATE USER carol IDENTIFIED BY 'carol'",
        "GRANT ALL PRIVILEGES TO carol",
        "CREATE USER dave IDENTIFIED BY 'dave'",
        "GRANT ALL PRIVILEGES TO dave",
        "GRANT IMPERSONATE_USER carol TO alice",
        "GRANT IMPERSONATE_USER carol TO bob",
    ]:
        cursor.execute(stmt)
    conn.close()

    driver = GraphDatabase.driver(_bolt_uri(), auth=_ALICE, max_connection_pool_size=1, encrypted=False)
    try:
        # Sub-case 1: alice impersonates carol; SHOW CURRENT USER must report carol.
        with driver.session(impersonated_user="carol") as s:
            user = s.run("SHOW CURRENT USER").single()["user"]
            assert user == "carol", f"alice: expected effective user 'carol', got '{user}'"

        # Sub-case 2: bob on the same pooled connection (LOGOFF+LOGON) with identical imp_user=carol.
        # RuntimeConfig cache must be cleared so ImpersonateUserAuth runs for bob and carol is applied.
        with driver.session(auth=_BOB, impersonated_user="carol") as s:
            user = s.run("SHOW CURRENT USER").single()["user"]
            assert user == "carol", (
                f"bob: imp_user=carol not applied after LOGOFF+LOGON (got '{user}'). "
                "RuntimeConfig cache was not cleared on LOGOFF — Configure() was skipped."
            )

        # Sub-case 3: dave has no IMPERSONATE_USER carol right; server must reject the request,
        # proving ImpersonateUserAuth is called for the new session user after each LOGOFF+LOGON.
        with pytest.raises(Exception, match="Failed to impersonate user"):
            with driver.session(auth=("dave", "dave"), impersonated_user="carol") as s:
                s.run("SHOW CURRENT USER").consume()
    finally:
        driver.close()


# ---------------------------------------------------------------------------
# Test 5: LOGOFF refreshes session login timestamp
# ---------------------------------------------------------------------------


def _own_session(session, username: str):
    rows = [r for r in session.run("SHOW SESSIONS") if r["username"] == username]
    assert len(rows) == 1, f"expected exactly one session for {username}, got {rows}"
    return rows[0]


def test_logoff_refreshes_session_login_timestamp(test_name):
    """After LOGOFF+LOGON on the same connection, SHOW SESSIONS must report the new user's
    login time, not the time the TCP connection was opened."""
    interactive_mg_runner.start_all(get_instances_description(test_name), keep_directories=False)

    driver = GraphDatabase.driver(_bolt_uri(), auth=_ALICE, max_connection_pool_size=1, encrypted=False)
    try:
        with driver.session() as alice:
            alice_row = _own_session(alice, "alice")
            alice_session_id = alice_row["session_id"]
            alice_login_ts = alice_row["login_timestamp"]

        time.sleep(0.2)

        # max_connection_pool_size=1: driver reuses alice's connection and sends LOGOFF+LOGON.
        with driver.session(auth=_BOB) as bob:
            bob_row = _own_session(bob, "bob")
            bob_session_id = bob_row["session_id"]
            bob_login_ts = bob_row["login_timestamp"]

        assert bob_session_id == alice_session_id, (
            f"Expected the same connection (session_id) after LOGOFF+LOGON; "
            f"alice={alice_session_id!r} bob={bob_session_id!r}."
        )
        assert bob_login_ts > alice_login_ts, (
            f"bob's login_timestamp ({bob_login_ts!r}) must be strictly later than "
            f"alice's ({alice_login_ts!r}); LOGOFF+LOGON must refresh the timestamp."
        )
    finally:
        driver.close()


# ---------------------------------------------------------------------------
# Test 6-9: impersonation Configure — failure / revoke / sticky USE DATABASE on one live connection
# ---------------------------------------------------------------------------


def _admin(*statements: str) -> None:
    conn = connect(host="127.0.0.1", port=BOLT_PORT, username="alice", password="alice")
    cursor = conn.cursor()
    for stmt in statements:
        cursor.execute(stmt)
    conn.close()


_CREATE_GINA = "CREATE USER gina IDENTIFIED BY 'gina'"
_RECONNECTED = "driver reconnected; test would not prove connection-state handling"


def _driver_session_id(driver, timeout_s: float = 10.0) -> str:
    """session_id of the pool-size-1 driver's only connection; polls because _admin() connections close
    asynchronously."""
    deadline = time.monotonic() + timeout_s
    while True:
        with driver.session() as s:
            rows = list(s.run("SHOW SESSIONS"))
        if len(rows) == 1:
            return rows[0]["session_id"]
        assert time.monotonic() < deadline, f"expected exactly the driver's session, got {rows}"
        time.sleep(0.1)


def _current_user(driver, **session_kwargs) -> str:
    with driver.session(**session_kwargs) as s:
        return s.run("SHOW CURRENT USER").single()["user"]


@requires_enterprise
def test_failed_db_access_does_not_leak_impersonated_user(test_name):
    interactive_mg_runner.start_all(get_instances_description(test_name), keep_directories=False)
    _admin(
        "CREATE DATABASE db2",
        "GRANT DATABASE * TO alice",
        "GRANT IMPERSONATE_USER bob TO alice",
    )

    # bob has no access to db2.
    driver = GraphDatabase.driver(_bolt_uri(), auth=_ALICE, max_connection_pool_size=1, encrypted=False)
    try:
        assert _current_user(driver) == "alice"
        session_id = _driver_session_id(driver)
        with pytest.raises(Exception, match="not authorized on the database"):
            _current_user(driver, impersonated_user="bob", database="db2")
        assert _current_user(driver) == "alice", "failed {imp_user: bob, db: db2} leaked bob into the next message"
        assert _driver_session_id(driver) == session_id, _RECONNECTED
    finally:
        driver.close()


@requires_enterprise
def test_db_scoped_impersonation_grant_does_not_leak_user(test_name):
    """IMPERSONATE_USER granted only via a db2-scoped role must not impersonate on the default database."""
    interactive_mg_runner.start_all(get_instances_description(test_name), keep_directories=False)
    _admin(
        "CREATE DATABASE db2",
        "CREATE USER eve IDENTIFIED BY 'eve'",
        "CREATE ROLE impdb2",
        "GRANT IMPERSONATE_USER bob TO ROLE impdb2",
        "GRANT DATABASE db2 TO ROLE impdb2",
        "SET ROLE FOR eve TO impdb2 ON db2",
    )
    driver = GraphDatabase.driver(_bolt_uri(), auth=("eve", "eve"), max_connection_pool_size=1, encrypted=False)
    try:
        assert _current_user(driver) == "eve"
        session_id = _driver_session_id(driver)
        with pytest.raises(Exception, match="Failed to impersonate user"):
            _current_user(driver, impersonated_user="bob")
        with pytest.raises(Exception, match="not authorized on the database"):
            _current_user(driver, impersonated_user="bob", database="db2")
        with driver.session() as s:
            assert s.run("SHOW CURRENT USER").single()["user"] == "eve", "db-scoped grant leaked bob onto memgraph"
            assert s.run("SHOW DATABASE").single()["Current"] == "memgraph"
        assert _driver_session_id(driver) == session_id, _RECONNECTED
    finally:
        driver.close()


@requires_enterprise
@pytest.mark.parametrize("database", [None, "memgraph"], ids=["same_extra", "changed_extra"])
def test_revoked_impersonation_rejected_on_live_connection(test_name, database):
    """REVOKE IMPERSONATE_USER from another connection takes effect at the next imp_user message of an
    already-impersonating connection, whether or not the message metadata changed.
    The login (gina) is not a superadmin, so the revoke is not masked by implicit privileges."""
    interactive_mg_runner.start_all(get_instances_description(test_name), keep_directories=False)
    _admin(_CREATE_GINA, "GRANT IMPERSONATE_USER bob TO gina")

    driver = GraphDatabase.driver(_bolt_uri(), auth=_GINA, max_connection_pool_size=1, encrypted=False)
    try:
        session_id = _driver_session_id(driver)
        assert _current_user(driver, impersonated_user="bob") == "bob"
        _admin("REVOKE IMPERSONATE_USER FROM gina")

        with pytest.raises(Exception, match="Failed to impersonate user"):
            _current_user(driver, impersonated_user="bob", database=database)
        assert _current_user(driver) == "gina"
        assert _driver_session_id(driver) == session_id, _RECONNECTED
    finally:
        driver.close()


@requires_enterprise
def test_use_database_survives_auth_write_while_impersonating(test_name):
    """An unrelated auth write re-validates the impersonation but must not reset a sticky USE DATABASE.

    Only bob (not the login gina) can access db2, so the re-check must use the originally validated database."""
    interactive_mg_runner.start_all(get_instances_description(test_name), keep_directories=False)
    _admin(
        "CREATE DATABASE db2",
        "GRANT DATABASE db2 TO bob",
        _CREATE_GINA,
        "GRANT IMPERSONATE_USER bob TO gina",
    )

    driver = GraphDatabase.driver(_bolt_uri(), auth=_GINA, max_connection_pool_size=1, encrypted=False)
    try:
        session_id = _driver_session_id(driver)
        with driver.session(impersonated_user="bob") as s:
            s.run("USE DATABASE db2").consume()
            assert s.run("SHOW DATABASE").single()["Current"] == "db2"

            _admin("CREATE USER unrelated IDENTIFIED BY 'unrelated'")

            assert s.run("SHOW DATABASE").single()["Current"] == "db2", "USE DATABASE lost after an auth write"
            assert s.run("SHOW CURRENT USER").single()["user"] == "bob"
        assert _driver_session_id(driver) == session_id, _RECONNECTED
    finally:
        driver.close()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
