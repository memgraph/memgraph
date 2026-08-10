# Copyright 2022 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import multiprocessing
import sys
import time
import uuid
from contextlib import suppress
from queue import Empty

import mgclient
import pytest
from common import (
    assert_connection_alive,
    connect,
    execute_and_fetch_all,
    get_own_session_uuid,
    get_session_uuid_by_username,
    wait_until,
    wait_until_terminated,
)

# Tests
# -------------------------
#
# No module-level "suppress the first user's auto-privileges" fixture: every test below drops all
# the users it creates in its own finalizer, so between tests the instance genuinely has zero
# users, and AuthQueryHandler::CreateUser (src/glue/auth_handler.cpp) auto-promotes whichever user
# happens to be created while HasUsers() == false to a full superuser -- there is no way to durably
# suppress that without keeping a permanent user around, and a permanent user is not an option here:
# once ANY user exists, mgclient's anonymous connect() (used by every superadmin_cursor bootstrap
# connection below) is refused outright with "Authentication failure" -- confirmed empirically
# against this instance; that is a Bolt-handshake-level rule, not the later per-query
# CheckAuthorized bypass. Nor does `CREATE ROLE` first help (transaction_queue's identically-named
# fixture does this, and it looked like it worked here by coincidence): Auth::CreateBuiltinRoles
# skips creating the "admin"/"readonly"/"readwrite" roles once ANY role already exists, so a
# pre-existing dummy role only changes *how* the first user gets promoted (direct permission grants
# vs. builtin-role membership) -- not *whether* it does. The one test that needs a genuinely
# unprivileged user (test_unprivileged_user_refused) instead strips both forms explicitly, right
# after creating that user, regardless of which one it received.


def test_idle_session_terminated(request):
    """An idle session (no query in flight) is still discoverable and killable by an admin.

    The admin cursor must itself be an AUTHENTICATED connection: TerminateSessions' per-target
    check is `same_user(target, caller) || privilege_checker(caller, ...)`, and an anonymous
    caller (user_or_role_ == nullptr) fails both halves once a real user (idle_bob) exists --
    see InterpreterContext::TerminateSessions in src/query/interpreter_context.cpp. The bootstrap
    connection (superadmin_cursor) stays anonymous on purpose: CheckAuthorized exempts anonymous
    callers from the general privilege gate, which is what lets it CREATE USER/GRANT at all.
    """
    superadmin_cursor = connect().cursor()
    execute_and_fetch_all(superadmin_cursor, "CREATE USER idle_admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT TRANSACTION_MANAGEMENT TO idle_admin")
    execute_and_fetch_all(superadmin_cursor, "CREATE USER idle_bob")

    def on_exit():
        execute_and_fetch_all(superadmin_cursor, "DROP USER idle_admin")
        execute_and_fetch_all(superadmin_cursor, "DROP USER idle_bob")

    request.addfinalizer(on_exit)

    admin_cursor = connect(username="idle_admin", password="").cursor()

    bob_connection = connect(username="idle_bob", password="")
    bob_cursor = bob_connection.cursor()
    execute_and_fetch_all(bob_cursor, "RETURN 1")  # bob is now idle

    bob_uuid = get_session_uuid_by_username(admin_cursor, "idle_bob")

    results = execute_and_fetch_all(admin_cursor, f"TERMINATE SESSIONS '{bob_uuid}'")
    assert results == [(bob_uuid, True)]

    wait_until_terminated(bob_cursor)

    wait_until(
        lambda: bob_uuid not in [row[1] for row in execute_and_fetch_all(admin_cursor, "SHOW ACTIVE USERS INFO")],
        message="terminated session's uuid is still listed in SHOW ACTIVE USERS INFO",
    )


def _run_long_query(outcome_queue: multiprocessing.Queue) -> None:
    """Runs CALL infinite_query.long_query() to completion or abort, in its OWN OS process.

    Must NOT be a thread: mgclient's Cursor.execute() is a synchronous C extension call that blocks
    on the socket without releasing the GIL for the call's entire duration (confirmed empirically --
    a pure-Python timing loop on another thread does not get scheduled at all while a sibling thread
    blocks inside a long-running execute()). A `threading.Thread` running this call would therefore
    starve every other cursor in the same Python process -- including the admin's own TERMINATE
    SESSIONS call -- for as long as the query runs, which is exactly what hung the whole module.
    tests/e2e/transaction_queue/test_transaction_queue.py hits the same constraint and works around
    it the same way, with multiprocessing.Process instead of threading.Thread.
    """
    connection = connect(username="busy_bob", password="")
    cursor = connection.cursor()
    try:
        rows = execute_and_fetch_all(cursor, "CALL infinite_query.long_query() YIELD my_id RETURN my_id")
        outcome_queue.put(("rows", rows))
    except mgclient.Error as exc:
        outcome_queue.put(("error", str(exc)))


def test_busy_session_terminated(request):
    """A session with a query in flight is cooperatively aborted, and the connection itself closes.

    Same admin-must-be-authenticated requirement as test_idle_session_terminated -- see its
    docstring for why an anonymous caller cannot terminate a real user's session.
    """
    superadmin_cursor = connect().cursor()
    execute_and_fetch_all(superadmin_cursor, "CREATE USER busy_admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT TRANSACTION_MANAGEMENT TO busy_admin")
    execute_and_fetch_all(superadmin_cursor, "CREATE USER busy_bob")

    def on_exit():
        execute_and_fetch_all(superadmin_cursor, "DROP USER busy_admin")
        execute_and_fetch_all(superadmin_cursor, "DROP USER busy_bob")

    request.addfinalizer(on_exit)

    admin_cursor = connect(username="busy_admin", password="").cursor()

    outcome_queue = multiprocessing.Queue()
    # daemon=True: if a bug below leaves this process blocked forever, it must not also block the
    # test process itself from exiting.
    query_process = multiprocessing.Process(target=_run_long_query, args=(outcome_queue,), daemon=True)
    query_process.start()

    def ensure_long_query_stopped():
        """Safety net, always run (LIFO, before on_exit drops the users): an assertion failure
        below must not leave CALL infinite_query.long_query() occupying a server worker forever --
        that starved worker is what turned one failing test into the whole module hanging.
        """
        try:
            bob_uuid = get_session_uuid_by_username(admin_cursor, "busy_bob")
            execute_and_fetch_all(admin_cursor, f"TERMINATE SESSIONS '{bob_uuid}'")
        except (mgclient.Error, AssertionError):
            pass  # already terminated by the test body, or busy_bob's session is already gone
        query_process.join(timeout=10)
        if query_process.is_alive():
            query_process.terminate()
            query_process.join(timeout=5)

    request.addfinalizer(ensure_long_query_stopped)

    wait_until(
        lambda: any(row[0] == "busy_bob" for row in execute_and_fetch_all(admin_cursor, "SHOW ACTIVE USERS INFO")),
        message="busy_bob's session never showed up in SHOW ACTIVE USERS INFO",
    )
    bob_uuid = get_session_uuid_by_username(admin_cursor, "busy_bob")

    results = execute_and_fetch_all(admin_cursor, f"TERMINATE SESSIONS '{bob_uuid}'")
    assert results == [(bob_uuid, True)]

    query_process.join(timeout=10)
    assert not query_process.is_alive(), "busy_bob's long query never returned after TERMINATE SESSIONS"
    try:
        outcome_kind, outcome_detail = outcome_queue.get(timeout=5)
    except Empty:
        pytest.fail("busy_bob's long query process exited without reporting an outcome")
    assert (
        outcome_kind == "error"
    ), f"busy_bob's long query completed successfully instead of being terminated: {outcome_detail}"

    wait_until(
        lambda: not any(row[0] == "busy_bob" for row in execute_and_fetch_all(admin_cursor, "SHOW ACTIVE USERS INFO")),
        message="busy_bob's session is still listed in SHOW ACTIVE USERS INFO after TERMINATE SESSIONS",
    )


def test_cannot_terminate_own_session():
    """Terminating the caller's own session is refused, and the caller keeps working."""
    cursor = connect().cursor()
    own_uuid = get_own_session_uuid(cursor)

    results = execute_and_fetch_all(cursor, f"TERMINATE SESSIONS '{own_uuid}'")
    assert results == [(own_uuid, False)]

    assert_connection_alive(cursor)


def test_empty_session_id_refused():
    """An empty session id is refused and must not touch any other live connection.

    Guards a real footgun: interpreters are registered before authentication completes, so a
    mid-handshake session carries an empty uuid; without the empty-id guard, TERMINATE SESSIONS ''
    would match every such session at once.
    """
    admin_cursor = connect().cursor()
    survivor_cursor = connect().cursor()
    execute_and_fetch_all(survivor_cursor, "RETURN 1")  # goes idle

    results = execute_and_fetch_all(admin_cursor, "TERMINATE SESSIONS ''")
    assert results == [("", False)]

    assert_connection_alive(survivor_cursor)


def test_unknown_session_id():
    """A session id that matches nothing is reported as not-killed, without raising."""
    cursor = connect().cursor()
    unknown_uuid = str(uuid.uuid4())

    results = execute_and_fetch_all(cursor, f"TERMINATE SESSIONS '{unknown_uuid}'")
    assert results == [(unknown_uuid, False)]


def test_unprivileged_user_refused(request):
    """Authorization is per target: same-user always works, cross-user needs TRANSACTION_MANAGEMENT.

    Mirrors TERMINATE TRANSACTIONS's own idiom on purpose -- there is deliberately no statement-level
    privilege for TERMINATE SESSIONS either.
    """
    superadmin_cursor = connect().cursor()
    execute_and_fetch_all(superadmin_cursor, "CREATE USER unpriv_actor")
    execute_and_fetch_all(superadmin_cursor, "CREATE USER unpriv_target")
    # unpriv_actor must start genuinely unprivileged for the "cross-user, unprivileged: refused"
    # assertion below to mean anything. If the instance currently has zero *other* users, CREATE
    # USER auto-promotes the new user to a superuser (see the module-level comment above) -- either
    # as direct permission grants or, once a builtin "admin" role already exists from an earlier
    # such promotion, as membership in that role. Strip both forms unconditionally: which one (if
    # either) actually applied depends on this instance's history, not on this test.
    execute_and_fetch_all(superadmin_cursor, "REVOKE ALL PRIVILEGES FROM unpriv_actor")
    execute_and_fetch_all(superadmin_cursor, "CLEAR ROLE FOR unpriv_actor")

    def on_exit():
        execute_and_fetch_all(superadmin_cursor, "DROP USER unpriv_actor")
        execute_and_fetch_all(superadmin_cursor, "DROP USER unpriv_target")

    request.addfinalizer(on_exit)

    actor_connection_1 = connect(username="unpriv_actor", password="")
    actor_cursor_1 = actor_connection_1.cursor()
    actor_connection_2 = connect(username="unpriv_actor", password="")
    actor_cursor_2 = actor_connection_2.cursor()
    execute_and_fetch_all(actor_cursor_2, "RETURN 1")  # goes idle

    target_connection = connect(username="unpriv_target", password="")
    target_cursor = target_connection.cursor()
    execute_and_fetch_all(target_cursor, "RETURN 1")  # goes idle

    # SHOW ACTIVE USERS INFO requires the STATS privilege (required_privileges.cpp), which
    # unpriv_actor was just stripped of -- so the uuid lookups below go through superadmin_cursor
    # (anonymous callers bypass CheckAuthorized entirely) rather than actor_cursor_1. SET SESSION
    # TRACE OFF, used by get_own_session_uuid, needs no privilege at all and works on either.
    actor_own_uuid = get_own_session_uuid(actor_cursor_1)
    actor_second_uuid = get_session_uuid_by_username(superadmin_cursor, "unpriv_actor", exclude_uuid=actor_own_uuid)
    target_uuid = get_session_uuid_by_username(superadmin_cursor, "unpriv_target")

    # Cross-user, unprivileged: refused, target untouched.
    results = execute_and_fetch_all(actor_cursor_1, f"TERMINATE SESSIONS '{target_uuid}'")
    assert results == [(target_uuid, False)]
    assert_connection_alive(target_cursor)

    # Same-user, unprivileged: allowed -- identical to TERMINATE TRANSACTIONS's own-transaction rule.
    results = execute_and_fetch_all(actor_cursor_1, f"TERMINATE SESSIONS '{actor_second_uuid}'")
    assert results == [(actor_second_uuid, True)]
    wait_until_terminated(actor_cursor_2)

    # Cross-user, now privileged via TRANSACTION_MANAGEMENT: succeeds against the same target.
    execute_and_fetch_all(superadmin_cursor, "GRANT TRANSACTION_MANAGEMENT TO unpriv_actor")
    results = execute_and_fetch_all(actor_cursor_1, f"TERMINATE SESSIONS '{target_uuid}'")
    assert results == [(target_uuid, True)]
    wait_until_terminated(target_cursor)


def _run_drop_fa_db(outcome_queue: multiprocessing.Queue) -> None:
    """Runs DROP DATABASE fa_db FORCE ABORT to completion, in its OWN OS process.

    Must NOT be a thread, for the same reason as _run_long_query above: this call blocks the GIL
    for as long as the drain takes (up to kDrainDeadline), which would starve the test's own
    watch_cursor -- and therefore its TERMINATE SESSIONS call -- if they shared a process. That
    would make the drop always pay the full drain deadline instead of converging early, defeating
    the entire point of this test.
    """
    connection = connect(username="fa_admin", password="")
    cursor = connection.cursor()
    started = time.time()
    try:
        rows = execute_and_fetch_all(cursor, "DROP DATABASE fa_db FORCE ABORT")
        outcome_queue.put(("rows", rows, time.time() - started))
    except mgclient.Error as exc:
        outcome_queue.put(("error", str(exc), time.time() - started))


def test_force_abort_converges_after_terminate(request):
    """Headline case: DROP DATABASE ... FORCE ABORT can only converge once TERMINATE SESSIONS
    releases an idle holder's accessor -- the drop's own cooperative-cancel sweep cannot reach it.

    fa_bob pins fa_db by running USE DATABASE and then going idle: he never starts a transaction, so
    InterpreterContext::TerminateTransactions (the drop's best-effort cooperative-cancel step) never
    matches him -- GetTransactionId() is only set while a query/transaction is actually running. The
    drop's Phase-2 drain wait is bounded at 10 s (dbms_handler.hpp's kDrainDeadline): left alone it
    would still eventually "succeed" (the tenant is dropped and DETACHED either way, per
    NotificationCode::DROP_DATABASE_DETACHED), but only after paying that full deadline. Terminating
    fa_bob's session while the drop is mid-wait must make it converge quickly instead.
    """
    superadmin_cursor = connect().cursor()
    execute_and_fetch_all(superadmin_cursor, "CREATE USER fa_admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT ALL PRIVILEGES TO fa_admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT DATABASE * TO fa_admin")
    execute_and_fetch_all(superadmin_cursor, "CREATE USER fa_bob")

    def on_exit():
        try:
            execute_and_fetch_all(superadmin_cursor, "DROP DATABASE fa_db")
        except mgclient.Error:
            pass  # expected once the test itself already dropped it
        execute_and_fetch_all(superadmin_cursor, "DROP USER fa_admin")
        execute_and_fetch_all(superadmin_cursor, "DROP USER fa_bob")

    request.addfinalizer(on_exit)

    admin_cursor = connect(username="fa_admin", password="").cursor()
    # DROP DATABASE ... FORCE ABORT runs in its own process (see _run_drop_fa_db); watching and
    # terminating needs its own socket regardless, since the drop's own connection is unusable
    # until DROP DATABASE returns.
    watch_cursor = connect(username="fa_admin", password="").cursor()

    execute_and_fetch_all(admin_cursor, "CREATE DATABASE fa_db")
    execute_and_fetch_all(superadmin_cursor, "GRANT DATABASE fa_db TO fa_bob")
    execute_and_fetch_all(superadmin_cursor, "GRANT MULTI_DATABASE_USE TO fa_bob")

    bob_connection = connect(username="fa_bob", password="")
    bob_cursor = bob_connection.cursor()
    execute_and_fetch_all(bob_cursor, "USE DATABASE fa_db")  # bob now idles, pinning fa_db's accessor

    outcome_queue = multiprocessing.Queue()
    # daemon=True: a stuck DROP DATABASE must not also keep the test process alive past this test.
    drop_process = multiprocessing.Process(target=_run_drop_fa_db, args=(outcome_queue,), daemon=True)
    drop_process.start()

    def ensure_fa_bob_terminated():
        """Safety net, always run (LIFO, before on_exit drops the database/users): if the in-test
        TERMINATE SESSIONS call below is never reached (an earlier assertion failed), fa_bob would
        otherwise still be idling inside fa_db, and on_exit's DROP DATABASE would pay the full
        drain deadline -- or hang altogether if _run_drop_fa_db above is still mid-flight.
        """
        try:
            fa_bob_uuid = get_session_uuid_by_username(watch_cursor, "fa_bob")
            execute_and_fetch_all(watch_cursor, f"TERMINATE SESSIONS '{fa_bob_uuid}'")
        except (mgclient.Error, AssertionError):
            pass  # already terminated by the test body, or fa_bob's session is already gone
        drop_process.join(timeout=15)
        if drop_process.is_alive():
            drop_process.terminate()
            drop_process.join(timeout=5)

    request.addfinalizer(ensure_fa_bob_terminated)

    wait_until(
        lambda: any(
            row[0] == "fa_db" and row[1] == "DRAINING" for row in execute_and_fetch_all(watch_cursor, "SHOW DATABASES")
        ),
        message="DROP DATABASE ... FORCE ABORT never reached its bounded drain wait",
    )

    bob_uuid = get_session_uuid_by_username(watch_cursor, "fa_bob")
    terminate_rows = execute_and_fetch_all(watch_cursor, f"TERMINATE SESSIONS '{bob_uuid}'")
    assert terminate_rows == [(bob_uuid, True)]

    drop_process.join(timeout=15)
    assert not drop_process.is_alive(), "DROP DATABASE ... FORCE ABORT never returned after TERMINATE SESSIONS"
    try:
        outcome_kind, outcome_detail, elapsed = outcome_queue.get(timeout=5)
    except Empty:
        pytest.fail("DROP DATABASE ... FORCE ABORT process exited without reporting an outcome")
    assert outcome_kind == "rows", f"DROP DATABASE ... FORCE ABORT raised: {outcome_detail}"
    assert outcome_detail == [("Successfully deleted fa_db",)]
    # kDrainDeadline is 10 s; converging well under that is the only way to tell "released promptly"
    # apart from "timed out anyway" -- both eventually return the same STATUS row.
    assert elapsed < 5.0, (
        f"drop took {elapsed:.1f}s -- looks like it paid the full drain deadline "
        "instead of converging once fa_bob's session was torn down"
    )

    assert not any(row[0] == "fa_db" for row in execute_and_fetch_all(watch_cursor, "SHOW DATABASES"))


def test_database_scoped_admin_confined_to_own_tenant(request):
    """Regression: a database-scoped admin must not be able to terminate a session in another tenant.

    TERMINATE SESSIONS used to authorize with std::nullopt as the database scope, which auth::models
    reads as "unfiltered": User::GetPermissions skips the HasAccess gate entirely, and
    Roles::GetFilteredRoles returns every role regardless of which databases those roles are granted
    on. A tenant_a-scoped admin could therefore kill a session living in tenant_b. The check now runs
    against the target session's own current database -- see InterpreterContext::TerminateSessions.

    Both halves are asserted on purpose. The cross-tenant refusal on its own would pass just as
    happily if TERMINATE SESSIONS were simply broken for everyone; the same-tenant kill that follows
    is what proves the new check is *scoped* rather than merely denying.
    """
    superadmin_cursor = connect().cursor()

    # CREATE DATABASE needs MULTI_DATABASE_EDIT, which scoped_admin must not have -- so the tenant
    # DDL goes through a separate fully privileged user, as in test_force_abort_converges_after_
    # terminate. Creating it first also means scoped_admin is never this instance's first user, and
    # so is never auto-promoted (see the module-level comment); the explicit stripping below makes
    # that guarantee independent of ordering anyway.
    execute_and_fetch_all(superadmin_cursor, "CREATE USER tenant_ddl_admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT ALL PRIVILEGES TO tenant_ddl_admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT DATABASE * TO tenant_ddl_admin")

    ddl_cursor = connect(username="tenant_ddl_admin", password="").cursor()
    execute_and_fetch_all(ddl_cursor, "CREATE DATABASE tenant_a")
    execute_and_fetch_all(ddl_cursor, "CREATE DATABASE tenant_b")

    execute_and_fetch_all(superadmin_cursor, "CREATE USER scoped_admin")
    execute_and_fetch_all(superadmin_cursor, "REVOKE ALL PRIVILEGES FROM scoped_admin")
    execute_and_fetch_all(superadmin_cursor, "CLEAR ROLE FOR scoped_admin")
    # Do not rely on the default database grant: auth::Databases' constructor grants every fresh user
    # the `memgraph` database, and a first user is granted *all* of them (allow_all_). Either would
    # blur what "scoped to tenant_a" means here, so revoke the lot and grant back exactly tenant_a.
    # RevokeAll() also blanks the user's main database, and Databases::GetMain() throws for a main it
    # has no access to -- so main has to be re-pointed at tenant_a before this user can log in.
    execute_and_fetch_all(superadmin_cursor, "REVOKE DATABASE * FROM scoped_admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT DATABASE tenant_a TO scoped_admin")
    execute_and_fetch_all(superadmin_cursor, "SET MAIN DATABASE tenant_a FOR scoped_admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT TRANSACTION_MANAGEMENT TO scoped_admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT MULTI_DATABASE_USE TO scoped_admin")

    # Two distinct usernames, one session each, so get_session_uuid_by_username stays unambiguous.
    for victim, tenant in (("victim_a", "tenant_a"), ("victim_b", "tenant_b")):
        execute_and_fetch_all(superadmin_cursor, f"CREATE USER {victim}")
        execute_and_fetch_all(superadmin_cursor, f"GRANT DATABASE {tenant} TO {victim}")
        execute_and_fetch_all(superadmin_cursor, f"GRANT MULTI_DATABASE_USE TO {victim}")

    def on_exit():
        for db in ("tenant_a", "tenant_b"):
            try:
                execute_and_fetch_all(ddl_cursor, f"DROP DATABASE {db}")
            except mgclient.Error:
                pass  # an earlier assertion failed and left a session parked inside the tenant
        for user in ("scoped_admin", "victim_a", "victim_b", "tenant_ddl_admin"):
            execute_and_fetch_all(superadmin_cursor, f"DROP USER {user}")

    request.addfinalizer(on_exit)

    admin_connection = connect(username="scoped_admin", password="")
    admin_cursor = admin_connection.cursor()

    victim_a_connection = connect(username="victim_a", password="")
    victim_a_cursor = victim_a_connection.cursor()
    execute_and_fetch_all(victim_a_cursor, "USE DATABASE tenant_a")  # now idles inside tenant_a

    victim_b_connection = connect(username="victim_b", password="")
    victim_b_cursor = victim_b_connection.cursor()
    execute_and_fetch_all(victim_b_cursor, "USE DATABASE tenant_b")  # now idles inside tenant_b

    def release_tenants():
        """Safety net, always run (LIFO, before on_exit drops the databases): a session idling inside
        a tenant pins that tenant's accessor, so on_exit's plain DROP DATABASE would pay the drain
        deadline or fail outright. Closing the client sockets is enough to release them, but the
        server tears the sessions down on their own strands -- poll for that rather than assume it.
        """
        for connection in (admin_connection, victim_a_connection, victim_b_connection):
            with suppress(mgclient.Error):
                connection.close()
        wait_until(
            lambda: not any(
                row[0] in ("scoped_admin", "victim_a", "victim_b")
                for row in execute_and_fetch_all(superadmin_cursor, "SHOW ACTIVE USERS INFO")
            ),
            message="tenant_a/tenant_b sessions were still registered after their connections closed",
        )

    request.addfinalizer(release_tenants)

    # SHOW ACTIVE USERS INFO requires the STATS privilege, which scoped_admin deliberately lacks, so
    # the uuid lookups go through superadmin_cursor -- exactly as in test_unprivileged_user_refused.
    victim_a_uuid = get_session_uuid_by_username(superadmin_cursor, "victim_a")
    victim_b_uuid = get_session_uuid_by_username(superadmin_cursor, "victim_b")

    # Cross-tenant: scoped_admin has TRANSACTION_MANAGEMENT but no access to tenant_b, so it cannot
    # reach a session sitting there. This is the regression itself.
    results = execute_and_fetch_all(admin_cursor, f"TERMINATE SESSIONS '{victim_b_uuid}'")
    assert results == [(victim_b_uuid, False)]
    assert_connection_alive(victim_b_cursor)

    # Own tenant: same admin, same statement, target inside tenant_a -- must still work. Without this
    # half, the assertion above would be satisfied by a TERMINATE SESSIONS that never kills anything.
    results = execute_and_fetch_all(admin_cursor, f"TERMINATE SESSIONS '{victim_a_uuid}'")
    assert results == [(victim_a_uuid, True)]
    wait_until_terminated(victim_a_cursor)


def test_dbless_session_terminated_via_default_db_fallback(request):
    """A session holding NO database has no tenant for a database-scoped privilege to be evaluated
    against, so InterpreterContext::TerminateSessions (src/query/interpreter_context.cpp) falls back
    to authorizing against dbms::kDefaultDB ("memgraph") instead of refusing outright. An admin who
    holds TRANSACTION_MANAGEMENT scoped to "memgraph" can therefore terminate a dbless session even
    though that session itself never touched "memgraph".

    This used to be a deliberate fail-closed refusal (a dbless session was unevictable by anyone but
    the same user), which made the DROP DATABASE ... FORCE ABORT scenario that produces one of the
    three dbless routes below effectively un-administrable. The fallback trades that for a narrower
    exposure: a "memgraph"-scoped admin can now reach sessions belonging to no tenant at all.

    The dbless state is reached through a real production route, not a synthetic one. TryDefaultDB
    (src/glue/SessionHL.cpp) asks GetDefaultDB for the user's main database; when Databases::GetMain
    throws because the user has no access to it, GetDefaultDB returns nullopt and TryDefaultDB calls
    interpreter_.ResetDB() and lets the connection through regardless ("Support non-db connection").
    REVOKE DATABASE * is what arms that path: Databases::RevokeAll (src/auth/models.cpp) clears the
    grants and blanks main_db_ in one go. The session still authenticates and still gets its uuid --
    SetSessionInfo runs before TryDefaultDB -- so it stays findable while holding nothing. Findable
    is all it is: that login is the only place the AuthException is swallowed, so the session can
    never run a single query afterwards, which is what both probes below have to work around.

    Enterprise only: the entire multi-database surface used here is MG_ENTERPRISE-gated.
    """
    superadmin_cursor = connect().cursor()

    # full_admin is created first on purpose: that makes nodb_victim a non-first user, so its dbless
    # state below is produced by the explicit REVOKE rather than having to fight first-user
    # auto-promotion (see the module-level comment). Any auto-promotion full_admin itself receives
    # only reinforces what the two explicit grants already give it.
    execute_and_fetch_all(superadmin_cursor, "CREATE USER full_admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT ALL PRIVILEGES TO full_admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT DATABASE * TO full_admin")

    execute_and_fetch_all(superadmin_cursor, "CREATE USER nodb_victim")
    execute_and_fetch_all(superadmin_cursor, "REVOKE DATABASE * FROM nodb_victim")

    # nodb_control keeps the `memgraph` grant auth::Databases' constructor hands every fresh user, so
    # it differs from nodb_victim in exactly one respect: it holds a database.
    execute_and_fetch_all(superadmin_cursor, "CREATE USER nodb_control")

    def on_exit():
        for user in ("full_admin", "nodb_victim", "nodb_control"):
            execute_and_fetch_all(superadmin_cursor, f"DROP USER {user}")

    request.addfinalizer(on_exit)

    admin_connection = connect(username="full_admin", password="")
    admin_cursor = admin_connection.cursor()

    victim_connection = connect(username="nodb_victim", password="")
    victim_cursor = victim_connection.cursor()

    control_connection = connect(username="nodb_control", password="")
    control_cursor = control_connection.cursor()
    execute_and_fetch_all(control_cursor, "RETURN 1")  # goes idle, inside `memgraph`

    def release_sessions():
        """Safety net, always run (LIFO, before on_exit drops the users): if the fallback under test
        regresses back into a refusal, nodb_victim would again be unevictable and leak a live session
        into every later test's SHOW ACTIVE USERS INFO. Closing the client sockets releases them, but
        the server tears the sessions down on their own strands -- poll for that.
        """
        for connection in (admin_connection, victim_connection, control_connection):
            with suppress(mgclient.Error):
                connection.close()
        wait_until(
            lambda: not any(
                row[0] in ("full_admin", "nodb_victim", "nodb_control")
                for row in execute_and_fetch_all(superadmin_cursor, "SHOW ACTIVE USERS INFO")
            ),
            message="sessions were still registered after their connections closed",
        )

    request.addfinalizer(release_sessions)

    # SHOW ACTIVE USERS INFO reports (username, session uuid, login timestamp) and no database
    # column, so the dbless state has to be established by probing the session itself, below.
    victim_uuid = get_session_uuid_by_username(superadmin_cursor, "nodb_victim")
    control_uuid = get_session_uuid_by_username(superadmin_cursor, "nodb_control")

    def assert_victim_is_dbless():
        """Dblessness proven by the error the session raises: holding no database, every query fails
        while resolving one instead of executing.
        """
        with pytest.raises(mgclient.DatabaseError, match="No access to the set default database"):
            execute_and_fetch_all(victim_cursor, "RETURN 1")

    def assert_victim_still_registered():
        """The victim's liveness probe, necessarily observed from outside the session itself.

        common.py's assert_connection_alive cannot serve here: it runs RETURN 1, and this session
        cannot execute *any* query. Every Bolt RUN re-enters SessionHL::Configure ->
        RuntimeConfig::Configure (src/glue/SessionHL.cpp), whose "Step 3: Determine final target
        database" re-resolves the user's default database through session_user_or_role_->
        GetDefaultDB() with no try/catch -- and for a REVOKE DATABASE * user that is
        Databases::GetMain throwing AuthException. Only login's TryDefaultDB catches it, so the
        connection is registered and wedged at once. Liveness is therefore read off SHOW ACTIVE USERS
        INFO on another connection: the uuid is still listed, i.e. the session was not torn down.
        """
        rows = execute_and_fetch_all(superadmin_cursor, "SHOW ACTIVE USERS INFO")
        assert any(row[1] == victim_uuid for row in rows), f"session {victim_uuid} is no longer registered"

    # Assumption checks, before anything is asserted about the fallback: nodb_victim really did log
    # in, really is registered, and really holds no database. Without these the fallback below would
    # pass just as happily against a session that simply could not be found.
    assert_victim_is_dbless()
    assert_victim_still_registered()

    # The fallback itself: the most privileged caller the instance can produce reaches a target that
    # sits in no tenant, because the check falls back to authorizing against "memgraph".
    results = execute_and_fetch_all(admin_cursor, f"TERMINATE SESSIONS '{victim_uuid}'")
    assert results == [(victim_uuid, True)]
    # wait_until_terminated can't be used on victim_cursor: its RETURN 1 already raises before
    # termination too (assert_victim_is_dbless), so that probe can't tell "killed" from "was always
    # wedged". Liveness is read the same way assert_victim_still_registered reads it -- off SHOW
    # ACTIVE USERS INFO on a different connection -- polled until the uuid disappears.
    wait_until(
        lambda: not any(
            row[1] == victim_uuid for row in execute_and_fetch_all(superadmin_cursor, "SHOW ACTIVE USERS INFO")
        ),
        message=f"session {victim_uuid} was still registered after being terminated",
    )

    # Control, same caller and same statement, against a target whose only difference is that it holds
    # a database of its own. Without it the fallback above would be indistinguishable from a
    # full_admin that can terminate anything at all, dbless or not.
    results = execute_and_fetch_all(admin_cursor, f"TERMINATE SESSIONS '{control_uuid}'")
    assert results == [(control_uuid, True)]
    wait_until_terminated(control_cursor)


def test_dbless_session_refused_when_caller_lacks_default_db_access(request):
    """Negative control for the default-db fallback (see
    test_dbless_session_terminated_via_default_db_fallback): a caller who holds TRANSACTION_MANAGEMENT
    but has no DATABASE access to "memgraph" must still be refused against a dbless target.

    Without this test, a regression that dropped the privilege_checker call for a dbless target
    entirely (i.e. an unconditional grant instead of a scoped fallback) would pass the suite just as
    happily as the fix -- the positive test alone only proves the fallback authorizes against *some*
    privilege, not that it is still scoped. TRANSACTION_MANAGEMENT is granted globally, but
    IsAuthorized additionally requires DATABASE access to the db name it is evaluated against (see
    test_database_scoped_admin_confined_to_own_tenant), so a caller confined to some other tenant
    fails the check even while holding the privilege everywhere it does have access.
    """
    superadmin_cursor = connect().cursor()

    # A side tenant purely so scoped_only_admin has somewhere to log in without touching `memgraph` --
    # CREATE DATABASE needs MULTI_DATABASE_EDIT, which scoped_only_admin must not have, so the DDL
    # goes through a separately privileged user, as in test_database_scoped_admin_confined_to_own_tenant.
    execute_and_fetch_all(superadmin_cursor, "CREATE USER tenant_ddl_admin2")
    execute_and_fetch_all(superadmin_cursor, "GRANT ALL PRIVILEGES TO tenant_ddl_admin2")
    execute_and_fetch_all(superadmin_cursor, "GRANT DATABASE * TO tenant_ddl_admin2")

    ddl_cursor = connect(username="tenant_ddl_admin2", password="").cursor()
    execute_and_fetch_all(ddl_cursor, "CREATE DATABASE side_tenant")

    execute_and_fetch_all(superadmin_cursor, "CREATE USER scoped_only_admin")
    execute_and_fetch_all(superadmin_cursor, "REVOKE ALL PRIVILEGES FROM scoped_only_admin")
    execute_and_fetch_all(superadmin_cursor, "CLEAR ROLE FOR scoped_only_admin")
    # Same reasoning as test_database_scoped_admin_confined_to_own_tenant: revoke the default `memgraph`
    # grant every fresh user gets and re-point main at side_tenant before granting back exactly that.
    execute_and_fetch_all(superadmin_cursor, "REVOKE DATABASE * FROM scoped_only_admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT DATABASE side_tenant TO scoped_only_admin")
    execute_and_fetch_all(superadmin_cursor, "SET MAIN DATABASE side_tenant FOR scoped_only_admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT TRANSACTION_MANAGEMENT TO scoped_only_admin")

    execute_and_fetch_all(superadmin_cursor, "CREATE USER nodb_victim2")
    execute_and_fetch_all(superadmin_cursor, "REVOKE DATABASE * FROM nodb_victim2")

    def on_exit():
        try:
            execute_and_fetch_all(ddl_cursor, "DROP DATABASE side_tenant")
        except mgclient.Error:
            pass  # an earlier assertion failed and left a session parked inside the tenant
        for user in ("tenant_ddl_admin2", "scoped_only_admin", "nodb_victim2"):
            execute_and_fetch_all(superadmin_cursor, f"DROP USER {user}")

    request.addfinalizer(on_exit)

    admin_connection = connect(username="scoped_only_admin", password="")
    admin_cursor = admin_connection.cursor()

    victim_connection = connect(username="nodb_victim2", password="")
    victim_cursor = victim_connection.cursor()

    def release_sessions():
        """Safety net, always run (LIFO, before on_exit drops the users and the database): closing the
        client sockets releases the accessors, but the server tears the sessions down on their own
        strands -- poll for that, same pattern as the fallback test above.
        """
        for connection in (admin_connection, victim_connection):
            with suppress(mgclient.Error):
                connection.close()
        wait_until(
            lambda: not any(
                row[0] in ("scoped_only_admin", "nodb_victim2")
                for row in execute_and_fetch_all(superadmin_cursor, "SHOW ACTIVE USERS INFO")
            ),
            message="sessions were still registered after their connections closed",
        )

    request.addfinalizer(release_sessions)

    victim_uuid = get_session_uuid_by_username(superadmin_cursor, "nodb_victim2")

    # Same dbless proof as the fallback test: the session can run no query at all, so it can only
    # be failing to resolve its (nonexistent) default database.
    with pytest.raises(mgclient.DatabaseError, match="No access to the set default database"):
        execute_and_fetch_all(victim_cursor, "RETURN 1")

    # The refusal: scoped_only_admin holds TRANSACTION_MANAGEMENT, but not on "memgraph", and the
    # fallback authorizes against "memgraph" -- so it must still be denied.
    results = execute_and_fetch_all(admin_cursor, f"TERMINATE SESSIONS '{victim_uuid}'")
    assert results == [(victim_uuid, False)]

    rows = execute_and_fetch_all(superadmin_cursor, "SHOW ACTIVE USERS INFO")
    assert any(row[1] == victim_uuid for row in rows), f"session {victim_uuid} is no longer registered"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
