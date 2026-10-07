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

import sys

import mgclient
import pytest


def connect():
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True
    return connection


def execute(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()


def usernames(cursor):
    return {row[0] for row in execute(cursor, "SHOW USERS")}


@pytest.fixture
def cursor():
    connection = connect()
    cur = connection.cursor()
    yield cur
    # Drop through the same connection: the first user created turns authentication on, so a fresh connection
    # would be refused before it could clean up.
    try:
        cur.execute("ROLLBACK")
        cur.fetchall()
    except Exception:
        pass
    for user in usernames(cur):
        try:
            execute(cur, f"DROP USER {user}")
        except Exception:
            pass


def test_committed_users_are_visible(cursor):
    execute(cursor, "BEGIN")
    execute(cursor, "CREATE USER alice")
    execute(cursor, "CREATE USER bob")
    execute(cursor, "COMMIT")

    assert {"alice", "bob"} <= usernames(cursor)


def test_rolled_back_users_never_appear(cursor):
    execute(cursor, "BEGIN")
    execute(cursor, "CREATE USER carol")
    execute(cursor, "ROLLBACK")

    assert "carol" not in usernames(cursor)


def test_uncommitted_users_are_invisible_to_another_session(cursor):
    # Opened before any user exists: the first committed user turns authentication on, and this connection has no
    # credentials to re-establish itself with.
    other = connect().cursor()

    execute(cursor, "BEGIN")
    execute(cursor, "CREATE USER dave")
    assert "dave" not in usernames(other), "uncommitted user leaked to another session"

    execute(cursor, "COMMIT")
    assert "dave" in usernames(other), "committed user not visible to an existing session"


def test_the_transaction_sees_its_own_writes(cursor):
    execute(cursor, "BEGIN")
    execute(cursor, "CREATE USER erin")
    assert "erin" in usernames(cursor), "a transaction must read its own uncommitted writes"
    execute(cursor, "ROLLBACK")


def test_mixing_auth_and_data_queries_is_rejected(cursor):
    execute(cursor, "BEGIN")
    execute(cursor, "CREATE USER frank")
    with pytest.raises(mgclient.DatabaseError, match="cannot mix auth queries with data queries"):
        execute(cursor, "CREATE (n:Node)")
    # The failed statement already aborted the transaction, so there is nothing left to roll back.


def test_profile_queries_are_rejected_in_an_auth_transaction(cursor):
    # User profiles are out of scope: UserProfiles reads an in-memory cache rather than the store, so the overlay
    # cannot isolate or roll them back. After an auth statement a profile query is not an auth query, so the mixing
    # guard is what refuses it; the profile guard itself is covered by the test below.
    execute(cursor, "BEGIN")
    execute(cursor, "CREATE USER grace")
    with pytest.raises(mgclient.DatabaseError, match="cannot mix auth queries with data queries"):
        execute(cursor, "CREATE PROFILE limited")


def test_a_profile_query_is_rejected_as_the_first_statement(cursor):
    # The other ordering. Arriving before any auth statement, a profile query would otherwise be let through and
    # write durably at once, outliving the ROLLBACK.
    execute(cursor, "BEGIN")
    with pytest.raises(mgclient.DatabaseError, match="Managing users is not allowed in multicommand transactions"):
        execute(cursor, "CREATE PROFILE early")


def test_a_read_only_auth_transaction_commits(cursor):
    # Nothing to publish means no system transaction is taken, so COMMIT leaves through a different exit from a
    # transaction that wrote. The session has to come back usable either way.
    execute(cursor, "CREATE USER alice")
    execute(cursor, "BEGIN")
    assert usernames(cursor) == {"alice"}
    execute(cursor, "COMMIT")

    execute(cursor, "CREATE USER bob")
    assert usernames(cursor) == {"alice", "bob"}


def test_profile_reads_are_allowed_in_a_data_transaction(cursor):
    # Only writes cannot be isolated. A profile query is not an auth query, so this is an ordinary data
    # transaction, and rejecting the SHOW family here would take away what works outside one.
    execute(cursor, "CREATE PROFILE readable LIMIT sessions 1")
    execute(cursor, "BEGIN")
    assert any(row[0] == "readable" for row in execute(cursor, "SHOW PROFILES"))
    execute(cursor, "COMMIT")
    execute(cursor, "DROP PROFILE readable")


def test_a_profile_read_does_not_hold_the_system_lock(cursor):
    # A read publishes nothing, so it must not take a system transaction. Taking one would hold the system mutex
    # for the rest of the transaction: another session's system queries would be refused for that whole time, and
    # a second read here would find the mutex already taken.
    other = connect().cursor()
    execute(cursor, "CREATE PROFILE held LIMIT sessions 1")

    execute(cursor, "BEGIN")
    assert any(row[0] == "held" for row in execute(cursor, "SHOW PROFILES"))

    # Another session's system queries still go through.
    execute(other, "CREATE PROFILE elsewhere LIMIT sessions 1")

    # And a second read in this transaction does not need a second system transaction.
    assert any(row[0] == "elsewhere" for row in execute(cursor, "SHOW PROFILES"))
    execute(cursor, "COMMIT")

    execute(cursor, "DROP PROFILE held")
    execute(cursor, "DROP PROFILE elsewhere")


def test_a_tenant_profile_read_does_not_hold_the_system_lock(cursor):
    # Same rule as the user profile read above: a read publishes nothing, so it takes no system transaction and
    # leaves the system mutex free for the rest of the transaction.
    other = connect().cursor()
    execute(cursor, "CREATE TENANT PROFILE tenant_held LIMIT memory_limit 100MB")

    execute(cursor, "BEGIN")
    assert any(row[0] == "tenant_held" for row in execute(cursor, "SHOW TENANT PROFILES"))

    execute(other, "CREATE PROFILE tenant_elsewhere LIMIT sessions 1")
    assert execute(cursor, "SHOW TENANT PROFILES")
    execute(cursor, "COMMIT")

    execute(cursor, "DROP TENANT PROFILE tenant_held")
    execute(cursor, "DROP PROFILE tenant_elsewhere")


def test_profile_writes_are_still_rejected_in_a_data_transaction(cursor):
    # The write half of the same guard, and the reason it exists. After a data statement a profile query does not
    # mix modes, so the profile guard is what refuses it.
    execute(cursor, "BEGIN")
    execute(cursor, "MATCH (n) RETURN n")
    with pytest.raises(mgclient.DatabaseError, match="Managing users is not allowed in multicommand transactions"):
        execute(cursor, "CREATE PROFILE unwritable LIMIT sessions 1")


def test_a_transaction_conflicts_with_a_concurrent_change(cursor):
    # Requirement 6: two transactions touching the same users cannot both win. The first reads the user list,
    # another session changes it underneath, and the first is refused at COMMIT rather than overwriting silently.
    # Opened before the first user exists, since creating one turns authentication on.
    other = connect().cursor()

    execute(cursor, "CREATE USER alice")

    execute(cursor, "BEGIN")
    assert usernames(cursor) == {"alice"}

    # The other session commits a change to the set this transaction just read.
    execute(other, "CREATE USER bob")

    execute(cursor, "CREATE USER carol")
    # Reported as a serialization conflict, the same class the data path uses, so a driver retries rather than
    # treating the query itself as wrong.
    with pytest.raises(mgclient.DatabaseError, match="Retry this transaction"):
        execute(cursor, "COMMIT")

    # The loser's write is gone; the winner's stands.
    assert usernames(other) == {"alice", "bob"}


def test_a_terminated_auth_transaction_cannot_commit(cursor):
    # Termination is cooperative: TERMINATE marks the transaction, and the committer is responsible for refusing
    # to go ahead. An auth transaction releases the data accessor, so it takes a different commit path from a data
    # transaction and has to make that check for itself.
    other = connect().cursor()

    execute(cursor, "BEGIN")
    execute(cursor, "CREATE USER doomed")

    execute(other, 'TERMINATE TRANSACTIONS "*"')

    with pytest.raises(mgclient.DatabaseError):
        execute(cursor, "COMMIT")

    assert "doomed" not in usernames(other), "a terminated transaction committed anyway"


def test_an_auth_transaction_does_not_commit_after_demotion(cursor):
    # The role is checked when a statement is prepared, so COMMIT has to check it again: a transaction opened on
    # MAIN would otherwise write into a replica's auth store, which the instance that stays MAIN never sees.
    other = connect().cursor()
    execute(cursor, "BEGIN")
    execute(cursor, "CREATE USER zed")

    execute(other, "SET REPLICATION ROLE TO REPLICA WITH PORT 10000")
    try:
        with pytest.raises(mgclient.DatabaseError, match="not main anymore"):
            execute(cursor, "COMMIT")
    finally:
        execute(other, "SET REPLICATION ROLE TO MAIN")

    assert "zed" not in usernames(other)


def database_grants(cursor, user):
    return execute(cursor, f"SHOW DATABASE PRIVILEGES FOR {user}")[0][0]


# SET MAIN DATABASE needs access to the database first. Access through `*` leaves the user's record unchanged when
# d1 is dropped, so only the existence check can catch it.
@pytest.mark.parametrize(
    "setup, statement",
    [(None, "GRANT DATABASE d1 TO u"), ("GRANT DATABASE * TO u", "SET MAIN DATABASE d1 FOR u")],
)
def test_a_database_dropped_before_commit_fails_the_commit(cursor, setup, statement):
    # The statement checks that the database exists, but the write lands at COMMIT. A database dropped in between
    # must fail the commit, or a database later created under the same name inherits the change.
    other = connect().cursor()
    execute(cursor, "CREATE DATABASE d1")
    execute(cursor, "CREATE USER u")
    if setup:
        execute(cursor, setup)

    execute(cursor, "BEGIN")
    execute(cursor, statement)
    execute(other, "DROP DATABASE d1")
    with pytest.raises(mgclient.DatabaseError, match='unknown database "d1"'):
        execute(cursor, "COMMIT")

    execute(cursor, "CREATE DATABASE d1")
    try:
        assert "d1" not in database_grants(cursor, "u")
    finally:
        execute(cursor, "DROP DATABASE d1")


def transaction_watcher(cursor):
    execute(cursor, "CREATE USER watcher IDENTIFIED BY 'watcherpw'")
    execute(cursor, "GRANT TRANSACTION_MANAGEMENT TO watcher")
    watcher = mgclient.connect(host="localhost", port=7687, username="watcher", password="watcherpw")
    watcher.autocommit = True
    return watcher.cursor()


def test_show_transactions_masks_a_password_in_an_open_auth_transaction(cursor):
    # A transaction stays open for as long as its client likes, so the statement text SHOW TRANSACTIONS returns
    # must not carry the password to a user who may manage transactions but not auth.
    watcher = transaction_watcher(cursor)

    execute(cursor, "BEGIN")
    execute(cursor, "CREATE USER x IDENTIFIED BY 'hunter2'")
    shown = str(execute(watcher, "SHOW TRANSACTIONS"))

    assert "CREATE USER x" in shown
    assert "hunter2" not in shown


def test_show_transactions_shows_a_data_statement_as_written(cursor):
    # The masker reads "rpa'" in 'Sherpa' as the start of a credential; only auth statements are masked.
    watcher = transaction_watcher(cursor)

    execute(cursor, "BEGIN")
    execute(cursor, "MATCH (n {name: 'Sherpa'}) RETURN n")
    shown = str(execute(watcher, "SHOW TRANSACTIONS"))

    assert "MATCH (n {name: 'Sherpa'}) RETURN n" in shown


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
