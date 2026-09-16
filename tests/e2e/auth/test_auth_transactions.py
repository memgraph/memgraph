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
    with pytest.raises(mgclient.DatabaseError):
        execute(cursor, "CREATE (n:Node)")
    # The failed statement already aborted the transaction, so there is nothing left to roll back.


def test_profile_queries_are_rejected_in_an_auth_transaction(cursor):
    # User profiles are out of scope: UserProfiles reads an in-memory cache rather than the store, so the overlay
    # cannot isolate or roll them back.
    execute(cursor, "BEGIN")
    execute(cursor, "CREATE USER grace")
    with pytest.raises(mgclient.DatabaseError):
        execute(cursor, "CREATE USER PROFILE limited LIMIT sessions 1")


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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
