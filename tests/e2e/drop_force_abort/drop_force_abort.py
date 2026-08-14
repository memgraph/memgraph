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
E2E coverage for releasing a session's database accessor between queries (`DROP DATABASE ... FORCE`).

An idle *pooled* Bolt connection used to hold onto its `Interpreter::current_db_.db_acc_` until its
next request, pinning the tenant's gatekeeper so a `DROP DATABASE ... FORCE` could never free the
database's storage. The session now releases that accessor once a query finishes, so a dropped
database is fully gone: the idle session sees its database vanish on its next query, `SHOW DATABASES`
no longer lists it, and the session is not bricked. An explicit transaction keeps its accessor for
the duration of the transaction, and repeated queries transparently re-acquire it.
"""

import sys
import uuid

import mgclient
import pytest

BOLT_PORT = 7687
DB_NAME_PREFIX = "e2e_dfa_"


def connect(database="memgraph", autocommit=True):
    conn = mgclient.connect(host="localhost", port=BOLT_PORT)
    conn.autocommit = True
    if database != "memgraph":
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {database}")
    conn.autocommit = autocommit
    return conn


def execute(cursor, query, params=None):
    if params is None:
        cursor.execute(query)
    else:
        cursor.execute(query, params)


def fetch_all(cursor, query, params=None):
    execute(cursor, query, params)
    return cursor.fetchall()


def unique_db_name(tag):
    return f"{DB_NAME_PREFIX}{tag}_{uuid.uuid4().hex[:8]}"


def create_database(cursor, name):
    try:
        execute(cursor, f"CREATE DATABASE {name}")
        cursor.fetchall()
    except mgclient.DatabaseError as exc:
        lowered = str(exc).lower()
        if "enterprise" in lowered or "not supported" in lowered:
            pytest.skip(f"CREATE DATABASE requires enterprise features: {exc}")
        raise


def list_databases(cursor):
    return [row[0] for row in fetch_all(cursor, "SHOW DATABASES")]


@pytest.fixture(scope="module")
def make_database():
    """
    Factory fixture, module-scoped: the admin connection it owns is set up once, but each call
    mints a fresh, uniquely-named database so tests stay independent and repeat-safe. Cleanup uses
    FORCE so a test's deliberately-left-idle pinning connection can't block it.
    """
    admin = connect()
    created = []

    def _make(tag):
        name = unique_db_name(tag)
        create_database(admin.cursor(), name)
        created.append(name)
        return name

    yield _make

    cursor = admin.cursor()
    for name in created:
        try:
            execute(cursor, f"DROP DATABASE {name} FORCE")
            cursor.fetchall()
        except mgclient.DatabaseError:
            pass
    admin.close()


def pin_database_idle(db_name):
    """Open a connection that touches `db_name` and then goes idle without closing -- the pooled accessor under test."""
    conn = connect(database=db_name)
    cursor = conn.cursor()
    execute(cursor, "CREATE (:N {x: 1})")
    cursor.fetchall()
    assert fetch_all(cursor, "MATCH (n) RETURN count(n)")[0][0] == 1
    return conn


def force_drop(db_name):
    """Run `DROP DATABASE ... FORCE` from a fresh admin connection."""
    admin = connect()
    cursor = admin.cursor()
    execute(cursor, f"DROP DATABASE {db_name} FORCE")
    cursor.fetchall()


def test_idle_session_after_drop_is_usable(make_database):
    db_name = make_database("idle_usable")
    idle = pin_database_idle(db_name)
    idle_cursor = idle.cursor()

    # The pin proves the accessor was actually held before the drop: without the release-between-queries
    # fix, `idle` keeps its db_acc_ and the next query below would read the zombie instead of throwing.
    assert fetch_all(idle_cursor, "MATCH (n) RETURN count(n)")[0][0] == 1

    force_drop(db_name)

    # The session released its accessor after its last query, so the drop converged and its database is
    # gone: the next query re-acquires and finds nothing.
    with pytest.raises(mgclient.DatabaseError):
        fetch_all(idle_cursor, "MATCH (n) RETURN n LIMIT 1")

    # A session whose database vanished underneath it is not bricked: SHOW DATABASES still works.
    assert db_name not in list_databases(idle_cursor)


def test_dropped_database_is_gone(make_database):
    db_name = make_database("gone")
    force_drop(db_name)

    fresh_cursor = connect().cursor()
    assert db_name not in list_databases(fresh_cursor)


def test_explicit_transaction_keeps_its_database(make_database):
    db_name = make_database("explicit_txn")
    cursor = connect(database=db_name).cursor()

    execute(cursor, "BEGIN")
    assert fetch_all(cursor, "SHOW DATABASE")[0][0] == db_name
    execute(cursor, "CREATE (:N {x: 1})")
    assert fetch_all(cursor, "SHOW DATABASE")[0][0] == db_name
    assert fetch_all(cursor, "MATCH (n) RETURN count(n)")[0][0] == 1
    assert fetch_all(cursor, "SHOW DATABASE")[0][0] == db_name
    execute(cursor, "COMMIT")

    assert fetch_all(cursor, "SHOW DATABASE")[0][0] == db_name


def test_repeated_queries_on_non_default_database(make_database):
    db_name = make_database("repeated")
    cursor = connect(database=db_name).cursor()

    for i in range(200):
        execute(cursor, "CREATE (:N {i: $i})", {"i": i})
        cursor.fetchall()

    assert fetch_all(cursor, "MATCH (n:N) RETURN count(n)")[0][0] == 200


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
