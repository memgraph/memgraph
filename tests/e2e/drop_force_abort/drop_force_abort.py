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
E2E coverage for `DROP DATABASE ... FORCE ABORT` draining against a live accessor.

The drop bounds its drain against `DbmsHandler::kDrainDeadline` (10s) and converges once the
tenant's gatekeeper has no holder left but the drop's own accessor. An idle *pooled* Bolt
connection used to hold onto its `Interpreter::current_db_.db_acc_` until its next request, so the
drain always ran to the full 10s deadline instead of converging in milliseconds. `Successfully
deleted <db>` is returned either way, so wall-clock elapsed time -- not the result row -- is the
signal that distinguishes a converged drop from an expired one.
"""

import sys
import time
import uuid

import mgclient
import pytest

BOLT_PORT = 7687
DB_NAME_PREFIX = "e2e_dfa_"

# Generous margin under the 10s kDrainDeadline: a converged drop is expected to take milliseconds,
# an expired one always takes ~10s, so anything below this line still cleanly tells the two apart.
CONVERGENCE_BUDGET_SEC = 2.0


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
    FORCE ABORT so a test's deliberately-left-idle pinning connection can't block it.
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
            execute(cursor, f"DROP DATABASE {name} FORCE ABORT")
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


def force_abort_drop(db_name):
    """Run `DROP DATABASE ... FORCE ABORT` from a fresh admin connection and return elapsed seconds."""
    admin = connect()
    cursor = admin.cursor()
    started = time.monotonic()
    execute(cursor, f"DROP DATABASE {db_name} FORCE ABORT")
    cursor.fetchall()
    return time.monotonic() - started


def test_force_abort_converges_against_idle_connection(make_database):
    db_name = make_database("converges")
    pin_database_idle(db_name)

    elapsed = force_abort_drop(db_name)

    assert elapsed < CONVERGENCE_BUDGET_SEC, (
        f"DROP DATABASE {db_name} FORCE ABORT took {elapsed:.3f}s against an idle pooled "
        f"connection (budget {CONVERGENCE_BUDGET_SEC}s). A converged drop is expected to take "
        f"milliseconds; anything this slow means the drain expired against kDrainDeadline (10s) "
        f"instead of converging."
    )


def test_idle_session_after_drop_is_usable(make_database):
    db_name = make_database("idle_usable")
    idle = pin_database_idle(db_name)
    idle_cursor = idle.cursor()

    force_abort_drop(db_name)

    with pytest.raises(mgclient.DatabaseError):
        fetch_all(idle_cursor, "MATCH (n) RETURN n LIMIT 1")

    # A session whose database vanished underneath it is not bricked: SHOW DATABASES still works.
    assert db_name not in list_databases(idle_cursor)


def test_dropped_database_is_gone(make_database):
    db_name = make_database("gone")
    force_abort_drop(db_name)

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
