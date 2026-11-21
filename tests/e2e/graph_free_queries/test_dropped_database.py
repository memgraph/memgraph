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
from common import connect, execute_and_fetch_all


def database_states(cursor):
    return dict((row[0], row[1]) for row in execute_and_fetch_all(cursor, "SHOW DATABASES"))


def granted_databases(cursor, user_or_role):
    return execute_and_fetch_all(cursor, f"SHOW DATABASE PRIVILEGES FOR {user_or_role}")[0][0]


def try_execute(cursor, query):
    """Run a best-effort cleanup statement; a missing object must not mask the test's own failure."""
    try:
        execute_and_fetch_all(cursor, query)
    except mgclient.DatabaseError:
        pass


@pytest.fixture(name="admin")
def admin_cursor():
    cursor = connect().cursor()
    yield cursor
    execute_and_fetch_all(cursor, "USE DATABASE memgraph")
    for row in execute_and_fetch_all(cursor, "SHOW DATABASES"):
        # A tenant already in DROPPING state was force-dropped and is draining asynchronously; re-dropping
        # it races the defer worker and errors NON_EXISTENT. Only clean up live (HOT/COLD) tenants here.
        if row[0].startswith("tenant_") and row[1] != "DROPPING":
            execute_and_fetch_all(cursor, f"DROP DATABASE {row[0]} FORCE")


@pytest.mark.parametrize("query", ["RETURN 1", "CALL mg.procedures() YIELD name", "MATCH (n) RETURN n"])
def test_query_on_dropped_database_errors_and_server_survives(admin, query):
    """A session keeps a database selected while it is force-dropped out from under it. The next query on
    that session has no database to run against and must say so.

    A graph-free query is the case worth covering: opening a storage transaction is what checks that the
    session still has a database, and a query that opens none never reaches that check."""
    execute_and_fetch_all(admin, "CREATE DATABASE tenant_drop")

    tenant = connect().cursor()
    execute_and_fetch_all(tenant, "USE DATABASE tenant_drop")
    assert execute_and_fetch_all(tenant, "RETURN 1") == [(1,)]

    # A plain DROP refuses while a session holds the database; FORCE takes it away regardless, which is
    # also what a replica does when it applies a drop from main.
    execute_and_fetch_all(admin, "DROP DATABASE tenant_drop FORCE")

    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(tenant, query)

    # The server is still serving: the failure was the query's, not the process's.
    assert execute_and_fetch_all(admin, "RETURN 1") == [(1,)]


def test_create_or_replace_database_replaces_existing(admin):
    """CREATE OR REPLACE DATABASE is a non-force drop-if-present followed by a create: the old data,
    schema and database-scoped grants are gone, and the name stays listed and usable."""
    execute_and_fetch_all(admin, "CREATE DATABASE tenant_replace")
    tenant = connect().cursor()
    execute_and_fetch_all(tenant, "USE DATABASE tenant_replace")
    execute_and_fetch_all(tenant, "CREATE (:A {id: 1})")
    execute_and_fetch_all(tenant, "CREATE INDEX ON :A(id)")
    execute_and_fetch_all(tenant, "CREATE CONSTRAINT ON (n:A) ASSERT n.id IS UNIQUE")
    execute_and_fetch_all(admin, "CREATE ROLE replace_role")
    execute_and_fetch_all(admin, "GRANT DATABASE tenant_replace TO replace_role")

    assert execute_and_fetch_all(tenant, "MATCH (n:A) RETURN count(n)") == [(1,)]
    assert execute_and_fetch_all(tenant, "SHOW INDEX INFO")
    assert "tenant_replace" in granted_databases(admin, "replace_role")

    # A session holding the database makes the non-force drop half refuse (USING); release it so the
    # replace can drop the tenant.
    execute_and_fetch_all(tenant, "USE DATABASE memgraph")

    execute_and_fetch_all(admin, "CREATE OR REPLACE DATABASE tenant_replace")

    assert database_states(admin).get("tenant_replace") == "HOT"
    execute_and_fetch_all(tenant, "USE DATABASE tenant_replace")
    assert execute_and_fetch_all(tenant, "MATCH (n:A) RETURN count(n)") == [(0,)]
    assert execute_and_fetch_all(tenant, "SHOW INDEX INFO") == []
    assert "tenant_replace" not in granted_databases(admin, "replace_role")


def test_create_or_replace_database_nonexistent_creates(admin):
    """A missing name is not an error for the drop half: NON_EXISTENT is swallowed and the create
    half makes the database."""
    assert "tenant_replace_new" not in database_states(admin)

    execute_and_fetch_all(admin, "CREATE OR REPLACE DATABASE tenant_replace_new")

    assert database_states(admin).get("tenant_replace_new") == "HOT"
    tenant = connect().cursor()
    execute_and_fetch_all(tenant, "USE DATABASE tenant_replace_new")
    assert execute_and_fetch_all(tenant, "MATCH (n) RETURN count(n)") == [(0,)]


def test_create_or_replace_database_fatal_guards(admin):
    """The drop half keeps plain non-force DROP's refusals of the default database and of an in-use
    database, and a refused replace mutates nothing."""
    execute_and_fetch_all(admin, "CREATE (:Keep {id: 1})")
    with pytest.raises(mgclient.DatabaseError, match="Cannot delete the default database."):
        execute_and_fetch_all(admin, "CREATE OR REPLACE DATABASE memgraph")
    assert execute_and_fetch_all(admin, "MATCH (n:Keep) RETURN count(n)") == [(1,)]
    assert "memgraph" in database_states(admin)

    execute_and_fetch_all(admin, "CREATE DATABASE tenant_guard")
    execute_and_fetch_all(admin, "CREATE ROLE guard_role")
    execute_and_fetch_all(admin, "GRANT DATABASE tenant_guard TO guard_role")
    grants_before = granted_databases(admin, "guard_role")
    assert "tenant_guard" in grants_before

    # A session whose current database is tenant_guard holds a database accessor, so a non-force
    # drop sees the tenant as in use.
    holder = connect()
    holder_cursor = holder.cursor()
    execute_and_fetch_all(holder_cursor, "USE DATABASE tenant_guard")
    execute_and_fetch_all(holder_cursor, "CREATE (:Held {id: 1})")

    with pytest.raises(mgclient.DatabaseError, match="Cannot delete tenant_guard, it is currently being used."):
        execute_and_fetch_all(admin, "CREATE OR REPLACE DATABASE tenant_guard")
    assert granted_databases(admin, "guard_role") == grants_before
    assert execute_and_fetch_all(holder_cursor, "MATCH (n:Held) RETURN count(n)") == [(1,)]


def test_create_or_replace_database_requires_target_access():
    """Replacing an existing database also needs MULTI_DATABASE_EDIT on that database, as DROP does: a
    user with the privilege but no access to the target is refused until access is granted."""
    superadmin = connect().cursor()
    tenant = None
    scoped_connection = None
    try:
        execute_and_fetch_all(superadmin, "CREATE DATABASE tenant_scope")

        tenant = connect().cursor()
        execute_and_fetch_all(tenant, "USE DATABASE tenant_scope")
        execute_and_fetch_all(tenant, "CREATE (:Guard {id: 1})")
        execute_and_fetch_all(tenant, "USE DATABASE memgraph")

        execute_and_fetch_all(superadmin, "CREATE USER scoped IDENTIFIED BY 'scoped'")
        execute_and_fetch_all(superadmin, "GRANT MULTI_DATABASE_EDIT TO scoped")
        execute_and_fetch_all(superadmin, "GRANT DATABASE memgraph TO scoped")
        scoped_connection = connect(username="scoped", password="scoped")
        scoped = scoped_connection.cursor()

        with pytest.raises(mgclient.DatabaseError) as exc:
            execute_and_fetch_all(scoped, "CREATE OR REPLACE DATABASE tenant_scope")
        assert "not authorized" in str(exc.value).lower(), exc.value

        execute_and_fetch_all(tenant, "USE DATABASE tenant_scope")
        assert execute_and_fetch_all(tenant, "MATCH (n:Guard) RETURN count(n)") == [(1,)]
        execute_and_fetch_all(tenant, "USE DATABASE memgraph")

        # Reconnect after the grant so a session-cached privilege snapshot sees it too.
        scoped_connection.close()
        scoped_connection = None
        execute_and_fetch_all(superadmin, "GRANT DATABASE tenant_scope TO scoped")
        scoped_connection = connect(username="scoped", password="scoped")
        scoped = scoped_connection.cursor()
        execute_and_fetch_all(scoped, "CREATE OR REPLACE DATABASE tenant_scope")

        assert database_states(superadmin).get("tenant_scope") == "HOT"
        execute_and_fetch_all(tenant, "USE DATABASE tenant_scope")
        assert execute_and_fetch_all(tenant, "MATCH (n) RETURN count(n)") == [(0,)]
    finally:
        if scoped_connection is not None:
            scoped_connection.close()
        if tenant is not None:
            tenant.close()
        try_execute(superadmin, "USE DATABASE memgraph")
        try_execute(superadmin, "DROP USER scoped")
        try_execute(superadmin, "DROP DATABASE tenant_scope")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
