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


@pytest.fixture(name="admin")
def admin_cursor():
    cursor = connect().cursor()
    yield cursor
    execute_and_fetch_all(cursor, "USE DATABASE memgraph")
    for row in execute_and_fetch_all(cursor, "SHOW DATABASES"):
        # A force-dropped tenant still draining behind a live accessor lingers as a DETACHED row but
        # is already gone and unaddressable by name, so a second DROP ... FORCE says "does not exist".
        # Skip it; HOT/COLD tenants of the same prefix are still dropped.
        if row[0].startswith("tenant_") and row[1] != "DETACHED":
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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
