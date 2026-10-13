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

"""An open Bolt session must not outlive, nor be revived by, a DROP USER once its principal is refreshed.

Ordinary queries use the cached session-long principal, so tests force a refresh via refresh_principal instead.
"""

import os
import sys

import pytest
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

URI = "bolt://localhost:7687"
ADMIN = ("sa", "sa")
NOT_AUTHORIZED = "You are not authorized to execute this query"

requires_enterprise = pytest.mark.skipif(
    not (os.environ.get("MEMGRAPH_ENTERPRISE_LICENSE") and os.environ.get("MEMGRAPH_ORGANIZATION_NAME")),
    reason="impersonation needs an enterprise license (MEMGRAPH_ENTERPRISE_LICENSE + MEMGRAPH_ORGANIZATION_NAME)",
)


def run(driver, query, **session_kwargs):
    with driver.session(**session_kwargs) as session:
        return session.run(query).values()


def pooled_driver(user, password):
    # One pooled connection: every session() reuses the same server-side Bolt session.
    return GraphDatabase.driver(URI, auth=(user, password), max_connection_pool_size=1)


@pytest.fixture
def opened():
    drivers = []
    yield drivers
    for d in drivers:
        d.close()


@pytest.fixture
def admin(opened):
    # The first user created turns authentication on, so the bootstrap connection is the only unauthenticated one.
    with GraphDatabase.driver(URI, auth=("", "")) as bootstrap:
        run(bootstrap, "CREATE USER sa IDENTIFIED BY 'sa'")
        run(bootstrap, "GRANT ALL PRIVILEGES TO sa")
    driver = GraphDatabase.driver(URI, auth=ADMIN)
    opened.append(driver)
    yield driver
    try:
        for (name,) in run(driver, "SHOW USERS"):
            if name != "sa":
                run(driver, f"DROP USER `{name}`")
        run(driver, "DROP USER sa")
    except Neo4jError:
        pass


def grant_label_permissions(admin, *users):
    # Fine-grained permissions are enterprise-only; where unavailable the coarse privilege is the only gate.
    for user in users:
        try:
            run(admin, f"GRANT CREATE ON NODES CONTAINING LABELS * TO {user}")
        except Neo4jError:
            pass


def refresh_principal(admin, driver):
    """SHOW TRANSACTIONS refreshes the caller's principal while the admin holds an open transaction; denial is fine."""
    with admin.session() as holder, holder.begin_transaction() as tx:
        tx.run("MATCH (n) RETURN count(n)").consume()
        try:
            run(driver, "SHOW TRANSACTIONS")
        except Neo4jError:
            pass


def test_recreated_user_does_not_revive_session(admin, opened):
    run(admin, "CREATE USER u IDENTIFIED BY 'pw'")
    run(admin, "GRANT MATCH TO u")
    grant_label_permissions(admin, "u")

    old = pooled_driver("u", "pw")
    opened.append(old)
    assert run(old, "MATCH (n) RETURN count(n)") == [[0]]
    with pytest.raises(Neo4jError, match=NOT_AUTHORIZED):
        run(old, "CREATE (:D1)")  # precondition: the old principal has no CREATE

    # Same name, different uuid, and no query from u in between.
    run(admin, "DROP USER u")
    run(admin, "CREATE USER u IDENTIFIED BY 'pw'")
    run(admin, "GRANT MATCH, CREATE TO u")
    grant_label_permissions(admin, "u")

    refresh_principal(admin, old)
    with pytest.raises(Neo4jError, match=NOT_AUTHORIZED):
        run(old, "CREATE (:D1)")  # the refreshed old session stays dropped instead of adopting the new grants

    fresh = pooled_driver("u", "pw")
    opened.append(fresh)
    run(fresh, "CREATE (:D1)")  # control: the new user really can CREATE, on a new connection


@requires_enterprise
def test_dropped_impersonator_cannot_switch_target(admin, opened):
    for target in ("c", "t"):
        run(admin, f"CREATE USER {target} IDENTIFIED BY 'pw'")
        run(admin, f"GRANT MATCH, CREATE TO {target}")
    run(admin, "CREATE USER r IDENTIFIED BY 'pw'")
    run(admin, "GRANT MATCH TO r")
    run(admin, "GRANT IMPERSONATE_USER c, t TO r")
    grant_label_permissions(admin, "c", "t", "r")

    session_driver = pooled_driver("r", "pw")
    opened.append(session_driver)
    assert run(session_driver, "SHOW CURRENT USER", impersonated_user="c") == [["c"]]

    run(admin, "DROP USER r")

    # A changed impersonation target re-runs the check, which must now refresh r and find it gone.
    with pytest.raises(Neo4jError, match="Failed to impersonate user 't'"):
        run(session_driver, "SHOW CURRENT USER", impersonated_user="t")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
