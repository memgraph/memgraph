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

"""An open pooled Bolt session must track admin changes on its next query.

DROP USER / REVOKE / role changes / fine-grained label changes reach the already-open session, a dropped user is
not revived by a same-named recreation, and a dropped or de-privileged impersonator is refused.
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
        if any(role == "rl" for role, *_ in run(driver, "SHOW ROLES")):  # the other roles are built in
            run(driver, "DROP ROLE rl")
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

    with pytest.raises(Neo4jError, match=NOT_AUTHORIZED):
        run(old, "CREATE (:D1)")  # the open old session stays dropped instead of adopting the new grants

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


def impersonation_setup(admin):
    run(admin, "CREATE USER c IDENTIFIED BY 'pw'")
    run(admin, "GRANT MATCH, CREATE TO c")
    run(admin, "CREATE USER r IDENTIFIED BY 'pw'")
    run(admin, "GRANT MATCH TO r")
    run(admin, "GRANT IMPERSONATE_USER c TO r")
    grant_label_permissions(admin, "c", "r")


@requires_enterprise
@pytest.mark.parametrize(
    "change",
    ["DROP USER r", "REVOKE IMPERSONATE_USER FROM r", "DENY IMPERSONATE_USER c TO r"],
    ids=["drop_user", "revoke_privilege", "deny_target"],
)
def test_impersonator_is_refused_for_the_same_target(admin, opened, change):
    impersonation_setup(admin)

    driver = pooled_driver("r", "pw")
    opened.append(driver)
    assert run(driver, "SHOW CURRENT USER", impersonated_user="c") == [["c"]]
    run(driver, "CREATE (:Imp)", impersonated_user="c")  # precondition: acting as c works

    run(admin, change)

    # Same target as before: the impersonation check must still be re-evaluated against current auth state.
    with pytest.raises(Neo4jError, match="Failed to impersonate user 'c'"):
        run(driver, "SHOW CURRENT USER", impersonated_user="c")


def test_revoked_privilege_reaches_open_session(admin, opened):
    run(admin, "CREATE USER u IDENTIFIED BY 'pw'")
    run(admin, "GRANT MATCH, CREATE TO u")
    grant_label_permissions(admin, "u")
    driver = pooled_driver("u", "pw")
    opened.append(driver)
    run(driver, "CREATE (:D3)")  # precondition

    run(admin, "REVOKE CREATE FROM u")

    with pytest.raises(Neo4jError, match=NOT_AUTHORIZED):
        run(driver, "CREATE (:D3)")
    run(driver, "MATCH (n) RETURN count(n)")  # reads are still allowed


def test_dropped_user_reaches_open_session(admin, opened):
    run(admin, "CREATE USER u IDENTIFIED BY 'pw'")
    run(admin, "GRANT MATCH TO u")
    grant_label_permissions(admin, "u")
    driver = pooled_driver("u", "pw")
    opened.append(driver)
    assert run(driver, "MATCH (n) RETURN count(n)") == [[0]]  # precondition

    run(admin, "DROP USER u")

    with pytest.raises(Neo4jError):
        run(driver, "MATCH (n) RETURN count(n)")


@pytest.mark.parametrize(
    "change",
    ["REVOKE CREATE FROM rl", "REVOKE ROLE rl FROM u", "CLEAR ROLE FOR u"],
    ids=["role_privilege_revoked", "role_removed_from_user", "role_cleared"],
)
def test_role_change_reaches_open_session(admin, opened, change):
    run(admin, "CREATE ROLE rl")
    run(admin, "GRANT CREATE TO rl")
    run(admin, "CREATE USER u IDENTIFIED BY 'pw'")
    run(admin, "GRANT MATCH TO u")
    run(admin, "SET ROLE FOR u TO rl")
    grant_label_permissions(admin, "u", "rl")
    driver = pooled_driver("u", "pw")
    opened.append(driver)
    run(driver, "CREATE (:D3)")  # precondition: CREATE comes from the role only

    run(admin, change)

    with pytest.raises(Neo4jError, match=NOT_AUTHORIZED):
        run(driver, "CREATE (:D3)")


def test_granted_privilege_reaches_open_session(admin, opened):
    run(admin, "CREATE USER u IDENTIFIED BY 'pw'")
    run(admin, "GRANT MATCH TO u")
    grant_label_permissions(admin, "u")
    driver = pooled_driver("u", "pw")
    opened.append(driver)
    with pytest.raises(Neo4jError, match=NOT_AUTHORIZED):
        run(driver, "CREATE (:D3)")  # precondition

    run(admin, "GRANT CREATE TO u")
    grant_label_permissions(admin, "u")

    run(driver, "CREATE (:D3)")


@requires_enterprise
def test_label_denial_reaches_open_session(admin, opened):
    run(admin, "CREATE USER u IDENTIFIED BY 'pw'")
    run(admin, "GRANT MATCH, CREATE TO u")
    run(admin, "GRANT CREATE ON NODES CONTAINING LABELS * TO u")
    driver = pooled_driver("u", "pw")
    opened.append(driver)
    run(driver, "CREATE (:Secret)")  # precondition

    run(admin, "DENY CREATE ON NODES CONTAINING LABELS :Secret TO u")

    with pytest.raises(Neo4jError):
        run(driver, "CREATE (:Secret)")
    run(driver, "CREATE (:Public)")  # other labels stay allowed


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
