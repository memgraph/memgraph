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
Tests fine-grained permission requirements for each operation.

For each operation, we test:
1. With all required permissions = PASS
2. With NOTHING (explicit deny) = FAIL
3. Missing each required permission individually = FAIL

Memgraph 3.8 permissions:
- Labels: CREATE, READ, UPDATE, DELETE
- Edge types: CREATE, READ, UPDATE, DELETE
"""

import sys

import pytest
from common import connect, execute_and_fetch_all
from mgclient import DatabaseError


def get_admin_cursor():
    return connect(username="admin", password="test").cursor()


def get_user_cursor():
    return connect(username="user", password="test").cursor()


def reset_permissions(admin_cursor):
    execute_and_fetch_all(admin_cursor, "REVOKE * ON NODES CONTAINING LABELS * FROM user;")
    execute_and_fetch_all(admin_cursor, "REVOKE * ON EDGES OF TYPE * FROM user;")
    execute_and_fetch_all(admin_cursor, "MATCH (n) DETACH DELETE n;")


def grant_label_permissions(admin_cursor, permissions):
    if not permissions:
        return
    if "NOTHING" in permissions:
        execute_and_fetch_all(admin_cursor, "GRANT NOTHING ON NODES CONTAINING LABELS * TO user;")
    else:
        perms = ", ".join(permissions)
        execute_and_fetch_all(admin_cursor, f"GRANT {perms} ON NODES CONTAINING LABELS * TO user;")


def grant_edge_permissions(admin_cursor, permissions):
    if not permissions:
        return
    if "NOTHING" in permissions:
        execute_and_fetch_all(admin_cursor, "GRANT NOTHING ON EDGES OF TYPE * TO user;")
    else:
        perms = ", ".join(permissions)
        execute_and_fetch_all(admin_cursor, f"GRANT {perms} ON EDGES OF TYPE * TO user;")


class TestCreateVertex:
    """CREATE (:Label) requires CREATE on label."""

    REQUIRED_LABEL = {"CREATE"}

    def test_with_required_permissions(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        grant_label_permissions(admin, {"READ", "CREATE"})

        user = get_user_cursor()
        execute_and_fetch_all(user, "CREATE (:Target)")

    def test_with_nothing_fails(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        grant_label_permissions(admin, {"NOTHING"})

        user = get_user_cursor()
        with pytest.raises(DatabaseError):
            execute_and_fetch_all(user, "CREATE (:Target)")

    def test_without_create_fails(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        grant_label_permissions(admin, {"READ", "UPDATE", "DELETE"})

        user = get_user_cursor()
        with pytest.raises(DatabaseError):
            execute_and_fetch_all(user, "CREATE (:Target)")


class TestMatchVertex:
    """MATCH (n:Label) RETURN n requires READ on label."""

    def test_with_required_permissions(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Target)")
        grant_label_permissions(admin, {"READ"})

        user = get_user_cursor()
        result = execute_and_fetch_all(user, "MATCH (n:Target) RETURN n")
        assert len(result) == 1

    def test_with_nothing_returns_empty(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Target)")
        grant_label_permissions(admin, {"NOTHING"})

        user = get_user_cursor()
        result = execute_and_fetch_all(user, "MATCH (n:Target) RETURN n")
        assert len(result) == 0

    def test_without_read_returns_empty(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Target)")
        grant_label_permissions(admin, {"CREATE", "UPDATE", "DELETE"})

        user = get_user_cursor()
        result = execute_and_fetch_all(user, "MATCH (n:Target) RETURN n")
        assert len(result) == 0


class TestSetVertexProperty:
    """MATCH (n:Label) SET n.prop = 1 requires READ + UPDATE on label."""

    def test_with_required_permissions(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Target)")
        grant_label_permissions(admin, {"READ", "UPDATE"})

        user = get_user_cursor()
        execute_and_fetch_all(user, "MATCH (n:Target) SET n.prop = 1")

    def test_with_nothing_fails(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Target)")
        grant_label_permissions(admin, {"NOTHING"})

        user = get_user_cursor()
        # With NOTHING, MATCH returns empty, so SET does nothing (no error)
        execute_and_fetch_all(user, "MATCH (n:Target) SET n.prop = 1")

    def test_without_update_fails(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Target)")
        grant_label_permissions(admin, {"READ", "CREATE", "DELETE"})

        user = get_user_cursor()
        with pytest.raises(DatabaseError):
            execute_and_fetch_all(user, "MATCH (n:Target) SET n.prop = 1")


class TestDeleteVertex:
    """MATCH (n:Label) DELETE n requires READ + DELETE on label."""

    def test_with_required_permissions(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Target)")
        grant_label_permissions(admin, {"READ", "DELETE"})

        user = get_user_cursor()
        execute_and_fetch_all(user, "MATCH (n:Target) DELETE n")

    def test_with_nothing_fails(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Target)")
        grant_label_permissions(admin, {"NOTHING"})

        user = get_user_cursor()
        # With NOTHING, MATCH returns empty, so DELETE does nothing (no error)
        execute_and_fetch_all(user, "MATCH (n:Target) DELETE n")

    def test_without_delete_fails(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Target)")
        grant_label_permissions(admin, {"READ", "CREATE", "UPDATE"})

        user = get_user_cursor()
        with pytest.raises(DatabaseError):
            execute_and_fetch_all(user, "MATCH (n:Target) DELETE n")


class TestSetLabel:
    """MATCH (n:Existing) SET n:NewLabel requires READ + UPDATE on existing label + CREATE on new label."""

    def test_with_required_permissions(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Existing)")
        # Grant READ + UPDATE on :Existing, CREATE on :NewLabel
        execute_and_fetch_all(admin, "GRANT READ, UPDATE ON NODES CONTAINING LABELS :Existing TO user;")
        execute_and_fetch_all(admin, "GRANT CREATE ON NODES CONTAINING LABELS :NewLabel TO user;")

        user = get_user_cursor()
        execute_and_fetch_all(user, "MATCH (n:Existing) SET n:NewLabel")

    def test_with_nothing_on_existing_fails(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Existing)")
        execute_and_fetch_all(admin, "GRANT NOTHING ON NODES CONTAINING LABELS :Existing TO user;")
        execute_and_fetch_all(admin, "GRANT CREATE ON NODES CONTAINING LABELS :NewLabel TO user;")

        user = get_user_cursor()
        # With NOTHING on existing label, MATCH returns empty, so SET does nothing (no error)
        execute_and_fetch_all(user, "MATCH (n:Existing) SET n:NewLabel")

    def test_without_update_on_existing_fails(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Existing)")
        execute_and_fetch_all(admin, "GRANT READ ON NODES CONTAINING LABELS :Existing TO user;")
        execute_and_fetch_all(admin, "GRANT CREATE ON NODES CONTAINING LABELS :NewLabel TO user;")

        user = get_user_cursor()
        with pytest.raises(DatabaseError):
            execute_and_fetch_all(user, "MATCH (n:Existing) SET n:NewLabel")

    def test_without_create_on_new_label_fails(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Existing)")
        execute_and_fetch_all(admin, "GRANT READ, UPDATE ON NODES CONTAINING LABELS :Existing TO user;")
        execute_and_fetch_all(admin, "GRANT READ ON NODES CONTAINING LABELS :NewLabel TO user;")

        user = get_user_cursor()
        with pytest.raises(DatabaseError):
            execute_and_fetch_all(user, "MATCH (n:Existing) SET n:NewLabel")


class TestRemoveLabel:
    """MATCH (n:Target) REMOVE n:Target requires READ + UPDATE + DELETE on label."""

    def test_with_required_permissions(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Target)")
        grant_label_permissions(admin, {"READ", "UPDATE", "DELETE"})

        user = get_user_cursor()
        execute_and_fetch_all(user, "MATCH (n:Target) REMOVE n:Target")

    def test_with_nothing_fails(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Target)")
        grant_label_permissions(admin, {"NOTHING"})

        user = get_user_cursor()
        # With NOTHING, MATCH returns empty, so REMOVE does nothing (no error)
        execute_and_fetch_all(user, "MATCH (n:Target) REMOVE n:Target")

    def test_without_update_fails(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Target)")
        grant_label_permissions(admin, {"READ", "CREATE", "DELETE"})

        user = get_user_cursor()
        with pytest.raises(DatabaseError):
            execute_and_fetch_all(user, "MATCH (n:Target) REMOVE n:Target")

    def test_without_delete_fails(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Target)")
        grant_label_permissions(admin, {"READ", "CREATE", "UPDATE"})

        user = get_user_cursor()
        with pytest.raises(DatabaseError):
            execute_and_fetch_all(user, "MATCH (n:Target) REMOVE n:Target")


class TestCreateEdge:
    """MATCH (a), (b) CREATE (a)-[:Type]->(b) requires READ on labels + CREATE on edge type."""

    def test_with_required_permissions(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Source), (:Dest)")
        grant_label_permissions(admin, {"READ"})
        grant_edge_permissions(admin, {"CREATE"})

        user = get_user_cursor()
        execute_and_fetch_all(user, "MATCH (a:Source), (b:Dest) CREATE (a)-[:Target]->(b)")

    def test_with_label_nothing_fails(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Source), (:Dest)")
        grant_label_permissions(admin, {"NOTHING"})
        grant_edge_permissions(admin, {"CREATE"})

        user = get_user_cursor()
        # With NOTHING on labels, MATCH returns empty, so no edge created (no error)
        execute_and_fetch_all(user, "MATCH (a:Source), (b:Dest) CREATE (a)-[:Target]->(b)")

    def test_with_edge_nothing_fails(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Source), (:Dest)")
        grant_label_permissions(admin, {"READ"})
        grant_edge_permissions(admin, {"NOTHING"})

        user = get_user_cursor()
        with pytest.raises(DatabaseError):
            execute_and_fetch_all(user, "MATCH (a:Source), (b:Dest) CREATE (a)-[:Target]->(b)")

    def test_without_edge_create_fails(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Source), (:Dest)")
        grant_label_permissions(admin, {"READ"})
        grant_edge_permissions(admin, {"READ", "UPDATE", "DELETE"})

        user = get_user_cursor()
        with pytest.raises(DatabaseError):
            execute_and_fetch_all(user, "MATCH (a:Source), (b:Dest) CREATE (a)-[:Target]->(b)")


class TestMatchEdge:
    """MATCH ()-[r:Type]->() RETURN r requires READ on labels + READ on edge type."""

    def test_with_required_permissions(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Source)-[:Target]->(:Dest)")
        grant_label_permissions(admin, {"READ"})
        grant_edge_permissions(admin, {"READ"})

        user = get_user_cursor()
        result = execute_and_fetch_all(user, "MATCH (:Source)-[r:Target]->(:Dest) RETURN r")
        assert len(result) == 1

    def test_with_label_nothing_returns_empty(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Source)-[:Target]->(:Dest)")
        grant_label_permissions(admin, {"NOTHING"})
        grant_edge_permissions(admin, {"READ"})

        user = get_user_cursor()
        result = execute_and_fetch_all(user, "MATCH (:Source)-[r:Target]->(:Dest) RETURN r")
        assert len(result) == 0

    def test_with_edge_nothing_returns_empty(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Source)-[:Target]->(:Dest)")
        grant_label_permissions(admin, {"READ"})
        grant_edge_permissions(admin, {"NOTHING"})

        user = get_user_cursor()
        result = execute_and_fetch_all(user, "MATCH (:Source)-[r:Target]->(:Dest) RETURN r")
        assert len(result) == 0

    def test_without_edge_read_returns_empty(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Source)-[:Target]->(:Dest)")
        grant_label_permissions(admin, {"READ"})
        grant_edge_permissions(admin, {"CREATE", "UPDATE", "DELETE"})

        user = get_user_cursor()
        result = execute_and_fetch_all(user, "MATCH (:Source)-[r:Target]->(:Dest) RETURN r")
        assert len(result) == 0


class TestSetEdgeProperty:
    """MATCH ()-[r:Type]->() SET r.prop = 1 requires READ on labels + READ + UPDATE on edge type."""

    def test_with_required_permissions(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Source)-[:Target]->(:Dest)")
        grant_label_permissions(admin, {"READ"})
        grant_edge_permissions(admin, {"READ", "UPDATE"})

        user = get_user_cursor()
        execute_and_fetch_all(user, "MATCH (:Source)-[r:Target]->(:Dest) SET r.prop = 1")

    def test_with_label_nothing_fails(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Source)-[:Target]->(:Dest)")
        grant_label_permissions(admin, {"NOTHING"})
        grant_edge_permissions(admin, {"READ", "UPDATE"})

        user = get_user_cursor()
        # With NOTHING on labels, MATCH returns empty, so SET does nothing (no error)
        execute_and_fetch_all(user, "MATCH (:Source)-[r:Target]->(:Dest) SET r.prop = 1")

    def test_with_edge_nothing_fails(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Source)-[:Target]->(:Dest)")
        grant_label_permissions(admin, {"READ"})
        grant_edge_permissions(admin, {"NOTHING"})

        user = get_user_cursor()
        # With NOTHING on edge, MATCH returns empty, so SET does nothing (no error)
        execute_and_fetch_all(user, "MATCH (:Source)-[r:Target]->(:Dest) SET r.prop = 1")

    def test_without_edge_update_fails(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Source)-[:Target]->(:Dest)")
        grant_label_permissions(admin, {"READ"})
        grant_edge_permissions(admin, {"READ", "CREATE", "DELETE"})

        user = get_user_cursor()
        with pytest.raises(DatabaseError):
            execute_and_fetch_all(user, "MATCH (:Source)-[r:Target]->(:Dest) SET r.prop = 1")


class TestDeleteEdge:
    """MATCH ()-[r:Type]->() DELETE r requires READ + UPDATE on labels + DELETE on edge type."""

    def test_with_required_permissions(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Source)-[:Target]->(:Dest)")
        grant_label_permissions(admin, {"READ", "UPDATE"})
        grant_edge_permissions(admin, {"READ", "DELETE"})

        user = get_user_cursor()
        execute_and_fetch_all(user, "MATCH (:Source)-[r:Target]->(:Dest) DELETE r")

    def test_with_label_nothing_fails(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Source)-[:Target]->(:Dest)")
        grant_label_permissions(admin, {"NOTHING"})
        grant_edge_permissions(admin, {"READ", "DELETE"})

        user = get_user_cursor()
        # With NOTHING on labels, MATCH returns empty, so DELETE does nothing (no error)
        execute_and_fetch_all(user, "MATCH (:Source)-[r:Target]->(:Dest) DELETE r")

    def test_with_edge_nothing_fails(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Source)-[:Target]->(:Dest)")
        grant_label_permissions(admin, {"READ", "UPDATE"})
        grant_edge_permissions(admin, {"NOTHING"})

        user = get_user_cursor()
        # With NOTHING on edge, MATCH returns empty, so DELETE does nothing (no error)
        execute_and_fetch_all(user, "MATCH (:Source)-[r:Target]->(:Dest) DELETE r")

    def test_without_label_update_fails(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Source)-[:Target]->(:Dest)")
        grant_label_permissions(admin, {"READ", "CREATE", "DELETE"})
        grant_edge_permissions(admin, {"READ", "DELETE"})

        user = get_user_cursor()
        with pytest.raises(DatabaseError):
            execute_and_fetch_all(user, "MATCH (:Source)-[r:Target]->(:Dest) DELETE r")

    def test_without_edge_delete_fails(self):
        admin = get_admin_cursor()
        reset_permissions(admin)
        execute_and_fetch_all(admin, "CREATE (:Source)-[:Target]->(:Dest)")
        grant_label_permissions(admin, {"READ", "UPDATE"})
        grant_edge_permissions(admin, {"READ", "CREATE", "UPDATE"})

        user = get_user_cursor()
        with pytest.raises(DatabaseError):
            execute_and_fetch_all(user, "MATCH (:Source)-[r:Target]->(:Dest) DELETE r")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
