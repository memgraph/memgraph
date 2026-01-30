# Copyright 2025 Memgraph Ltd.
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
from contextlib import contextmanager

import pytest
from common import memgraph, provide_user
from gqlalchemy import Memgraph


@contextmanager
def _ensure_clean_state_and_create_admin(memgraph):
    """Helper function to ensure clean state and create admin user.

    Returns:
        A cleanup function that should be called to remove the admin user.
    """
    # Check that there are no existing users (except potentially the default admin)
    try:
        users = list(memgraph.execute_and_fetch("SHOW USERS;"))
        # Filter out the default admin user if it exists
        non_admin_users = [user for user in users if user.get("user") != "admin"]
        assert len(non_admin_users) == 0, f"Expected no non-admin users, found: {non_admin_users}"
    except Exception:
        assert False, "Failed to check users"

    # Check that there are no existing roles
    try:
        roles = list(memgraph.execute_and_fetch("SHOW ROLES;"))
        assert len(roles) == 0, f"Expected no roles, found: {roles}"
    except Exception:
        assert False, "Failed to check roles"

    # Create admin user (it should automatically get all privileges)
    try:
        memgraph.execute("CREATE USER admin;")
        admin_created = True
    except Exception:
        assert False, "Failed to create admin"

    try:
        yield
    finally:
        """Cleanup function to remove the admin user if it was created by this function."""
        if admin_created:
            try:
                memgraph.execute("DROP USER admin;")
            except Exception:
                pass  # Ignore errors during cleanup


def test_show_privileges_user_isolation_basic(memgraph):
    """Test that SHOW PRIVILEGES results are not influenced by the currently logged in user."""
    # Ensure clean state and create admin user
    with _ensure_clean_state_and_create_admin(memgraph):
        # Create two users with different privilege levels
        memgraph.execute("CREATE USER alice;")
        memgraph.execute("CREATE USER bob;")

        # Grant different privileges to each user
        memgraph.execute("GRANT CREATE, DELETE, AUTH TO alice;")
        memgraph.execute("GRANT MATCH, MERGE, AUTH TO bob;")

        # Connect as alice and check bob's privileges
        alice_conn = Memgraph(username="alice", password="")
        alice_bob_privileges = list(alice_conn.execute_and_fetch("SHOW PRIVILEGES FOR bob;"))

        # Connect as bob and check bob's privileges
        bob_conn = Memgraph(username="bob", password="")
        bob_bob_privileges = list(bob_conn.execute_and_fetch("SHOW PRIVILEGES FOR bob;"))

        # The results should be identical regardless of who is asking
        assert len(alice_bob_privileges) == len(bob_bob_privileges), "Privilege count should be the same"

        # Sort both results for comparison (privileges might be returned in different order)
        alice_sorted = sorted(alice_bob_privileges, key=lambda x: x.get("privilege", ""))
        bob_sorted = sorted(bob_bob_privileges, key=lambda x: x.get("privilege", ""))

        assert (
            alice_sorted == bob_sorted
        ), f"Privilege results should be identical. Alice view: {alice_sorted}, Bob view: {bob_sorted}"

        # Verify that bob's privileges are what we expect (MATCH, MERGE)
        privilege_names = [row.get("privilege", "") for row in bob_sorted]
        assert "MATCH" in privilege_names, "Bob should have MATCH privilege"
        assert "MERGE" in privilege_names, "Bob should have MERGE privilege"
        assert "CREATE" not in privilege_names, "Bob should not have CREATE privilege"
        assert "DELETE" not in privilege_names, "Bob should not have DELETE privilege"

        # Clean up
        memgraph.execute("DROP USER alice;")
        memgraph.execute("DROP USER bob;")


def test_show_privileges_user_isolation_with_roles(memgraph):
    """Test that SHOW PRIVILEGES results are not influenced by the currently logged in user when roles are involved."""
    # Ensure clean state and create admin user
    with _ensure_clean_state_and_create_admin(memgraph):
        # Create users and roles
        memgraph.execute("CREATE USER alice;")
        memgraph.execute("CREATE USER bob;")
        memgraph.execute("CREATE USER charlie;")
        memgraph.execute("CREATE ROLE admin_role;")
        memgraph.execute("CREATE ROLE user_role;")

        # Grant privileges to roles
        memgraph.execute("GRANT CREATE, DELETE, MATCH, AUTH TO admin_role;")
        memgraph.execute("GRANT MATCH, MERGE, AUTH TO user_role;")

        # Assign roles to users
        memgraph.execute("SET ROLE FOR alice TO admin_role;")
        memgraph.execute("SET ROLE FOR bob TO user_role;")
        memgraph.execute("SET ROLE FOR charlie TO admin_role, user_role;")

        # Connect as alice and check charlie's privileges
        alice_conn = Memgraph(username="alice", password="")
        alice_charlie_privileges = list(alice_conn.execute_and_fetch("SHOW PRIVILEGES FOR charlie;"))

        # Connect as bob and check charlie's privileges
        bob_conn = Memgraph(username="bob", password="")
        bob_charlie_privileges = list(bob_conn.execute_and_fetch("SHOW PRIVILEGES FOR charlie;"))

        # Connect as charlie and check his own privileges
        charlie_conn = Memgraph(username="charlie", password="")
        charlie_charlie_privileges = list(charlie_conn.execute_and_fetch("SHOW PRIVILEGES FOR charlie;"))

        # All three should return identical results
        assert (
            len(alice_charlie_privileges) == len(bob_charlie_privileges) == len(charlie_charlie_privileges)
        ), "Privilege count should be the same"

        # Sort all results for comparison
        alice_sorted = sorted(alice_charlie_privileges, key=lambda x: x.get("privilege", ""))
        bob_sorted = sorted(bob_charlie_privileges, key=lambda x: x.get("privilege", ""))
        charlie_sorted = sorted(charlie_charlie_privileges, key=lambda x: x.get("privilege", ""))

        assert alice_sorted == bob_sorted == charlie_sorted, "All privilege results should be identical"

        # Verify that charlie has privileges from both roles
        privilege_names = [row.get("privilege", "") for row in charlie_sorted]
        expected_privileges = ["CREATE", "DELETE", "MATCH", "MERGE"]
        for privilege in expected_privileges:
            assert privilege in privilege_names, f"Charlie should have {privilege} privilege from roles"

        # Clean up
        memgraph.execute("DROP USER alice;")
        memgraph.execute("DROP USER bob;")
        memgraph.execute("DROP USER charlie;")
        memgraph.execute("DROP ROLE admin_role;")
        memgraph.execute("DROP ROLE user_role;")


def test_show_privileges_user_isolation_with_deny_privileges(memgraph):
    """Test that SHOW PRIVILEGES results are not influenced by the currently logged in user when DENY privileges are involved."""
    # Ensure clean state and create admin user
    with _ensure_clean_state_and_create_admin(memgraph):
        # Create users
        memgraph.execute("CREATE USER alice;")
        memgraph.execute("CREATE USER bob;")

        # Grant and deny different privileges to each user
        memgraph.execute("GRANT CREATE, DELETE, AUTH TO alice;")
        memgraph.execute("DENY MATCH TO alice;")

        memgraph.execute("GRANT MATCH, MERGE, AUTH TO bob;")
        memgraph.execute("DENY CREATE TO bob;")

        # Connect as alice and check bob's privileges
        alice_conn = Memgraph(username="alice", password="")
        alice_bob_privileges = list(alice_conn.execute_and_fetch("SHOW PRIVILEGES FOR bob;"))

        # Connect as bob and check bob's privileges
        bob_conn = Memgraph(username="bob", password="")
        bob_bob_privileges = list(bob_conn.execute_and_fetch("SHOW PRIVILEGES FOR bob;"))

        # The results should be identical
        assert len(alice_bob_privileges) == len(bob_bob_privileges), "Privilege count should be the same"

        # Sort both results for comparison
        alice_sorted = sorted(alice_bob_privileges, key=lambda x: x.get("privilege", ""))
        bob_sorted = sorted(bob_bob_privileges, key=lambda x: x.get("privilege", ""))

        assert alice_sorted == bob_sorted, "Privilege results should be identical"

        # Verify bob's privileges (should have MATCH, MERGE granted and CREATE denied)
        privilege_data = {row.get("privilege", ""): row.get("effective", "") for row in bob_sorted}
        assert privilege_data.get("MATCH") == "GRANT", "Bob should have MATCH granted"
        assert privilege_data.get("MERGE") == "GRANT", "Bob should have MERGE granted"
        assert privilege_data.get("CREATE") == "DENY", "Bob should have CREATE denied"

        # Clean up
        memgraph.execute("DROP USER alice;")
        memgraph.execute("DROP USER bob;")


def test_show_privileges_user_isolation_multi_database(memgraph):
    """Test that SHOW PRIVILEGES results are not influenced by the currently logged in user in multi-database environment."""
    # Ensure clean state and create admin user
    with _ensure_clean_state_and_create_admin(memgraph):
        # Create databases
        memgraph.execute("CREATE DATABASE db1;")
        memgraph.execute("CREATE DATABASE db2;")

        # Create users
        memgraph.execute("CREATE USER alice;")
        memgraph.execute("CREATE USER bob;")

        # Grant database access
        memgraph.execute("GRANT DATABASE db1 TO alice;")
        memgraph.execute("GRANT DATABASE db2 TO bob;")
        # memgraph.execute("DENY DATABASE memgraph FROM bob;") Bob needs memgraph to query auth privileges (memgraph == system database)
        memgraph.execute("DENY DATABASE db1 FROM bob;")
        memgraph.execute("SET MAIN DATABASE db2 FOR bob;")

        # Grant different privileges on different databases
        memgraph.execute("GRANT CREATE, AUTH, MULTI_DATABASE_USE TO alice;")
        memgraph.execute("GRANT MATCH, AUTH TO bob;")

        # Connect as alice and check bob's privileges on db2
        alice_conn = Memgraph(username="alice", password="")
        alice_bob_privileges = list(alice_conn.execute_and_fetch("SHOW PRIVILEGES FOR bob ON DATABASE db2;"))
        assert list(alice_conn.execute_and_fetch("SHOW PRIVILEGES FOR bob ON DATABASE db1;")) == []
        list(alice_conn.execute_and_fetch("USE DATABASE db1;"))
        assert alice_bob_privileges == list(alice_conn.execute_and_fetch("SHOW PRIVILEGES FOR bob ON DATABASE db2;"))
        assert list(alice_conn.execute_and_fetch("SHOW PRIVILEGES FOR bob ON DATABASE db1;")) == []

        # Connect as bob and check his own privileges on db2
        bob_conn = Memgraph(username="bob", password="")
        print(list(bob_conn.execute_and_fetch("SHOW DATABASE;")))
        bob_bob_privileges = list(bob_conn.execute_and_fetch("SHOW PRIVILEGES FOR bob ON CURRENT;"))
        bob_bob_privileges = list(bob_conn.execute_and_fetch("SHOW PRIVILEGES FOR bob ON MAIN;"))
        bob_bob_privileges = list(bob_conn.execute_and_fetch("SHOW PRIVILEGES FOR bob ON DATABASE db2;"))
        assert list(bob_conn.execute_and_fetch("SHOW PRIVILEGES FOR bob ON DATABASE db1;")) == []

        # The results should be identical
        assert len(alice_bob_privileges) == len(bob_bob_privileges), "Privilege count should be the same"

        # Sort both results for comparison
        alice_sorted = sorted(alice_bob_privileges, key=lambda x: x.get("privilege", ""))
        bob_sorted = sorted(bob_bob_privileges, key=lambda x: x.get("privilege", ""))

        assert alice_sorted == bob_sorted, "Privilege results should be identical"

        # Verify bob has MATCH privilege on db2
        privilege_names = [row.get("privilege", "") for row in bob_sorted]
        assert "MATCH" in privilege_names, "Bob should have MATCH privilege on db2"

        # Clean up
        memgraph.execute("DROP USER alice;")
        memgraph.execute("DROP USER bob;")
        memgraph.execute("DROP DATABASE db1 FORCE;")
        memgraph.execute("DROP DATABASE db2 FORCE;")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
