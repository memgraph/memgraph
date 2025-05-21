# Copyright 2023 Memgraph Ltd.
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

import pytest
from common import memgraph, provide_user
from gqlalchemy import Memgraph


def test_user_creation(memgraph):
    memgraph.execute("CREATE USER mrma;")
    with pytest.raises(Exception):
        memgraph.execute("CREATE USER mrma;")
    memgraph.execute("CREATE USER IF NOT EXISTS mrma;")


def test_role_creation(memgraph):
    memgraph.execute("CREATE ROLE mrma;")
    with pytest.raises(Exception):
        memgraph.execute("CREATE ROLE mrma;")
    memgraph.execute("CREATE ROLE IF NOT EXISTS mrma;")


def test_show_current_user_if_no_users(memgraph):
    results = list(memgraph.execute_and_fetch("SHOW CURRENT USER;"))
    assert len(results) == 1 and "user" in results[0] and results[0]["user"] == None


def test_show_current_user(provide_user):
    USERNAME = "anthony"
    memgraph_with_user = Memgraph(username=USERNAME, password="password")
    results = list(memgraph_with_user.execute_and_fetch("SHOW CURRENT USER;"))
    assert len(results) == 1 and "user" in results[0] and results[0]["user"] == USERNAME


def test_show_current_role_if_no_roles(memgraph):
    results = list(memgraph.execute_and_fetch("SHOW CURRENT ROLE;"))
    assert len(results) == 1 and "role" in results[0] and results[0]["role"] == None


def test_show_current_roles_if_no_roles(memgraph):
    results = list(memgraph.execute_and_fetch("SHOW CURRENT ROLES;"))
    assert len(results) == 1 and "role" in results[0] and results[0]["role"] == None


def test_show_current_role_with_user(provide_user):
    USERNAME = "anthony"
    memgraph_with_user = Memgraph(username=USERNAME, password="password")
    results = list(memgraph_with_user.execute_and_fetch("SHOW CURRENT ROLE;"))
    assert len(results) == 1 and "role" in results[0] and results[0]["role"] == None


def test_show_current_roles_with_user(provide_user):
    USERNAME = "anthony"
    memgraph_with_user = Memgraph(username=USERNAME, password="password")
    results = list(memgraph_with_user.execute_and_fetch("SHOW CURRENT ROLES;"))
    assert len(results) == 1 and "role" in results[0] and results[0]["role"] == None


def test_show_current_role_with_user_has_roles(memgraph):
    # Create a user and roles
    memgraph.execute("CREATE USER test_user;")
    memgraph.execute("CREATE ROLE admin_role;")
    memgraph.execute("CREATE ROLE user_role;")
    memgraph.execute("SET ROLE FOR test_user TO admin_role, user_role;")

    # Connect as the user and check current roles
    memgraph_with_user = Memgraph(username="test_user", password="")
    results = list(memgraph_with_user.execute_and_fetch("SHOW CURRENT ROLE;"))
    assert len(results) == 2  # Should return both roles
    role_names = [row["role"] for row in results]
    assert "admin_role" in role_names
    assert "user_role" in role_names

    # Clean up
    memgraph.execute("DROP USER test_user;")
    memgraph.execute("DROP ROLE admin_role;")
    memgraph.execute("DROP ROLE user_role;")


def test_show_current_roles_with_user_has_roles(memgraph):
    # Create a user and roles
    memgraph.execute("CREATE USER test_user;")
    memgraph.execute("CREATE ROLE admin_role;")
    memgraph.execute("CREATE ROLE user_role;")
    memgraph.execute("SET ROLE FOR test_user TO admin_role, user_role;")

    # Connect as the user and check current roles
    memgraph_with_user = Memgraph(username="test_user", password="")
    results = list(memgraph_with_user.execute_and_fetch("SHOW CURRENT ROLES;"))
    assert len(results) == 2  # Should return both roles
    role_names = [row["role"] for row in results]
    assert "admin_role" in role_names
    assert "user_role" in role_names

    # Clean up
    memgraph.execute("DROP USER test_user;")
    memgraph.execute("DROP ROLE admin_role;")
    memgraph.execute("DROP ROLE user_role;")


def test_show_current_role_single_role(memgraph):
    # Create a user and single role
    memgraph.execute("CREATE USER test_user;")
    memgraph.execute("CREATE ROLE single_role;")
    memgraph.execute("SET ROLE FOR test_user TO single_role;")

    # Connect as the user and check current role
    memgraph_with_user = Memgraph(username="test_user", password="")
    results = list(memgraph_with_user.execute_and_fetch("SHOW CURRENT ROLE;"))
    assert len(results) == 1
    assert results[0]["role"] == "single_role"

    # Clean up
    memgraph.execute("DROP USER test_user;")
    memgraph.execute("DROP ROLE single_role;")


def test_show_current_roles_single_role(memgraph):
    # Create a user and single role
    memgraph.execute("CREATE USER test_user;")
    memgraph.execute("CREATE ROLE single_role;")
    memgraph.execute("SET ROLE FOR test_user TO single_role;")

    # Connect as the user and check current roles
    memgraph_with_user = Memgraph(username="test_user", password="")
    results = list(memgraph_with_user.execute_and_fetch("SHOW CURRENT ROLES;"))
    assert len(results) == 1
    assert results[0]["role"] == "single_role"

    # Clean up
    memgraph.execute("DROP USER test_user;")
    memgraph.execute("DROP ROLE single_role;")


def test_show_current_roles_multi_tenant(memgraph):
    # NOTE used client is not capable of multi-tenant queries
    # Create a user and roles
    memgraph.execute("CREATE USER test_user;")
    memgraph.execute("CREATE ROLE admin_role;")
    memgraph.execute("CREATE ROLE user_role;")
    memgraph.execute("CREATE DATABASE db1;")
    memgraph.execute("CREATE DATABASE db2;")
    memgraph.execute("GRANT DATABASE * TO admin_role;")
    memgraph.execute("GRANT DATABASE * TO user_role;")
    memgraph.execute("SET ROLE FOR test_user TO admin_role, user_role ON db1;")
    memgraph.execute("SET ROLE FOR test_user TO user_role ON db2;")

    # Connect as the user and check current roles
    memgraph_with_user = Memgraph(username="test_user", password="")

    # memgraph
    memgraph_with_user.execute_and_fetch("USE DATABASE memgraph;")
    results = list(memgraph_with_user.execute_and_fetch("SHOW CURRENT ROLES;"))
    assert len(results) == 1 and "role" in results[0] and results[0]["role"] == None

    memgraph_with_user = Memgraph(username="test_user", password="")
    memgraph.execute("SET ROLE FOR test_user TO admin_role ON memgraph;")
    results = list(memgraph_with_user.execute_and_fetch("SHOW CURRENT ROLES;"))
    assert len(results) == 1 and "role" in results[0] and results[0]["role"] == "admin_role"

    memgraph_with_user = Memgraph(username="test_user", password="")
    memgraph.execute("SET ROLE FOR test_user TO user_role ON memgraph;")
    results = list(memgraph_with_user.execute_and_fetch("SHOW CURRENT ROLES;"))
    assert len(results) == 1 and "role" in results[0] and results[0]["role"] == "user_role"

    memgraph_with_user = Memgraph(username="test_user", password="")
    memgraph.execute("SET ROLE FOR test_user TO admin_role, user_role ON memgraph;")
    results = list(memgraph_with_user.execute_and_fetch("SHOW CURRENT ROLES;"))
    assert len(results) == 2 and "role" in results[0] and "role" in results[1]
    assert "admin_role" == results[0]["role"] or "admin_role" == results[1]["role"]
    assert "user_role" == results[0]["role"] or "user_role" == results[1]["role"]

    # # db1
    # memgraph_with_user.execute_and_fetch("USE DATABASE db1;", connection=connection)
    # results = list(memgraph_with_user.execute_and_fetch("SHOW CURRENT ROLES;", connection=connection))
    # assert len(results) == 1
    # role_names = [row["role"] for row in results]
    # assert len(role_names) == 2
    # assert "admin_role" in role_names
    # assert "user_role" in role_names

    # # db2
    # memgraph_with_user.execute_and_fetch("USE DATABASE db2;", connection=connection)
    # results = list(memgraph_with_user.execute_and_fetch("SHOW CURRENT ROLES;", connection=connection))
    # assert len(results) == 1
    # role_names = [row["role"] for row in results]
    # assert len(role_names) == 1
    # assert "user_role" in role_names

    # Clean up
    memgraph.execute("DROP USER test_user;")
    memgraph.execute("DROP ROLE admin_role;")
    memgraph.execute("DROP ROLE user_role;")
    memgraph.execute("DROP DATABASE db1;")
    memgraph.execute("DROP DATABASE db2;")


def test_add_user_w_sha256(memgraph):
    memgraph.execute(
        "CREATE USER sha256 IDENTIFIED BY 'sha256:5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8';"
    )
    memgraph_with_user = Memgraph(username="sha256", password="password")
    results = list(memgraph_with_user.execute_and_fetch("SHOW CURRENT USER;"))
    assert len(results) == 1 and "user" in results[0] and results[0]["user"] == "sha256"


def test_add_user_w_sha256_multiple(memgraph):
    memgraph.execute(
        "CREATE USER sha256_multiple IDENTIFIED BY 'sha256-multiple:c9b03b8e38797c175ad62939faa723ad506e272face534e2d4fe991f0b000cec';"
    )
    memgraph_with_user = Memgraph(username="sha256_multiple", password="pass")
    results = list(memgraph_with_user.execute_and_fetch("SHOW CURRENT USER;"))
    assert len(results) == 1 and "user" in results[0] and results[0]["user"] == "sha256_multiple"


def test_add_user_w_bcrypt(memgraph):
    memgraph.execute(
        "CREATE USER bcrypt IDENTIFIED BY 'bcrypt:$2a$12$ueWpo7FfYrBwoFwBhaCD1ucO4hbwKtOtr9MvxCELJaNq746xhvqYy';"
    )
    memgraph_with_user = Memgraph(username="bcrypt", password="word")
    results = list(memgraph_with_user.execute_and_fetch("SHOW CURRENT USER;"))
    assert len(results) == 1 and "user" in results[0] and results[0]["user"] == "bcrypt"


def test_set_role_syntax_variations(memgraph):
    """Test the new SET ROLE syntax with ROLE/ROLES keywords and database-specific clauses"""
    # Create test users and roles
    memgraph.execute("CREATE USER test_user;")
    memgraph.execute("CREATE ROLE role1;")
    memgraph.execute("CREATE ROLE role2;")
    memgraph.execute("CREATE ROLE role3;")

    # Test SET ROLE (singular)
    memgraph.execute("SET ROLE FOR test_user TO role1;")

    # Test SET ROLES (plural)
    memgraph.execute("SET ROLES FOR test_user TO role1, role2;")

    # Test SET ROLE with database-specific clause
    try:
        memgraph.execute("SET ROLE FOR test_user TO role1, role2 ON db1, db2;")
        assert False, "Expected exception"
    except Exception as e:
        pass

    memgraph.execute("CREATE DATABASE db1;")
    memgraph.execute("CREATE DATABASE db2;")
    memgraph.execute("CREATE DATABASE db3;")

    # Test SET ROLES with database-specific clause
    try:
        memgraph.execute("SET ROLES FOR test_user TO role1, role2, role3 ON db1, db2, db3;")
        assert False, "Expected exception"
    except Exception as e:
        pass

    memgraph.execute("GRANT DATABASE db1 TO role1;")
    memgraph.execute("GRANT DATABASE db2 TO role1;")
    memgraph.execute("GRANT DATABASE db1 TO role2;")
    memgraph.execute("GRANT DATABASE db2 TO role2;")
    memgraph.execute("GRANT DATABASE db3 TO role3;")
    memgraph.execute("CLEAR ROLE FOR test_user;")
    memgraph.execute("SET ROLE FOR test_user TO role1, role2 ON db1, db2;")

    # Clean up
    memgraph.execute("DROP USER test_user;")
    memgraph.execute("DROP ROLE role1;")
    memgraph.execute("DROP ROLE role2;")
    memgraph.execute("DROP ROLE role3;")
    memgraph.execute("DROP DATABASE db1;")
    memgraph.execute("DROP DATABASE db2;")
    memgraph.execute("DROP DATABASE db3;")


def test_clear_role_syntax_variations(memgraph):
    """Test the new CLEAR ROLE syntax with ROLE/ROLES keywords and database-specific clauses"""
    # Create test users and roles
    memgraph.execute("CREATE USER test_user;")
    memgraph.execute("CREATE ROLE role1;")
    memgraph.execute("CREATE ROLE role2;")

    # Set roles first
    memgraph.execute("SET ROLE FOR test_user TO role1, role2;")

    # Test CLEAR ROLE (singular)
    memgraph.execute("CLEAR ROLE FOR test_user;")

    # Set roles again
    memgraph.execute("SET ROLE FOR test_user TO role1, role2;")

    # Test CLEAR ROLES (plural)
    memgraph.execute("CLEAR ROLES FOR test_user;")

    # Set roles again
    memgraph.execute("SET ROLE FOR test_user TO role1, role2;")

    # Test CLEAR ROLE with database-specific clause
    memgraph.execute("CLEAR ROLE FOR test_user ON db1, db2;")

    # Test CLEAR ROLES with database-specific clause
    memgraph.execute("CLEAR ROLES FOR test_user ON db1, db2;")

    # Clean up
    memgraph.execute("DROP USER test_user;")
    memgraph.execute("DROP ROLE role1;")
    memgraph.execute("DROP ROLE role2;")


def test_show_role_syntax_variations(memgraph):
    """Test the new SHOW ROLE syntax with ROLE/ROLES keywords and database-specific clauses"""
    # Create test users and roles
    memgraph.execute("CREATE USER test_user;")
    memgraph.execute("CREATE ROLE role1;")
    memgraph.execute("CREATE ROLE role2;")
    memgraph.execute("SET ROLE FOR test_user TO role1, role2;")

    # Test SHOW ROLE (singular) - should work without database specification
    results = list(memgraph.execute_and_fetch("SHOW ROLE FOR test_user;"))
    assert len(results) > 0
    # Should show both roles
    role_names = [row["role"] for row in results]
    assert "role1" in role_names
    assert "role2" in role_names

    # Test SHOW ROLES (plural) - should work without database specification
    results = list(memgraph.execute_and_fetch("SHOW ROLES FOR test_user;"))
    assert len(results) > 0
    # Should show both roles
    role_names = [row["role"] for row in results]
    assert "role1" in role_names
    assert "role2" in role_names

    memgraph.execute("CREATE DATABASE db1;")
    memgraph.execute("CREATE DATABASE db2;")
    memgraph.execute("GRANT DATABASE db1 TO role1;")
    memgraph.execute("GRANT DATABASE db2 TO role2;")

    # Test that SHOW ROLE still works without database specification even in multi-database environment
    results = list(memgraph.execute_and_fetch("SHOW ROLE FOR test_user;"))
    assert len(results) > 0
    # Should still show both roles
    role_names = [row["role"] for row in results]
    assert "role1" in role_names
    assert "role2" in role_names

    memgraph.execute("CLEAR ROLE FOR test_user;")
    memgraph.execute("SET ROLE FOR test_user TO role1 ON db1;")
    memgraph.execute("SET ROLE FOR test_user TO role2 ON db2;")

    # Test SHOW ROLE ON MAIN
    results = list(memgraph.execute_and_fetch("SHOW ROLE FOR test_user ON MAIN;"))
    assert results == [{"role": "null"}]

    # Test SHOW ROLES ON MAIN
    results = list(memgraph.execute_and_fetch("SHOW ROLES FOR test_user ON MAIN;"))
    assert results == [{"role": "null"}]

    # Test SHOW ROLE ON CURRENT
    results = list(memgraph.execute_and_fetch("SHOW ROLE FOR test_user ON CURRENT;"))
    assert results == [{"role": "null"}]

    memgraph.execute("USE DATABASE db1;")
    # Test SHOW ROLES ON CURRENT
    results = list(memgraph.execute_and_fetch("SHOW ROLES FOR test_user ON CURRENT;"))
    assert results == [{"role": "role1"}]

    # Test SHOW ROLE ON DATABASE
    results = list(memgraph.execute_and_fetch("SHOW ROLE FOR test_user ON DATABASE db1;"))
    assert results == [{"role": "role1"}]

    # Test SHOW ROLES ON DATABASE
    results = list(memgraph.execute_and_fetch("SHOW ROLES FOR test_user ON DATABASE db1;"))
    assert results == [{"role": "role1"}]

    # Test SHOW ROLES ON DATABASE
    results = list(memgraph.execute_and_fetch("SHOW ROLES FOR test_user ON DATABASE db2;"))
    assert results == [{"role": "role2"}]

    # Clean up
    memgraph.execute("USE DATABASE memgraph;")
    memgraph.execute("DROP USER test_user;")
    memgraph.execute("DROP ROLE role1;")
    memgraph.execute("DROP ROLE role2;")
    memgraph.execute("DROP DATABASE db1;")
    memgraph.execute("DROP DATABASE db2;")


def test_database_specific_role_management(memgraph):
    """Test database-specific role assignment and management"""
    # Create test databases, users, and roles
    memgraph.execute("CREATE DATABASE db1;")
    memgraph.execute("CREATE DATABASE db2;")
    memgraph.execute("CREATE DATABASE db3;")
    memgraph.execute("CREATE USER test_user;")
    memgraph.execute("CREATE ROLE role1;")
    memgraph.execute("CREATE ROLE role2;")
    memgraph.execute("CREATE ROLE role3;")

    # Test setting roles for specific databases
    try:
        memgraph.execute("SET ROLE FOR test_user TO role1 ON db1;")
        assert False, "Expected exception"
    except Exception as e:
        pass

    memgraph.execute("GRANT DATABASE db1 TO role1;")
    memgraph.execute("GRANT DATABASE db2 TO role2;")
    memgraph.execute("GRANT DATABASE db3 TO role3;")
    # Test clearing roles for specific databases
    memgraph.execute("SET ROLE FOR test_user TO role1 ON db1;")
    memgraph.execute("CLEAR ROLE FOR test_user ON db1;")
    memgraph.execute("CLEAR ROLES FOR test_user ON db2;")

    # Test showing roles for specific databases
    results = list(memgraph.execute_and_fetch("SHOW ROLE FOR test_user ON DATABASE db1;"))
    assert results == [{"role": "null"}]

    results = list(memgraph.execute_and_fetch("SHOW ROLES FOR test_user ON DATABASE db2;"))
    assert results == [{"role": "null"}]

    # Test that SHOW ROLE works without database specification even with database-specific roles
    # Set some database-specific roles
    memgraph.execute("SET ROLE FOR test_user TO role1 ON db1;")
    memgraph.execute("SET ROLE FOR test_user TO role2 ON db2;")

    # SHOW ROLE without database specification should still work
    results = list(memgraph.execute_and_fetch("SHOW ROLE FOR test_user;"))
    assert len(results) > 0, "SHOW ROLE should work without database specification"

    # SHOW ROLES without database specification should also work
    results = list(memgraph.execute_and_fetch("SHOW ROLES FOR test_user;"))
    assert len(results) > 0, "SHOW ROLES should work without database specification"

    # Clean up
    memgraph.execute("DROP USER test_user;")
    memgraph.execute("DROP ROLE role1;")
    memgraph.execute("DROP ROLE role2;")
    memgraph.execute("DROP ROLE role3;")
    memgraph.execute("DROP DATABASE db1;")
    memgraph.execute("DROP DATABASE db2;")
    memgraph.execute("DROP DATABASE db3;")


def test_role_syntax_compatibility(memgraph):
    """Test that both ROLE and ROLES keywords work interchangeably"""
    # Create test users and roles
    memgraph.execute("CREATE USER test_user;")
    memgraph.execute("CREATE ROLE role1;")
    memgraph.execute("CREATE ROLE role2;")

    # Test that SET ROLE and SET ROLES produce the same result
    memgraph.execute("SET ROLE FOR test_user TO role1, role2;")
    results1 = list(memgraph.execute_and_fetch("SHOW ROLE FOR test_user;"))

    memgraph.execute("CLEAR ROLE FOR test_user;")
    memgraph.execute("SET ROLES FOR test_user TO role1, role2;")
    results2 = list(memgraph.execute_and_fetch("SHOW ROLES FOR test_user;"))

    # Both should show the same roles
    assert len(results1) == len(results2)

    # Test that CLEAR ROLE and CLEAR ROLES work the same
    memgraph.execute("CLEAR ROLE FOR test_user;")
    results3 = list(memgraph.execute_and_fetch("SHOW ROLE FOR test_user;"))

    memgraph.execute("SET ROLE FOR test_user TO role1, role2;")
    memgraph.execute("CLEAR ROLES FOR test_user;")
    results4 = list(memgraph.execute_and_fetch("SHOW ROLES FOR test_user;"))

    # Both should clear the roles
    assert len(results3) == len(results4)

    # Clean up
    memgraph.execute("DROP USER test_user;")
    memgraph.execute("DROP ROLE role1;")
    memgraph.execute("DROP ROLE role2;")


def test_complex_role_privilege_flow(memgraph):
    """Test complex role and privilege flow with multiple users, roles, and databases."""
    # Create users
    memgraph.execute("CREATE USER alice;")
    memgraph.execute("CREATE USER bob;")
    memgraph.execute("CREATE USER charlie;")

    # Create roles
    memgraph.execute("CREATE ROLE user;")
    memgraph.execute("CREATE ROLE architect;")
    memgraph.execute("CREATE ROLE moderator;")
    memgraph.execute("CREATE ROLE admin;")
    memgraph.execute("CREATE ROLE support;")

    # Create databases
    memgraph.execute("CREATE DATABASE db1;")
    memgraph.execute("CREATE DATABASE db2;")
    memgraph.execute("CREATE DATABASE db3;")

    # Grant database access to roles
    memgraph.execute("GRANT DATABASE db1 TO architect;")
    memgraph.execute("GRANT DATABASE db1 TO moderator;")
    memgraph.execute("GRANT DATABASE db2 TO support;")
    memgraph.execute("GRANT DATABASE db2 TO admin;")

    # Set main databases for roles
    memgraph.execute("SET MAIN DATABASE db1 FOR architect;")
    memgraph.execute("SET MAIN DATABASE db2 FOR support;")

    # Set roles for charlie
    memgraph.execute("SET ROLE FOR charlie TO user;")
    memgraph.execute("SET ROLE FOR charlie TO architect, moderator ON db1;")
    memgraph.execute("SET ROLE FOR charlie TO admin, support ON db2;")

    # Check SHOW PRIVILEGES FOR charlie ON DATABASE db1 - should give empty result initially
    results = list(memgraph.execute_and_fetch("SHOW PRIVILEGES FOR charlie ON DATABASE db1;"))
    assert len(results) == 0, f"Expected empty privileges, got: {results}"

    # Grant match privilege to architect role
    memgraph.execute("GRANT match TO architect;")

    # Check SHOW PRIVILEGES FOR charlie ON DATABASE db1 - should show match granted to role
    results = list(memgraph.execute_and_fetch("SHOW PRIVILEGES FOR charlie ON DATABASE db1;"))
    assert len(results) == 1, "Expected privileges after granting match to architect role"

    # Check that match privilege is granted to architect role
    for row in results:
        # Check that the row contains privilege 'MATCH', effective 'GRANT', and description mentions 'GRANTED TO ROLE'
        assert (
            row.get("privilege", "").upper() == "MATCH"
            and row.get("effective", "").upper() == "GRANT"
            and "GRANTED TO ROLE" in row.get("description", "").upper()
        ), f"Expected privilege row for MATCH GRANT to role, got: {row}"

    # Deny match privilege to moderator role
    memgraph.execute("DENY match TO moderator;")

    # Check SHOW PRIVILEGES FOR charlie ON DATABASE db1 - should show deny denied to role
    results = list(memgraph.execute_and_fetch("SHOW PRIVILEGES FOR charlie ON DATABASE db1;"))
    assert len(results) == 1, "Expected privileges after denying match to moderator role"

    # Check that match privilege is denied to moderator role
    for row in results:
        assert (
            row.get("privilege", "").upper() == "MATCH"
            and row.get("effective", "").upper() == "DENY"
            and "DENIED TO ROLE" in row.get("description", "").upper()
        ), f"Expected privilege row for MATCH DENY to role, got: {row}"

    # Clean up
    memgraph.execute("DROP USER alice;")
    memgraph.execute("DROP USER bob;")
    memgraph.execute("DROP USER charlie;")
    memgraph.execute("DROP ROLE user;")
    memgraph.execute("DROP ROLE architect;")
    memgraph.execute("DROP ROLE moderator;")
    memgraph.execute("DROP ROLE admin;")
    memgraph.execute("DROP ROLE support;")
    memgraph.execute("DROP DATABASE db1;")
    memgraph.execute("DROP DATABASE db2;")
    memgraph.execute("DROP DATABASE db3;")


def test_show_databases_for_user_and_role(memgraph):
    default_db = "memgraph"
    """Test SHOW DATABASES FOR <user> and SHOW DATABASES FOR <role> with multiple roles and grants/denies."""
    # Setup: create databases, users, and roles
    memgraph.execute("CREATE DATABASE db1;")
    memgraph.execute("CREATE DATABASE db2;")
    memgraph.execute("CREATE DATABASE db3;")
    memgraph.execute("CREATE USER alice;")
    memgraph.execute("CREATE USER bob;")
    memgraph.execute("CREATE ROLE architect;")
    memgraph.execute("CREATE ROLE moderator;")
    memgraph.execute("CREATE ROLE admin;")

    # Grant/deny database access to roles
    memgraph.execute("GRANT DATABASE db1 TO architect;")
    memgraph.execute("GRANT DATABASE db2 TO architect;")
    memgraph.execute("GRANT DATABASE db2 TO moderator;")
    memgraph.execute("GRANT DATABASE db3 TO admin;")
    memgraph.execute("DENY DATABASE db2 FROM architect;")  # architect: db1 only

    # Assign roles to users
    memgraph.execute("SET ROLE FOR alice TO architect, moderator;")
    memgraph.execute("SET ROLE FOR bob TO admin, moderator;")

    # As admin, check SHOW DATABASES FOR <role>
    result = list(memgraph.execute_and_fetch("SHOW DATABASE PRIVILEGES FOR architect;"))
    # architect: granted db1, denied db2, not granted db3
    # The query returns columns "grants" and "denies", e.g. {"grants": "*", "denies": ["db1"]}
    # We want to check that architect can access all except db1 (i.e., all except denied)
    # For this test, since only db1 is denied, and grants is "*", the accessible dbs are all except db1.
    # But for the test setup, only db1, db2, db3 exist, so accessible = db2, db3
    for row in result:
        grants = row.get("grants", [])
        denies = row.get("denies", [])

        assert grants == ["db1", default_db], f"architect should see only db1, got {grants}"
        assert denies == ["db2"], f"architect should see only db2, got {denies}"

    result = list(memgraph.execute_and_fetch("SHOW DATABASE PRIVILEGES FOR moderator;"))
    for row in result:
        grants = row.get("grants", [])
        denies = row.get("denies", [])
        assert grants == ["db2", default_db], f"moderator should see only db2, got {grants}"
        assert denies == [], f"moderator should see no denies, got {denies}"

    # As admin, check SHOW DATABASES FOR <user>
    result = list(memgraph.execute_and_fetch("SHOW DATABASE PRIVILEGES FOR alice;"))
    for row in result:
        grants = row.get("grants", [])
        denies = row.get("denies", [])
        assert grants == "*", f"alice should see only db1, got {grants}"
        assert denies == ["db2"], f"alice should see no denies, got {denies}"

    result = list(memgraph.execute_and_fetch("SHOW DATABASE PRIVILEGES FOR bob;"))
    for row in result:
        grants = row.get("grants", [])
        denies = row.get("denies", [])
        assert grants == ["db2", "db3", default_db], f"bob should see only db2, db3, and memgraph, got {grants}"
        assert denies == [], f"bob should see no denies, got {denies}"

    # Clean up
    memgraph.execute("DROP USER alice;")
    memgraph.execute("DROP USER bob;")
    memgraph.execute("DROP ROLE architect;")
    memgraph.execute("DROP ROLE moderator;")
    memgraph.execute("DROP ROLE admin;")
    memgraph.execute("DROP DATABASE db1;")
    memgraph.execute("DROP DATABASE db2;")
    memgraph.execute("DROP DATABASE db3;")
def test_user_profiles(memgraph):
    try:
        list(memgraph.execute_and_fetch("CREATE PROFILE profile;"))
        list(memgraph.execute_and_fetch("CREATE PROFILE Profile LIMIT sessions 1;"))
        list(memgraph.execute_and_fetch("CREATE PROFILE profile2 LIMIT SessionS 2;"))
    except Exception as e:
        assert False, f"Failed to create profiles: {e}"

    with pytest.raises(Exception):
        memgraph.execute("CREATE PROFILE profile LIMIT sessions 1;")

    results = list(memgraph.execute_and_fetch("SHOW PROFILES;"))
    assert len(results) == 3
    assert {"profile": "profile"} in results
    assert {"profile": "Profile"} in results
    assert {"profile": "profile2"} in results

    results = list(memgraph.execute_and_fetch("SHOW PROFILE profile;"))
    assert len(results) == 2
    assert {"limit": "sessions", "value": "UNLIMITED"} in results
    assert {"limit": "transactions_memory", "value": "UNLIMITED"} in results

    results = list(memgraph.execute_and_fetch("SHOW PROFILE Profile;"))
    assert len(results) == 2
    assert {"limit": "sessions", "value": 1} in results
    assert {"limit": "transactions_memory", "value": "UNLIMITED"} in results

    results = list(memgraph.execute_and_fetch("SHOW PROFILE profile2;"))
    assert len(results) == 2
    assert {"limit": "sessions", "value": 2} in results
    assert {"limit": "transactions_memory", "value": "UNLIMITED"} in results

    with pytest.raises(Exception):
        memgraph.execute("SHOW PROFILE non_profile;")

    try:
        list(memgraph.execute_and_fetch("UPDATE PROFILE profile LIMIT Transactions_MEMORY 100MB;"))
    except Exception as e:
        assert False, f"Failed to create profiles: {e}"
    results = list(memgraph.execute_and_fetch("SHOW PROFILE profile;"))
    assert len(results) == 2
    assert {"limit": "sessions", "value": "UNLIMITED"} in results
    assert {"limit": "transactions_memory", "value": "100MB"} in results

    with pytest.raises(Exception):
        memgraph.execute("UPDATE PROFILE non_profile LIMIT Transactions_MEMORY 100MB;")
    with pytest.raises(Exception):
        memgraph.execute("UPDATE PROFILE profile LIMIT NON_LIMIT 100MB;")

    try:
        list(memgraph.execute_and_fetch("DROP PROFILE profile;"))
    except Exception as e:
        assert False, f"Failed to create profiles: {e}"

    results = list(memgraph.execute_and_fetch("SHOW PROFILES;"))
    assert len(results) == 2
    assert {"profile": "profile"} not in results
    assert {"profile": "Profile"} in results
    assert {"profile": "profile2"} in results

    with pytest.raises(Exception):
        memgraph.execute("DROP PROFILE non_profile;")

    memgraph.execute("CREATE USER mrma;")
    memgraph.execute("CREATE USER bcrypt;")

    try:
        list(memgraph.execute_and_fetch("CREATE PROFILE profile;"))
        list(memgraph.execute_and_fetch("SET PROFILE FOR mrma TO profile;"))
        list(memgraph.execute_and_fetch("SET PROFILE FOR bcrypt TO Profile;"))
    except Exception as e:
        assert False, f"Failed to create profiles: {e}"

    results = list(memgraph.execute_and_fetch("SHOW PROFILE FOR mrma;"))
    assert len(results) == 1
    assert {"profile": "profile"} in results
    results = list(memgraph.execute_and_fetch("SHOW PROFILE FOR bcrypt;"))
    assert len(results) == 1
    assert {"profile": "Profile"} in results

    results = list(memgraph.execute_and_fetch("SHOW USERS FOR PROFILE profile;"))
    assert len(results) == 1
    assert {"user": "mrma"} in results
    results = list(memgraph.execute_and_fetch("SHOW USERS FOR PROFILE Profile;"))
    assert len(results) == 1
    assert {"user": "bcrypt"} in results
    results = list(memgraph.execute_and_fetch("SHOW USERS FOR PROFILE profile2;"))
    assert len(results) == 0

    try:
        list(memgraph.execute_and_fetch("SET PROFILE FOR mrma TO Profile;"))
    except Exception as e:
        assert False, f"Failed to create profiles: {e}"

    results = list(memgraph.execute_and_fetch("SHOW PROFILE FOR mrma;"))
    assert len(results) == 1
    assert {"profile": "Profile"} in results

    results = list(memgraph.execute_and_fetch("SHOW USERS FOR PROFILE profile;"))
    assert len(results) == 0
    results = list(memgraph.execute_and_fetch("SHOW USERS FOR PROFILE Profile;"))
    assert len(results) == 2
    assert {"user": "mrma"} in results
    assert {"user": "bcrypt"} in results
    results = list(memgraph.execute_and_fetch("SHOW USERS FOR PROFILE profile2;"))
    assert len(results) == 0

    try:
        list(memgraph.execute_and_fetch("CLEAR PROFILE FOR mrma;"))
    except Exception as e:
        assert False, f"Failed to create profiles: {e}"

    results = list(memgraph.execute_and_fetch("SHOW USERS FOR PROFILE profile;"))
    assert len(results) == 0
    results = list(memgraph.execute_and_fetch("SHOW USERS FOR PROFILE Profile;"))
    assert len(results) == 1
    assert {"user": "mrma"} not in results
    assert {"user": "bcrypt"} in results
    results = list(memgraph.execute_and_fetch("SHOW USERS FOR PROFILE profile2;"))
    assert len(results) == 0

    try:
        list(memgraph.execute_and_fetch("SET PROFILE FOR mrma TO profile;"))
        results = list(memgraph.execute_and_fetch("SHOW PROFILE FOR mrma;"))
        assert len(results) == 1
        assert {"profile": "profile"} in results
        list(memgraph.execute_and_fetch("DROP PROFILE profile;"))
        results = list(memgraph.execute_and_fetch("SHOW PROFILE FOR mrma;"))
        assert len(results) == 1
        assert {"profile": "null"} in results
    except Exception as e:
        assert False, f"Failed to create profiles: {e}"

    memgraph.execute("DROP USER bcrypt;")
    results = list(memgraph.execute_and_fetch("SHOW USERS FOR PROFILE Profile;"))
    assert len(results) == 0
    results = list(memgraph.execute_and_fetch("SHOW USERS FOR PROFILE profile2;"))
    assert len(results) == 0

    with pytest.raises(Exception):
        memgraph.execute("SET PROFILE FOR non_user TO profile;")
    with pytest.raises(Exception):
        memgraph.execute("SET PROFILE FOR user TO non_profile;")


def test_show_role_no_database_specification_required(memgraph):
    """Test that SHOW ROLE FOR user no longer forces database specification."""
    # Create test user and roles
    memgraph.execute("CREATE USER test_user;")
    memgraph.execute("CREATE ROLE global_role;")
    memgraph.execute("CREATE ROLE db_specific_role;")

    # Set a global role
    memgraph.execute("SET ROLE FOR test_user TO global_role;")

    # Test that SHOW ROLE works without database specification
    results = list(memgraph.execute_and_fetch("SHOW ROLE FOR test_user;"))
    assert len(results) == 1
    assert results[0]["role"] == "global_role"

    # Test that SHOW ROLES works without database specification
    results = list(memgraph.execute_and_fetch("SHOW ROLES FOR test_user;"))
    assert len(results) == 1
    assert results[0]["role"] == "global_role"

    # Create a database and set database-specific role
    memgraph.execute("CREATE DATABASE test_db;")
    memgraph.execute("GRANT DATABASE test_db TO db_specific_role;")
    memgraph.execute("SET ROLE FOR test_user TO db_specific_role ON test_db;")

    # Test that SHOW ROLE still works without database specification in multi-database environment
    results = list(memgraph.execute_and_fetch("SHOW ROLE FOR test_user;"))
    assert len(results) > 0, "SHOW ROLE should work without database specification even in multi-database environment"

    # Test that SHOW ROLES still works without database specification in multi-database environment
    results = list(memgraph.execute_and_fetch("SHOW ROLES FOR test_user;"))
    assert len(results) > 0, "SHOW ROLES should work without database specification even in multi-database environment"

    # Test with multiple global roles
    memgraph.execute("CREATE ROLE second_global_role;")
    memgraph.execute("SET ROLE FOR test_user TO global_role, second_global_role;")

    results = list(memgraph.execute_and_fetch("SHOW ROLE FOR test_user;"))
    assert len(results) == 2
    role_names = [row["role"] for row in results]
    assert "global_role" in role_names
    assert "second_global_role" in role_names

    # Clean up
    memgraph.execute("DROP USER test_user;")
    memgraph.execute("DROP ROLE global_role;")
    memgraph.execute("DROP ROLE second_global_role;")
    memgraph.execute("DROP ROLE db_specific_role;")
    memgraph.execute("DROP DATABASE test_db;")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
