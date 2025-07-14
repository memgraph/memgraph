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

    # Test SHOW ROLE (singular)
    results = list(memgraph.execute_and_fetch("SHOW ROLE FOR test_user;"))
    assert len(results) > 0

    # Test SHOW ROLES (plural)
    results = list(memgraph.execute_and_fetch("SHOW ROLES FOR test_user;"))
    assert len(results) > 0

    memgraph.execute("CREATE DATABASE db1;")
    memgraph.execute("CREATE DATABASE db2;")
    memgraph.execute("GRANT DATABASE db1 TO role1;")
    memgraph.execute("GRANT DATABASE db2 TO role2;")
    try:
        memgraph.execute("SHOW ROLE FOR test_user;")
        assert False, "Expected exception"
    except Exception as e:
        pass

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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
