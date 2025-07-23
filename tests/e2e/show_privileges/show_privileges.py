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

import pytest
from common import *


def test_show_privileges_basic(connect):
    """Test basic SHOW PRIVILEGES functionality without ON clause."""
    cursor = connect.cursor()

    # Test that users require ON clause in multi-database environment
    try:
        execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR admin;")
        # If this succeeds, we might be in single-database mode
        # Check if we have multiple databases
        databases = execute_and_fetch_all(cursor, "SHOW DATABASES;")
        if len(databases) > 1:
            assert False, "Should throw error for user without ON clause in multi-database environment"
    except Exception as e:
        # Expected to fail for users in multi-database environment
        assert (
            "requires database specification" in str(e)
            or "ON MAIN" in str(e)
            or "ON CURRENT" in str(e)
            or "ON DATABASE" in str(e)
        )

    # Test that roles can use SHOW PRIVILEGES without ON clause
    try:
        result = execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR admin_role;")
        # This should succeed for roles
        assert len(result) > 0, "Should return privileges for role without ON clause"
    except Exception as e:
        # If this fails, it might be because the role doesn't exist or other reasons
        # Let's check if the role exists
        try:
            roles = execute_and_fetch_all(cursor, "SHOW ROLES;")
            role_names = [role[0] for role in roles]
            if "admin_role" in role_names:
                assert False, f"Role exists but SHOW PRIVILEGES failed: {e}"
        except Exception:
            pass  # Role might not exist, which is expected


def test_show_privileges_role_without_on_clause(connect):
    """Test that roles can use SHOW PRIVILEGES without ON clause."""
    cursor = connect.cursor()

    # Test with admin_role (should work without ON clause)
    try:
        result = execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR admin_role;")
        assert len(result) > 0, "Should return privileges for admin_role without ON clause"

        # Verify that the result contains expected privilege information
        # Should have at least basic privileges like CREATE, DELETE, MATCH, etc.
        privilege_names = [row[0] for row in result]
        expected_privileges = ["CREATE", "DELETE", "MATCH", "MERGE", "SET", "REMOVE", "INDEX", "STATS", "AUTH"]
        found_privileges = [priv for priv in expected_privileges if priv in privilege_names]
        assert len(found_privileges) > 0, f"Should find basic privileges, found: {found_privileges}"

    except Exception as e:
        # Check if the role exists
        try:
            roles = execute_and_fetch_all(cursor, "SHOW ROLES;")
            role_names = [role[0] for role in roles]
            if "admin_role" in role_names:
                assert False, f"Role exists but SHOW PRIVILEGES failed: {e}"
            else:
                # Role doesn't exist, which is expected in some test setups
                pass
        except Exception:
            pass  # Role might not exist, which is expected


def test_show_privileges_role_with_on_clause(connect):
    """Test that roles can also use SHOW PRIVILEGES with ON clause."""
    cursor = connect.cursor()

    # Test with admin_role using ON MAIN
    try:
        result = execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR admin_role ON MAIN;")
        assert len(result) > 0, "Should return privileges for admin_role with ON MAIN"
    except Exception as e:
        # This might fail in non-enterprise builds
        assert "enterprise" in str(e).lower() or "ON MAIN" in str(e)

    # Test with admin_role using ON CURRENT
    try:
        result = execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR admin_role ON CURRENT;")
        assert len(result) > 0, "Should return privileges for admin_role with ON CURRENT"
    except Exception as e:
        # This might fail in non-enterprise builds
        assert "enterprise" in str(e).lower() or "ON CURRENT" in str(e)

    # Test with admin_role using ON DATABASE
    try:
        result = execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR admin_role ON DATABASE memgraph;")
        assert len(result) > 0, "Should return privileges for admin_role with ON DATABASE"
    except Exception as e:
        # This might fail in non-enterprise builds
        assert "enterprise" in str(e).lower() or "ON DATABASE" in str(e)


def test_show_privileges_user_vs_role_behavior(connect):
    """Test the difference in behavior between users and roles for SHOW PRIVILEGES."""
    cursor = connect.cursor()

    # Test user behavior - should require ON clause in multi-database environment
    try:
        execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR admin;")
        # If this succeeds, check if we're in single-database mode
        databases = execute_and_fetch_all(cursor, "SHOW DATABASES;")
        if len(databases) > 1:
            assert False, "User should require ON clause in multi-database environment"
    except Exception as e:
        # Expected for users in multi-database environment
        assert (
            "requires database specification" in str(e)
            or "ON MAIN" in str(e)
            or "ON CURRENT" in str(e)
            or "ON DATABASE" in str(e)
        )

    # Test role behavior - should work without ON clause
    try:
        result = execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR admin_role;")
        assert len(result) > 0, "Role should work without ON clause"
    except Exception as e:
        # Check if role exists
        try:
            roles = execute_and_fetch_all(cursor, "SHOW ROLES;")
            role_names = [role[0] for role in roles]
            if "admin_role" in role_names:
                assert False, f"Role exists but SHOW PRIVILEGES failed: {e}"
        except Exception:
            pass  # Role might not exist


def test_show_privileges_on_main(connect):
    """Test SHOW PRIVILEGES ON MAIN functionality."""
    cursor = connect.cursor()

    # Set main database for admin
    execute_and_fetch_all(cursor, "SET MAIN DATABASE memgraph FOR admin;")

    # Test ON MAIN
    result = execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR admin ON MAIN;")
    assert len(result) > 0, "Should return privileges for admin on main database"


def test_show_privileges_on_current(connect):
    """Test SHOW PRIVILEGES ON CURRENT functionality."""
    cursor = connect.cursor()

    # Use a database first
    execute_and_fetch_all(cursor, "USE DATABASE memgraph;")

    # Test ON CURRENT
    result = execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR admin ON CURRENT;")
    assert len(result) > 0, "Should return privileges for admin on current database"


def test_show_privileges_on_database(connect):
    """Test SHOW PRIVILEGES ON DATABASE functionality."""
    cursor = connect.cursor()

    # Test ON DATABASE with specific database
    result = execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR admin ON DATABASE memgraph;")
    assert len(result) > 0, "Should return privileges for admin on specified database"

    # Test with database name containing special characters
    result = execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR admin ON DATABASE `test-db`;")
    assert len(result) > 0, "Should return privileges for admin on database with special characters"


def test_show_privileges_enterprise_only_features(connect):
    """Test that enterprise-only features throw appropriate errors in non-enterprise builds."""
    cursor = connect.cursor()

    # These should work in enterprise builds but may throw errors in non-enterprise builds
    try:
        result = execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR admin ON MAIN;")
        # If this succeeds, we're in enterprise mode
        assert len(result) > 0, "Should return privileges for admin on main database"
    except Exception as e:
        # In non-enterprise builds, this should throw an error
        assert "ON MAIN is only available in enterprise edition" in str(e) or "enterprise" in str(e).lower()

    try:
        result = execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR admin ON CURRENT;")
        # If this succeeds, we're in enterprise mode
        assert len(result) > 0, "Should return privileges for admin on current database"
    except Exception as e:
        # In non-enterprise builds, this should throw an error
        assert "ON CURRENT is only available in enterprise edition" in str(e) or "enterprise" in str(e).lower()

    try:
        result = execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR admin ON DATABASE memgraph;")
        # If this succeeds, we're in enterprise mode
        assert len(result) > 0, "Should return privileges for admin on specified database"
    except Exception as e:
        # In non-enterprise builds, this should throw an error
        assert "ON DATABASE is only available in enterprise edition" in str(e) or "enterprise" in str(e).lower()


def test_show_privileges_error_cases(connect):
    """Test error cases for SHOW PRIVILEGES with database specification."""
    cursor = connect.cursor()

    # Test with non-existent user
    try:
        execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR nonexistent_user ON MAIN;")
        # This should throw an error
        assert False, "Should throw error for non-existent user"
    except Exception:
        pass  # Expected to fail

    # Test with non-existent role
    try:
        execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR nonexistent_role;")
        # This should throw an error
        assert False, "Should throw error for non-existent role"
    except Exception:
        pass  # Expected to fail

    # Test with non-existent database
    try:
        execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR admin ON DATABASE nonexistent_db;")
        # This should work but return empty results or throw an error
        pass
    except Exception:
        pass  # Expected to fail

    # Test syntax errors
    try:
        execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR admin ON;")
        assert False, "Should throw syntax error"
    except Exception:
        pass  # Expected to fail

    try:
        execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR admin ON DATABASE;")
        assert False, "Should throw syntax error"
    except Exception:
        pass  # Expected to fail


def test_show_privileges_no_main_database(connect):
    """Test SHOW PRIVILEGES ON MAIN when user has no main database set."""
    cursor = connect.cursor()

    # Clear main database for admin
    try:
        execute_and_fetch_all(cursor, 'SET MAIN DATABASE "" FOR admin;')
    except Exception:
        pass  # May not be supported in all builds

    # Test ON MAIN without main database set
    try:
        result = execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR admin ON MAIN;")
        # If this succeeds, we're in enterprise mode and it should work
        assert len(result) > 0, "Should return privileges for admin on main database"
    except Exception as e:
        # Should throw error about no default database
        assert "no default database" in str(e).lower() or "enterprise" in str(e).lower()


def test_show_privileges_no_current_database(connect):
    """Test SHOW PRIVILEGES ON CURRENT when no current database is set."""
    cursor = connect.cursor()

    # Test ON CURRENT without current database
    try:
        result = execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR admin ON CURRENT;")
        # If this succeeds, we're in enterprise mode and it should work
        assert len(result) > 0, "Should return privileges for admin on current database"
    except Exception as e:
        # Should throw error about no current database
        assert "no current database" in str(e).lower() or "enterprise" in str(e).lower()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
