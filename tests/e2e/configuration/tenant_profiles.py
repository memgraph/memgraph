#!/usr/bin/env python3
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
E2E tests for tenant profile commands.

These tests run against the default 'memgraph' database which is always
available. Multi-tenancy (CREATE DATABASE) requires an enterprise license,
so DB-isolation tests are separate.
"""

import os
import sys

import mgclient
import pytest

BOLT_PORT = int(os.environ.get("BOLT_PORT", "7687"))


def connect():
    conn = mgclient.connect(host="localhost", port=BOLT_PORT)
    conn.autocommit = True
    return conn


def execute(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()


def cleanup_profiles(cursor):
    """Remove all tenant profiles created during tests."""
    try:
        rows = execute(cursor, "SHOW TENANT PROFILES")
        for row in rows:
            name = row[0]
            # Detach all databases first
            execute(cursor, f"REMOVE TENANT PROFILE FROM DATABASE memgraph")
            try:
                execute(cursor, f"DROP TENANT PROFILE {name}")
            except Exception:
                pass
    except Exception:
        pass


@pytest.fixture(autouse=True)
def cleanup():
    """Clean up tenant profiles after each test."""
    yield
    conn = connect()
    cursor = conn.cursor()
    cleanup_profiles(cursor)
    conn.close()


def test_create_and_show_tenant_profile():
    conn = connect()
    cursor = conn.cursor()

    execute(cursor, "CREATE TENANT PROFILE test_prof LIMIT memory_limit 256 MB")

    rows = execute(cursor, "SHOW TENANT PROFILES")
    assert len(rows) == 1
    assert rows[0][0] == "test_prof"
    assert "256" in rows[0][1]  # 256.00MiB

    rows = execute(cursor, "SHOW TENANT PROFILE test_prof")
    assert len(rows) == 1
    assert rows[0][0] == "test_prof"

    conn.close()


def test_alter_tenant_profile():
    conn = connect()
    cursor = conn.cursor()

    execute(cursor, "CREATE TENANT PROFILE alter_prof LIMIT memory_limit 100 MB")
    execute(cursor, "ALTER TENANT PROFILE alter_prof SET memory_limit 500 MB")

    rows = execute(cursor, "SHOW TENANT PROFILE alter_prof")
    assert "500" in rows[0][1]

    conn.close()


def test_drop_tenant_profile():
    conn = connect()
    cursor = conn.cursor()

    execute(cursor, "CREATE TENANT PROFILE drop_prof LIMIT memory_limit 100 MB")
    execute(cursor, "DROP TENANT PROFILE drop_prof")

    rows = execute(cursor, "SHOW TENANT PROFILES")
    names = [r[0] for r in rows]
    assert "drop_prof" not in names

    conn.close()


def test_drop_fails_with_attached_database():
    conn = connect()
    cursor = conn.cursor()

    execute(cursor, "CREATE TENANT PROFILE attached_prof LIMIT memory_limit 100 MB")
    execute(cursor, "SET TENANT PROFILE ON DATABASE memgraph TO attached_prof")

    with pytest.raises(mgclient.DatabaseError, match="attached"):
        execute(cursor, "DROP TENANT PROFILE attached_prof")

    # Detach and then drop should work.
    execute(cursor, "REMOVE TENANT PROFILE FROM DATABASE memgraph")
    execute(cursor, "DROP TENANT PROFILE attached_prof")

    conn.close()


def test_set_and_remove_tenant_profile_on_database():
    conn = connect()
    cursor = conn.cursor()

    execute(cursor, "CREATE TENANT PROFILE db_prof LIMIT memory_limit 200 MB")
    execute(cursor, "SET TENANT PROFILE ON DATABASE memgraph TO db_prof")

    # Verify the limit is visible in SHOW STORAGE INFO ON DATABASE.
    rows = execute(cursor, "SHOW STORAGE INFO ON DATABASE memgraph")
    info = {r[0]: r[1] for r in rows}
    assert "200" in info["tenant_memory_limit"]

    # Remove and verify limit is cleared.
    execute(cursor, "REMOVE TENANT PROFILE FROM DATABASE memgraph")
    rows = execute(cursor, "SHOW STORAGE INFO ON DATABASE memgraph")
    info = {r[0]: r[1] for r in rows}
    assert info["tenant_memory_limit"] == "unlimited"

    conn.close()


def test_create_duplicate_profile_fails():
    conn = connect()
    cursor = conn.cursor()

    execute(cursor, "CREATE TENANT PROFILE dup_prof LIMIT memory_limit 100 MB")
    with pytest.raises(mgclient.DatabaseError, match="already exists"):
        execute(cursor, "CREATE TENANT PROFILE dup_prof LIMIT memory_limit 200 MB")

    conn.close()


def test_alter_nonexistent_profile_fails():
    conn = connect()
    cursor = conn.cursor()

    with pytest.raises(mgclient.DatabaseError, match="not found"):
        execute(cursor, "ALTER TENANT PROFILE ghost_prof SET memory_limit 100 MB")

    conn.close()


def test_show_nonexistent_profile_fails():
    conn = connect()
    cursor = conn.cursor()

    with pytest.raises(mgclient.DatabaseError, match="not found"):
        execute(cursor, "SHOW TENANT PROFILE ghost_prof")

    conn.close()


def test_show_memory_info_default():
    """SHOW MEMORY INFO returns at least one row for the default database."""
    conn = connect()
    cursor = conn.cursor()

    rows = execute(cursor, "SHOW MEMORY INFO")
    assert len(rows) >= 1

    # Find the default database row
    names = [r[0] for r in rows]
    assert "memgraph" in names

    default_row = [r for r in rows if r[0] == "memgraph"][0]
    name, mem_tracked, profile, mem_limit = default_row
    assert name == "memgraph"
    assert "B" in mem_tracked  # e.g. "368.00KiB"
    assert profile is None  # no profile attached by default
    assert mem_limit == "unlimited"

    conn.close()


def test_show_memory_info_with_profile():
    """SHOW MEMORY INFO reflects the attached tenant profile."""
    conn = connect()
    cursor = conn.cursor()

    execute(cursor, "CREATE TENANT PROFILE mem_prof LIMIT memory_limit 512 MB")
    execute(cursor, "SET TENANT PROFILE ON DATABASE memgraph TO mem_prof")

    rows = execute(cursor, "SHOW MEMORY INFO")
    default_row = [r for r in rows if r[0] == "memgraph"][0]
    _, _, profile, mem_limit = default_row
    assert profile == "mem_prof"
    assert "512" in mem_limit

    # Clean up
    execute(cursor, "REMOVE TENANT PROFILE FROM DATABASE memgraph")

    conn.close()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
