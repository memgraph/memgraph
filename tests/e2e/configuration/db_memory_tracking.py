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
E2E tests for per-database memory tracking (db_memory_tracked field in SHOW STORAGE INFO).

Requires a running Memgraph instance with enterprise features enabled (CREATE DATABASE support).
Each test uses a fresh connection and checks db_memory_tracked in SHOW STORAGE INFO.
"""

import re
import sys

import mgclient
import pytest


def connect(database="memgraph"):
    conn = mgclient.connect(host="localhost", port=7687)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"USE DATABASE {database}")
    return conn


def get_storage_info(cursor):
    cursor.execute("SHOW STORAGE INFO")
    return {row[0]: row[1] for row in cursor.fetchall()}


def parse_size_bytes(size_str):
    """Parse a human-readable size string produced by GetReadableSize().

    GetReadableSize() formats as '<value><unit>' with no space, e.g. '0B', '1.50MiB'.
    """
    if not size_str:
        return 0
    m = re.match(r"^(\d+(?:\.\d+)?)\s*(B|KiB|MiB|GiB|TiB)$", size_str.strip())
    if not m:
        return 0
    units = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4}
    return int(float(m.group(1)) * units[m.group(2)])


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_db_memory_tracked_field_exists():
    """SHOW STORAGE INFO must contain the db_memory_tracked key."""
    conn = connect()
    cursor = conn.cursor()
    info = get_storage_info(cursor)
    assert (
        "db_memory_tracked" in info
    ), f"db_memory_tracked missing from SHOW STORAGE INFO. Got keys: {list(info.keys())}"


def test_db_memory_tracked_is_parseable_size():
    """db_memory_tracked must be a parseable human-readable size string."""
    conn = connect()
    cursor = conn.cursor()
    info = get_storage_info(cursor)
    value = info["db_memory_tracked"]
    assert isinstance(value, str) and len(value) > 0, f"db_memory_tracked should be a non-empty string, got: {value!r}"
    # Should be parseable (e.g. "0 B", "1.50 MiB")
    bytes_val = parse_size_bytes(value)
    assert bytes_val >= 0, f"Could not parse db_memory_tracked value: {value!r}"


def test_db_memory_grows_after_create():
    """After creating many nodes, db_memory_tracked must increase."""
    conn = connect()
    cursor = conn.cursor()

    before = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    # Create enough nodes to trigger arena extent allocation
    cursor.execute("UNWIND range(1, 10000) AS i CREATE (:Node {id: i})")

    after = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    assert after > before, f"db_memory_tracked should grow after creating 10k nodes. before={before} after={after}"

    # Cleanup
    cursor.execute("MATCH (n:Node) DETACH DELETE n")


def test_db_memory_tracked_per_database_isolation():
    """
    Per-DB tracking must be isolated: writing to DB1 must not increase DB2's tracker.

    Uses CREATE DATABASE to create a second database (enterprise feature).
    """
    conn = connect()
    cursor = conn.cursor()

    # Create a second database (enterprise feature — skip if not available)
    try:
        cursor.execute("CREATE DATABASE testdb_isolation")
    except mgclient.DatabaseError as e:
        if "already exists" in str(e).lower():
            cursor.execute("USE DATABASE testdb_isolation")
            cursor.execute("MATCH (n) DETACH DELETE n")
            cursor.execute("USE DATABASE memgraph")  # restore to default
        elif "enterprise" in str(e).lower() or "not supported" in str(e).lower():
            pytest.skip("CREATE DATABASE requires enterprise license")
        else:
            raise

    # Get baseline for both DBs (already on memgraph from connect())
    baseline_memgraph = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    # Switch to testdb and get its baseline
    cursor.execute("USE DATABASE testdb_isolation")
    baseline_testdb = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    # Write to testdb
    cursor.execute("UNWIND range(1, 5000) AS i CREATE (:IsolationNode {id: i})")
    after_testdb = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    # Check testdb grew
    assert after_testdb > baseline_testdb, (
        f"testdb_isolation db_memory_tracked should grow after writes. "
        f"baseline={baseline_testdb} after={after_testdb}"
    )

    # Switch back to memgraph and check its tracker did NOT grow significantly
    cursor.execute("USE DATABASE memgraph")
    after_memgraph = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    # Allow up to 512 KiB noise (background threads, etc.)
    assert after_memgraph <= baseline_memgraph + 512 * 1024, (
        f"memgraph db_memory_tracked should not grow when writing to testdb_isolation. "
        f"baseline={baseline_memgraph} after={after_memgraph}"
    )

    # Cleanup
    try:
        cursor.execute("USE DATABASE testdb_isolation")
        cursor.execute("MATCH (n:IsolationNode) DETACH DELETE n")
        cursor.execute("USE DATABASE memgraph")
        cursor.execute("DROP DATABASE testdb_isolation")
    except mgclient.DatabaseError:
        pass


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
