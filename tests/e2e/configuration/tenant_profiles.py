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
E2E coverage for tenant profiles. Replaces the gql_behave feature file.

Verifies:
  * CRUD lifecycle and error paths.
  * Attaching a profile to a database propagates the configured limit to
    SHOW STORAGE INFO ON DATABASE; ALTER updates it; REMOVE clears it.
  * SHOW MEMORY INFO returns one row per database with the attached profile
    name and configured limit.
  * The configured limit is actually enforced — a query that would exceed it
    aborts with a memory error.
"""

import re
import sys

import mgclient
import pytest

BOLT_PORT = 7687
TEST_DB = "tenant_profiles_e2e_db"

# Must match --memory-limit in tests/e2e/configuration/workloads.yaml for the
# "Tenant profiles check" workload. With this set, an unattached DB's
# tenant_memory_limit falls back to this value rather than "unlimited".
GLOBAL_LIMIT_BYTES = 2048 * 1024 * 1024  # 2 GiB


def connect():
    conn = mgclient.connect(host="localhost", port=BOLT_PORT)
    conn.autocommit = True
    return conn


def execute(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()


def parse_size_bytes(size_str):
    if not size_str or size_str == "unlimited":
        return 0
    match = re.match(r"^(\d+(?:\.\d+)?)\s*(B|KiB|MiB|GiB|TiB)$", size_str.strip())
    assert match, f"Unrecognized size string: {size_str!r}"
    units = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4}
    return int(float(match.group(1)) * units[match.group(2)])


def storage_info_on(cursor, db_name):
    return {row[0]: row[1] for row in execute(cursor, f"SHOW STORAGE INFO ON DATABASE {db_name}")}


def _attached_dbs(databases_column):
    return {d.strip() for d in databases_column.split(",") if d.strip()}


@pytest.fixture(autouse=True)
def cleanup():
    """Drop all tenant profiles and any non-default DBs before and after each test."""

    def _purge():
        try:
            conn = connect()
            cur = conn.cursor()
            for name, _, dbs in execute(cur, "SHOW TENANT PROFILES"):
                for db in _attached_dbs(dbs):
                    cur.execute(f"REMOVE TENANT PROFILE FROM DATABASE {db}")
                cur.execute(f"DROP TENANT PROFILE {name}")
            for row in execute(cur, "SHOW DATABASES"):
                if row[0] != "memgraph":
                    cur.execute("USE DATABASE memgraph")
                    cur.execute(f"DROP DATABASE {row[0]}")
            conn.close()
        except Exception:
            # Best-effort: leftover state from a previous failure shouldn't mask the next test.
            pass

    _purge()
    yield
    _purge()


def test_crud_lifecycle():
    """CREATE → SHOW (all + one) → ALTER → DROP."""
    conn = connect()
    cur = conn.cursor()

    execute(cur, "CREATE TENANT PROFILE p LIMIT memory_limit 100 MB")

    rows = execute(cur, "SHOW TENANT PROFILES")
    assert len(rows) == 1
    name, limit, dbs = rows[0]
    assert name == "p"
    assert parse_size_bytes(limit) == 100 * 1024 * 1024
    assert dbs == ""

    rows = execute(cur, "SHOW TENANT PROFILE p")
    assert len(rows) == 1 and rows[0][0] == "p"

    execute(cur, "ALTER TENANT PROFILE p SET memory_limit 500 MB")
    rows = execute(cur, "SHOW TENANT PROFILE p")
    assert parse_size_bytes(rows[0][1]) == 500 * 1024 * 1024

    execute(cur, "DROP TENANT PROFILE p")
    assert execute(cur, "SHOW TENANT PROFILES") == []


def test_error_paths():
    conn = connect()
    cur = conn.cursor()

    execute(cur, "CREATE TENANT PROFILE p LIMIT memory_limit 100 MB")

    with pytest.raises(Exception, match="already exists"):
        execute(cur, "CREATE TENANT PROFILE p LIMIT memory_limit 100 MB")

    with pytest.raises(Exception, match="not found"):
        execute(cur, "ALTER TENANT PROFILE ghost SET memory_limit 100 MB")

    with pytest.raises(Exception, match="not found"):
        execute(cur, "DROP TENANT PROFILE ghost")

    with pytest.raises(Exception, match="not found"):
        execute(cur, "SHOW TENANT PROFILE ghost")

    with pytest.raises(Exception):
        execute(cur, "CREATE TENANT PROFILE bad LIMIT typo_limit 100 MB")

    execute(cur, "SET TENANT PROFILE ON DATABASE memgraph TO p")
    with pytest.raises(Exception, match="attached"):
        execute(cur, "DROP TENANT PROFILE p")


def test_attach_lifecycle_propagates_limit():
    """SET surfaces the limit in SHOW STORAGE INFO; ALTER updates it; REMOVE falls back
    to the global --memory-limit."""
    conn = connect()
    cur = conn.cursor()

    execute(cur, "CREATE TENANT PROFILE p LIMIT memory_limit 200 MB")
    execute(cur, "SET TENANT PROFILE ON DATABASE memgraph TO p")

    info = storage_info_on(cur, "memgraph")
    assert parse_size_bytes(info["tenant_memory_limit"]) == 200 * 1024 * 1024

    rows = execute(cur, "SHOW TENANT PROFILES")
    assert "memgraph" in _attached_dbs(rows[0][2])

    execute(cur, "ALTER TENANT PROFILE p SET memory_limit 500 MB")
    info = storage_info_on(cur, "memgraph")
    assert parse_size_bytes(info["tenant_memory_limit"]) == 500 * 1024 * 1024

    execute(cur, "REMOVE TENANT PROFILE FROM DATABASE memgraph")
    info = storage_info_on(cur, "memgraph")
    assert parse_size_bytes(info["tenant_memory_limit"]) == GLOBAL_LIMIT_BYTES

    rows = execute(cur, "SHOW TENANT PROFILES")
    assert "memgraph" not in _attached_dbs(rows[0][2])


def test_show_memory_info_per_database():
    """SHOW MEMORY INFO and SHOW STORAGE INFO ON DATABASE agree on per-DB profile state.
    REMOVE TENANT PROFILE falls the displayed limit back to --memory-limit."""
    conn = connect()
    cur = conn.cursor()

    execute(cur, f"CREATE DATABASE {TEST_DB}")
    execute(cur, "CREATE TENANT PROFILE gold LIMIT memory_limit 1024 MB")
    execute(cur, f"SET TENANT PROFILE ON DATABASE {TEST_DB} TO gold")

    by_name = {row[0]: row for row in execute(cur, "SHOW MEMORY INFO")}
    assert {"memgraph", TEST_DB}.issubset(by_name.keys())

    # Default DB has no profile attached → falls back to the global cap.
    assert by_name["memgraph"][2] is None
    assert parse_size_bytes(by_name["memgraph"][3]) == GLOBAL_LIMIT_BYTES

    # Test DB carries the gold profile @ 1 GiB.
    assert by_name[TEST_DB][2] == "gold"
    assert parse_size_bytes(by_name[TEST_DB][3]) == 1024**3

    # SHOW STORAGE INFO ON DATABASE must agree on the limit for both DBs.
    assert parse_size_bytes(storage_info_on(cur, "memgraph")["tenant_memory_limit"]) == GLOBAL_LIMIT_BYTES
    assert parse_size_bytes(storage_info_on(cur, TEST_DB)["tenant_memory_limit"]) == 1024**3

    # REMOVE the tenant profile → the test DB's displayed limit reverts to --memory-limit.
    execute(cur, f"REMOVE TENANT PROFILE FROM DATABASE {TEST_DB}")
    by_name = {row[0]: row for row in execute(cur, "SHOW MEMORY INFO")}
    assert by_name[TEST_DB][2] is None
    assert parse_size_bytes(by_name[TEST_DB][3]) == GLOBAL_LIMIT_BYTES
    assert parse_size_bytes(storage_info_on(cur, TEST_DB)["tenant_memory_limit"]) == GLOBAL_LIMIT_BYTES


def test_limit_is_enforced():
    """A query that exceeds the configured limit is rejected with a clean OOM error,
    and the server stays up afterwards."""
    conn = connect()
    cur = conn.cursor()

    execute(cur, "CREATE TENANT PROFILE tight LIMIT memory_limit 50 MB")
    execute(cur, "SET TENANT PROFILE ON DATABASE memgraph TO tight")

    with pytest.raises(Exception, match=r"[Mm]emory limit exceeded"):
        execute(cur, "UNWIND range(1, 1000000) AS i CREATE (:N {id: i, idx: i})")

    # Server must stay up — a fresh connection should work.
    survivor = connect()
    assert execute(survivor.cursor(), "RETURN 1 AS x") == [(1,)]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
