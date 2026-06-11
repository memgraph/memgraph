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

"""
End-to-end tests for the hot/cold-tenants feature (commit 7c Part B).

These tests validate the suspend->resume lifecycle that unit tests cannot reach
because they exercise the full binary path: durability flags, the experimental
gate, the SUSPEND/RESUME Cypher commands, and the USE-DATABASE block-and-resume seam.

Enterprise license is provided via the environment variables
MEMGRAPH_ENTERPRISE_LICENSE and MEMGRAPH_ORGANIZATION_NAME, which the
MemgraphInstanceRunner subprocess inherits from the calling shell.

SHOW DATABASES with the experiment enabled emits four columns:
  Name, State, Connections, "Idle seconds"
State values are "ready" (HOT) and "cold" (COLD/suspended).
"""

from __future__ import annotations

import os
import sys
import time

import interactive_mg_runner
import mgclient
import pytest
from common import connect, execute_and_fetch_all, get_data_path, get_logs_path

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

# Logical file name used for path helpers (mirrors the "file" variable in durability tests).
file = "hot_cold_tenants"


# ---------------------------------------------------------------------------
# Common instance descriptions
# ---------------------------------------------------------------------------

# Args shared by every instance that must be suspend-eligible:
#   - WAL enabled + snapshot interval > 0  => PERIODIC_SNAPSHOT_WITH_WAL
#   - data-recovery-on-startup=true        => resume can replay durability files
#   - min-hot-residency=0                  => anti-thrash guard disabled for testing
#   - experimental-enabled=hot-cold-tenants => SUSPEND/RESUME commands available
_HOT_COLD_ARGS_BASE = [
    "--bolt-port",
    "7687",
    "--log-level",
    "TRACE",
    "--experimental-enabled=hot-cold-tenants",
    "--storage-wal-enabled=true",
    "--storage-snapshot-interval-sec=300",
    "--data-recovery-on-startup=true",
    "--storage-hot-cold-min-hot-residency-sec=0",
]

# Args for T4: the experiment is intentionally absent so SUSPEND/RESUME raise errors.
_NO_EXPERIMENT_ARGS_BASE = [
    "--bolt-port",
    "7687",
    "--log-level",
    "TRACE",
    "--storage-wal-enabled=true",
    "--storage-snapshot-interval-sec=300",
    "--data-recovery-on-startup=true",
]


def instance_description(test_name: str) -> dict:
    """Return the cluster description for a single experiment-enabled instance."""
    return {
        "instance_1": {
            "args": _HOT_COLD_ARGS_BASE,
            "log_file": f"{get_logs_path(file, test_name)}/instance_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_1",
            "setup_queries": [],
        },
    }


def instance_description_no_experiment(test_name: str) -> dict:
    """Return the cluster description for a single instance WITHOUT the experiment flag."""
    return {
        "instance_noexp": {
            "args": _NO_EXPERIMENT_ARGS_BASE,
            "log_file": f"{get_logs_path(file, test_name)}/instance_noexp.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_noexp",
            "setup_queries": [],
        },
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def cleanup_after_test():
    """Kill all Memgraph instances and clean up data directories after each test."""
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def _get_db_state(cursor: mgclient.Cursor, db_name: str) -> str | None:
    """
    Return the 'State' column for *db_name* from SHOW DATABASES.

    With the hot-cold experiment enabled, SHOW DATABASES emits four columns:
    Name, State, Connections, "Idle seconds".  Returns None if the database
    is not listed.
    """
    rows = execute_and_fetch_all(cursor, "SHOW DATABASES")
    for row in rows:
        if row[0] == db_name:
            # row[1] is the State column ("ready" or "cold")
            return str(row[1])
    return None


def _wait_for_db_state(
    cursor: mgclient.Cursor,
    db_name: str,
    expected_state: str,
    timeout_s: float = 10.0,
    poll_s: float = 0.2,
) -> bool:
    """Poll SHOW DATABASES until *db_name* reaches *expected_state* or timeout."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        state = _get_db_state(cursor, db_name)
        if state == expected_state:
            return True
        time.sleep(poll_s)
    return False


# ---------------------------------------------------------------------------
# T1 — manual suspend->resume round-trip with data integrity
# ---------------------------------------------------------------------------


def test_t1_suspend_resume_data_intact(test_name):
    """
    Suspend a tenant, verify SHOW DATABASES reports it as 'cold', then resume it
    and confirm the data survived the round-trip.

    Validates:
    - SUSPEND DATABASE succeeds and returns a status row.
    - SHOW DATABASES lists the tenant with State='cold' after suspension.
    - RESUME DATABASE succeeds and returns a status row.
    - Data written before suspension is fully accessible after resumption.
    """
    instances = instance_description(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)

    # Default connection (stays on the 'memgraph' system database).
    conn_default = connect(host="localhost", port=7687)
    cursor_default = conn_default.cursor()

    # Create tenant database from the default connection.
    execute_and_fetch_all(cursor_default, "CREATE DATABASE db1")

    # Work connection: switch to db1 and populate data.
    conn_work = connect(host="localhost", port=7687)
    cursor_work = conn_work.cursor()
    execute_and_fetch_all(cursor_work, "USE DATABASE db1")
    execute_and_fetch_all(cursor_work, "CREATE (:Node)")
    execute_and_fetch_all(cursor_work, "CREATE (:Node)")
    execute_and_fetch_all(cursor_work, "CREATE (:Node)")

    # Verify data before suspension.
    count_before = execute_and_fetch_all(cursor_work, "MATCH (n) RETURN count(n) AS c")
    assert count_before[0][0] == 3, f"Expected 3 nodes before suspend, got {count_before[0][0]}"

    # Connection-scoped: cursor_work holds db1's accessor for its whole session, so SUSPEND would
    # return ACTIVE_CONNECTIONS while it is open. Close it so db1 has zero connections.
    conn_work.close()

    # SUSPEND from the default connection (always on 'memgraph').
    suspend_result = execute_and_fetch_all(cursor_default, "SUSPEND DATABASE db1")
    assert len(suspend_result) == 1, f"SUSPEND DATABASE should return 1 row, got {suspend_result}"
    assert "suspended" in suspend_result[0][0].lower(), f"SUSPEND status message unexpected: {suspend_result[0][0]}"

    # SHOW DATABASES must list db1 as 'cold'.
    db_state = _get_db_state(cursor_default, "db1")
    assert db_state is not None, "db1 should still appear in SHOW DATABASES after suspension"
    assert db_state == "cold", f"Expected State='cold' after SUSPEND, got '{db_state}'"

    # RESUME from the default connection.
    resume_result = execute_and_fetch_all(cursor_default, "RESUME DATABASE db1")
    assert len(resume_result) == 1, f"RESUME DATABASE should return 1 row, got {resume_result}"
    assert "resumed" in resume_result[0][0].lower(), f"RESUME status message unexpected: {resume_result[0][0]}"

    # Poll until db1 is back to 'ready' before reading data (resume is synchronous
    # in the RESUME DATABASE command path, but we poll defensively).
    assert _wait_for_db_state(
        cursor_default, "db1", "ready", timeout_s=15.0
    ), "db1 did not return to 'ready' state within 15 seconds after RESUME"

    # Verify data survived the suspend->resume round-trip (fresh connection — the original
    # work connection was closed before SUSPEND).
    conn_verify = connect(host="localhost", port=7687)
    cursor_verify = conn_verify.cursor()
    execute_and_fetch_all(cursor_verify, "USE DATABASE db1")
    count_after = execute_and_fetch_all(cursor_verify, "MATCH (n) RETURN count(n) AS c")
    assert count_after[0][0] == 3, (
        f"Expected 3 nodes after resume, got {count_after[0][0]} "
        "(data loss: suspend->resume did not recover storage)"
    )


# ---------------------------------------------------------------------------
# T2 — USE DATABASE on a COLD tenant block-and-resumes (connection-scoped seam)
# ---------------------------------------------------------------------------


def test_t2_use_database_block_resumes_cold_tenant(test_name):
    """
    Verify that a fresh data connection attaching to a suspended (COLD) tenant via
    USE DATABASE reheats it synchronously (block-and-resume) and the subsequent
    query succeeds — no "resuming" exception in the connection-scoped model.

    This tests the SetCurrentDB/USE -> ResumeForSession -> Resume_ code path that
    unit tests cannot fully exercise (it requires the full Memgraph binary with
    durability and the experimental flag).

    Expected behaviour:
    1. USE DATABASE on a COLD tenant blocks until recovery completes, then succeeds.
    2. The query then returns the correct result, proving storage was rebuilt from
       its durability files.
    """
    instances = instance_description(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)

    conn_default = connect(host="localhost", port=7687)
    cursor_default = conn_default.cursor()

    # Create db2 and populate 2 nodes.
    execute_and_fetch_all(cursor_default, "CREATE DATABASE db2")
    conn_work = connect(host="localhost", port=7687)
    cursor_work = conn_work.cursor()
    execute_and_fetch_all(cursor_work, "USE DATABASE db2")
    execute_and_fetch_all(cursor_work, "CREATE (:Node)")
    execute_and_fetch_all(cursor_work, "CREATE (:Node)")

    # Connection-scoped: close the work connection so db2 has zero connections and can suspend.
    conn_work.close()

    # Suspend db2 from the default connection.
    execute_and_fetch_all(cursor_default, "SUSPEND DATABASE db2")
    assert _get_db_state(cursor_default, "db2") == "cold", "db2 should be cold after SUSPEND"

    # A fresh connection attaching to the COLD tenant must block-and-resume on USE — no exception.
    conn_resume = connect(host="localhost", port=7687)
    cursor_resume = conn_resume.cursor()
    execute_and_fetch_all(cursor_resume, "USE DATABASE db2")  # synchronously resumes; must not raise
    rows = execute_and_fetch_all(cursor_resume, "MATCH (n) RETURN count(n) AS c")
    final_count = rows[0][0]
    conn_resume.close()

    assert final_count == 2, (
        f"Expected 2 nodes after block-and-resume, got {final_count} " "(resume did not recover storage)"
    )


# ---------------------------------------------------------------------------
# T3 — a connected session pins the tenant; closing it makes it suspendable
# ---------------------------------------------------------------------------


def test_t3_connected_session_blocks_suspend(test_name):
    """
    Connection-scoped model: a session holds its DB accessor for its whole lifetime
    (not just per-query).  Therefore an OPEN connection on a tenant pins it HOT and
    SUSPEND returns ACTIVE_CONNECTIONS; only after the connection closes (gatekeeper
    count back to sole-accessor) does SUSPEND succeed.

    This is the suspend-safety mechanism the connection-scoped model relies on: the
    gatekeeper use-count, incremented by every live session accessor.
    """
    instances = instance_description(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)

    conn_default = connect(host="localhost", port=7687)
    cursor_default = conn_default.cursor()

    execute_and_fetch_all(cursor_default, "CREATE DATABASE db3")

    # conn_session: switch to db3 and leave the connection OPEN (holds db3's accessor).
    conn_session = connect(host="localhost", port=7687)
    cursor_session = conn_session.cursor()
    execute_and_fetch_all(cursor_session, "USE DATABASE db3")
    execute_and_fetch_all(cursor_session, "RETURN 1 AS x")

    # While the session is open, SUSPEND must fail with ACTIVE_CONNECTIONS.
    suspend_blocked_raised = False
    suspend_error_msg = ""
    try:
        execute_and_fetch_all(cursor_default, "SUSPEND DATABASE db3")
    except Exception as exc:
        suspend_blocked_raised = True
        suspend_error_msg = str(exc)

    assert suspend_blocked_raised, (
        "Expected SUSPEND DATABASE db3 to fail while a session is connected to db3 "
        "(connection-scoped accessor pins the tenant HOT), but it succeeded."
    )
    assert (
        "active connections" in suspend_error_msg.lower()
    ), f"Expected an ACTIVE_CONNECTIONS error, got: {suspend_error_msg!r}"

    # Close the session → db3 drops to zero connections.
    conn_session.close()

    # Now SUSPEND must succeed.
    suspend_result = execute_and_fetch_all(cursor_default, "SUSPEND DATABASE db3")
    assert len(suspend_result) == 1, f"SUSPEND should return 1 row, got {suspend_result}"
    assert "suspended" in suspend_result[0][0].lower(), f"SUSPEND status message unexpected: {suspend_result[0][0]}"

    db_state = _get_db_state(cursor_default, "db3")
    assert db_state == "cold", f"Expected db3 to be 'cold' after SUSPEND, got '{db_state}'"


# ---------------------------------------------------------------------------
# T4 — flag-off rejection
# ---------------------------------------------------------------------------


def test_t4_flag_off_suspend_resume_rejected(test_name):
    """
    Verify that SUSPEND DATABASE and RESUME DATABASE are rejected with a helpful
    error message when the hot-cold-tenants experiment flag is NOT enabled.

    The error message must mention the experiment name so operators know which
    flag to add.
    """
    instances = instance_description_no_experiment(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)

    conn = connect(host="localhost", port=7687)
    cursor = conn.cursor()

    execute_and_fetch_all(cursor, "CREATE DATABASE db4")

    # SUSPEND should raise with a mention of the experiment flag.
    suspend_raised = False
    suspend_error_msg = ""
    try:
        execute_and_fetch_all(cursor, "SUSPEND DATABASE db4")
    except Exception as exc:
        suspend_raised = True
        suspend_error_msg = str(exc)

    assert suspend_raised, "Expected SUSPEND DATABASE to raise when experiment is disabled"
    assert (
        "hot-cold-tenants" in suspend_error_msg.lower()
    ), f"Expected error to mention 'hot-cold-tenants', got: {suspend_error_msg!r}"

    # RESUME should raise with the same enable-hint.
    resume_raised = False
    resume_error_msg = ""
    try:
        execute_and_fetch_all(cursor, "RESUME DATABASE db4")
    except Exception as exc:
        resume_raised = True
        resume_error_msg = str(exc)

    assert resume_raised, "Expected RESUME DATABASE to raise when experiment is disabled"
    assert (
        "hot-cold-tenants" in resume_error_msg.lower()
    ), f"Expected error to mention 'hot-cold-tenants', got: {resume_error_msg!r}"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
