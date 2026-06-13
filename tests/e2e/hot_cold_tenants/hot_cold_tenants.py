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


# ---------------------------------------------------------------------------
# T5 — automatic memory-watermark eviction scheduler
# ---------------------------------------------------------------------------

# Instance args for T5: eviction scheduler enabled with an aggressively low
# watermark so that normal process memory (>>2.6 MiB at 1% of 256 MiB) keeps
# the high-watermark permanently crossed and the scheduler suspends idle tenants
# within a few poll cycles.
_EVICTION_ARGS = [
    "--bolt-port",
    "7687",
    "--log-level",
    "TRACE",
    "--experimental-enabled=hot-cold-tenants",
    "--storage-wal-enabled=true",
    "--storage-snapshot-interval-sec=300",
    "--data-recovery-on-startup=true",
    "--storage-hot-cold-min-hot-residency-sec=0",
    # Eviction scheduler flags
    "--storage-hot-cold-eviction-enabled=true",
    "--storage-hot-cold-eviction-poll-interval-sec=1",
    "--storage-hot-cold-eviction-high-watermark-percent=1",
    "--storage-hot-cold-eviction-low-watermark-percent=1",
    "--storage-hot-cold-eviction-max-per-cycle=3",
    # 256 MiB limit → high watermark ~2.6 MiB; process baseline easily exceeds it.
    "--memory-limit=256",
]


def instance_description_eviction(test_name: str) -> dict:
    """Return a cluster description for the auto-eviction T5 test."""
    return {
        "instance_evict": {
            "args": _EVICTION_ARGS,
            "log_file": f"{get_logs_path(file, test_name)}/instance_evict.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_evict",
            "setup_queries": [],
        },
    }


def _read_instance_log(test_name: str) -> str:
    """Return the content of the T5 instance log, or an empty string if not found."""
    log_path = os.path.join(
        interactive_mg_runner.BUILD_DIR,
        "tests",
        "e2e",
        f"hot_cold_tenants/{file}/{test_name}/instance_evict.log",
    )
    try:
        with open(log_path, encoding="utf-8", errors="replace") as fh:
            return fh.read()
    except OSError:
        return ""


def test_t5_auto_eviction_under_memory_pressure(test_name):
    """
    Verify that the automatic memory-watermark eviction scheduler suspends an idle
    non-default tenant when memory usage exceeds the high-watermark. Usage is the max
    of tracked allocations and real resident memory (RSS), so the trigger fires under
    either kind of pressure.

    Setup:
    - 256 MiB memory limit; high-watermark = 1% (~2.6 MiB).  The Memgraph process
      baseline (tracked allocation AND RSS) easily exceeds 2.6 MiB, so the scheduler
      will fire on the very first poll tick (1 s interval).
    - ``idle_db`` is created, populated (2 000 nodes), and then left completely idle.
    - The default ``memgraph`` DB is kept alive on ``conn_default`` but never writes,
      so it is NOT the coldest tenant and must never be auto-evicted (the scheduler
      also skips kDefaultDB unconditionally).

    Expected behaviour:
    1. Within ~20 s idle_db transitions to State='cold' (auto-suspended by the
       eviction scheduler).
    2. After auto-suspension, a fresh connection's USE DATABASE idle_db block-and-
       resumes the tenant synchronously, and the subsequent query returns
       count(n) == 2 000.

    If the scheduler does not fire within the timeout the test fails with:
    - the last ``SHOW DATABASES`` output, and
    - the lines from the instance log that contain the word "eviction".
    """
    instances = instance_description_eviction(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)

    # Default connection stays on the 'memgraph' system DB throughout.
    conn_default = connect(host="localhost", port=7687)
    cursor_default = conn_default.cursor()

    # Create the tenant that will be auto-evicted.
    execute_and_fetch_all(cursor_default, "CREATE DATABASE idle_db")

    # Populate idle_db over a dedicated connection. Connection-scoped: this open
    # connection pins idle_db HOT for its whole lifetime, so the 1-second-interval
    # eviction scheduler cannot evict it mid-populate (it is closed below to release).
    #
    # USE DATABASE must be issued as an autocommit (implicit) transaction; only then
    # can we switch the connection to explicit-transaction mode for the heavy CREATE.
    conn_populate = mgclient.connect(host="localhost", port=7687)
    conn_populate.autocommit = True
    cursor_populate = conn_populate.cursor()
    cursor_populate.execute("USE DATABASE idle_db")

    # Switch to explicit-transaction mode so the CREATE runs as one atomic op.
    conn_populate.autocommit = False
    cursor_populate.execute("UNWIND range(1, 2000) AS i CREATE (:Pad {i: i})")
    conn_populate.commit()

    # Close the populate connection so idle_db has zero active connections/accessors.
    conn_populate.close()

    # idle_db is now HOT with data but completely idle.  The scheduler fires every
    # 1 s.  Poll SHOW DATABASES until idle_db goes 'cold' (auto-evicted).
    EVICTION_TIMEOUT_S = 20.0
    last_show_dbs: list[tuple] = []

    deadline = time.monotonic() + EVICTION_TIMEOUT_S
    became_cold = False
    while time.monotonic() < deadline:
        last_show_dbs = execute_and_fetch_all(cursor_default, "SHOW DATABASES")
        for row in last_show_dbs:
            if row[0] == "idle_db" and str(row[1]) == "cold":
                became_cold = True
                break
        if became_cold:
            break
        time.sleep(1.0)

    if not became_cold:
        # Extract eviction-related log lines for diagnostics.
        full_log = _read_instance_log(test_name)
        eviction_lines = [ln for ln in full_log.splitlines() if "eviction" in ln.lower()]
        eviction_excerpt = "\n".join(eviction_lines[-40:]) if eviction_lines else "(no eviction log lines found)"
        pytest.fail(
            f"idle_db did not reach State='cold' within {EVICTION_TIMEOUT_S}s.\n"
            f"Last SHOW DATABASES output: {last_show_dbs!r}\n"
            f"Instance log (eviction lines):\n{eviction_excerpt}"
        )

    # -------------------------------------------------------------------------
    # Round-trip: confirm data survived auto-eviction. A fresh connection's
    # USE DATABASE block-and-resumes the COLD tenant; retry in a bounded loop to
    # tolerate the brief window where the scheduler is mid-suspend.
    # -------------------------------------------------------------------------
    RESUME_TIMEOUT_S = 20.0
    deadline_resume = time.monotonic() + RESUME_TIMEOUT_S
    final_count: int | None = None
    last_resume_exc: Exception | None = None

    while time.monotonic() < deadline_resume:
        try:
            conn_check = connect(host="localhost", port=7687)
            cursor_check = conn_check.cursor()
            execute_and_fetch_all(cursor_check, "USE DATABASE idle_db")
            rows = execute_and_fetch_all(cursor_check, "MATCH (n:Pad) RETURN count(n) AS c")
            final_count = rows[0][0]
            conn_check.close()
            break
        except Exception as exc:
            last_resume_exc = exc
            time.sleep(0.5)

    assert final_count is not None, (
        f"idle_db did not become queryable within {RESUME_TIMEOUT_S}s after auto-eviction. "
        f"Last exception: {last_resume_exc!r}"
    )
    assert final_count == 2000, (
        f"Expected 2000 nodes after auto-eviction resume, got {final_count} "
        "(data loss: auto-suspend->resume did not recover storage)"
    )


# ---------------------------------------------------------------------------
# T6 — idle-session reaper: an open-but-idle pooled connection no longer pins
#      its tenant after the idle timeout, and transparently reheats on next use
# ---------------------------------------------------------------------------

# Reaper enabled with a short idle timeout; the eviction poll interval doubles as the reaper sweep
# cadence (1s). Auto-eviction itself is left OFF — the reaper only releases the idle accessor; we
# prove the release via a manual SUSPEND from a different connection succeeding.
_REAPER_ARGS = [
    "--bolt-port",
    "7687",
    "--log-level",
    "TRACE",
    "--experimental-enabled=hot-cold-tenants",
    "--storage-wal-enabled=true",
    "--storage-snapshot-interval-sec=300",
    "--data-recovery-on-startup=true",
    "--storage-hot-cold-min-hot-residency-sec=0",
    "--storage-hot-cold-idle-session-timeout-sec=2",
    "--storage-hot-cold-eviction-poll-interval-sec=1",
]


def instance_description_reaper(test_name: str) -> dict:
    return {
        "instance_reaper": {
            "args": _REAPER_ARGS,
            "log_file": f"{get_logs_path(file, test_name)}/instance_reaper.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_reaper",
            "setup_queries": [],
        },
    }


def test_t6_idle_session_reaper_unpins_then_reheats(test_name):
    """
    With the idle-session reaper enabled (idle timeout 2s), a connection that ran a query on db6 and
    then sits OPEN and IDLE must, after the timeout, have its accessor released by the background
    reaper — so a SUSPEND DATABASE db6 from a DIFFERENT connection succeeds despite db6 still having
    an open connection. The idle connection is NOT dropped: a subsequent query on it block-and-resumes
    db6 and reads the original data.
    """
    instances = instance_description_reaper(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)

    conn_default = connect(host="localhost", port=7687)
    cursor_default = conn_default.cursor()
    execute_and_fetch_all(cursor_default, "CREATE DATABASE db6")

    # Idle (pooled) connection: switch to db6, write data, then go idle (do NOT close).
    conn_idle = connect(host="localhost", port=7687)
    cursor_idle = conn_idle.cursor()
    execute_and_fetch_all(cursor_idle, "USE DATABASE db6")
    execute_and_fetch_all(cursor_idle, "CREATE (:Node)")
    execute_and_fetch_all(cursor_idle, "CREATE (:Node)")

    # Immediately, the idle connection pins db6 -> SUSPEND must fail.
    suspend_blocked = False
    try:
        execute_and_fetch_all(cursor_default, "SUSPEND DATABASE db6")
    except Exception as exc:
        suspend_blocked = "active connections" in str(exc).lower()
    assert suspend_blocked, "SUSPEND db6 should initially fail (idle connection still pins it)"

    # After the idle timeout the reaper releases the idle connection's accessor. Poll SUSPEND until it
    # succeeds (bounded), proving the pin was dropped without closing the connection.
    deadline = time.monotonic() + 20.0
    suspended = False
    last_err = ""
    while time.monotonic() < deadline:
        try:
            res = execute_and_fetch_all(cursor_default, "SUSPEND DATABASE db6")
            if res and "suspended" in res[0][0].lower():
                suspended = True
                break
        except Exception as exc:
            last_err = str(exc)
        time.sleep(0.5)
    assert suspended, f"reaper should have unpinned db6 so SUSPEND succeeds within 20s; last error: {last_err!r}"
    assert _get_db_state(cursor_default, "db6") == "cold", "db6 should be cold after reaper-enabled SUSPEND"

    # The idle connection was never closed: a fresh query on it block-and-resumes db6 and reads data.
    count = execute_and_fetch_all(cursor_idle, "MATCH (n) RETURN count(n) AS c")
    assert count[0][0] == 2, (
        f"the pooled connection must transparently reheat db6 and read its 2 nodes, got {count[0][0]}"
    )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
