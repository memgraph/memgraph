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
import urllib.error
import urllib.request

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
    assert (
        count[0][0] == 2
    ), f"the pooled connection must transparently reheat db6 and read its 2 nodes, got {count[0][0]}"


# ---------------------------------------------------------------------------
# T7 — runtime-settable eviction knobs (watermarks / max-per-cycle / poll-interval)
# ---------------------------------------------------------------------------

# Reuse the eviction instance config (eviction enabled + experiment) so the poll-interval observer is
# actually attached to a running scheduler; the four settings are registered regardless of enablement.
_RUNTIME_KNOBS_ARGS = list(_EVICTION_ARGS)


def instance_description_runtime_knobs(test_name: str) -> dict:
    return {
        "instance_knobs": {
            "args": _RUNTIME_KNOBS_ARGS,
            "log_file": f"{get_logs_path(file, test_name)}/instance_knobs.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_knobs",
            "setup_queries": [],
        },
    }


_EVICTION_SETTING_KEYS = {
    "high": "storage.hot_cold.eviction_high_watermark_percent",
    "low": "storage.hot_cold.eviction_low_watermark_percent",
    "max": "storage.hot_cold.eviction_max_per_cycle",
    "poll": "storage.hot_cold.eviction_poll_interval_sec",
}


def test_t7_runtime_settable_eviction_knobs(test_name):
    """
    Verify the eviction knobs are runtime-settable via SET DATABASE SETTING and validated:
    - SHOW DATABASE SETTING returns the CLI-configured values.
    - Valid SET values round-trip (visible via SHOW).
    - Out-of-range watermarks (0, >100), a zero poll-interval, and a non-numeric value are rejected.
    """
    instances = instance_description_runtime_knobs(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)
    conn = connect(host="localhost", port=7687)
    cursor = conn.cursor()

    def show(key: str) -> str:
        rows = execute_and_fetch_all(cursor, f"SHOW DATABASE SETTING '{key}'")
        return str(rows[0][0])

    def set_value(key: str, value: str):
        execute_and_fetch_all(cursor, f"SET DATABASE SETTING '{key}' TO '{value}'")

    # The _EVICTION_ARGS configure high=1, low=1, max=3, poll=1.
    assert show(_EVICTION_SETTING_KEYS["high"]) == "1"
    assert show(_EVICTION_SETTING_KEYS["low"]) == "1"
    assert show(_EVICTION_SETTING_KEYS["max"]) == "3"
    assert show(_EVICTION_SETTING_KEYS["poll"]) == "1"

    # Valid runtime changes round-trip.
    set_value(_EVICTION_SETTING_KEYS["high"], "90")
    set_value(_EVICTION_SETTING_KEYS["low"], "60")
    set_value(_EVICTION_SETTING_KEYS["max"], "7")
    set_value(_EVICTION_SETTING_KEYS["poll"], "5")
    assert show(_EVICTION_SETTING_KEYS["high"]) == "90"
    assert show(_EVICTION_SETTING_KEYS["low"]) == "60"
    assert show(_EVICTION_SETTING_KEYS["max"]) == "7"
    assert show(_EVICTION_SETTING_KEYS["poll"]) == "5"

    # Invalid values are rejected and leave the previous value intact.
    for key, bad in [
        (_EVICTION_SETTING_KEYS["high"], "0"),
        (_EVICTION_SETTING_KEYS["high"], "150"),
        (_EVICTION_SETTING_KEYS["low"], "101"),
        (_EVICTION_SETTING_KEYS["poll"], "0"),
        (_EVICTION_SETTING_KEYS["max"], "abc"),
    ]:
        with pytest.raises(Exception):
            set_value(key, bad)
    # The rejected writes did not mutate state.
    assert show(_EVICTION_SETTING_KEYS["high"]) == "90"
    assert show(_EVICTION_SETTING_KEYS["low"]) == "60"
    assert show(_EVICTION_SETTING_KEYS["poll"]) == "5"
    assert show(_EVICTION_SETTING_KEYS["max"]) == "7"

    conn.close()


# ---------------------------------------------------------------------------
# T8 — make-room-on-resume: reheating a COLD tenant under memory pressure evicts the coldest idle peer
# ---------------------------------------------------------------------------

# Eviction enabled, but the PERIODIC poll-interval is set huge (3600 s) so the background scheduler does
# not fire during the test. Make-room-on-resume (synchronous, fired from the resume path) is the ONLY
# eviction trigger exercised here, isolating it from the periodic scheduler.
_MAKEROOM_ARGS = [
    "--bolt-port",
    "7687",
    "--log-level",
    "TRACE",
    "--experimental-enabled=hot-cold-tenants",
    "--storage-wal-enabled=true",
    "--storage-snapshot-interval-sec=300",
    "--data-recovery-on-startup=true",
    "--storage-hot-cold-min-hot-residency-sec=0",
    "--storage-hot-cold-eviction-enabled=true",
    "--storage-hot-cold-eviction-poll-interval-sec=3600",  # periodic scheduler effectively OFF during test
    "--storage-hot-cold-eviction-high-watermark-percent=1",
    "--storage-hot-cold-eviction-low-watermark-percent=1",
    "--storage-hot-cold-eviction-max-per-cycle=3",
    "--memory-limit=256",  # high watermark ~2.6 MiB; process RSS always exceeds it -> resume always makes room
]


def instance_description_makeroom(test_name: str) -> dict:
    return {
        "instance_makeroom": {
            "args": _MAKEROOM_ARGS,
            "log_file": f"{get_logs_path(file, test_name)}/instance_makeroom.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_makeroom",
            "setup_queries": [],
        },
    }


def test_t8_make_room_on_resume_evicts_coldest_peer(test_name):
    """
    With the periodic eviction scheduler effectively disabled (poll = 3600 s), prove that reheating a
    COLD tenant under memory pressure synchronously evicts the coldest idle PEER (make-room-on-resume):

    1. Create + populate tenant_a and tenant_b; close their populate connections so both are HOT idle.
    2. Manually SUSPEND tenant_a -> cold. tenant_b stays HOT and idle.
    3. USE DATABASE tenant_a on a fresh connection: this block-and-resumes tenant_a. Memory is over the
       1% high watermark (process RSS >> 2.6 MiB), so make-room fires post-publish and evicts the
       coldest idle peer = tenant_b. tenant_a itself is protected (the resume still holds its accessor).
    4. Assert tenant_b is 'cold' right after the USE returns (make-room is synchronous), while tenant_a
       is 'ready'. The periodic scheduler (3600 s) cannot account for this within the test window.
    """
    instances = instance_description_makeroom(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)

    conn_default = connect(host="localhost", port=7687)
    cursor_default = conn_default.cursor()

    for name in ("tenant_a", "tenant_b"):
        execute_and_fetch_all(cursor_default, f"CREATE DATABASE {name}")
        conn_pop = mgclient.connect(host="localhost", port=7687)
        conn_pop.autocommit = True
        cur_pop = conn_pop.cursor()
        cur_pop.execute(f"USE DATABASE {name}")
        conn_pop.autocommit = False
        cur_pop.execute("UNWIND range(1, 1000) AS i CREATE (:Pad {i: i})")
        conn_pop.commit()
        conn_pop.close()

    # Both tenants are HOT and idle. Suspend tenant_a so the next USE has to resume it.
    execute_and_fetch_all(cursor_default, "SUSPEND DATABASE tenant_a")
    assert _wait_for_db_state(cursor_default, "tenant_a", "cold", timeout_s=5.0), "tenant_a should suspend to cold"
    assert _get_db_state(cursor_default, "tenant_b") == "ready", "tenant_b must still be HOT before the resume"

    # Resume tenant_a on a fresh connection: make-room fires post-publish and evicts the coldest peer.
    conn_resume = mgclient.connect(host="localhost", port=7687)
    conn_resume.autocommit = True
    cur_resume = conn_resume.cursor()
    cur_resume.execute("USE DATABASE tenant_a")
    # Read tenant_a to confirm it reheated with data intact.
    cur_resume.execute("MATCH (n) RETURN count(n) AS c")
    a_count = cur_resume.fetchall()[0][0]
    assert a_count == 1000, f"tenant_a must reheat with its 1000 nodes, got {a_count}"

    # make-room is synchronous within the resume, so tenant_b is already cold (or becomes so promptly).
    assert _wait_for_db_state(
        cursor_default, "tenant_b", "cold", timeout_s=5.0
    ), "make-room-on-resume should have evicted the coldest idle peer tenant_b after resuming tenant_a"
    assert _get_db_state(cursor_default, "tenant_a") == "ready", "the just-resumed tenant_a must stay HOT (protected)"

    conn_resume.close()


# ---------------------------------------------------------------------------
# T9 — replica-side hot/cold: COLD replica tenant reheats on replication traffic
# ---------------------------------------------------------------------------

_T9_BOLT_PORTS = {"main": 7687, "replica_1": 7688}
_T9_REPLICATION_PORT = 10001


def _hot_cold_args_for_port(bolt_port: int) -> list[str]:
    """Return a copy of _HOT_COLD_ARGS_BASE with the --bolt-port value replaced."""
    args = list(_HOT_COLD_ARGS_BASE)
    # _HOT_COLD_ARGS_BASE layout: ["--bolt-port", "7687", ...]
    bolt_idx = args.index("--bolt-port")
    args[bolt_idx + 1] = str(bolt_port)
    return args


def instance_description_replication(test_name: str) -> dict:
    """Return a 2-instance cluster description for the T9 replication test."""
    return {
        "replica_1": {
            "args": _hot_cold_args_for_port(_T9_BOLT_PORTS["replica_1"]),
            "log_file": f"{get_logs_path(file, test_name)}/replica_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica_1",
            "setup_queries": [
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {_T9_REPLICATION_PORT};",
            ],
        },
        "main": {
            "args": _hot_cold_args_for_port(_T9_BOLT_PORTS["main"]),
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{_T9_REPLICATION_PORT}';",
            ],
        },
    }


def test_t9_replica_resumes_cold_tenant_on_replication_traffic(test_name):
    """
    Prove the replica-side hot/cold replication MVP:

    1. A tenant suspended (COLD) on a REPLICA is automatically resumed when the MAIN
       replicates new writes to it, and ends up HOT with all data (old + new).
    2. While cold, periodic heartbeats from main do NOT reheat it.

    Setup:
    - main on bolt 7687, replica_1 on bolt 7688, replication port 10001.
    - Both instances use the hot-cold experimental args (WAL + snapshot-interval=300 +
      data-recovery-on-startup=true + min-hot-residency=0).
    - Replication mode: SYNC (so main's commit drives PrepareCommit to the replica
      synchronously, which must resume the cold tenant to apply the WAL delta).

    Steps:
    1. Start both instances.
    2. CREATE DATABASE tenant1 on main (replicated to replica).
    3. Write 5 nodes into tenant1 on main.
    4. Verify tenant1 has 5 nodes on the replica.
    5. SUSPEND tenant1 on the replica (retry if replication apply accessor is still active).
    6. Poll until tenant1 is 'cold' on the replica.
    7. Sleep 4s and confirm tenant1 is STILL 'cold' (heartbeats must not reheat).
    8. Write 4 more nodes (ids 6–9) into tenant1 on main; SYNC commit drives replica resume.
    9. Poll until tenant1 is 'ready' on the replica (reheated by replication traffic).
    10. Verify tenant1 has 9 nodes on the replica (old + new data intact).
    """
    instances = instance_description_replication(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)

    # -------------------------------------------------------------------------
    # Step 2: create tenant1 on main (replicated automatically).
    # -------------------------------------------------------------------------
    main_default = connect(host="localhost", port=_T9_BOLT_PORTS["main"])
    cursor_main_default = main_default.cursor()
    execute_and_fetch_all(cursor_main_default, "CREATE DATABASE tenant1;")

    # -------------------------------------------------------------------------
    # Step 3: write 5 nodes into tenant1 on main.
    # -------------------------------------------------------------------------
    conn_main_t1 = connect(host="localhost", port=_T9_BOLT_PORTS["main"])
    cursor_main_t1 = conn_main_t1.cursor()
    execute_and_fetch_all(cursor_main_t1, "USE DATABASE tenant1;")
    execute_and_fetch_all(cursor_main_t1, "UNWIND range(1, 5) AS i CREATE (:N {id: i});")

    # -------------------------------------------------------------------------
    # Step 4: verify tenant1 has 5 nodes on the replica.
    # USE DATABASE tenant1 on the replica may block until the CREATE DATABASE
    # replicates; retry in a bounded loop.
    # -------------------------------------------------------------------------
    REPLICA_SYNC_TIMEOUT_S = 30.0
    deadline = time.monotonic() + REPLICA_SYNC_TIMEOUT_S
    replica_count: int | None = None
    last_replica_exc: Exception | None = None

    while time.monotonic() < deadline:
        try:
            conn_replica_t1 = connect(host="localhost", port=_T9_BOLT_PORTS["replica_1"])
            cursor_replica_t1 = conn_replica_t1.cursor()
            execute_and_fetch_all(cursor_replica_t1, "USE DATABASE tenant1;")
            rows = execute_and_fetch_all(cursor_replica_t1, "MATCH (n:N) RETURN count(n);")
            replica_count = rows[0][0]
            conn_replica_t1.close()
            if replica_count == 5:
                break
        except Exception as exc:
            last_replica_exc = exc
        time.sleep(0.5)

    assert replica_count == 5, (
        f"Expected 5 nodes on replica after initial replication, got {replica_count!r}. "
        f"Last exception: {last_replica_exc!r}"
    )

    # -------------------------------------------------------------------------
    # Step 5: SUSPEND tenant1 on the replica.
    # Close the main tenant1 session first (no longer needed until step 8).
    # The replication-apply accessor may still be releasing; retry with a bounded loop.
    # The replica's SHOW DATABASES cursor must live on the replica's default db.
    # -------------------------------------------------------------------------
    conn_main_t1.close()

    replica_default = connect(host="localhost", port=_T9_BOLT_PORTS["replica_1"])
    cursor_replica_default = replica_default.cursor()

    SUSPEND_RETRY_TIMEOUT_S = 15.0
    deadline_suspend = time.monotonic() + SUSPEND_RETRY_TIMEOUT_S
    suspended = False
    last_suspend_err = ""
    while time.monotonic() < deadline_suspend:
        try:
            result = execute_and_fetch_all(cursor_replica_default, "SUSPEND DATABASE tenant1;")
            if result and "suspended" in result[0][0].lower():
                suspended = True
                break
        except Exception as exc:
            last_suspend_err = str(exc)
            if "active connections" not in last_suspend_err.lower():
                # Unexpected error — re-raise immediately.
                raise
        time.sleep(0.5)

    assert suspended, (
        f"Could not SUSPEND tenant1 on replica within {SUSPEND_RETRY_TIMEOUT_S}s. " f"Last error: {last_suspend_err!r}"
    )

    # -------------------------------------------------------------------------
    # Step 6: poll until tenant1 is 'cold' on the replica.
    # -------------------------------------------------------------------------
    became_cold = _wait_for_db_state(cursor_replica_default, "tenant1", "cold", timeout_s=30.0)
    assert became_cold, (
        f"tenant1 did not reach State='cold' on replica within 30s after SUSPEND. "
        f"Current state: {_get_db_state(cursor_replica_default, 'tenant1')!r}"
    )

    # -------------------------------------------------------------------------
    # Step 7: heartbeat-no-reheat check.
    # Sleep long enough for several main heartbeat cycles; tenant1 must remain cold.
    # -------------------------------------------------------------------------
    time.sleep(4.0)
    state_after_heartbeats = _get_db_state(cursor_replica_default, "tenant1")
    assert state_after_heartbeats == "cold", (
        f"Periodic heartbeats from main must NOT reheat a cold replica tenant, "
        f"but tenant1 state is '{state_after_heartbeats}' after 4s sleep."
    )

    # -------------------------------------------------------------------------
    # Step 8: write 4 more nodes on main; SYNC commit drives PrepareCommit to the
    # replica, which must resume tenant1 to apply the WAL delta.
    # -------------------------------------------------------------------------
    conn_main_t1_b = connect(host="localhost", port=_T9_BOLT_PORTS["main"])
    cursor_main_t1_b = conn_main_t1_b.cursor()
    execute_and_fetch_all(cursor_main_t1_b, "USE DATABASE tenant1;")
    execute_and_fetch_all(cursor_main_t1_b, "UNWIND range(6, 9) AS i CREATE (:N {id: i});")
    conn_main_t1_b.close()

    # -------------------------------------------------------------------------
    # Step 9: poll until tenant1 is 'ready' on the replica (reheated by traffic).
    # -------------------------------------------------------------------------
    reheated = _wait_for_db_state(cursor_replica_default, "tenant1", "ready", timeout_s=30.0)
    assert reheated, (
        f"tenant1 did not reheat to 'ready' on replica within 30s after replication traffic. "
        f"Current state: {_get_db_state(cursor_replica_default, 'tenant1')!r}"
    )

    # -------------------------------------------------------------------------
    # Step 10: verify all 9 nodes are present on the replica (old + new data intact).
    # -------------------------------------------------------------------------
    VERIFY_TIMEOUT_S = 30.0
    deadline_verify = time.monotonic() + VERIFY_TIMEOUT_S
    final_count: int | None = None
    last_verify_exc: Exception | None = None

    while time.monotonic() < deadline_verify:
        try:
            conn_replica_verify = connect(host="localhost", port=_T9_BOLT_PORTS["replica_1"])
            cursor_replica_verify = conn_replica_verify.cursor()
            execute_and_fetch_all(cursor_replica_verify, "USE DATABASE tenant1;")
            rows = execute_and_fetch_all(cursor_replica_verify, "MATCH (n:N) RETURN count(n);")
            final_count = rows[0][0]
            conn_replica_verify.close()
            if final_count == 9:
                break
        except Exception as exc:
            last_verify_exc = exc
        time.sleep(0.5)

    assert final_count == 9, (
        f"Expected 9 nodes on replica after reheat (5 original + 4 replicated), "
        f"got {final_count!r}. Last exception: {last_verify_exc!r}"
    )


# ---------------------------------------------------------------------------
# T10 — hot/cold metrics are exported on the Prometheus/OpenMetrics endpoint
# ---------------------------------------------------------------------------

# Non-default metrics port: the default (9091) may be occupied by another local Memgraph
# (e.g. a dev/knowledge-graph instance), which would make the scrape hit the wrong server.
# The T10 instance is started with a matching --metrics-port below.
_METRICS_PORT = 19091
_HOT_COLD_METRIC_NAMES = [
    "memgraph_hot_cold_suspended_tenants",
    "memgraph_hot_cold_suspends_total",
    "memgraph_hot_cold_resumes_total",
    "memgraph_hot_cold_resume_failures_total",
    "memgraph_hot_cold_evictions_total",
    "memgraph_hot_cold_resume_latency_seconds",
]


def _scrape_metrics(port: int = _METRICS_PORT, timeout_s: float = 15.0, poll_s: float = 0.5) -> str:
    """
    GET http://localhost:{port}/metrics and return the response body as text.

    Retries until *timeout_s* in case the HTTP server is not ready yet (it
    starts slightly after the Bolt server is up).  Raises ``AssertionError``
    if the endpoint never becomes reachable within the timeout.
    """
    url = f"http://localhost:{port}/metrics"
    deadline = time.monotonic() + timeout_s
    last_exc: Exception | None = None

    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except (urllib.error.URLError, OSError) as exc:
            last_exc = exc
            time.sleep(poll_s)

    raise AssertionError(
        f"Metrics endpoint {url!r} did not become reachable within {timeout_s}s. " f"Last error: {last_exc!r}"
    )


def _parse_metric_value(text: str, name: str) -> float:
    """
    Return the current value of *name* from an OpenMetrics/Prometheus text scrape.

    Scans for the first non-comment line that starts with exactly ``name``
    followed by a space (no labels).  The value field is the second
    whitespace-separated token (index 1), tolerant to an optional timestamp.

    Raises ``AssertionError`` if no matching data line is found.
    """
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        if line.startswith(name + " "):
            parts = line.split()
            # OpenMetrics format: metric_name value [timestamp]
            return float(parts[1])

    raise AssertionError(
        f"Metric {name!r} not found in scrape output. "
        f"Available lines (first 40):\n" + "\n".join(text.splitlines()[:40])
    )


def test_t10_hot_cold_metrics_exported(test_name):
    """
    Verify that all six hot/cold metrics are exported on the Prometheus/OpenMetrics
    HTTP endpoint (default port 9091, path /metrics).

    Steps:
    1. Start a single hot-cold-enabled instance (bolt 7687).
    2. Create + populate m_db; close the work connection.
    3. SUSPEND m_db from the default connection; poll until State='cold'.
    4. Scrape /metrics:
       - All 6 metric names appear (HELP/TYPE lines count).
       - memgraph_hot_cold_suspended_tenants == 1.
       - memgraph_hot_cold_suspends_total >= 1.
    5. RESUME m_db; poll until State='ready'.
    6. Scrape /metrics again:
       - memgraph_hot_cold_suspended_tenants == 0.
       - memgraph_hot_cold_resumes_total >= 1.
    """
    # The metrics HTTP server defaults to --metrics-format=JSON, which serves a CURATED JSON object
    # (no hot/cold metrics, not line-parseable). Force OpenMetrics so the Prometheus text endpoint
    # (which auto-exports every registered metric, including our global hot/cold ones) is served.
    instances = {
        "instance_1": {
            "args": _HOT_COLD_ARGS_BASE + ["--metrics-format=OpenMetrics", f"--metrics-port={_METRICS_PORT}"],
            "log_file": f"{get_logs_path(file, test_name)}/instance_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_1",
            "setup_queries": [],
        },
    }
    interactive_mg_runner.start_all(instances, keep_directories=False)

    # Default connection (stays on the 'memgraph' system database throughout).
    conn_default = connect(host="localhost", port=7687)
    cursor_default = conn_default.cursor()

    # Create m_db and write a few nodes.
    execute_and_fetch_all(cursor_default, "CREATE DATABASE m_db")

    conn_work = connect(host="localhost", port=7687)
    cursor_work = conn_work.cursor()
    execute_and_fetch_all(cursor_work, "USE DATABASE m_db")
    execute_and_fetch_all(cursor_work, "CREATE (:Node {x: 1})")
    execute_and_fetch_all(cursor_work, "CREATE (:Node {x: 2})")

    # Close the work connection so m_db has zero active connections (suspend-eligible).
    conn_work.close()

    # Suspend m_db.
    suspend_result = execute_and_fetch_all(cursor_default, "SUSPEND DATABASE m_db")
    assert len(suspend_result) == 1, f"SUSPEND DATABASE should return 1 row, got {suspend_result!r}"
    assert "suspended" in suspend_result[0][0].lower(), f"Unexpected SUSPEND message: {suspend_result[0][0]!r}"

    # Poll until m_db reaches 'cold'.
    assert _wait_for_db_state(
        cursor_default, "m_db", "cold", timeout_s=15.0
    ), "m_db did not reach State='cold' within 15s after SUSPEND"

    # --- Scrape 1: after suspension ---
    metrics_text_after_suspend = _scrape_metrics()

    # All 6 metric names must appear somewhere in the scrape output (HELP/TYPE lines are fine).
    for metric_name in _HOT_COLD_METRIC_NAMES:
        assert metric_name in metrics_text_after_suspend, (
            f"Expected metric {metric_name!r} to appear in /metrics output after suspend, but it was absent. "
            f"Scrape excerpt (first 60 lines):\n" + "\n".join(metrics_text_after_suspend.splitlines()[:60])
        )

    # suspended_tenants must be exactly 1 (only m_db is cold).
    suspended_val = _parse_metric_value(metrics_text_after_suspend, "memgraph_hot_cold_suspended_tenants")
    assert (
        suspended_val == 1.0
    ), f"Expected memgraph_hot_cold_suspended_tenants == 1 after suspending m_db, got {suspended_val}"

    # suspends_total must be >= 1.
    suspends_val = _parse_metric_value(metrics_text_after_suspend, "memgraph_hot_cold_suspends_total")
    assert suspends_val >= 1.0, f"Expected memgraph_hot_cold_suspends_total >= 1 after SUSPEND, got {suspends_val}"

    # Resume m_db.
    resume_result = execute_and_fetch_all(cursor_default, "RESUME DATABASE m_db")
    assert len(resume_result) == 1, f"RESUME DATABASE should return 1 row, got {resume_result!r}"
    assert "resumed" in resume_result[0][0].lower(), f"Unexpected RESUME message: {resume_result[0][0]!r}"

    # Poll until m_db returns to 'ready'.
    assert _wait_for_db_state(
        cursor_default, "m_db", "ready", timeout_s=15.0
    ), "m_db did not return to State='ready' within 15s after RESUME"

    # --- Scrape 2: after resumption ---
    metrics_text_after_resume = _scrape_metrics()

    # suspended_tenants must now be 0.
    suspended_val_after = _parse_metric_value(metrics_text_after_resume, "memgraph_hot_cold_suspended_tenants")
    assert (
        suspended_val_after == 0.0
    ), f"Expected memgraph_hot_cold_suspended_tenants == 0 after RESUME, got {suspended_val_after}"

    # resumes_total must be >= 1.
    resumes_val = _parse_metric_value(metrics_text_after_resume, "memgraph_hot_cold_resumes_total")
    assert resumes_val >= 1.0, f"Expected memgraph_hot_cold_resumes_total >= 1 after RESUME, got {resumes_val}"

    # The resume-latency histogram must have observed at least one sample (proves the Observe path
    # fired on the successful resume — and independently confirms the histogram is exported).
    latency_count = _parse_metric_value(metrics_text_after_resume, "memgraph_hot_cold_resume_latency_seconds_count")
    assert (
        latency_count >= 1.0
    ), f"Expected memgraph_hot_cold_resume_latency_seconds_count >= 1 after RESUME, got {latency_count}"


# ---------------------------------------------------------------------------
# T11 — GRANT/REVOKE DATABASE and SET MAIN DATABASE succeed on a COLD tenant
#        without reheating it (metadata-only path via Contains())
# ---------------------------------------------------------------------------


def test_t11_auth_grants_on_cold_tenant(test_name):
    """
    Validate that multi-tenant auth commands succeed against a COLD (suspended) tenant
    and do NOT reheat it.

    Background:
      Before the fix, GRANT DATABASE / REVOKE DATABASE / SET MAIN DATABASE used a
      throwing DbmsHandler::Get() call as an existence guard.  Get() returns an error
      for COLD tenants (the in-memory accessor is absent), so those commands raised
      "Tried to retrieve an unknown database" on any suspended tenant.  The fix
      switched the existence guard to the non-throwing, HOT+COLD-aware Contains(),
      because auth commands only touch the KV metadata store — they never need to
      open the storage layer.

      A follow-up fix extended Contains()-based guarding to two read-side commands that
      previously also called the throwing Get():
        - SHOW PRIVILEGES FOR <user> ON DATABASE <db>
        - SHOW ROLE FOR <user> ON DATABASE <db>
      Both are now covered by this test.

    This test verifies:
    1. GRANT DATABASE auth_db TO u1  — succeeds while auth_db is COLD.
    2. SET MAIN DATABASE auth_db FOR u1 — succeeds while auth_db is COLD.
    3. SHOW DATABASE PRIVILEGES FOR u1 — returns a row that includes "auth_db",
       proving the grant was persisted.
    4. REVOKE DATABASE auth_db FROM u1 — succeeds while auth_db is COLD.
    5. SHOW PRIVILEGES FOR u1 ON DATABASE auth_db — must not raise against a COLD
       tenant (read-side auth command now using Contains()).
    6. SHOW ROLE FOR u1 ON DATABASE auth_db — must not raise against a COLD tenant
       (read-side auth command now using Contains()).
    7. After all six auth gestures, auth_db is STILL 'cold' (none of them reheated it).

    Connection strategy:
      conn_default is opened before any user is created so it retains admin privileges
      throughout the test (the canonical Memgraph pattern: an already-open admin
      session is unaffected by the creation of the first user).  The short-lived work
      connection is also opened before CREATE USER so it needs no credentials.
    """
    instances = instance_description(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)

    # Step 1: open the admin connection BEFORE any user is created (retains full admin).
    conn_default = connect(host="localhost", port=7687)
    cursor_default = conn_default.cursor()

    # Step 2: create auth_db.
    execute_and_fetch_all(cursor_default, "CREATE DATABASE auth_db")

    # Step 3: populate auth_db via a separate work connection so it has durability
    # content and zero remaining connections after we close it.  Opened before CREATE
    # USER so no credentials are needed.
    conn_work = connect(host="localhost", port=7687)
    cursor_work = conn_work.cursor()
    execute_and_fetch_all(cursor_work, "USE DATABASE auth_db")
    execute_and_fetch_all(cursor_work, "CREATE (:N)")
    conn_work.close()

    # Step 4: create user (no password — matches the pattern in tests/e2e/auth/auth_queries.py).
    execute_and_fetch_all(cursor_default, "CREATE USER u1")

    # Step 5: suspend auth_db and wait for it to reach the COLD state.
    suspend_result = execute_and_fetch_all(cursor_default, "SUSPEND DATABASE auth_db")
    assert len(suspend_result) == 1, f"SUSPEND DATABASE should return 1 row, got {suspend_result!r}"
    assert "suspended" in suspend_result[0][0].lower(), f"SUSPEND status message unexpected: {suspend_result[0][0]!r}"
    assert _wait_for_db_state(
        cursor_default, "auth_db", "cold", timeout_s=15.0
    ), "auth_db did not reach State='cold' within 15s after SUSPEND"

    # Step 6: auth commands while auth_db is COLD — each must succeed without raising.

    # 6a. GRANT DATABASE auth_db TO u1
    grant_rows = execute_and_fetch_all(cursor_default, "GRANT DATABASE auth_db TO u1")
    # Some Memgraph versions return an empty result set; an exception would have failed the test.
    _ = grant_rows  # result may be empty; absence of exception is the assertion.

    # 6b. SET MAIN DATABASE auth_db FOR u1 (requires the grant above).
    execute_and_fetch_all(cursor_default, "SET MAIN DATABASE auth_db FOR u1")

    # 6c. SHOW DATABASE PRIVILEGES FOR u1 — must confirm auth_db is accessible.
    # SHOW DATABASE PRIVILEGES FOR <user> returns rows of the form (grants, denies) where
    # grants is either "*" (all databases) or a list of database names, and denies is a list.
    # After GRANT DATABASE auth_db TO u1 + SET MAIN DATABASE auth_db FOR u1, auth_db must
    # either appear by name in the grants list or the grants value must be "*" (wildcard =
    # all databases, which includes auth_db).  Both forms confirm the grant was persisted and
    # that Contains() correctly resolved auth_db in the COLD state.
    priv_rows = execute_and_fetch_all(cursor_default, "SHOW DATABASE PRIVILEGES FOR u1")
    assert len(priv_rows) > 0, f"SHOW DATABASE PRIVILEGES FOR u1 returned no rows: {priv_rows!r}"
    grants_field = priv_rows[0][0]
    denies_field = priv_rows[0][1]
    # grants_field is either the string "*" (all databases) or a list of db names.
    auth_db_accessible = (
        grants_field == "*"
        or (isinstance(grants_field, (list, tuple)) and "auth_db" in grants_field)
        or "auth_db" in str(grants_field)
    )
    assert auth_db_accessible, (
        f"Expected SHOW DATABASE PRIVILEGES FOR u1 to grant access to auth_db "
        f"(either explicitly or via '*'), got grants={grants_field!r}, denies={denies_field!r}. "
        f"Full rows: {priv_rows!r}"
    )
    # auth_db must not be in the denies list (that would block access despite the grant).
    assert "auth_db" not in str(
        denies_field
    ), f"auth_db must not appear in the denies list after GRANT, got denies={denies_field!r}"

    # 6d. REVOKE DATABASE auth_db FROM u1.
    execute_and_fetch_all(cursor_default, "REVOKE DATABASE auth_db FROM u1")

    # 6e. SHOW PRIVILEGES FOR u1 ON DATABASE auth_db — read-side command, must NOT raise
    # against a COLD tenant (fixed to use Contains() instead of the throwing Get()).
    # Grammar: showPrivileges = SHOW PRIVILEGES FOR <userOrRole> (ON (MAIN|CURRENT|DATABASE <db>))?
    # The assertion is absence-of-exception; we optionally inspect the result shape.
    show_priv_rows = execute_and_fetch_all(cursor_default, "SHOW PRIVILEGES FOR u1 ON DATABASE auth_db")
    assert isinstance(
        show_priv_rows, list
    ), f"SHOW PRIVILEGES FOR u1 ON DATABASE auth_db must return a list (got {type(show_priv_rows)!r})"

    # 6f. SHOW ROLE FOR u1 ON DATABASE auth_db — read-side command, must NOT raise
    # against a COLD tenant (fixed to use Contains() instead of the throwing Get()).
    # Grammar: showRoleForUser = SHOW (ROLE|ROLES) FOR USER? <userOrRoleName> (ON (MAIN|CURRENT|DATABASE <db>))?
    # USER keyword is optional per the grammar rule; 'FOR u1' is accepted.
    show_role_rows = execute_and_fetch_all(cursor_default, "SHOW ROLE FOR u1 ON DATABASE auth_db")
    assert isinstance(
        show_role_rows, list
    ), f"SHOW ROLE FOR u1 ON DATABASE auth_db must return a list (got {type(show_role_rows)!r})"

    # Step 7: correctness bonus — all six auth gestures must NOT have reheated auth_db.
    state_after_auth = _get_db_state(cursor_default, "auth_db")
    assert state_after_auth == "cold", (
        f"Auth gestures (GRANT/SET MAIN/SHOW DATABASE PRIVILEGES/REVOKE/"
        f"SHOW PRIVILEGES ON DATABASE/SHOW ROLE ON DATABASE) must not reheat a COLD tenant, "
        f"but auth_db state is '{state_after_auth}' after the auth operations."
    )

    # Step 8: cleanup.
    conn_default.close()


# ---------------------------------------------------------------------------
# T12 — HA promotion correctly resumes a COLD tenant
#        (guards DoToMainPromotion -> ResumeColdTenantsForPromotion)
# ---------------------------------------------------------------------------


def test_t12_promotion_resumes_cold_tenant(test_name):
    """
    Prove that promoting a replica to MAIN via ``SET REPLICATION ROLE TO MAIN``
    correctly resumes any COLD (suspended) tenant so it becomes a fully
    functional HOT tenant on the new MAIN.

    Background:
      ``DoToMainPromotion`` (replication_handler.cpp) updates each DB's epoch,
      commit timestamp, and TTL via a ``ForEach`` loop.  ``ForEach`` skips COLD
      tenants (their in-memory accessor is absent).  A COLD tenant on the promoted
      replica therefore kept its pre-promotion epoch and stayed permanently COLD.

      The fix calls ``dbms_handler_.ResumeColdTenantsForPromotion()`` *before* the
      epoch-update loops, so every COLD tenant is synchronously resumed (becomes
      HOT) and then receives the correct new epoch as part of
      ``SET REPLICATION ROLE TO MAIN``.

      Pre-fix: tenant1 stays 'cold' after promotion (epoch not updated, inaccessible).
      Post-fix: tenant1 transitions to 'ready' after promotion (data intact, writable).

    Cluster setup:
      Reuses the same 2-instance description as T9 (``instance_description_replication``):
        - main on bolt 7687, replica_1 on bolt 7688, replication port 10001.
        - Both instances carry the hot-cold experimental args.
        - Replication mode: SYNC.

    Steps:
      1. Start both instances.
      2. Create tenant1 on main (replicated to replica).
      3. Write 5 nodes into tenant1 on main.
      4. Poll until tenant1 has 5 nodes on the replica (bounded retry: CREATE DATABASE
         replication may lag slightly).
      5. SUSPEND tenant1 on the replica (bounded retry: replication-apply accessor may
         still be releasing); poll until State='cold'.
      6. PROMOTE the replica to MAIN: ``SET REPLICATION ROLE TO MAIN`` on the replica's
         default connection.  This invokes DoToMainPromotion.
      7. KEY ASSERTION: poll ``_wait_for_db_state(..., "ready", timeout_s=30.0)`` —
         promotion must have resumed the COLD tenant via ResumeColdTenantsForPromotion.
         Pre-fix: this assertion FAILS (tenant1 stays 'cold').
      8. DATA INTACT: fresh connection to the new main (port 7688); USE DATABASE tenant1;
         MATCH (n:N) RETURN count(n); == 5.
      9. WRITABLE AS NEW MAIN (epoch correct): write 2 more nodes (ids 6-7); verify
         count == 7.  A write failure here would indicate the epoch was not correctly
         updated (the pre-fix symptom for an epoch-stale tenant).

    Note:
      The eviction scheduler is NOT enabled in ``_HOT_COLD_ARGS_BASE``, so after
      promotion tenant1 stays HOT — the 'ready' assertion is stable.
    """
    instances = instance_description_replication(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)

    # -------------------------------------------------------------------------
    # Step 2: create tenant1 on main (replicated automatically).
    # -------------------------------------------------------------------------
    main_default = connect(host="localhost", port=_T9_BOLT_PORTS["main"])
    cursor_main_default = main_default.cursor()
    execute_and_fetch_all(cursor_main_default, "CREATE DATABASE tenant1;")

    # -------------------------------------------------------------------------
    # Step 3: write 5 nodes into tenant1 on main.
    # -------------------------------------------------------------------------
    conn_main_t1 = connect(host="localhost", port=_T9_BOLT_PORTS["main"])
    cursor_main_t1 = conn_main_t1.cursor()
    execute_and_fetch_all(cursor_main_t1, "USE DATABASE tenant1;")
    execute_and_fetch_all(cursor_main_t1, "UNWIND range(1, 5) AS i CREATE (:N {id: i});")
    conn_main_t1.close()

    # -------------------------------------------------------------------------
    # Step 4: poll until tenant1 has 5 nodes on the replica.
    # USE DATABASE tenant1 on the replica may block until the CREATE DATABASE
    # replicates; retry in a bounded loop (mirrors T9 step 4).
    # -------------------------------------------------------------------------
    REPLICA_SYNC_TIMEOUT_S = 30.0
    deadline = time.monotonic() + REPLICA_SYNC_TIMEOUT_S
    replica_count: int | None = None
    last_replica_exc: Exception | None = None

    while time.monotonic() < deadline:
        try:
            conn_replica_t1 = connect(host="localhost", port=_T9_BOLT_PORTS["replica_1"])
            cursor_replica_t1 = conn_replica_t1.cursor()
            execute_and_fetch_all(cursor_replica_t1, "USE DATABASE tenant1;")
            rows = execute_and_fetch_all(cursor_replica_t1, "MATCH (n:N) RETURN count(n);")
            replica_count = rows[0][0]
            conn_replica_t1.close()
            if replica_count == 5:
                break
        except Exception as exc:
            last_replica_exc = exc
        time.sleep(0.5)

    assert replica_count == 5, (
        f"Expected 5 nodes on replica after initial replication, got {replica_count!r}. "
        f"Last exception: {last_replica_exc!r}"
    )

    # -------------------------------------------------------------------------
    # Step 5: SUSPEND tenant1 on the replica.
    # The replication-apply accessor may still be releasing; retry with a bounded
    # loop tolerating 'active connections' errors (mirrors T9 step 5).
    # The replica's SHOW DATABASES cursor must live on the replica's default db.
    # -------------------------------------------------------------------------
    replica_default = connect(host="localhost", port=_T9_BOLT_PORTS["replica_1"])
    cursor_replica_default = replica_default.cursor()

    SUSPEND_RETRY_TIMEOUT_S = 15.0
    deadline_suspend = time.monotonic() + SUSPEND_RETRY_TIMEOUT_S
    suspended = False
    last_suspend_err = ""
    while time.monotonic() < deadline_suspend:
        try:
            result = execute_and_fetch_all(cursor_replica_default, "SUSPEND DATABASE tenant1;")
            if result and "suspended" in result[0][0].lower():
                suspended = True
                break
        except Exception as exc:
            last_suspend_err = str(exc)
            if "active connections" not in last_suspend_err.lower():
                raise
        time.sleep(0.5)

    assert suspended, (
        f"Could not SUSPEND tenant1 on replica within {SUSPEND_RETRY_TIMEOUT_S}s. " f"Last error: {last_suspend_err!r}"
    )

    # Poll until tenant1 is confirmed cold before we attempt promotion.
    became_cold = _wait_for_db_state(cursor_replica_default, "tenant1", "cold", timeout_s=30.0)
    assert became_cold, (
        f"tenant1 did not reach State='cold' on replica within 30s after SUSPEND. "
        f"Current state: {_get_db_state(cursor_replica_default, 'tenant1')!r}"
    )

    # -------------------------------------------------------------------------
    # Step 6: promote the replica to MAIN.
    # SET REPLICATION ROLE TO MAIN invokes DoToMainPromotion, which calls
    # ResumeColdTenantsForPromotion() (the fix) before updating epochs.
    # -------------------------------------------------------------------------
    execute_and_fetch_all(cursor_replica_default, "SET REPLICATION ROLE TO MAIN;")

    # -------------------------------------------------------------------------
    # Step 7: KEY ASSERTION — promotion must have resumed the COLD tenant.
    # Pre-fix: tenant1 stays 'cold' (ResumeColdTenantsForPromotion missing).
    # Post-fix: tenant1 becomes 'ready' within the promotion path.
    # -------------------------------------------------------------------------
    reheated = _wait_for_db_state(cursor_replica_default, "tenant1", "ready", timeout_s=30.0)
    assert reheated, (
        "promotion must resume the cold tenant (ResumeColdTenantsForPromotion); "
        "pre-fix it stays cold. "
        f"Current state after SET REPLICATION ROLE TO MAIN: "
        f"{_get_db_state(cursor_replica_default, 'tenant1')!r}"
    )

    # -------------------------------------------------------------------------
    # Step 8: data intact on the new main.
    # A fresh connection to the former replica (now MAIN, port 7688).
    # -------------------------------------------------------------------------
    VERIFY_TIMEOUT_S = 30.0
    deadline_verify = time.monotonic() + VERIFY_TIMEOUT_S
    count_after_promotion: int | None = None
    last_verify_exc: Exception | None = None
    conn_new_main: mgclient.Connection | None = None

    while time.monotonic() < deadline_verify:
        try:
            conn_new_main = connect(host="localhost", port=_T9_BOLT_PORTS["replica_1"])
            cursor_new_main = conn_new_main.cursor()
            execute_and_fetch_all(cursor_new_main, "USE DATABASE tenant1;")
            rows = execute_and_fetch_all(cursor_new_main, "MATCH (n:N) RETURN count(n);")
            count_after_promotion = rows[0][0]
            break
        except Exception as exc:
            last_verify_exc = exc
            if conn_new_main is not None:
                try:
                    conn_new_main.close()
                except Exception:
                    pass
                conn_new_main = None
        time.sleep(0.5)

    assert count_after_promotion == 5, (
        f"Expected 5 nodes on the new main after promotion, got {count_after_promotion!r}. "
        f"Last exception: {last_verify_exc!r}"
    )

    # -------------------------------------------------------------------------
    # Step 9: writable as the new MAIN (epoch correct).
    # Write 2 more nodes (ids 6-7); a write failure proves the epoch was stale
    # (the pre-fix symptom for an epoch-not-updated tenant after ForEach skip).
    # -------------------------------------------------------------------------
    assert conn_new_main is not None, "new-main connection should still be open from step 8"
    execute_and_fetch_all(cursor_new_main, "UNWIND range(6, 7) AS i CREATE (:N {id: i});")
    rows_final = execute_and_fetch_all(cursor_new_main, "MATCH (n:N) RETURN count(n);")
    assert rows_final[0][0] == 7, (
        f"Expected 7 nodes after writing 2 more on the promoted MAIN, got {rows_final[0][0]}. "
        "A write failure or wrong count indicates the epoch was not correctly updated "
        "during promotion (pre-fix symptom: ForEach skipped the cold tenant)."
    )
    conn_new_main.close()


# ---------------------------------------------------------------------------
# T13 — a COLD tenant stays COLD across a process restart (cross-restart
#       persistence), then reheats lazily with its data intact.
# ---------------------------------------------------------------------------


def test_t13_cold_tenant_survives_restart(test_name):
    """
    Suspend a tenant, restart the instance (keeping its data directory), and
    verify the tenant comes back COLD (a lazy shell — NOT rebuilt hot), then
    reheats on first access with its data intact.

    Validates the cross-restart COLD-persistence feature:
    - SUSPEND persists a COLD marker in the durability KVStore.
    - After a restart, SHOW DATABASES reports the tenant as 'cold' (it was NOT
      rebuilt HOT at startup — the whole point of the feature: zero RAM/replay
      for a tenant that was evicted before the restart).
    - USE DATABASE block-and-resumes the restored COLD shell, and the data
      written before the suspend+restart survived.
    """
    instances = instance_description(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)

    # Create + populate db1.
    conn_default = connect(host="localhost", port=7687)
    cursor_default = conn_default.cursor()
    execute_and_fetch_all(cursor_default, "CREATE DATABASE db1")

    conn_work = connect(host="localhost", port=7687)
    cursor_work = conn_work.cursor()
    execute_and_fetch_all(cursor_work, "USE DATABASE db1")
    for _ in range(5):
        execute_and_fetch_all(cursor_work, "CREATE (:Node)")
    count_before = execute_and_fetch_all(cursor_work, "MATCH (n) RETURN count(n) AS c")
    assert count_before[0][0] == 5, f"Expected 5 nodes before suspend, got {count_before[0][0]}"
    # Release the connection-scoped accessor so SUSPEND is not blocked by ACTIVE_CONNECTIONS.
    conn_work.close()

    # SUSPEND -> COLD (persists the COLD marker durably).
    execute_and_fetch_all(cursor_default, "SUSPEND DATABASE db1")
    assert _get_db_state(cursor_default, "db1") == "cold", "db1 should be COLD after SUSPEND"
    conn_default.close()

    # --- RESTART the instance, keeping its data directory. ---
    interactive_mg_runner.stop_all(keep_directories=True)
    interactive_mg_runner.start_all(instances, keep_directories=True)

    conn_after = connect(host="localhost", port=7687)
    cursor_after = conn_after.cursor()

    # The tenant must recover COLD — NOT rebuilt HOT. SHOW DATABASES does not reheat, so a 'cold'
    # state here proves the COLD marker persisted and the tenant came back as a lazy shell.
    state_after_restart = _get_db_state(cursor_after, "db1")
    assert state_after_restart == "cold", (
        f"After restart db1 must recover COLD (lazy shell), got '{state_after_restart}'. "
        "A 'ready' here means the tenant was rebuilt HOT at startup — cross-restart persistence broke."
    )

    # Lazily reheat via USE DATABASE (block-and-resume) and confirm the data survived.
    conn_verify = connect(host="localhost", port=7687)
    cursor_verify = conn_verify.cursor()
    execute_and_fetch_all(cursor_verify, "USE DATABASE db1")
    count_after = execute_and_fetch_all(cursor_verify, "MATCH (n) RETURN count(n) AS c")
    assert count_after[0][0] == 5, (
        f"Expected 5 nodes after restart+resume, got {count_after[0][0]} "
        "(data loss across the cross-restart COLD recovery path)"
    )

    # And the tenant is now back to 'ready'.
    assert _wait_for_db_state(
        cursor_after, "db1", "ready", timeout_s=15.0
    ), "db1 did not return to 'ready' after the post-restart lazy resume"
    conn_verify.close()
    conn_after.close()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
