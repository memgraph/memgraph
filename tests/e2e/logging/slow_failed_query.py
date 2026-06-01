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

import glob
import os
import re
import sys
import time

import mgclient
import pytest

_BUILD_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
LOG_GLOB = os.path.join(_BUILD_DIR, "e2e", "logs", "slow_failed_query*.log")


def _active_log_path():
    candidates = glob.glob(LOG_GLOB)
    assert candidates, f"memgraph did not create a log file matching {LOG_GLOB}"
    return max(candidates, key=os.path.getmtime)


def _connect():
    conn = mgclient.connect(host="localhost", port=7687)
    conn.autocommit = True
    return conn


def _run(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    try:
        return cur.fetchall()
    except mgclient.DatabaseError:
        return []


def _read_appended(log_path, start_offset, *, expect, timeout=3.0):
    deadline = time.monotonic() + timeout
    while True:
        with open(log_path, "r") as f:
            f.seek(start_offset)
            content = f.read()
        if all(s in content for s in expect):
            return content
        if time.monotonic() >= deadline:
            return content
        time.sleep(0.05)


def _read_after_settle(log_path, start_offset, settle=0.3):
    """For negative assertions: wait a short fixed window for any erroneous emit to flush, then read."""
    deadline = time.monotonic() + settle
    while time.monotonic() < deadline:
        time.sleep(0.05)
    with open(log_path, "r") as f:
        f.seek(start_offset)
        return f.read()


# --- Slow-query log -------------------------------------------------------------------


def test_slow_query_logged_when_threshold_zero():
    """min_duration_ms=0 logs every successful query as Postgres's auto_explain does."""
    log_path = _active_log_path()
    start = os.path.getsize(log_path)

    conn = _connect()
    _run(conn, 'SET SESSION SETTING "log.min_duration_ms" TO "0"')
    _run(conn, "RETURN 'slow_marker_zero'")

    content = _read_appended(log_path, start, expect=["slow_marker_zero", "[slow-query]"])
    assert "[slow-query]" in content
    assert "slow_marker_zero" in content


def test_slow_query_skipped_when_threshold_disabled():
    """min_duration_ms=-1 means off; even a long query must not emit a [slow-query] line."""
    log_path = _active_log_path()
    start = os.path.getsize(log_path)

    conn = _connect()
    _run(conn, 'SET SESSION SETTING "log.min_duration_ms" TO "-1"')
    _run(conn, "RETURN 'slow_marker_off'")

    content = _read_after_settle(log_path, start)
    relevant = [line for line in content.splitlines() if "slow_marker_off" in line]
    for line in relevant:
        assert "[slow-query]" not in line


def test_slow_query_skipped_when_below_positive_threshold():
    """A positive threshold logs only queries that exceed it; a trivial query stays under."""
    log_path = _active_log_path()
    start = os.path.getsize(log_path)

    conn = _connect()
    # 10 minutes: no trivial RETURN will ever cross this, exercising the >0 gate path.
    _run(conn, 'SET SESSION SETTING "log.min_duration_ms" TO "600000"')
    _run(conn, "RETURN 'slow_marker_under'")

    content = _read_after_settle(log_path, start)
    relevant = [line for line in content.splitlines() if "slow_marker_under" in line]
    for line in relevant:
        assert "[slow-query]" not in line, f"fast query must not cross a 600s threshold; got: {line!r}"


def test_slow_query_plan_block_inclusion_gated_by_log_query_plan():
    """log.query_plan=false omits the PLAN: block but still emits the header line."""
    log_path = _active_log_path()
    start = os.path.getsize(log_path)

    conn = _connect()
    _run(conn, 'SET SESSION SETTING "log.min_duration_ms" TO "0"')
    _run(conn, 'SET SESSION SETTING "log.query_plan" TO "false"')
    _run(conn, "RETURN 'slow_no_plan'")

    content = _read_appended(log_path, start, expect=["slow_no_plan", "[slow-query]"])
    relevant = [line for line in content.splitlines() if "slow_no_plan" in line and "[slow-query]" in line]
    assert relevant, f"expected a [slow-query] line containing slow_no_plan; got: {content!r}"
    # The PLAN: block follows the header on a new line; with plan=false it must not appear.
    full = "\n".join(content.splitlines())
    idx = full.find("slow_no_plan")
    # Check the next ~200 chars after the marker for an unexpected PLAN: block.
    assert "PLAN:" not in full[idx : idx + 200]


# --- Failed-query log -----------------------------------------------------------------


def test_failed_query_logged_when_enabled():
    """log.failed_queries=true emits a [failed-query] line for parse failures."""
    log_path = _active_log_path()
    start = os.path.getsize(log_path)

    conn = _connect()
    _run(conn, 'SET SESSION SETTING "log.failed_queries" TO "true"')
    try:
        _run(conn, "THIS IS NOT VALID CYPHER")
    except mgclient.DatabaseError:
        pass

    content = _read_appended(log_path, start, expect=["[failed-query]"])
    assert "[failed-query]" in content
    assert "THIS IS NOT VALID CYPHER" in content


def test_failed_query_off_emits_nothing():
    log_path = _active_log_path()
    start = os.path.getsize(log_path)

    conn = _connect()
    _run(conn, 'SET SESSION SETTING "log.failed_queries" TO "false"')
    try:
        _run(conn, "MATCH (n) RETURN n.unknown_function_xyz()")
    except mgclient.DatabaseError:
        pass

    content = _read_after_settle(log_path, start)
    relevant = [line for line in content.splitlines() if "unknown_function_xyz" in line]
    for line in relevant:
        assert "[failed-query]" not in line


def test_failed_query_logged_for_runtime_error():
    """A query that parses and plans but throws during Pull is logged with its query text."""
    log_path = _active_log_path()
    start = os.path.getsize(log_path)

    conn = _connect()
    _run(conn, 'SET SESSION SETTING "log.failed_queries" TO "true"')
    try:
        # Division by zero on the second row: a genuine execution-time (Pull) failure.
        _run(conn, "UNWIND [1, 0] AS denom_marker RETURN 1 / denom_marker")
    except mgclient.DatabaseError:
        pass

    content = _read_appended(log_path, start, expect=["[failed-query]", "denom_marker"])
    relevant = [line for line in content.splitlines() if "[failed-query]" in line and "denom_marker" in line]
    assert relevant, f"runtime failure must log a [failed-query] line carrying the query text; got: {content!r}"


def test_failed_query_logged_on_commit_failure():
    """A query that executes fine but fails at commit must still log its (non-empty) query text.

    Regression: the query string is moved out before Commit(); the failed-query log must fall
    back to the captured copy so commit-time failures don't emit query=\"\".
    """
    log_path = _active_log_path()

    conn = _connect()
    _run(conn, "CREATE CONSTRAINT ON (n:CommitFail) ASSERT n.id IS UNIQUE")
    _run(conn, "CREATE (:CommitFail {id: 'commit_dup_marker'})")

    start = os.path.getsize(log_path)
    _run(conn, 'SET SESSION SETTING "log.failed_queries" TO "true"')
    try:
        _run(conn, "CREATE (:CommitFail {id: 'commit_dup_marker'})")
    except mgclient.DatabaseError:
        pass

    content = _read_appended(log_path, start, expect=["[failed-query]"])
    relevant = [line for line in content.splitlines() if "[failed-query]" in line and "commit_dup_marker" in line]
    assert relevant, f'commit-time failure must log the query text, not query=""; got: {content!r}'


# --- Session isolation ----------------------------------------------------------------


def test_session_overlay_is_isolated_between_connections():
    """One session enables slow logging; a parallel session at default must not emit."""
    log_path = _active_log_path()
    start = os.path.getsize(log_path)

    a = _connect()
    b = _connect()
    _run(a, 'SET SESSION SETTING "log.min_duration_ms" TO "0"')
    # b stays at the default (-1) — global flag is off.

    _run(a, "RETURN 'iso_a'")
    _run(b, "RETURN 'iso_b'")

    content = _read_appended(log_path, start, expect=["iso_a"])
    relevant_a = [line for line in content.splitlines() if "iso_a" in line and "[slow-query]" in line]
    relevant_b = [line for line in content.splitlines() if "iso_b" in line and "[slow-query]" in line]
    assert relevant_a, "session A opted in — its query should be slow-logged"
    assert not relevant_b, "session B did not opt in — must not appear in slow-query stream"


def test_reset_session_setting_reverts_to_global_default():
    log_path = _active_log_path()
    start = os.path.getsize(log_path)

    conn = _connect()
    _run(conn, 'SET SESSION SETTING "log.min_duration_ms" TO "0"')
    _run(conn, "RETURN 'before_reset'")

    _run(conn, 'RESET SESSION SETTING "log.min_duration_ms"')
    _run(conn, "RETURN 'after_reset'")

    content = _read_appended(log_path, start, expect=["before_reset"])
    relevant_before = [line for line in content.splitlines() if "before_reset" in line and "[slow-query]" in line]
    relevant_after = [line for line in content.splitlines() if "after_reset" in line and "[slow-query]" in line]
    assert relevant_before, "before RESET the session opted in — its query should be slow-logged"
    assert not relevant_after, "after RESET the session must follow the (disabled) global default"


# --- Allow-list -----------------------------------------------------------------------


def test_unknown_setting_key_is_rejected():
    conn = _connect()
    with pytest.raises(mgclient.DatabaseError) as exc:
        _run(conn, 'SET SESSION SETTING "nonexistent.key" TO "1"')
    assert "cannot be set per-session" in str(exc.value)


def test_log_level_not_session_overridable():
    """log.level was deliberately rejected as a per-session setting in the trace refactor."""
    conn = _connect()
    with pytest.raises(mgclient.DatabaseError) as exc:
        _run(conn, 'SET SESSION SETTING "log.level" TO "DEBUG"')
    assert "cannot be set per-session" in str(exc.value)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
