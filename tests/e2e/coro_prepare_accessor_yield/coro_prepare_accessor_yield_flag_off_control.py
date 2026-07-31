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

# CONTROL for the coroutine-park Prepare path e2e (coro_prepare_accessor_yield.py). Runs the
# IDENTICAL scenario (common.run_responsiveness_scenario) against a cluster where
# --experimental-coro-prepare-accessor-yield is absent (default false), and asserts the OPPOSITE
# outcome: the probe must be SLOW.
#
# Why this file exists: a "P is fast" assertion alone proves nothing unless we also demonstrate
# that the same scenario, without parking, makes P slow. Without this contrast, a fast box, a
# scenario that never actually contends, or parking being silently broken could all produce a
# passing flag-ON test for the wrong reason. This file is what makes the flag-ON <1s result
# meaningful.

import glob
import os
import sys
import time

import common
import mgclient
import pytest


@pytest.fixture(autouse=True)
def clean_database():
    common.clean_database()
    yield
    common.clean_database()


def test_flag_off_control_probe_stays_blocked():
    """Control assertion: with parking off, NUM_CONTENDERS (== --bolt-num-workers + 1, saturating
    every LP pool worker plus the one always-present HP thread -- see common.py's module
    docstring for why the +1 is required) conflicting CREATE INDEX statements block their thread
    in try_lock_for instead of parking it, so every thread stays pinned until the WRITE holder
    releases -- an unrelated RETURN 1 must observe HIGH latency here. If this assertion fails (P
    comes back fast even with the flag off), the scenario is not actually discriminating and the
    flag-ON pass in coro_prepare_accessor_yield.py would not mean what it claims to mean."""
    p_elapsed, _ = common.run_responsiveness_scenario()
    print(
        f"[flag-off-control] P (RETURN 1) latency under contention: {p_elapsed:.3f}s "
        f"(pass threshold: > {common.BLOCKED_THRESHOLD_SECONDS}s)"
    )
    assert p_elapsed > common.BLOCKED_THRESHOLD_SECONDS, (
        f"RETURN 1 took only {p_elapsed:.3f}s while every bolt worker should have been pinned "
        f"by conflicting CREATE INDEX contenders (parking OFF) -- expected > "
        f"{common.BLOCKED_THRESHOLD_SECONDS}s. Either the scenario failed to contend the pool, "
        "or parking is engaging even with the flag off."
    )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-s"]))


# --- Control half of the Prepare-phase observability pair ------------------------------
#
# Same query, flag OFF: Prepare runs inline inside Session::Execute_, under the per-message
# ScopedSessionLog it installs, so the [failed-query] line is emitted with no help from anyone. This
# is the baseline the flag-on arm must match -- without it, that arm could be passing for reasons
# unrelated to the guard (e.g. if the log line came from some earlier, non-Prepare path).

LOG_GLOB = os.path.join(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")),
    "e2e",
    "logs",
    "coro_prepare_accessor_yield_flag_off_control_2*.log",
)


def _active_log_path():
    candidates = glob.glob(LOG_GLOB)
    assert candidates, f"memgraph did not create a log file matching {LOG_GLOB}"
    return max(candidates, key=os.path.getmtime)


def _read_appended(log_path, start_offset, *, expect, timeout=5.0):
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


def test_flag_off_control_prepare_failure_logs_failed_query():
    """Baseline: the same Prepare-phase failure logs [failed-query] on the ordinary inline path."""
    log_path = _active_log_path()
    start = os.path.getsize(log_path)

    conn = mgclient.connect(host="localhost", port=7687)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute('SET SESSION SETTING "log.failed_queries" TO "true"')
    try:
        cur.fetchall()
    except mgclient.DatabaseError:
        pass

    marker = "coro_prepare_failed_marker"
    try:
        cur.execute(f"RETURN {marker}")  # same query as the flag-on arm; see its docstring
        cur.fetchall()
    except mgclient.DatabaseError:
        pass

    content = _read_appended(log_path, start, expect=["[failed-query]", marker])
    relevant = [line for line in content.splitlines() if "[failed-query]" in line and marker in line]
    assert relevant, f"baseline (flag-off) failed to log a Prepare-phase failure; got: {content[-2000:]!r}"
