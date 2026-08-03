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

# e2e for the coroutine-park Prepare path (--experimental-coro-prepare-accessor-yield=true),
# Session-surgery Stage B. See common.py for the full mechanism writeup and the shared scenario
# helpers.
#
# This is the DISCRIMINATING half of the test: the assertion below must FAIL if parking is
# broken (or silently falls back to plain blocking) and can only pass under genuine parking. Its
# contrast/control counterpart, run against a cluster where the flag is off, lives in
# coro_prepare_accessor_yield_flag_off_control.py and asserts the opposite outcome on the exact
# same scenario -- see that file for why the contrast matters.

import os
import sys
import time

import common
import mgclient
import pytest

# Log file stem for this arm; its workloads.yaml entry sets the matching --log-file.
LOG_STEM = "coro_prepare_accessor_yield"


@pytest.fixture(autouse=True)
def clean_database():
    common.clean_database()
    yield
    common.clean_database()


def test_flag_on_park_keeps_probe_responsive():
    """The discriminating assertion. With NUM_CONTENDERS (== --bolt-num-workers + 1, saturating
    every LP pool worker plus the one always-present HP thread -- see common.py's module
    docstring for why the +1 is required) conflicting CREATE INDEX statements pinning every
    thread, a completely unrelated RETURN 1 must still come back fast -- proving the contended
    threads were parked and freed, not blocked in try_lock_for for the whole HOLD_SECONDS. This
    assertion fails outright under plain blocking: see test_flag_off_control_probe_stays_blocked
    in the control file, which reproduces the identical scenario with the flag off and asserts
    the opposite (high latency)."""
    p_elapsed, contenders = common.run_responsiveness_scenario()
    print(
        f"[flag-on] P (RETURN 1) latency under contention: {p_elapsed:.3f}s "
        f"(pass threshold: < {common.RESPONSIVE_THRESHOLD_SECONDS}s)"
    )
    assert p_elapsed < common.RESPONSIVE_THRESHOLD_SECONDS, (
        f"RETURN 1 took {p_elapsed:.3f}s while every bolt worker was pinned by conflicting "
        f"CREATE INDEX contenders (parking ON) -- expected < {common.RESPONSIVE_THRESHOLD_SECONDS}s. "
        "Workers should have been parked and freed instead of blocking."
    )

    # Second, independent assertion on the SAME run: the parked contenders must actually get the
    # lock, not merely fail to hang. Flag-on they park, which frees the workers H's own COMMIT needs
    # to dispatch, so H releases EARLY and its WRITE release wakes them (F5) -- observed ~1.5s, i.e.
    # well inside TIMEOUT_SEC and even below HOLD_SECONDS. Flag-off the same contenders ride out the
    # timeout (observed 6.0s), so this assertion is what separates the two states on outcome rather
    # than only on the probe's latency.
    # An access-timeout here is the signature of a campaign that woke but could no longer acquire,
    # which is what a consumed/dead pending handle produces (the abandon re-probe must use plain
    # TryAccess, never TryAccessWithPending -- see AcquireAwaitable). Deliberately asserted only in
    # the flag-ON test: flag-off these same contenders ride out the timeout by design, which is why
    # run_responsiveness_scenario() itself stays neutral about contender outcomes.
    timed_out = {i: r for i, r in contenders.items() if r["error"] and "access to the storage" in r["error"]}
    assert not timed_out, (
        "parked contenders hit the storage access timeout instead of acquiring after the holder "
        f"released: { {i: round(r['elapsed'], 3) for i, r in timed_out.items()} } "
        f"(HOLD_SECONDS={common.HOLD_SECONDS}s, TIMEOUT_SEC={common.TIMEOUT_SEC}s). Expected each to "
        "succeed at ~HOLD_SECONDS. If elapsed is ~TIMEOUT_SEC this is a woken-but-cannot-acquire "
        "regression; if elapsed is only marginally above HOLD_SECONDS the scenario's 1s margin has "
        "become too tight and the constants need widening, not the code."
    )


def test_flag_on_timeout_semantics_preserved():
    """Parking changes HOW a contended accessor acquire waits, not the eventual outcome: a
    CREATE INDEX that genuinely cannot acquire within --storage-access-timeout-sec must still
    fail with the same ReadOnlyAccessTimeout message, at roughly the configured timeout -- not
    hang indefinitely, not succeed early, and not raise a different error."""
    result = common.run_timeout_preserved_scenario()
    print(f"[flag-on] timeout-preserved contender elapsed: {result['elapsed']:.3f}s, error: {result['error']!r}")
    assert result["error"] is not None, "CREATE INDEX should have timed out while the WRITE accessor was held"
    assert result["error"].startswith(
        "Cannot get read-only access to the storage."
    ), f"unexpected error message: {result['error']}"
    assert result["elapsed"] >= common.TIMEOUT_SEC * 0.5, f"timeout fired implausibly fast ({result['elapsed']:.2f}s)"
    assert (
        result["elapsed"] < common.TIMEOUT_PRESERVED_HOLD_SECONDS
    ), f"timeout should fire before the holder releases ({result['elapsed']:.2f}s)"


def test_flag_on_accessor_timeout_logs_the_query_text():
    """A Prepare-phase failure BEFORE the accessor is acquired must still log its query text.

    This is the complement of the test above, and it covers the hole that one could not: PrepareCoro
    creates its QueryExecution in Phase 3, i.e. AFTER the acquire, so a pre-acquire failure reaches
    HandlePrepareFailure with no QueryExecution to read the query string from. The *AccessTimeout this
    feature exists to handle is exactly such a failure, so flag-on it logged [failed-query] with
    query="" while the identical failure flag-off logged the real text.

    Deliberately an accessor TIMEOUT and not a planning error: a planning error happens in
    PlanAndFinalize, which is past Phase 3, where the QueryExecution already exists -- so it would
    pass either way.
    """
    log_path = common.active_log_path(LOG_STEM)
    start = os.path.getsize(log_path)

    # No SET SESSION SETTING here: log.failed_queries is SESSION-scoped, and the scenario helper opens
    # its own connections, so a setting applied here would not reach the query that times out. The
    # cluster runs with --log-failed-queries=true instead (workloads.yaml).
    #
    # Same scenario as the timeout test: a held WRITE accessor makes CREATE INDEX time out.
    result = common.run_timeout_preserved_scenario()
    assert result["error"] is not None, "scenario did not time out, so nothing was logged to check"

    content = common.read_appended(log_path, start, expect=["[failed-query]"])
    failed = [line for line in content.splitlines() if "[failed-query]" in line]
    assert failed, f"an accessor timeout emitted no [failed-query] line at all; got: {content[-2000:]!r}"
    with_text = [line for line in failed if "CREATE INDEX" in line]
    assert with_text, (
        "the [failed-query] line for an accessor timeout carries no query text -- pre-acquire failures "
        f"lose it, while flag-off keeps it. lines were: {failed!r}"
    )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-s"]))


# --- Prepare-phase observability under the coro path -----------------------------------
#
# The flag moves the whole Prepare phase off the Bolt dispatcher: Session::Execute_ returns
# kNeedsCoroPrepare and destroys its per-message ScopedSessionLog before any Prepare work runs, so
# everything TLS-gated downstream -- session-trace events, and the [failed-query] log via
# MaybeEmitFailedQueryLog's "TLS guard absent => no bolt message is in flight" gate -- goes silent
# unless PrepareCoro re-installs the guard for its own non-suspending regions.
#
# That regression is invisible from the client (the query still fails correctly, the operator just
# stops being told why), and it lands on precisely the contended workloads this flag exists for.
# The flag-off control asserts the same query DOES log, so this pair pins the contract "flag-on must
# not cost diagnostics" rather than merely "some log line exists".


def test_flag_on_prepare_failure_still_logs_failed_query():
    """A Prepare-phase failure must still emit its [failed-query] line under the coro path.

    The query matters, and most obvious candidates do NOT test this. Measured against a build with
    the guard deliberately removed, these all still logged, because their failure never enters the
    guard-less region: a parse error ("THIS IS NOT VALID CYPHER") and an unknown function or
    procedure all fail while building the AST, before Execute_ returns kNeedsCoroPrepare; a
    division-by-zero fails during Pull, which Execute_ still handles inline under its own guard.

    An UNBOUND VARIABLE is different: it survives parsing and fails in symbol resolution inside
    PlanAndFinalize -- i.e. inside PrepareCoro's try, on a pool worker, after Execute_'s guard is
    gone -- and it fails late enough that query_execution exists, so the log line carries the query
    text. On the guard-less build it emitted nothing at all. Do not "simplify" this to a parse error.
    """
    log_path = common.active_log_path(LOG_STEM)
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
        cur.execute(f"RETURN {marker}")  # unbound variable -> fails in PlanAndFinalize
        cur.fetchall()
    except mgclient.DatabaseError:
        pass  # expected: the query must still fail for the client

    content = common.read_appended(log_path, start, expect=["[failed-query]", marker])
    relevant = [line for line in content.splitlines() if "[failed-query]" in line and marker in line]
    assert relevant, (
        "a Prepare-phase failure on the coro path emitted no [failed-query] line carrying the query "
        f"text -- Prepare ran without a session-log guard. got: {content[-2000:]!r}"
    )
