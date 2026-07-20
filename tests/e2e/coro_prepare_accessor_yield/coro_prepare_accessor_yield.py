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

import sys

import common
import pytest


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
    p_elapsed, _ = common.run_responsiveness_scenario()
    print(
        f"[flag-on] P (RETURN 1) latency under contention: {p_elapsed:.3f}s "
        f"(pass threshold: < {common.RESPONSIVE_THRESHOLD_SECONDS}s)"
    )
    assert p_elapsed < common.RESPONSIVE_THRESHOLD_SECONDS, (
        f"RETURN 1 took {p_elapsed:.3f}s while every bolt worker was pinned by conflicting "
        f"CREATE INDEX contenders (parking ON) -- expected < {common.RESPONSIVE_THRESHOLD_SECONDS}s. "
        "Workers should have been parked and freed instead of blocking."
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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-s"]))
