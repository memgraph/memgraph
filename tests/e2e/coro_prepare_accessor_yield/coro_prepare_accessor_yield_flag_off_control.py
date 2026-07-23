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

# Control for the flag-ON e2e: runs the IDENTICAL scenario with the flag absent (default false) and
# asserts the OPPOSITE outcome -- the probe must be SLOW. This contrast is what makes the flag-ON
# <1s result meaningful (a fast box or a non-contending scenario could otherwise pass for the
# wrong reason).

import sys

import common
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
