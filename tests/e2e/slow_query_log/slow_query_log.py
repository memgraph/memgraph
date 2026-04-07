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
import sys
import time

import pytest
from common import memgraph

SLOW_QUERY_LOG_DIR = "/tmp/mg_slow_query_log_test"


def _read_slow_query_log():
    """Read the contents of the slow query log file."""
    pattern = os.path.join(SLOW_QUERY_LOG_DIR, "slow_queries*.log")
    files = glob.glob(pattern)
    assert len(files) > 0, f"No slow query log file found in {SLOW_QUERY_LOG_DIR}"
    contents = ""
    for f in files:
        with open(f, "r") as fh:
            contents += fh.read()
    return contents


def test_slow_cartesian_product_is_logged(memgraph):
    """Run a cartesian product via UNWIND, verify it appears in the slow query log."""

    # Cartesian product: 1000 x 1000 = 1M combinations, should exceed 1000ms threshold
    slow_query = "UNWIND range(1, 1000) AS a UNWIND range(1, 1000) AS b RETURN count(*)"
    list(memgraph.execute_and_fetch(slow_query))

    # Give spdlog a moment to flush
    time.sleep(2)

    log_contents = _read_slow_query_log()
    assert slow_query in log_contents, f"Expected slow query in log but got:\n{log_contents}"
    assert "duration:" in log_contents, f"Expected duration in log but got:\n{log_contents}"


def test_fast_query_not_logged(memgraph):
    """Run a fast query and verify it does NOT appear in the slow query log."""
    fast_query = "RETURN 1 AS n"
    list(memgraph.execute_and_fetch(fast_query))

    time.sleep(1)

    log_contents = _read_slow_query_log()
    assert fast_query not in log_contents, f"Fast query should not be in slow query log:\n{log_contents}"


def test_threshold_zero_disables_logging(memgraph):
    """Set threshold to 0, run a slow query, verify nothing new is appended."""
    # Record current log size
    log_before = _read_slow_query_log()

    # Disable slow query logging at runtime
    memgraph.execute('SET DATABASE SETTING "slow-query-log-threshold-ms" TO "0";')

    # Run the same expensive cartesian product
    slow_query = "UNWIND range(1, 1000) AS a UNWIND range(1, 1000) AS b RETURN count(*)"
    list(memgraph.execute_and_fetch(slow_query))

    time.sleep(2)

    log_after = _read_slow_query_log()
    assert (
        log_after == log_before
    ), f"No new entries should be logged when threshold is 0.\nBefore:\n{log_before}\nAfter:\n{log_after}"

    # Restore threshold for other tests
    memgraph.execute('SET DATABASE SETTING "slow-query-log-threshold-ms" TO "1000";')


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
