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

FAILED_QUERY_LOG_DIR = "/tmp/mg_failed_query_log_test"


def _read_failed_query_log():
    """Read the contents of the failed query log file."""
    pattern = os.path.join(FAILED_QUERY_LOG_DIR, "failed_queries*.log")
    files = glob.glob(pattern)
    assert len(files) > 0, f"No failed query log file found in {FAILED_QUERY_LOG_DIR}"
    contents = ""
    for f in files:
        with open(f, "r") as fh:
            contents += fh.read()
    return contents


def test_failed_query_is_logged(memgraph):
    """Run a syntactically invalid query and verify it appears in the failed query log."""
    bad_query = "RTRN 1"
    try:
        list(memgraph.execute_and_fetch(bad_query))
    except Exception:
        pass  # Expected to fail

    # Give spdlog a moment to flush
    time.sleep(1)

    log_contents = _read_failed_query_log()
    assert bad_query in log_contents, f"Expected bad query in log but got:\n{log_contents}"
    assert "error:" in log_contents.lower(), f"Expected error message in log but got:\n{log_contents}"


def test_successful_query_not_logged(memgraph):
    """Run a valid query and verify it does NOT appear in the failed query log."""
    good_query = "RETURN 1 AS n"
    list(memgraph.execute_and_fetch(good_query))

    time.sleep(1)

    log_contents = _read_failed_query_log()
    assert good_query not in log_contents, f"Successful query should not be in failed query log:\n{log_contents}"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
