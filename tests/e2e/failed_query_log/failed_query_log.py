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
import uuid

import pytest
from common import memgraph

# Must match the directory passed via --failed-query-log-directory in workloads.yaml.
FAILED_QUERY_LOG_DIR = "/tmp/mg_failed_query_log_test"


def _read_failed_query_log() -> str:
    """Read all failed-query log files. Returns '' if none exist yet.

    Note: we do not clean the directory between runs. Each test uses a unique
    marker string so previous-run residue cannot cause false positives, and the
    daily-rotation sink already opened its file at memgraph startup -- deleting
    it from the test would orphan the inode and silently drop log writes.
    """
    contents = ""
    for f in sorted(glob.glob(os.path.join(FAILED_QUERY_LOG_DIR, "failed_queries*.log"))):
        with open(f) as fh:
            contents += fh.read()
    return contents


def test_parse_failure_is_logged(memgraph):
    marker = uuid.uuid4().hex
    bad_query = f"RTRN {marker}"
    with pytest.raises(Exception):
        list(memgraph.execute_and_fetch(bad_query))

    # spdlog has flush_on(info), so no sleep is needed.
    log = _read_failed_query_log()
    assert marker in log, f"Expected marker {marker} in log:\n{log}"
    assert "error:" in log.lower()
    # Format sanity: session_uuid, username, db tags should appear on the line.
    line = next(l for l in log.splitlines() if marker in l)
    assert line.count("[") >= 3, f"Expected [uuid] [user] [db] tags on line:\n{line}"


def test_semantic_failure_is_logged(memgraph):
    # Syntactically valid but fails semantic analysis (different code path
    # than parse failures: hits the prepare-time catch).
    marker = uuid.uuid4().hex
    bad_query = f"RETURN unbound_var_{marker}"
    with pytest.raises(Exception):
        list(memgraph.execute_and_fetch(bad_query))

    log = _read_failed_query_log()
    assert marker in log, f"Expected marker {marker} in log:\n{log}"
    assert "unbound" in log.lower(), f"Expected 'unbound' error in log:\n{log}"


def test_successful_query_not_logged(memgraph):
    marker = uuid.uuid4().hex
    list(memgraph.execute_and_fetch(f"RETURN '{marker}' AS m"))

    log = _read_failed_query_log()
    assert marker not in log, f"Successful-query marker leaked into failed log:\n{log}"


def test_runtime_disable_via_setting(memgraph):
    # Disable logging by setting the directory to '' at runtime, fail a query,
    # confirm nothing was written. Then restore and confirm it works again.
    list(memgraph.execute_and_fetch("SET DATABASE SETTING 'failed-query-log-directory' TO ''"))

    silent_marker = uuid.uuid4().hex
    with pytest.raises(Exception):
        list(memgraph.execute_and_fetch(f"RTRN {silent_marker}"))

    log = _read_failed_query_log()
    assert silent_marker not in log, f"Failed query was logged despite disabled setting:\n{log}"

    list(memgraph.execute_and_fetch(f"SET DATABASE SETTING 'failed-query-log-directory' TO '{FAILED_QUERY_LOG_DIR}'"))

    audible_marker = uuid.uuid4().hex
    with pytest.raises(Exception):
        list(memgraph.execute_and_fetch(f"RTRN {audible_marker}"))

    log = _read_failed_query_log()
    assert audible_marker in log, f"Failed query not logged after re-enabling setting:\n{log}"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
