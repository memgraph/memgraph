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

# The harness writes the log under <build>/e2e/logs/, dated by spdlog's daily_file_sink.
# Pick the newest match — older dated files from past runs may linger.
_BUILD_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
LOG_GLOB = os.path.join(_BUILD_DIR, "e2e", "logs", "session_trace*.log")

SESSION_TAG = re.compile(r"\[session=([^\]]+)\]")


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


def _enable_trace(conn):
    # The handler returns this session's uuid (result header: "session uuid").
    return _run(conn, "SET SESSION TRACE ON")[0][0]


def _read_appended(log_path, start_offset, *, expect, timeout=2.0):
    """Poll the log past `start_offset` until every string in `expect` is present, or `timeout`
    elapses (caller's assertion then surfaces the failure). spdlog sync flushes per record and
    emits run synchronously before the bolt response, so once the last expected marker is
    visible the file is settled — any unexpected trailing emit would already be in the OS cache."""
    deadline = time.monotonic() + timeout
    while True:
        with open(log_path, "r") as f:
            f.seek(start_offset)
            content = f.read()
        if all(s in content for s in expect):
            return content.splitlines()
        if time.monotonic() >= deadline:
            return content.splitlines()
        time.sleep(0.05)


def _tags_by_session(lines):
    """Group the [session=<uuid>]-tagged lines by their session uuid."""
    by_session = {}
    for line in lines:
        m = SESSION_TAG.search(line)
        if m:
            by_session.setdefault(m.group(1), []).append(line)
    return by_session


def test_sessions_interleave_into_one_tagged_log():
    """One log file, sessions disambiguated by [session=<uuid>]; an untraced session is never tagged."""
    log_path = _active_log_path()
    start_offset = os.path.getsize(log_path)

    a, b, c = _connect(), _connect(), _connect()
    uuid_a = _enable_trace(a)
    uuid_b = _enable_trace(b)
    # c stays untraced.

    _run(a, "RETURN 'marker_alpha'")
    _run(b, "RETURN 'marker_beta'")
    _run(c, "RETURN 'marker_gamma'")

    by_session = _tags_by_session(_read_appended(log_path, start_offset, expect=["marker_alpha", "marker_beta"]))

    # Both traced sessions appear in the single file; the untraced one does not.
    assert set(by_session) == {uuid_a, uuid_b}, f"unexpected tagged sessions: {set(by_session)}"

    a_lines = "\n".join(by_session[uuid_a])
    b_lines = "\n".join(by_session[uuid_b])

    # Each session's query is attributed to its own tag, never the other's.
    assert "marker_alpha" in a_lines and "marker_alpha" not in b_lines
    assert "marker_beta" in b_lines and "marker_beta" not in a_lines
    # The untraced session's query never reaches the tagged trace stream.
    assert "marker_gamma" not in a_lines and "marker_gamma" not in b_lines


def test_set_session_trace_off_stops_emission():
    """After SET SESSION TRACE OFF, subsequent queries produce no tagged trace lines."""
    log_path = _active_log_path()
    start_offset = os.path.getsize(log_path)

    conn = _connect()
    uuid = _enable_trace(conn)
    _run(conn, "RETURN 'marker_while_on'")

    _run(conn, "SET SESSION TRACE OFF")
    _run(conn, "RETURN 'marker_while_off'")

    own = "\n".join(_tags_by_session(_read_appended(log_path, start_offset, expect=["marker_while_on"])).get(uuid, []))

    assert "marker_while_on" in own, "a query run while trace was ON should be tagged"
    assert "marker_while_off" not in own, "a query run after SET SESSION TRACE OFF must not be tagged"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
