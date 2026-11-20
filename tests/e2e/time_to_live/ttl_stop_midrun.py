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

"""DISABLE TTL and STORAGE MODE must answer while a multi-batch TTL run is deleting.

DISABLE TTL must return and stop the deletions; STORAGE MODE ON_DISK_TRANSACTIONAL must be refused
while TTL is enabled, and the server must keep serving and keep reaping.

Every query that can block runs in its own child process: mgclient holds the GIL while blocked, so a
daemon-thread join(timeout) would hang the whole pytest process instead of failing. Killing the child
at the deadline records the timeout as the test's assertion failure.
"""

import json
import os
import signal
import subprocess
import sys
import time

import mgclient
import pytest

HOST = "localhost"
PORT = 7687

BATCH = 20_000
NODES = 400_000
CREATE = "UNWIND range($a, $b) AS i " "CREATE (:TTL {ttl: 0, id: i, p: 'padding-padding-padding-padding'})"
TIMEOUT = 15.0

# Set when a probe hits its deadline. An environment variable rather than a module global: pytest
# imports this file as a test module while ``__main__`` runs the same file.
TIMED_OUT_ENV = "TTL_STOP_MIDRUN_TIMED_OUT"

# Runs one statement on a brand-new Bolt connection. argv: host, port, database, query. Prints a
# line once connected, then runs the statement once "go" arrives on stdin.
_CHILD = r"""
import json
import sys

import mgclient

host, port, database, query = sys.argv[1:5]
connection = mgclient.connect(host=host, port=int(port))
connection.autocommit = True
try:
    cursor = connection.cursor()
    cursor.execute("USE DATABASE " + database)
    print("ready", flush=True)
    if sys.stdin.readline().strip() != "go":
        sys.exit(0)  # the test process went away without firing
    cursor.execute(query)
    print(json.dumps({"rows": cursor.fetchall() if cursor.description is not None else None}))
finally:
    connection.close()
"""


def _run(cursor, query, params=None):
    cursor.execute(query, params or {})
    return cursor.fetchall() if cursor.description is not None else None


def armed_query(database, query):
    """Start a child that connects and waits; ``fire`` it to run ``query`` without process start-up delay."""
    child = subprocess.Popen(
        [sys.executable, "-c", _CHILD, HOST, str(PORT), database, query],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert child.stdout.readline().strip() == "ready", child.stderr.read()
    return child


def fire(child, timeout=TIMEOUT):
    """Run an armed child's query; return ``(status, detail)``.

    Status is ``ok`` (detail = rows), ``err`` (detail = child stderr) or ``timeout``; the child is
    killed at the deadline so a stalled server never blocks the test process.
    """
    try:
        stdout, stderr = child.communicate(input="go\n", timeout=timeout)
    except subprocess.TimeoutExpired:
        child.kill()
        child.communicate()
        os.environ[TIMED_OUT_ENV] = "1"
        return "timeout", f"no answer within {timeout:g}s"
    if child.returncode != 0:
        return "err", (stderr or stdout).strip() or f"child exited {child.returncode}"
    try:
        return "ok", json.loads(stdout.strip().splitlines()[-1])["rows"]
    except (IndexError, ValueError, KeyError) as exc:
        return "err", f"unparsable child output {stdout!r}: {exc}"


def fresh_query(database, query, timeout=TIMEOUT):
    """Run ``query`` on a new connection in a child process; see ``fire``."""
    return fire(armed_query(database, query), timeout)


def _kill_wedged_server():
    """SIGKILL the instance this workload started: a wedged server never finishes the SIGTERM."""
    directory = os.path.dirname(os.path.realpath(__file__))
    while not (
        os.path.isfile(os.path.join(directory, "CMakeLists.txt")) and os.path.isdir(os.path.join(directory, "src"))
    ):
        parent = os.path.dirname(directory)
        if parent == directory:
            return
        directory = parent
    expected = os.path.realpath(os.path.join(directory, "build", "memgraph"))
    for entry in filter(str.isdigit, os.listdir("/proc")):
        try:
            exe = os.path.realpath(os.readlink(f"/proc/{entry}/exe"))
            with open(f"/proc/{entry}/cmdline", "rb") as cmdline_file:
                argv = cmdline_file.read().split(b"\0")
            if exe == expected and b"--bolt-port" in argv and str(PORT).encode() in argv:
                os.kill(int(entry), signal.SIGKILL)
                return
        except OSError:
            continue


def _load_and_enable(database):
    """Create ``database``, load NODES expired :TTL vertices and enable TTL; return a session on it.

    The session only counts vertices before any statement that could wedge the server is issued.
    """
    connection = mgclient.connect(host=HOST, port=PORT)
    connection.autocommit = True
    cursor = connection.cursor()
    _run(cursor, f"CREATE DATABASE {database}")
    _run(cursor, f"USE DATABASE {database}")
    for start in range(0, NODES, BATCH):
        _run(cursor, CREATE, {"a": start, "b": start + BATCH - 1})
    _run(cursor, 'ENABLE TTL EVERY "1s"')
    return connection


def _count(database):
    status, detail = fresh_query(database, "MATCH (n:TTL) RETURN count(n)")
    assert status == "ok", f"a fresh session was not served: {status} {detail}"
    return detail[0][0]


def _run_midrun(database, query):
    """Run ``query`` as soon as the TTL run has deleted its first batch; return ``fire``'s result.

    The query's child connects before the run starts, so it lands within milliseconds of the first deletion.
    """
    connection = _load_and_enable(database)
    child = armed_query(database, query)
    try:
        cursor = connection.cursor()
        deadline = time.time() + TIMEOUT
        while _run(cursor, "MATCH (n:TTL) RETURN count(n)")[0][0] == NODES:
            if time.time() >= deadline:
                child.kill()
                child.communicate()
                pytest.fail("TTL did not start deleting expired vertices in time")
            time.sleep(0.01)
    finally:
        connection.close()
    return fire(child)


def test_disable_ttl_midrun_returns():
    # A whole run takes a few hundred milliseconds; retry on a fresh database if it still beat DISABLE TTL.
    for attempt in range(3):
        database = f"ttl_disable_midrun{attempt}"
        status, detail = _run_midrun(database, "DISABLE TTL")
        assert status == "ok", f"DISABLE TTL did not answer within {TIMEOUT:g}s mid-run: {status} {detail}"
        before = _count(database)
        if before > 0:
            break
    else:
        pytest.fail("the TTL run finished before DISABLE TTL in every attempt; the mid-run case was not exercised")

    time.sleep(2.0)
    after = _count(database)
    assert after == before, f"TTL kept reaping after DISABLE TTL returned: {before} -> {after}"


def test_storage_mode_refused_while_ttl_enabled():
    database = "ttl_storage_mode_refused"
    status, detail = _run_midrun(database, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    assert status == "err", f"STORAGE MODE must be refused while TTL is enabled: {status} {detail}"
    assert "time-to-live" in detail.lower() or "ttl" in detail.lower(), detail

    deadline = time.time() + TIMEOUT
    while _count(database) > 0 and time.time() < deadline:
        time.sleep(0.2)
    assert _count(database) == 0, "TTL stopped deleting after the refused switch"


if __name__ == "__main__":
    # -x: a red test may leave the server wedged, so later scenarios must not run against it.
    rc = pytest.main([__file__, "-rA", "-x"])
    if rc != 0 and os.environ.get(TIMED_OUT_ENV):
        _kill_wedged_server()
    sys.exit(rc)
