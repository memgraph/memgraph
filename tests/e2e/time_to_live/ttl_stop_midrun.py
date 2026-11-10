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

"""A multi-batch TTL run must not make a stop/join or an exclusive switch hang.

While a large TTL run is deleting, DISABLE TTL and STORAGE MODE issued from fresh Bolt sessions must
answer, and a fresh session must still be served. A refused STORAGE MODE must leave TTL running; a
successful one must leave TTL off.

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
TTL_EVERY = 'ENABLE TTL EVERY "1s"'
CREATE = (
    "UNWIND range($a, $b) AS i "
    "CREATE (:TTL {ttl: 0, id: i, p: 'padding-padding-padding-padding'})"
)

DISABLE_TIMEOUT = 15.0
STORAGE_MODE_TIMEOUT = 15.0
MATCH_TIMEOUT = 10.0
PROBE_ATTEMPTS = 60

# Set when a probe hits its deadline; the instance is then wedged in the TTL stop/join deadlock and cannot
# finish the SIGTERM shutdown the harness sends, so the test kills it instead of hanging teardown.
# An environment variable rather than a module global: pytest imports this file as a test module
# while ``__main__`` runs the same file, and only process-wide state is shared by both copies.
TIMED_OUT_ENV = "TTL_STOP_MIDRUN_TIMED_OUT"

# Runs one statement on a brand-new Bolt connection in a separate process. argv:
# host, port, database, query, json-encoded params. Prints {"rows": ...} on success.
_CHILD = r"""
import json
import sys

import mgclient

host, port, database, query, params = sys.argv[1:6]
connection = mgclient.connect(host=host, port=int(port))
connection.autocommit = True
try:
    cursor = connection.cursor()
    cursor.execute("USE DATABASE " + database)
    cursor.execute(query, json.loads(params))
    rows = cursor.fetchall() if cursor.description is not None else None
    print(json.dumps({"rows": rows}))
finally:
    connection.close()
"""


def _connect():
    connection = mgclient.connect(host=HOST, port=PORT)
    connection.autocommit = True
    return connection


def _run(connection, query, params=None):
    cursor = connection.cursor()
    cursor.execute(query, params or {})
    if cursor.description is not None:
        return cursor.fetchall()
    return None


def fresh_query(database, query, timeout, params=None):
    """Run ``query`` on a brand-new connection in a child process.

    Returns ``(status, detail)`` with status in ``{"ok", "err", "timeout"}``. The child is killed
    when ``timeout`` expires, so a stalled server never blocks the test process.
    """
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            _CHILD,
            HOST,
            str(PORT),
            database,
            query,
            json.dumps(params or {}),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        stdout, stderr = child.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        child.kill()
        child.communicate()
        os.environ[TIMED_OUT_ENV] = "1"
        return "timeout", f"no answer within {timeout:g}s"
    if child.returncode == 0:
        try:
            return "ok", json.loads(stdout.strip().splitlines()[-1])["rows"]
        except (IndexError, ValueError, KeyError) as exc:
            return "err", f"unparsable child output {stdout!r}: {exc}"
    return "err", (stderr or stdout).strip() or f"child exited {child.returncode}"


def _project_root():
    """The checkout this test runs from (``<root>/tests/e2e`` or ``<root>/build/tests/e2e``)."""
    directory = os.path.dirname(os.path.realpath(__file__))
    for _ in range(8):
        if os.path.isfile(os.path.join(directory, "CMakeLists.txt")) and os.path.isdir(
            os.path.join(directory, "src")
        ):
            return directory
        parent = os.path.dirname(directory)
        if parent == directory:
            break
        directory = parent
    return None


def _kill_wedged_server():
    """SIGKILL the instance this workload started, if a probe timed out on it.

    The regression leaves the storage-ttl worker and the stop/join thread deadlocked, so the harness's
    SIGTERM never completes and teardown would block; killing the wedged process lets the runner
    report the assertion failure instead.
    """
    root = _project_root()
    if root is None:
        return
    expected = os.path.realpath(os.path.join(root, "build", "memgraph"))
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            exe = os.path.realpath(os.readlink(f"/proc/{entry}/exe"))
            with open(f"/proc/{entry}/cmdline", "rb") as cmdline_file:
                argv = cmdline_file.read().split(b"\0")
        except OSError:
            continue
        if exe != expected or b"--bolt-port" not in argv or str(PORT).encode() not in argv:
            continue
        try:
            os.kill(int(entry), signal.SIGKILL)
        except OSError:
            pass
        return


def _create_database(database):
    connection = _connect()
    try:
        _run(connection, f"CREATE DATABASE {database}")
    finally:
        connection.close()


def _load_and_enable(database, nodes=NODES, ttl_every=TTL_EVERY):
    """Create ``database``, load ``nodes`` expired :TTL vertices, and enable TTL.

    Every session is closed before returning, so the exclusive STORAGE MODE probe sees only its
    own connection (an extra live session makes the switch refuse before it can reach the join).
    """
    _create_database(database)
    connection = _connect()
    try:
        _run(connection, f"USE DATABASE {database}")
        for start in range(0, nodes, BATCH):
            _run(connection, CREATE, {"a": start, "b": start + BATCH - 1})
        _run(connection, ttl_every)
    finally:
        connection.close()


def _count(database, query="MATCH (n:TTL) RETURN count(n)"):
    status, detail = fresh_query(database, query, MATCH_TIMEOUT)
    if status != "ok":
        return None
    return detail[0][0]


def _wait_until_ttl_deleting(database, total, timeout=15.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        count = _count(database)
        if count is not None and count < total:
            return count
        time.sleep(0.05)
    return None


def test_disable_ttl_midrun_returns():
    """DISABLE TTL during a multi-batch run must answer and stop TTL."""
    database = "ttl_disable_midrun"
    _load_and_enable(database)

    count_at_start = _wait_until_ttl_deleting(database, NODES)
    assert count_at_start is not None, "TTL did not start deleting expired vertices within 15s"

    status, detail = fresh_query(database, "DISABLE TTL", DISABLE_TIMEOUT)
    assert status == "ok", (
        f"DISABLE TTL did not answer within {DISABLE_TIMEOUT:g}s while a multi-batch TTL run was "
        f"deleting (count at issue {count_at_start}): {status} {detail}"
    )

    before = _count(database)
    assert before is not None
    time.sleep(2.0)
    after = _count(database)
    assert after is not None
    assert after == before, f"TTL kept reaping after DISABLE TTL returned: {before} -> {after}"


def test_storage_mode_refusal_restores_ttl():
    """A refused switch must keep TTL running; a successful switch must leave TTL off."""
    refused = "ttl_refusal_restore"
    _create_database(refused)
    connection = _connect()
    try:
        _run(connection, f"USE DATABASE {refused}")
        _run(connection, "CREATE (:Keep)")  # keeps the database non-empty -> switch is refused
        _run(connection, TTL_EVERY)
    finally:
        connection.close()

    status, detail = fresh_query(refused, "STORAGE MODE ON_DISK_TRANSACTIONAL", STORAGE_MODE_TIMEOUT)
    assert status == "err", (
        f"expected a clear refusal switching a non-empty database, got {status}: {detail}"
    )

    # The refusal must restore (here: keep) TTL: a node created after the refusal is still reaped.
    connection = _connect()
    try:
        _run(connection, f"USE DATABASE {refused}")
        _run(connection, "CREATE (:TTL {ttl: 0})")
    finally:
        connection.close()
    deadline = time.time() + 15.0
    while time.time() < deadline:
        if _count(refused) == 1:  # only the Keep vertex remains
            break
        time.sleep(0.2)
    assert _count(refused) == 1, "TTL was not left running after a refused STORAGE MODE"

    # A successful switch on an empty TTL-enabled database must leave TTL disabled on the new storage.
    switched = "ttl_switch_success"
    _create_database(switched)
    connection = _connect()
    try:
        _run(connection, f"USE DATABASE {switched}")
        _run(connection, 'ENABLE TTL EVERY "1d"')
    finally:
        connection.close()
    # Let the one immediate run finish, so the scheduler is parked when the exclusive region starts.
    time.sleep(0.5)

    status, detail = fresh_query(switched, "STORAGE MODE ON_DISK_TRANSACTIONAL", STORAGE_MODE_TIMEOUT)
    assert status == "ok", f"STORAGE MODE on an empty TTL-enabled database did not answer: {status} {detail}"

    connection = _connect()
    try:
        _run(connection, f"USE DATABASE {switched}")
        _run(connection, "CREATE (:TTL {ttl: 0})")
    finally:
        connection.close()
    time.sleep(3.0)
    assert _count(switched) == 1, "TTL was still reaping on the new on-disk storage after the switch"


def test_storage_mode_midrun_and_sessions():
    """STORAGE MODE and fresh sessions must answer during a multi-batch TTL run."""
    database = "ttl_storage_mode_midrun"
    _load_and_enable(database)

    count_at_start = _wait_until_ttl_deleting(database, NODES)
    assert count_at_start is not None, "TTL did not start deleting expired vertices within 15s"

    answered = False
    status = "err"
    for attempt in range(1, PROBE_ATTEMPTS + 1):
        status, detail = fresh_query(database, "STORAGE MODE ON_DISK_TRANSACTIONAL", STORAGE_MODE_TIMEOUT)
        assert status != "timeout", (
            f"STORAGE MODE did not answer within {STORAGE_MODE_TIMEOUT:g}s at attempt {attempt} while a "
            f"multi-batch TTL run was in flight (count at issue {count_at_start}): {detail}"
        )
        if status == "ok":
            answered = True
            break
        # A clear refusal (database contains data) is an answer; retry because the hang needs the
        # exclusive region to start at a TTL batch boundary.
        time.sleep(0.2)

    assert answered or status == "err", "STORAGE MODE neither succeeded nor refused clearly during the TTL run"

    session_status, session_detail = fresh_query(database, "MATCH (n) RETURN count(n)", MATCH_TIMEOUT)
    assert session_status == "ok", (
        f"a fresh session did not answer within {MATCH_TIMEOUT:g}s during/after the STORAGE MODE probe: "
        f"{session_status} {session_detail}"
    )


if __name__ == "__main__":
    # -x: a red test leaves the server deadlocked, so the remaining scenarios must not run
    # against it and hang the workload; each scenario needs its own live instance, and on a green run
    # all of them run.
    rc = pytest.main([__file__, "-rA", "-x"])
    if rc != 0 and os.environ.get(TIMED_OUT_ENV):
        _kill_wedged_server()
    sys.exit(rc)
