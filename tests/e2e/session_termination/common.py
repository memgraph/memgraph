# Copyright 2022 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import time
import typing

import mgclient
import pytest


def connect(**kwargs) -> mgclient.Connection:
    connection = mgclient.connect(host="localhost", port=7687, **kwargs)
    connection.autocommit = True
    return connection


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: dict = {}) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()


def get_own_session_uuid(cursor: mgclient.Cursor) -> str:
    """Returns the uuid of the session `cursor` belongs to.

    `SET SESSION TRACE OFF` is a side-effect-free no-op when trace is already off (the default), and
    its single-row result is exactly the caller's own session uuid -- see PrepareSessionTraceQuery in
    src/query/interpreter.cpp. Used to disambiguate "my other connection" from "myself" when two
    connections share a username (SHOW ACTIVE USERS INFO can't tell them apart by username alone).
    """
    return execute_and_fetch_all(cursor, "SET SESSION TRACE OFF")[0][0]


def get_session_uuid_by_username(
    cursor: mgclient.Cursor, username: str, exclude_uuid: typing.Optional[str] = None
) -> str:
    """Finds exactly one active session's uuid for `username` via SHOW ACTIVE USERS INFO.

    Column order is (username, session uuid, login timestamp) -- see
    tests/e2e/show_active_users_info/test_show_active_users_info.py, which pins that shape.
    `exclude_uuid` filters out a uuid already known to belong to a different connection of the same
    username (see get_own_session_uuid).
    """
    rows = execute_and_fetch_all(cursor, "SHOW ACTIVE USERS INFO")
    candidates = [row[1] for row in rows if row[0] == username and row[1] != exclude_uuid]
    assert len(candidates) == 1, f"expected exactly one session for username={username!r}, got {candidates}"
    return candidates[0]


def wait_until(
    predicate: typing.Callable[[], bool],
    timeout: float = 10.0,
    interval: float = 0.1,
    message: str = "condition was not met in time",
) -> None:
    """Polls `predicate` until it returns True, or fails the test once `timeout` seconds elapse.

    Never hangs: every call site gets a hard, explicit deadline instead of relying on a fixed sleep.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(interval)
    pytest.fail(message)


def assert_connection_alive(cursor: mgclient.Cursor) -> None:
    """Asserts `cursor`'s connection is still usable, right now, with no waiting.

    Used for survivors that must NOT have been touched. Deliberately does not swallow the raised
    error: if the connection is unexpectedly dead, that exception is the useful failure message.
    """
    assert execute_and_fetch_all(cursor, "RETURN 1") == [(1,)]


def wait_until_terminated(cursor: mgclient.Cursor, timeout: float = 10.0) -> None:
    """Polls `cursor`'s connection until it is dead, or fails once `timeout` seconds elapse.

    RequestTermination (src/communication/v2/session.hpp) only posts the close onto the target
    session's own strand -- by the time TERMINATE SESSIONS returns its result rows, the socket may
    not be closed yet, so asserting death must poll instead of checking immediately.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            execute_and_fetch_all(cursor, "RETURN 1")
        except mgclient.Error:
            return
        time.sleep(0.1)
    pytest.fail("connection was not terminated within the timeout")
