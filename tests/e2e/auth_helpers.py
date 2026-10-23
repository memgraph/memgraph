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

"""Helpers for e2e tests that create users on an instance without any.

Creating the first user turns authentication on, and an anonymous session is refused on its next query from then on.
The session that creates the first user therefore has to log in as that user right after the creation, and that user
has to be dropped last: dropping it ends the session, and with no users left an anonymous session is valid again.
"""

from __future__ import annotations

import contextlib
import re
from collections.abc import Callable, Iterator
from typing import Any

CREATE_USER = re.compile(
    r"^\s*CREATE\s+USER\s+(?:IF\s+NOT\s+EXISTS\s+)?(`[^`]+`|\w+)(?:\s+IDENTIFIED\s+BY\s+'((?:[^'\\]|\\.)*)')?",
    re.IGNORECASE,
)


def parse_create_user(query: str) -> tuple[str, str] | None:
    """Returns (name, password) of a `CREATE USER` query, or None for any other query. The password is "" when the
    query sets none."""
    match = CREATE_USER.match(query)
    if match is None:
        return None
    return match.group(1).strip("`"), re.sub(r"\\(.)", r"\1", match.group(2) or "")


def _run(connection: Any, query: str) -> None:
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.fetchall()


@contextlib.contextmanager
def as_first_user(connect_fn: Callable[..., Any], name: str, password: str | None = None) -> Iterator[Any]:
    """Creates `name` as the first user and yields a connection logged in as it.

    `connect_fn(username=..., password=...)` must return an autocommit mgclient connection. The user is dropped on
    exit through the yielded connection; that DROP ends the session, so it is allowed to fail.
    """
    anonymous = connect_fn(username="", password="")
    try:
        if password is None:
            _run(anonymous, f"CREATE USER `{name}`")
        else:
            escaped = password.replace("\\", "\\\\").replace("'", "\\'")
            _run(anonymous, f"CREATE USER `{name}` IDENTIFIED BY '{escaped}'")
    finally:
        anonymous.close()

    connection = connect_fn(username=name, password=password or "")
    try:
        yield connection
    finally:
        with contextlib.suppress(Exception):
            _run(connection, f"DROP USER `{name}`")


def first_user_cursor(request: Any, connect_fn: Callable[..., Any], name: str, password: str | None = None) -> Any:
    """Like `as_first_user`, for a pytest test: returns a cursor of the first user and drops the user in a finalizer.

    Finalizers run in reverse registration order, so a test's own cleanup registered afterwards runs while the user
    still exists.
    """
    stack = contextlib.ExitStack()
    request.addfinalizer(stack.close)
    return stack.enter_context(as_first_user(connect_fn, name, password)).cursor()
