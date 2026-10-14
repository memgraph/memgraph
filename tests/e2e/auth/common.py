# Copyright 2023 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import re

import pytest
from gqlalchemy import Memgraph

BOOTSTRAP_USER = "bootstrap_admin"
BOOTSTRAP_PASSWORD = "bootstrap"

CREATE_USER = re.compile(
    r"^\s*CREATE\s+USER\s+(?:IF\s+NOT\s+EXISTS\s+)?(`[^`]+`|\w+)(?:\s+IDENTIFIED\s+BY\s+'((?:[^'\\]|\\.)*)')?",
    re.IGNORECASE,
)


class AuthMemgraph:
    """A Memgraph client for tests that create users.

    An anonymous session is refused as soon as the first user exists, so the client switches to a real user at that
    moment. With `login_as_first_user` the user being created becomes the session user (and keeps the privileges
    the first user is given); otherwise a bootstrap admin is created first and the session runs as it, leaving the
    users the test creates free to be changed without cutting off the session.
    """

    def __init__(self, login_as_first_user: bool) -> None:
        self._login_as_first_user = login_as_first_user
        self.user = None
        self.client = Memgraph()

    def _login(self, user: str, password: str) -> None:
        self.client = Memgraph(username=user, password=password)
        self.user = user

    def _bootstrap(self, query: str) -> None:
        if self.user is not None or self._login_as_first_user or not CREATE_USER.match(query):
            return
        self.client.execute(f"CREATE USER {BOOTSTRAP_USER} IDENTIFIED BY '{BOOTSTRAP_PASSWORD}';")
        self._login(BOOTSTRAP_USER, BOOTSTRAP_PASSWORD)

    def execute(self, query: str, *args, **kwargs) -> None:
        self._bootstrap(query)
        self.client.execute(query, *args, **kwargs)
        created = CREATE_USER.match(query)
        if created and self.user is None:
            self._login(created.group(1).strip("`"), re.sub(r"\\(.)", r"\1", created.group(2) or ""))

    def execute_and_fetch(self, query: str, *args, **kwargs):
        self._bootstrap(query)
        return self.client.execute_and_fetch(query, *args, **kwargs)

    def cleanup(self) -> None:
        client = self.client
        try:
            client.execute("RETURN 1;")
        except Exception:
            # The session's user was dropped by the test; with no users left an anonymous session is valid again.
            client = Memgraph()

        client.drop_indexes()
        client.ensure_constraints([])
        client.drop_database()

        try:
            users = [next(iter(row.values())) for row in client.execute_and_fetch("SHOW USERS;")]
        except Exception:
            users = []
        # The session's own user goes last: dropping it ends the session.
        for user in sorted(users, key=lambda name: name == self.user):
            try:
                client.execute(f"DROP USER `{user}`;")
            except Exception:
                pass
        # With every user gone authentication is off again.
        client = Memgraph() if users else client

        for statement in (
            "DROP ROLE mrma;",
            "DROP ROLE admin;",
            "DROP ROLE readwrite;",
            "DROP ROLE readonly;",
            "DROP PROFILE profile;",
            "DROP PROFILE Profile;",
            "DROP PROFILE profile2;",
        ):
            try:
                client.execute(statement)
            except Exception:
                pass


@pytest.fixture
def memgraph(**kwargs) -> AuthMemgraph:
    memgraph = AuthMemgraph(login_as_first_user=False)

    yield memgraph

    memgraph.cleanup()


@pytest.fixture
def first_user_memgraph() -> AuthMemgraph:
    """For tests that depend on the user they create being the first one (it holds the builtin admin role)."""
    memgraph = AuthMemgraph(login_as_first_user=True)

    yield memgraph

    memgraph.cleanup()


@pytest.fixture
def provide_user() -> Memgraph:
    memgraph = Memgraph()

    # Create superuser first so anthony is not the first user and gets no builtin role
    memgraph.execute("CREATE USER superuser IDENTIFIED BY 'superpassword';")
    memgraph = Memgraph(username="superuser", password="superpassword")
    memgraph.execute("CREATE USER anthony IDENTIFIED BY 'password';")

    yield None

    memgraph.execute("DROP USER anthony;")
    memgraph.execute("DROP USER superuser;")
