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

"""
Explicit auth transactions (BEGIN; CREATE/DROP USER; COMMIT) while DDL keeps main_lock_ contended, under
--experimental-enabled=commit-lock-narrowing. Runs on its own instance (own workload): creating a user turns auth on
for every connection, which would break the auth=None connections of test_commit_lock_narrowing.py.
Standalone: MEMGRAPH_BOLT_PORT=<port> python3 -m pytest -x -q <this file>
"""

import sys
import threading
from collections.abc import Iterator

import pytest
from helpers import URI, run_bounded, start_contender
from neo4j import GraphDatabase, basic_auth

ADMIN = basic_auth("admin", "admin_pw")
N_CLIENTS = 4
USERS_PER_CLIENT = 3


@pytest.fixture
def admin() -> Iterator[None]:
    # One session: once the first user exists, new no-auth connections are rejected. The first user is auto-granted
    # all privileges (and a same-named role, which makes a bare GRANT ... TO admin ambiguous), so no GRANT here.
    with GraphDatabase.driver(URI, auth=None) as d, d.session() as s:
        s.run("CREATE USER admin IDENTIFIED BY 'admin_pw'").consume()
    yield
    with GraphDatabase.driver(URI, auth=ADMIN) as d, d.session() as s:
        for name in show_users(s):
            if name != "admin":
                s.run(f"DROP USER {name}").consume()
        s.run("DROP USER admin").consume()  # auth is off again afterwards


def show_users(s) -> set[str]:
    return {r["user"] for r in s.run("SHOW USERS").data()}


def run_in_txns(template: str) -> None:
    """Every client runs USERS_PER_CLIENT explicit transactions of `template`; any error or hang fails."""
    errors: list[str] = []

    def client(i: int) -> None:
        try:
            with GraphDatabase.driver(URI, auth=ADMIN, max_transaction_retry_time=0) as d:
                for k in range(USERS_PER_CLIENT):

                    def txn() -> None:
                        with d.session() as s, s.begin_transaction() as tx:
                            tx.run(template.format(name=f"u_{i}_{k}")).consume()
                            tx.commit()

                    run_bounded(txn)
        except Exception as e:  # noqa: BLE001
            errors.append(repr(e))

    threads = [threading.Thread(target=client, args=(i,), daemon=True) for i in range(N_CLIENTS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60.0 * USERS_PER_CLIENT)
    assert not any(t.is_alive() for t in threads), "client thread hung"
    assert not errors, f"client errors: {errors[:5]}"


def test_auth_txn_under_begin_contention(admin: None):
    """Each CREATE/DROP USER transaction must succeed exactly once (a repeat would error) and none may hang."""
    expected = {f"u_{i}_{k}" for i in range(N_CLIENTS) for k in range(USERS_PER_CLIENT)}
    for contender in start_contender(0.0, auth=ADMIN):
        assert contender.first.wait(30), "contender never completed a DDL statement"
        ddl_at_start = contender.ok
        run_in_txns("CREATE USER {name}")
        with GraphDatabase.driver(URI, auth=ADMIN) as d, d.session() as s:
            assert show_users(s) - {"admin"} == expected
        run_in_txns("DROP USER {name}")
        with GraphDatabase.driver(URI, auth=ADMIN) as d, d.session() as s:
            assert show_users(s) == {"admin"}
        assert contender.ok - ddl_at_start >= 1, "contender completed no DDL during the clients: no contention"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
