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

# Bolt db="<database>@<branch>" pins every transaction to a branch. Needs --experimental-enabled=versioning and an
# enterprise license.

import sys

import pytest
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError

URI = "bolt://localhost:7687"
NO_AUTH = ("", "")


@pytest.fixture
def driver():
    # One pooled connection, so every session below reuses the same server-side Bolt session.
    with GraphDatabase.driver(URI, auth=NO_AUTH, max_connection_pool_size=1) as drv:
        with drv.session(database="memgraph") as s:
            s.run("MATCH (n) DETACH DELETE n").consume()
            s.run("CREATE (:V {id: 1})").consume()
            s.run("CREATE BRANCH pinned").consume()
        yield drv
    with GraphDatabase.driver(URI, auth=NO_AUTH) as drv, drv.session(database="memgraph") as s:
        if "pinned" in [row["name"] for row in s.run("SHOW BRANCHES")]:
            s.run("DROP BRANCH pinned FORCE").consume()


def _count(session, label: str) -> int:
    return session.run(f"MATCH (n:{label}) RETURN count(n) AS c").single()["c"]


def test_db_with_branch_runs_on_the_branch_and_plain_db_is_main(driver):
    with driver.session(database="memgraph@pinned") as s:
        s.run("CREATE (:OnBranch)").consume()
        s.execute_write(lambda tx: tx.run("CREATE (:InTx)").consume())
        assert s.execute_read(lambda tx: tx.run("MATCH (n:InTx) RETURN count(n) AS c").single()["c"]) == 1
        assert _count(s, "OnBranch") == 1
        assert _count(s, "V") == 1

    for database in ("memgraph", "memgraph@main"):
        with driver.session(database=database) as s:
            assert _count(s, "OnBranch") == 0, database
            assert _count(s, "InTx") == 0, database


def test_request_without_db_after_a_pin_runs_on_main(driver):
    with driver.session(database="memgraph@pinned") as s:
        s.run("CREATE (:OnBranch)").consume()
    with driver.session() as s:
        assert _count(s, "OnBranch") == 0


def test_unknown_branch_and_more_than_one_at_are_refused(driver):
    with driver.session(database="memgraph@missing") as s:
        with pytest.raises(ClientError, match="Branch 'missing' does not exist."):
            s.run("RETURN 1").consume()
    with driver.session(database="memgraph@pinned@x") as s:
        with pytest.raises(ClientError, match="Invalid database name 'memgraph@pinned@x'"):
            s.run("RETURN 1").consume()


def test_pinned_session_refuses_use_branch_but_allows_using_branch(driver):
    with driver.session(database="memgraph@pinned") as s:
        s.run("CREATE (:OnBranch)").consume()
        with pytest.raises(ClientError, match="prohibited if the session explicitly defines the database"):
            s.run("USE BRANCH main").consume()
        assert s.run("USING BRANCH main MATCH (n:OnBranch) RETURN count(n) AS c").single()["c"] == 0
        assert _count(s, "OnBranch") == 1


def test_dropped_pinned_branch_is_never_followed(driver):
    with driver.session(database="memgraph@pinned") as s:
        assert _count(s, "V") == 1
        with GraphDatabase.driver(URI, auth=NO_AUTH) as admin, admin.session(database="memgraph") as a:
            a.run("DROP BRANCH pinned FORCE").consume()
        for _ in range(2):
            with pytest.raises(ClientError, match="Branch 'pinned' no longer exists."):
                s.run("MATCH (n:V) RETURN count(n)").consume()
        # A branch re-created under the same name is a different branch: the pin stays on the dropped one and only a
        # new connection resolves the name to the new one.
        with GraphDatabase.driver(URI, auth=NO_AUTH) as other:
            with other.session(database="memgraph") as a:
                a.run("CREATE BRANCH pinned").consume()
            with pytest.raises(ClientError, match=r"this connection stays on the dropped one"):
                s.run("MATCH (n:V) RETURN count(n)").consume()
            with other.session(database="memgraph@pinned") as fresh:
                assert _count(fresh, "V") == 1
    with driver.session(database="memgraph") as s:
        assert _count(s, "V") == 1


def test_schema_assert_is_main_only(driver):
    call = "CALL schema.assert({V: ['id']}, {}) YIELD label RETURN label"
    with driver.session(database="memgraph@pinned") as s:
        with pytest.raises(ClientError, match="not allowed on a branch"):
            s.run(call).consume()
        s.run(f"USING BRANCH main {call}").consume()
    with driver.session(database="memgraph") as s:
        with pytest.raises(ClientError, match="not allowed on a branch"):
            s.run(f"USING BRANCH pinned {call}").consume()
        assert len(s.run("SHOW INDEX INFO").data()) == 1
        s.run("DROP INDEX ON :V(id)").consume()


def test_branch_part_needs_branch_use(driver):
    def run(database: str, query: str):
        # A fresh connection: privileges are read when a session logs in.
        with GraphDatabase.driver(URI, auth=("pin_user", "pw")) as user, user.session(database=database) as s:
            return s.run(query).single()[0]

    with GraphDatabase.driver(URI, auth=NO_AUTH) as admin, admin.session(database="memgraph") as a:
        # The first user gets every privilege; the second starts with none.
        a.run("CREATE USER pin_admin").consume()
        a.run("CREATE USER pin_user IDENTIFIED BY 'pw'").consume()
        try:
            for database in ("memgraph@pinned", "memgraph@main"):
                with pytest.raises(ClientError, match="not authorized to use branches"):
                    run(database, "RETURN 1")
            a.run("GRANT BRANCH_USE TO pin_user").consume()
            assert run("memgraph@pinned", "SHOW BRANCH") == "pinned"
        finally:
            a.run("DROP USER pin_user").consume()
            a.run("DROP USER pin_admin").consume()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
