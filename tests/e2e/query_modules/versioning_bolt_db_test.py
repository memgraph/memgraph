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

# Needs a server with --experimental-enabled=versioning (enterprise license).
# Bolt `db="<database>@<branch>"` pins every transaction to a branch.

import sys

import pytest
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError

URI = "bolt://localhost:7687"


@pytest.fixture
def driver():
    with GraphDatabase.driver(URI, auth=("", "")) as drv:
        with drv.session(database="memgraph") as s:
            s.run("MATCH (n) DETACH DELETE n").consume()
            s.run("CREATE (:V {id: 1})").consume()
            s.run("CREATE BRANCH 'pinned'").consume()
        yield drv
        with drv.session(database="memgraph") as s:
            s.run("DROP BRANCH 'pinned' FORCE").consume()


def _count(session, label: str, **kwargs) -> int:
    return session.run(f"MATCH (n:{label}) RETURN count(n) AS c", **kwargs).single()["c"]


def test_db_with_branch_runs_on_branch_and_plain_db_is_main(driver):
    with driver.session(database="memgraph@pinned") as s:
        s.run("CREATE (:OnBranch)").consume()
        assert _count(s, "OnBranch") == 1
        assert _count(s, "V") == 1
        s.execute_write(lambda tx: tx.run("CREATE (:InTx)").consume())
        assert s.execute_read(lambda tx: tx.run("MATCH (n:InTx) RETURN count(n) AS c").single()["c"]) == 1

    with driver.session(database="memgraph") as s:
        assert _count(s, "OnBranch") == 0
        assert _count(s, "InTx") == 0

    with driver.session(database="memgraph@main") as s:
        assert _count(s, "OnBranch") == 0


def test_unknown_branch_is_refused(driver):
    with driver.session(database="memgraph@missing") as s:
        with pytest.raises(ClientError, match="Branch 'missing' does not exist."):
            s.run("RETURN 1").consume()


def test_more_than_one_at_is_refused(driver):
    with driver.session(database="memgraph@pinned@x") as s:
        with pytest.raises(ClientError, match="Invalid database name"):
            s.run("RETURN 1").consume()


def test_pinned_session_refuses_use_branch_but_allows_using_branch(driver):
    with driver.session(database="memgraph@pinned") as s:
        with pytest.raises(ClientError, match="prohibited if the session explicitly defines the database"):
            s.run("USE BRANCH 'main'").consume()
        s.run("CREATE (:OnBranch)").consume()
        assert _count(s, "OnBranch") == 1
    with driver.session(database="memgraph@pinned") as s:
        assert s.run("USING BRANCH 'main' MATCH (n:OnBranch) RETURN count(n) AS c").single()["c"] == 0


def test_pinned_branch_dropped_fails_each_transaction_until_db_changes(driver):
    with driver.session(database="memgraph@pinned") as s:
        assert _count(s, "V") == 1
        with driver.session(database="memgraph") as admin:
            admin.run("DROP BRANCH 'pinned' FORCE").consume()
        for _ in range(2):
            with pytest.raises(ClientError, match="Branch 'pinned' no longer exists."):
                s.run("MATCH (n:V) RETURN count(n)").consume()
    with driver.session(database="memgraph") as s:
        assert _count(s, "V") == 1
        s.run("CREATE BRANCH 'pinned'").consume()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
