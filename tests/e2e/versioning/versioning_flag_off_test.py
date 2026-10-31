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

# Without --experimental-enabled=versioning the server behaves as before graph versioning existed: nothing the feature
# added is visible. Needs an enterprise license (multi-tenancy, auth).

import sys

import pytest
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError

URI = "bolt://localhost:7687"
NO_AUTH = ("", "")
BRANCH_PRIVILEGES = {"BRANCH_USE", "BRANCH_EDIT"}


@pytest.fixture
def admin():
    with GraphDatabase.driver(URI, auth=NO_AUTH) as drv, drv.session(database="memgraph") as s:
        yield s


def test_at_in_a_database_name_is_an_ordinary_character(admin):
    admin.run("CREATE DATABASE `a@b`").consume()
    try:
        with GraphDatabase.driver(URI, auth=NO_AUTH) as drv, drv.session(database="a@b") as s:
            s.run("CREATE (:OnAtDb)").consume()
            assert s.run("SHOW DATABASE").single()[0] == "a@b"
            assert s.run("MATCH (n:OnAtDb) RETURN count(n) AS c").single()["c"] == 1
        admin.run("CREATE DATABASE plain").consume()
        admin.run("RENAME DATABASE plain TO `c@d`").consume()
        assert "c@d" in [row[0] for row in admin.run("SHOW DATABASES")]
    finally:
        for name in ("a@b", "c@d", "plain"):
            try:
                admin.run(f"DROP DATABASE `{name}` FORCE").consume()
            except ClientError:
                pass


@pytest.mark.parametrize(
    "query",
    [
        "CREATE BRANCH b",
        "DROP BRANCH b",
        "SHOW BRANCH",
        "SHOW BRANCHES",
        "USE BRANCH b",
        "USING BRANCH b MATCH (n) RETURN n",
    ],
)
def test_branch_statements_say_versioning_is_disabled(admin, query):
    with pytest.raises(ClientError, match="Graph versioning is disabled"):
        admin.run(query).consume()


def test_branch_words_stay_usable_as_identifiers(admin):
    row = admin.run(
        "UNWIND [1] AS branch "
        "CREATE (n:branch {branch: branch, branches: 2, BRANCH_USE: 3, BRANCH_EDIT: 4}) "
        "WITH n, branch AS branches "
        "RETURN n.branch AS a, n.branches AS b, n.BRANCH_USE AS c, n.BRANCH_EDIT AS d, branches"
    ).single()
    assert (row["a"], row["b"], row["c"], row["d"], row["branches"]) == (1, 2, 3, 4, 1)
    admin.run("MATCH (n:branch) DELETE n").consume()


def test_branch_privileges_are_neither_granted_nor_listed(admin):
    def privileges(user: str):
        return {row[0] for row in admin.run(f"SHOW PRIVILEGES FOR {user} ON MAIN")}

    admin.run("CREATE USER flag_off_first").consume()
    admin.run("CREATE USER flag_off_second").consume()
    try:
        admin.run("GRANT ALL PRIVILEGES TO flag_off_second").consume()
        for user in ("flag_off_first", "flag_off_second"):
            granted = privileges(user)
            assert "MATCH" in granted, user
            assert not (granted & BRANCH_PRIVILEGES), user
    finally:
        admin.run("DROP USER flag_off_second").consume()
        admin.run("DROP USER flag_off_first").consume()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
