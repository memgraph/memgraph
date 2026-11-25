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

# Without --experimental-enabled=versioning '@' is an ordinary character in a database name. Needs an enterprise license
# (multi-tenancy).

import sys

import pytest
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError

URI = "bolt://localhost:7687"
NO_AUTH = ("", "")


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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
