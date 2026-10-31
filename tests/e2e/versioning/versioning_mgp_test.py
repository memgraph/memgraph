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

# A query a procedure runs through mgp_execute_query stays on the caller's branch. Needs
# --experimental-enabled=versioning and an enterprise license.

import contextlib
import sys

import mgclient
import pytest
from common import connect, execute_and_fetch_all


def _count(cursor, label: str) -> int:
    return execute_and_fetch_all(cursor, f"MATCH (n:{label}) RETURN count(n)")[0][0]


def _nested(cursor, query: str, prefix: str = "") -> list:
    rows = execute_and_fetch_all(cursor, f"{prefix}CALL nested_query.run('{query}') YIELD row RETURN row")
    return [row[0] for row in rows]


@pytest.fixture
def cursor():
    """A session with branch `b` forked from main (one :V vertex); the branch is dropped afterwards."""
    with contextlib.closing(connect()) as connection:
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "CREATE (:V)")
        execute_and_fetch_all(cursor, "CREATE BRANCH b")
        yield cursor
        execute_and_fetch_all(cursor, "USE BRANCH main")
        execute_and_fetch_all(cursor, "DROP BRANCH b")
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")


def test_nested_query_runs_on_the_session_branch(cursor):
    execute_and_fetch_all(cursor, "USE BRANCH b")
    execute_and_fetch_all(cursor, "CREATE (:OnBranch)")

    assert _nested(cursor, "MATCH (n:OnBranch) RETURN count(n) AS c") == [{"c": 1}]
    assert _nested(cursor, "MATCH (n:V) RETURN count(n) AS c") == [{"c": 1}]
    _nested(cursor, "CREATE (:FromModule)")
    assert _count(cursor, "FromModule") == 1

    execute_and_fetch_all(cursor, "USE BRANCH main")
    assert _count(cursor, "FromModule") == 0
    assert _count(cursor, "OnBranch") == 0


def test_python_procedure_reads_and_writes_the_session_branch(cursor):
    execute_and_fetch_all(cursor, "USE BRANCH b")
    execute_and_fetch_all(cursor, "CREATE (:OnBranch)")

    assert execute_and_fetch_all(cursor, "CALL branch_probe.count('OnBranch') YIELD count RETURN count") == [(1,)]
    assert execute_and_fetch_all(cursor, "CALL branch_probe.count('V') YIELD count RETURN count") == [(1,)]
    assert execute_and_fetch_all(cursor, "CALL branch_probe.create('FromPython') YIELD created RETURN created") == [
        (True,)
    ]
    assert _count(cursor, "FromPython") == 1

    execute_and_fetch_all(cursor, "USE BRANCH main")
    assert _count(cursor, "FromPython") == 0
    assert execute_and_fetch_all(cursor, "CALL branch_probe.count('OnBranch') YIELD count RETURN count") == [(0,)]


def test_nested_query_follows_using_branch(cursor):
    execute_and_fetch_all(cursor, "USING BRANCH b CREATE (:OnBranch)")
    assert _nested(cursor, "MATCH (n:OnBranch) RETURN count(n) AS c", "USING BRANCH b ") == [{"c": 1}]
    assert _nested(cursor, "MATCH (n:OnBranch) RETURN count(n) AS c") == [{"c": 0}]

    execute_and_fetch_all(cursor, "USE BRANCH b")
    _nested(cursor, "CREATE (:ToMain)", "USING BRANCH main ")
    assert _count(cursor, "ToMain") == 0
    execute_and_fetch_all(cursor, "USE BRANCH main")
    assert _count(cursor, "ToMain") == 1


def test_nested_query_releases_the_branch(cursor):
    execute_and_fetch_all(cursor, "USE BRANCH b")
    _nested(cursor, "CREATE (:Tmp)")
    with pytest.raises(mgclient.DatabaseError):
        _nested(cursor, "CREATE INDEX ON :Tmp(p)")
    execute_and_fetch_all(cursor, "USE BRANCH main")
    assert execute_and_fetch_all(cursor, "SHOW BRANCH b")[0][2] == 0
    assert execute_and_fetch_all(cursor, "SHOW INDEX INFO") == []


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
