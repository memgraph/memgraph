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

# SKELETON (not wired into workloads.yaml). mgp_execute_query is only reachable from a C++ module
# (mgp::ExecuteQuery), so this needs a procedure `versioning_mgp.run(query :: STRING)` added to
# query_modules/query_modules/module.cpp that calls mgp::ExecuteQuery(graph, query) and returns the rows,
# and the cluster started with --versioning-enabled (enterprise license). The unit test
# tests/unit/versioning_mgp.cpp covers the attach itself (CurrentDB::AttachBranchOf).

import sys

import pytest
from common import connect, execute_and_fetch_all

pytestmark = pytest.mark.skip(reason="needs versioning_mgp.run in the e2e C++ module")


def test_nested_query_runs_on_callers_branch():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE (:V {id: 1})")
    execute_and_fetch_all(cursor, "CREATE BRANCH 'b' FROM 'main'")
    execute_and_fetch_all(cursor, "CHECKOUT BRANCH 'b'")
    execute_and_fetch_all(cursor, "CREATE (:OnlyOnBranch)")

    # Reads see the branch.
    rows = execute_and_fetch_all(cursor, "CALL versioning_mgp.run('MATCH (n:OnlyOnBranch) RETURN count(n) AS c') YIELD row RETURN row")
    assert rows[0][0]["c"] == 1

    # Writes land on the branch, not on main.
    execute_and_fetch_all(cursor, "CALL versioning_mgp.run('CREATE (:FromModule)') YIELD row RETURN row")
    assert execute_and_fetch_all(cursor, "MATCH (n:FromModule) RETURN count(n)")[0][0] == 1
    execute_and_fetch_all(cursor, "CHECKOUT BRANCH 'main'")
    assert execute_and_fetch_all(cursor, "MATCH (n:FromModule) RETURN count(n)")[0][0] == 0


def test_nested_query_under_using_version_main_runs_on_main():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE BRANCH 'b2' FROM 'main'")
    execute_and_fetch_all(cursor, "CHECKOUT BRANCH 'b2'")
    execute_and_fetch_all(
        cursor, "USING VERSION 'main' CALL versioning_mgp.run('CREATE (:ToMain)') YIELD row RETURN row"
    )
    execute_and_fetch_all(cursor, "CHECKOUT BRANCH 'main'")
    assert execute_and_fetch_all(cursor, "MATCH (n:ToMain) RETURN count(n)")[0][0] == 1


def test_schema_assert_rejected_on_branch():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE (:V {id: 1})")
    execute_and_fetch_all(cursor, "CREATE BRANCH 'b3' FROM 'main'")
    execute_and_fetch_all(cursor, "CHECKOUT BRANCH 'b3'")

    with pytest.raises(Exception, match="versioning branch"):
        execute_and_fetch_all(cursor, "CALL schema.assert({V: ['id']}, {}) YIELD label RETURN label")

    # The session is still usable and main is unaffected.
    assert execute_and_fetch_all(cursor, "MATCH (n:V) RETURN count(n)")[0][0] == 1
    execute_and_fetch_all(cursor, "CHECKOUT BRANCH 'main'")
    assert execute_and_fetch_all(cursor, "SHOW INDEX INFO") == []


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
