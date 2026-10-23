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

# Covers mgp_execute_query (nested query from a C++ module, `versioning_mgp.run`) and schema.assert on a
# versioning branch. The server runs with --versioning-enabled (enterprise license).

import contextlib
import sys
from collections.abc import Iterator

import pytest
from common import connect, execute_and_fetch_all


def _count(cursor, label: str) -> int:
    return execute_and_fetch_all(cursor, f"MATCH (n:{label}) RETURN count(n)")[0][0]


@pytest.fixture
def branch() -> Iterator[str]:
    """Forks branch `b` from main (which holds one :V vertex); the test checks it out itself. Dropped on teardown."""
    name = "b"
    with contextlib.closing(connect()) as connection:
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "CREATE (:V {id: 1})")
        execute_and_fetch_all(cursor, f"CREATE BRANCH '{name}' FROM 'main'")
    yield name
    with contextlib.closing(connect()) as connection:
        execute_and_fetch_all(connection.cursor(), f"DROP BRANCH '{name}'")


@pytest.fixture
def cursor(branch):
    """Session under test; closed before the branch fixture drops the branch (a live checkout blocks DROP)."""
    with contextlib.closing(connect()) as connection:
        yield connection.cursor()


def test_nested_query_runs_on_callers_branch(branch, cursor):
    execute_and_fetch_all(cursor, f"CHECKOUT BRANCH '{branch}'")
    execute_and_fetch_all(cursor, "CREATE (:OnlyOnBranch)")

    # Reads see the branch (and the main data it forked from).
    rows = execute_and_fetch_all(
        cursor, "CALL versioning_mgp.run('MATCH (n:OnlyOnBranch) RETURN count(n) AS c') YIELD row RETURN row"
    )
    assert [r[0]["c"] for r in rows] == [1]
    rows = execute_and_fetch_all(
        cursor, "CALL versioning_mgp.run('MATCH (n:V) RETURN count(n) AS c') YIELD row RETURN row"
    )
    assert [r[0]["c"] for r in rows] == [1]

    # Writes land on the branch, not on main.
    execute_and_fetch_all(cursor, "CALL versioning_mgp.run('CREATE (:FromModule)') YIELD row RETURN row")
    assert _count(cursor, "FromModule") == 1
    execute_and_fetch_all(cursor, "CHECKOUT BRANCH 'main'")
    assert _count(cursor, "FromModule") == 0
    assert _count(cursor, "OnlyOnBranch") == 0


def test_nested_query_under_using_version_main_runs_on_main(branch, cursor):
    execute_and_fetch_all(cursor, f"CHECKOUT BRANCH '{branch}'")
    execute_and_fetch_all(cursor, "CREATE (:OnlyOnBranch)")

    rows = execute_and_fetch_all(
        cursor,
        "USING VERSION 'main' CALL versioning_mgp.run('MATCH (n:OnlyOnBranch) RETURN count(n) AS c') YIELD row RETURN row",
    )
    assert [r[0]["c"] for r in rows] == [0]

    execute_and_fetch_all(
        cursor, "USING VERSION 'main' CALL versioning_mgp.run('CREATE (:ToMain)') YIELD row RETURN row"
    )
    # Still on the branch: the main write is invisible here, visible on main.
    assert _count(cursor, "ToMain") == 0
    execute_and_fetch_all(cursor, "CHECKOUT BRANCH 'main'")
    assert _count(cursor, "ToMain") == 1


def test_schema_assert_rejected_on_branch(branch, cursor):
    execute_and_fetch_all(cursor, f"CHECKOUT BRANCH '{branch}'")

    with pytest.raises(Exception, match="versioning branch"):
        execute_and_fetch_all(cursor, "CALL schema.assert({V: ['id']}, {}) YIELD label RETURN label")

    # The session is still usable and main is unaffected.
    assert _count(cursor, "V") == 1
    execute_and_fetch_all(cursor, "CHECKOUT BRANCH 'main'")
    assert execute_and_fetch_all(cursor, "SHOW INDEX INFO") == []


def test_drop_branch_after_nested_query():
    with contextlib.closing(connect()) as connection:
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "CREATE BRANCH 'dropme' FROM 'main'")
        execute_and_fetch_all(cursor, "CHECKOUT BRANCH 'dropme'")
        execute_and_fetch_all(cursor, "CALL versioning_mgp.run('CREATE (:Tmp)') YIELD row RETURN row")
        assert _count(cursor, "Tmp") == 1
        execute_and_fetch_all(cursor, "CHECKOUT BRANCH 'main'")

        # The nested query's attach to the branch must have been released.
        execute_and_fetch_all(cursor, "DROP BRANCH 'dropme'")
        with pytest.raises(Exception):
            execute_and_fetch_all(cursor, "CHECKOUT BRANCH 'dropme'")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
