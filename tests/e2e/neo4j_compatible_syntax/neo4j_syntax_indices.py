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

import sys

import pytest
from common import cursor, execute_and_fetch_all


def _index_rows(cursor):
    return [(row[0], row[1], row[2]) for row in execute_and_fetch_all(cursor, "SHOW INDEX INFO")]


def _constraint_rows(cursor):
    return execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO")


def test_neo4j_syntax_create_and_drop_indices(cursor):
    execute_and_fetch_all(cursor, "CREATE INDEX person_name_idx FOR (n:Person) ON (n.name)")
    execute_and_fetch_all(cursor, "CREATE INDEX person_full_idx FOR (n:Person) ON (n.first, n.last)")
    execute_and_fetch_all(cursor, "CREATE INDEX knows_since_idx FOR ()-[r:KNOWS]-() ON (r.since)")

    rows = _index_rows(cursor)
    assert ("label+property", "Person", ["name"]) in rows
    assert ("label+property", "Person", ["first", "last"]) in rows
    assert ("edge-type+property", "KNOWS", "since") in rows

    execute_and_fetch_all(cursor, "DROP INDEX ON :Person(name)")
    execute_and_fetch_all(cursor, "DROP INDEX ON :Person(first, last)")
    execute_and_fetch_all(cursor, "DROP EDGE INDEX ON :KNOWS(since)")

    assert _index_rows(cursor) == []


def test_neo4j_syntax_create_and_drop_constraints(cursor):
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT person_unique FOR (n:Person) REQUIRE n.email IS UNIQUE")
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT person_exists FOR (n:Person) REQUIRE n.name IS NOT NULL")
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT person_typed FOR (n:Person) REQUIRE n.age IS :: INTEGER")

    rows = _constraint_rows(cursor)
    assert ("unique", "Person", ["email"], "") in rows
    assert ("exists", "Person", "name", "") in rows
    assert ("data_type", "Person", "age", "INTEGER") in rows

    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE")
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name)")
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT n.age IS TYPED INTEGER")

    assert _constraint_rows(cursor) == []


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
