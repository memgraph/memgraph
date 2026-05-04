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

import mgclient
import pytest
from common import cursor, execute_and_fetch_all


def _index_rows(cursor):
    return [(row[0], row[1], row[2], row[3]) for row in execute_and_fetch_all(cursor, "SHOW INDEX INFO")]


def _explain(cursor, query):
    return [row[0] for row in execute_and_fetch_all(cursor, f"EXPLAIN {query}")]


def _constraint_rows(cursor):
    return execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO")


def test_neo4j_syntax_create_and_drop_indices(cursor):
    # Mixed dataset: some nodes/edges should land in each index, others should not.
    execute_and_fetch_all(
        cursor,
        """
        CREATE (alice:Person {name: 'Alice'}),
               (bob:Person {name: 'Bob'}),
               (charlie:Person {name: 'Charlie'}),
               (:Person {first: 'John', last: 'Smith'}),
               (:Person {first: 'Jane', last: 'Doe'}),
               (:Person {age: 30}),
               (:Animal {name: 'Rex'}),
               (:Animal {name: 'Whiskers'}),
               (alice)-[:KNOWS {since: 2020}]->(bob),
               (bob)-[:KNOWS {since: 2021}]->(charlie),
               (alice)-[:KNOWS {since: 2022}]->(charlie),
               (alice)-[:KNOWS]->(bob),
               (alice)-[:LIKES {since: 2023}]->(bob)
        """,
    )

    execute_and_fetch_all(cursor, "CREATE INDEX person_name_idx FOR (n:Person) ON (n.name)")
    execute_and_fetch_all(cursor, "CREATE INDEX person_full_idx FOR (n:Person) ON (n.first, n.last)")
    execute_and_fetch_all(cursor, "CREATE INDEX knows_since_idx FOR ()-[r:KNOWS]-() ON (r.since)")

    rows = _index_rows(cursor)
    # Only Person nodes with the indexed properties contribute to the count;
    # Animal nodes and Persons missing the properties are excluded.
    assert ("label+property", "Person", ["name"], 3) in rows
    assert ("label+property", "Person", ["first", "last"], 2) in rows
    # Only KNOWS edges with `since` set are counted; the bare KNOWS and LIKES are excluded.
    assert ("edge-type+property", "KNOWS", "since", 3) in rows

    # EXPLAIN: each indexed read should pick the matching scan operator.
    name_plan = _explain(cursor, "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n")
    assert any("ScanAllByLabelProperties (n :Person {name})" in line for line in name_plan), name_plan

    composite_plan = _explain(
        cursor,
        "MATCH (n:Person) WHERE n.first = 'John' AND n.last = 'Smith' RETURN n",
    )
    assert any("ScanAllByLabelProperties (n :Person {first, last})" in line for line in composite_plan), composite_plan

    edge_plan = _explain(cursor, "MATCH (a)-[r:KNOWS]->(b) WHERE r.since = 2020 RETURN r")
    assert any("ScanAllByEdgeTypePropertyValue (a)-[r:KNOWS {since}]->(b)" in line for line in edge_plan), edge_plan

    # Execute the queries and check that filtering returns exactly the matching rows.
    name_results = execute_and_fetch_all(cursor, "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.name AS name")
    assert [row[0] for row in name_results] == ["Alice"]

    composite_results = execute_and_fetch_all(
        cursor,
        "MATCH (n:Person) WHERE n.first = 'John' AND n.last = 'Smith' " "RETURN n.first AS first, n.last AS last",
    )
    assert composite_results == [("John", "Smith")]

    edge_results = execute_and_fetch_all(
        cursor, "MATCH (a)-[r:KNOWS]->(b) WHERE r.since = 2020 RETURN r.since AS since"
    )
    assert [row[0] for row in edge_results] == [2020]

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

    # Insert a valid Person that satisfies all three constraints.
    execute_and_fetch_all(cursor, "CREATE (:Person {email: 'alice@example.com', name: 'Alice', age: 30})")

    # Unique violation: another Person with the same email.
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(cursor, "CREATE (:Person {email: 'alice@example.com', name: 'Alice2', age: 31})")

    # Exists violation: Person without `name`.
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(cursor, "CREATE (:Person {email: 'bob@example.com', age: 25})")

    # Type violation: `age` is not an INTEGER.
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(cursor, "CREATE (:Person {email: 'carol@example.com', name: 'Carol', age: 'thirty'})")

    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE")
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name)")
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT n.age IS TYPED INTEGER")

    assert _constraint_rows(cursor) == []


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
