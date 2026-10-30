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


def _try_execute(cursor, query):
    """Return (rows, None) on success or (None, error text) if the query raises a DatabaseError."""
    try:
        return execute_and_fetch_all(cursor, query), None
    except mgclient.DatabaseError as e:
        return None, str(e)


def _column_names(cursor):
    return [d.name for d in cursor.description]


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
    assert any("ScanAllByEdgeTypeProperty (a)-[r:KNOWS {since}]->(b)" in line for line in edge_plan), edge_plan

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


def test_boolean_map_key_literal(cursor):
    rows = execute_and_fetch_all(cursor, "RETURN {false:1} AS m, {`false`:1} AS ctrl_m")
    assert rows[0][0] == {"false": 1}
    assert rows[0][1] == {"false": 1}
    assert execute_and_fetch_all(cursor, "RETURN keys({false:1}) AS k")[0][0] == ["false"]
    assert execute_and_fetch_all(cursor, "RETURN keys({`false`:1}) AS k")[0][0] == ["false"]
    assert execute_and_fetch_all(cursor, "RETURN keys({fAlse:1}) AS k")[0][0] == ["fAlse"]
    assert execute_and_fetch_all(cursor, "WITH {false:1} AS m RETURN m['false'] AS v, m['true'] AS v2")[0] == (1, None)
    rows, err = _try_execute(cursor, "RETURN keys({true:1,false:2}) AS k")
    assert err is None and sorted(rows[0][0]) == ["false", "true"], (rows, err)
    # Control: a boolean in expression position is still a literal.
    assert execute_and_fetch_all(cursor, "RETURN false")[0][0] is False
    rows, err = _try_execute(cursor, "RETURN false, true")
    assert err is None and rows == [(False, True)], (rows, err)
    cursor.execute("RETURN 1 AS false")
    assert _column_names(cursor) == ["false"]


def test_boolean_name_positions(cursor):
    assert execute_and_fetch_all(cursor, "CREATE (n) SET n.p = {false:1} RETURN n.p AS p")[0][0] == {"false": 1}
    assert execute_and_fetch_all(cursor, "CREATE (n) SET n = {false:1} RETURN properties(n) AS props")[0][0] == {
        "false": 1
    }
    assert execute_and_fetch_all(cursor, "CREATE (n) SET n.false = 1 RETURN properties(n) AS props")[0][0] == {
        "false": 1
    }
    assert execute_and_fetch_all(cursor, "WITH {a:1} AS n RETURN n{false: 1} AS m")[0][0] == {"false": 1}
    assert execute_and_fetch_all(cursor, "WITH {a:1} AS n RETURN n{.false} AS m")[0][0] == {"false": None}
    # Shorthand `n{false}` names a variable; the unbound error names the written spelling.
    rows, err = _try_execute(cursor, "WITH {a:1} AS n RETURN n{false} AS m")
    assert err is not None and "Unbound variable: false." in err, (rows, err)
    assert execute_and_fetch_all(cursor, "CREATE (n:false) RETURN labels(n) AS l")[0][0] == ["false"]
    assert execute_and_fetch_all(cursor, "CREATE (a)-[r:false]->(b) RETURN type(r) AS t")[0][0] == "false"
    cursor.execute("MATCH (n)-[false]->(m) RETURN *")
    assert sorted(_column_names(cursor)) == ["false", "m", "n"]
    cursor.execute("MATCH (a)-[*WSHORTEST (r, n | 1) false]->(b) RETURN *")
    assert sorted(_column_names(cursor)) == ["a", "b", "false"]
    cursor.execute("MATCH (false)-[*WSHORTEST (true, n | 1) tw]->(b) RETURN *")
    assert sorted(_column_names(cursor)) == ["b", "false", "tw"]
    # `false.l` does not resolve through a different binding named `true`.
    rows, err = _try_execute(cursor, "CREATE (true {l:'K'}) CREATE (n: false.l) RETURN 1")
    assert err is not None and "Unbound variable: false." in err, (rows, err)
    rows, err = _try_execute(cursor, "RETURN e::false")
    assert err is not None and "'false'" in err, (rows, err)
    execute_and_fetch_all(cursor, "SET PARAMETER false = 1")
    execute_and_fetch_all(cursor, "SET PARAMETER true = 2")
    names = [row[0] for row in execute_and_fetch_all(cursor, "SHOW PARAMETERS")]
    assert "false" in names and "true" in names


def test_boolean_ddl_schema_keys(cursor):
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Lk(false)")
    assert ("label+property", "Lk", ["false"], 0) in _index_rows(cursor)
    # Index DDL variables `false` and `true` are different variables.
    rows, err = _try_execute(cursor, "CREATE INDEX FOR (false:Lk2) ON (true.x)")
    assert err is not None, (rows, err)
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT FOR (n:Lc) REQUIRE (n.false) IS UNIQUE")
    assert ("unique", "Lc", ["false"], "") in _constraint_rows(cursor)
    # Constraint DDL variables `false` and `true` are different variables.
    rows, err = _try_execute(cursor, "CREATE CONSTRAINT FOR (false:Lc2) REQUIRE (true.x) IS UNIQUE")
    assert err is not None, (rows, err)
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Lh(false)")
    plan = _explain(cursor, "USING INDEX :Lh(false) MATCH (n:Lh) WHERE n.false = 1 RETURN n")
    assert any("ScanAllByLabelProperties (n :Lh {false})" in line for line in plan), plan


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
