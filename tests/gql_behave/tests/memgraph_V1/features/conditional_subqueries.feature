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

Feature: Conditional subqueries
    CALL (...) { WHEN p THEN body [WHEN ...] [ELSE body] } runs the first branch whose predicate holds.

    Scenario: Each row takes the first branch whose predicate holds
        Given an empty graph
        And having executed
            """
            CREATE (alice:Person {name: 'Alice', age: 65}), (bob:Person {name: 'Bob', age: 25}),
                   (charlie:Person {name: 'Charlie', age: 61}), (daniel:Person {name: 'Daniel', age: 39}),
                   (eskil:Person {name: 'Eskil', age: 39})
            """
        When executing query:
            """
            MATCH (n:Person)
            CALL (*) {
              WHEN n.age > 60 THEN { RETURN 'old' AS finalOutput }
              WHEN n.age > 30 THEN { RETURN 'mid' AS finalOutput }
              ELSE { RETURN 'young' AS finalOutput }
            }
            RETURN n.name AS name, finalOutput
            ORDER BY name
            """
        Then the result should be, in order:
            | name      | finalOutput |
            | 'Alice'   | 'old'       |
            | 'Bob'     | 'young'     |
            | 'Charlie' | 'old'       |
            | 'Daniel'  | 'mid'       |
            | 'Eskil'   | 'mid'       |

    Scenario: A later predicate and an untaken branch are not evaluated
        Given an empty graph
        When executing query:
            """
            UNWIND [0, 1] AS i
            CALL (i) {
              WHEN i = 0 THEN RETURN 'zero' AS x
              WHEN 1 / i = 1 THEN RETURN 'one' AS x
              ELSE RETURN 1 / i AS x
            }
            RETURN i, x
            ORDER BY i
            """
        Then the result should be, in order:
            | i | x      |
            | 0 | 'zero' |
            | 1 | 'one'  |

    Scenario: A null predicate does not match
        Given an empty graph
        When executing query:
            """
            UNWIND [{}] AS m
            CALL (m) {
              WHEN null THEN RETURN 'null literal' AS x
              WHEN m.a > 1 THEN RETURN 'gt' AS x
              WHEN NOT (m.a > 1) THEN RETURN 'le' AS x
              ELSE RETURN 'else' AS x
            }
            RETURN x
            """
        Then the result should be:
            | x      |
            | 'else' |

    Scenario: A returning body with no matching branch drops the row
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2, 3] AS i
            CALL (i) { WHEN i = 2 THEN RETURN i * 10 AS x }
            RETURN i, x
            """
        Then the result should be:
            | i | x  |
            | 2 | 20 |

    Scenario: OPTIONAL CALL keeps a row no branch matches, with null columns
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2, 3] AS i
            OPTIONAL CALL (i) { WHEN i = 1 THEN RETURN 'a' AS x WHEN i = 2 THEN UNWIND [] AS z RETURN z AS x }
            RETURN i, x
            ORDER BY i
            """
        Then the result should be, in order:
            | i | x    |
            | 1 | 'a'  |
            | 2 | null |
            | 3 | null |

    Scenario: A RETURN-less body keeps every row and runs only the matching branch
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2, 3] AS i
            CALL (i) { WHEN i = 1 THEN CREATE (:T {v: i}) WHEN i = 2 THEN CREATE (:T {v: i * 10}) }
            RETURN i
            ORDER BY i
            """
        Then the result should be:
            | i |
            | 1 |
            | 2 |
            | 3 |
        When executing control query:
            """
            MATCH (t:T) RETURN t.v AS v ORDER BY v
            """
        Then the result should be:
            | v  |
            | 1  |
            | 20 |

    Scenario: A branch yields each of its rows, and a branch with no rows drops the input row
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2, 3] AS i
            CALL (i) {
              WHEN i = 1 THEN UNWIND [10, 20, 30] AS j RETURN j AS x
              WHEN i = 2 THEN UNWIND [] AS j RETURN j AS x
              ELSE RETURN 0 AS x
            }
            RETURN i, x
            ORDER BY i, x
            """
        Then the result should be, in order:
            | i | x  |
            | 1 | 10 |
            | 1 | 20 |
            | 1 | 30 |
            | 3 | 0  |

    Scenario: An aggregation runs only in the rows that take its branch, and an untaken one yields no row
        Given an empty graph
        And having executed
            """
            CREATE (alice:Person {name: 'Alice', age: 65}), (bob:Person {name: 'Bob', age: 25}),
                   (charlie:Person {name: 'Charlie', age: 61}), (daniel:Person {name: 'Daniel', age: 39}),
                   (bob)-[:WORKS_FOR]->(alice), (alice)-[:WORKS_FOR]->(daniel), (charlie)-[:WORKS_FOR]->(daniel)
            """
        When executing query:
            """
            MATCH (n:Person)
            CALL (n) { WHEN n.age > 50 THEN MATCH (n)<-[:WORKS_FOR]-(e) RETURN count(e) AS c WHEN n.age < 30 THEN RETURN -1 AS c }
            RETURN n.name AS name, c
            ORDER BY name
            """
        Then the result should be, in order:
            | name      | c  |
            | 'Alice'   | 1  |
            | 'Bob'     | -1 |
            | 'Charlie' | 0  |

    Scenario: A star in one branch names the same columns as an explicit RETURN, without the imports
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2] AS i
            CALL (i) { WHEN i = 1 THEN WITH i, 2 AS z RETURN * ELSE RETURN 3 AS z }
            RETURN i, z
            ORDER BY i
            """
        Then the result should be, in order:
            | i | z |
            | 1 | 2 |
            | 2 | 3 |

    Scenario: A star column keeps its value on every row its body yields
        Given an empty graph
        When executing query:
            """
            CALL () {
              WHEN true THEN WITH 'abc' AS s, [1, 2] AS l, {a: 1} AS m UNWIND [1, 2, 3] AS i RETURN *
              ELSE RETURN 'z' AS s, [] AS l, {} AS m, 0 AS i
            }
            RETURN s, l, m, i
            ORDER BY i
            """
        Then the result should be, in order:
            | s     | l      | m      | i |
            | 'abc' | [1, 2] | {a: 1} | 1 |
            | 'abc' | [1, 2] | {a: 1} | 2 |
            | 'abc' | [1, 2] | {a: 1} | 3 |

    Scenario Outline: A body that returns only imports still drops a row no branch yields
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2] AS i
            CALL (i) { <body> }
            RETURN i
            """
        Then the result should be:
            | i |
            | 1 |

        Examples:
            | body                                                    |
            | WHEN i = 1 THEN RETURN i                                |
            | WHEN i = 1 THEN RETURN *                                |
            | WHEN i = 2 THEN UNWIND [] AS z RETURN i ELSE RETURN i   |

    Scenario: A CASE expression may sit in a predicate and in a branch's RETURN
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2, 3] AS i
            CALL (i) {
              WHEN CASE WHEN i = 1 THEN true WHEN i = 2 THEN false ELSE null END THEN RETURN CASE i WHEN 1 THEN 'a' ELSE 'b' END AS x
              ELSE RETURN CASE WHEN i = 2 THEN 'c' ELSE 'd' END AS x
            }
            RETURN i, x
            ORDER BY i
            """
        Then the result should be, in order:
            | i | x   |
            | 1 | 'a' |
            | 2 | 'c' |
            | 3 | 'd' |

    Scenario: A branch nests a conditional directly or in its own CALL
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2, 3, 4] AS i
            CALL (i) {
              WHEN i < 3 THEN { WHEN EXISTS { UNWIND range(2, i) AS u RETURN u } THEN RETURN 'two' AS x ELSE RETURN 'one' AS x }
              ELSE {
                CALL (i) { WHEN i = 3 THEN RETURN 'three' AS y ELSE RETURN 'four' AS y }
                RETURN y AS x
              }
            }
            RETURN i, x
            ORDER BY i
            """
        Then the result should be, in order:
            | i | x       |
            | 1 | 'one'   |
            | 2 | 'two'   |
            | 3 | 'three' |
            | 4 | 'four'  |

    Scenario: A braced branch may contain UNION
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2] AS i
            CALL (i) { WHEN i = 1 THEN { RETURN 'a' AS x UNION RETURN 'b' AS x } ELSE RETURN 'c' AS x }
            RETURN i, x
            ORDER BY i, x
            """
        Then the result should be, in order:
            | i | x   |
            | 1 | 'a' |
            | 1 | 'b' |
            | 2 | 'c' |

    Scenario Outline: A predicate may read the graph through EXISTS, COUNT or a pattern
        # exists() is memgraph syntax.
        Given an empty graph
        And having executed
            """
            CREATE (alice:Person {name: 'Alice'}), (bob:Person {name: 'Bob'}), (charlie:Person {name: 'Charlie'}),
                   (eskil:Person {name: 'Eskil'}), (bob)-[:LOVES]->(eskil), (charlie)-[:LOVES]->(alice),
                   (charlie)-[:LOVES]->(bob)
            """
        When executing query:
            """
            MATCH (n:Person)
            CALL (n) { WHEN <predicate> THEN RETURN 'lover' AS x ELSE RETURN 'no' AS x }
            RETURN n.name AS name, x
            ORDER BY name
            """
        Then the result should be, in order:
            | name      | x       |
            | 'Alice'   | 'no'    |
            | 'Bob'     | <bob>   |
            | 'Charlie' | 'lover' |
            | 'Eskil'   | 'no'    |

        Examples:
            | predicate                           | bob     |
            | EXISTS { (n)-[:LOVES]->() }         | 'lover' |
            | (n)-[:LOVES]->()                    | 'lover' |
            | exists((n)-[:LOVES]->())            | 'lover' |
            | COUNT { (n)-[:LOVES]->() } >= 2     | 'no'    |

    Scenario: A RETURN-less body runs IN TRANSACTIONS
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2, 3] AS i
            CALL (i) { WHEN i <> 2 THEN CREATE (:TX {v: i}) } IN TRANSACTIONS OF 1 ROWS
            RETURN i
            ORDER BY i
            """
        Then the result should be:
            | i |
            | 1 |
            | 2 |
            | 3 |
        When executing control query:
            """
            MATCH (t:TX) RETURN count(t) AS c
            """
        Then the result should be:
            | c |
            | 2 |

    Scenario: A returning body runs IN TRANSACTIONS
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2, 3] AS i
            CALL (i) { WHEN i <> 2 THEN CREATE (t:TX {v: i}) RETURN t.v AS v } IN TRANSACTIONS OF 1 ROWS
            RETURN i, v
            ORDER BY i
            """
        Then the result should be, in order:
            | i | v |
            | 1 | 1 |
            | 3 | 3 |

    Scenario Outline: A write in any branch is visible after the CALL
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2, 3] AS i
            CALL (i) { <body> }
            MATCH (w:W)
            RETURN i, x, count(w) AS c
            ORDER BY i
            """
        Then the result should be, in order:
            | i | x | c |
            | 1 | 1 | 2 |
            | 2 | 1 | 2 |
            | 3 | 2 | 2 |

        Examples:
            | body                                                                                                     |
            | WHEN i = 1 THEN CREATE (:W) RETURN 1 AS x WHEN i = 2 THEN MERGE (:W {v: i}) RETURN 1 AS x ELSE RETURN 2 AS x |
            | WHEN i < 3 THEN { WHEN true THEN CREATE (:W) RETURN 1 AS x ELSE RETURN 3 AS x } ELSE RETURN 2 AS x       |
            | WHEN i < 3 THEN { CREATE (:W) RETURN 1 AS x UNION ALL RETURN 1 AS x LIMIT 0 } ELSE RETURN 2 AS x          |
            | WHEN i < 3 THEN FOREACH (k IN [1] \| CREATE (:W)) RETURN 1 AS x ELSE RETURN 2 AS x                        |
            # Control: a write in the last branch.
            | WHEN i = 3 THEN RETURN 2 AS x ELSE CREATE (:W) RETURN 1 AS x                                             |

    Scenario: A write procedure in a branch is visible after the CALL
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2, 3] AS i
            CALL (i) {
              WHEN i < 3 THEN CALL example_c.write_procedure('v') YIELD created_vertex RETURN 1 AS x
              ELSE RETURN 2 AS x
            }
            MATCH (n)
            RETURN i, x, count(n) AS c
            ORDER BY i
            """
        Then the result should be, in order:
            | i | x | c |
            | 1 | 1 | 2 |
            | 2 | 1 | 2 |
            | 3 | 2 | 2 |

    Scenario: A MERGE before a WITH in a branch is visible to every outer row
        Given an empty graph
        When executing query:
            """
            UNWIND [0, 1] AS i
            CALL (i) {
              WHEN i < 5 THEN MERGE (n:R {id: 1}) ON CREATE SET n.k = 0 ON MATCH SET n.k = 1 WITH n RETURN n
              ELSE RETURN null AS n
            }
            RETURN i, n.k AS k
            ORDER BY i
            """
        Then the result should be, in order:
            | i | k |
            | 0 | 1 |
            | 1 | 1 |

    Scenario: A MERGE in a branch that runs for some rows only
        Given an empty graph
        And having executed
            """
            CREATE (alice:Person {name: 'Alice', age: 65}), (bob:Person {name: 'Bob', age: 25}),
                   (charlie:Person {name: 'Charlie', age: 61}), (daniel:Person {name: 'Daniel', age: 39}),
                   (eskil:Person {name: 'Eskil', age: 39}), (bob)-[:WORKS_FOR]->(alice),
                   (alice)-[:WORKS_FOR]->(daniel), (charlie)-[:WORKS_FOR]->(daniel)
            """
        When executing query:
            """
            MATCH (n:Person)
            OPTIONAL MATCH (n)-[:WORKS_FOR]->(manager:Person)
            CALL (*) {
              WHEN manager IS NULL THEN {
                MERGE (newManager:Person {name: 'Peter', age: 36})
                MERGE (n)-[:WORKS_FOR]->(newManager)
                RETURN newManager, n.name AS employee
              }
            }
            RETURN newManager.name AS newManager, collect(employee) AS employees
            """
        Then the result should be (ignoring element order for lists):
            | newManager | employees           |
            | 'Peter'    | ['Daniel', 'Eskil'] |
        When executing control query:
            """
            MATCH (:Person)-[r:WORKS_FOR]->(:Person {name: 'Peter'}) RETURN count(r) AS c
            """
        Then the result should be:
            | c |
            | 2 |

    Scenario: A second conditional reads what the first one wrote on other rows
        # Bob is created, and so scanned, before Alice: her group is set only when every row passed the first CALL.
        Given an empty graph
        And having executed
            """
            CREATE (bob:Person {name: 'Bob', age: 25}), (alice:Person {name: 'Alice', age: 65}),
                   (charlie:Person {name: 'Charlie', age: 61}), (daniel:Person {name: 'Daniel', age: 39}),
                   (eskil:Person {name: 'Eskil', age: 39}), (bob)-[:WORKS_FOR]->(alice),
                   (alice)-[:WORKS_FOR]->(daniel), (charlie)-[:WORKS_FOR]->(daniel)
            """
        When executing query:
            """
            MATCH (n:Person)
            OPTIONAL MATCH (n)-[r:WORKS_FOR]->(m:Person)
            CALL (*) {
              WHEN n.age > 60 THEN { SET n.ageGroup = 'Veteran' RETURN n.ageGroup AS ageGroup }
              WHEN n.age >= 35 AND n.age <= 59 THEN { SET n.ageGroup = 'Senior' RETURN n.ageGroup AS ageGroup }
              ELSE { SET n.ageGroup = 'Junior' RETURN n.ageGroup AS ageGroup }
            }
            CALL (*) {
              WHEN m.age > n.age THEN { RETURN collect([m.name, m.ageGroup]) AS manager }
            }
            RETURN n.name AS name, ageGroup, manager
            """
        Then the result should be:
            | name  | ageGroup | manager                  |
            | 'Bob' | 'Junior' | [['Alice', 'Veteran']]   |

    Scenario Outline: The columns of a multi-branch body are visible to a later star
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2] AS i
            CALL (i) { WHEN i = 1 THEN RETURN 10 AS a, 'x' AS b ELSE RETURN 'y' AS b, 20 AS a }
            <tail>
            """
        Then the result should be, in order:
            | a  | b   | i |
            | 10 | 'x' | 1 |
            | 20 | 'y' | 2 |

        Examples:
            | tail                                                              |
            | RETURN * ORDER BY i                                               |
            | WITH * RETURN a, b, i ORDER BY i                                  |
            | CALL (*) { RETURN a + i AS s } RETURN s - i AS a, b, i ORDER BY i |

    Scenario: A branch ending in a standalone procedure call is RETURN-less
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2] AS i
            CALL (i) { WHEN i = 1 THEN CALL mg.procedures() YIELD name }
            RETURN i
            ORDER BY i
            """
        Then the result should be:
            | i |
            | 1 |
            | 2 |

    Scenario: A later pattern comprehension predicate is not evaluated when an earlier predicate is true
        # EXISTS, COUNT and COLLECT pull to a closure; a comprehension needs its fold deferred to the predicate.
        Given an empty graph
        And having executed:
            """
            CREATE (:A {k: 1})-[:R]->(:B {k: 2}), (:A {k: 3}), (:A {k: 5})-[:R]->(:B {k: 6})
            """
        And parameters are:
            | z | 0 |
        When executing query:
            """
            MATCH (a:A)
            CALL (a) { WHEN a.k > 0 THEN RETURN 'pos' AS x WHEN size([(a)-[:R]->(b) WHERE b.k / $z > 0 | b.k]) > 0 THEN RETURN 'other' AS x }
            RETURN a.k AS k, x
            ORDER BY k
            """
        Then the result should be, in order:
            | k | x     |
            | 1 | 'pos' |
            | 3 | 'pos' |
            | 5 | 'pos' |

    Scenario: An EXISTS operand after a true OR operand is not evaluated
        Given an empty graph
        And having executed:
            """
            CREATE (:A {k: 1})-[:R]->(:B {k: 2}), (:A {k: 3}), (:A {k: 5})-[:R]->(:B {k: 6})
            """
        And parameters are:
            | z | 0 |
        When executing query:
            """
            MATCH (a:A)
            CALL (a) { WHEN a.k > 0 OR EXISTS { MATCH (a)-[:R]->(b) WHERE b.k / $z > 0 } THEN RETURN 'pos' AS x }
            RETURN a.k AS k, x
            ORDER BY k
            """
        Then the result should be, in order:
            | k | x     |
            | 1 | 'pos' |
            | 3 | 'pos' |
            | 5 | 'pos' |

    Scenario: A reached subquery predicate still raises its error
        Given an empty graph
        And having executed:
            """
            CREATE (:A {k: 1})-[:R]->(:B {k: 2}), (:A {k: 3}), (:A {k: 5})-[:R]->(:B {k: 6})
            """
        And parameters are:
            | z | 0 |
        When executing query:
            """
            MATCH (a:A)
            CALL (a) { WHEN a.k > 4 THEN RETURN 'big' AS x WHEN EXISTS { MATCH (a)-[:R]->(b) WHERE b.k / $z > 0 } THEN RETURN 'out' AS x }
            RETURN a.k AS k, x
            """
        Then an error should be raised

    Scenario: Subquery predicates choose per row
        Given an empty graph
        And having executed:
            """
            CREATE (:A {k: 1})-[:R]->(:B {k: 2}), (:A {k: 3}), (:A {k: 5})-[:R]->(:B {k: 6})
            """
        When executing query:
            """
            MATCH (a:A)
            CALL (a) { WHEN COUNT { (a)-->() } = 1 AND a.k > 4 THEN RETURN 'far' AS x WHEN EXISTS { (a)-->() } THEN RETURN 'near' AS x ELSE RETURN 'none' AS x }
            RETURN a.k AS k, x
            ORDER BY k
            """
        Then the result should be, in order:
            | k | x      |
            | 1 | 'near' |
            | 3 | 'none' |
            | 5 | 'far'  |

    Scenario: A property predicate sees a property an earlier row's branch set
        Given an empty graph
        And having executed:
            """
            CREATE (:C {v: 0})
            """
        When executing query:
            """
            MATCH (c:C)
            UNWIND [1, 2, 3] AS i
            CALL (c, i) { WHEN c.v < 2 THEN SET c.v = c.v + 1 RETURN 'inc' AS x ELSE RETURN 'skip' AS x }
            RETURN i, x
            ORDER BY i
            """
        Then the result should be, in order:
            | i | x      |
            | 1 | 'inc'  |
            | 2 | 'inc'  |
            | 3 | 'skip' |

    Scenario: Branch columns are matched by name, in any order and of any type
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2, 3] AS i
            CALL (i) { WHEN i = 1 THEN RETURN 'b' AS y, 1 AS x WHEN i = 2 THEN RETURN 2 AS x, 20 AS y ELSE RETURN 3 AS x, null AS y }
            RETURN i, x, y
            ORDER BY i
            """
        Then the result should be, in order:
            | i | x | y    |
            | 1 | 1 | 'b'  |
            | 2 | 2 | 20   |
            | 3 | 3 | null |

    Scenario: A pattern comprehension after a true OR operand in one predicate is still evaluated
        # The comprehension is planned eagerly inside one predicate, as in WHERE.
        Given an empty graph
        And having executed:
            """
            CREATE (:A {k: 1})-[:R]->(:B {k: 2}), (:A {k: 3}), (:A {k: 5})-[:R]->(:B {k: 6})
            """
        And parameters are:
            | z | 0 |
        When executing query:
            """
            MATCH (a:A)
            CALL (a) { WHEN a.k > 0 OR size([(a)-[:R]->(b) WHERE b.k / $z > 0 | b.k]) > 0 THEN RETURN 'pos' AS x }
            RETURN a.k AS k, x
            """
        Then an error should be raised

    Scenario Outline: A subquery predicate sees a write before the CALL
        Given an empty graph
        When executing query:
            """
            CREATE (c:C {v: 1})
            WITH c
            CALL (c) { WHEN <predicate> THEN RETURN 'seen' AS x ELSE RETURN 'none' AS x }
            RETURN x
            """
        Then the result should be:
            | x      |
            | 'seen' |

        Examples:
            | predicate                     |
            | EXISTS { MATCH (x:C) }        |
            | COUNT { MATCH (x:C) } = 1     |

    Scenario: A branch seeks a label-property index on an imported value
        Given an empty graph
        And with new index :L(p)
        And having executed
            """
            CREATE (:L {p: 1}), (:L {p: 2}), (:L {p: 2}), (:A {p: 1}), (:A {p: 2}), (:A {p: 3})
            """
        When executing query:
            """
            MATCH (a:A)
            CALL (a) { WHEN a.p > 1 THEN MATCH (m:L) WHERE m.p = a.p RETURN count(m) AS c ELSE RETURN -1 AS c }
            RETURN a.p AS p, c ORDER BY p
            """
        Then the result should be, in order:
            | p | c  |
            | 1 | -1 |
            | 2 | 2  |
            | 3 | 0  |

    Scenario: A predicate subquery seeks a label-property index on an imported value
        Given an empty graph
        And with new index :L(p)
        And having executed
            """
            CREATE (:L {p: 1}), (:L {p: 2}), (:L {p: 2}), (:A {p: 1}), (:A {p: 2}), (:A {p: 3})
            """
        When executing query:
            """
            MATCH (a:A)
            CALL (a) { WHEN COUNT { MATCH (m:L) WHERE m.p = a.p } > 1 THEN RETURN 'many' AS r ELSE RETURN 'few' AS r }
            RETURN a.p AS p, r ORDER BY p
            """
        Then the result should be, in order:
            | p | r      |
            | 1 | 'few'  |
            | 2 | 'many' |
            | 3 | 'few'  |

    Scenario: A branch seeks an edge property index on an imported value
        Given an empty graph
        And with new edge index :(w)
        And having executed
            """
            CREATE ()-[:T {w: 1}]->(), ()-[:T {w: 2}]->(), ()-[:T {w: 2}]->()
            """
        When executing query:
            """
            UNWIND [1, 2, 3] AS i
            CALL (i) { WHEN i < 3 THEN MATCH ()-[r]->() WHERE r.w = i RETURN count(r) AS c ELSE RETURN -1 AS c }
            RETURN i, c ORDER BY i
            """
        Then the result should be, in order:
            | i | c  |
            | 1 | 1  |
            | 2 | 2  |
            | 3 | -1 |

    Scenario: A branch hash-joins two patterns
        Given an empty graph
        And having executed
            """
            CREATE (:X {p: 1}), (:X {p: 2}), (:Z {p: 2}), (:Z {p: 2}), (:Z {p: 3})
            """
        When executing query:
            """
            UNWIND [1, 2] AS i
            CALL (i) { WHEN i = 1 THEN MATCH (x:X), (z:Z) WHERE z.p = x.p RETURN count(*) AS c ELSE RETURN -1 AS c }
            RETURN i, c ORDER BY i
            """
        Then the result should be, in order:
            | i | c  |
            | 1 | 2  |
            | 2 | -1 |
