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
    The same branches may form the body of EXISTS { }, COUNT { } and COLLECT { }.

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
        Then the result should be:
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
        Then the result should be:
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
        Then the result should be:
            | i | x    |
            | 1 | 'a'  |
            | 2 | null |
            | 3 | null |

    Scenario: A unit body keeps every row and runs only the matching branch
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
        Then the result should be:
            | i | x  |
            | 1 | 10 |
            | 1 | 20 |
            | 1 | 30 |
            | 3 | 0  |

    Scenario: An untaken aggregating branch yields no row
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2] AS i
            CALL (i) { WHEN i = 1 THEN MATCH (n:Missing) RETURN count(n) AS c }
            RETURN i, c
            """
        Then the result should be:
            | i | c |
            | 1 | 0 |

    Scenario: An aggregation runs only in the rows that take its branch
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
            CALL (n) { WHEN n.age > 50 THEN MATCH (n)<-[:WORKS_FOR]-(e) RETURN count(e) AS c ELSE RETURN -1 AS c }
            RETURN n.name AS name, c
            ORDER BY name
            """
        Then the result should be:
            | name      | c  |
            | 'Alice'   | 1  |
            | 'Bob'     | -1 |
            | 'Charlie' | 0  |
            | 'Daniel'  | -1 |

    Scenario: Branch columns are matched by name and may differ in type
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2] AS i
            CALL (i) { WHEN i = 1 THEN RETURN 1 AS x, 2 AS y ELSE RETURN 'twenty' AS y, 10 AS x }
            RETURN i, x, y
            ORDER BY i
            """
        Then the result should be:
            | i | x  | y        |
            | 1 | 1  | 2        |
            | 2 | 10 | 'twenty' |

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
            | WHEN i = 1 THEN RETURN i WHEN i = 3 THEN RETURN i       |
            | WHEN i = 2 THEN UNWIND [] AS z RETURN i ELSE RETURN i   |

    Scenario: A branch nests a conditional directly or in its own CALL
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2, 3, 4] AS i
            CALL (i) {
              WHEN i = 1 THEN RETURN 'one' AS x
              WHEN i = 2 THEN { WHEN i > 1 THEN RETURN 'two' AS x ELSE RETURN 'never' AS x }
              ELSE {
                CALL (i) { WHEN i = 3 THEN RETURN 'three' AS y ELSE RETURN 'four' AS y }
                RETURN y AS x
              }
            }
            RETURN i, x
            ORDER BY i
            """
        Then the result should be:
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
        Then the result should be:
            | i | x   |
            | 1 | 'a' |
            | 1 | 'b' |
            | 2 | 'c' |

    Scenario Outline: A predicate may read the graph through EXISTS, COUNT or a pattern
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
        Then the result should be:
            | name      | x       |
            | 'Alice'   | 'no'    |
            | 'Bob'     | <bob>   |
            | 'Charlie' | 'lover' |
            | 'Eskil'   | 'no'    |

        Examples:
            | predicate                           | bob     |
            | EXISTS { (n)-[:LOVES]->() }         | 'lover' |
            | (n)-[:LOVES]->()                    | 'lover' |
            | COUNT { (n)-[:LOVES]->() } >= 2     | 'no'    |

    Scenario: A unit body runs IN TRANSACTIONS
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
        Then the result should be:
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
        Then the result should be:
            | i | x | c |
            | 1 | 1 | 2 |
            | 2 | 1 | 2 |
            | 3 | 2 | 2 |

        Examples:
            | body                                                                                                     |
            | WHEN i = 1 THEN CREATE (:W) RETURN 1 AS x WHEN i = 2 THEN MERGE (:W {v: i}) RETURN 1 AS x ELSE RETURN 2 AS x |
            | WHEN i < 3 THEN { WHEN true THEN CREATE (:W) RETURN 1 AS x ELSE RETURN 3 AS x } ELSE RETURN 2 AS x       |
            | WHEN i < 3 THEN { CREATE (:W) RETURN 1 AS x UNION ALL RETURN 1 AS x LIMIT 0 } ELSE RETURN 2 AS x          |
            # Control: a write in the last branch.
            | WHEN i = 3 THEN RETURN 2 AS x ELSE CREATE (:W) RETURN 1 AS x                                             |

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
        Then the result should be:
            | i | a  | b   |
            | 1 | 10 | 'x' |
            | 2 | 20 | 'y' |

        Examples:
            | tail                                                              |
            | RETURN * ORDER BY i                                               |
            | WITH * RETURN i, a, b ORDER BY i                                  |
            | CALL (*) { RETURN a + i AS s } RETURN i, s - i AS a, b ORDER BY i |

    Scenario: Three branches with several columns each fill every column
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2, 3] AS i
            CALL (i) {
              WHEN i = 1 THEN RETURN 1 AS a, 'one' AS b
              WHEN i = 2 THEN RETURN 'two' AS b, 2 AS a
              ELSE RETURN 3 AS a, 'three' AS b
            }
            RETURN i, a, b
            ORDER BY i
            """
        Then the result should be:
            | i | a | b       |
            | 1 | 1 | 'one'   |
            | 2 | 2 | 'two'   |
            | 3 | 3 | 'three' |

    Scenario: A branch ending in a standalone procedure call is a unit branch
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

    Scenario Outline: A standalone procedure call branch must match the other branches
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2] AS i
            CALL (i) { <body> }
            RETURN i
            """
        Then an error should be raised

        Examples:
            | body                                                                              |
            | WHEN i = 1 THEN CALL mg.procedures() YIELD name WHERE name = 'x'                  |
            | WHEN i = 1 THEN CALL mg.procedures() YIELD name ELSE CREATE (:Q)                  |
            | WHEN i = 1 THEN CREATE (:Q) ELSE { WHEN true THEN CALL mg.procedures() YIELD name } |

    Scenario: A later EXISTS predicate is not evaluated when an earlier predicate is true
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
            CALL (a) { WHEN a.k > 0 THEN RETURN 'pos' AS x WHEN EXISTS { MATCH (a)-[:R]->(b) WHERE b.k / $z > 0 } THEN RETURN 'out' AS x }
            RETURN a.k AS k, x
            """
        Then the result should be:
            | k | x     |
            | 1 | 'pos' |
            | 3 | 'pos' |
            | 5 | 'pos' |

    Scenario: A later COUNT predicate is not evaluated when an earlier predicate is true
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
            CALL (a) { WHEN a.k > 0 THEN RETURN 'pos' AS x WHEN COUNT { UNWIND [1 / $z] AS u RETURN u } > 0 THEN RETURN 'cnt' AS x ELSE RETURN 'e' AS x }
            RETURN a.k AS k, x
            """
        Then the result should be:
            | k | x     |
            | 3 | 'pos' |
            | 5 | 'pos' |
            | 1 | 'pos' |

    Scenario: A later COLLECT predicate is not evaluated when an earlier predicate is true
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
            CALL (a) { WHEN a.k > 0 THEN RETURN 'pos' AS x WHEN size(COLLECT { UNWIND [1 / $z] AS u RETURN u }) > 0 THEN RETURN 'col' AS x }
            RETURN a.k AS k, x
            """
        Then the result should be:
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
            """
        Then the result should be:
            | k | x     |
            | 3 | 'pos' |
            | 5 | 'pos' |
            | 1 | 'pos' |

    Scenario: A pattern comprehension in a later predicate is not evaluated when an earlier predicate is true
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
            CALL (a) { WHEN a.k > 0 THEN RETURN 'pos' AS x WHEN size([(a)-[:R]->(b) WHERE b.k / $z > 0 | b.k]) > 0 THEN RETURN 'out' AS x }
            RETURN a.k AS k, x
            """
        Then the result should be:
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
        # Neo4j raises an ArithmeticError; this step cannot check the text.
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
            """
        Then the result should be:
            | k | x      |
            | 3 | 'none' |
            | 5 | 'far'  |
            | 1 | 'near' |

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
            """
        Then the result should be:
            | i | x      |
            | 1 | 'inc'  |
            | 2 | 'inc'  |
            | 3 | 'skip' |

    Scenario: Ten branches each take their own rows
        Given an empty graph
        When executing query:
            """
            UNWIND range(0, 19) AS i
            CALL (i) { WHEN i % 10 = 0 THEN RETURN 0 AS x WHEN i % 10 = 1 THEN RETURN 1 AS x WHEN i % 10 = 2 THEN RETURN 2 AS x
                       WHEN i % 10 = 3 THEN RETURN 3 AS x WHEN i % 10 = 4 THEN RETURN 4 AS x WHEN i % 10 = 5 THEN RETURN 5 AS x
                       WHEN i % 10 = 6 THEN RETURN 6 AS x WHEN i % 10 = 7 THEN RETURN 7 AS x WHEN i % 10 = 8 THEN RETURN 8 AS x
                       ELSE RETURN 9 AS x }
            RETURN x, count(*) AS c
            """
        Then the result should be:
            | x | c |
            | 0 | 2 |
            | 1 | 2 |
            | 2 | 2 |
            | 3 | 2 |
            | 4 | 2 |
            | 5 | 2 |
            | 6 | 2 |
            | 7 | 2 |
            | 8 | 2 |
            | 9 | 2 |

    Scenario: Branches with columns in different orders fill each column by name
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2, 3] AS i
            CALL (i) { WHEN i = 1 THEN RETURN 'b' AS y, 1 AS x WHEN i = 2 THEN RETURN 2 AS x, 'c' AS y ELSE RETURN 3 AS x, null AS y }
            RETURN i, x, y
            """
        Then the result should be:
            | i | x | y    |
            | 1 | 1 | 'b'  |
            | 2 | 2 | 'c'  |
            | 3 | 3 | null |

    Scenario: A branch's rows reach the caller, and a branch with no rows drops the caller's row
        Given an empty graph
        And having executed:
            """
            CREATE (:A {k: 1})-[:R]->(:B {k: 2}), (:A {k: 3}), (:A {k: 5})-[:R]->(:B {k: 6})
            """
        When executing query:
            """
            MATCH (a:A)
            CALL (a) { WHEN EXISTS { (a)-->() } THEN MATCH (a)-->(b) UNWIND [b.k, b.k * 10] AS x RETURN x ELSE MATCH (a)<--(b) RETURN b.k AS x }
            RETURN a.k AS k, x
            """
        Then the result should be:
            | k | x  |
            | 1 | 2  |
            | 1 | 20 |
            | 5 | 6  |
            | 5 | 60 |

    Scenario: OPTIONAL CALL keeps a row that no branch takes, and one whose branch has no rows
        Given an empty graph
        And having executed:
            """
            CREATE (:A {k: 1})-[:R]->(:B {k: 2}), (:A {k: 3}), (:A {k: 5})-[:R]->(:B {k: 6})
            """
        When executing query:
            """
            MATCH (a:A)
            OPTIONAL CALL (a) { WHEN a.k = 1 THEN MATCH (a)-->(b) RETURN b.k AS x WHEN a.k = 5 THEN MATCH (a)<--(b) RETURN b.k AS x }
            RETURN a.k AS k, x
            """
        Then the result should be:
            | k | x    |
            | 1 | 2    |
            | 3 | null |
            | 5 | null |

    Scenario: A null predicate is not taken
        Given an empty graph
        When executing query:
            """
            UNWIND [1, null, 3] AS i
            CALL (i) { WHEN i > 2 THEN RETURN 'big' AS x WHEN i < 2 THEN RETURN 'small' AS x ELSE RETURN 'other' AS x }
            RETURN i, x
            """
        Then the result should be:
            | i    | x       |
            | 1    | 'small' |
            | null | 'other' |
            | 3    | 'big'   |

    Scenario: An aggregating branch that is not taken yields no row
        Given an empty graph
        And having executed:
            """
            CREATE (:A {k: 1})-[:R]->(:B {k: 2}), (:A {k: 3}), (:A {k: 5})-[:R]->(:B {k: 6})
            """
        When executing query:
            """
            MATCH (a:A)
            CALL (a) { WHEN a.k = 1 THEN MATCH (b:B) RETURN count(b) AS c WHEN a.k = 3 THEN MATCH (b:Nope) RETURN count(b) AS c }
            RETURN a.k AS k, c
            """
        Then the result should be:
            | k | c |
            | 1 | 2 |
            | 3 | 0 |

    Scenario: A nested WHEN chooses inside the outer branch
        Given an empty graph
        And having executed:
            """
            CREATE (:A {k: 1})-[:R]->(:B {k: 2}), (:A {k: 3}), (:A {k: 5})-[:R]->(:B {k: 6})
            """
        When executing query:
            """
            MATCH (a:A)
            CALL (a) { WHEN a.k < 4 THEN { WHEN EXISTS { (a)-->() } THEN RETURN 'n1' AS x ELSE RETURN 'n2' AS x } ELSE RETURN 'e' AS x }
            RETURN a.k AS k, x
            """
        Then the result should be:
            | k | x    |
            | 1 | 'n1' |
            | 3 | 'n2' |
            | 5 | 'e'  |

    Scenario: A conditional body feeds a later star
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2] AS i
            CALL (i) { WHEN i = 1 THEN RETURN 10 AS a ELSE RETURN 20 AS a }
            RETURN *
            """
        Then the result should be:
            | a  | i |
            | 10 | 1 |
            | 20 | 2 |

    Scenario: A pattern comprehension after a true OR operand in one predicate is still evaluated
        # Deliberate divergence from Neo4j: inside one predicate memgraph plans a pattern comprehension as an eager RollUpApply, as in WHERE on master; Neo4j evaluates it lazily. Recorded in ~/work/backlog.md.
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

    Scenario: A branch's count does not lose the caller's row under parallel execution
        Given an empty graph
        And having executed:
            """
            CREATE (:P {k: 1}), (:P {k: 2}), (:P {k: 30}) WITH 1 AS x UNWIND range(1, 200) AS i CREATE (:Q)
            """
        When executing query:
            """
            MATCH (n:P)
            CALL (n) { WHEN n.k < 10 THEN MATCH (m:Q) RETURN count(m) AS c ELSE RETURN -1 AS c }
            RETURN n.k AS k, c
            """
        Then the result should be:
            | k  | c   |
            | 1  | 200 |
            | 2  | 200 |
            | 30 | -1  |

    Scenario: A true parameter predicate is taken
        Given an empty graph
        And having executed:
            """
            CREATE (:A {k: 1})-[:R]->(:B {k: 2}), (:A {k: 3}), (:A {k: 5})-[:R]->(:B {k: 6})
            """
        And parameters are:
            | p | true |
        When executing query:
            """
            MATCH (a:A)
            CALL (a) { WHEN $p THEN RETURN 'yes' AS x ELSE RETURN 'no' AS x }
            RETURN a.k AS k, x
            """
        Then the result should be:
            | k | x   |
            | 1 | 'yes' |
            | 3 | 'yes' |
            | 5 | 'yes' |

    Scenario: A null parameter predicate is not taken
        Given an empty graph
        And having executed:
            """
            CREATE (:A {k: 1})-[:R]->(:B {k: 2}), (:A {k: 3}), (:A {k: 5})-[:R]->(:B {k: 6})
            """
        And parameters are:
            | p | null |
        When executing query:
            """
            MATCH (a:A)
            CALL (a) { WHEN $p THEN RETURN 'yes' AS x ELSE RETURN 'no' AS x }
            RETURN a.k AS k, x
            """
        Then the result should be:
            | k | x   |
            | 1 | 'no' |
            | 3 | 'no' |
            | 5 | 'no' |

    Scenario Outline: A predicate may test a pattern
        # exists() is memgraph syntax; Neo4j measured the bare pattern.
        Given an empty graph
        And having executed:
            """
            CREATE (:A {k: 1})-[:R]->(:B {k: 2}), (:A {k: 3}), (:A {k: 5})-[:R]->(:B {k: 6})
            """
        When executing query:
            """
            MATCH (a:A)
            CALL (a) { WHEN <predicate> THEN RETURN 'out' AS x ELSE RETURN 'none' AS x }
            RETURN a.k AS k, x
            """
        Then the result should be:
            | k | x      |
            | 1 | 'out'  |
            | 3 | 'none' |
            | 5 | 'out'  |

        Examples:
            | predicate                |
            | (a)-[:R]->()             |
            | exists((a)-[:R]->())     |

    Scenario: A predicate that is not a boolean raises
        Given an empty graph
        And having executed:
            """
            CREATE (:A {k: 1})-[:R]->(:B {k: 2}), (:A {k: 3}), (:A {k: 5})-[:R]->(:B {k: 6})
            """
        When executing query:
            """
            MATCH (a:A)
            CALL (a) { WHEN a.k THEN RETURN 'yes' AS x }
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

    Scenario: EXISTS is true when the taken branch returns a row, and its branches see the outer variables
        Given an empty graph
        And having executed
            """
            CREATE (alice:Person {name: 'Alice', age: 65}), (bob:Person {name: 'Bob', age: 25}),
                   (charlie:Person {name: 'Charlie', age: 61}), (daniel:Person {name: 'Daniel', age: 39}),
                   (eskil:Person {name: 'Eskil', age: 39}),
                   (bob)-[:WORKS_FOR]->(alice), (alice)-[:WORKS_FOR]->(daniel), (charlie)-[:WORKS_FOR]->(daniel),
                   (bob)-[:LOVES]->(eskil), (charlie)-[:LOVES]->(alice)
            """
        When executing query:
            """
            MATCH (n:Person)
            WHERE EXISTS {
              WHEN n.age > 40 THEN { RETURN n.name AS x }
              ELSE { MATCH (n)-[:LOVES]->(x:Person) RETURN x }
            }
            RETURN n.name AS name
            ORDER BY name
            """
        Then the result should be:
            | name      |
            | 'Alice'   |
            | 'Bob'     |
            | 'Charlie' |

    Scenario: COUNT counts the rows of the taken branch
        Given an empty graph
        And having executed
            """
            CREATE (alice:Person {name: 'Alice', age: 65}), (bob:Person {name: 'Bob', age: 25}),
                   (charlie:Person {name: 'Charlie', age: 61}), (daniel:Person {name: 'Daniel', age: 39}),
                   (eskil:Person {name: 'Eskil', age: 39}),
                   (bob)-[:WORKS_FOR]->(alice), (alice)-[:WORKS_FOR]->(daniel), (charlie)-[:WORKS_FOR]->(daniel),
                   (bob)-[:LOVES]->(eskil), (charlie)-[:LOVES]->(alice)
            """
        When executing query:
            """
            MATCH (n:Person)
            RETURN n.name AS name,
                   COUNT { WHEN n.age > 40 THEN MATCH (n)-[:WORKS_FOR]-(m) RETURN m ELSE RETURN 1 AS m } AS c
            ORDER BY name
            """
        Then the result should be:
            | name      | c |
            | 'Alice'   | 2 |
            | 'Bob'     | 1 |
            | 'Charlie' | 1 |
            | 'Daniel'  | 1 |
            | 'Eskil'   | 1 |

    Scenario: COLLECT collects the column of the taken branch
        Given an empty graph
        And having executed
            """
            CREATE (alice:Person {name: 'Alice', age: 65}), (bob:Person {name: 'Bob', age: 25}),
                   (charlie:Person {name: 'Charlie', age: 61}), (daniel:Person {name: 'Daniel', age: 39}),
                   (eskil:Person {name: 'Eskil', age: 39}),
                   (bob)-[:WORKS_FOR]->(alice), (alice)-[:WORKS_FOR]->(daniel), (charlie)-[:WORKS_FOR]->(daniel),
                   (bob)-[:LOVES]->(eskil), (charlie)-[:LOVES]->(alice)
            """
        When executing query:
            """
            MATCH (n:Person)
            RETURN n.name AS name,
                   COLLECT { WHEN n.age > 40 THEN MATCH (n)-[:WORKS_FOR]-(m) RETURN m.name AS m ELSE RETURN 'young' AS m } AS c
            ORDER BY name
            """
        Then the result should be (ignoring element order for lists):
            | name      | c                 |
            | 'Alice'   | ['Bob', 'Daniel'] |
            | 'Bob'     | ['young']         |
            | 'Charlie' | ['Daniel']        |
            | 'Daniel'  | ['young']         |
            | 'Eskil'   | ['young']         |

    Scenario: With no matching branch and no ELSE, the folds give false, 0 and an empty list
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2] AS i
            RETURN i,
                   EXISTS { WHEN i = 1 THEN RETURN i AS x } AS e,
                   COUNT { WHEN i = 1 THEN RETURN i AS x } AS c,
                   COLLECT { WHEN i = 1 THEN RETURN i AS x } AS l
            ORDER BY i
            """
        Then the result should be:
            | i | e     | c | l   |
            | 1 | true  | 1 | [1] |
            | 2 | false | 0 | []  |

    Scenario: A predicate reads an outer variable that no clause imports
        Given an empty graph
        And having executed
            """
            CREATE (alice:Person {name: 'Alice', age: 65}), (bob:Person {name: 'Bob', age: 25}),
                   (charlie:Person {name: 'Charlie', age: 61}), (daniel:Person {name: 'Daniel', age: 39}),
                   (eskil:Person {name: 'Eskil', age: 39}),
                   (bob)-[:WORKS_FOR]->(alice), (alice)-[:WORKS_FOR]->(daniel), (charlie)-[:WORKS_FOR]->(daniel),
                   (bob)-[:LOVES]->(eskil), (charlie)-[:LOVES]->(alice)
            """
        When executing query:
            """
            MATCH (n:Person)
            WITH n, n.age > 40 AS old
            WHERE EXISTS { WHEN old THEN MATCH (n)-[:WORKS_FOR]->(m) WHERE m.age < n.age RETURN m }
            RETURN n.name AS name
            ORDER BY name
            """
        Then the result should be:
            | name      |
            | 'Alice'   |
            | 'Charlie' |

    Scenario: An expression body runs only the first branch whose predicate holds
        Given an empty graph
        When executing query:
            """
            UNWIND [0, 1] AS i
            RETURN i,
                   COUNT {
                     WHEN i = 0 THEN RETURN 1 AS x
                     WHEN 1 / i = 1 THEN UNWIND [1, 2] AS x RETURN x
                     ELSE RETURN 1 / i AS x
                   } AS c
            ORDER BY i
            """
        Then the result should be:
            | i | c |
            | 0 | 1 |
            | 1 | 2 |

    Scenario: A branch of an expression body may hold a nested WHEN or a UNION
        Given an empty graph
        And having executed
            """
            CREATE (alice:Person {name: 'Alice', age: 65}), (bob:Person {name: 'Bob', age: 25}),
                   (charlie:Person {name: 'Charlie', age: 61}), (daniel:Person {name: 'Daniel', age: 39}),
                   (eskil:Person {name: 'Eskil', age: 39}),
                   (bob)-[:WORKS_FOR]->(alice), (alice)-[:WORKS_FOR]->(daniel), (charlie)-[:WORKS_FOR]->(daniel),
                   (bob)-[:LOVES]->(eskil), (charlie)-[:LOVES]->(alice)
            """
        When executing query:
            """
            MATCH (n:Person)
            RETURN n.name AS name,
                   COLLECT {
                     WHEN n.age > 40 THEN {
                       WHEN n.age > 62 THEN MATCH (n)-[:WORKS_FOR]-(m) RETURN m.name AS m
                       ELSE RETURN 'sixties' AS m
                     }
                     ELSE RETURN 'young' AS m
                   } AS c,
                   COUNT {
                     WHEN n.age < 40 THEN { MATCH (n)-[:LOVES]->(m) RETURN m UNION MATCH (n)-[:WORKS_FOR]->(m) RETURN m }
                   } AS k
            ORDER BY name
            """
        Then the result should be (ignoring element order for lists):
            | name      | c                 | k |
            | 'Alice'   | ['Bob', 'Daniel'] | 0 |
            | 'Bob'     | ['young']         | 2 |
            | 'Charlie' | ['sixties']       | 0 |
            | 'Daniel'  | ['young']         | 0 |
            | 'Eskil'   | ['young']         | 0 |

    Scenario: A branch of an expression body seeks a label-property index by an outer value
        Given an empty graph
        And having executed
            """
            CREATE (alice:Person {name: 'Alice', age: 65}), (bob:Person {name: 'Bob', age: 25}),
                   (charlie:Person {name: 'Charlie', age: 61}), (daniel:Person {name: 'Daniel', age: 39}),
                   (eskil:Person {name: 'Eskil', age: 39}),
                   (bob)-[:WORKS_FOR]->(alice), (alice)-[:WORKS_FOR]->(daniel), (charlie)-[:WORKS_FOR]->(daniel),
                   (bob)-[:LOVES]->(eskil), (charlie)-[:LOVES]->(alice)
            """
        And with new index :Person(name)
        When executing query:
            """
            UNWIND ['Alice', 'Bob', 'Zed'] AS who
            RETURN who,
                   COLLECT {
                     WHEN who = 'Alice' THEN MATCH (p:Person {name: who})-[:WORKS_FOR]->(m) RETURN m.name AS v
                     WHEN who = 'Bob' THEN MATCH (p:Person) WHERE p.name = who RETURN p.age AS v
                   } AS c
            ORDER BY who
            """
        Then the result should be:
            | who     | c          |
            | 'Alice' | ['Daniel'] |
            | 'Bob'   | [25]       |
            | 'Zed'   | []         |

    Scenario Outline: An expression body with WHEN branches is refused where its rules are broken
        Given an empty graph
        When executing query:
            """
            MATCH (n)
            RETURN <expression> AS r
            """
        Then an error should be raised

        Examples:
            | expression                                                                                 |
            | EXISTS { WHEN n.age > 40 THEN MATCH (n)-->() ELSE MATCH (n)<--() }                         |
            | COUNT { WHEN n.age > 40 THEN RETURN 1 AS x ELSE MATCH (n)-->() }                           |
            | COUNT { WHEN true THEN { WHEN false THEN RETURN 1 AS x ELSE MATCH (n)-->() } }             |
            | COUNT { WHEN true THEN CREATE (:Q) RETURN 1 AS a }                                         |
            | EXISTS { WHEN true THEN RETURN 1 AS x ELSE { WHEN true THEN SET n.p = 1 RETURN 1 AS x } }  |
            | COLLECT { WHEN true THEN RETURN 1 AS a, 2 AS b ELSE RETURN 3 AS a, 4 AS b }                |
            | COLLECT { WHEN true THEN RETURN 1 AS a ELSE RETURN 2 AS b }                                |
            | EXISTS { WHEN count(n) > 0 THEN RETURN 1 AS x }                                            |
            | EXISTS { WHEN n.age > 40 THEN MATCH (n)-->(m) RETURN m WHEN m IS NULL THEN RETURN 1 AS m } |
