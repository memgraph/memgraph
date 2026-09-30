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
