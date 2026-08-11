Feature: Pattern comprehensions

   Scenario: Top-level pattern comprehension
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 1}), (b:N {id: 2}), (c:N {id: 3}), (a)-[:R]->(b), (b)-[:R]->(c)
            """
        When executing query:
            """
            MATCH (x) RETURN [(x)-->(y) | y.id]
            """
        Then the result should be:
            | [(x)-->(y) \| y.id] |
            | [2]                 |
            | [3]                 |
            | []                  |

   Scenario: Pattern comprehension inside a list literal
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 1}), (b:N {id: 2}), (c:N {id: 3}), (a)-[:R]->(b), (b)-[:R]->(c)
            """
        When executing query:
            """
            MATCH (x) RETURN [[(x)-->(y) | y.id]]
            """
        Then the result should be:
            | [[(x)-->(y) \| y.id]] |
            | [[2]]                 |
            | [[3]]                 |
            | [[]]                  |

   Scenario: Pattern comprehension inside a list literal inside a list literal
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 1}), (b:N {id: 2}), (c:N {id: 3}), (a)-[:R]->(b), (b)-[:R]->(c)
            """
        When executing query:
            """
            MATCH (x) RETURN [[[(x)-->(y) | y.id]]]
            """
        Then the result should be:
            | [[[(x)-->(y) \| y.id]]] |
            | [[[2]]]                 |
            | [[[3]]]                 |
            | [[[]]]                  |

   Scenario: Nested pattern comprehension - pattern comprehension in result expression
        Given an empty graph
        When executing query:
            """
            RETURN [()--() | [()--() | 1]] AS x
            """
        Then the result should be:
            | x  |
            | [] |

   Scenario: Nested pattern comprehension with data
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 1})-[:R]->(b:N {id: 2})-[:R]->(c:N {id: 3})
            """
        When executing query:
            """
            RETURN [()--() | [()--() | 1]] AS x
            """
        Then the result should be:
            | x                          |
            | [[1, 1, 1, 1], [1, 1, 1, 1], [1, 1, 1, 1], [1, 1, 1, 1]] |

   Scenario: Three levels of nested pattern comprehensions
        Given an empty graph
        When executing query:
            """
            RETURN [()--() | [()--() | [()--() | 1]]] AS x
            """
        Then the result should be:
            | x  |
            | [] |

   Scenario: Pattern comprehension in ORDER BY
        Given an empty graph
        When executing query:
            """
            RETURN 1 AS x ORDER BY length([()--() | 1])
            """
        Then the result should be:
            | x |
            | 1 |

   Scenario: Multiple pattern comprehensions in ORDER BY
        Given an empty graph
        When executing query:
            """
            RETURN 1 AS x ORDER BY length([()--() | 1]), length([()--() | 2])
            """
        Then the result should be:
            | x |
            | 1 |

   Scenario: Pattern comprehension in WITH WHERE
        Given an empty graph
        When executing query:
            """
            WITH 1 AS a WHERE [()--() | 1] = [] RETURN a
            """
        Then the result should be:
            | a |
            | 1 |

   Scenario: Pattern comprehension in CALL subquery WITH WHERE
        Given an empty graph
        When executing query:
            """
            CALL { WITH 1 AS a WHERE [()--() | 1] = [] RETURN a } RETURN a
            """
        Then the result should be:
            | a |
            | 1 |

   Scenario: Nested pattern comprehension in MATCH WHERE
        Given an empty graph
        When executing query:
            """
            MATCH (n) WHERE [(n)--() | [(n)--() | 1]] = [] RETURN n
            """
        Then the result should be empty

   Scenario: Nested pattern comprehension in MATCH WHERE with data
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 1})-[:R]->(b:N {id: 2})
            """
        When executing query:
            """
            MATCH (n) WHERE [(n)--() | [(n)--() | 1]] != [] RETURN n.id AS id ORDER BY id
            """
        Then the result should be:
            | id |
            | 1  |
            | 2  |

   Scenario: Nested pattern comprehension with bound variable from outer pattern
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 1})-[:R]->(b:N {id: 2})-[:R]->(c:N {id: 3})
            """
        When executing query:
            """
            MATCH (n) WHERE n.id = 2 RETURN [(n)--(m) | [(n)--(m) | n.id]] AS nested
            """
        Then the result should be:
            | nested       |
            | [[2], [2]]   |

   Scenario: Pattern comprehension in UNWIND
        Given an empty graph
        When executing query:
            """
            UNWIND [()--() | 1] AS x RETURN x
            """
        Then the result should be empty

   Scenario: Pattern comprehension in UNWIND with data
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 1})-[:R]->(b:N {id: 2})
            """
        When executing query:
            """
            UNWIND [()--() | 1] AS x RETURN x
            """
        Then the result should be:
            | x |
            | 1 |
            | 1 |

   Scenario: Pattern comprehension in FOREACH
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 1})-[:R]->(b:N {id: 2})
            """
        When executing query:
            """
            MATCH (n) FOREACH (x IN [()--() | 1] | SET n.prop = x) RETURN n.id AS id, n.prop AS prop ORDER BY id
            """
        Then the result should be:
            | id | prop |
            | 1  | 1    |
            | 2  | 1    |

   Scenario: Pattern comprehension in CREATE property
        Given an empty graph
        And having executed:
            """
            CREATE ()-[:R]->()
            """
        When executing query:
            """
            CREATE (n {prop: [()--() | 1]}) RETURN n.prop AS prop
            """
        Then the result should be:
            | prop   |
            | [1, 1] |

   Scenario: Pattern comprehension in SET
        Given an empty graph
        And having executed:
            """
            CREATE ()-[:R]->(), (n:N)
            """
        When executing query:
            """
            MATCH (n:N) SET n.prop = [()--() | 1] RETURN n.prop AS prop
            """
        Then the result should be:
            | prop   |
            | [1, 1] |

   Scenario: Pattern comprehension in MERGE properties
        Given an empty graph
        When executing query:
            """
            MERGE (n:Test {val: [()--() | 1]}) RETURN n.val AS val
            """
        Then the result should be:
            | val |
            | []  |

   Scenario: Pattern comprehension in MERGE ON CREATE SET
        Given an empty graph
        And having executed:
            """
            CREATE ()-[:R]->()
            """
        When executing query:
            """
            MERGE (n:Test) ON CREATE SET n.prop = [()--() | 1] RETURN n.prop AS prop
            """
        Then the result should be:
            | prop   |
            | [1, 1] |

   Scenario: Pattern comprehension referencing CREATE-created node
        Given an empty graph
        When executing query:
            """
            CREATE (n)-[:R]->(m {id: 2}) RETURN [(n)-->(x) | x.id] AS prop
            """
        Then the result should be:
            | prop |
            | [2]  |

   Scenario: Pattern comprehension referencing MERGE-created node
        Given an empty graph
        When executing query:
            """
            MERGE (n)-[:R]->(m {id: 2}) RETURN [(n)-->(x) | x.id] AS prop
            """
        Then the result should be:
            | prop |
            | [2]  |

   Scenario: Named path variable in pattern comprehension
        Given an empty graph
        And having executed:
            """
            CREATE (:A)-[:R]->(:B)
            """
        When executing query:
            """
            MATCH (n:A) RETURN [path = (n)-->() | length(path)] AS lengths
            """
        Then the result should be:
            | lengths |
            | [1]     |

   Scenario: Named path variable in pattern comprehension with variable length
        Given an empty graph
        And having executed:
            """
            CREATE (:A)-[:R]->()-[:R]->()
            """
        When executing query:
            """
            MATCH (n:A) RETURN [path = (n)-[*1..2]->() | length(path)] AS lengths
            """
        Then the result should be:
            | lengths |
            | [1, 2]  |

   Scenario: Pattern comprehension in FOREACH referencing loop variable
        Given an empty graph
        And having executed:
            """
            CREATE (a {id: 1})-[:R]->(b {val: 10})
            """
        And having executed:
            """
            FOREACH (x IN [1, 2] | CREATE (n:R {x: x, found: [(a)-[:R]->(b) WHERE a.id = x | b.val]}))
            """
        When executing query:
            """
            MATCH (n:R) RETURN n.x AS x, n.found AS found ORDER BY x
            """
        Then the result should be:
            | x | found |
            | 1 | [10]  |
            | 2 | []    |

   Scenario: Nested pattern comprehension where inner starts from outer's expansion node
        Given an empty graph
        And having executed:
            """
            CREATE (a:Node {id: 1})-[:R]->(b:Node {id: 2})-[:R]->(c:Node {id: 3})
            """
        When executing query:
            """
            MATCH (n {id: 1})
            RETURN [(n)-[]->(adjacent) | {id: adjacent.id, next: [(adjacent)-[]->(adjacent_to_adjacent) | adjacent_to_adjacent.id]}] AS nested
            """
        Then the result should be:
            | nested                   |
            | [{id: 2, next: [3]}]     |

   Scenario: Pattern comprehension with variable-length path after CREATE should see new data
        Given an empty graph
        When executing query:
            """
            CREATE (a:Start {name: 'a'})-[:R]->(b:Mid {name: 'b'})-[:R]->(c:End {name: 'c'})
            WITH a
            RETURN [(a)-[*1..2]->(x) | x.name] AS reachable
            """
        Then the result should be:
            | reachable      |
            | ['b', 'c']     |

   Scenario: Pattern comprehension inside count aggregate
        Given an empty graph
        And having executed:
            """
            CREATE (p:Person {id: 1})-[:OWNS]->(i:Item {name: 'DVD'})
            CREATE (q:Person {id: 2})-[:OWNS]->(j:Item {name: 'Book'})
            """
        When executing query:
            """
            MATCH (p:Person) RETURN count([(p)-[:OWNS]->(i) | i.name]) AS c
            """
        Then the result should be:
            | c |
            | 2 |

   Scenario: Pattern comprehension inside sum with size
        Given an empty graph
        And having executed:
            """
            CREATE (p:Person {id: 1})-[:OWNS]->(i:Item {name: 'DVD'})
            CREATE (q:Person {id: 2})-[:OWNS]->(j:Item {name: 'Book'}), (q)-[:OWNS]->(k:Item {name: 'Phone'})
            """
        When executing query:
            """
            MATCH (p:Person) RETURN sum(size([(p)-[:OWNS]->(i) | i.name])) AS total
            """
        Then the result should be:
            | total |
            | 3     |

   Scenario: Pattern comprehension inside collect aggregate
        Given an empty graph
        And having executed:
            """
            CREATE (p:Person {id: 1})-[:OWNS]->(i:Item {name: 'DVD'})
            CREATE (q:Person {id: 2})-[:OWNS]->(j:Item {name: 'Book'})
            """
        When executing query:
            """
            MATCH (p:Person) RETURN collect([(p)-[:OWNS]->(i) | i.name]) AS items
            """
        Then the result should be:
            | items               |
            | [['DVD'], ['Book']] |

   Scenario: Pattern comprehension both inside and outside aggregate
        Given an empty graph
        And having executed:
            """
            CREATE (p:Person {id: 1})-[:OWNS]->(i:Item {name: 'DVD'})
            CREATE (q:Person {id: 2})-[:OWNS]->(j:Item {name: 'Book'})
            """
        When executing query:
            """
            MATCH (p:Person)
            RETURN p.id AS id, count([(p)-[:OWNS]->(i) | i.name]) AS cnt, [(p)-[:OWNS]->(x) | x.name] AS items
            ORDER BY id
            """
        Then the result should be:
            | id | cnt | items    |
            | 1  | 1   | ['DVD']  |
            | 2  | 1   | ['Book'] |

    Scenario: Pattern comprehension with no external references combined with aggregate
        Given an empty graph
        And having executed:
            """
            CREATE (:Person {id: 1})-[:KNOWS]->(:Person {id: 2})
            CREATE (:Person {id: 3})-[:KNOWS]->(:Person {id: 4})
            """
        When executing query:
            """
            MATCH (p:Person) WHERE p.id IN [1, 3]
            RETURN count(*) AS cnt, [()-[:KNOWS]->() | 1] AS edges
            """
        Then the result should be:
            | cnt | edges  |
            | 2   | [1, 1] |

    Scenario: Pattern comprehension in the WHERE of a WITH filters on the bound variable
        Given an empty graph
        And having executed:
            """
            CREATE (:Person {name: 'Regina'})-[:ACTED_IN]->(:Movie {title: 'Jerry'})
            CREATE (:Person {name: 'Bob'})
            """
        When executing query:
            """
            MATCH (p:Person)
            WITH p
            WHERE size([(p)-[:ACTED_IN]->(m) | m]) > 0
            RETURN p.name AS name
            """
        Then the result should be:
            | name     |
            | 'Regina' |

    Scenario: Pattern comprehension in the ORDER BY of a WITH sorts on the bound variable
        Given an empty graph
        # Created fewest-edges-first, so the expected order is the reverse of the scan order: a sort key that is the
        # same for every row - an uncorrelated whole-graph count - cannot produce it.
        And having executed:
            """
            CREATE (:Person {name: 'Bob'})
            CREATE (:Person {name: 'Regina'})-[:ACTED_IN]->(:Movie {title: 'Jerry'})
            CREATE (a:Person {name: 'Zoe'})-[:ACTED_IN]->(:Movie {title: 'M1'})
            CREATE (a)-[:ACTED_IN]->(:Movie {title: 'M2'})
            """
        When executing query:
            """
            MATCH (p:Person)
            WITH p
            ORDER BY size([(p)-[:ACTED_IN]->(m) | m]) DESC
            RETURN p.name AS name
            """
        Then the result should be, in order:
            | name     |
            | 'Zoe'    |
            | 'Regina' |
            | 'Bob'    |

    Scenario: Pattern comprehension in the WHERE of an aggregating WITH
        Given an empty graph
        And having executed:
            """
            CREATE (:Person {name: 'Regina'})-[:ACTED_IN]->(:Movie {title: 'Jerry'})
            CREATE (:Person {name: 'Bob'})
            """
        When executing query:
            """
            MATCH (p:Person)
            WITH p, count(*) AS c
            WHERE size([(p)-[:ACTED_IN]->(m) | m]) > 0
            RETURN p.name AS name, c
            """
        Then the result should be:
            | name     | c |
            | 'Regina' | 1 |

    Scenario: Pattern comprehension in the ORDER BY of an aggregating WITH
        Given an empty graph
        And having executed:
            """
            CREATE (a:Person {name: 'Zoe'})-[:ACTED_IN]->(:Movie {title: 'M1'})
            CREATE (a)-[:ACTED_IN]->(:Movie {title: 'M2'})
            CREATE (:Person {name: 'Regina'})-[:ACTED_IN]->(:Movie {title: 'Jerry'})
            CREATE (:Person {name: 'Bob'})
            """
        When executing query:
            """
            MATCH (p:Person)
            WITH p, count(*) AS c
            ORDER BY size([(p)-[:ACTED_IN]->(m) | m]) DESC
            RETURN p.name AS name
            """
        Then the result should be, in order:
            | name     |
            | 'Zoe'    |
            | 'Regina' |
            | 'Bob'    |

    Scenario: Pattern comprehension in the ORDER BY of a RETURN sorts on the bound variable
        Given an empty graph
        # As above: fewest edges created first, so the scan order is the reverse of the expected one.
        And having executed:
            """
            CREATE (:Person {name: 'Bob'})
            CREATE (:Person {name: 'Regina'})-[:ACTED_IN]->(:Movie {title: 'Jerry'})
            CREATE (a:Person {name: 'Zoe'})-[:ACTED_IN]->(:Movie {title: 'M1'})
            CREATE (a)-[:ACTED_IN]->(:Movie {title: 'M2'})
            """
        When executing query:
            """
            MATCH (p:Person)
            RETURN p.name AS name
            ORDER BY size([(p)-[:ACTED_IN]->(m) | m]) DESC
            """
        Then the result should be, in order:
            | name     |
            | 'Zoe'    |
            | 'Regina' |
            | 'Bob'    |

    Scenario: Pattern comprehension in the WHERE of a WITH that also has an ORDER BY
        Given an empty graph
        And having executed:
            """
            CREATE (a:Person {name: 'Zoe'})-[:ACTED_IN]->(:Movie {title: 'M1'})
            CREATE (a)-[:ACTED_IN]->(:Movie {title: 'M2'})
            CREATE (:Person {name: 'Regina'})-[:ACTED_IN]->(:Movie {title: 'Jerry'})
            CREATE (:Person {name: 'Bob'})
            """
        When executing query:
            """
            MATCH (p:Person)
            WITH p
            ORDER BY p.name
            WHERE size([(p)-[:ACTED_IN]->(m) | m]) > 0
            RETURN p.name AS name
            """
        Then the result should be, in order:
            | name     |
            | 'Regina' |
            | 'Zoe'    |

    Scenario: Pattern comprehensions in both the ORDER BY and the WHERE of a WITH
        Given an empty graph
        And having executed:
            """
            CREATE (a:Person {name: 'Zoe'})-[:ACTED_IN]->(:Movie {title: 'M1'})
            CREATE (a)-[:ACTED_IN]->(:Movie {title: 'M2'})
            CREATE (:Person {name: 'Regina'})-[:ACTED_IN]->(:Movie {title: 'Jerry'})
            CREATE (:Person {name: 'Bob'})
            """
        When executing query:
            """
            MATCH (p:Person)
            WITH p
            ORDER BY size([(p)-[:ACTED_IN]->(m) | m]) DESC
            WHERE size([(p)-[:ACTED_IN]->(x) | x]) > 0
            RETURN p.name AS name
            """
        Then the result should be, in order:
            | name     |
            | 'Zoe'    |
            | 'Regina' |

    Scenario: Pattern comprehension in the WHERE of a WITH that renames the variable it references
        Given an empty graph
        And having executed:
            """
            CREATE (a:Person {name: 'Zoe'})-[:ACTED_IN]->(:Movie {title: 'M1'})
            CREATE (a)-[:ACTED_IN]->(:Movie {title: 'M2'})
            CREATE (:Person {name: 'Regina'})-[:ACTED_IN]->(:Movie {title: 'Jerry'})
            CREATE (:Person {name: 'Bob'})
            """
        When executing query:
            """
            MATCH (p:Person)
            WITH p AS q
            ORDER BY q.name
            WHERE size([(p)-[:ACTED_IN]->(m) | m]) > 0
            RETURN q.name AS name
            """
        Then the result should be, in order:
            | name     |
            | 'Regina' |
            | 'Zoe'    |

    Scenario: Pattern comprehension in the WHERE of a WITH preceded by a write clause
        Given an empty graph
        And having executed:
            """
            CREATE (a:Person {name: 'Zoe'})-[:ACTED_IN]->(:Movie {title: 'M1'})
            CREATE (:Person {name: 'Regina'})-[:ACTED_IN]->(:Movie {title: 'Jerry'})
            CREATE (:Person {name: 'Bob'})
            """
        When executing query:
            """
            MATCH (p:Person)
            SET p.seen = 1
            WITH p
            WHERE size([(p)-[:ACTED_IN]->(m) | m]) > 0
            RETURN p.name AS name
            """
        Then the result should be:
            | name     |
            | 'Zoe'    |
            | 'Regina' |

    Scenario: Pattern comprehension in the WHERE of a WITH preceded by a FOREACH
        Given an empty graph
        And having executed:
            """
            CREATE (a:Person {name: 'Zoe'})-[:ACTED_IN]->(:Movie {title: 'M1'})
            CREATE (:Person {name: 'Regina'})-[:ACTED_IN]->(:Movie {title: 'Jerry'})
            CREATE (:Person {name: 'Bob'})
            """
        When executing query:
            """
            MATCH (p:Person)
            FOREACH (i IN [1] | SET p.seen = 1)
            WITH p
            WHERE size([(p)-[:ACTED_IN]->(m) | m]) > 0
            RETURN p.name AS name
            """
        Then the result should be:
            | name     |
            | 'Zoe'    |
            | 'Regina' |

    Scenario: WITH ORDER BY LIMIT still filters after the limit
        Given an empty graph
        And having executed:
            """
            CREATE (a:Person {name: 'Zoe'})-[:ACTED_IN]->(:Movie {title: 'M1'})
            CREATE (:Person {name: 'Regina'})-[:ACTED_IN]->(:Movie {title: 'Jerry'})
            CREATE (:Person {name: 'Bob'})
            """
        When executing query:
            """
            MATCH (p:Person)
            WITH p
            ORDER BY p.name
            LIMIT 2
            WHERE size([(p)-[:ACTED_IN]->(m) | m]) > 0
            RETURN p.name AS name
            """
        Then the result should be:
            | name     |
            | 'Regina' |

    Scenario: Pattern comprehension over a node created inside the same FOREACH body
        Given an empty graph
        And having executed:
            """
            CREATE (:Person {name: 'Zoe'})-[:ACTED_IN]->(:Movie {title: 'M1'})
            CREATE (:Person {name: 'Regina'})
            """
        And having executed:
            """
            MATCH (p:Person)
            FOREACH (i IN [1] |
              CREATE (q:Marker)
              SET q.cnt = size([(q)-[:ACTED_IN]->(m) | m]))
            """
        When executing query:
            """
            MATCH (q:Marker)
            RETURN count(q) AS created, collect(DISTINCT q.cnt) AS counts
            """
        Then the result should be:
            | created | counts |
            | 2       | [0]    |

    Scenario: Pattern comprehension over a node created by an enclosing FOREACH body
        Given an empty graph
        And having executed:
            """
            CREATE (:Person {name: 'Zoe'})-[:ACTED_IN]->(:Movie {title: 'M1'})
            """
        And having executed:
            """
            MATCH (p:Person)
            FOREACH (i IN [1] |
              CREATE (q:Marker)
              FOREACH (j IN [1] |
                SET q.cnt = size([(q)-[:ACTED_IN]->(m) | m])))
            """
        When executing query:
            """
            MATCH (q:Marker)
            RETURN count(q) AS created, collect(DISTINCT q.cnt) AS counts
            """
        Then the result should be:
            | created | counts |
            | 1       | [0]    |

    Scenario: Pattern comprehension in a FOREACH body sees edges created in that body
        Given an empty graph
        And having executed:
            """
            CREATE (:Person {name: 'Zoe'})
            CREATE (:Extra)-[:ACTED_IN]->(:Movie {title: 'Old'})
            """
        And having executed:
            """
            MATCH (p:Person)
            FOREACH (i IN [1] |
              CREATE (q:Marker)-[:ACTED_IN]->(:Movie {title: 'New'})
              SET q.cnt = size([(q)-[:ACTED_IN]->(m) | m]))
            """
        When executing query:
            """
            MATCH (q:Marker)
            RETURN collect(q.cnt) AS counts
            """
        Then the result should be:
            | counts |
            | [1]    |

    Scenario: A comprehension on a pre-FOREACH symbol is unaffected by one bound inside the body
        Given an empty graph
        And having executed:
            """
            CREATE (:Person {name: 'Zoe'})-[:ACTED_IN]->(:Movie {title: 'M1'})
            CREATE (:Person {name: 'Regina'})
            """
        And having executed:
            """
            MATCH (p:Person)
            FOREACH (i IN [1] |
              CREATE (q:Marker)
              SET q.outer = size([(p)-[:ACTED_IN]->(m) | m]),
                  q.inner = size([(q)-[:ACTED_IN]->(m2) | m2]))
            """
        When executing query:
            """
            MATCH (q:Marker)
            RETURN sum(q.outer) AS outer_sum, collect(DISTINCT q.inner) AS inners
            """
        Then the result should be:
            | outer_sum | inners |
            | 1         | [0]    |

    Scenario: Pattern comprehension in MERGE ON CREATE correlates to the merged node
        Given an empty graph
        And having executed:
            """
            CREATE (a:Person {name: 'Zoe'})-[:ACTED_IN]->(:Movie {title: 'M1'})
            CREATE (:Person {name: 'Regina'})
            """
        And having executed:
            """
            MATCH (p:Person)
            MERGE (q:Marker {id: 1})
              ON CREATE SET q.cnt = size([(q)-[:ACTED_IN]->(m) | m])
            """
        When executing query:
            """
            MATCH (q:Marker)
            RETURN count(q) AS n, collect(q.cnt) AS counts
            """
        Then the result should be:
            | n | counts |
            | 1 | [0]    |

    Scenario: Pattern comprehension in MERGE ON MATCH correlates to the matched node
        Given an empty graph
        And having executed:
            """
            CREATE (a:Person {name: 'Zoe'})-[:ACTED_IN]->(:Movie {title: 'M1'})
            CREATE (b:Person {name: 'Regina'})-[:ACTED_IN]->(:Movie {title: 'J1'})
            CREATE (b)-[:ACTED_IN]->(:Movie {title: 'J2'})
            """
        And having executed:
            """
            MATCH (p:Person)
            MERGE (q:Person {name: 'Zoe'})
              ON MATCH SET q.cnt = size([(q)-[:ACTED_IN]->(m) | m])
            """
        When executing query:
            """
            MATCH (q:Person {name: 'Zoe'})
            RETURN q.cnt AS cnt
            """
        Then the result should be:
            | cnt |
            | 1   |

    Scenario: Pattern comprehension in a MERGE ON CREATE inside a FOREACH body
        Given an empty graph
        And having executed:
            """
            CREATE (a:Person {name: 'Zoe'})-[:ACTED_IN]->(:Movie {title: 'M1'})
            CREATE (:Person {name: 'Regina'})
            """
        And having executed:
            """
            MATCH (p:Person)
            FOREACH (i IN [1] |
              MERGE (q:Marker {id: 1})
                ON CREATE SET q.cnt = size([(q)-[:ACTED_IN]->(m) | m]))
            """
        When executing query:
            """
            MATCH (q:Marker)
            RETURN count(q) AS n, collect(q.cnt) AS counts
            """
        Then the result should be:
            | n | counts |
            | 1 | [0]    |

    Scenario: Variable-length pattern comprehension in the WHERE of a WITH preceded by a write clause
        Given an empty graph
        And having executed:
            """
            CREATE (a:Person {id: 1})-[:FRIEND]->(b:Person {id: 2})
            CREATE (b)-[:FRIEND]->(:Person {id: 9})
            """
        When executing query:
            """
            MATCH (p:Person) SET p.seen = 1
            WITH p AS q WHERE size([(q)-[:FRIEND*1..2]->(m) | m]) > 0
            RETURN q.id AS id ORDER BY id
            """
        Then the result should be:
            | id |
            | 1  |
            | 2  |

    Scenario: Variable-length pattern comprehension in the ORDER BY of a WITH preceded by a write clause
        Given an empty graph
        And having executed:
            """
            CREATE (a:Person {id: 9})-[:FRIEND]->(b:Person {id: 2})
            CREATE (b)-[:FRIEND]->(:Person {id: 1})
            """
        When executing query:
            """
            MATCH (p:Person) SET p.seen = 1
            WITH p AS q ORDER BY size([(q)-[:FRIEND*1..2]->(m) | m]) DESC, q.id
            RETURN q.id AS id
            """
        # Ids run opposite to reachability on purpose: an uncorrelated key ties every row and the
        # q.id tie-break would give 1, 2, 9.
        Then the result should be, in order:
            | id |
            | 9  |
            | 2  |
            | 1  |

    Scenario: Nested variable-length pattern comprehension inside a FOREACH body
        Given an empty graph
        And having executed:
            """
            CREATE (h:Hub {id: 1})-[:FRIEND]->(m:Hub {id: 2})
            CREATE (m)-[:FRIEND]->(:Hub {id: 3})
            """
        And having executed:
            """
            MATCH (h:Hub {id: 1})
            FOREACH (i IN [1] |
              CREATE (q:Marker)
              SET q.cnt = [(h)-[:FRIEND]->(x) | size([(x)-[:FRIEND*1..2]->(y) | y])])
            """
        When executing query:
            """
            MATCH (q:Marker)
            RETURN count(q) AS n, collect(q.cnt) AS counts
            """
        # The nested variable-length branch must actually run: h reaches one node, which reaches one
        # more. Planning this used to abort the process.
        Then the result should be:
            | n | counts |
            | 1 | [[1]]  |

    Scenario: Variable-length pattern comprehension over a node its own query part creates is rejected
        Given an empty graph
        When executing query:
            """
            CREATE (a:Person)-[:FRIEND]->(:Person)
            RETURN [(a)-[:FRIEND*1..2]->(x) | x] AS reachable
            """
        Then an error should be raised

    Scenario: Pattern comprehension may not reuse an already bound relationship variable
        Given an empty graph
        And having executed:
            """
            CREATE (:Person {id: 1})-[:FRIEND]->(:Person {id: 2})
            """
        When executing query:
            """
            MATCH (a:Person)-[r:FRIEND]->(b:Person)
            WITH a, r WHERE size([(a)-[r]->(y) | y]) > 0
            RETURN a.id AS id
            """
        Then an error should be raised

    Scenario: Variable-length pattern comprehension over a matched node a write clause reuses
        Given an empty graph
        And having executed:
            """
            CREATE (a:Person {id: 1})-[:FRIEND]->(b:Person {id: 2})
            CREATE (b)-[:FRIEND]->(:Person {id: 9})
            """
        When executing query:
            """
            MATCH (a:Person {id: 1}) CREATE (a)-[:SEEN]->(:Marker)
            RETURN [(a)-[:FRIEND*1..2]->(x) | x.id] AS ids
            """
        Then the result should be:
            | ids    |
            | [2, 9] |

    Scenario: A pattern comprehension's own path variable does not overwrite an outer path
        Given an empty graph
        And having executed:
            """
            CREATE (a:Hop {id: 10})-[:S]->(b:Hop {id: 11})
            CREATE (b)-[:S]->(:Hop {id: 12})
            CREATE (:Side {id: 1})-[:R]->(:Side {id: 4})
            """
        When executing query:
            """
            MATCH p = (:Hop {id: 10})-[:S*2]->(:Hop {id: 12})
            RETURN [n IN nodes(p) | n.id] AS ids, [p = (:Side)-[:R]->(y:Side) | y.id] AS inner
            """
        Then the result should be:
            | ids          | inner |
            | [10, 11, 12] | [4]   |

    Scenario: A named path inside a CALL subquery does not overwrite the caller's
        Given an empty graph
        And having executed:
            """
            CREATE (a:Hop {id: 10})-[:S]->(b:Hop {id: 11})
            CREATE (b)-[:S]->(:Hop {id: 12})
            CREATE (:Side {id: 1})-[:R]->(:Side {id: 4})
            """
        When executing query:
            """
            MATCH p = (:Hop {id: 10})-[:S*2]->(:Hop {id: 12})
            CALL {
              MATCH p = (:Side)-[:R]->(y:Side)
              RETURN y
            }
            RETURN [n IN nodes(p) | n.id] AS ids, y.id AS yid
            """
        Then the result should be:
            | ids          | yid |
            | [10, 11, 12] | 4   |
    # Does NOT discriminate: without the CallProcedure drain the ungated drain at the SET takes this comprehension and
    # gives the same answer. The sibling scenario below, with no preceding write, is the one that needs the new drain.
    Scenario: Pattern comprehension in a CALL YIELD WHERE after a write
        Given an empty graph
        And having executed:
            """
            CREATE (a:Person {name: 'Alice'})-[:ACTED_IN]->(:Movie {title: 'M1'})
            CREATE (:Person {name: 'Bob'})
            """
        When executing query:
            """
            MATCH (p:Person) SET p.z = 1
            CALL mg.procedures() YIELD name
            WHERE size([(p)-[:ACTED_IN]->(m) | m]) > 0
            RETURN DISTINCT p.name AS name
            """
        Then the result should be:
            | name    |
            | 'Alice' |

    Scenario: Pattern comprehension in a CALL YIELD WHERE with no preceding write
        Given an empty graph
        And having executed:
            """
            CREATE (a:Person {name: 'Alice'})-[:ACTED_IN]->(:Movie {title: 'M1'})
            CREATE (:Person {name: 'Bob'})
            """
        When executing query:
            """
            MATCH (p:Person)
            CALL mg.procedures() YIELD name
            WHERE size([(p)-[:ACTED_IN]->(m) | m]) > 0
            RETURN DISTINCT p.name AS name
            """
        Then the result should be:
            | name    |
            | 'Alice' |


    # The comprehension correlates to nothing, so the old rule left it on the pre-write view and it counted only the
    # pre-existing edge. What decides the view is the write history, not whether the branch reads an outer symbol.
    Scenario: Uncorrelated pattern comprehension after a write sees that write
        Given an empty graph
        And having executed:
            """
            CREATE (:A)-[:R]->(:B)
            """
        When executing query:
            """
            CREATE (:A)-[:R]->(:B)
            CREATE (t:T)
            SET t.c = size([(x:A)-[:R]->(y:B) | y])
            RETURN t.c AS c
            """
        Then the result should be:
            | c |
            | 2 |

    Scenario: Pattern comprehension in a WITH projection after a write is evaluated per row
        Given an empty graph
        And having executed:
            """
            CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
            CREATE (a)-[:ACTED_IN]->(:Movie {title: 'M1'}), (a)-[:ACTED_IN]->(:Movie {title: 'M2'})
            CREATE (b)-[:ACTED_IN]->(:Movie {title: 'M3'})
            """
        When executing query:
            """
            MATCH (p:Person) SET p.z = 1
            WITH p.name AS n, [(p)-[:ACTED_IN]->(m) | m] AS lst
            RETURN n, size(lst) AS s
            """
        Then the result should be:
            | n         | s |
            | 'Alice'   | 2 |
            | 'Bob'     | 1 |
            | 'Carol'   | 0 |

    Scenario: Pattern comprehension in a WITH WHERE after a write is evaluated per row
        Given an empty graph
        And having executed:
            """
            CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
            CREATE (a)-[:ACTED_IN]->(:Movie {title: 'M1'}), (a)-[:ACTED_IN]->(:Movie {title: 'M2'})
            CREATE (b)-[:ACTED_IN]->(:Movie {title: 'M3'})
            """
        When executing query:
            """
            MATCH (p:Person) SET p.z = 1
            WITH p.name AS n WHERE size([(p)-[:ACTED_IN]->(m) | m]) > 0
            RETURN n
            """
        Then the result should be:
            | n       |
            | 'Alice' |
            | 'Bob'   |

    Scenario: Pattern comprehension in RETURN sees the effects of the CREATE it follows
        Given an empty graph
        And having executed:
            """
            CREATE (:A)-[:R]->(:B)
            """
        When executing query:
            """
            CREATE (:A)-[:R]->(:B) RETURN size([(x:A)-[:R]->(y:B) | y]) AS s
            """
        Then the result should be:
            | s |
            | 2 |

    # Does NOT discriminate: with the comprehension on the main chain it is spliced below the Merge, so the SET still
    # reads a written slot and the value coincides. Kept as a regression guard only - the difference is plan shape,
    # and `MergeBranchComprehensionOverOuterSymbolStaysInBranch` in tests/unit/query_plan.cpp is what pins it.
    Scenario: Pattern comprehension in a MERGE ON CREATE over an already-bound outer node
        Given an empty graph
        And having executed:
            """
            CREATE (a:Person {name: 'Zoe'})-[:ACTED_IN]->(:Movie {title: 'M1'})
            """
        And having executed:
            """
            MATCH (p:Person)
            MERGE (q:Marker {id: 1})
              ON CREATE SET q.cnt = size([(p)-[:ACTED_IN]->(m) | m])
            """
        When executing query:
            """
            MATCH (q:Marker) RETURN q.cnt AS cnt
            """
        Then the result should be:
            | cnt |
            | 1   |

    # Discriminates, unlike the scenario above: the MERGE creates the edge the comprehension counts, so the value
    # differs by where the RollUpApply sits. On the main chain it is the Merge's input - evaluated before the create
    # and on the pre-MERGE view - and counts only the pre-existing edge.
    Scenario: Pattern comprehension in a MERGE ON CREATE counts what the MERGE just created
        Given an empty graph
        And having executed:
            """
            CREATE (a:Person {name: 'Zoe'})-[:ACTED_IN]->(:Movie {title: 'M1'})
            """
        And having executed:
            """
            MATCH (p:Person)
            MERGE (p)-[:ACTED_IN]->(q:Movie {title: 'New'})
              ON CREATE SET q.cnt = size([(p)-[:ACTED_IN]->(m) | m])
            """
        When executing query:
            """
            MATCH (q:Movie {title: 'New'}) RETURN q.cnt AS cnt
            """
        Then the result should be:
            | cnt |
            | 2   |

    # The comprehension reads `name`, which the CALL itself binds, so its RollUpApply must sit above the
    # CallProcedure. Below it the frame slot is unwritten on the first input row and stale from the previous row on
    # every later one, so the filter silently matched nothing and the query returned no rows.
    Scenario: Pattern comprehension in a CALL YIELD WHERE over a yielded symbol
        Given an empty graph
        And having executed:
            """
            CREATE (:Proc {name: 'mg.procedures'})-[:R]->(:Target)
            """
        When executing query:
            """
            CALL mg.procedures() YIELD name
            WHERE size([(p:Proc)-[:R]->(q) WHERE p.name = name | q]) > 0
            RETURN name
            """
        Then the result should be:
            | name            |
            | 'mg.procedures' |

    # A comprehension in a WITH is evaluated after the clauses before it, so after the DELETE - it can no longer
    # expand from the node that DELETE removed. Pinning the error: before origin-clause gating the comprehension was
    # drained at the DELETE and evaluated below it, returning a count of edges that no longer existed.
    Scenario: Pattern comprehension over a node deleted by a preceding clause raises an error
        Given an empty graph
        And having executed:
            """
            CREATE (:A)-[:R]->(:B)
            """
        When executing query:
            """
            MATCH (n:A) DETACH DELETE n
            WITH n, [(n)-[e:R]->(m) | m] AS lst
            RETURN size(lst) AS s
            """
        Then an error should be raised
