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
