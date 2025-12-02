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

   Scenario: Pattern comprehension in CREATE
        Given an empty graph
        When executing query:
            """
            CREATE (n {prop: [()--() | 1]}) RETURN n.prop AS prop
            """
        Then the result should be:
            | prop |
            | []   |

   Scenario: Pattern comprehension in CREATE with data
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 1})-[:R]->(b:N {id: 2})
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
            CREATE (n:N {id: 1})
            """
        When executing query:
            """
            MATCH (n) SET n.prop = [()--() | 1] RETURN n.prop AS prop
            """
        Then the result should be:
            | prop |
            | []   |

   Scenario: Pattern comprehension in SET with data
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 1})-[:R]->(b:N {id: 2}), (c:N {id: 3})
            """
        When executing query:
            """
            MATCH (n) WHERE n.id = 3 SET n.prop = [()--() | 1] RETURN n.prop AS prop
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
        When executing query:
            """
            MERGE (n:Test2) ON CREATE SET n.prop = [()--() | 1] RETURN n.prop AS prop
            """
        Then the result should be:
            | prop |
            | []   |

   Scenario: Pattern comprehension in MERGE ON CREATE SET with data
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 1})-[:R]->(b:N {id: 2})
            """
        When executing query:
            """
            MERGE (n:Test3) ON CREATE SET n.prop = [()--() | 1] RETURN n.prop AS prop
            """
        Then the result should be:
            | prop   |
            | [1, 1] |

   Scenario: Pattern comprehension referencing CREATE-created node in SET
        Given an empty graph
        When executing query:
            """
            CREATE (n:Node {id: 1})-[:R]->(m:Node {id: 2}) SET n.prop = [(n)-->(x) | x.id] RETURN n.prop AS prop
            """
        Then the result should be:
            | prop |
            | [2]  |

   Scenario: Pattern comprehension referencing MERGE-created node in SET
        Given an empty graph
        When executing query:
            """
            MERGE (n:Node {id: 1})-[:R]->(m:Node {id: 2}) SET n.prop = [(n)-->(x) | x.id] RETURN n.prop AS prop
            """
        Then the result should be:
            | prop |
            | [2]  |

   Scenario: Pattern comprehension in RETURN referencing same CREATE node
        Given an empty graph
        When executing query:
            """
            CREATE (n:Node {id: 1})-[:R]->(m:Node {id: 2}) RETURN [(n)-->(x) | x.id] AS neighbors
            """
        Then the result should be:
            | neighbors |
            | [2]       |

   Scenario: Multiple pattern comprehensions with different CREATE dependencies
        Given an empty graph
        When executing query:
            """
            CREATE (a:Node {id: 1})-[:R]->(b:Node {id: 2}), (c:Node {id: 3})
            SET a.neighbors = [(a)-->(x) | x.id], c.count = size([(c)--() | 1])
            RETURN a.neighbors AS a_neighbors, c.count AS c_count
            """
        Then the result should be:
            | a_neighbors | c_count |
            | [2]         | 0       |

   Scenario: Named path variable in pattern comprehension
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 1})-[:R]->(b:N {id: 2})
            """
        When executing query:
            """
            MATCH (n) RETURN [path = (n)--() | length(path)] AS lengths ORDER BY n.id
            """
        Then the result should be:
            | lengths |
            | [1]     |
            | [1]     |

   Scenario: Named path variable in pattern comprehension with multiple hops
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 1})-[:R]->(b:N {id: 2})-[:R]->(c:N {id: 3})
            """
        When executing query:
            """
            MATCH (n) WHERE n.id = 1 RETURN [path = (n)-[*1..2]->() | length(path)] AS lengths
            """
        Then the result should be:
            | lengths |
            | [1, 2]  |

   Scenario: Pattern comprehension in FOREACH referencing loop variable in WHERE
        Given an empty graph
        And having executed:
            """
            CREATE (a:Source {id: 1})-[:R]->(b:Target {val: 10}), (a)-[:R]->(c:Target {val: 20})
            """
        And having executed:
            """
            FOREACH (x IN [1, 2] | CREATE (n:Result {source_id: x, found: [(a:Source)-[:R]->(t) WHERE a.id = x | t.val]}))
            """
        When executing query:
            """
            MATCH (n:Result) RETURN n.source_id AS id, n.found AS found ORDER BY id
            """
        Then the result should be:
            | id | found     |
            | 1  | [10, 20]  |
            | 2  | []        |

   Scenario: Pattern comprehension in FOREACH CREATE property
        Given an empty graph
        And having executed:
            """
            CREATE (:N {id: 1})-[:R]->(:N {id: 2})
            """
        And having executed:
            """
            FOREACH (i IN [1, 2, 3] | CREATE (n:X {val: i, neighbors: [()-[:R]->() | 1]}))
            """
        When executing query:
            """
            MATCH (n:X) RETURN n.val AS val, n.neighbors AS neighbors ORDER BY val
            """
        Then the result should be:
            | val | neighbors |
            | 1   | [1]       |
            | 2   | [1]       |
            | 3   | [1]       |
