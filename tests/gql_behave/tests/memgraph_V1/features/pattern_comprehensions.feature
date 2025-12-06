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

    Scenario: Pattern comprehension in where
        Given an empty graph
        When executing query:
            """
            WITH 1 AS a
            WHERE [(b)--() | b] = []
            RETURN *;
            """
        Then the result should be:
            | a  |
            | 1  |
