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
