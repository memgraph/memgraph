Feature: Unstable

    Scenario: With test 01:
        Given an empty graph
        And having executed:
            """
            CREATE (a:A), (b:B), (c:C), (d:D), (e:E), (a)-[:R]->(b), (b)-[:R]->(c), (b)-[:R]->(d), (c)-[:R]->(a), (c)-[:R]->(e), (d)-[:R]->(e)
            """
        When executing query:
            """
            MATCH (:A)--(a)-->() WITH a, COUNT(*) AS n WHERE n > 1 RETURN a
            """
        Then the result should be:
            | a    |
            | (:B) |

    Scenario: Count test 06:
        Given an empty graph
        And having executed
            """
            CREATE (), (), (), (), ()
            """
        When executing query:
            """
            MATCH (n) RETURN COUNT(*) AS n
            """
        Then the result should be:
            | n |
            | 5 |

    Scenario: Test exponential operator
        When executing query:
            """
            RETURN 3^2=81^0.5 as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test one big mathematical equation
        When executing query:
            """
            RETURN (3+2*4-3/2%2*10)/5.0^2.0=0.04 as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Keys test:
        When executing query:
            """
            RETURN KEYS( {true: 123, a: null, b: 'x', null: null} ) AS a
            """
        Then the result should be:
            | a                          |
            | ['true', 'a', 'b', 'null'] |

    Scenario: StartsWith test4
        Given an empty graph
        And having executed
            """
            CREATE(a{name: 1}), (b{name: 2}), (c{name: null})
            """
        When executing query:
            """
            MATCH (n)
            WHERE n.name STARTS WITH null
            return n.name
            """
        Then the result should be empty

    Scenario: EndsWith test4
        Given an empty graph
        And having executed
            """
            CREATE(a{name: 1}), (b{name: 2}), (c{name: null})
            """
        When executing query:
            """
            MATCH (n)
            WHERE n.name ENDS WITH null
            return n.name
            """
        Then the result should be empty

    Scenario: Contains test4
        Given an empty graph
        And having executed
            """
            CREATE(a{name: 1}), (b{name: 2}), (c{name: null})
            """
        When executing query:
            """
            MATCH (n)
            WHERE n.name CONTAINS null
            return n.name
            """
        Then the result should be empty

    Scenario: E test:
        When executing query:
            """
            RETURN E() as n
            """
        Then the result should be:
            | n                   |
            | 2.718281828459045   |
