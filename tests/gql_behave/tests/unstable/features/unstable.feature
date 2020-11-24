Feature: Unstable

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
