Feature: Edge Indices Usage
    Scenario: Scan all by edge type and property equality:
        Given an empty graph
        And having executed
            """
            CREATE EDGE INDEX ON :E(p);
            """
        And having executed
            """
            CREATE ()-[:E {p: 1}]->(), ()-[:E {p: 2}]->(), ()-[:E {p: 2}]->(), ()-[:F {p: 2}]->()
            """
        When executing query:
            """
            MATCH ()-[e:E {p: 2}]->() RETURN count(e) AS result
            """
        Then the result should be:
            | result |
            | 2      |

    Scenario: Scan all by edge property range:
        Given an empty graph
        And having executed
            """
            CREATE GLOBAL EDGE INDEX ON :(p);
            """
        And having executed
            """
            CREATE ()-[:E {p: 10}]->(), ()-[:E {p: 20}]->(), ()-[:F {p: 30}]->()
            """
        When executing query:
            """
            MATCH ()-[e]->() WHERE e.p > 15 RETURN count(e) AS result
            """
        Then the result should be:
            | result |
            | 2      |

    Scenario: Scan all by edge type and property range:
        Given an empty graph
        And having executed
            """
            CREATE EDGE INDEX ON :E(p);
            """
        And having executed
            """
            CREATE ()-[:E {p: 10}]->(), ()-[:E {p: 20}]->(), ()-[:F {p: 20}]->()
            """
        When executing query:
            """
            MATCH ()-[e:E]->() WHERE e.p >= 15 RETURN count(e) AS result
            """
        Then the result should be:
            | result |
            | 1      |
