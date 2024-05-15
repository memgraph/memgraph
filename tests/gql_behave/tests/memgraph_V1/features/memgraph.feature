Feature: Memgraph only tests (queries in which we choose to be incompatible with neo4j)

    Scenario: Multiple sets (undefined behaviour)
        Given an empty graph
        And having executed
            """
            CREATE (n{x: 3})-[:X]->(m{x: 5})
            """
        When executing query:
            """
            MATCH (n)--(m) SET n.x = n.x + 1 SET m.x = m.x + 2 SET m.x = n.x RETURN n.x
            """
	    # TODO: Figure out if we can define a test with multiple possible outputs in cucumber,
	    # until then this test just documents behaviour instead of testing it.
            #        Then the result should be:
            #            | n.x |    | n.x |
            #            |  5  | or |  7  |
            #            |  5  |    |  7  |

    Scenario: Multiple comparisons
        Given an empty graph
        When executing query:
            """
            RETURN 1 < 10 > 5 < 7 > 6 < 8 AS x
            """
        Then the result should be:
            | x    |
            | true |

    Scenario: Use deleted node
        Given an empty graph
        When executing query:
            """
            CREATE(a:A), (b:B), (c:C), (a)-[:T]->(b) WITH a DETACH DELETE a WITH a MATCH(a)-[r:T]->() RETURN r
            """
        Then an error should be raised

    Scenario: In test3
        When executing query:
            """
            WITH [[1], 2, 3, 4] AS l
            RETURN 1 IN l as x
            """
        Then the result should be:
            | x     |
            | false |

    Scenario: In test8
        When executing query:
            """
            WITH [[[[1]]], 2, 3, 4] AS l
            RETURN 1 IN l as x
            """
        Then the result should be:
            | x     |
            | false |

    Scenario: Keyword as symbolic name
        Given an empty graph
        And having executed
            """
            CREATE(a:DELete)
            """
        When executing query:
            """
            MATCH (n) RETURN n
            """
        Then the result should be:
            | n         |
            | (:DELete) |

    Scenario: Aggregation in CASE:
        Given an empty graph
        When executing query:
            """
            MATCH (n) RETURN CASE count(n) WHEN 10 THEN 10 END
            """
        Then an error should be raised

    Scenario: Create Enum:
        Given an empty graph
        When executing query:
            """
            CREATE ENUM Status VALUES { Good, Okay, Bad };
            """
        Then an error should be raised

    Scenario: Show Enums:
        Given an empty graph
        When executing query:
            """
            SHOW ENUMS;
            """
        Then an error should be raised
