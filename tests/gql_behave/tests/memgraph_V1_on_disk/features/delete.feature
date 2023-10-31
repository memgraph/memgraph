Feature: Delete

    Scenario: Delete all from database and yield number of deleted items
        Given an empty graph
        And having executed
            """
            CREATE (n), (m), (o)
            """
        When executing query:
            """
            MATCH (n) DETACH DELETE n RETURN COUNT(*) AS cnt
            """
        Then the result should be:
        | cnt |
        | 3   |

    Scenario: Delete all from database and match nothing after
        Given an empty graph
        And having executed
            """
            CREATE (n), (m)    Scenario: Detach deleting paths
        Given an empty graph
        And having executed:
        """
        CREATE (x:X), (n1), (n2), (n3)
        CREATE (x)-[:R]->(n1)
        CREATE (n1)-[:R]->(n2)
        CREATE (n2)-[:R]->(n3)
        """
        When executing query:
        """
        MATCH p = (:X)-->()-->()-->()
        DETACH DELETE p
        """
        Then the result should be empty
        And the side effects should be:
          | -nodes         | 4 |
          | -relationships | 3 |
          | -labels        | 1 |

            """
        When executing query:
            """
            MATCH (n) DETACH DELETE n WITH n MATCH (m) RETURN COUNT(*) AS cnt
            """
        Then the result should be:
        | cnt |
        | 0   |

    Scenario: Delete relationship in the pattern
        Given an empty graph
        And having executed
            """
            CREATE (n)-[:REL]->(m), (k)-[:REL]->(z)
            """
        When executing query:
            """
            MATCH (n)-[r:REL]->(m) DELETE r RETURN COUNT(*) AS cnt
            """
        Then the result should be:
        | cnt |
        | 2   |

    Scenario: Delete node and return property throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n {prop: 1});
            """
        When executing query:
            """
            MATCH (n) DETACH DELETE n RETURN n.prop AS prop
            """
        Then an error should be raised

    Scenario: Delete node, set property throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n {prop: 1});
            """
        When executing query:
            """
            MATCH (n) DETACH DELETE n SET n.prop = 2
            """
        Then the result should be empty

    Scenario: Delete node, set property and return throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n {prop: 1});
            """
        When executing query:
            """
            MATCH (n) DETACH DELETE n SET n.prop = 2 RETURN n
            """
        Then an error should be raised

    Scenario: Delete node, remove property throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n {prop: 1});
            """
        When executing query:
            """
            MATCH (n) DETACH DELETE n REMOVE n.prop
            """
        Then the result should be empty

    Scenario: Delete node, remove property and return throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n {prop: 1});
            """
        When executing query:
            """
            MATCH (n) DETACH DELETE n REMOVE n.prop RETURN n
            """
        Then an error should be raised

    Scenario: Delete node, set label throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n {prop: 1});
            """
        When executing query:
            """
            MATCH (n) DETACH DELETE n SET n:Label
            """
        Then the result should be empty

    Scenario: Delete node, set label and return throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n {prop: 1});
            """
        When executing query:
            """
            MATCH (n) DETACH DELETE n SET n:Label RETURN n
            """
        Then an error should be raised

    Scenario: Delete node, remove label throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n:Label {prop: 1});
            """
        When executing query:
            """
            MATCH (n) DETACH DELETE n REMOVE n:Label
            """
        Then the result should be empty

    Scenario: Delete node, remove label and return throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n:Label {prop: 1});
            """
        When executing query:
            """
            MATCH (n) DETACH DELETE n REMOVE n:Label RETURN n
            """
        Then an error should be raised

    Scenario: Delete node, set update properties and return throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n:Label {prop: 1});
            """
        When executing query:
            """
            MATCH (n) DETACH DELETE n SET n += {prop: 2} RETURN n
            """
        Then an error should be raised

    Scenario: Delete node, set properties throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n:Label {prop: 1});
            """
        When executing query:
            """
            MATCH (n) DETACH DELETE n SET n += {prop: 2}
            """
        Then the result should be empty

    Scenario: Delete node, set replace properties and return throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n:Label {prop: 1});
            """
        When executing query:
            """
            MATCH (n) DETACH DELETE n SET n = {prop: 2} RETURN n
            """
        Then an error should be raised

    Scenario: Delete node, set replace properties throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n:Label {prop: 1});
            """
        When executing query:
            """
            MATCH (n) DETACH DELETE n SET n = {prop: 2}
            """
        Then the result should be empty

    Scenario: Delete node, set property and return it with aggregation throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n:Label {prop: 1});
            """
        When executing query:
            """
            MATCH (n) DETACH DELETE n SET n.prop = 1 WITH n RETURN n
            """
        Then an error should be raised


    Scenario: Detach deleting paths
        Given an empty graph
        And having executed:
        """
         CREATE (x:X), (n1), (n2), (n3)
         CREATE (x)-[:R]->(n1)
         CREATE (n1)-[:R]->(n2)
         CREATE (n2)-[:R]->(n3)
         """
        When executing query:
          """
          MATCH p = (:X)-->()-->()-->()
          DETACH DELETE p
          """
        Then the result should be empty
        And the side effects should be:
          | -nodes         | 4 |
          | -relationships | 3 |
          | -labels        | 1 |
