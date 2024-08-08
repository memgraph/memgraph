Feature: Periodic delete

    Scenario: Periodic delete all from database and yield number of deleted items
        Given an empty graph
        And having executed
            """
            CREATE (n), (m), (o)
            """
        When executing query:
            """
            USING PERIODIC COMMIT 1 MATCH (n) DETACH DELETE n RETURN COUNT(*) AS cnt
            """
        Then the result should be:
        | cnt |
        | 3   |

    Scenario: Periodic delete all from database and match nothing after
        Given an empty graph
        And having executed
            """
            CREATE (n), (m)
            """
        When executing query:
            """
            USING PERIODIC COMMIT 1  MATCH (n) DETACH DELETE n WITH n MATCH (m) RETURN COUNT(*) AS cnt
            """
        Then the result should be:
        | cnt |
        | 0   |

    Scenario: Periodic delete relationship in the pattern
        Given an empty graph
        And having executed
            """
            CREATE (n)-[:REL]->(m), (k)-[:REL]->(z)
            """
        When executing query:
            """
            USING PERIODIC COMMIT 1 MATCH (n)-[r:REL]->(m) DELETE r RETURN COUNT(*) AS cnt
            """
        Then the result should be:
        | cnt |
        | 2   |

    Scenario: Periodic delete node and return property throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n {prop: 1});
            """
        When executing query:
            """
            USING PERIODIC COMMIT 1  MATCH (n) DETACH DELETE n RETURN n.prop AS prop
            """
        Then an error should be raised

    Scenario: Periodic delete node, set property throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n {prop: 1});
            """
        When executing query:
            """
            USING PERIODIC COMMIT 1 MATCH (n) DETACH DELETE n SET n.prop = 2
            """
        Then an error should be raised

    Scenario: Periodic delete node, set property and return throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n {prop: 1});
            """
        When executing query:
            """
            USING PERIODIC COMMIT 1 MATCH (n) DETACH DELETE n SET n.prop = 2 RETURN n
            """
        Then an error should be raised

    Scenario: Periodic delete node, remove property throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n {prop: 1});
            """
        When executing query:
            """
            USING PERIODIC COMMIT 1 (n) DETACH DELETE n REMOVE n.prop
            """
        Then an error should be raised

    Scenario: Periodic delete node, remove property and return throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n {prop: 1});
            """
        When executing query:
            """
            USING PERIODIC COMMIT 1 MATCH (n) DETACH DELETE n REMOVE n.prop RETURN n
            """
        Then an error should be raised

    Scenario: Periodic delete node, set label throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n {prop: 1});
            """
        When executing query:
            """
            USING PERIODIC COMMIT 1 MATCH (n) DETACH DELETE n SET n:Label
            """
        Then an error should be raised

    Scenario: Periodic delete node, set label and return throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n {prop: 1});
            """
        When executing query:
            """
            USING PERIODIC COMMIT 1 MATCH (n) DETACH DELETE n SET n:Label RETURN n
            """
        Then an error should be raised

    Scenario: Periodic delete node, remove label throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n:Label {prop: 1});
            """
        When executing query:
            """
            USING PERIODIC COMMIT 1 MATCH (n) DETACH DELETE n REMOVE n:Label
            """
        Then an error should be raised

    Scenario: Periodic delete node, remove label and return throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n:Label {prop: 1});
            """
        When executing query:
            """
            USING PERIODIC COMMIT 1 MATCH (n) DETACH DELETE n REMOVE n:Label RETURN n
            """
        Then an error should be raised

    Scenario: Periodic delete node, set update properties and return throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n:Label {prop: 1});
            """
        When executing query:
            """
            USING PERIODIC COMMIT 1 MATCH (n) DETACH DELETE n SET n += {prop: 2} RETURN n
            """
        Then an error should be raised

    Scenario: Periodic delete node, set properties throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n:Label {prop: 1});
            """
        When executing query:
            """
            USING PERIODIC COMMIT 1 MATCH (n) DETACH DELETE n SET n += {prop: 2}
            """
        Then an error should be raised

    Scenario: Periodic delete node, set replace properties and return throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n:Label {prop: 1});
            """
        When executing query:
            """
            USING PERIODIC COMMIT 1 MATCH (n) DETACH DELETE n SET n = {prop: 2} RETURN n
            """
        Then an error should be raised

    Scenario: Peirodic delete node, set replace properties throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n:Label {prop: 1});
            """
        When executing query:
            """
            USING PERIODIC COMMIT 1 MATCH (n) DETACH DELETE n SET n = {prop: 2}
            """
        Then an error should be raised

    Scenario: Periodic delete node, set property and return it with aggregation throws an error
        Given an empty graph
        And having executed
            """
            CREATE (n:Label {prop: 1});
            """
        When executing query:
            """
            USING PERIODIC COMMIT 1 MATCH (n) DETACH DELETE n SET n.prop = 1 WITH n RETURN n
            """
        Then an error should be raised

    Scenario: Periodic delete a relationship that is already deleted in a previous DETACH DELETE clause
        Given an empty graph
        When executing query:
        """
        USING PERIODIC COMMIT 1 CREATE (n0)<-[r0:T]-(n1) DETACH DELETE n0 DETACH DELETE r0 RETURN n1;
        """
        Then the result should be:
        | n1 |
        | () |


    Scenario: Periodic detach deleting paths
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
          USING PERIODIC COMMIT 1 MATCH p = (:X)-->()-->()-->()
          DETACH DELETE p
          """
        Then an error should be raised
