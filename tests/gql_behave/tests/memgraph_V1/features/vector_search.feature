Feature: Vector search related features

    Scenario: Create vector index:
        Given an empty graph
        And with new vector index :L1(prop1)
        And having executed
            """
            CREATE (:L1 {prop1: [1.0, 2.0, 3.0]});
            """
        When executing query:
            """
            SHOW INDEX INFO
            """
        Then the result should be:
            | index type | label | property | count |
            | 'vector'   | 'L1'  | 'prop1'  | 1     |

    Scenario: Drop vector index:
        Given an empty graph
        And having executed
            """
            CREATE VECTOR INDEX ON :L1(prop1);
            """
        And having executed
            """
            DROP VECTOR INDEX ON :L1(prop1);
            """
        When executing query:
            """
            SHOW INDEX INFO
            """
        Then the result should be:
            | index type             | label | property | count |
