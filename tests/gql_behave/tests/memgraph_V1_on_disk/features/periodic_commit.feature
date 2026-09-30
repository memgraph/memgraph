Feature: Periodic commit

    Scenario: Not implemented periodic commit:
        Given an empty graph
        When executing query:
            """
            USING PERIODIC COMMIT 10 CREATE (a:A)
            """
        Then an error should be raised

    Scenario: Not implemented nested periodic commit:
        Given an empty graph
        When executing query:
            """
            UNWIND range(1, 100) as x CALL { CREATE (:N) } IN TRANSACTIONS OF 10 ROWS;
            """
        Then an error should be raised
