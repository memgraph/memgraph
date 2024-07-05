Feature: Periodic commit

    Scenario: Not implemented:
        Given an empty graph
        When executing query:
            """
            USING PERIODIC COMMIT 10 CREATE (a:A)
            """
        Then an error should be raised
