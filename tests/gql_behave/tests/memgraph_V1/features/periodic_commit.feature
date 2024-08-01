Feature: Periodic commit

    Scenario: Using periodic commit unwind create with return
        Given an empty graph
        When executing query:
            """
            USING PERIODIC COMMIT 1 UNWIND range(1, 3) AS x CREATE (a:A {id: x})
            """
        Then the result should be empty

    Scenario: Using periodic commit unwind create with return
        Given an empty graph
        When executing query:
            """
            USING PERIODIC COMMIT 1 UNWIND range(1, 3) AS x CREATE (a:A {id: x}) RETURN a
            """
        Then the result should be:
            | a            |
            | (:A {id: 1}) |
            | (:A {id: 2}) |
            | (:A {id: 3}) |

    Scenario: Using periodic commit unwind create post verify check
        Given an empty graph
        And having executed
            """
            USING PERIODIC COMMIT 1 UNWIND range(1, 3) AS x CREATE (a:A {id: x})
            """
        When executing query:
            """
            MATCH (a) RETURN a
            """
        Then the result should be:
            | a            |
            | (:A {id: 1}) |
            | (:A {id: 2}) |
            | (:A {id: 3}) |

    Scenario: Call periodic commit unwind create without return
        Given an empty graph
        When executing query:
            """
            UNWIND range(1, 3) as x CALL { WITH x CREATE (:A {id: x}) } IN TRANSACTIONS OF 1 ROWS
            """
        Then the result should be empty

    Scenario: Call periodic commit unwind create with return
        Given an empty graph
        And having executed
            """
            UNWIND range(1, 3) as x CALL { WITH x CREATE (a:A {id: x}) RETURN a } IN TRANSACTIONS OF 1 ROWS RETURN a
            """
        Then the result should be:
            | a            |
            | (:A {id: 1}) |
            | (:A {id: 2}) |
            | (:A {id: 3}) |

    Scenario: Call periodic commit unwind create without return post check
        Given an empty graph
        And having executed
            """
            UNWIND range(1, 3) as x CALL { WITH x CREATE (a:A {id: x}) RETURN a } IN TRANSACTIONS OF 1 ROWS
            """
        When executing query:
            """
            MATCH (a) RETURN a
            """
        Then the result should be:
            | a            |
            | (:A {id: 1}) |
            | (:A {id: 2}) |
            | (:A {id: 3}) |

    Scenario: Call periodic commit unwind create without return post check 2
        Given an empty graph
        And having executed
            """
            UNWIND range(1, 3) as x CALL { CREATE (a:A) } IN TRANSACTIONS OF 1 ROWS
            """
        When executing query:
            """
            MATCH (a) RETURN count(a) AS cnt
            """
        Then the result should be:
            | cnt |
            | 3   |

    Scenario: Call periodic commit unwind create whole query nested
        Given an empty graph
        And having executed
            """
            CALL { UNWIND range(1, 3) as x CREATE (a:A) } IN TRANSACTIONS OF 1 ROWS
            """
        When executing query:
            """
            MATCH (a) RETURN count(a) AS cnt
            """
        Then the result should be:
            | cnt |
            | 3   |
