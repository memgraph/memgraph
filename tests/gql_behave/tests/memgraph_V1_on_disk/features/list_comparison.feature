Feature: List comparison

    Scenario: Lists compare lexicographically
        When executing query:
            """
            RETURN [1] < [1, null] AS a, [1, 2] >= [1, null] AS b, [1] < [0.0 / 0.0] AS c
            """
        Then the result should be:
            | a    | b    | c    |
            | true | null | null |

    Scenario Outline: A list bound keeps the same rows with and without an index
        Given an empty graph
        And having executed:
            """
            UNWIND [[1], [1, 2], [1, 3], [1, null], [null, 1], [2], 5, 'a'] AS v
            CREATE (:L {p: v})
            """
        And having executed:
            """
            <index>
            """
        When executing query:
            """
            MATCH (n:L) WHERE n.p > [1, 2] RETURN n.p AS p
            """
        Then the result should be:
            | p      |
            | [1, 3] |
            | [2]    |

        Examples:
            | index                 |
            | RETURN 1              |
            | CREATE INDEX ON :L(p) |
