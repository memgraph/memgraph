Feature: List comparison

    Scenario: Lists compare lexicographically
        When executing query:
            """
            RETURN [1, 2] < [1, 3] AS a, [2] > [1, 9] AS b, [1] < [1, 0] AS c, [1] < [1, null] AS d,
                   [1, 2] >= [1, null] AS e, [null, 1] < [null, 2] AS f, [1, 'a'] < [1, 1] AS g,
                   [1, 'a'] < [2, 1] AS h, [[1, 2], [3]] > [[1, 2], [2, 9]] AS i, [1] < 1 AS j,
                   [] < [null] AS k, [1] <= [1.0] AS l
            """
        Then the result should be:
            | a    | b    | c    | d    | e    | f    | g    | h    | i    | j    | k    | l    |
            | true | true | true | true | null | null | null | true | true | null | true | true |

    Scenario: A NaN element leaves two lists incomparable
        When executing query:
            """
            RETURN [1] < [0.0 / 0.0] AS a, [0.0 / 0.0, 1] < [0.0 / 0.0, 2] AS b, [[0.0 / 0.0]] < [[1]] AS c,
                   NOT ([1] < [0.0 / 0.0]) AS d, [1, 0.0 / 0.0] < [2] AS e, 1 < 0.0 / 0.0 AS f
            """
        Then the result should be:
            | a    | b    | c    | d    | e    | f     |
            | null | null | null | null | true | false |

    Scenario Outline: A list lower bound keeps the same rows with and without an index
        Given an empty graph
        And having executed:
            """
            UNWIND [[1], [1, 2], [1, 3], [1, null], [null, 1], [2], 5, 'a'] AS v
            CREATE (:L {p: v, q: 1})
            """
        And having executed:
            """
            <index>
            """
        When executing query:
            """
            MATCH (n:L) WHERE n.q = 1 AND n.p > [1, 2] RETURN n.p AS p ORDER BY p
            """
        Then the result should be, in order:
            | p      |
            | [1, 3] |
            | [2]    |

        Examples:
            | index                       |
            | RETURN 1                    |
            | CREATE INDEX ON :L(p)       |
            | CREATE INDEX ON :L(q, p)    |
            | CREATE GLOBAL INDEX ON :(p) |

    Scenario Outline: A list upper bound keeps the same rows with and without a global index
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
            MATCH (n) WHERE n.p <= [1, 3] RETURN n.p AS p ORDER BY p
            """
        Then the result should be, in order:
            | p      |
            | [1]    |
            | [1, 2] |
            | [1, 3] |

        Examples:
            | index                       |
            | RETURN 1                    |
            | CREATE GLOBAL INDEX ON :(p) |

    Scenario Outline: A list bound on an edge property keeps the same rows with and without an index
        Given an empty graph
        And having executed:
            """
            UNWIND [[1], [1, 2], [1, 3], [1, null], [null, 1], [2], 5, 'a'] AS v
            CREATE (:L)-[:R {p: v}]->(:M)
            """
        And having executed:
            """
            <index>
            """
        When executing query:
            """
            MATCH ()-[r:R]->() WHERE r.p > [1, 2] RETURN r.p AS p ORDER BY p
            """
        Then the result should be, in order:
            | p      |
            | [1, 3] |
            | [2]    |

        Examples:
            | index                            |
            | RETURN 1                         |
            | CREATE EDGE INDEX ON :R(p)       |
            | CREATE GLOBAL EDGE INDEX ON :(p) |
