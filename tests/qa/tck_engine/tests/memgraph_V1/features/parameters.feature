Feature: Parameters

    Scenario: Simple parameter names:
        Given an empty graph
        And parameters are:
            | y | 2 |
            | x | 1 |
        When executing query:
            """
            RETURN $x, $y, 5
            """
        Then the result should be:
            | $x | $y | 5 |
            | 1  | 2  | 5 |

    Scenario: Integers as parameter names:
        Given an empty graph
        And parameters are:
            | 0 | 5 |
            | 2 | 6 |
        When executing query:
            """
            RETURN $0, $2
            """
        Then the result should be:
            | $0 | $2 |
            | 5  | 6  |

    Scenario: Escaped symbolic names as parameter names:
        Given an empty graph
        And parameters are:
            | a b  | 2 |
            | a `b | 3 |
        When executing query:
            """
            RETURN $`a b`, $`a ``b`
            """
        Then the result should be:
            | $`a b` | $`a ``b` |
            |   2    |    3     |

    Scenario: Lists as parameters:
        Given an empty graph
        And parameters are:
            | a  | [1, 2, 3] |
        When executing query:
            """
            RETURN $a
            """
        Then the result should be:
            |       $a       |
            |   [1, 2, 3]    |

    Scenario: Parameters in match:
	Given an empty graph
        And having executed:
            """
            CREATE (a {x : 10})
            """
        And parameters are:
            | a | 10 |
        When executing query:
            """
            MATCH (a {x : $a}) RETURN a.x
            """
        Then the result should be:
            | a.x |
            | 10  |
