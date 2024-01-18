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

    Scenario: Label parameters in match:
        Given an empty graph
        And having executed:
            """
            CREATE (a:Label1 {x : 10})
            """
        And parameters are:
            | a     | 10     |
            | label | Label1 |
        When executing query:
            """
            MATCH (a:$label {x : $a}) RETURN a
            """
        Then the result should be:
            | a                |
            | (:Label1{x: 10}) |

    Scenario: Label parameters in create and match
        Given an empty graph
        And parameters are:
            | a     | 10     |
            | label | Label1 |
        When executing query:
            """
            CREATE (a:$label {x: $a})
            """
        When executing query:
            """
            MATCH (a:$label {x: $a}) RETURN a
            """
        Then the result should be:
            | a                |
            | (:Label1{x: 10}) |

    Scenario: Label parameters in merge
        Given an empty graph
        And parameters are:
            | a     | 10     |
            | label | Label1 |
        When executing query:
            """
            MERGE (a:$label {x: $a}) RETURN a
            """
        Then the result should be:
            | a                |
            | (:Label1{x: 10}) |

    Scenario: Label parameters in set label
        Given an empty graph
        And having executed:
            """
            CREATE (a:Label1 {x : 10})
            """
        And parameters are:
            | new_label | Label2 |
        When executing query:
            """
            MATCH (a:Label1 {x: 10}) SET a:$new_label
            """
        When executing query:
            """
            MATCH (a:Label1:Label1 {x: 10}) RETURN a
            """
        Then the result should be:
            | a                        |
            | (:Label1:Label2 {x: 10}) |

    Scenario: Label parameters in remove label
        Given an empty graph
        And having executed:
            """
            CREATE (a:Label1:LabelToRemove {x : 10})
            """
        And parameters are:
            | label_to_remove | LabelToRemove |
        When executing query:
            """
            MATCH (a {x: 10}) REMOVE a:$label_to_remove
            """
        When executing query:
            """
            MATCH (a {x: 10}) RETURN a
            """
        Then the result should be:
            | a                 |
            | (:Label1 {x: 10}) |
