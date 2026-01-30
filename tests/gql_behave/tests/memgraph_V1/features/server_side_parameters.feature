Feature: Server-side parameters

    Scenario: Set global parameter and show parameters
        Given an empty graph
        When executing query:
            """
            SET GLOBAL PARAMETER x="value"
            """
        When executing query:
            """
            SHOW PARAMETERS
            """
        Then the result should be:
            | name | value     | scope   |
            | 'x'  | '"value"' | 'global' |

    Scenario: Set global parameters (literal and from client parameter) and show
        Given an empty graph
        And parameters are:
            | config | "my_config" |
        When executing query:
            """
            SET GLOBAL PARAMETER y=$config
            """
        When executing query:
            """
            SET GLOBAL PARAMETER x="value"
            """
        When executing query:
            """
            SHOW PARAMETERS
            """
        Then the result should be (ignoring element order for lists):
            | name | value        | scope   |
            | 'x'  | '"value"'    | 'global' |
            | 'y'  | '"my_config"'| 'global' |

    Scenario: Unset parameter
        Given an empty graph
        And having executed:
            """
            SET GLOBAL PARAMETER x="value"
            """
        When executing query:
            """
            UNSET PARAMETER x
            """
        When executing query:
            """
            SHOW PARAMETERS
            """
        Then the result should be empty

    Scenario: Use server side parameters in queries
        Given an empty graph
        And having executed:
            """
            SET GLOBAL PARAMETER x="value"
            """
       And having executed:
            """
            CREATE (:Node {property: $x});
            """
        When executing query:
            """
            MATCH (n) RETURN n;
            """
        Then the result should be:
            | n                           |
            | (:Node {property: 'value'}) |

    Scenario: Override server side parameters in queries
        Given an empty graph
        And parameters are:
            | x | "overrrided_value" |
        And having executed:
            """
            SET GLOBAL PARAMETER x="value"
            """
       And having executed:
            """
            CREATE (:Node {property: $x});
            """
        When executing query:
            """
            MATCH (n) RETURN n;
            """
        Then the result should be:
            | n                                      |
            | (:Node {property: 'overrrided_value'}) |
