Feature: Server-side parameters

    Scenario: Set global parameter and show parameters
        Given an empty graph
        When executing query:
            """
            SET GLOBAL PARAMETER x='value'
            """
        When executing query:
            """
            SHOW PARAMETERS
            """
        Then the result should be:
            | name | value       | scope   |
            | 'x'  | '"value"' | 'global' |

    Scenario: Set database parameter and show parameters
        Given an empty graph
        When executing query:
            """
            SET PARAMETER x='value'
            """
        When executing query:
            """
            SHOW PARAMETERS
            """
        Then the result should be:
            | name | value       | scope    |
            | 'x'  | '"value"' | 'database' |

    Scenario: Set both global and database parameter and show parameters
        Given an empty graph
        When executing query:
            """
            SET GLOBAL PARAMETER x='global_val'
            """
        When executing query:
            """
            SET PARAMETER y='db_val'
            """
        When executing query:
            """
            SHOW PARAMETERS
            """
        Then the result should be (ignoring element order for lists):
            | name | value          | scope    |
            | 'x'  | '"global_val"' | 'global'   |
            | 'y'  | '"db_val"'     | 'database' |

    Scenario: Cannot set database parameter when global parameter with same name exists
        Given an empty graph
        And having executed:
            """
            SET GLOBAL PARAMETER x='global_val'
            """
        When executing query:
            """
            SET PARAMETER x='db_val'
            """
        Then an error should be raised

    Scenario: Unset database parameter
        Given an empty graph
        And having executed:
            """
            SET PARAMETER x='value'
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

    Scenario: Changing database shows only that database's parameters
        Given an empty graph
        And having executed:
            """
            CREATE DATABASE db_a;
            """
        And having executed:
            """
            CREATE DATABASE db_b;
            """
        And having executed:
            """
            USE DATABASE db_a;
            """
        And having executed:
            """
            SET PARAMETER x='from_a';
            """
        And having executed:
            """
            USE DATABASE db_b;
            """
        When executing query:
            """
            SHOW PARAMETERS
            """
        Then the result should be empty

    Scenario: Use database parameter in query
        Given an empty graph
        And having executed:
            """
            SET PARAMETER x='value'
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

    Scenario: Set global parameters (literal and from client parameter) and show
        Given an empty graph
        And parameters are:
            | config | my_config |
        When executing query:
            """
            SET GLOBAL PARAMETER y=$config
            """
        When executing query:
            """
            SET GLOBAL PARAMETER x='value'
            """
        When executing query:
            """
            SHOW PARAMETERS
            """
        Then the result should be (ignoring element order for lists):
            | name | value            | scope   |
            | 'x'  | '"value"'      | 'global' |
            | 'y'  | '"my_config"'  | 'global' |

    Scenario: Unset parameter
        Given an empty graph
        And having executed:
            """
            SET GLOBAL PARAMETER x='value'
            """
        When executing query:
            """
            UNSET GLOBAL PARAMETER x
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
            SET GLOBAL PARAMETER x='value'
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
            | x | overrrided_value |
        And having executed:
            """
            SET GLOBAL PARAMETER x='value'
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

    Scenario: Set global parameter with literal expression (map, list)
        Given an empty graph
        When executing query:
            """
            SET GLOBAL PARAMETER x={a: 1, b: 2}
            """
        When executing query:
            """
            SHOW PARAMETERS
            """
        Then the result should be:
            | name | value                | scope   |
            | 'x'  | '{"a":1,"b":2}'     | 'global' |
        When executing query:
            """
            SET GLOBAL PARAMETER y=[10, 20, 30]
            """
        When executing query:
            """
            SHOW PARAMETERS
            """
        Then the result should be (ignoring element order for lists):
            | name | value                | scope   |
            | 'x'  | '{"a":1,"b":2}'     | 'global' |
            | 'y'  | '[10,20,30]'         | 'global' |

    Scenario: Set global parameter with config map (quoted keys, literal : literal)
        Given an empty graph
        When executing query:
            """
            SET GLOBAL PARAMETER x={'k1': 'v1', "k2": 2}
            """
        When executing query:
            """
            SHOW PARAMETERS
            """
        Then the result should be:
            | name | value                    | scope   |
            | 'x'  | '{"k1":"v1","k2":2}'    | 'global' |

    Scenario: Set global parameter with config nested config map
        Given an empty graph
        When executing query:
            """
            SET GLOBAL PARAMETER x={ key: {nested1: "value1", nested2: "value"} }
            """
        And having executed:
            """
            CREATE (:Node {property: $x.key.nested1});
            """
        When executing query:
            """
            MATCH (n) RETURN n;
            """
        Then the result should be:
            | n                            |
            | (:Node {property: 'value1'}) |
