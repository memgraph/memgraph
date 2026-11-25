Feature: Expressions

    Scenario: Test equal operator
        Given an empty graph
        When executing query:
            """
            CREATE (a)
            RETURN 1=1 and 1.0=1.0 and 'abc'='abc' and false=false and a.age is null as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test not equal operator
        Given an empty graph
        When executing query:
            """
            CREATE (a{age: 1})
            RETURN not 1<>1 and 1.0<>1.1 and 'abcd'<>'abc' and false<>true and a.age is not null as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test greater operator
        Given an empty graph
        When executing query:
            """
            RETURN 2>1 and not 1.0>1.1 and 'abcd'>'abc' as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test less operator
        Given an empty graph
        When executing query:
            """
            RETURN not 2<1 and 1.0<1.1 and not 'abcd'<'abc' as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test greater equal operator
        Given an empty graph
        When executing query:
            """
            RETURN 2>=2 and not 1.0>=1.1 and 'abcd'>='abc' as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test less equal operator
        Given an empty graph
        When executing query:
            """
            RETURN 2<=2 and 1.0<=1.1 and not 'abcd'<='abc' as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test plus operator
        Given an empty graph
        When executing query:
            """
            RETURN 3+2=1.09+3.91 as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test minus operator
        Given an empty graph
        When executing query:
            """
            RETURN 3-2=1.09-0.09 as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test multiply operator
        Given an empty graph
        When executing query:
            """
            RETURN 3*2=1.5*4 as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test divide operator1
        Given an empty graph
        When executing query:
            """
            RETURN 3/2<>7.5/5 as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test divide operator2
        Given an empty graph
        When executing query:
            """
            RETURN 3.0/2=7.5/5 as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test mod operator
        Given an empty graph
        When executing query:
            """
            RETURN 3%2=1 as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test one big logical equation
        Given an empty graph
        When executing query:
            """
            RETURN not true or true and false or not ((true xor false or true) and true or false xor true ) as n
            """
        Then the result should be:
            |   n   |
            | false |

    Scenario: Test IS NULL binds looser than arithmetic
        Given an empty graph
        When executing query:
            """
            RETURN (null + 1) * 1 IS NULL AS result
            """
        Then the result should be:
            | result |
            | true   |

    Scenario Outline: Integer arithmetic that leaves the int64 range raises an error
        Given an empty graph
        When executing query:
            """
            RETURN <expression> AS r
            """
        Then an error should be raised

        Examples:
            | expression                                |
            | 9223372036854775807 + 1                   |
            | (-9223372036854775807 - 1) - 1            |
            | 9223372036854775807 * 2                   |
            | (-9223372036854775807 - 1) * -1           |
            | -(-9223372036854775807 - 1)               |
            | (-9223372036854775807 - 1) / -1           |
            | 1 / 0                                     |
            | 1 % 0                                     |

    Scenario Outline: Integer arithmetic at the edge of the int64 range
        Given an empty graph
        When executing query:
            """
            RETURN <expression> AS r
            """
        Then the result should be:
            | r       |
            | <value> |

        Examples:
            | expression                                  | value                |
            | (-9223372036854775807 - 1) % -1             | 0                    |
            | (-9223372036854775807 - 1) / 1              | -9223372036854775808 |
            | (-9223372036854775807 - 1 + 1) / -1         | 9223372036854775807  |
            | 9223372036854775807 - 1 + 1                 | 9223372036854775807  |

    Scenario: Constant folding leaves an overflow in an untaken branch unevaluated
        Given an empty graph
        When executing query:
            """
            RETURN CASE WHEN false THEN 9223372036854775807 + 1 ELSE 0 END AS r
            """
        Then the result should be:
            | r |
            | 0 |

    Scenario: Double division by zero does not raise
        Given an empty graph
        When executing query:
            """
            RETURN 5.0 / 0 AS r
            """
        Then the result should be:
            | r   |
            | inf |
