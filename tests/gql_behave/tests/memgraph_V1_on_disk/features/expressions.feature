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
