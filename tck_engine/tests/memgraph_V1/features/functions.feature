Feature: Functions

    Scenario: Sqrt test 01:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b{x: 7}), (c{x: 5}), (d{x: 'x'})
            """
        When executing query:
            """
            MATCH (a) RETURN SQRT(a.x) AS n
            """
        Then an error should be raised

    Scenario: Sqrt test 02:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b), (c{x: 9}), (d{x: null})
            """
        When executing query:
            """
            MATCH (a) RETURN SQRT(a.x) AS n
            """
        Then the result should be:
            | n    |
            | 1.0  |
            | null |
            | 3.0  |
            | null |

    Scenario: ToBoolean test 01:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b{x: 0}))
            """
        When executing query:
            """
            MATCH (a) RETURN TOBOOLEAN(a.x) AS n
            """
        Then an error should be raised

    Scenario: ToBoolean test 02:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 'TrUe'}), (b{x: 'not bool'}), (c{x: faLsE}), (d{x: null}), (e{x: 'fALse'}), (f{x: tRuE})
            """
        When executing query:
            """
            MATCH (a) RETURN TOBOOLEAN(a.x) AS n
            """
        Then the result should be:
            | n     |
            | true  |
            | null  |
            | false |
            | null  |
            | false |
            | true  |

    Scenario: ToInteger test 01:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: true}))
            """
        When executing query:
            """
            MATCH (a) RETURN TOINTEGER(a.x) AS n
            """
        Then an error should be raised

    Scenario: ToInteger test 02:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b{x: 'not int'}), (c{x: '-12'}), (d{x: null}), (e{x: '1.2'}), (f{x: '1.9'})
            """
        When executing query:
            """
            MATCH (a) RETURN TOINTEGER(a.x) AS n
            """
        Then the result should be:
            | n    |
            | 1    |
            | null |
            | -12  |
            | null |
            | 1    |
            | 1    |


    Scenario: ToFloat test 01:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: true}))
            """
        When executing query:
            """
            MATCH (a) RETURN TOFLOAT(a.x) AS n
            """
        Then an error should be raised

    Scenario: ToFloat test 02:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b{x: 'not float'}), (c{x: '-12'}), (d{x: null}), (e{x: '1.2'}), (f{x: 1.9})
            """
        When executing query:
            """
            MATCH (a) RETURN TOFLOAT(a.x) AS n
            """
        Then the result should be:
            | n     |
            | 1.0   |
            | null  |
            | -12.0 |
            | null  |
            | 1.2   |
            | 1.9   |


    Scenario: Abs test 01:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: true})
            """
        When executing query:
            """
            MATCH (a) RETURN ABS(a.x) AS n
            """
        Then an error should be raised

    Scenario: Abs test 02:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: '1.0'})
            """
        When executing query:
            """
            MATCH (a) RETURN ABS(a.x) AS n
            """
        Then an error should be raised

    Scenario: Abs test 03:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (c{x: -12}), (d{x: null}), (e{x: -2.3}), (f{x: 1.9})
            """
        When executing query:
            """
            MATCH (a) RETURN ABS(a.x) AS n
            """
        Then the result should be:
            | n    |
            | 1    |
            | 12   |
            | null |
            | 2.3  |
            | 1.9  |


    Scenario: Exp test 01:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: true}))
            """
        When executing query:
            """
            MATCH (a) RETURN EXP(a.x) AS n
            """
        Then an error should be raised

    Scenario: Exp test 02:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: '1.0'})),
            """
        When executing query:
            """
            MATCH (a) RETURN EXP(a.x) AS n
            """
        Then an error should be raised

    Scenario: Exp test 03:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (c{x: -12}), (d{x: null}), (e{x: -2.3}), (f{x: 1.9})
            """
        When executing query:
            """
            MATCH (a) RETURN EXP(a.x) AS n
            """
        Then the result should be:
            | n                     |
            | 2.718281828459045     |
            | .00000614421235332821 |
            | null                  |
            | 0.10025884372280375   |
            | 6.6858944422792685    |

    Scenario: Log test 01:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: true}))
            """
        When executing query:
            """
            MATCH (a) RETURN LOG(a.x) AS n
            """
        Then an error should be raised

    Scenario: Log test 02:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: '1.0'})),
            """
        When executing query:
            """
            MATCH (a) RETURN LOG(a.x) AS n
            """
        Then an error should be raised

    Scenario: Log test 03:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 0.123}), (c{x: -12}), (d{x: null}), (e{x: 27})
            """
        When executing query:
            """
            MATCH (a) RETURN LOG(a.x) AS n
            """
        Then the result should be:
            | n                   |
            | -2.0955709236097197 |
            | nan                 |
            | null                |
            | 3.295836866004329   |

    Scenario: Log10 test 01:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: true}))
            """
        When executing query:
            """
            MATCH (a) RETURN LOG10(a.x) AS n
            """
        Then an error should be raised

    Scenario: Log10 test 02:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: '1.0'})),
            """
        When executing query:
            """
            MATCH (a) RETURN LOG10(a.x) AS n
            """
        Then an error should be raised

    Scenario: Log10 test 03:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 0.123}), (c{x: -12}), (d{x: null}), (e{x: 27})
            """
        When executing query:
            """
            MATCH (a) RETURN LOG10(a.x) AS n
            """
        Then the result should be:
            | n                   |
            | -0.9100948885606021 |
            | nan                 |
            | null                |
            | 1.4313637641589874  |


    Scenario: Sin test 01:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: true}))
            """
        When executing query:
            """
            MATCH (a) RETURN SIN(a.x) AS n
            """
        Then an error should be raised

    Scenario: Sin test 02:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: '1.0'})),
            """
        When executing query:
            """
            MATCH (a) RETURN SIN(a.x) AS n
            """
        Then an error should be raised

    Scenario: Sin test 03:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 0.123}), (c{x: -12}), (d{x: null}), (e{x: 27})
            """
        When executing query:
            """
            MATCH (a) RETURN SIN(a.x) AS n
            """
        Then the result should be:
            | n                   |
            | 0.12269009002431533 |
            | 0.5365729180004349  |
            | null                |
            | 0.956375928404503   |

    Scenario: Cos test 01:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: true}))
            """
        When executing query:
            """
            MATCH (a) RETURN COS(a.x) AS n
            """
        Then an error should be raised

    Scenario: Cos test 02:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: '1.0'})),
            """
        When executing query:
            """
            MATCH (a) RETURN COS(a.x) AS n
            """
        Then an error should be raised

    Scenario: Cos test 03:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 0.123}), (c{x: -12}), (d{x: null}), (e{x: 27})
            """
        When executing query:
            """
            MATCH (a) RETURN COS(a.x) AS n
            """
        Then the result should be:
            | n                   |
            | 0.9924450321351935  |
            | 0.8438539587324921  |
            | null                |
            | -0.2921388087338362 |
