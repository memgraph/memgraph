Feature: INT64_MIN literal should parse successfully

    Scenario: RETURN -9223372036854775808 returns INT64_MIN
        Given an empty graph
        When executing query:
            """
            RETURN -9223372036854775808 AS x
            """
        Then the result should be:
            | x                    |
            | -9223372036854775808 |

    Scenario: Double negation of INT64_MIN magnitude still overflows
        Given an empty graph
        When executing query:
            """
            RETURN - -9223372036854775808 AS x
            """
        Then an error should be raised

    Scenario: INT64_MAX literal still works
        Given an empty graph
        When executing query:
            """
            RETURN 9223372036854775807 AS x
            """
        Then the result should be:
            | x                   |
            | 9223372036854775807 |
