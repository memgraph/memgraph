Feature: Issue 2023 - extract() in WHERE clause must not leave a leftover filter

    Scenario: WHERE membership over extract() of a list-of-maps property
        Given an empty graph
        And having executed:
            """
            CREATE (:Actor {domain:'test.com', infringes:[{brand:'Customer A'}, {brand:'Customer B'}]})
            """
        When executing query:
            """
            MATCH (i:Actor {domain:'test.com'})
            WHERE 'Customer A' IN extract(v IN i.infringes | v.brand)
            RETURN i.domain AS d
            """
        Then the result should be:
            | d          |
            | 'test.com' |

    Scenario: WHERE membership over extract() with no matching value returns no rows
        Given an empty graph
        And having executed:
            """
            CREATE (:Actor {domain:'test.com', infringes:[{brand:'Customer A'}]})
            """
        When executing query:
            """
            MATCH (i:Actor {domain:'test.com'})
            WHERE 'Customer Z' IN extract(v IN i.infringes | v.brand)
            RETURN i.domain AS d
            """
        Then the result should be empty
