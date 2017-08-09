Feature: Map operators

  Scenario: Returning a map
        When executing query:
            """
            RETURN {a: 1, b: 'bla', c: [1, 2], d: {a: 42}} as result
            """
        Then the result should be:
            | result                                  |
            | {a: 1, b: 'bla', c: [1, 2], d: {a: 42}} |


  Scenario: Storing a map property fails
        When executing query:
            """
            CREATE (n {property: {a: 1, b: 2}})
            """
	Then an error should be raised


  Scenario: A property lookup on a map literal
        When executing query:
            """
            WITH {a: 1, b: 'bla', c: {d: 42}} AS x RETURN x.a, x.c.d
            """
        Then the result should be:
            | x.a | x.c.d |
            | 1   | 42    |
