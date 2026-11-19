Feature: Map operators

  Scenario: Returning a map
        When executing query:
            """
            RETURN {a: 1, b: 'bla', c: [1, 2], d: {a: 42}} as result
            """
        Then the result should be:
            | result                                  |
            | {a: 1, b: 'bla', c: [1, 2], d: {a: 42}} |


  Scenario: Storing a map property
        Given an empty graph
        And having executed
            """
            CREATE ({prop: {x: 1, y: true, z: "bla"}})
            """
        When executing query:
            """
            MATCH (n) RETURN n.prop
            """
        Then the result should be:
            | n.prop                    |
            | {x: 1, y: true, z: 'bla'} |


  Scenario: A property lookup on a map literal
        When executing query:
            """
            WITH {a: 1, b: 'bla', c: {d: 42}} AS x RETURN x.a, x.c.d
            """
        Then the result should be:
            | x.a | x.c.d |
            | 1   | 42    |


  Scenario: Map indexing
        When executing query:
            """
            WITH {a: 1, b: 'bla', c: {d: 42}} AS x RETURN x["a"] as xa, x["c"]["d"] as xcd, x["z"] as xz
            """
        Then the result should be:
            | xa | xcd | xz   |
            | 1  | 42  | null |


  Scenario: An unquoted boolean keyword keeps its spelling when used as a name
        Given an empty graph
        When executing query:
            """
            CREATE (n:false {false: 1}) RETURN {false: 1} AS m, keys(n) AS k, labels(n) AS l
            """
        Then the result should be:
            | m          | k         | l         |
            | {false: 1} | ['false'] | ['false'] |
