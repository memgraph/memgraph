Feature: Indexed label disjunction scan on disk

    Background:
        Given an empty graph
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """

    Scenario: A label disjunction keeps every upstream row (indexes :A, :B)
        Given with new index :A
        And with new index :B
        And parameters are:
            | xs | [1, 2] |
        When executing query:
            """
            UNWIND $xs AS x MATCH (n:A|B) WITH x, n.n AS v ORDER BY x, v RETURN x, collect(v) AS vs ORDER BY x
            """
        Then the result should be, in order:
            | x | vs                                   |
            | 1 | ['a1', 'a2', 'ab', 'ac', 'b1', 'bc'] |
            | 2 | ['a1', 'a2', 'ab', 'ac', 'b1', 'bc'] |

    Scenario: A label removed in a writing subquery does not repeat a node (indexes :A, :B)
        Given with new index :A
        And with new index :B
        When executing query:
            """
            MATCH (n:A|B) CALL { WITH n REMOVE n:A WITH n MATCH (m:O) RETURN count(m) AS k } RETURN count(*) AS c
            """
        Then the result should be:
            | c |
            | 6 |
