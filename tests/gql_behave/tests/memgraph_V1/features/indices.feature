Feature: Indices
    Scenario: Creating a composite index
        Given an empty graph
        And with new index :L1(a, b, c)
        When executing query:
            """
            SHOW INDEX INFO;
            """
        Then the result should be:
            | index type       | label | property        | count |
            | 'label+property' | 'L1'  | ['a', 'b', 'c'] | 0     |

    Scenario: Creating a nested index
        Given an empty graph
        And with new index :L1(a.b, c.d.e, f)
        When executing query:
            """
            SHOW INDEX INFO;
            """
        Then the result should be:
            | index type       | label | property              | count |
            | 'label+property' | 'L1'  | ['a.b', 'c.d.e', 'f'] | 0     |

    Scenario: Stats are created for all prefixes of a composite index
        Given an empty graph
        And with new index :L1(a, b, c)
        And having executed:
            """
            CREATE (:L1 {a: 11, b: 23, c:42 });
            """
        When executing query:
            """
            ANALYZE GRAPH;
            """
        Then the result should be:
            | label | property        | num estimation nodes | num groups | avg group size | chi-squared value | avg degree |
            | 'L1'  | ['a']           | 1                    | 1          | 1.0            | 0.0               | 0.0        |
            | 'L1'  | ['a', 'b']      | 1                    | 1          | 1.0            | 0.0               | 0.0        |
            | 'L1'  | ['a', 'b', 'c'] | 1                    | 1          | 1.0            | 0.0               | 0.0        |

    Scenario: Dropping an index deletes all computed stats for the index
        Given an empty graph
        And having executed:
            """
            CREATE INDEX ON :L1(a, b, c);
            """
        And having executed:
            """
            ANALYZE GRAPH;
            """
        And having executed:
            """
            DROP INDEX ON :L1(a, b, c);
            """
        When executing query:
            """
            ANALYZE GRAPH DELETE STATISTICS;
            """
        Then the result should be empty
