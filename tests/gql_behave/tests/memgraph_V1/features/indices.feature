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

    Scenario: Cannot create a composite index with duplicate keys
        Given an empty graph
        When executing query:
            """
            CREATE INDEX ON :L1(a, b, a)
            """
        Then an error should be raised

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

    Scenario: Can create a nested index with duplicate top-most properties
        Given an empty graph
        And with new index :L1(a.b, a.c, a.d)
        When executing query:
            """
            SHOW INDEX INFO;
            """
        Then the result should be:
            | index type       | label | property              | count |
            | 'label+property' | 'L1'  | ['a.b', 'a.c', 'a.d'] | 0     |

    Scenario: Cannot create a nested index with duplicate path prefixes 01
        Given an empty graph
        When executing query:
            """
            CREATE INDEX ON :L1(a, a.b)
            """
        Then an error should be raised

    Scenario: Cannot create a nested index with duplicate path prefixes 02
        Given an empty graph
        When executing query:
            """
            CREATE INDEX ON :L1(a, a.b.c)
            """
        Then an error should be raised

    Scenario: Cannot create a nested index with duplicate path prefixes 03
        Given an empty graph
        When executing query:
            """
            CREATE INDEX ON :L1(a.b, a.b.c)
            """
        Then an error should be raised

    Scenario: Cannot create a nested index with duplicate path prefixes 04
        Given an empty graph
        When executing query:
            """
            CREATE INDEX ON :L1(a.b, a.b.c.d)
            """
        Then an error should be raised

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

    Scenario: IN works with label+property indices
        Given an empty graph
        And with new index :L1(a)
        And having executed:
            """
            CREATE (:L1 {a: 2}), (:L1 {a: 3}), (:L1 {a: 5});
            """
        When executing query:
            """
            MATCH (x:L1) WHERE x.a IN [2, 5] RETURN x.a;
            """
        Then the result should be:
            | x.a |
            | 2   |
            | 5   |

    Scenario: Global edge indices show correctly in index info
        Given an empty graph
        And with new edge index :(prop1)
        And with new edge index :(prop2)
        When executing query:
            """
            SHOW INDEX INFO;
            """
        Then the result should be:
            | index type      | label | property | count |
            | 'edge-property' | null  | 'prop1'  | 0     |
            | 'edge-property' | null  | 'prop2'  | 0     |

    Scenario: DROP INDEX WITH CONFIG order ASC drops only the ASC index
        Given an empty graph
        And having executed:
            """
            CREATE INDEX ON :L(prop);
            """
        And having executed:
            """
            CREATE INDEX ON :L(prop) WITH CONFIG {"order": "DESC"};
            """
        And having executed:
            """
            DROP INDEX ON :L(prop) WITH CONFIG {"order": "ASC"};
            """
        When executing query:
            """
            SHOW INDEX INFO;
            """
        Then the result should be:
            | index type              | label | property | count |
            | 'label+property (DESC)' | 'L'   | ['prop'] | 0     |

    Scenario: DROP INDEX WITH CONFIG order DESC drops only the DESC index
        Given an empty graph
        And having executed:
            """
            CREATE INDEX ON :L(prop);
            """
        And having executed:
            """
            CREATE INDEX ON :L(prop) WITH CONFIG {"order": "DESC"};
            """
        And having executed:
            """
            DROP INDEX ON :L(prop) WITH CONFIG {"order": "DESC"};
            """
        When executing query:
            """
            SHOW INDEX INFO;
            """
        Then the result should be:
            | index type       | label | property | count |
            | 'label+property' | 'L'   | ['prop'] | 0     |

    Scenario: DROP INDEX without config drops both ASC and DESC
        Given an empty graph
        And having executed:
            """
            CREATE INDEX ON :L(prop);
            """
        And having executed:
            """
            CREATE INDEX ON :L(prop) WITH CONFIG {"order": "DESC"};
            """
        And having executed:
            """
            DROP INDEX ON :L(prop);
            """
        When executing query:
            """
            SHOW INDEX INFO;
            """
        Then the result should be empty

    Scenario: DROP INDEX WITH CONFIG for a missing order leaves the existing order untouched
        Given an empty graph
        And having executed:
            """
            CREATE INDEX ON :L(prop);
            """
        And having executed:
            """
            DROP INDEX ON :L(prop) WITH CONFIG {"order": "DESC"};
            """
        When executing query:
            """
            SHOW INDEX INFO;
            """
        Then the result should be:
            | index type       | label | property | count |
            | 'label+property' | 'L'   | ['prop'] | 0     |

    Scenario: DROP INDEX WITH CONFIG rejects an invalid order value
        Given an empty graph
        And having executed:
            """
            CREATE INDEX ON :L(prop);
            """
        When executing query:
            """
            DROP INDEX ON :L(prop) WITH CONFIG {"order": "SIDEWAYS"};
            """
        Then an error should be raised

    Scenario: CREATE INDEX WITH CONFIG is rejected on a label-only index
        Given an empty graph
        When executing query:
            """
            CREATE INDEX ON :L WITH CONFIG {"order": "ASC"};
            """
        Then an error should be raised

    Scenario: DROP INDEX WITH CONFIG is rejected on a label-only index
        Given an empty graph
        And having executed:
            """
            CREATE INDEX ON :L;
            """
        When executing query:
            """
            DROP INDEX ON :L WITH CONFIG {"order": "ASC"};
            """
        Then an error should be raised

    Scenario: DROP INDEX WITH CONFIG rejects an unknown config key
        Given an empty graph
        And having executed:
            """
            CREATE INDEX ON :L(prop);
            """
        When executing query:
            """
            DROP INDEX ON :L(prop) WITH CONFIG {"foo": "ASC"};
            """
        Then an error should be raised
