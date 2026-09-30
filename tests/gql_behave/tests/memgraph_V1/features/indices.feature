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

    Scenario: An index whose bound reads another pattern's property returns the unindexed result
        Given an empty graph
        And having executed:
            """
            CREATE (:L1 {a: 'b'}), (:L1 {a: 'x'}), (:L2 {b: 'm'}), (:L2 {b: 'c'});
            """
        And with new index :L2(b)
        When executing query:
            """
            MATCH (n :L1), (m :L2) WITH * WHERE n.a < m.b RETURN m.b ORDER BY m.b;
            """
        Then the result should be:
            | m.b |
            | 'c' |
            | 'm' |

    Scenario: A correlated pattern comprehension returns the unindexed result
        Given an empty graph
        And having executed:
            """
            CREATE (a:L {id: 1}), (b:L {id: 2}), (c:L {id: 1})
            CREATE (a)-[:E]->(b), (a)-[:E]->(c), (b)-[:E]->(c);
            """
        And with new index :L(id)
        When executing query:
            """
            MATCH (m:L) RETURN m.id AS id, size([ (n:L {id: m.id})-[]-(q) | q ]) AS c ORDER BY id, c;
            """
        Then the result should be:
            | id | c |
            | 1  | 4 |
            | 1  | 4 |
            | 2  | 2 |

    Scenario: A correlated pattern comprehension over an edge property returns the unindexed result
        Given an empty graph
        And having executed:
            """
            CREATE EDGE INDEX ON :E(w);
            """
        And having executed:
            """
            CREATE (a:L {w: 1}), (b:L {w: 2})
            CREATE (a)-[:E {w: 1}]->(b), (a)-[:E {w: 2}]->(b);
            """
        When executing query:
            """
            MATCH (m:L) RETURN m.w AS w, size([ ()-[r:E {w: m.w}]->() | r ]) AS c ORDER BY w;
            """
        Then the result should be:
            | w | c |
            | 1 | 1 |
            | 2 | 1 |

    # The body reads the scanned variable only through its WHERE, so the index cannot be sought by the COUNT.
    Scenario: A subquery that reads the scanned variable returns the unindexed result
        Given an empty graph
        And having executed:
            """
            CREATE (:N {v: 1, id: 1}), (:N {v: 2, id: 2}), (:N {v: 1, id: 3})
            CREATE (:X {id: 1}), (:X {id: 2}), (:X {id: 2});
            """
        And with new index :N(v)
        When executing query:
            """
            MATCH (n:N) WHERE n.v = COUNT { MATCH (x:X) WHERE x.id = n.id } RETURN n.id AS id ORDER BY id;
            """
        Then the result should be, in order:
            | id |
            | 1  |
            | 2  |

    # ORDER BY lets the parallel pass plan the parallel index scans.
    Scenario Outline: A list bound keeps the same rows with and without an index
        Given an empty graph
        And having executed:
            """
            UNWIND [[1], [1, 2], [1, 3], [1, null], [null, 1], [2], 5, 'a'] AS v
            CREATE (:L {p: v, q: 1})-[:R {p: v}]->(:M)
            """
        And having executed:
            """
            <index>
            """
        When executing query:
            """
            <match> WITH <var>.p AS p ORDER BY p RETURN collect(p) AS ps
            """
        Then the result should be:
            | ps     |
            | <rows> |

        Examples:
            | index                            | match                                                | var | rows                  |
            | RETURN 1                         | MATCH (n:L) WHERE n.q = 1 AND n.p > [1, 2]           | n   | [[1, 3], [2]]         |
            | CREATE INDEX ON :L(p)            | MATCH (n:L) WHERE n.q = 1 AND n.p > [1, 2]           | n   | [[1, 3], [2]]         |
            | CREATE INDEX ON :L(q, p)         | MATCH (n:L) WHERE n.q = 1 AND n.p > [1, 2]           | n   | [[1, 3], [2]]         |
            | CREATE GLOBAL INDEX ON :(p)      | MATCH (n:L) WHERE n.q = 1 AND n.p > [1, 2]           | n   | [[1, 3], [2]]         |
            | RETURN 1                         | MATCH (n) WHERE n.p <= [1, 3]                        | n   | [[1], [1, 2], [1, 3]] |
            | CREATE GLOBAL INDEX ON :(p)      | MATCH (n) WHERE n.p <= [1, 3]                        | n   | [[1], [1, 2], [1, 3]] |
            | RETURN 1                         | MATCH ()-[r:R]->() WHERE r.p > [1, 2]                | r   | [[1, 3], [2]]         |
            | CREATE EDGE INDEX ON :R(p)       | MATCH ()-[r:R]->() WHERE r.p > [1, 2]                | r   | [[1, 3], [2]]         |
            | CREATE GLOBAL EDGE INDEX ON :(p) | MATCH ()-[r:R]->() WHERE r.p > [1, 2]                | r   | [[1, 3], [2]]         |

    Scenario Outline: A label disjunction keeps the rows of its upstream with and without an index
        Given an empty graph
        And having executed:
            """
            CREATE (a1:A {p: 1}), (a2:A {p: 2}), (b1:B {p: 1}), (ab:A:B {p: 2}), (z1:Z), (z2:Z)
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1)
            """
        And having executed:
            """
            <index_a>
            """
        And having executed:
            """
            <index_b>
            """
        When executing query:
            """
            <query>
            """
        Then the result should be:
            | r     |
            | <r>   |

        Examples:
            | index_a              | index_b              | query                                                                                                | r                |
            | RETURN 1             | RETURN 1             | UNWIND [1, 2] AS x MATCH (n:A\|B) WITH x, count(*) AS c ORDER BY x RETURN collect([x, c]) AS r        | [[1, 4], [2, 4]] |
            | CREATE INDEX ON :A   | CREATE INDEX ON :B   | UNWIND [1, 2] AS x MATCH (n:A\|B) WITH x, count(*) AS c ORDER BY x RETURN collect([x, c]) AS r        | [[1, 4], [2, 4]] |
            | CREATE INDEX ON :A   | CREATE INDEX ON :B   | UNWIND [1, 1] AS x MATCH (n:A\|B) RETURN count(*) AS r                                               | 8                |
            | CREATE INDEX ON :A   | CREATE INDEX ON :B   | UNWIND [1, 2] AS x MATCH (n) WHERE n:A OR n:B WITH x, count(*) AS c ORDER BY x RETURN collect([x, c]) AS r | [[1, 4], [2, 4]] |
            | RETURN 1             | RETURN 1             | UNWIND [1, 2, 3] AS x MATCH (n:A\|B {p: x}) WITH x, count(*) AS c ORDER BY x RETURN collect([x, c]) AS r | [[1, 2], [2, 2]] |
            | CREATE INDEX ON :A(p) | CREATE INDEX ON :B(p) | UNWIND [1, 2, 3] AS x MATCH (n:A\|B {p: x}) WITH x, count(*) AS c ORDER BY x RETURN collect([x, c]) AS r | [[1, 2], [2, 2]] |
            | RETURN 1             | RETURN 1             | MATCH (z:Z) WITH z MATCH (z)-[*1..2]->(n:A\|B) RETURN count(*) AS r                                  | 4                |
            | CREATE INDEX ON :A   | CREATE INDEX ON :B   | MATCH (z:Z) WITH z MATCH (z)-[*1..2]->(n:A\|B) RETURN count(*) AS r                                  | 4                |
            | CREATE INDEX ON :A   | CREATE INDEX ON :B   | MATCH (z:Z) WITH z MATCH (z)-->(n:A\|B) RETURN count(*) AS r                                         | 4                |
            | CREATE INDEX ON :A   | CREATE INDEX ON :B   | MATCH (m:A) WITH m MATCH (n:A\|B) RETURN count(*) AS r                                               | 12               |
            | CREATE INDEX ON :A   | CREATE INDEX ON :B   | UNWIND [1, 2] AS x MATCH (n:A\|B) WITH x, n LIMIT 6 WITH x, count(*) AS c ORDER BY x RETURN collect([x, c]) AS r | [[1, 4], [2, 2]] |
            | CREATE INDEX ON :A(p) | CREATE INDEX ON :B(p) | UNWIND [1, 2] AS x MATCH (n:A\|B) WHERE n.p >= x WITH x, count(*) AS c ORDER BY x RETURN collect([x, c]) AS r | [[1, 4], [2, 2]] |
            | CREATE INDEX ON :A(p) | CREATE INDEX ON :B(p) | UNWIND [1, 2] AS x MATCH (n:A\|B) WHERE n.p IN [1, 2, 3] WITH x, count(*) AS c ORDER BY x RETURN collect([x, c]) AS r | [[1, 4], [2, 4]] |
            | CREATE INDEX ON :A(p) | CREATE INDEX ON :B(p) | MATCH (n:A\|B) WHERE n.p IN [1, 2, 3] RETURN count(*) AS r                                          | 4                |
            | CREATE INDEX ON :A   | CREATE INDEX ON :B   | MATCH (z:Z) WITH z, COUNT { MATCH (z)-->(k), (n:A\|B) } AS c RETURN collect(c) AS r               | [8, 8]           |

    Scenario: A write before an indexed label disjunction runs once
        Given an empty graph
        And having executed:
            """
            CREATE INDEX ON :A
            """
        And having executed:
            """
            CREATE INDEX ON :B
            """
        And having executed:
            """
            CREATE (:A), (:A), (:B), (:A:B)
            """
        When executing query:
            """
            CREATE (w:W) WITH w MATCH (n:A|B) WITH count(*) AS c MATCH (w:W) RETURN c, count(w) AS ws
            """
        Then the result should be:
            | c | ws |
            | 4 | 1  |

    Scenario Outline: An indexed label disjunction that seeks on a variable-length edge list keeps the rows
        Given an empty graph
        And having executed:
            """
            CREATE INDEX ON :A
            """
        And having executed:
            """
            CREATE INDEX ON :B
            """
        And having executed:
            """
            CREATE INDEX ON :A(p)
            """
        And having executed:
            """
            CREATE INDEX ON :B(p)
            """
        And having executed:
            """
            CREATE (:Z)-[:R]->(:A {p: 1})-[:R]->(:A {p: 2})-[:R]->(:A {p: 3})-[:R]->(:B {p: 3}), (:B {p: 1}), (:A:B {p: 2})
            """
        When executing query:
            """
            <query>
            """
        Then the result should be:
            | r |
            | 6 |

        Examples:
            | query                                                                                                        |
            | MATCH (a:Z)-[r*1..3]->(b) UNWIND [1] AS x MATCH (n:A\|B) WHERE n.p = size(r) RETURN count(DISTINCT n) AS r   |
            | MATCH (a:Z)-[r*1..3]->(b) OPTIONAL MATCH (n:A\|B) WHERE n.p = size(r) RETURN count(DISTINCT n) AS r          |
