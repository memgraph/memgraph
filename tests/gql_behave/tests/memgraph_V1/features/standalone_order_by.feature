Feature: Standalone ORDER BY, SKIP, OFFSET and LIMIT clauses

    Scenario: ORDER BY after WITH WHERE, then RETURN with LIMIT
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | lo | 1 |
        When executing query:
            """
            MATCH (n:N) WITH n, n.k AS k WHERE k > $lo ORDER BY k DESC RETURN n.k AS k LIMIT 1
            """
        Then the result should be, in order:
            | k |
            | 5 |

    Scenario: ORDER BY after WITH WHERE, then RETURN with LIMIT (indexes :N(k))
        Given an empty graph
        And with new index :N(k)
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | lo | 1 |
        When executing query:
            """
            MATCH (n:N) WITH n, n.k AS k WHERE k > $lo ORDER BY k DESC RETURN n.k AS k LIMIT 1
            """
        Then the result should be, in order:
            | k |
            | 5 |

    Scenario: ORDER BY after WITH WHERE sorts on two keys in mixed directions
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | lo | 1 |
            | l  | 2 |
        When executing query:
            """
            MATCH (n:N) WITH n WHERE n.k > $lo ORDER BY n.g DESC, n.k ASC LIMIT $l RETURN n.k AS k
            """
        Then the result should be, in order:
            | k |
            | 2 |
            | 4 |

    Scenario: ORDER BY with SKIP and LIMIT after WITH WHERE
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | lo | 1 |
            | s  | 1 |
            | l  | 2 |
        When executing query:
            """
            MATCH (n:N) WITH n WHERE n.k > $lo ORDER BY n.k DESC SKIP $s LIMIT $l RETURN n.k AS k
            """
        Then the result should be, in order:
            | k |
            | 4 |
            | 3 |

    Scenario: ORDER BY with SKIP and LIMIT after WITH WHERE (indexes :N(k))
        Given an empty graph
        And with new index :N(k)
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | lo | 1 |
            | s  | 1 |
            | l  | 2 |
        When executing query:
            """
            MATCH (n:N) WITH n WHERE n.k > $lo ORDER BY n.k DESC SKIP $s LIMIT $l RETURN n.k AS k
            """
        Then the result should be, in order:
            | k |
            | 4 |
            | 3 |

    Scenario: LIMIT after WITH WHERE
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | xs | [3, 1, 4, 2] |
            | lo | 1            |
            | l  | 2            |
        When executing query:
            """
            UNWIND $xs AS a WITH a WHERE a > $lo LIMIT $l RETURN a
            """
        Then the result should be:
            | a |
            | 3 |
            | 4 |

    Scenario: SKIP after WITH WHERE
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | xs | [3, 1, 4, 2] |
            | lo | 1            |
            | s  | 1            |
        When executing query:
            """
            UNWIND $xs AS a WITH a WHERE a > $lo SKIP $s RETURN a
            """
        Then the result should be:
            | a |
            | 4 |
            | 2 |

    Scenario: ORDER BY after WITH DISTINCT WHERE
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | xs | [2, 1, 3, 3] |
            | lo | 1            |
        When executing query:
            """
            UNWIND $xs AS a WITH DISTINCT a WHERE a > $lo ORDER BY a DESC RETURN a
            """
        Then the result should be, in order:
            | a |
            | 3 |
            | 2 |

    Scenario: ORDER BY after an aggregating WITH WHERE
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | z | 0 |
        When executing query:
            """
            MATCH (n:N) WITH n.g AS g, count(*) AS c WHERE c > $z ORDER BY c DESC, g LIMIT 1 RETURN g, c
            """
        Then the result should be, in order:
            | g   | c |
            | 'a' | 3 |

    Scenario: ORDER BY after MATCH WHERE sorts on a property the RETURN does not project
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | hi | 5 |
        When executing query:
            """
            MATCH (n:N) WHERE n.k < $hi ORDER BY n.k DESC LIMIT 1 RETURN n.g AS g
            """
        Then the result should be, in order:
            | g   |
            | 'b' |

    Scenario: ORDER BY after MATCH WHERE sorts on a property the RETURN does not project (indexes :N(k))
        Given an empty graph
        And with new index :N(k)
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | hi | 5 |
        When executing query:
            """
            MATCH (n:N) WHERE n.k < $hi ORDER BY n.k DESC LIMIT 1 RETURN n.g AS g
            """
        Then the result should be, in order:
            | g   |
            | 'b' |

    Scenario: ORDER BY and LIMIT after UNWIND
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | xs | [3, 1, 4, 2] |
            | l  | 2            |
        When executing query:
            """
            UNWIND $xs AS a ORDER BY a DESC LIMIT $l RETURN a
            """
        Then the result should be, in order:
            | a |
            | 4 |
            | 3 |

    Scenario: ORDER BY over an OPTIONAL MATCH null
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | xs | [7, 2, 4] |
            | l  | 2         |
        When executing query:
            """
            UNWIND $xs AS x OPTIONAL MATCH (n:N {k: x}) ORDER BY n.k DESC LIMIT $l RETURN x, n.k AS k
            """
        Then the result should be, in order:
            | x | k    |
            | 7 | null |
            | 4 | 4    |

    Scenario: ORDER BY and LIMIT after CREATE
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | xs | [3, 1, 2] |
            | l  | 2         |
        When executing query:
            """
            UNWIND $xs AS a CREATE (t:T {v: a}) ORDER BY t.v LIMIT $l RETURN t.v AS v
            """
        Then the result should be, in order:
            | v |
            | 1 |
            | 2 |

    Scenario: ORDER BY and LIMIT before SET writes only the kept rows
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | l | 2 |
        When executing query:
            """
            MATCH (n:N) ORDER BY n.k DESC LIMIT $l SET n.top = true WITH count(*) AS w MATCH (m:N) WHERE m.top RETURN m.k AS k ORDER BY k
            """
        Then the result should be, in order:
            | k |
            | 4 |
            | 5 |

    Scenario: ORDER BY and LIMIT before a MATCH on the same variable
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        When executing query:
            """
            MATCH (n:N) ORDER BY n.k DESC LIMIT 1 MATCH (n)-[:R]->(m) RETURN m.k AS k
            """
        Then the result should be, in order:
            | k  |
            | 50 |

    Scenario: ORDER BY and LIMIT before a MATCH on the same variable (indexes :N(k))
        Given an empty graph
        And with new index :N(k)
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        When executing query:
            """
            MATCH (n:N) ORDER BY n.k DESC LIMIT 1 MATCH (n)-[:R]->(m) RETURN m.k AS k
            """
        Then the result should be, in order:
            | k  |
            | 50 |

    Scenario: ORDER BY at the start of a query
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | x | 1 |
            | l | 0 |
        When executing query:
            """
            ORDER BY $x LIMIT $l RETURN 1 AS x
            """
        Then the result should be empty

    Scenario: LIMIT before ORDER BY limits first and sorts after
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | xs | [3, 1, 4, 2] |
            | l  | 2            |
        When executing query:
            """
            UNWIND $xs AS a LIMIT $l ORDER BY a RETURN a
            """
        Then the result should be, in order:
            | a |
            | 1 |
            | 3 |

    Scenario: LIMIT before SKIP limits first and skips after
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | xs | [3, 1, 4, 2] |
            | l  | 2            |
            | s  | 1            |
        When executing query:
            """
            UNWIND $xs AS a LIMIT $l SKIP $s RETURN a
            """
        Then the result should be:
            | a |
            | 1 |

    Scenario: The second of two ORDER BY clauses decides the order
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | xs | [3, 1, 4, 2] |
        When executing query:
            """
            UNWIND $xs AS a ORDER BY a DESC ORDER BY a ASC LIMIT 1 RETURN a
            """
        Then the result should be, in order:
            | a |
            | 1 |

    Scenario: OFFSET stays unsupported
        # OFFSET is not a synonym of SKIP.
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | xs | [3, 1, 2] |
            | s  | 1         |
        When executing query:
            """
            UNWIND $xs AS a ORDER BY a OFFSET $s RETURN a
            """
        Then an error should be raised

    Scenario: ORDER BY and LIMIT in a CALL subquery body
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | xs | [2, 1, 3] |
            | d  | 10        |
        When executing query:
            """
            UNWIND $xs AS a CALL (a) { UNWIND [a, a + $d] AS z ORDER BY z DESC LIMIT 1 RETURN z } RETURN a, z ORDER BY a
            """
        Then the result should be, in order:
            | a | z  |
            | 1 | 11 |
            | 2 | 12 |
            | 3 | 13 |

    Scenario: WITH WHERE then ORDER BY and LIMIT in a CALL subquery body
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | xs | [2, 1] |
            | d  | 10     |
            | e  | 20     |
        When executing query:
            """
            UNWIND $xs AS a CALL (a) { UNWIND [a, a + $d, a + $e] AS z WITH z WHERE z > a ORDER BY z DESC LIMIT 1 RETURN z } RETURN a, z ORDER BY a
            """
        Then the result should be, in order:
            | a | z  |
            | 1 | 21 |
            | 2 | 22 |

    Scenario: LIMIT in a COUNT subquery body
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | hi | 3         |
            | xs | [1, 2, 3] |
            | l  | 2         |
        When executing query:
            """
            MATCH (n:N) WHERE n.k < $hi RETURN n.k AS k, COUNT { UNWIND $xs AS z LIMIT $l RETURN z } AS c ORDER BY k
            """
        Then the result should be, in order:
            | k | c |
            | 1 | 2 |
            | 2 | 2 |

    Scenario: ORDER BY and LIMIT in both UNION legs
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | xs | [3, 1, 2] |
            | ys | [5, 4]    |
            | l  | 2         |
        When executing query:
            """
            UNWIND $xs AS a ORDER BY a LIMIT $l RETURN a UNION ALL UNWIND $ys AS a ORDER BY a DESC LIMIT 1 RETURN a
            """
        Then the result should be, in order:
            | a |
            | 1 |
            | 2 |
            | 5 |

    Scenario: Aggregation in a standalone ORDER BY is refused
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | xs | [2, 1] |
        When executing query:
            """
            UNWIND $xs AS a ORDER BY count(*) RETURN a
            """
        # A unit test asserts the error text.
        Then an error should be raised

    Scenario: A standalone ORDER BY takes no WHERE
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | xs | [3, 1, 2] |
            | lo | 1         |
            | hi | 3         |
        When executing query:
            """
            UNWIND $xs AS a WITH a WHERE a > $lo ORDER BY a DESC WHERE a < $hi RETURN a
            """
        # A unit test asserts the error text.
        Then an error should be raised

    Scenario: A query cannot end with ORDER BY
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | xs | [2, 1] |
        When executing query:
            """
            UNWIND $xs AS a ORDER BY a
            """
        # A unit test asserts the error text.
        Then an error should be raised

    Scenario: ORDER BY after WITH WHERE cannot read a variable the WITH dropped
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | xs | [3, 1, 2] |
            | lo | 1         |
        When executing query:
            """
            UNWIND $xs AS a WITH a AS b WHERE b > $lo ORDER BY a DESC RETURN b
            """
        # A unit test asserts the error text.
        Then an error should be raised

    Scenario: A standalone LIMIT after RETURN is refused
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | xs | [3, 1, 2] |
            | l  | 1         |
        When executing query:
            """
            UNWIND $xs AS a RETURN a LIMIT $l LIMIT $l
            """
        # A unit test asserts the error text.
        Then an error should be raised

    Scenario: ORDER BY before WHERE in a WITH projection
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | xs | [3, 1, 2] |
            | lo | 2         |
            | l  | 2         |
        When executing query:
            """
            UNWIND $xs AS a WITH a ORDER BY a DESC LIMIT $l WHERE a > $lo RETURN a
            """
        Then the result should be, in order:
            | a |
            | 3 |

    Scenario: ORDER BY, SKIP and LIMIT in a RETURN projection
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | xs | [3, 1, 4, 2] |
            | s  | 1            |
            | l  | 1            |
        When executing query:
            """
            UNWIND $xs AS a RETURN a ORDER BY a DESC SKIP $s LIMIT $l
            """
        Then the result should be, in order:
            | a |
            | 3 |

    Scenario: SKIP and LIMIT stay usable as variable names
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | xs | [3, 1, 2] |
            | l  | 2         |
        When executing query:
            """
            UNWIND $xs AS skip WITH skip AS limit ORDER BY limit DESC LIMIT $l RETURN limit
            """
        Then the result should be, in order:
            | limit |
            | 3     |
            | 2     |

    Scenario: SKIP and LIMIT stay usable as unaliased RETURN items
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {k: 1, g: 'a'}), (b:N {k: 2, g: 'b'}), (c:N {k: 3, g: 'a'}), (d:N {k: 4, g: 'b'}), (e:N {k: 5, g: 'a'}),
                   (:M {k: 9}), (e)-[:R]->(:M {k: 50}), (a)-[:R]->(:M {k: 10})
            """
        And parameters are:
            | xs | [3, 1, 2] |
            | lo | 1         |
        When executing query:
            """
            UNWIND $xs AS skip WITH skip, skip AS limit WHERE skip > $lo RETURN skip, limit
            """
        Then the result should be:
            | skip | limit |
            | 3    | 3     |
            | 2    | 2     |
